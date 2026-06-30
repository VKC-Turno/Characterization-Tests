"""DCIR R0 extraction — within-step pulse detection (lab-reference port).

The DCIR protocol fires several short ~10 s high-current pulses on top of a
slow baseline discharge, all inside ONE ``CC_DChg`` step. We detect the
individual pulses by thresholding |I| at the midpoint of baseline/peak, healing
sub-2 s dropouts, and splitting each high-current region into round(dur/10 s)
pulses. SoC at each pulse is tracked via a running ∫I·dt integral.

dcir_i_at_pulse is the SIGNED mean current (negative for discharge); r_dc uses
the magnitude.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import DCIR_ANCHOR_SCHEMA
from ._steps import build_steps, soc_at_step_starts

MIN_PULSE_DURATION_S   = 1.0
MIN_BASELINE_CURRENT_A = 1.0
PULSE_LEN_S            = 10.0      # nominal single-pulse duration
MERGE_GAP_S            = 2.0       # heal detector dropouts shorter than this
MIN_PEAK_OVER_BASELINE = 1.5       # a step has pulses only if peak ≫ baseline


def _find_pulses(d: pd.DataFrame):
    """Return [(start_idx, end_idx)] of individual pulses inside one CC_DChg step."""
    if len(d) < 2:
        return []
    abs_i = d["current_a"].abs().to_numpy()
    base = float(np.median(abs_i))
    peak = float(np.nanmax(abs_i))
    if peak < MIN_PEAK_OVER_BASELINE * max(base, MIN_BASELINE_CURRENT_A):
        return []
    thr = (base + peak) / 2.0
    in_pulse = abs_i > thr
    if not in_pulse.any():
        return []
    edges = np.diff(in_pulse.astype(int))
    starts = list(np.where(edges == 1)[0] + 1)
    ends = list(np.where(edges == -1)[0] + 1)
    if in_pulse[0]:
        starts.insert(0, 0)
    if in_pulse[-1]:
        ends.append(len(d))

    t = (d["absolute_time"] - d["absolute_time"].iloc[0]).dt.total_seconds().to_numpy()
    # merge dropouts < MERGE_GAP_S into contiguous regions
    regions = []
    cur_s, cur_e = starts[0], ends[0]
    for s, e in zip(starts[1:], ends[1:]):
        gap = t[s] - t[cur_e - 1] if (cur_e - 1) < len(t) else float("inf")
        if gap < MERGE_GAP_S:
            cur_e = e
        else:
            regions.append((cur_s, cur_e))
            cur_s, cur_e = s, e
    regions.append((cur_s, cur_e))

    # split each region into round(dur / PULSE_LEN_S) equal sub-pulses
    pulses = []
    for a, b in regions:
        dur = t[b - 1] - t[a]
        n = max(1, int(round(dur / PULSE_LEN_S)))
        if n == 1:
            pulses.append((a, b))
            continue
        bounds = np.linspace(a, b, n + 1).round().astype(int)
        for k in range(n):
            p0, p1 = int(bounds[k]), int(bounds[k + 1])
            if p1 - p0 >= 2:
                pulses.append((p0, p1))
    return pulses


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make = str(pdf["make"].iloc[0]); batch = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")
    nom = max_cap
    empty = pd.DataFrame(columns=[f.name for f in DCIR_ANCHOR_SCHEMA.fields])
    if not np.isfinite(nom) or nom <= 0:
        return empty

    step, pdf = build_steps(pdf)
    soc_start = soc_at_step_starts(step, nom)

    socs, cur_l, r_l = [], [], []
    for idx, s in step.sort_values("start_time").iterrows():
        if "CC_DChg" not in str(s["step_name"]):
            continue
        if abs(float(s["current_mean_a"])) < MIN_BASELINE_CURRENT_A:
            continue
        d = pdf[pdf["_block"] == s["_block"]].sort_values("absolute_time").reset_index(drop=True)
        if len(d) < 3 or float(d["current_a"].abs().median()) < MIN_BASELINE_CURRENT_A:
            continue
        bursts = _find_pulses(d)
        if not bursts:
            continue
        s0 = float(soc_start.get(idx, float("nan")))
        if not np.isfinite(s0):
            continue
        t = (d["absolute_time"] - d["absolute_time"].iloc[0]).dt.total_seconds().to_numpy()
        I = d["current_a"].to_numpy(dtype=float)
        dt = np.r_[0.0, np.diff(t)]
        soc_running = s0 + np.cumsum(I * dt / 3600.0 / nom)
        for (a, b) in bursts:
            if b - a < 2 or (t[b - 1] - t[a]) < MIN_PULSE_DURATION_S:
                continue
            burst_i = float(np.mean(I[a:b]))                 # SIGNED
            if abs(burst_i) < 1e-6:
                continue
            v_s = float(d["volt_v"].iloc[a]); v_e = float(d["volt_v"].iloc[b - 1])
            R = abs(v_s - v_e) / abs(burst_i) * 1000.0       # mΩ
            socs.append(round(float(soc_running[a]), 3))
            cur_l.append(burst_i)
            r_l.append(R)

    if not socs:
        return empty
    out = pd.DataFrame({
        "make": make, "batch": batch, "cell_no": cell_no, "max_cap": max_cap,
        "pulse_idx": list(range(1, len(socs) + 1)),     # detection order
        "soc": [float(x) for x in socs],
        "r0_mohm": [float(x) for x in r_l],
        "i_at_pulse": [float(x) for x in cur_l],
    })
    return out[[f.name for f in DCIR_ANCHOR_SCHEMA.fields]]


def extract_dcir_anchors(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("DCIR"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_extract_one_cell, schema=DCIR_ANCHOR_SCHEMA))
