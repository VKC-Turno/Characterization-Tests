"""GITT extraction (lab-reference port).

Walk (pulse, rest) step pairs. A usable pulse is a CC_DChg of 60–1800 s with
|I| ≥ 1 A followed by a rest ≥ 600 s. Per pulse:
    r_pulse  = |V_end − V_start| / |I| · 1000           (mΩ)
    SoC      = soc_at_step_starts (descends from 1.0)
    V_inf,τ  = single-exponential fit of the rest relaxation V(t)=V∞+A·e^(−t/τ)
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import GITT_PULSE_SCHEMA
from ._steps import build_steps, soc_at_step_starts

MIN_PULSE_DURATION_S = 60.0
MAX_PULSE_DURATION_S = 1800.0
MIN_PULSE_CURRENT_A  = 1.0
MIN_REST_DURATION_S  = 600.0
MIN_REST_SAMPLES     = 30


def _fit_relaxation(rest_d: pd.DataFrame, direction: int):
    if len(rest_d) < MIN_REST_SAMPLES:
        return float("nan"), float("nan")
    try:
        from scipy.optimize import curve_fit
    except Exception:
        return float("nan"), float("nan")
    t0 = rest_d["absolute_time"].iloc[0]
    t = (rest_d["absolute_time"] - t0).dt.total_seconds().to_numpy(dtype=float)
    v = rest_d["volt_v"].to_numpy(dtype=float)
    v0, v_last = float(v[0]), float(v[-1])
    V_inf_guess = v_last
    A_guess = v0 - V_inf_guess
    tau_guess = max(60.0, (t[-1] - t[0]) / 5.0)
    if direction < 0:
        A_lo, A_hi = -0.5, -1e-5
        Vinf_lo, Vinf_hi = v0, v0 + 1.0
    else:
        A_lo, A_hi = 1e-5, 0.5
        Vinf_lo, Vinf_hi = v0 - 1.0, v0
    tau_lo, tau_hi = 5.0, 1e5
    V_inf_guess = float(np.clip(V_inf_guess, Vinf_lo + 1e-6, Vinf_hi - 1e-6))
    A_guess = float(np.clip(A_guess, A_lo + 1e-7, A_hi - 1e-7))

    def model(tt, V_inf, A, tau):
        return V_inf + A * np.exp(-tt / tau)

    try:
        popt, _ = curve_fit(model, t, v, p0=(V_inf_guess, A_guess, tau_guess),
                            bounds=([Vinf_lo, A_lo, tau_lo], [Vinf_hi, A_hi, tau_hi]),
                            maxfev=10_000)
        return float(popt[0]), float(popt[2])
    except Exception:
        return float("nan"), float("nan")


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make = str(pdf["make"].iloc[0]); batch = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")
    nom = max_cap
    empty = pd.DataFrame(columns=[f.name for f in GITT_PULSE_SCHEMA.fields])
    if not np.isfinite(nom) or nom <= 0:
        return empty

    step, pdf = build_steps(pdf)
    ordered = step.sort_values("start_time").reset_index(drop=True)
    orig_index = step.sort_values("start_time").index
    soc_at_start = soc_at_step_starts(step, nom)

    rows = []
    pulse_idx = 0
    for i in range(len(ordered) - 1):
        pulse = ordered.iloc[i]; nxt = ordered.iloc[i + 1]
        if "CC_DChg" not in str(pulse["step_name"]):
            continue
        dur = float(pulse["duration_s"])
        if dur < MIN_PULSE_DURATION_S or dur > MAX_PULSE_DURATION_S:
            continue
        i_mean = float(pulse["current_mean_a"])
        if abs(i_mean) < MIN_PULSE_CURRENT_A:
            continue
        if "rest" not in str(nxt["step_name"]).lower() or float(nxt["duration_s"]) < MIN_REST_DURATION_S:
            continue

        r_mohm = abs(float(pulse["end_volt_v"]) - float(pulse["start_volt_v"])) / abs(i_mean) * 1000.0
        rest_d = pdf[pdf["_block"] == nxt["_block"]].sort_values("absolute_time").reset_index(drop=True)
        V_inf, tau = _fit_relaxation(rest_d, -1 if i_mean < 0 else 1)
        soc_v = soc_at_start.get(orig_index[i], float("nan"))
        pulse_idx += 1
        rows.append({"make": make, "batch": batch, "cell_no": cell_no, "max_cap": max_cap,
                     "pulse_idx": pulse_idx,
                     "soc": round(float(soc_v), 3) if pd.notna(soc_v) else float("nan"),
                     "r_pulse_mohm": float(r_mohm), "tau_diff_s": float(tau), "v_inf_v": float(V_inf)})
    if not rows:
        return empty
    return pd.DataFrame(rows)[[f.name for f in GITT_PULSE_SCHEMA.fields]]


def extract_gitt_pulses(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("GITT"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_extract_one_cell, schema=GITT_PULSE_SCHEMA))
