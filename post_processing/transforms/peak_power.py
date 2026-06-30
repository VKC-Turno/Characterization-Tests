"""Peak-power extraction (lab-reference port).

Short (≤30 s) high-current pulses at SoC anchors, both directions. Report the
sustained power |V·I| at 10 s into each pulse, labelled by the SoC at the
pulse start (from the SoC tracker, rounded to 2 dp).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import PEAK_POWER_SCHEMA
from ._steps import build_steps, soc_at_step_starts

PROBE_TIME_S = 10.0
MAX_PULSE_DURATION_S = 30.0


def _probe(d: pd.DataFrame, dt_target: float):
    """(power_w, v, i) at dt_target s into the pulse, or (nan,nan,nan) if short."""
    if d.empty:
        return float("nan"), float("nan"), float("nan")
    t = (d["absolute_time"] - d["absolute_time"].iloc[0]).dt.total_seconds().to_numpy()
    if t[-1] < dt_target:
        return float("nan"), float("nan"), float("nan")
    v = float(np.interp(dt_target, t, d["volt_v"].to_numpy()))
    i = float(np.interp(dt_target, t, d["current_a"].to_numpy()))
    return abs(v * i), v, i


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make = str(pdf["make"].iloc[0]); batch = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")
    nom = max_cap
    empty = pd.DataFrame(columns=[f.name for f in PEAK_POWER_SCHEMA.fields])
    if not np.isfinite(nom) or nom <= 0:
        return empty

    step, pdf = build_steps(pdf)
    soc_all = soc_at_step_starts(step, nom)
    short = step["duration_s"] <= MAX_PULSE_DURATION_S
    big = step["current_mean_a"].abs() > 1.0
    sn = step["step_name"].astype(str)

    rows = {}   # (direction, soc) -> row (last wins, like the reference dict)
    for direction, frag in (("dchg", "DChg"), ("chg", "CC_Chg")):
        sel = step[short & big & sn.str.contains(frag, case=False, na=False)]
        for idx, p in sel.iterrows():
            d = pdf[pdf["_block"] == p["_block"]].sort_values("absolute_time")
            pw, v, i = _probe(d, PROBE_TIME_S)
            soc_v = soc_all.get(idx, float("nan"))
            soc = round(float(soc_v), 2) if pd.notna(soc_v) else float("nan")
            rows[(direction, soc)] = {
                "make": make, "batch": batch, "cell_no": cell_no, "max_cap": max_cap,
                "direction": direction, "soc": float(soc), "p_peak_w": float(pw),
                "v_at_peak": float(v), "i_at_peak": float(i),
                "duration_s": float(p["duration_s"])}
    if not rows:
        return empty
    return pd.DataFrame(list(rows.values()))[[f.name for f in PEAK_POWER_SCHEMA.fields]]


def extract_peak_power(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("PeakPower"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_extract_one_cell, schema=PEAK_POWER_SCHEMA))
