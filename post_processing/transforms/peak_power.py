"""Peak-power extraction — P_max envelope per SoC, per direction.

Protocol: at multiple SoC anchors, the cell sees short pulses of increasing
current magnitude until a voltage cutoff is hit. Peak power = max(|V·I|)
across the pulse, with the instantaneous V and I recorded.

We treat each (cycle, direction) pair as one SoC anchor and emit:
  - p_peak_w, v_at_peak, i_at_peak, duration_s

SoC ordinal is taken from the cycle order — the protocol typically does
9 cycles spanning 0.1 → 0.9 SoC.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import PEAK_POWER_SCHEMA


def _derive_step_no(df: pd.DataFrame) -> pd.Series:
    if "cycler_step_no" in df.columns and df["cycler_step_no"].notna().any():
        return df["cycler_step_no"].astype("Int64")
    names = pd.Series(df["step_name"].astype(str).values)
    return (names != names.shift()).cumsum()


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make    = str(pdf["make"].iloc[0])
    batch   = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])

    pdf = pdf.sort_values("absolute_time").reset_index(drop=True).copy()
    pdf["_step_no"] = _derive_step_no(pdf)
    pdf["_power"]   = (pdf["volt_v"] * pdf["current_a"].abs()).astype(float)

    rows = []
    # Build per-step summary first to find which steps are pulses
    agg = pdf.groupby("_step_no").agg(
        step_name=("step_name", "first"),
        cycle_no= ("cycle_no", "first"),
        i_mean=   ("current_a", lambda s: float(s.abs().mean())),
        t_start=  ("absolute_time", "first"),
        t_end=    ("absolute_time", "last"),
    ).reset_index()
    agg["duration_s"] = (pd.to_datetime(agg["t_end"]) -
                          pd.to_datetime(agg["t_start"])).dt.total_seconds()

    # Anchor SoC by ordinal cycle index. Discover unique cycles in order
    cycles_seen = list(dict.fromkeys(agg["cycle_no"].dropna().astype(int).tolist()))
    soc_of_cycle = {c: (i + 1) / (len(cycles_seen) + 1) for i, c in enumerate(cycles_seen)} \
        if cycles_seen else {}

    for _, s in agg.iterrows():
        sn = str(s["step_name"])
        if s["i_mean"] < 1.0 or s["duration_s"] < 1.0 or s["duration_s"] > 120.0:
            continue
        if "DChg" in sn:
            direction = "dchg"
        elif "Chg" in sn:
            direction = "chg"
        else:
            continue

        rows_in_step = pdf[pdf["_step_no"] == s["_step_no"]]
        if rows_in_step.empty:
            continue
        peak_idx = rows_in_step["_power"].idxmax()
        peak = rows_in_step.loc[peak_idx]
        rows.append({
            "make":      make,
            "batch":     batch,
            "cell_no":   cell_no,
            "direction": direction,
            "soc":       float(soc_of_cycle.get(int(s["cycle_no"]),
                                                  (cycles_seen.index(int(s["cycle_no"])) + 1) /
                                                  (len(cycles_seen) + 1)
                                                  if cycles_seen and int(s["cycle_no"]) in cycles_seen
                                                  else 0.5)),
            "p_peak_w":  float(peak["_power"]),
            "v_at_peak": float(peak["volt_v"]),
            "i_at_peak": float(abs(peak["current_a"])),
            "duration_s": float(s["duration_s"]),
        })

    if not rows:
        return pd.DataFrame(columns=[f.name for f in PEAK_POWER_SCHEMA.fields])
    out = pd.DataFrame(rows)
    # Multiple pulses at the same (direction, soc) → keep the highest P_peak
    out = (out.sort_values("p_peak_w", ascending=False)
              .drop_duplicates(["direction", "soc"]))
    return out[[f.name for f in PEAK_POWER_SCHEMA.fields]]


def extract_peak_power(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("PeakPower"))
            .groupBy("make", "batch", "cell_no")
            .applyInPandas(_extract_one_cell, schema=PEAK_POWER_SCHEMA))
