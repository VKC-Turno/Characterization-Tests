"""Constant-power extraction — energy & duration at each constant-power level.

Protocol: the cell is discharged/charged at a constant power (CP_DChg /
CP_Chg) until a voltage cutoff. Multiple power levels are typically swept
across cycles. Per (cycle, direction) we emit:

  - power_w    = mean |V·I| during the CP step (≈ commanded set-point)
  - energy_wh  = ∫|V·I| dt over the step
  - duration_s = time-to-cutoff
  - q_ah       = capacity moved during the step
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import CONSTANT_POWER_SCHEMA


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
    pdf["_p_abs"]   = (pdf["volt_v"] * pdf["current_a"].abs()).astype(float)

    # Compute time deltas in seconds (per row) so we can trapezoid-integrate.
    t = pd.to_datetime(pdf["absolute_time"]).values.astype("datetime64[ms]").astype(np.int64) / 1000.0
    pdf["_t"] = t

    rows = []
    for sn, g in pdf.groupby("_step_no"):
        step_name = str(g["step_name"].iloc[0])
        if "CP" not in step_name:
            continue
        direction = "chg" if step_name == "CP_Chg" else "dchg" if step_name == "CP_DChg" else None
        if direction is None:
            continue
        if len(g) < 5:
            continue

        ts = g["_t"].to_numpy(); ps = g["_p_abs"].to_numpy()
        dur = float(ts[-1] - ts[0])
        if dur <= 1.0:
            continue

        # Trapezoidal integration → energy in J → convert to Wh
        energy_j  = float(np.trapezoid(ps, ts))
        energy_wh = energy_j / 3600.0
        power_w   = float(ps.mean())
        q_ah      = float(g["capacity_ah"].abs().max()) if g["capacity_ah"].notna().any() else float("nan")

        rows.append({
            "make":       make,
            "batch":      batch,
            "cell_no":    cell_no,
            "direction":  direction,
            "power_w":    round(power_w, 1),    # bucket by 0.1W resolution
            "energy_wh":  energy_wh,
            "duration_s": dur,
            "q_ah":       q_ah,
        })

    if not rows:
        return pd.DataFrame(columns=[f.name for f in CONSTANT_POWER_SCHEMA.fields])
    out = pd.DataFrame(rows)
    # Multiple steps at the same (direction, power_w) → keep the longest
    out = (out.sort_values("duration_s", ascending=False)
              .drop_duplicates(["direction", "power_w"]))
    return out[[f.name for f in CONSTANT_POWER_SCHEMA.fields]]


def extract_constant_power(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("ConstantPower"))
            .groupBy("make", "batch", "cell_no")
            .applyInPandas(_extract_one_cell, schema=CONSTANT_POWER_SCHEMA))
