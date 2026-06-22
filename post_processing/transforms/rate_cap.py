"""Rate-capability extraction — Q vs C-rate, both directions.

Protocol: at each C-rate the cell undergoes a (CCCV charge → rest →
CC discharge → rest) cycle. We aggregate per (cycle, direction) step
and infer C-rate from the mean current divided by the nominal capacity.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import RATE_CAP_SCHEMA


def _derive_step_no(df: pd.DataFrame) -> pd.Series:
    if "cycler_step_no" in df.columns and df["cycler_step_no"].notna().any():
        return df["cycler_step_no"].astype("Int64")
    names = pd.Series(df["step_name"].astype(str).values)
    return (names != names.shift()).cumsum()


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make    = str(pdf["make"].iloc[0])
    batch   = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    nominal = float(pdf["max_cap"].dropna().iloc[0]) if pdf["max_cap"].notna().any() else float("nan")

    pdf = pdf.sort_values("absolute_time").reset_index(drop=True).copy()
    pdf["_step_no"] = _derive_step_no(pdf)

    # Per-step aggregate — capacity column is monotone-within-step
    # capacity_ah is signed (+ on chg, − on dchg). Take absolute peak so the
    # discharge half-cycle isn't dropped by the q_step_ah > 0 filter below.
    step_lvl = (pdf
                .groupby(["_step_no", "cycle_no", "step_name"])
                .agg(q_step_ah=("capacity_ah", lambda s: float(s.abs().max())),
                     i_mean=("current_a", lambda s: float(s.abs().mean())),
                     v_avg=("volt_v", "mean"),
                     t_start=("absolute_time", "first"),
                     t_end=("absolute_time", "last"))
                .reset_index())
    step_lvl["duration_s"] = (pd.to_datetime(step_lvl["t_end"]) -
                                pd.to_datetime(step_lvl["t_start"])).dt.total_seconds()

    rows = []
    for _, s in step_lvl.iterrows():
        sn = str(s["step_name"])
        if "Chg" in sn and "DChg" not in sn:
            direction = "chg"
        elif "DChg" in sn:
            direction = "dchg"
        else:
            continue
        if s["i_mean"] < 0.5 or s["q_step_ah"] <= 0:
            continue
        c_rate = float(s["i_mean"] / nominal) if nominal and nominal > 0 else float("nan")
        # Energy ≈ V_avg × Q
        energy = float(s["v_avg"] * s["q_step_ah"]) if pd.notna(s["v_avg"]) else float("nan")
        rows.append({
            "make":      make,
            "batch":     batch,
            "cell_no":   cell_no,
            "direction": direction,
            "c_rate":    float(round(c_rate, 2)) if not np.isnan(c_rate) else float("nan"),
            "q_ah":      float(s["q_step_ah"]),
            "energy_wh": energy,
        })

    if not rows:
        return pd.DataFrame(columns=[f.name for f in RATE_CAP_SCHEMA.fields])
    out = pd.DataFrame(rows)
    # Multiple steps may land on the same (direction, c_rate) — keep the max Q
    out = (out.groupby(["make","batch","cell_no","direction","c_rate"], as_index=False)
              .agg({"q_ah": "max", "energy_wh": "max"}))
    return out[[f.name for f in RATE_CAP_SCHEMA.fields]]


def extract_rate_capability(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("RateCapability"))
            .groupBy("make", "batch", "cell_no")
            .applyInPandas(_extract_one_cell, schema=RATE_CAP_SCHEMA))
