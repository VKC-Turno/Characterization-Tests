"""Self-discharge extraction.

Protocol: charge to a known SoC → long Rest (often 7–30 days) → discharge
to measure recovered capacity. We extract:

  - rest_duration_s   = total rest length
  - v_start / v_end   = OCV at the start / end of the longest rest
  - dv_dt_mV_per_h    = drift rate (negative for ageing/leakage)
  - q_recovered_ah    = first CC_DChg capacity after the rest
  - retention_pct     = q_recovered / q_charged_before_rest
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import SELF_DISCHARGE_SCHEMA

LFP_PLATEAU_DV_PER_SOC = 0.05   # V per unit SoC — rough LFP plateau slope
DEFAULT_AMBIENT_C      = 25.0   # no temperature column in the raw export


def _derive_step_no(df: pd.DataFrame) -> pd.Series:
    if "cycler_step_no" in df.columns and df["cycler_step_no"].notna().any():
        return df["cycler_step_no"].astype("Int64")
    names = pd.Series(df["step_name"].astype(str).values)
    return (names != names.shift()).cumsum()


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make    = str(pdf["make"].iloc[0])
    batch   = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")

    pdf = pdf.sort_values("absolute_time").reset_index(drop=True).copy()
    pdf["_step_no"] = _derive_step_no(pdf)

    # `capacity_ah` is monotone-within-step but signed: positive on charge,
    # negative on discharge. Use the absolute peak so q_max is the size of
    # the swing, irrespective of direction.
    agg = pdf.groupby("_step_no").agg(
        step_name=("step_name", "first"),
        t_start=  ("absolute_time", "first"),
        t_end=    ("absolute_time", "last"),
        v_first=  ("volt_v", "first"),
        v_last=   ("volt_v", "last"),
        q_max=    ("capacity_ah", lambda s: float(s.abs().max())),
    ).reset_index()
    agg["duration_s"] = (pd.to_datetime(agg["t_end"]) -
                          pd.to_datetime(agg["t_start"])).dt.total_seconds()

    rests = agg[(agg["step_name"] == "Rest") & (agg["duration_s"] >= 3600)]
    if rests.empty:
        return pd.DataFrame(columns=[f.name for f in SELF_DISCHARGE_SCHEMA.fields])

    # Take the longest rest as the "self-discharge" interval
    rest = rests.sort_values("duration_s", ascending=False).iloc[0]
    rest_idx = int(agg.index[agg["_step_no"] == rest["_step_no"]][0])

    v_start, v_end = float(rest["v_first"]), float(rest["v_last"])
    dur_s = float(rest["duration_s"])
    dv_dt = (v_end - v_start) / (dur_s / 3600.0) * 1000.0   # mV/h

    # Charge step BEFORE the rest, discharge step AFTER (if present)
    q_before = float("nan"); q_after = float("nan")
    if rest_idx > 0:
        prev = agg.iloc[rest_idx - 1]
        if "Chg" in str(prev["step_name"]):
            q_before = float(prev["q_max"]) if pd.notna(prev["q_max"]) else float("nan")
    if rest_idx < len(agg) - 1:
        nxt = agg.iloc[rest_idx + 1]
        if "DChg" in str(nxt["step_name"]):
            q_after = float(nxt["q_max"]) if pd.notna(nxt["q_max"]) else float("nan")

    retention = (q_after / q_before * 100.0) if (q_before and q_before > 0 and pd.notna(q_after)) else float("nan")

    # dsoc_per_day is a CROSS-TEST column (needs the OCV curve to invert V→SoC),
    # so it's computed once in post_processing.post_join after the wide row is
    # assembled — NOT here. We only emit the inputs (v_start/v_end/rest).
    # Ambient is a constant 25 °C (no temperature column in the raw export).
    return pd.DataFrame([{
        "make":           make,
        "batch":          batch,
        "cell_no":        cell_no,
        "max_cap":        max_cap,
        "rest_duration_s": dur_s,
        "v_start":        v_start,
        "v_end":          v_end,
        "dv_dt_mV_per_h": float(dv_dt),
        "q_recovered_ah": q_after,
        "retention_pct":  retention,
        "dsoc_per_day":   float("nan"),
        "ambient_c":      DEFAULT_AMBIENT_C,
    }])[[f.name for f in SELF_DISCHARGE_SCHEMA.fields]]


def extract_self_discharge(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("SelfDischarge"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_extract_one_cell, schema=SELF_DISCHARGE_SCHEMA))
