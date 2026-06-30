"""Constant-power extraction (lab-reference port).

One row per ``CP_DChg`` step (step name contains "CP" and "DChg"):
    power_w    = mean(|V·I|) over the step's detail rows
    duration_s = step duration
    energy_wh  = power_w * duration_s / 3600
Arrays are sorted by ascending power.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import CONSTANT_POWER_SCHEMA
from ._steps import build_steps


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make = str(pdf["make"].iloc[0]); batch = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")
    empty = pd.DataFrame(columns=[f.name for f in CONSTANT_POWER_SCHEMA.fields])

    step, pdf = build_steps(pdf)
    sn = step["step_name"].astype(str)
    cp = step[sn.str.contains("CP", case=False, na=False)
              & sn.str.contains("DChg", case=False, na=False)]
    if cp.empty:
        return empty

    levels, durs, energies = [], [], []
    for _, s in cp.iterrows():
        d = pdf[pdf["_block"] == s["_block"]]
        if d.empty:
            continue
        p_w = float((d["volt_v"] * d["current_a"]).abs().mean())
        t_s = float(s["duration_s"])
        levels.append(p_w); durs.append(t_s); energies.append(p_w * t_s / 3600.0)
    if not levels:
        return empty

    order = np.argsort(levels)
    rows = [{"make": make, "batch": batch, "cell_no": cell_no, "max_cap": max_cap,
             "direction": "dchg", "power_w": float(levels[i]),
             "energy_wh": float(energies[i]), "duration_s": float(durs[i]),
             "q_ah": float("nan")} for i in order]
    return pd.DataFrame(rows)[[f.name for f in CONSTANT_POWER_SCHEMA.fields]]


def extract_constant_power(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("ConstantPower"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_extract_one_cell, schema=CONSTANT_POWER_SCHEMA))
