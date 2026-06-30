"""Rate-capability extraction — Q vs C-rate (lab-reference port).

Per direction, keep only steps whose capacity is a real full(ish) cycle
(``step_capacity_ah >= 0.5 * nominal``) and that aren't constant-power steps,
bucket by C-rate = round(|I_mean| / nominal, 2), and take the MEDIAN capacity
per bucket. The 0.5·nominal floor + CP exclusion drop the SoC-setup partial
discharges that otherwise create spurious low-C buckets.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import RATE_CAP_SCHEMA
from ._steps import build_steps

MIN_USEFUL_CAPACITY_FRAC = 0.50
RATE_ROUNDING = 2


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make = str(pdf["make"].iloc[0]); batch = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")
    nom = max_cap
    empty = pd.DataFrame(columns=[f.name for f in RATE_CAP_SCHEMA.fields])
    if not np.isfinite(nom) or nom <= 0:
        return empty

    step, _ = build_steps(pdf)
    sn = step["step_name"].astype(str)
    not_cp = ~sn.str.contains("CP", case=False, na=False)
    floor = step["step_capacity_ah"] >= MIN_USEFUL_CAPACITY_FRAC * nom

    rows = []
    masks = {
        "dchg": sn.str.contains("DChg", case=False, na=False) & not_cp & floor,
        "chg":  sn.str.contains("Chg",  case=False, na=False) & not_cp & floor,
    }
    for direction, mask in masks.items():
        sel = step[mask].copy()
        if sel.empty:
            continue
        sel["c_rate"] = (sel["current_mean_a"].abs() / nom).round(RATE_ROUNDING)
        by = (sel.groupby("c_rate", as_index=False)["step_capacity_ah"]
                 .median().sort_values("c_rate"))
        for _, r in by.iterrows():
            rows.append({"make": make, "batch": batch, "cell_no": cell_no,
                         "max_cap": max_cap, "direction": direction,
                         "c_rate": float(r["c_rate"]), "q_ah": float(r["step_capacity_ah"]),
                         "energy_wh": float("nan")})
    if not rows:
        return empty
    return pd.DataFrame(rows)[[f.name for f in RATE_CAP_SCHEMA.fields]]


def extract_rate_capability(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("RateCapability"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_extract_one_cell, schema=RATE_CAP_SCHEMA))
