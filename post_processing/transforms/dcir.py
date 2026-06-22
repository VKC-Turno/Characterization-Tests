"""DCIR R0 extraction at coarse SoC anchors.

DCIR test protocol: at a handful of pre-set SoCs (typically 0.2/0.5/0.8),
the cell sees a short ~10 s pulse. R0 = ΔV / ΔI taken in the first sample
after current step. We re-use the (Rest → CC_DChg) pattern from HPPC but
WITHOUT the VKC SoC labelling — DCIR cells start at the protocol-prescribed
SoC, so we read SoC from cycler metadata or label cycles in order.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import DCIR_ANCHOR_SCHEMA


# DCIR protocol anchors (3-point default). If your protocol differs, override
# by editing this list or passing a `soc_anchors` arg into _extract_one_cell.
DCIR_SOC_ANCHORS = (0.2, 0.5, 0.8)


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make, batch, cell_no = pdf["make"].iloc[0], pdf["batch"].iloc[0], pdf["cell_no"].iloc[0]
    pdf = pdf.sort_values("absolute_time").reset_index(drop=True).copy()

    if "cycler_step_no" in pdf.columns and pdf["cycler_step_no"].notna().any():
        step_col = "cycler_step_no"
    else:
        names = pdf["step_name"].astype(str)
        pdf["_step_no"] = (names != names.shift()).cumsum()
        step_col = "_step_no"

    agg = pdf.groupby(step_col).agg(
        step_name=("step_name", "first"),
        v_first=  ("volt_v", "first"),
        v_last=   ("volt_v", "last"),
        i_mean=   ("current_a", lambda s: float(s.abs().mean())),
        t_start=  ("absolute_time", "first"),
        t_end=    ("absolute_time", "last"),
    ).reset_index()
    agg["duration_s"] = (pd.to_datetime(agg["t_end"]) -
                          pd.to_datetime(agg["t_start"])).dt.total_seconds()

    # Some DCIR protocols sweep multiple short pulses at 0.2 / 0.5 / 0.8 SoC;
    # others ship a single long discharge (Voltaris / Turno's recent exports).
    # Accept both: emit one R0 row per (Rest → CC_DChg) transition with i ≥ 1 A
    # and label the SoC ordinally (1st pulse → DCIR_SOC_ANCHORS[0] etc.).
    pulses = []
    for i in range(1, len(agg)):
        cur, prev = agg.iloc[i], agg.iloc[i - 1]
        if (cur["step_name"] == "CC_DChg"
                and "Rest" in str(prev["step_name"])
                and cur["i_mean"] >= 1.0):
            r0 = (prev["v_last"] - cur["v_first"]) / cur["i_mean"] * 1000.0
            pulses.append({"r0_mohm": float(r0)})

    if not pulses:
        return pd.DataFrame(columns=[f.name for f in DCIR_ANCHOR_SCHEMA.fields])

    # Pulse N → DCIR_SOC_ANCHORS[N] when in range; else uniform fallback.
    n = len(pulses)
    anchors = (DCIR_SOC_ANCHORS if n <= len(DCIR_SOC_ANCHORS)
               else tuple((i + 1) / (n + 1) for i in range(n)))
    out = [{"make": str(make), "batch": str(batch), "cell_no": str(cell_no),
            "soc": float(anchors[idx]), "r0_mohm": p["r0_mohm"]}
           for idx, p in enumerate(pulses)]
    return pd.DataFrame(out)[[f.name for f in DCIR_ANCHOR_SCHEMA.fields]]


def extract_dcir_anchors(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("DCIR"))
            .groupBy("make", "batch", "cell_no")
            .applyInPandas(_extract_one_cell, schema=DCIR_ANCHOR_SCHEMA))
