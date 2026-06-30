"""OCV(SoC) curve extraction.

For each cell, identify the slow CC charge/discharge half-cycle (typical
C/25), then bin the voltage trace by integrated capacity and report V at
N + 1 SoC anchors (default 0, 0.1, …, 1.0).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import OCV_CURVE_SCHEMA


SOC_GRID = np.linspace(0.0, 1.0, 11)


def _derive_step_no(df: pd.DataFrame) -> pd.Series:
    """Prefer native ``cycler_step_no`` when populated; else detect name transitions."""
    if "cycler_step_no" in df.columns and df["cycler_step_no"].notna().any():
        return df["cycler_step_no"].astype("Int64")
    names = pd.Series(df["step_name"].astype(str).values)
    return (names != names.shift()).cumsum()


def _extract_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    make, batch, cell_no = pdf["make"].iloc[0], pdf["batch"].iloc[0], pdf["cell_no"].iloc[0]
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")
    pdf = pdf.sort_values("absolute_time").reset_index(drop=True).copy()
    pdf["_step_no"] = _derive_step_no(pdf)

    # The cycler exporter uses multiple naming variants for the charge step
    # (`CC_Chg` for some protocols, `CCCV_Chg` for OCVSOC). Accept any of them.
    CHG_NAMES  = {"CC_Chg", "CCCV_Chg"}
    DCHG_NAMES = {"CC_DChg"}

    out = []
    for direction, names in (("chg", CHG_NAMES), ("dchg", DCHG_NAMES)):
        slow = pdf[pdf["step_name"].isin(names)]
        if slow.empty:
            continue
        # Pick the longest matching step (= the slow C/25-style pass)
        sizes = slow.groupby("_step_no").size()
        if sizes.empty:
            continue
        longest_step = sizes.idxmax()
        seg = slow[slow["_step_no"] == longest_step].copy()
        if len(seg) < 20:
            continue

        # Capacity sign convention varies (chg positive, dchg negative). Work
        # with the absolute swing so SoC normalisation is direction-agnostic.
        q = seg["capacity_ah"].to_numpy(dtype=float)
        v = seg["volt_v"].to_numpy(dtype=float)
        q_abs = np.abs(q - q[0])
        q_max = q_abs.max()
        if q_max <= 0:
            continue
        soc_progress = q_abs / q_max     # 0 → 1 across the step in temporal order
        # SoC IN THE CELL: charge fills the cell (0 → 1); discharge empties it (1 → 0)
        soc = soc_progress if direction == "chg" else 1.0 - soc_progress
        order = np.argsort(soc)
        soc_s = soc[order]; v_s = v[order]
        for s_target in SOC_GRID:
            v_at_s = float(np.interp(s_target, soc_s, v_s))
            out.append({"make": str(make), "batch": str(batch), "cell_no": str(cell_no),
                        "max_cap": max_cap, "direction": direction,
                        "soc": float(s_target), "v_oc": v_at_s})

    if not out:
        return pd.DataFrame(columns=[f.name for f in OCV_CURVE_SCHEMA.fields])
    return pd.DataFrame(out)[[f.name for f in OCV_CURVE_SCHEMA.fields]]


def extract_ocv_curves(raw_df: DataFrame) -> DataFrame:
    """Spark transform: raw OCV/SOC time-series → (cell × direction × SoC, V_OC)."""
    return (raw_df
            .where(F.upper(F.col("test")).isin("OCVSOC", "OCV_SOC", "OCV"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_extract_one_cell, schema=OCV_CURVE_SCHEMA))
