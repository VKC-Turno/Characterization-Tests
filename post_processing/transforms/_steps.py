"""Shared step-aggregation + SoC tracker (ported 1:1 from the lab reference).

The per-test extractors need a step-level view of the raw detail rows plus the
state-of-charge at each step's start. These mirror
``lab_processing/local_aggregator._detail_to_steps`` and ``soc_at_step_starts``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def build_steps(pdf: pd.DataFrame):
    """Collapse one cell's raw rows into contiguous (cycle, step_name) blocks.

    Returns (step_df, pdf_with_block) where pdf_with_block has a ``_block`` column
    so callers can slice the detail rows of any step via ``pdf[pdf._block == b]``.
    `current_mean_a` is SIGNED; `step_capacity_ah` is |Δcapacity|.
    """
    pdf = pdf.sort_values("absolute_time").reset_index(drop=True).copy()
    cyc = pdf["cycle_no"].astype("Int64").astype(str)
    key = cyc + "|" + pdf["step_name"].astype(str)
    pdf["_block"] = (key != key.shift()).cumsum()

    g = pdf.groupby("_block", sort=False)
    step = g.agg(
        step_name=("step_name", "first"),
        start_time=("absolute_time", "first"),
        end_time=("absolute_time", "last"),
        start_volt_v=("volt_v", "first"),
        end_volt_v=("volt_v", "last"),
        current_mean_a=("current_a", "mean"),          # SIGNED
        _cap_start=("capacity_ah", "first"),
        _cap_end=("capacity_ah", "last"),
    ).reset_index()
    step["duration_s"] = (pd.to_datetime(step["end_time"]) -
                          pd.to_datetime(step["start_time"])).dt.total_seconds()
    step["step_capacity_ah"] = (step["_cap_end"] - step["_cap_start"]).abs()
    return step, pdf


def soc_at_step_starts(step: pd.DataFrame, nom: float, initial_soc: float = 1.0) -> pd.Series:
    """SoC at the START of each step (recorded BEFORE the step's own ΔSoC).

    Indexed like ``step``. SoC starts at 1.0, clipped to [0,1]; sign from
    ``current_mean_a``, magnitude from ``step_capacity_ah`` (or |I|·Δt/3600).
    """
    if step.empty or not np.isfinite(nom) or nom <= 0:
        return pd.Series(index=step.index, dtype=float)
    ordered = step.sort_values("start_time")
    soc = float(initial_soc)
    socs = {}
    for idx, s in ordered.iterrows():
        socs[idx] = soc
        cap = s.get("step_capacity_ah", np.nan)
        i_mean = s.get("current_mean_a", 0.0)
        dur = s.get("duration_s", np.nan)
        if pd.isna(i_mean) or abs(i_mean) < 1e-6:
            continue
        if pd.isna(cap) or abs(cap) < 1e-6:
            if pd.isna(dur) or dur <= 0:
                continue
            cap = abs(i_mean) * dur / 3600.0
        direction = 1.0 if i_mean > 0 else -1.0
        soc = float(np.clip(soc + direction * abs(cap) / nom, 0.0, 1.0))
    return pd.Series(socs, name="soc_at_start").reindex(step.index)
