"""HPPC pulse identification — VKC-Turno convention.

This is a Spark-friendly port of
``characterization_results/_hppc_pulse_id.py``. The detection logic is
preserved 1:1 (same constants, same Rest→CC_DChg pattern, same V-fast 2nd-
derivative criterion); we just wrap it in a pandas UDF so each cell is
processed in isolation by a worker, and the per-pulse rows are collected
back into a Spark DataFrame.

SoC convention: **pulse N is at N × 10 % SoC** (VKC convention). We do NOT
re-derive SoC from cell-level cumulative discharge here — see the comment
in the original module for rationale.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import PULSE_SCHEMA


# ─────────────────────── pulse-detection constants ───────────────────────
# Match the original implementation byte-for-byte. If you change these,
# also bump the version tag in the output (TODO: add `algo_version` column).
FLAT_THRESHOLD_FRACTION = 0.008
FLAT_WINDOW             = 5
MIN_REST_DURATION_S     = 600.0
PULSE_DURATION_RANGE_S  = (5.0, 60.0)
MIN_PULSE_CURRENT_A     = 1.0


def _detect_v_fast(V: np.ndarray, t: np.ndarray) -> int:
    """Index where |d²V/dt²| flattens (VKC criterion).

    Mirrors `_detect_v_fast` in characterization_results/_hppc_pulse_id.py.
    Falls back to the midpoint if no flat window is found.
    """
    if len(V) < FLAT_WINDOW + 3:
        return len(V) // 2
    dt = np.diff(t)
    dt = np.where(dt == 0, np.nan, dt)
    dV = np.diff(V) / dt
    d2V = np.diff(dV) / dt[1:]
    abs_d2v = np.abs(d2V)
    if not np.isfinite(abs_d2v).any():
        return len(V) // 2
    peak = float(np.nanmax(abs_d2v))
    if peak == 0:
        return len(V) // 2
    threshold = peak * FLAT_THRESHOLD_FRACTION
    for i in range(len(abs_d2v) - FLAT_WINDOW):
        if np.all(abs_d2v[i:i + FLAT_WINDOW] < threshold):
            return i + 2
    return len(V) // 2


def _derive_step_no(df: pd.DataFrame) -> pd.Series:
    """Prefer native ``cycler_step_no``; fall back to step-name transitions."""
    if "cycler_step_no" in df.columns and df["cycler_step_no"].notna().any():
        return df["cycler_step_no"].astype("Int64")
    names = pd.Series(df["step_name"].astype(str).values)
    return (names != names.shift()).cumsum()


def _detect_pulses_one_cell(pdf: pd.DataFrame) -> pd.DataFrame:
    """Run VKC pulse detection on one (make, batch, cell_no) group.

    Returns a DataFrame matching :data:`post_processing.config.PULSE_SCHEMA`
    (minus the partition columns, which Spark re-attaches automatically when
    `applyInPandas` returns the result).
    """
    make    = str(pdf["make"].iloc[0])
    batch   = str(pdf["batch"].iloc[0])
    cell_no = str(pdf["cell_no"].iloc[0])
    max_cap = float(pdf["max_cap"].iloc[0]) if pdf["max_cap"].notna().any() else float("nan")

    pdf = pdf.sort_values("absolute_time").reset_index(drop=True).copy()
    pdf["_step_no"] = _derive_step_no(pdf)

    agg = pdf.groupby("_step_no").agg(
        step_name=("step_name", "first"),
        n_samples=("volt_v", "count"),
        t_start=  ("absolute_time", "first"),
        t_end=    ("absolute_time", "last"),
        v_first=  ("volt_v", "first"),
        v_last=   ("volt_v", "last"),
        i_mean=   ("current_a", lambda s: float(s.abs().mean())),
        i_signed= ("current_a", "mean"),
        cycle_no= ("cycle_no", "first"),
    ).reset_index()
    agg["duration_s"] = (pd.to_datetime(agg["t_end"]) -
                          pd.to_datetime(agg["t_start"])).dt.total_seconds()

    rows = []
    pulse_idx = 0
    for i in range(1, len(agg)):
        cur, prev = agg.iloc[i], agg.iloc[i - 1]
        if not (cur["step_name"] == "CC_DChg"
                and "Rest" in str(prev["step_name"])
                and prev["duration_s"] >= MIN_REST_DURATION_S
                and PULSE_DURATION_RANGE_S[0] <= cur["duration_s"] <= PULSE_DURATION_RANGE_S[1]
                and cur["i_mean"] >= MIN_PULSE_CURRENT_A):
            continue
        pulse_idx += 1
        soc_start = pulse_idx * 0.1    # VKC convention. Period.

        sn = int(cur["_step_no"])
        pulse_rows = pdf[pdf["_step_no"] == sn].reset_index(drop=True)
        V = pulse_rows["volt_v"].to_numpy(dtype=float)
        t = (pd.to_datetime(pulse_rows["absolute_time"]).values
              .astype("datetime64[ms]").astype(float)) / 1000.0
        t = t - t[0]
        i_fast = _detect_v_fast(V, t)
        V_fast = float(V[i_fast]) if 0 <= i_fast < len(V) else float(V[len(V)//2])
        t_post = float(t[0]); t_fast = float(t[i_fast]); t_end = float(t[-1])
        tau1 = max(t_fast - t_post, 1e-6)
        tau2 = max(t_end  - t_fast, 1e-6)

        # I_step is the SIGNED mean current (negative for discharge) — this is
        # what hppc_i_at_pulse_dchg reports. Resistances use the magnitude.
        I_step = float(cur["i_signed"])
        R0  = abs((prev["v_last"] - cur["v_first"]) / I_step) * 1000.0
        R1  = abs((cur["v_first"] - V_fast) / I_step) * 1000.0
        R2  = abs((V_fast - cur["v_last"])  / I_step) * 1000.0
        R30 = abs((prev["v_last"] - cur["v_last"]) / I_step) * 1000.0
        C1  = tau1 / (R1 * 1e-3) if R1 != 0 else float("nan")
        C2  = tau2 / (R2 * 1e-3) if R2 != 0 else float("nan")

        rows.append({
            "make":       make,
            "batch":      batch,
            "cell_no":    cell_no,
            "max_cap":    max_cap,
            "cycle_no":   int(cur["cycle_no"]) if pd.notna(cur["cycle_no"]) else None,
            "pulse_idx":  pulse_idx,
            "step_no":    sn,
            "duration_s": float(cur["duration_s"]),
            "I_step":     I_step,
            "soc_start":  float(soc_start),
            "V_pre":      float(prev["v_last"]),
            "V_post":     float(cur["v_first"]),
            "V_fast":     V_fast,
            "V_end":      float(cur["v_last"]),
            "tau1_s":     float(tau1),
            "tau2_s":     float(tau2),
            "R0_mOhm":    float(R0),
            "R1_mOhm":    float(R1),
            "R2_mOhm":    float(R2),
            "C1_F":       float(C1),
            "C2_F":       float(C2),
            "R_30s_mOhm": float(R30),
        })

    if not rows:
        # applyInPandas needs an empty DataFrame matching PULSE_SCHEMA when no
        # pulses were detected (sparse cells / corrupt files).
        return pd.DataFrame(columns=[f.name for f in PULSE_SCHEMA.fields])
    out = pd.DataFrame(rows)
    # Force the column order to match PULSE_SCHEMA — applyInPandas matches by position.
    return out[[f.name for f in PULSE_SCHEMA.fields]]


def detect_hppc_pulses(raw_df: DataFrame) -> DataFrame:
    """Spark transform: raw HPPC time-series → one row per detected pulse.

    The input is assumed already filtered to ``test == 'HPPC'``. We group by
    (make, batch, cell_no) and run :func:`_detect_pulses_one_cell` on each
    group via Arrow-backed pandas UDF.
    """
    return (raw_df
            .where(F.col("test") == F.lit("HPPC"))
            .groupBy("make", "batch", "cell_no", "max_cap")
            .applyInPandas(_detect_pulses_one_cell, schema=PULSE_SCHEMA))
