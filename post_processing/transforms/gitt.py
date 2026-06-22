"""GITT pulse extraction.

GITT protocol = a long CC pulse (typically 10–30 min at 0.1 C) followed by
a long rest (~60 min) to let the cell relax to equilibrium. Each pulse
sits at a known SoC, so the protocol sweeps SoC by stepping through pulses.

Per pulse we extract:
  - R_pulse  = (V_pre - V_during_pulse) / I_step           (mΩ)
  - V_inf    = OCV reached at the end of the relaxation     (V)
  - tau_diff = exponential time-constant of the relaxation  (s)

SoC is labelled ordinally — pulse N is the N-th equilibrium point. We do
NOT impose a 10 % grid (unlike VKC HPPC) because GITT cells typically run
20 or 40 anchors with non-uniform spacing.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import GITT_PULSE_SCHEMA


MIN_PULSE_DURATION_S = 60.0       # long pulse, not HPPC's 10s
MAX_PULSE_DURATION_S = 3600.0
MIN_REST_DURATION_S  = 600.0      # ≥10 min rest for relaxation
MIN_PULSE_CURRENT_A  = 0.5


def _exp_relax_tau(t: np.ndarray, v: np.ndarray) -> float:
    """Crude tau estimate: time to reach 63% of (v_end - v_start).

    Falls back to NaN if relaxation is flat or non-monotonic.
    """
    if len(v) < 5 or t[-1] - t[0] <= 0:
        return float("nan")
    v_target = v[0] + 0.632 * (v[-1] - v[0])
    if (v[-1] - v[0]) == 0:
        return float("nan")
    idx = np.argmin(np.abs(v - v_target))
    return float(t[idx] - t[0])


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

    agg = pdf.groupby("_step_no").agg(
        step_name=("step_name", "first"),
        t_start=  ("absolute_time", "first"),
        t_end=    ("absolute_time", "last"),
        v_first=  ("volt_v", "first"),
        v_last=   ("volt_v", "last"),
        i_mean=   ("current_a", lambda s: float(s.abs().mean())),
    ).reset_index()
    agg["duration_s"] = (pd.to_datetime(agg["t_end"]) -
                          pd.to_datetime(agg["t_start"])).dt.total_seconds()

    rows = []
    pulse_idx = 0
    n_pulses_total = sum(1 for i in range(1, len(agg))
                          if agg.iloc[i]["step_name"] in ("CC_DChg", "CC_Chg")
                             and MIN_PULSE_DURATION_S <= agg.iloc[i]["duration_s"] <= MAX_PULSE_DURATION_S
                             and "Rest" in str(agg.iloc[i-1]["step_name"])
                             and agg.iloc[i-1]["duration_s"] >= MIN_REST_DURATION_S
                             and agg.iloc[i]["i_mean"] >= MIN_PULSE_CURRENT_A)
    if n_pulses_total == 0:
        return pd.DataFrame(columns=[f.name for f in GITT_PULSE_SCHEMA.fields])

    for i in range(2, len(agg)):
        cur, prev = agg.iloc[i-1], agg.iloc[i-2]
        rest_after = agg.iloc[i] if i < len(agg) else None
        if not (cur["step_name"] in ("CC_DChg", "CC_Chg")
                and "Rest" in str(prev["step_name"])
                and prev["duration_s"] >= MIN_REST_DURATION_S
                and MIN_PULSE_DURATION_S <= cur["duration_s"] <= MAX_PULSE_DURATION_S
                and cur["i_mean"] >= MIN_PULSE_CURRENT_A
                and rest_after is not None
                and "Rest" in str(rest_after["step_name"])):
            continue
        pulse_idx += 1

        I_step = float(cur["i_mean"])
        # R is a magnitude; sign of (V_pre − V_first) flips between charge and
        # discharge pulses (V rises during charge, falls during discharge).
        r_pulse = abs(prev["v_last"] - cur["v_first"]) / I_step * 1000.0   # mΩ

        # Relaxation curve = the Rest immediately after this pulse
        sn_rest = int(rest_after["_step_no"])
        relax = pdf[pdf["_step_no"] == sn_rest].reset_index(drop=True)
        v_relax = relax["volt_v"].to_numpy(dtype=float)
        t_relax = (pd.to_datetime(relax["absolute_time"]).values
                    .astype("datetime64[ms]").astype(float)) / 1000.0
        if len(v_relax) >= 2:
            v_inf = float(v_relax[-1])
            tau   = _exp_relax_tau(t_relax - t_relax[0], v_relax)
        else:
            v_inf = float(cur["v_last"]); tau = float("nan")

        rows.append({
            "make":          make,
            "batch":         batch,
            "cell_no":       cell_no,
            "pulse_idx":     pulse_idx,
            "soc":           pulse_idx / n_pulses_total,
            "r_pulse_mohm":  float(r_pulse),
            "tau_diff_s":    float(tau),
            "v_inf_v":       v_inf,
        })

    if not rows:
        return pd.DataFrame(columns=[f.name for f in GITT_PULSE_SCHEMA.fields])
    return pd.DataFrame(rows)[[f.name for f in GITT_PULSE_SCHEMA.fields]]


def extract_gitt_pulses(raw_df: DataFrame) -> DataFrame:
    return (raw_df
            .where(F.col("test") == F.lit("GITT"))
            .groupBy("make", "batch", "cell_no")
            .applyInPandas(_extract_one_cell, schema=GITT_PULSE_SCHEMA))
