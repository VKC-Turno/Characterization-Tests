"""Cross-test (post-join) column calculations.

Some result columns depend on the OUTPUT of more than one test transform, so
they can only be computed AFTER the per-test wide row has been assembled (and,
for incremental runs, after the dependent tests have been co-loaded — see
DEPENDENCY_GROUPS in the runner). Keep that logic here so every runner
(incremental, Glue, batch) computes these columns identically.

Currently handled:
  * ``self_disch_dsoc_per_day`` — invert the discharge OCV curve (V→SoC) at the
    self-discharge start/end voltages; fall back to the LFP-plateau approximation
    only when the cell has no OCV curve.

Add future cross-test columns here and call :func:`apply` once on the assembled
``pandas`` frame.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

LFP_PLATEAU_DV_PER_SOC = 0.05   # V per unit SoC — rough LFP plateau slope


def _as_array(v) -> np.ndarray:
    if isinstance(v, (list, tuple, np.ndarray)):
        return np.asarray(v, dtype=float)
    return np.array([])


def dsoc_per_day(row) -> "float | None":
    """SoC drift per day during the self-discharge rest.

    Returns None when the cell has no self-discharge measurement (so the caller
    leaves the column untouched / null-safe). Uses OCV inversion when the OCV
    curve is present, else the LFP-plateau approximation.
    """
    v_s = row.get("self_disch_v_start_v")
    v_e = row.get("self_disch_v_end_v")
    dur_h = row.get("self_disch_rest_duration_h")
    if (v_s is None or v_e is None or dur_h is None
            or pd.isna(v_s) or pd.isna(v_e) or pd.isna(dur_h) or float(dur_h) <= 0):
        return None
    days = float(dur_h) / 24.0

    v_oc = _as_array(row.get("v_oc_curve"))
    soc = _as_array(row.get("ocv_soc_grid"))
    if len(v_oc) > 1 and len(v_oc) == len(soc):
        order = np.argsort(v_oc)                      # interp needs ascending V
        soc_start = float(np.interp(float(v_s), v_oc[order], soc[order]))
        soc_end = float(np.interp(float(v_e), v_oc[order], soc[order]))
        return (soc_start - soc_end) / days           # positive = self-discharge

    # fallback: no OCV curve for this cell → plateau approximation
    return -(float(v_e) - float(v_s)) / LFP_PLATEAU_DV_PER_SOC / days


def apply(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute all cross-test columns on the assembled per-cell wide frame."""
    rows = []
    for _, row in pdf.iterrows():
        row = row.copy()
        d = dsoc_per_day(row)
        if d is not None:
            row["self_disch_dsoc_per_day"] = d
        rows.append(row)
    return pd.DataFrame(rows)
