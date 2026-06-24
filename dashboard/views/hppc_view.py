"""HPPC pulse-table view.

Three subviews:
  1. Per-cell R/C vs SoC — overlay every parameter (R0/R1/R2/C1/C2) for one cell
  2. Cohort compare      — overlay one parameter across all cells in a make+batch
  3. Pulse-detail table  — raw per-pulse table (V_pre/V_post/V_fast/V_end, tau)
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_cells, list_partitions, read_test)


# Per-parameter axis labels — keeps plots self-documenting
PARAM_LABELS = {
    "R0_mOhm":  "R₀ (mΩ)",
    "R1_mOhm":  "R₁ (mΩ)",
    "R2_mOhm":  "R₂ (mΩ)",
    "C1_F":     "C₁ (F)",
    "C2_F":     "C₂ (F)",
    "R_30s_mOhm": "R₃₀ₛ (mΩ)",
}


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("HPPC — pulse identification (VKC method)")
    st.caption("`pulse N → N × 10 % SoC`. Same logic as `_hppc_pulse_id.py`.")

    parts = list_partitions(data_root, "HPPC")
    if parts.empty:
        st.warning(
            "No HPPC parquet found under "
            f"`{data_root}/HPPC/`. Run\n```\npython post_processing_script/scripts/run_local.py --job hppc\n```\nfirst.")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No HPPC data for make `{make}` batch `{batch or 'All'}`.")
        return

    # HPPC's "detail panel" needs a single (batch, cell). If the user picked
    # All-batches / All-cells, fall back to the first available so the panel
    # still has something to draw — but flag what we picked.
    batch_pick = batch or sorted(sub["batch"].unique())[0]
    cells = list_cells(data_root, "HPPC", make=make, batch=batch_pick)
    cell_pick = cell if (cell and cell in cells) else (cells[0] if cells else None)
    if cell is None or batch is None:
        st.caption(f"Detail panel auto-selected `batch={batch_pick}`, `cell={cell_pick}` "
                   "(pick a specific batch/cell in the sidebar to lock it).")

    if cell_pick is None:
        st.info("No cells for this make/batch.")
        return

    full = read_test(data_root, "HPPC", make=make, batch=batch_pick)
    one  = full[full["cell_no"] == cell_pick].sort_values("pulse_idx")

    if one.empty:
        st.info("No pulses recorded for this cell.")
        return

    # ── 1) all params vs SoC for the chosen cell ──
    st.subheader(f"All ECM params vs SoC — `{make}_{cell_pick}` (batch {batch_pick})")
    fig = make_subplots(
        rows=2, cols=3,
        subplot_titles=[PARAM_LABELS[k] for k in (
            "R0_mOhm", "R1_mOhm", "R2_mOhm", "C1_F", "C2_F", "R_30s_mOhm")],
        shared_xaxes=True,
    )
    layout = [
        ("R0_mOhm",    1, 1), ("R1_mOhm",    1, 2), ("R2_mOhm",    1, 3),
        ("C1_F",       2, 1), ("C2_F",       2, 2), ("R_30s_mOhm", 2, 3),
    ]
    for col, r, c in layout:
        fig.add_trace(go.Scatter(x=one["soc_start"], y=one[col],
                                 mode="lines+markers", name=col,
                                 showlegend=False), row=r, col=c)
        fig.update_xaxes(title_text="SoC", row=r, col=c, range=[0, 1])
    fig.update_layout(height=560, margin=dict(t=40, b=10, l=10, r=10))
    st.plotly_chart(fig, use_container_width=True)

    # ── 2) cohort compare on one chosen param ──
    # Cohort-compare honours the SIDEBAR's batch filter, not batch_pick — so
    # when sidebar = "All", we overlay every batch's cells in one plot.
    cohort_df = read_test(data_root, "HPPC", make=make, batch=batch)
    if cell is not None:
        cohort_df = cohort_df[cohort_df["cell_no"] == cell]
    cohort_df = annotate_cell_label(cohort_df, batch_filter=batch).sort_values(
        ["cell_label", "pulse_idx"])

    st.subheader(f"Cohort compare — {make}, batch {batch or 'All'}, cell {cell or 'All'}")
    if batch is None and "batch" in cohort_df.columns:
        actual = sorted(cohort_df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "HPPC", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)
    pick = st.selectbox("Parameter", list(PARAM_LABELS.keys()), index=0,
                        format_func=lambda k: PARAM_LABELS[k])
    fig2 = px.line(
        cohort_df,
        x="soc_start", y=pick, color="cell_label",
        markers=True,
        labels={"soc_start": "SoC", pick: PARAM_LABELS[pick],
                "cell_label": "Cell"},
    )
    fig2.update_layout(height=420, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig2, use_container_width=True)

    # ── 3) raw pulse table ──
    st.subheader(f"Pulse details — `{make}_{cell_pick}`")
    cols_to_show = [
        "pulse_idx", "soc_start", "cycle_no", "step_no", "duration_s",
        "I_step", "V_pre", "V_post", "V_fast", "V_end",
        "tau1_s", "tau2_s",
        "R0_mOhm", "R1_mOhm", "R2_mOhm", "C1_F", "C2_F", "R_30s_mOhm",
    ]
    st.dataframe(
        one[cols_to_show].style.format({c: "{:.4g}" for c in cols_to_show
                                         if c not in ("pulse_idx", "cycle_no", "step_no")}),
        use_container_width=True,
        hide_index=True,
    )
