"""GITT view — R_pulse / tau_diff / V_inf vs SoC."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("GITT — pulse + relaxation")
    st.caption("Long-pulse R, V_inf, and exponential τ_diff per SoC anchor.")

    parts = list_partitions(data_root, "GITT")
    if parts.empty:
        st.warning(
            f"No GITT parquet under `{data_root}/GITT/`. Run\n"
            "```\npython post_processing_script/scripts/run_local.py --job gitt\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No GITT data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, "GITT", make=make, batch=batch)
    if df.empty:
        st.info("No rows.")
        return
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values(["cell_label", "pulse_idx"])

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "GITT", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.subheader("R_pulse")
        fig = px.line(df, x="soc", y="r_pulse_mohm", color="cell_label", markers=True,
                       labels={"soc": "SoC", "r_pulse_mohm": "R_pulse (mΩ)", "cell_label": "Cell"})
        fig.update_layout(height=360, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("τ_diff")
        fig = px.line(df, x="soc", y="tau_diff_s", color="cell_label", markers=True,
                       labels={"soc": "SoC", "tau_diff_s": "τ_diff (s)", "cell_label": "Cell"})
        fig.update_layout(height=360, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)
    with c3:
        st.subheader("V_inf (OCV)")
        fig = px.line(df, x="soc", y="v_inf_v", color="cell_label", markers=True,
                       labels={"soc": "SoC", "v_inf_v": "V_inf (V)", "cell_label": "Cell"})
        fig.update_layout(height=360, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
