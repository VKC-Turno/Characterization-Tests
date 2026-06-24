"""Constant-power view — energy/duration delivered at each P level."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("Constant power")
    st.caption("Energy and time-to-cutoff at each constant-power set-point.")

    parts = list_partitions(data_root, "CONSTANT_POWER")
    if parts.empty:
        st.warning(
            f"No CONSTANT_POWER parquet under `{data_root}/CONSTANT_POWER/`. Run\n"
            "```\npython post_processing_script/scripts/run_local.py --job constant_power\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No constant-power data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, "CONSTANT_POWER", make=make, batch=batch)
    if df.empty:
        st.info("No rows.")
        return
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values(
        ["cell_label", "direction", "power_w"])

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "CONSTANT_POWER", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Energy vs power")
        fig = px.line(df, x="power_w", y="energy_wh", color="cell_label", line_dash="direction",
                       markers=True,
                       labels={"power_w": "P (W)", "energy_wh": "Energy (Wh)", "cell_label": "Cell"})
        fig.update_layout(height=380, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Time-to-cutoff vs power")
        fig = px.line(df, x="power_w", y="duration_s", color="cell_label", line_dash="direction",
                       markers=True,
                       labels={"power_w": "P (W)", "duration_s": "Time (s)", "cell_label": "Cell"})
        fig.update_layout(height=380, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
