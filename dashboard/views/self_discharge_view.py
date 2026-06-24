"""Self-discharge view — drift rate, retention, by-cell table."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("Self-discharge")
    st.caption("ΔV/Δt during long open-circuit rest; capacity retention after.")

    parts = list_partitions(data_root, "SELF_DISCHARGE")
    if parts.empty:
        st.warning(
            f"No SELF_DISCHARGE parquet under `{data_root}/SELF_DISCHARGE/`. Run\n"
            "```\npython post_processing_script/scripts/run_local.py --job self_discharge\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No self-discharge data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, "SELF_DISCHARGE", make=make, batch=batch)
    if df.empty:
        st.info("No rows.")
        return
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values("cell_label")

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "SELF_DISCHARGE", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Drift rate (mV/h)")
        fig = px.bar(df, x="cell_label", y="dv_dt_mV_per_h",
                      labels={"cell_label": "Cell", "dv_dt_mV_per_h": "ΔV/Δt (mV/h)"})
        fig.update_layout(height=360, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Capacity retention")
        fig = px.bar(df, x="cell_label", y="retention_pct",
                      labels={"cell_label": "Cell", "retention_pct": "Retention (%)"})
        fig.update_layout(height=360, margin=dict(t=10, b=10, l=10, r=10),
                          yaxis_range=[0, 110])
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
