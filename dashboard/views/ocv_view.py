"""OCV(SoC) curves view — charge / discharge overlay."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_cells, list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("OCV(SoC) curves")
    st.caption("Slow CC charge / discharge, binned to a 0–1 SoC grid (11 anchors).")

    parts = list_partitions(data_root, "OCV")
    if parts.empty:
        st.warning(
            f"No OCV parquet found under `{data_root}/OCV/`. Run\n"
            "```\npython post_processing_script/scripts/run_local.py --job ocv\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No OCV data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, "OCV", make=make, batch=batch)
    if df.empty:
        st.info("No OCV rows for this selection.")
        return
    # Restrict to the sidebar's Cell unless it's "All"
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values(
        ["cell_label", "direction", "soc"])

    if df.empty:
        st.info("No rows.")
        return

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "OCV", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    st.subheader("V_oc vs SoC")
    fig = px.line(
        df, x="soc", y="v_oc",
        color="cell_label", line_dash="direction", markers=True,
        labels={"soc": "SoC", "v_oc": "V_OC (V)", "cell_label": "Cell"},
    )
    fig.update_layout(height=480, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
