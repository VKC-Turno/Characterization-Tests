"""DCIR R0 anchors view."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("DCIR — R₀ anchors")
    st.caption("Short pulse R₀ at protocol SoC anchors (default 0.2 / 0.5 / 0.8).")

    parts = list_partitions(data_root, "DCIR")
    if parts.empty:
        st.warning(
            f"No DCIR parquet found under `{data_root}/DCIR/`. Run\n"
            "```\npython post_processing_script/scripts/run_local.py --job dcir\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No DCIR data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, "DCIR", make=make, batch=batch)
    if df.empty:
        st.info("No rows.")
        return
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values(["cell_label", "soc"])

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "DCIR", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    st.subheader("R₀ vs SoC, per cell")
    fig = px.line(
        df, x="soc", y="r0_mohm", color="cell_label", markers=True,
        labels={"soc": "SoC", "r0_mohm": "R₀ (mΩ)", "cell_label": "Cell"},
    )
    fig.update_layout(height=420, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
