"""Rate-capability view — Q vs C-rate, per direction."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("Rate capability")
    st.caption("Discharge / charge capacity as a function of C-rate.")

    parts = list_partitions(data_root, "RATE_CAP")
    if parts.empty:
        st.warning(
            f"No RATE_CAP parquet under `{data_root}/RATE_CAP/`. Run\n"
            "```\npython post_processing_script/scripts/run_local.py --job rate_cap\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No rate-cap data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, "RATE_CAP", make=make, batch=batch)
    if df.empty:
        st.info("No rows.")
        return
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values(
        ["cell_label", "direction", "c_rate"])

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "RATE_CAP", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    st.subheader("Q vs C-rate")
    fig = px.line(df, x="c_rate", y="q_ah", color="cell_label", line_dash="direction",
                   markers=True,
                   labels={"c_rate": "C-rate", "q_ah": "Q (Ah)", "cell_label": "Cell"})
    fig.update_layout(height=440, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Energy vs C-rate")
    fig2 = px.line(df, x="c_rate", y="energy_wh", color="cell_label", line_dash="direction",
                    markers=True,
                    labels={"c_rate": "C-rate", "energy_wh": "Energy (Wh)", "cell_label": "Cell"})
    fig2.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
