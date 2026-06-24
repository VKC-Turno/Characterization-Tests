"""Peak-power view — P_max envelope per SoC, per direction."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("Peak power")
    st.caption("Max V·I during short pulses, per SoC anchor.")

    parts = list_partitions(data_root, "PEAK_POWER")
    if parts.empty:
        st.warning(
            f"No PEAK_POWER parquet under `{data_root}/PEAK_POWER/`. Run\n"
            "```\npython post_processing_script/scripts/run_local.py --job peak_power\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No peak-power data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, "PEAK_POWER", make=make, batch=batch)
    if df.empty:
        st.info("No rows.")
        return
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values(
        ["cell_label", "direction", "soc"])

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, "PEAK_POWER", make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    st.subheader("P_peak vs SoC")
    fig = px.line(df, x="soc", y="p_peak_w", color="cell_label", line_dash="direction",
                   markers=True,
                   labels={"soc": "SoC", "p_peak_w": "P_peak (W)", "cell_label": "Cell"})
    fig.update_layout(height=440, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("V at P_peak vs SoC")
    fig2 = px.line(df, x="soc", y="v_at_peak", color="cell_label", line_dash="direction",
                    markers=True,
                    labels={"soc": "SoC", "v_at_peak": "V (V)", "cell_label": "Cell"})
    fig2.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
