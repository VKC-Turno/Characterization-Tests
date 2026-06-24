"""Per-cycle aggregates view — SoH trajectory, capacity, coulombic efficiency."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from data_loader import (annotate_cell_label, explain_batch_coverage,
                          list_cells, list_partitions, read_test)


def render(data_root: str, make: str | None = None,
           batch: str | None = None, cell: str | None = None) -> None:
    st.header("Per-cycle aggregates")
    st.caption("dchg cap → SoH; chg cap; coulombic efficiency vs cycle.")

    test = st.radio("Source", ("CYCLES_LONG", "CYCLES_RPT"), horizontal=True,
                    help="Longterm cycling vs RPT cycles.")
    parts = list_partitions(data_root, test)
    if parts.empty:
        st.warning(
            f"No parquet under `{data_root}/{test}/`. Run\n"
            f"```\npython post_processing_script/scripts/run_local.py --job {test.lower()}\n```")
        return

    sub = parts[parts["make"] == make] if make else parts
    if batch is not None:
        sub = sub[sub["batch"].astype(str) == str(batch)]
    if sub.empty:
        st.info(f"No {test} data for make `{make}` batch `{batch or 'All'}`.")
        return

    df = read_test(data_root, test, make=make, batch=batch)
    if df.empty:
        st.info("No rows.")
        return
    if cell is not None:
        df = df[df["cell_no"] == cell]
    df = annotate_cell_label(df, batch_filter=batch).sort_values(["cell_label", "cycle_no"])

    if df.empty or df["dchg_cap_ah"].isna().all():
        st.info("No discharge data.")
        return

    if batch is None and "batch" in df.columns:
        actual = sorted(df["batch"].astype(str).unique())
        severity, msg = explain_batch_coverage(data_root, test, make, batch, actual)
        (st.info if severity == "info" else st.caption)(msg)

    # SoH = dchg_cap / max(dchg_cap per cell)
    df = df.copy()
    df["soh"] = df.groupby("cell_label")["dchg_cap_ah"].transform(
        lambda s: s / s.max() if s.max() and s.max() > 0 else s)

    st.subheader("SoH trajectory")
    fig = px.line(
        df, x="cycle_no", y="soh", color="cell_label", markers=False,
        labels={"cycle_no": "Cycle", "soh": "SoH", "cell_label": "Cell"},
    )
    fig.add_hline(y=0.80, line_dash="dash", annotation_text="EOL = 0.80")
    fig.update_layout(height=420, margin=dict(t=10, b=10, l=10, r=10),
                      yaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Discharge capacity")
        fig2 = px.line(df, x="cycle_no", y="dchg_cap_ah", color="cell_label",
                       labels={"cycle_no": "Cycle", "dchg_cap_ah": "Q_dchg (Ah)", "cell_label": "Cell"})
        fig2.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig2, use_container_width=True)
    with c2:
        st.subheader("Coulombic efficiency")
        fig3 = px.line(df, x="cycle_no", y="coulombic_eff", color="cell_label",
                       labels={"cycle_no": "Cycle", "coulombic_eff": "CE", "cell_label": "Cell"})
        fig3.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10),
                           yaxis_range=[0.9, 1.05])
        st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Raw")
    st.dataframe(df, use_container_width=True, hide_index=True)
