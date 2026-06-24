"""Streamlit dashboard for the PySpark post-processing outputs.

Run from the repo root:

    .venv/bin/python -m streamlit run post_processing_dashboard/app.py

Or point at a custom output dir:

    POST_PROCESSING_OUTPUT=/tmp/eve_only \\
      .venv/bin/python -m streamlit run post_processing_dashboard/app.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

# Make sibling modules importable when run via `streamlit run`
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from data_loader import (  # noqa: E402
    DEFAULT_DATA_ROOT, list_batches, list_cells_any, list_makes, summary_counts,
)
from views import (  # noqa: E402
    hppc_view, ocv_view, dcir_view, cycles_view,
    gitt_view, rate_cap_view, self_discharge_view,
    peak_power_view, constant_power_view,
)


st.set_page_config(
    page_title="Battery post-processing dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─────────────────────────── sidebar ───────────────────────────
with st.sidebar:
    st.title("Post-processing")
    st.caption("Reads parquet written by `post_processing_script`")

    # We need a data root to enumerate makes; for the Make widget we read it
    # from env (or the default) BEFORE rendering the editable text box. If the
    # user edits the data root below, Streamlit re-runs the whole script and
    # the Make widget rebuilds against the new path on the next pass.
    data_root_default = os.environ.get(
        "POST_PROCESSING_OUTPUT", str(DEFAULT_DATA_ROOT))
    data_root_for_makes = st.session_state.get("data_root", data_root_default)

    makes = list_makes(data_root_for_makes) if Path(data_root_for_makes).exists() else []
    if not makes:
        st.warning("No make=<X>/ partitions found yet.")
        make = batch = cell = None
    else:
        make = st.selectbox("Make", makes,
                            help="Global filter — every view uses this make.")

        # Batch — sentinel "All" = no filter
        batch_opts = ["All"] + list_batches(data_root_for_makes, make)
        batch_pick = st.selectbox("Batch", batch_opts,
                                  help='"All" = read across every batch for the make.')
        batch = None if batch_pick == "All" else batch_pick

        # Cell — sentinel "All" = no filter
        cell_opts = ["All"] + list_cells_any(data_root_for_makes, make, batch)
        cell_pick = st.selectbox("Cell", cell_opts,
                                 help='"All" = every cell in the (make, batch).')
        cell = None if cell_pick == "All" else cell_pick

    data_root = st.text_input(
        "Data root",
        value=data_root_default,
        key="data_root",
        help="Folder containing HPPC/, OCV/, DCIR/, CYCLES_LONG/ etc.",
    )

    if not Path(data_root).exists():
        st.error(f"`{data_root}` does not exist.")
        st.stop()

    page = st.radio(
        "View",
        ("Summary",
         "HPPC", "OCV", "DCIR", "GITT",
         "Rate cap.", "Self-discharge",
         "Peak power", "Constant power",
         "Per-cycle"),
        index=0,
    )

    st.divider()
    st.caption(
        "Re-run the jobs from the repo root:\n\n"
        "```\npython post_processing_script/scripts/run_local.py --job all\n```"
    )


# ─────────────────────────── main pane ───────────────────────────
if page == "Summary":
    st.header("What's loaded")
    st.caption(f"Data root: `{data_root}`")
    counts = summary_counts(data_root)

    c = st.columns(len(counts))
    for i, row in counts.iterrows():
        with c[i]:
            st.metric(
                label=row["test"],
                value=f"{row['rows']:,} rows",
                delta=f"{row['cells']} cells · {row['makes']} makes · {row['batches']} batches",
                delta_color="off",
            )

    st.divider()
    st.dataframe(counts, hide_index=True, use_container_width=True)

    with st.expander("How this hangs together", expanded=False):
        st.markdown(
            """
            1. **`post_processing_script/`** runs PySpark transforms on raw cycler
               CSVs under `Data/` and writes Hive-partitioned parquet to
               `post_processing_script/output/<TEST>/make=<X>/batch=<Y>/`.
            2. **`post_processing_dashboard/`** (this app) reads those parquet
               files via `pyarrow.dataset` with partition-prune filters.
            3. The same parquet layout works on S3 — point `Data root` at an
               `s3://…` URI once you deploy the Glue job.
            """
        )

elif page == "HPPC":
    hppc_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "OCV":
    ocv_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "DCIR":
    dcir_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "GITT":
    gitt_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "Rate cap.":
    rate_cap_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "Self-discharge":
    self_discharge_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "Peak power":
    peak_power_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "Constant power":
    constant_power_view.render(data_root, make=make, batch=batch, cell=cell)
elif page == "Per-cycle":
    cycles_view.render(data_root, make=make, batch=batch, cell=cell)
