"""Read the parquet outputs from post_processing_script.

Designed to work against:
  - the local output dir produced by `scripts/run_local.py`
    (default: post_processing_script/output/)
  - the same layout on S3 once Glue lands it (set DATA_ROOT to s3://...).

Uses pyarrow.dataset so Hive partitioning (`make=X/batch=Y/`) and partition-
prune predicates work without DuckDB.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.dataset as pads
import streamlit as st


# Default — relative to repo root when running `streamlit run` from there.
DEFAULT_DATA_ROOT = Path("post_processing_script/output")


# Maps the dashboard's test-tab key → subfolder under DATA_ROOT and the
# expected partition columns. Keep names lowercase to match the writer.
TEST_LAYOUTS = {
    "HPPC":           {"path": "HPPC",            "partitions": ["make", "batch"]},
    "OCV":            {"path": "OCV",             "partitions": ["make", "batch"]},
    "DCIR":           {"path": "DCIR",            "partitions": ["make", "batch"]},
    "GITT":           {"path": "GITT",            "partitions": ["make", "batch"]},
    "RATE_CAP":       {"path": "RATE_CAP",        "partitions": ["make", "batch"]},
    "SELF_DISCHARGE": {"path": "SELF_DISCHARGE",  "partitions": ["make", "batch"]},
    "PEAK_POWER":     {"path": "PEAK_POWER",      "partitions": ["make", "batch"]},
    "CONSTANT_POWER": {"path": "CONSTANT_POWER",  "partitions": ["make", "batch"]},
    "CYCLES_LONG":    {"path": "CYCLES_LONG",     "partitions": ["make", "batch"]},
    "CYCLES_RPT":     {"path": "CYCLES_RPT",      "partitions": ["make", "batch"]},
}


# ─────────────────────────── core readers ───────────────────────────

@st.cache_data(show_spinner=False)
def list_partitions(data_root: str, test: str) -> pd.DataFrame:
    """Return one row per (make, batch) found on disk for a given test.

    Used to populate the sidebar filters without scanning row data.
    """
    layout = TEST_LAYOUTS[test]
    path = Path(data_root) / layout["path"]
    if not path.exists():
        return pd.DataFrame(columns=layout["partitions"])
    # Walk the filesystem — pyarrow.dataset will scan files, but we only need
    # the directory names so this is faster.
    parts: list[dict] = []
    for sub in path.rglob("*.parquet"):
        meta = {}
        for p in sub.parts:
            if "=" in p:
                k, v = p.split("=", 1)
                meta[k] = v
        if all(k in meta for k in layout["partitions"]):
            parts.append({k: meta[k] for k in layout["partitions"]})
    if not parts:
        return pd.DataFrame(columns=layout["partitions"])
    df = pd.DataFrame(parts).drop_duplicates().reset_index(drop=True)
    return df.sort_values(layout["partitions"]).reset_index(drop=True)


@st.cache_data(show_spinner="Loading parquet…")
def read_test(data_root: str,
              test: str,
              *,
              make: Optional[str] = None,
              batch: Optional[str] = None,
              cell_no: Optional[str] = None) -> pd.DataFrame:
    """Read one test's parquet dataset with partition-prune filters."""
    layout = TEST_LAYOUTS[test]
    path = str(Path(data_root) / layout["path"])
    if not Path(path).exists():
        return pd.DataFrame()
    ds = pads.dataset(path, format="parquet", partitioning="hive")

    # pyarrow infers partition column types from values — `batch=1` → int32,
    # `batch=2026Q1` → string. Match the partition schema to avoid
    # "Function 'equal' has no kernel matching" errors.
    part_types = {f.name: f.type for f in ds.partitioning.schema} \
        if ds.partitioning is not None else {}

    def _cast(name: str, value):
        t = part_types.get(name)
        if t is not None and str(t).startswith("int"):
            try:
                return int(value)
            except (TypeError, ValueError):
                return value
        return str(value)

    expr = None
    if make:
        e = pads.field("make") == _cast("make", make)
        expr = e if expr is None else expr & e
    if batch:
        e = pads.field("batch") == _cast("batch", batch)
        expr = e if expr is None else expr & e
    if cell_no:
        e = pads.field("cell_no") == str(cell_no)
        expr = e if expr is None else expr & e

    table = ds.to_table(filter=expr) if expr is not None else ds.to_table()
    return table.to_pandas()


@st.cache_data(show_spinner=False)
def list_cells(data_root: str, test: str,
               *, make: Optional[str] = None, batch: Optional[str] = None) -> list[str]:
    """Distinct cell_no values present for a test (optionally per make/batch)."""
    df = read_test(data_root, test, make=make, batch=batch)
    if df.empty or "cell_no" not in df.columns:
        return []
    return sorted(df["cell_no"].dropna().astype(str).unique().tolist())


# ─────────────────────────── summary helpers ───────────────────────────

@st.cache_data(show_spinner=False)
def list_makes(data_root: str) -> list[str]:
    """Distinct `make=<X>` partition values across every test folder.

    Used by the global Make selector in the sidebar — picks one make and
    every view inherits it.
    """
    makes: set[str] = set()
    for test in TEST_LAYOUTS:
        df = list_partitions(data_root, test)
        if not df.empty and "make" in df.columns:
            makes.update(df["make"].dropna().astype(str).tolist())
    return sorted(makes)


@st.cache_data(show_spinner=False)
def list_batches(data_root: str, make: str) -> list[str]:
    """Distinct `batch=<X>` partition values for one make, across all tests.

    A batch may exist for some tests but not others (e.g. HPPC has REPT
    batch 2; OCV doesn't). We return the union — the sidebar picker is a
    "best-effort" filter that views ignore when the test has no rows for it.
    """
    batches: set[str] = set()
    for test in TEST_LAYOUTS:
        df = list_partitions(data_root, test)
        if df.empty or "make" not in df.columns:
            continue
        sub = df[df["make"].astype(str) == str(make)]
        batches.update(sub["batch"].dropna().astype(str).tolist())
    # Numeric-aware sort so '10' doesn't come before '2'
    return sorted(batches, key=lambda b: (int(b) if b.isdigit() else 10**9, b))


@st.cache_data(show_spinner=False)
def list_cells_any(data_root: str, make: str,
                   batch: Optional[str] = None) -> list[str]:
    """Distinct cell_no values for (make, batch) across every test folder.

    `batch=None` means union across all batches for that make.
    """
    cells: set[str] = set()
    for test in TEST_LAYOUTS:
        df = read_test(data_root, test, make=make, batch=batch)
        if df.empty or "cell_no" not in df.columns:
            continue
        cells.update(df["cell_no"].dropna().astype(str).tolist())
    return sorted(cells)


def explain_batch_coverage(
    data_root: str, test: str, make: str, sidebar_batch: Optional[str],
    actual_batches_in_df: list[str],
) -> tuple[str, str]:
    """Return (severity, message) explaining what 'Batch=All' actually resolved to.

    Three states:
      - 'ok'   : 2+ batches matched — multi-batch overlay is meaningful
      - 'info' : 1 batch matched out of N batches the make has in OTHER tests
                 (so user understands the data is missing, not the filter)
      - 'warn' : 0 batches (data_loader returned empty for this test+make)
    """
    if sidebar_batch is not None:
        return "ok", f"Showing batch `{sidebar_batch}` only (sidebar filter)."

    have_for_test  = sorted(set(actual_batches_in_df))
    have_for_make  = list_batches(data_root, make)   # union across every test

    if len(have_for_test) >= 2:
        return "ok", (
            f"Showing **{len(have_for_test)} batches** of {test} for `{make}`: "
            f"{', '.join(have_for_test)}.")
    if len(have_for_test) == 1:
        missing = [b for b in have_for_make if b not in have_for_test]
        extra = (f" Other tests have batches {missing} for `{make}` on disk, "
                  f"but {test} only has batch `{have_for_test[0]}`. "
                  f"Pull/process more raw {test} files to populate the rest."
                  if missing else "")
        return "info", (
            f"Only batch `{have_for_test[0]}` of {test} exists on disk for "
            f"`{make}` — that's why the chart looks the same as picking it directly.{extra}")
    return "warn", f"No {test} data for `{make}` on disk."


def annotate_cell_label(df: pd.DataFrame, batch_filter: Optional[str]) -> pd.DataFrame:
    """Add a `cell_label` column views can pass to color= in Plotly.

    - When the sidebar locks a single batch, label = ``<cell_no>`` (terse).
    - When batch is "All" → batch_filter is None → prefix with the batch so
      traces from different batches show up as distinct legend entries
      (e.g. ``b1_0001``, ``b2_0001``).
    """
    if df.empty or "cell_no" not in df.columns:
        return df
    df = df.copy()
    if batch_filter is None and "batch" in df.columns:
        df["cell_label"] = ("b" + df["batch"].astype(str)
                            + "_" + df["cell_no"].astype(str))
    else:
        df["cell_label"] = df["cell_no"].astype(str)
    return df


def summary_counts(data_root: str) -> pd.DataFrame:
    """Per-test summary table for the landing page."""
    rows = []
    for test in TEST_LAYOUTS:
        df = read_test(data_root, test)
        if df.empty:
            rows.append({"test": test, "rows": 0, "cells": 0, "makes": 0, "batches": 0})
            continue
        rows.append({
            "test":    test,
            "rows":    len(df),
            "cells":   df["cell_no"].nunique() if "cell_no" in df.columns else 0,
            "makes":   df["make"].nunique()    if "make"    in df.columns else 0,
            "batches": df["batch"].nunique()   if "batch"   in df.columns else 0,
        })
    return pd.DataFrame(rows)
