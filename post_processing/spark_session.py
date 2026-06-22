"""Spark session builder.

The same builder is used locally and on AWS Glue. When running on Glue, the
SparkSession already exists — `build_spark()` detects that and returns the
existing context.

Configs set here are the ones we actually care about for this workload:
  - Arrow on for pandas UDFs (HPPC pulse detection is a pandas-UDF transform)
  - Dynamic partition overwrite so re-runs of one (make, batch) don't blow
    away sibling partitions
  - Snappy parquet (Glue default; cheap CPU)
"""
from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession


# Java 17+ closed off internal modules that Arrow's memory layer pokes at.
# These --add-opens flags re-open just what Arrow needs. Glue runs Java 17,
# Glue 5 runs Java 21 — same flags work for both.
_JAVA_OPENS = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)


def _on_glue() -> bool:
    """True when running inside an AWS Glue runtime."""
    return any(k in os.environ for k in ("GLUE_VERSION", "AWS_GLUE_JOB_NAME"))


def build_spark(app_name: str = "battery-post-processing",
                *,
                local_cores: Optional[str] = None,
                arrow_batch_size: int = 10_000) -> SparkSession:
    """Return a SparkSession appropriate for the current environment.

    Parameters
    ----------
    app_name
        Spark UI application label.
    local_cores
        When running locally, master URL (e.g. "local[4]"). Defaults to
        `local[*]` minus 1 core (see [[system-unstable-under-full-load]]).
    arrow_batch_size
        Rows per Arrow batch handed to pandas UDFs. 10k is a sane default for
        HPPC (each cell × cycle group is < 5k rows).
    """
    # Already inside Glue — its runtime built the session; just return it.
    if _on_glue():
        return SparkSession.builder.getOrCreate()

    # IMPORTANT: in local mode the driver JVM starts *before* SparkConf is read,
    # so spark.driver.extraJavaOptions arrives too late. _JAVA_OPTIONS is read
    # by the JVM at launch unconditionally — the cleanest way to inject the
    # --add-opens flags Arrow needs on Java 17+. Idempotent.
    existing = os.environ.get("_JAVA_OPTIONS", "")
    if "--add-opens" not in existing:
        os.environ["_JAVA_OPTIONS"] = (existing + " " + _JAVA_OPENS).strip()

    # Workers spawn fresh Python interpreters. Pin them to the current one so
    # they inherit the same venv (pyarrow, numpy, pandas). Glue ignores this.
    import sys as _sys
    os.environ.setdefault("PYSPARK_PYTHON", _sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _sys.executable)

    cores = local_cores or _safe_local_master()
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(cores)
        # Arrow pandas-UDF performance
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(arrow_batch_size))
        # Don't nuke sibling partitions on overwrite
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Parquet defaults that match Glue
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # Smaller shuffle partitions for local (default 200 is overkill)
        .config("spark.sql.shuffle.partitions", "32")
        # Quiet startup noise
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _safe_local_master() -> str:
    """Pick `local[N]` leaving at least one core free.

    Matches the `--n-jobs 5` cap convention used elsewhere in this repo —
    full-load runs have OOM'd the host before.
    """
    import multiprocessing as mp
    n = max(1, mp.cpu_count() - 1)
    n = min(n, 5)
    return f"local[{n}]"
