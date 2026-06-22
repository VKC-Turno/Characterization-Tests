"""HPPC pulse-identification job.

Reads raw HPPC time-series, applies the VKC pulse detector, and writes
partitioned parquet. Designed to run identically locally and on Glue.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from ..io import read_raw_csv, write_partitioned_parquet
from ..transforms import detect_hppc_pulses


def run_hppc_job(spark: SparkSession,
                 input_path: str,
                 output_path: str,
                 *,
                 input_fmt: str = "csv") -> int:
    """Execute the HPPC pulse-detection pipeline end-to-end.

    Returns the row count written (for assertions in tests / job logs).
    """
    raw = read_raw_csv(spark, input_path, fmt=input_fmt)
    # Cells may carry no `test` column when the source is a per-test folder.
    # Backfill 'HPPC' if missing so the transform's predicate succeeds.
    from pyspark.sql import functions as F
    raw = raw.withColumn("test", F.coalesce(F.col("test"), F.lit("HPPC")))

    pulses = detect_hppc_pulses(raw)
    pulses = pulses.repartition("make", "batch")

    write_partitioned_parquet(
        pulses, output_path,
        partition_by=("make", "batch"),
        sort_within=("cell_no", "cycle_no", "pulse_idx"),
    )
    return pulses.count()
