"""Per-cycle aggregate job (works for Longterm, RPT, RateCapability)."""
from __future__ import annotations

from pyspark.sql import SparkSession

from ..io import read_raw_csv, write_partitioned_parquet
from ..transforms import aggregate_per_cycle


def run_cycle_job(spark: SparkSession,
                  input_path: str,
                  output_path: str,
                  *,
                  input_fmt: str = "csv") -> int:
    raw = read_raw_csv(spark, input_path, fmt=input_fmt)
    cycles = aggregate_per_cycle(raw)
    cycles = cycles.repartition("make", "batch")
    write_partitioned_parquet(
        cycles, output_path,
        partition_by=("make", "batch"),
        sort_within=("cell_no", "cycle_no"),
    )
    return cycles.count()
