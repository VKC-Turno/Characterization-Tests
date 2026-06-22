"""GITT pulse-extraction job."""
from __future__ import annotations

from pyspark.sql import SparkSession, functions as F

from ..io import read_raw_csv, write_partitioned_parquet
from ..transforms import extract_gitt_pulses


def run_gitt_job(spark: SparkSession,
                 input_path: str,
                 output_path: str,
                 *,
                 input_fmt: str = "csv") -> int:
    raw = read_raw_csv(spark, input_path, fmt=input_fmt)
    raw = raw.withColumn("test", F.coalesce(F.col("test"), F.lit("GITT")))
    pulses = extract_gitt_pulses(raw).repartition("make", "batch")
    write_partitioned_parquet(
        pulses, output_path,
        partition_by=("make", "batch"),
        sort_within=("cell_no", "pulse_idx"),
    )
    return pulses.count()
