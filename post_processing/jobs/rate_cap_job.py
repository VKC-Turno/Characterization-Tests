"""Rate-capability extraction job."""
from __future__ import annotations

from pyspark.sql import SparkSession, functions as F

from ..io import read_raw_csv, write_partitioned_parquet
from ..transforms import extract_rate_capability


def run_rate_cap_job(spark: SparkSession,
                     input_path: str,
                     output_path: str,
                     *,
                     input_fmt: str = "csv") -> int:
    raw = read_raw_csv(spark, input_path, fmt=input_fmt)
    raw = raw.withColumn("test", F.coalesce(F.col("test"), F.lit("RateCapability")))
    out = extract_rate_capability(raw).repartition("make", "batch")
    write_partitioned_parquet(
        out, output_path,
        partition_by=("make", "batch"),
        sort_within=("cell_no", "direction", "c_rate"),
    )
    return out.count()
