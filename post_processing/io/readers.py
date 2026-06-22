"""Read raw cycler exports into a typed Spark DataFrame.

Source layout (local):
    Data/HPPC/<make>_HPPC_cell_<id>.csv
    Data/OCVSOC/<make>_OCV_cell_<id>.csv
    ...

Source layout (S3, after Glue uploads / Athena UNLOAD):
    s3://<lake>/raw/<TEST>/<make>=<X>/<batch>=<Y>/*.parquet
    or  s3://<lake>/raw/<TEST>/*.csv

Both paths use the same reader by passing the right `path_pattern` and
`fmt`. We deliberately do NOT touch the AWS SDK — Spark/Hadoop pick up
credentials from the environment when the user runs against S3.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import RAW_SCHEMA


def read_raw_csv(spark: SparkSession,
                 path_pattern: str,
                 *,
                 fmt: str = "csv") -> DataFrame:
    """Read the raw cycler export.

    Parameters
    ----------
    spark
        Active SparkSession.
    path_pattern
        Glob accepted by Spark, e.g. ``Data/HPPC/*.csv`` or
        ``s3://bucket/raw/HPPC/*.parquet``.
    fmt
        ``csv`` or ``parquet``. Defaults to csv for the local file layout.

    Returns
    -------
    DataFrame
        Cast to ``RAW_SCHEMA``, with ``test`` / ``make`` / ``batch`` filled
        from the row contents (NOT inferred from the path).
    """
    if fmt == "csv":
        # NOT all raw CSV exports have the same columns. HPPC has `cycler_step_no`
        # (re-pulled with the patched SELECT) but OCV/DCIR/GITT/etc. don't.
        # Read with the header (name-based), then re-project + cast to RAW_SCHEMA
        # so downstream transforms always see the same column set.
        raw = (spark.read
               .option("header", True)
               .option("mode", "PERMISSIVE")
               .option("inferSchema", False)
               .csv(path_pattern))

        from pyspark.sql.functions import lit, col

        existing = set(raw.columns)
        cols = []
        for field in RAW_SCHEMA.fields:
            if field.name in existing:
                cols.append(col(field.name).cast(field.dataType).alias(field.name))
            else:
                # column absent from this test's export — fill with NULL of right type
                cols.append(lit(None).cast(field.dataType).alias(field.name))
        df = raw.select(*cols)
    elif fmt == "parquet":
        df = spark.read.schema(RAW_SCHEMA).parquet(path_pattern)
    else:
        raise ValueError(f"Unsupported fmt={fmt!r}; use 'csv' or 'parquet'")

    # Drop rows missing the keys we partition on or join on
    df = df.where(F.col("absolute_time").isNotNull()
                  & F.col("cell_no").isNotNull()
                  & F.col("step_name").isNotNull())

    # Normalise types coming from CSV: zero-padded cell_no, trimmed step_name
    df = (df
          .withColumn("cell_no", F.lpad(F.col("cell_no").cast("string"), 4, "0"))
          .withColumn("step_name", F.trim(F.col("step_name")))
          .withColumn("make", F.upper(F.col("make")))
          .withColumn("batch", F.col("batch").cast("string"))
          )
    return df
