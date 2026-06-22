"""Write transformed outputs as partitioned parquet for cheap downstream querying.

Layout written:
    <root>/<test>/make=<X>/batch=<Y>/part-*.snappy.parquet

Querying from Athena / DuckDB after the fact:
    SELECT * FROM "<root>/HPPC"
    WHERE make = 'EVE' AND batch = '1' AND cell_no = '0002'

Partition pruning will avoid scanning every cohort.
"""
from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import DEFAULT_COMPRESSION, PARTITION_KEYS


def write_partitioned_parquet(df: DataFrame,
                              output_path: str,
                              *,
                              partition_by: Iterable[str] = PARTITION_KEYS,
                              compression: str = DEFAULT_COMPRESSION,
                              sort_within: Optional[Iterable[str]] = None,
                              mode: str = "overwrite") -> None:
    """Write `df` partitioned by `partition_by` to `output_path`.

    `sort_within` reorders each partition's rows before write so downstream
    range-scan queries (e.g. by cell_no + cycle_no) read sequentially.
    """
    parts = list(partition_by)

    out = df
    if sort_within:
        # sortWithinPartitions keeps each partition co-located; cheap, no shuffle.
        out = out.sortWithinPartitions(*sort_within)

    (out.write
        .mode(mode)
        .partitionBy(*parts)
        .option("compression", compression)
        .parquet(output_path))


def coalesce_for_small_outputs(df: DataFrame, max_files_per_partition: int = 1) -> DataFrame:
    """Hint: collapse to N files per partition. Use only for genuinely small
    aggregated outputs (HPPC pulse table is ~100 rows per cell — 1 file is fine).
    """
    return df.coalesce(max_files_per_partition)
