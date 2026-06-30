"""Per-cycle aggregates: charge/discharge capacity, energy, voltage range.

Pure Spark SQL (no UDF needed) — runs on Longterm, RPT, or any test with
``cycle_no`` populated. The output is the building block for SoH
computation downstream.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import CYCLE_AGG_SCHEMA  # noqa: F401  (kept for symmetry)


def aggregate_per_cycle(raw_df: DataFrame) -> DataFrame:
    """Collapse raw time-series rows into one row per (cell, cycle).

    Capacity columns use the maximum reported ``capacity_ah`` within each
    charge / discharge step, then sum across all charge / discharge steps of
    that cycle. This matches the convention used by the cycler exporter
    (capacity column resets each step and accumulates monotonically).
    """
    df = raw_df.withColumn(
        "_phase",
        F.when(F.lower(F.col("step_name")).contains("dchg"), F.lit("dchg"))
         .when(F.lower(F.col("step_name")).contains("chg"),  F.lit("chg"))
         .otherwise(F.lit("other")),
    ).withColumn(
        "_step_id",
        F.coalesce(F.col("cycler_step_no"),
                   # fallback: hash of cycle + phase + step_name
                   F.abs(F.hash("cycle_no", "_phase", "step_name"))),
    )

    # `capacity_ah` is monotone-within-step but signed: positive on charge,
    # negative on discharge. Use abs() so charge & discharge contribute the
    # SIZE of the swing — otherwise discharge max() returns 0 (starting value)
    # and SoH = dchg_cap / Q_rpt collapses to zero across the board.
    step_lvl = (df
                .where(F.col("_phase") != "other")
                .groupBy("make", "batch", "cell_no", "max_cap", "cycle_no", "_phase", "_step_id")
                .agg(F.max(F.abs(F.col("capacity_ah"))).alias("step_cap_ah"),
                     F.avg("volt_v").alias("step_avg_v"),
                     F.max("volt_v").alias("step_v_max"),
                     F.min("volt_v").alias("step_v_min"),
                     F.sum(F.expr("volt_v * abs(current_a)")).alias("step_e_proxy"))
                )

    chg  = step_lvl.where(F.col("_phase") == "chg")
    dchg = step_lvl.where(F.col("_phase") == "dchg")

    chg_agg = (chg.groupBy("make", "batch", "cell_no", "max_cap", "cycle_no")
                  .agg(F.sum("step_cap_ah").alias("chg_cap_ah"),
                       F.avg("step_avg_v").alias("avg_chg_v"),
                       F.max("step_v_max").alias("chg_v_max")))
    dchg_agg = (dchg.groupBy("make", "batch", "cell_no", "max_cap", "cycle_no")
                    .agg(F.sum("step_cap_ah").alias("dchg_cap_ah"),
                         F.avg("step_avg_v").alias("avg_dchg_v"),
                         F.min("step_v_min").alias("dchg_v_min")))

    joined = (chg_agg.join(dchg_agg, ["make", "batch", "cell_no", "max_cap", "cycle_no"], "fullouter")
                     .withColumn("coulombic_eff",
                                 F.when(F.col("chg_cap_ah") > 0,
                                        F.col("dchg_cap_ah") / F.col("chg_cap_ah"))))
    return joined
