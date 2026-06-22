"""Schemas + constants used by all transforms.

Schemas are declared explicitly so the Spark planner doesn't infer types from
millions of CSV rows on every run, and so partition prune predicates work.
"""
from __future__ import annotations

from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)


# ─────────────────────────── input schemas ───────────────────────────

# Raw cycler export — matches the SELECT in _pull_hppc.py (Athena `detail` table)
RAW_SCHEMA = StructType([
    StructField("cell_no",        StringType(),    True),
    StructField("cycle_no",       IntegerType(),   True),
    StructField("cycler_step_no", IntegerType(),   True),
    StructField("step_name",      StringType(),    True),
    StructField("absolute_time",  TimestampType(), True),
    StructField("volt_v",         DoubleType(),    True),
    StructField("current_a",      DoubleType(),    True),
    StructField("capacity_ah",    DoubleType(),    True),
    StructField("crate",          StringType(),    True),
    StructField("drate",          StringType(),    True),
    StructField("dod",            StringType(),    True),
    StructField("max_cap",        DoubleType(),    True),
    StructField("test",           StringType(),    True),
    StructField("make",           StringType(),    True),
    StructField("batch",          StringType(),    True),
])


# ─────────────────────── output schemas (transforms) ──────────────────

# One row per detected HPPC pulse. Partition columns (make/test/batch) live
# at the writer layer — keep them out of the transform schema so applyInPandas
# stays unaware of the partition key.
PULSE_SCHEMA = StructType([
    StructField("make",       StringType(),  False),
    StructField("batch",      StringType(),  False),
    StructField("cell_no",    StringType(),  False),
    StructField("cycle_no",   IntegerType(), True),
    StructField("pulse_idx",  IntegerType(), False),
    StructField("step_no",    IntegerType(), True),
    StructField("duration_s", DoubleType(),  True),
    StructField("I_step",     DoubleType(),  True),
    StructField("soc_start",  DoubleType(),  True),
    StructField("V_pre",      DoubleType(),  True),
    StructField("V_post",     DoubleType(),  True),
    StructField("V_fast",     DoubleType(),  True),
    StructField("V_end",      DoubleType(),  True),
    StructField("tau1_s",     DoubleType(),  True),
    StructField("tau2_s",     DoubleType(),  True),
    StructField("R0_mOhm",    DoubleType(),  True),
    StructField("R1_mOhm",    DoubleType(),  True),
    StructField("R2_mOhm",    DoubleType(),  True),
    StructField("C1_F",       DoubleType(),  True),
    StructField("C2_F",       DoubleType(),  True),
    StructField("R_30s_mOhm", DoubleType(),  True),
])

# Per-cycle aggregate output (one row per cell per cycle).
# Used by RPT / longterm / capacity transforms.
CYCLE_AGG_SCHEMA = StructType([
    StructField("make",         StringType(),  False),
    StructField("batch",        StringType(),  False),
    StructField("cell_no",      StringType(),  False),
    StructField("cycle_no",     IntegerType(), False),
    StructField("chg_cap_ah",   DoubleType(),  True),
    StructField("dchg_cap_ah",  DoubleType(),  True),
    StructField("chg_energy_wh",  DoubleType(), True),
    StructField("dchg_energy_wh", DoubleType(), True),
    StructField("v_max",        DoubleType(),  True),
    StructField("v_min",        DoubleType(),  True),
    StructField("avg_chg_v",    DoubleType(),  True),
    StructField("avg_dchg_v",   DoubleType(),  True),
    StructField("coulombic_eff", DoubleType(), True),
])

# OCV(SoC) curve — one row per SoC bin per cell
OCV_CURVE_SCHEMA = StructType([
    StructField("make",     StringType(),  False),
    StructField("batch",    StringType(),  False),
    StructField("cell_no",  StringType(),  False),
    StructField("direction", StringType(), False),     # "chg" | "dchg"
    StructField("soc",      DoubleType(),  False),
    StructField("v_oc",     DoubleType(),  True),
])

# DCIR (R0) anchors — one row per SoC anchor per cell
DCIR_ANCHOR_SCHEMA = StructType([
    StructField("make",     StringType(),  False),
    StructField("batch",    StringType(),  False),
    StructField("cell_no",  StringType(),  False),
    StructField("soc",      DoubleType(),  False),
    StructField("r0_mohm",  DoubleType(),  True),
])

# GITT — one row per pulse anchor
GITT_PULSE_SCHEMA = StructType([
    StructField("make",       StringType(),  False),
    StructField("batch",      StringType(),  False),
    StructField("cell_no",    StringType(),  False),
    StructField("pulse_idx",  IntegerType(), False),
    StructField("soc",        DoubleType(),  True),
    StructField("r_pulse_mohm", DoubleType(), True),
    StructField("tau_diff_s",   DoubleType(), True),
    StructField("v_inf_v",      DoubleType(), True),
])

# Rate capability — one row per C-rate per cell per direction
RATE_CAP_SCHEMA = StructType([
    StructField("make",      StringType(),  False),
    StructField("batch",     StringType(),  False),
    StructField("cell_no",   StringType(),  False),
    StructField("direction", StringType(),  False),    # "chg" | "dchg"
    StructField("c_rate",    DoubleType(),  False),
    StructField("q_ah",      DoubleType(),  True),
    StructField("energy_wh", DoubleType(),  True),
])

# Self-discharge — one row per cell with the OCV drift over the rest interval
SELF_DISCHARGE_SCHEMA = StructType([
    StructField("make",           StringType(),  False),
    StructField("batch",          StringType(),  False),
    StructField("cell_no",        StringType(),  False),
    StructField("rest_duration_s", DoubleType(), True),
    StructField("v_start",        DoubleType(),  True),
    StructField("v_end",          DoubleType(),  True),
    StructField("dv_dt_mV_per_h", DoubleType(),  True),
    StructField("q_recovered_ah", DoubleType(),  True),  # CC_DChg after rest
    StructField("retention_pct",  DoubleType(),  True),  # q_recovered / q_before_rest
])

# Peak power — one row per direction per SoC anchor; P_max from V·I envelope
PEAK_POWER_SCHEMA = StructType([
    StructField("make",      StringType(),  False),
    StructField("batch",     StringType(),  False),
    StructField("cell_no",   StringType(),  False),
    StructField("direction", StringType(),  False),    # "chg" | "dchg"
    StructField("soc",       DoubleType(),  False),
    StructField("p_peak_w",  DoubleType(),  True),
    StructField("v_at_peak", DoubleType(),  True),
    StructField("i_at_peak", DoubleType(),  True),
    StructField("duration_s", DoubleType(), True),
])

# Constant power — one row per power level per cell per direction
CONSTANT_POWER_SCHEMA = StructType([
    StructField("make",       StringType(),  False),
    StructField("batch",      StringType(),  False),
    StructField("cell_no",    StringType(),  False),
    StructField("direction",  StringType(),  False),   # "chg" | "dchg"
    StructField("power_w",    DoubleType(),  False),
    StructField("energy_wh",  DoubleType(),  True),
    StructField("duration_s", DoubleType(),  True),
    StructField("q_ah",       DoubleType(),  True),
])


# ─────────────────────── partitioning + storage ───────────────────────

# Parquet partition keys — order matters for prune efficiency.
# (make, batch) keeps each cohort/batch in its own small file set so a query
# like "REPT batch 1 HPPC" reads only the matching directory.
PARTITION_KEYS = ["make", "batch"]

# snappy = Glue default; ZSTD compresses ~30% better but writes slower.
# Override via the JOB arg --compression at job-launch time if desired.
DEFAULT_COMPRESSION = "snappy"

# Test names recognised in raw `test` column / step naming.
KNOWN_TESTS = (
    "HPPC", "OCVSOC", "OCV_SOC", "OCV", "DCIR", "GITT",
    "RateCapability", "RPT", "Longterm", "SelfDischarge",
    "PeakPower", "ConstantPower",
)
