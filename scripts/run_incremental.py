#!/usr/bin/env python
"""Incremental characterization run → Athena-backed result table.

One self-contained job:

  1. Read the watermark from
     ``s3://transformed-bin-table/_config/rd_characterization.json``.
  2. Find every (test, make, batch, cell) group in the raw Detail lake whose
     data is newer than the watermark.
  3. Download the FULL history for those groups (so per-test calculations are
     never truncated mid-test), read with PySpark.
  4. Run the transforms and build one wide row per cell (curves as list<double>).
  5. Null-safe patch each row into
     ``s3://transformed-bin-table/rd_characterization/`` partitioned by
     make/max_cap/cell/batch — a NULL result never overwrites an existing
     non-null value for that primary key (make, max_cap, cell, batch).
  6. Register partitions in Glue so Athena
     (characterization_database.rd_characterization) sees them, then advance
     the watermark.

Longterm is never read — no consolidated column depends on it.

Requires PySpark + Java 17 and AWS read/write access to both buckets.

    python scripts/run_incremental.py                 # incremental from watermark
    python scripts/run_incremental.py --since 2026-01-01
    python scripts/run_incremental.py --full          # ignore watermark, all data
    python scripts/run_incremental.py --makes REPT EVE # limit makes (testing)
    python scripts/run_incremental.py --dry-run        # compute, do not write S3
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import math
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

PKG_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PKG_ROOT))
os.environ["PYTHONPATH"] = str(PKG_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

REGION       = os.environ.get("AWS_REGION", "ap-south-1")
SRC_BUCKET   = "battery-rnd"
SRC_PREFIX   = "TS/Detail/Chemistry=LFP/"
DST_BUCKET   = "transformed-bin-table"
DST_PREFIX   = "rd_characterization/"
CONFIG_KEY   = "_config/rd_characterization.json"
GLUE_DB      = "characterization_database"
GLUE_TABLE   = "rd_characterization"
PROCESSOR_VERSION = "pyspark-incremental-1.0"

# cycler-original -> canonical RAW_SCHEMA. "Sequence number of step" is the
# monotonic per-step counter ("Step No" repeats and would collapse pulses).
COL_RENAME = {
    "Cycle No": "cycle_no", "Sequence number of step": "cycler_step_no",
    "Step name": "step_name", "Absolute time": "absolute_time",
    "volt(V)": "volt_v", "Current(A)": "current_a", "Capacity(Ah)": "capacity_ah",
    "Test": "test", "Make": "make", "Batch": "batch", "Cell": "cell_no",
}
# Tests we process (Longterm deliberately excluded).
KEEP_TESTS = {"HPPC", "OCVSOC", "DCIR", "GITT", "RateCapability",
              "SelfDischarge", "PeakPower", "ConstantPower", "RPT"}

# Cross-test dependencies: tests that must be co-loaded (full history) for the
# same cell whenever ANY member changes, so cross-test calculations have all
# their inputs. SelfDischarge's dsoc/day needs the OCVSOC curve to invert V→SoC.
DEPENDENCY_GROUPS = [{"SelfDischarge", "OCVSOC"}]

# ── result schema (matches the Athena table: 45 data cols, curves=list<double>) ──
_TS, _D, _S, _L = pa.timestamp("us", tz="UTC"), pa.float64(), pa.string(), pa.list_(pa.float64())
RESULT_FIELDS = [
    ("cohort", _S),
    ("test_session_start", _TS), ("test_session_end", _TS), ("chemistry", _S),
    ("qc_flag", _S), ("processor_version", _S), ("Soh", _D),
    ("q_rpt_ah", _D), ("q_rpt_chg_ah", _D),
    ("rpt_protocol_c_rate", _D), ("rate_cap_c_rates", _L), ("rate_cap_q_curve", _L),
    ("rate_cap_c_rates_chg", _L), ("rate_cap_q_chg_curve", _L), ("const_power_levels_w", _L),
    ("const_power_t_dis_curve", _L), ("const_power_energy_curve", _L),
    ("self_disch_rest_duration_h", _D), ("self_disch_v_start_v", _D), ("self_disch_v_end_v", _D),
    ("self_disch_dsoc_per_day", _D), ("self_disch_ambient_c", _D), ("ocv_soc_grid", _L),
    ("v_oc_curve", _L), ("ocv_soc_grid_chg", _L), ("v_oc_chg_curve", _L), ("ocv_details_path", _S),
    ("dcir_n_pulses", _D), ("dcir_soc_nominal", _L), ("dcir_soc_at_pulse", _L),
    ("dcir_i_at_pulse", _L), ("r_dc_curve", _L), ("peak_power_soc_grid", _L),
    ("p_peak_10s_dchg_curve", _L), ("p_peak_10s_chg_curve", _L), ("hppc_n_pulses_dchg", _D),
    ("hppc_soc_at_pulse_dchg", _L), ("hppc_i_at_pulse_dchg", _L), ("r0_dchg_curve", _L),
    ("r1_dchg_curve", _L), ("c1_dchg_curve", _L), ("r2_dchg_curve", _L), ("c2_dchg_curve", _L),
    ("gitt_soc_grid", _L), ("r_pulse_curve", _L), ("tau_diff_curve", _L), ("v_inf_curve", _L),
]
RESULT_SCHEMA = pa.schema(RESULT_FIELDS)
DATA_COLS = [n for n, _ in RESULT_FIELDS]
ARRAY_COLS = {n for n, t in RESULT_FIELDS if t == _L}
TS_COLS = {"test_session_start", "test_session_end"}


# ─────────────────────────── config / watermark ───────────────────────────
def load_config(s3) -> dict:
    try:
        body = s3.get_object(Bucket=DST_BUCKET, Key=CONFIG_KEY)["Body"].read()
        return json.loads(body)
    except ClientError:
        return {}


def save_config(s3, cfg: dict) -> None:
    s3.put_object(Bucket=DST_BUCKET, Key=CONFIG_KEY,
                  Body=json.dumps(cfg, indent=2).encode(), ContentType="application/json")


# ─────────────────────────── source discovery ───────────────────────────
def list_source(s3):
    """Yield {key, Test, Make, Batch, Cell, date} for each non-Longterm Detail file."""
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=SRC_BUCKET, Prefix=SRC_PREFIX):
        for o in pg.get("Contents", []):
            p = dict(seg.split("=", 1) for seg in o["Key"].split("/") if "=" in seg)
            if not {"Test", "Make", "Batch", "Cell", "YYYY", "MM", "DD"} <= p.keys():
                continue
            if p["Test"] not in KEEP_TESTS:
                continue
            date = f"{p['YYYY']}-{int(p['MM']):02d}-{int(p['DD']):02d}"
            yield {"key": o["Key"], "Test": p["Test"], "Make": p["Make"],
                   "Batch": p["Batch"], "Cell": p["Cell"], "date": date}


def select_changed(objs, watermark, makes=None):
    """Groups (test,make,batch,cell) with data after the watermark — plus, for
    any changed test that's in a DEPENDENCY_GROUP, the group's OTHER tests for
    the same cell (so cross-test calcs have all inputs). Returns the FULL history
    of every selected group."""
    by_group = defaultdict(list)            # (test,make,batch,cell) -> objs
    for o in objs:
        if makes and o["Make"] not in makes:
            continue
        by_group[(o["Test"], o["Make"], o["Batch"], o["Cell"])].append(o)

    changed = {grp for grp, items in by_group.items()
               if any(it["date"] > watermark for it in items)}

    needed = set(changed)
    for (test, mk, bt, cell) in changed:
        for grp_tests in DEPENDENCY_GROUPS:
            if test in grp_tests:
                for partner in grp_tests:
                    if (partner, mk, bt, cell) in by_group:   # partner has data for this cell
                        needed.add((partner, mk, bt, cell))

    keys = []
    for grp in needed:
        keys.extend(o["key"] for o in by_group[grp])
    return keys


# ─────────────────────────── wide-row build (PySpark) ───────────────────────────
def build_wide(spark, raw):
    from pyspark.sql import functions as F
    from post_processing.transforms import (
        detect_hppc_pulses, extract_ocv_curves, extract_dcir_anchors,
        extract_gitt_pulses, extract_rate_capability, extract_self_discharge,
        extract_peak_power, extract_constant_power, aggregate_per_cycle,
    )
    keys = ["make", "batch", "cell_no", "max_cap"]

    def ordered(df, order_col, val_cols):
        fields = [F.col(order_col).alias("_k")] + [F.col(c).alias(c) for c in val_cols]
        g = df.groupBy(*keys).agg(F.sort_array(F.collect_list(F.struct(*fields))).alias("_a"))
        return g.select(*keys, F.size("_a").cast("double").alias("_n"),
                        *[F.col("_a." + c).alias(c) for c in val_cols])

    # universe + session window (over the downloaded raw)
    wide = raw.groupBy(*keys).agg(
        F.min("absolute_time").alias("test_session_start"),
        F.max("absolute_time").alias("test_session_end"))

    def join(df):
        nonlocal wide
        wide = wide.join(df, keys, "left")

    h = ordered(detect_hppc_pulses(raw), "pulse_idx",
                ["soc_start", "I_step", "R0_mOhm", "R1_mOhm", "C1_F", "R2_mOhm", "C2_F"])
    join(h.select(*keys, F.col("_n").alias("hppc_n_pulses_dchg"),
                  F.col("soc_start").alias("hppc_soc_at_pulse_dchg"),
                  F.col("I_step").alias("hppc_i_at_pulse_dchg"),
                  F.col("R0_mOhm").alias("r0_dchg_curve"), F.col("R1_mOhm").alias("r1_dchg_curve"),
                  F.col("C1_F").alias("c1_dchg_curve"), F.col("R2_mOhm").alias("r2_dchg_curve"),
                  F.col("C2_F").alias("c2_dchg_curve")))

    o = extract_ocv_curves(raw)
    od = ordered(o.where(F.col("direction") == "dchg"), "soc", ["soc", "v_oc"])
    join(od.select(*keys, F.col("soc").alias("ocv_soc_grid"), F.col("v_oc").alias("v_oc_curve")))
    oc = ordered(o.where(F.col("direction") == "chg"), "soc", ["soc", "v_oc"])
    join(oc.select(*keys, F.col("soc").alias("ocv_soc_grid_chg"), F.col("v_oc").alias("v_oc_chg_curve")))

    d = ordered(extract_dcir_anchors(raw), "pulse_idx", ["soc", "r0_mohm", "i_at_pulse"])
    # nominal SoC anchors by pulse count: 1→[0.9], 2→[0.9,0.2], 3→[0.9,0.5,0.2],
    # else fall back to the empirical SoC array.
    nominal = (F.when(F.col("_n") == 1.0, F.array(F.lit(0.9)))
                .when(F.col("_n") == 2.0, F.array(F.lit(0.9), F.lit(0.2)))
                .when(F.col("_n") == 3.0, F.array(F.lit(0.9), F.lit(0.5), F.lit(0.2)))
                .otherwise(F.col("soc")))
    join(d.select(*keys, F.col("_n").alias("dcir_n_pulses"),
                  F.col("soc").alias("dcir_soc_at_pulse"), F.col("r0_mohm").alias("r_dc_curve"),
                  F.col("i_at_pulse").alias("dcir_i_at_pulse"), nominal.alias("dcir_soc_nominal")))

    g = ordered(extract_gitt_pulses(raw), "pulse_idx", ["soc", "r_pulse_mohm", "tau_diff_s", "v_inf_v"])
    join(g.select(*keys, F.col("soc").alias("gitt_soc_grid"), F.col("r_pulse_mohm").alias("r_pulse_curve"),
                  F.col("tau_diff_s").alias("tau_diff_curve"), F.col("v_inf_v").alias("v_inf_curve")))

    r = extract_rate_capability(raw)
    rd = ordered(r.where(F.col("direction") == "dchg"), "c_rate", ["c_rate", "q_ah"])
    join(rd.select(*keys, F.col("c_rate").alias("rate_cap_c_rates"), F.col("q_ah").alias("rate_cap_q_curve")))
    rc = ordered(r.where(F.col("direction") == "chg"), "c_rate", ["c_rate", "q_ah"])
    join(rc.select(*keys, F.col("c_rate").alias("rate_cap_c_rates_chg"), F.col("q_ah").alias("rate_cap_q_chg_curve")))

    cp = ordered(extract_constant_power(raw).where(F.col("direction") == "dchg"),
                 "power_w", ["power_w", "duration_s", "energy_wh"])
    join(cp.select(*keys, F.col("power_w").alias("const_power_levels_w"),
                   F.col("duration_s").alias("const_power_t_dis_curve"),
                   F.col("energy_wh").alias("const_power_energy_curve")))

    p = extract_peak_power(raw)
    pdis = ordered(p.where(F.col("direction") == "dchg"), "soc", ["soc", "p_peak_w"])
    join(pdis.select(*keys, F.col("soc").alias("peak_power_soc_grid"),
                     F.col("p_peak_w").alias("p_peak_10s_dchg_curve")))
    pchg = ordered(p.where(F.col("direction") == "chg"), "soc", ["p_peak_w"])
    join(pchg.select(*keys, F.col("p_peak_w").alias("p_peak_10s_chg_curve")))

    s = (extract_self_discharge(raw).groupBy(*keys)
         .agg(F.first("rest_duration_s").alias("_rd"),
              F.first("v_start").alias("self_disch_v_start_v"),
              F.first("v_end").alias("self_disch_v_end_v"),
              F.first("dsoc_per_day").alias("self_disch_dsoc_per_day"),
              F.first("ambient_c").alias("self_disch_ambient_c"))
         .withColumn("self_disch_rest_duration_h", F.col("_rd") / 3600.0).drop("_rd"))
    join(s)

    # RPT capacity + protocol C-rate (first non-null crate, fallback drate).
    rpt = (aggregate_per_cycle(raw.where(F.col("test") == "RPT")).groupBy(*keys)
           .agg(F.max("dchg_cap_ah").alias("q_rpt_ah"), F.max("chg_cap_ah").alias("q_rpt_chg_ah")))
    join(rpt)
    # crate is labelled like "0.333C"/"0.333D" — pull the numeric prefix.
    crate = (raw.where(F.col("test") == "RPT")
                .select(*keys, F.regexp_extract(
                    F.coalesce(F.col("crate"), F.col("drate")),
                    r"([0-9]*\.?[0-9]+)", 1).cast("double").alias("_cr"))
                .where(F.col("_cr").isNotNull())
                .groupBy(*keys).agg(F.first("_cr").alias("rpt_protocol_c_rate")))
    join(crate)

    # cohort = make; Soh = q_rpt / nominal × 100.
    wide = (wide.withColumn("cohort", F.col("make"))
                .withColumn("Soh", F.when(F.col("max_cap") > 0,
                                          F.col("q_rpt_ah") / F.col("max_cap") * 100.0)))
    # constants + the OCV details-store pointer (reference template).
    wide = (wide.withColumn("chemistry", F.lit("LFP"))
                .withColumn("qc_flag", F.lit("ok"))
                .withColumn("processor_version", F.lit(PROCESSOR_VERSION))
                .withColumn("ocv_details_path",
                            F.concat(F.lit("details/cohort="), F.col("make"),
                                     F.lit("/cell_id="), F.col("cell_no"),
                                     F.lit("/batch="), F.col("batch"),
                                     F.lit("/ocv.parquet"))))

    for c in DATA_COLS:
        if c not in wide.columns:
            wide = wide.withColumn(c, F.lit(None).cast("array<double>" if c in ARRAY_COLS else "double"))
    return wide.select(*keys, *DATA_COLS)


# ─────────────────────────── null-safe S3 patch ───────────────────────────
def _blank(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    if isinstance(v, (list, tuple, np.ndarray)):
        return len(v) == 0
    if isinstance(v, str):
        return v.strip() == ""
    return False


def _partition_prefix(make, max_cap, cell, batch) -> str:
    return (f"{DST_PREFIX}make={make}/max_cap={max_cap}/cell={cell}/batch={batch}/")


def _read_existing(s3, prefix):
    objs = s3.list_objects_v2(Bucket=DST_BUCKET, Prefix=prefix).get("Contents", [])
    parts = [o["Key"] for o in objs if o["Key"].endswith(".parquet")]
    if not parts:
        return None, []
    body = s3.get_object(Bucket=DST_BUCKET, Key=parts[0])["Body"].read()
    tbl = pq.read_table(io.BytesIO(body))
    row = tbl.to_pylist()[0] if tbl.num_rows else {}
    return row, parts


def patch_row(s3, keyvals, new_row, *, dry_run=False) -> str:
    """Null-safe merge `new_row` into the partition; rewrite a single file."""
    make, max_cap, cell, batch = keyvals
    prefix = _partition_prefix(make, max_cap, cell, batch)
    existing, old_parts = _read_existing(s3, prefix)

    merged = {}
    for c in DATA_COLS:
        nv = new_row.get(c)
        if _blank(nv) and existing is not None and not _blank(existing.get(c)):
            merged[c] = existing.get(c)            # keep existing non-null
        else:
            merged[c] = nv                         # new non-null (or both blank)

    # build a 1-row table conforming to RESULT_SCHEMA
    arrays = []
    for name, typ in RESULT_FIELDS:
        v = merged.get(name)
        if name in TS_COLS:
            arrays.append(pa.array([v], type=typ))
        elif name in ARRAY_COLS:
            lst = list(v) if isinstance(v, (list, tuple, np.ndarray)) and not _blank(v) else None
            arrays.append(pa.array([lst], type=typ))
        else:
            arrays.append(pa.array([None if _blank(v) else v], type=typ))
    table = pa.Table.from_arrays(arrays, schema=RESULT_SCHEMA)

    new_key = prefix + "part-0.snappy.parquet"
    if dry_run:
        return new_key
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    s3.put_object(Bucket=DST_BUCKET, Key=new_key, Body=buf.getvalue())
    for k in old_parts:                            # drop stale files → one row per PK
        if k != new_key:
            s3.delete_object(Bucket=DST_BUCKET, Key=k)
    return new_key


# ─────────────────────────── Glue table + partition registration ───────────────────────────
def _glue_type(t) -> str:
    if t == _TS:
        return "timestamp"
    if t == _D:
        return "double"
    if t == _S:
        return "string"
    return "array<double>"        # _L


def _table_input():
    sd = {
        "Columns": [{"Name": n, "Type": _glue_type(t)} for n, t in RESULT_FIELDS],
        "Location": f"s3://{DST_BUCKET}/{DST_PREFIX.rstrip('/')}",
        "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {"SerializationLibrary":
                      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
    }
    return {
        "Name": GLUE_TABLE, "TableType": "EXTERNAL_TABLE",
        "PartitionKeys": [{"Name": k, "Type": "string"}
                          for k in ("make", "max_cap", "cell", "batch")],
        "StorageDescriptor": sd,
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
    }


def ensure_table(glue) -> dict:
    """Return the Glue table, creating it if missing or updating it if the column
    set has drifted from RESULT_FIELDS (e.g. new Soh/cohort columns)."""
    try:
        tbl = glue.get_table(DatabaseName=GLUE_DB, Name=GLUE_TABLE)["Table"]
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=GLUE_DB, TableInput=_table_input())
        print(f"created Glue table {GLUE_DB}.{GLUE_TABLE}")
        return glue.get_table(DatabaseName=GLUE_DB, Name=GLUE_TABLE)["Table"]
    have = [c["Name"] for c in tbl["StorageDescriptor"]["Columns"]]
    want = [n for n, _ in RESULT_FIELDS]
    if have != want:
        glue.update_table(DatabaseName=GLUE_DB, TableInput=_table_input())
        print(f"updated Glue table schema ({len(want)} columns)")
        tbl = glue.get_table(DatabaseName=GLUE_DB, Name=GLUE_TABLE)["Table"]
    return tbl


def register_partitions(glue, pks):
    tbl = ensure_table(glue)
    sd = tbl["StorageDescriptor"]
    base = sd["Location"].rstrip("/")
    inputs = []
    for (make, max_cap, cell, batch) in pks:
        loc = f"{base}/make={make}/max_cap={max_cap}/cell={cell}/batch={batch}/"
        psd = dict(sd); psd["Location"] = loc
        inputs.append({"Values": [make, max_cap, cell, batch], "StorageDescriptor": psd})
    created = 0
    for i in range(0, len(inputs), 100):
        chunk = inputs[i:i + 100]
        resp = glue.batch_create_partition(DatabaseName=GLUE_DB, TableName=GLUE_TABLE,
                                           PartitionInputList=chunk)
        errs = [e for e in resp.get("Errors", [])
                if e.get("ErrorDetail", {}).get("ErrorCode") != "AlreadyExistsException"]
        created += len(chunk) - len(resp.get("Errors", []))
        for e in errs:
            print("  glue partition error:", e.get("ErrorDetail", {}).get("ErrorMessage"))
    return created


# ─────────────────────────── main ───────────────────────────
def _on_glue() -> bool:
    """True inside an AWS Glue runtime (read S3 directly instead of downloading)."""
    return any(k in os.environ for k in ("GLUE_VERSION", "AWS_GLUE_JOB_NAME"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="Override watermark (YYYY-MM-DD).")
    ap.add_argument("--full", action="store_true", help="Process all data (watermark = epoch).")
    ap.add_argument("--makes", nargs="*", default=None, help="Limit to these makes.")
    ap.add_argument("--cells", nargs="*", default=None,
                    help="Limit to specific cells, format MAKE:BATCH:CELL (testing). "
                         "Does NOT advance the watermark.")
    ap.add_argument("--dry-run", action="store_true", help="Compute but do not write S3/Glue.")
    args = ap.parse_args()

    s3 = boto3.client("s3", region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)

    cfg = load_config(s3)
    # Watermark precedence: --full (epoch) > --since > config last_run_date > epoch.
    watermark = "1970-01-01" if args.full else (args.since or cfg.get("last_run_date", "1970-01-01"))


    print(f"watermark = {watermark}  (config last_run_date={cfg.get('last_run_date')})")

    objs = list(list_source(s3))
    max_date = max((o["date"] for o in objs), default=watermark)
    makes = set(args.makes) if args.makes else None
    keys = select_changed(objs, watermark, makes)
    if args.cells:
        want = {tuple(c.split(":")) for c in args.cells}      # (Make,Batch,Cell)
        def _kmatch(k):
            p = dict(seg.split("=", 1) for seg in k.split("/") if "=" in seg)
            return (p.get("Make"), p.get("Batch"), p.get("Cell")) in want
        keys = [k for k in keys if _kmatch(k)]
    affected_groups = {(o["Make"], o["Batch"], o["Cell"])
                       for o in objs if o["key"] in set(keys)}
    print(f"changed files: {len(keys)} across {len(affected_groups)} cells; lake max date={max_date}")
    if not keys:
        print("nothing to do — watermark up to date.")
        return 0

    on_glue = _on_glue()
    raw_root = None
    if not on_glue:
        # Local mode: download the changed files, then read from local disk, and
        # give the single-JVM driver a big heap with broadcast joins OFF (a
        # broadcast OOM is what killed an early local run). On Glue we skip all
        # this and read S3 directly (distributed) — the driver's local /tmp isn't
        # visible to executors there.
        raw_root = Path(tempfile.mkdtemp(prefix="incr_raw_"))
        for k in keys:
            dest = raw_root / k[len(SRC_PREFIX):]
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(SRC_BUCKET, k, str(dest))
        print(f"downloaded {len(keys)} files -> {raw_root}")
        os.environ.setdefault("SPARK_LOCAL_DIRS", str(raw_root / "_spark_tmp"))
        os.environ.setdefault(
            "PYSPARK_SUBMIT_ARGS",
            "--driver-memory 8g "
            "--conf spark.sql.autoBroadcastJoinThreshold=-1 "
            "--conf spark.driver.maxResultSize=2g pyspark-shell")

    import pandas as pd
    from pyspark.sql import functions as F
    from post_processing import build_spark
    from post_processing.config import RAW_SCHEMA

    spark = build_spark(app_name="rd-characterization-incremental")
    try:
        if on_glue:
            base = f"s3://{SRC_BUCKET}/{SRC_PREFIX}"
            uris = [f"s3://{SRC_BUCKET}/{k}" for k in keys]
            raw = spark.read.option("basePath", base).parquet(*uris)
        else:
            raw = spark.read.option("basePath", str(raw_root)).parquet(
                str(raw_root) + "/Test=*/Make=*/Batch=*/Cell=*/*/*/*/*.parquet")
        for old, new in COL_RENAME.items():
            if old in raw.columns:
                raw = raw.withColumnRenamed(old, new)
        raw = (raw
               .withColumn("cell_no", F.lpad(F.col("cell_no").cast("string"), 4, "0"))
               .withColumn("make", F.upper(F.col("make")))
               .withColumn("batch", F.col("batch").cast("string"))
               .withColumn("step_name", F.trim(F.col("step_name")))
               .withColumn("cycle_no", F.col("cycle_no").cast("int"))
               .withColumn("cycler_step_no", F.col("cycler_step_no").cast("int"))
               .withColumn("max_cap", F.col("max_cap").cast("double"))
               .withColumn("volt_v", F.col("volt_v").cast("double"))
               .withColumn("current_a", F.col("current_a").cast("double"))
               .withColumn("capacity_ah", F.col("capacity_ah").cast("double"))
               .where(F.col("absolute_time").isNotNull()
                      & F.col("cell_no").isNotNull() & F.col("step_name").isNotNull()))
        raw = raw.select(*[f.name for f in RAW_SCHEMA.fields]).cache()
        wide = build_wide(spark, raw)
        pdf = wide.toPandas()
    finally:
        if not on_glue:                 # let Glue manage its own session lifecycle
            spark.stop()
        if raw_root is not None:
            shutil.rmtree(raw_root, ignore_errors=True)

    print(f"computed {len(pdf)} cell rows")
    from post_processing import post_join
    pdf = post_join.apply(pdf)          # cross-test columns (e.g. dsoc/day)
    written_pks = []
    for _, row in pdf.iterrows():
        make = str(row["make"])
        max_cap = ("%s" % row["max_cap"]) if row["max_cap"] is not None and not (
            isinstance(row["max_cap"], float) and math.isnan(row["max_cap"])) else "unknown"
        cell = str(row["cell_no"]); batch = str(row["batch"])
        new_row = {}
        for c in DATA_COLS:
            v = row[c]
            if c in TS_COLS and v is not None and not pd.isna(v):
                v = pd.Timestamp(v, tz="UTC") if pd.Timestamp(v).tzinfo is None else pd.Timestamp(v)
            new_row[c] = v
        patch_row(s3, (make, max_cap, cell, batch), new_row, dry_run=args.dry_run)
        written_pks.append((make, max_cap, cell, batch))
        print(f"  {'(dry) ' if args.dry_run else ''}patched make={make} max_cap={max_cap} cell={cell} batch={batch}")

    if not args.dry_run:
        created = register_partitions(glue, written_pks)
        print(f"registered {created} new Glue partitions ({GLUE_DB}.{GLUE_TABLE})")
        if args.cells:
            print("--cells targeted run: watermark NOT advanced")
        else:
            cfg["last_run_date"] = max_date
            cfg.setdefault("table", f"{GLUE_DB}.{GLUE_TABLE}")
            save_config(s3, cfg)
            print(f"watermark advanced -> {max_date}")
    else:
        print("dry-run: no S3/Glue writes, watermark unchanged")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
