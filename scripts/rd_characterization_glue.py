#!/usr/bin/env python
"""AWS Glue job entry point — rd_characterization_glue.

Paste THIS file as the Glue **Spark** job script. On every run it:

  1. downloads the code zip from s3://transformed-bin-table/glue-code/
     (any .zip there — e.g. the zipped Characterization-pyspark_v2 folder),
  2. extracts it and locates scripts/run_incremental.py inside (any nesting),
  3. executes run_incremental.py in the Glue Spark session.

run_incremental reads/advances its own watermark in
s3://transformed-bin-table/_config/rd_characterization.json, so a default run
needs no parameters. Optional Glue **job parameters** (all optional):

    --since      2026-01-01     process data after this date (override watermark)
    --mode       full           ignore the watermark, reprocess everything
    --makes      REPT,EVE       limit to these makes (comma-separated)
    --code_bucket transformed-bin-table          code-zip bucket
                                  (if omitted/not found, the newest .zip under
                                   glue-code/ is used)

Deploy:
    # zip the whole project folder (or just post_processing + scripts)
    cd '.../Characterization-pyspark_v2' && zip -r /tmp/code.zip .
    aws s3 cp /tmp/code.zip s3://transformed-bin-table/glue-code/Characterization-pyspark_v2.zip
    # Glue job: Type=Spark, Glue 4.0+ (Spark 3.3+/Python 3), this file as script.
    # Role: read s3://battery-rnd/*, read+write s3://transformed-bin-table/*,
    #       glue:GetTable/CreateTable/BatchCreatePartition on characterization_database.
"""
import os
import runpy
import sys
import zipfile

import boto3

DEFAULT_CODE_BUCKET = "transformed-bin-table"
CODE_PREFIX         = "glue-code/"
DEFAULT_CODE_ZIP    = "glue-code/Characterization-pyspark_v2.zip"
CODE_DIR            = "/tmp/rd_characterization_code"
LOCAL_ZIP           = "/tmp/rd_characterization.zip"


def _opt(name, default=None):
    """Read a `--name value` style Glue job parameter from sys.argv (optional)."""
    flag = "--" + name
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def _resolve_zip_key(s3, bucket, preferred):
    """Use `preferred` if it exists, else the newest .zip under glue-code/."""
    try:
        s3.head_object(Bucket=bucket, Key=preferred)
        return preferred
    except Exception:
        pass
    zips = []
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=CODE_PREFIX):
        for o in pg.get("Contents", []):
            if o["Key"].lower().endswith(".zip"):
                zips.append((o["LastModified"], o["Key"]))
    if not zips:
        raise FileNotFoundError(f"no .zip found under s3://{bucket}/{CODE_PREFIX}")
    return sorted(zips)[-1][1]          # newest


def _find_run_incremental(root):
    """Locate scripts/run_incremental.py anywhere under the extracted tree."""
    for dirpath, _dirs, files in os.walk(root):
        if "run_incremental.py" in files and os.path.basename(dirpath) == "scripts":
            return os.path.join(dirpath, "run_incremental.py")
    # fallback: any run_incremental.py
    for dirpath, _dirs, files in os.walk(root):
        if "run_incremental.py" in files:
            return os.path.join(dirpath, "run_incremental.py")
    raise FileNotFoundError("run_incremental.py not found in the code zip")


def main() -> None:
    bucket = _opt("code_bucket", DEFAULT_CODE_BUCKET)
    s3 = boto3.client("s3")
    key = _opt("code_zip") or _resolve_zip_key(s3, bucket, DEFAULT_CODE_ZIP)

    print(f"fetching code: s3://{bucket}/{key}")
    s3.download_file(bucket, key, LOCAL_ZIP)
    with zipfile.ZipFile(LOCAL_ZIP) as z:
        z.extractall(CODE_DIR)

    run_incr = _find_run_incremental(CODE_DIR)
    # run_incremental.py adds its own parent (parents[1]) to sys.path for the
    # post_processing package, so we only need to point runpy at it.
    print(f"running: {run_incr}")

    argv = ["run_incremental.py"]
    if _opt("mode") == "full":
        argv.append("--full")
    elif _opt("since"):
        argv += ["--since", _opt("since")]
    if _opt("makes"):
        argv += ["--makes"] + _opt("makes").split(",")
    sys.argv = argv
    print("args:", argv)

    # run_incremental ends with `raise SystemExit(main())`; runpy propagates that
    # SystemExit and Glue treats ANY SystemExit (even code 0) as a job failure.
    # Swallow a clean exit; only re-raise a genuine non-zero failure.
    try:
        runpy.run_path(run_incr, run_name="__main__")
    except SystemExit as e:
        if e.code not in (0, None):
            raise
        print("run_incremental completed (exit 0)")


if __name__ == "__main__":
    main()
