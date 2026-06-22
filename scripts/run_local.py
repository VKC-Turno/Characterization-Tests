#!/usr/bin/env python
"""Local runner — invokes one or more post-processing jobs on data under Data/.

Usage examples (run from repo root, i.e. /home/hj/Desktop/PINNs):

    # HPPC, all makes/batches
    python post_processing_script/scripts/run_local.py \\
        --job hppc \\
        --input  'Data/HPPC/*.csv' \\
        --output post_processing_script/output/HPPC

    # OCV(SoC) curves
    python post_processing_script/scripts/run_local.py \\
        --job ocv \\
        --input  'Data/OCVSOC/*.csv' \\
        --output post_processing_script/output/OCV

    # All-in-one (runs HPPC + OCV + DCIR + cycle aggregates)
    python post_processing_script/scripts/run_local.py --job all
"""
from __future__ import annotations

import argparse
import os
import sys
import zipfile
from pathlib import Path

# allow `python post_processing_script/scripts/run_local.py` from repo root
PKG_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PKG_ROOT))

# Workers spawn fresh Python interpreters that don't inherit this sys.path,
# so we also set PYTHONPATH (read at worker startup) and ship the package
# as a zip via SparkContext.addPyFile.
os.environ["PYTHONPATH"] = str(PKG_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

from post_processing import build_spark
from post_processing.jobs import (
    run_hppc_job, run_ocv_job, run_dcir_job, run_cycle_job,
    run_gitt_job, run_rate_cap_job, run_self_discharge_job,
    run_peak_power_job, run_constant_power_job,
)


def _zip_package(pkg_dir: Path, out: Path) -> Path:
    """Zip the `post_processing` package so workers can deserialize UDFs."""
    if out.exists():
        out.unlink()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in pkg_dir.rglob("*.py"):
            zf.write(p, p.relative_to(pkg_dir.parent))
    return out


JOBS = {
    "hppc":           (run_hppc_job,           "Data/HPPC/*.csv",
                       "post_processing_script/output/HPPC"),
    "ocv":            (run_ocv_job,            "Data/OCVSOC/*.csv",
                       "post_processing_script/output/OCV"),
    "dcir":           (run_dcir_job,           "Data/DCIR/*.csv",
                       "post_processing_script/output/DCIR"),
    "gitt":           (run_gitt_job,           "Data/GITT/*.csv",
                       "post_processing_script/output/GITT"),
    "rate_cap":       (run_rate_cap_job,       "Data/RateCapability/*.csv",
                       "post_processing_script/output/RATE_CAP"),
    "self_discharge": (run_self_discharge_job, "Data/SelfDischarge/*.csv",
                       "post_processing_script/output/SELF_DISCHARGE"),
    "peak_power":     (run_peak_power_job,     "Data/PeakPower/*.csv",
                       "post_processing_script/output/PEAK_POWER"),
    "constant_power": (run_constant_power_job, "Data/ConstantPower/*.csv",
                       "post_processing_script/output/CONSTANT_POWER"),
    "cycles_long":    (run_cycle_job,          "Data/Longterm/*.csv",
                       "post_processing_script/output/CYCLES_LONG"),
    "cycles_rpt":     (run_cycle_job,          "Data/RPT/*.csv",
                       "post_processing_script/output/CYCLES_RPT"),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, choices=list(JOBS.keys()) + ["all"])
    ap.add_argument("--input", default=None,
                    help="Override default glob. Required iff a non-default layout is used.")
    ap.add_argument("--output", default=None,
                    help="Override default output dir.")
    ap.add_argument("--input-fmt", default="csv", choices=("csv", "parquet"))
    args = ap.parse_args()

    spark = build_spark(app_name=f"local-{args.job}")

    # Ship the package zip to every worker so applyInPandas UDFs deserialize
    zip_path = _zip_package(PKG_ROOT / "post_processing",
                            PKG_ROOT / "post_processing.zip")
    spark.sparkContext.addPyFile(str(zip_path))

    targets = [args.job] if args.job != "all" else list(JOBS.keys())

    for j in targets:
        runner, default_in, default_out = JOBS[j]
        in_path  = args.input  or default_in
        out_path = args.output or default_out
        print(f"\n=== job={j}  in={in_path}  out={out_path}")
        n = runner(spark, in_path, out_path, input_fmt=args.input_fmt)
        print(f"=== job={j}: wrote {n:,} rows -> {out_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
