"""AWS Glue 4.0 entrypoint.

Deploy as the Glue Job script (Type = Spark). Glue invokes this file with
--JOB_NAME and the args defined in the job. We expose:

    --job          one of the 10 test pipelines:
                       hppc | ocv | dcir | gitt | rate_cap |
                       self_discharge | peak_power | constant_power |
                       cycles_long | cycles_rpt
    --input        S3 path glob to read   (e.g. s3://lake/raw/HPPC/*.parquet)
    --output       S3 path to write to    (e.g. s3://lake/processed/HPPC/)
    --input-fmt    csv | parquet (default parquet on Glue)

The package itself must be uploaded as a Glue --extra-py-files zip:
    cd Characterization_Tests_RD
    zip -r post_processing.zip post_processing
    aws s3 cp post_processing.zip       s3://<lake>/glue/libs/
    aws s3 cp scripts/glue_main.py      s3://<lake>/glue/jobs/

…and referenced in the Glue job config:
    --extra-py-files s3://<lake>/glue/libs/post_processing.zip

Example job-parameter set for an HPPC run:
    --extra-py-files  s3://<lake>/glue/libs/post_processing.zip
    --job             hppc
    --input           s3://<lake>/raw/HPPC/*.parquet
    --output          s3://<lake>/processed/HPPC/
    --input-fmt       parquet
"""
from __future__ import annotations

import sys
from typing import Dict

from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]

from post_processing import build_spark
from post_processing.jobs import (
    run_hppc_job, run_ocv_job, run_dcir_job, run_cycle_job,
    run_gitt_job, run_rate_cap_job, run_self_discharge_job,
    run_peak_power_job, run_constant_power_job,
)


JOB_MAP = {
    "hppc":           run_hppc_job,
    "ocv":            run_ocv_job,
    "dcir":           run_dcir_job,
    "gitt":           run_gitt_job,
    "rate_cap":       run_rate_cap_job,
    "self_discharge": run_self_discharge_job,
    "peak_power":     run_peak_power_job,
    "constant_power": run_constant_power_job,
    "cycles_long":    run_cycle_job,
    "cycles_rpt":     run_cycle_job,
}


def main() -> None:
    args: Dict[str, str] = getResolvedOptions(
        sys.argv, ["JOB_NAME", "job", "input", "output", "input-fmt"])

    spark = build_spark(app_name=args["JOB_NAME"])
    runner = JOB_MAP[args["job"]]
    n = runner(spark, args["input"], args["output"], input_fmt=args["input-fmt"])
    print(f"Glue job {args['JOB_NAME']!r}: wrote {n:,} rows to {args['output']}")


if __name__ == "__main__":
    main()
