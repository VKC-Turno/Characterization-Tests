"""PySpark post-processing pipeline for battery characterisation tests.

Designed to run identically:
  - locally (laptop / EC2) against `Data/` on the local filesystem
  - on AWS Glue (Spark 3.3+) against an S3 data lake

All transforms are pure PySpark + pandas UDFs; no AWS SDK or service-specific
code lives in this package.
"""

from .spark_session import build_spark
from .config import RAW_SCHEMA, PULSE_SCHEMA

__all__ = ["build_spark", "RAW_SCHEMA", "PULSE_SCHEMA"]
