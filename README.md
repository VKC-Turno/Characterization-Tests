# post_processing_script — PySpark post-processing for battery characterisation tests

Local-first PySpark package that re-implements the existing post-processing
logic (HPPC pulse identification, OCV/SOC curves, DCIR R0 anchors, per-cycle
aggregates) on top of Spark so the same code can run unchanged as an AWS
Glue job later.

**No AWS calls happen in this package.** The package reads from local CSVs
and writes to local Parquet. The `scripts/glue_main.py` file is an
entry-point stub for the eventual Glue deployment — it is never imported
by the local runner.

## Layout

```
post_processing_script/
├── post_processing/
│   ├── spark_session.py        # local + Glue-compatible session builder
│   ├── config.py               # input + output schemas (typed, explicit)
│   ├── io/
│   │   ├── readers.py          # name-aware CSV reader (handles missing cols)
│   │   └── writers.py          # partitioned Parquet writer
│   ├── transforms/             # one applyInPandas-style UDF per test
│   │   ├── hppc.py             # VKC pulse identification — R0/R1/R2/C1/C2
│   │   ├── ocv.py              # OCV(SoC) charge + discharge, 11 SoC anchors
│   │   ├── dcir.py             # DCIR R0 anchor(s)
│   │   ├── gitt.py             # long-pulse R + V_inf + τ_diff per anchor
│   │   ├── rate_cap.py         # Q vs C-rate, charge + discharge
│   │   ├── self_discharge.py   # ΔV/Δt drift, capacity retention
│   │   ├── peak_power.py       # P_max envelope per SoC, per direction
│   │   ├── constant_power.py   # energy + time-to-cutoff per P set-point
│   │   └── cycle_agg.py        # per-(cell, cycle) capacity / V / CE (Longterm + RPT)
│   └── jobs/                   # one orchestration entry-point per test
│       ├── hppc_job.py
│       ├── ocv_job.py
│       ├── dcir_job.py
│       ├── gitt_job.py
│       ├── rate_cap_job.py
│       ├── self_discharge_job.py
│       ├── peak_power_job.py
│       ├── constant_power_job.py
│       └── cycle_job.py        # shared by cycles_long + cycles_rpt
├── scripts/
│   ├── run_local.py            # local CLI runner (zips package + ships to workers)
│   └── glue_main.py            # AWS Glue entry-point stub
├── tests/                      # (placeholder for pytest fixtures)
├── requirements.txt
└── README.md
```

## Test coverage

All 10 raw test folders under `Data/` have a transform + job + dashboard view:

| Test            | Raw folder     | Job key          | Output table     | Row shape                          |
|-----------------|----------------|------------------|------------------|------------------------------------|
| HPPC            | HPPC           | `hppc`           | `HPPC/`          | one row per pulse                  |
| OCVSOC          | OCVSOC         | `ocv`            | `OCV/`           | one row per (cell, direction, SoC) |
| DCIR            | DCIR           | `dcir`           | `DCIR/`          | one row per R0 anchor              |
| GITT            | GITT           | `gitt`           | `GITT/`          | one row per pulse anchor           |
| RateCapability  | RateCapability | `rate_cap`       | `RATE_CAP/`      | one row per (cell, direction, C)   |
| SelfDischarge   | SelfDischarge  | `self_discharge` | `SELF_DISCHARGE/`| one row per cell                   |
| PeakPower       | PeakPower      | `peak_power`     | `PEAK_POWER/`    | one row per (cell, direction, SoC) |
| ConstantPower   | ConstantPower  | `constant_power` | `CONSTANT_POWER/`| one row per (cell, direction, P)   |
| Longterm        | Longterm       | `cycles_long`    | `CYCLES_LONG/`   | one row per (cell, cycle)          |
| RPT             | RPT            | `cycles_rpt`     | `CYCLES_RPT/`    | one row per (cell, cycle)          |

## Output layout (Hive-style, Athena-ready)

```
post_processing_script/output/<TEST>/make=<X>/batch=<Y>/part-*.snappy.parquet
```

Querying with DuckDB / Athena partition-prunes automatically:

```sql
SELECT * FROM read_parquet('output/HPPC/**/*.parquet', hive_partitioning=true)
WHERE make = 'EVE' AND batch = '1' AND cell_no = '0002';
```

## Local run

### Requirements
- Python 3.10+
- **Java 17** (NOT 21 — Spark 3.5 + Java 21 hits an Arrow `sun.misc.Unsafe` issue)
- Disk space for the output Parquet (HPPC: ~50 KB per cell)

If you only have Java 21 installed, you can grab a portable JDK 17 with no sudo:

```bash
curl -fsSL https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz \
  | tar -xz -C ~/
export JAVA_HOME=~/jdk-17.0.2
export PATH=$JAVA_HOME/bin:$PATH
```

### Install
```bash
cd /home/hj/Desktop/PINNs
.venv/bin/python -m pip install -r post_processing_script/requirements.txt
```

### Run one test
```bash
# valid --job values:
#   hppc | ocv | dcir | gitt | rate_cap | self_discharge
#   peak_power | constant_power | cycles_long | cycles_rpt
.venv/bin/python post_processing_script/scripts/run_local.py --job hppc
.venv/bin/python post_processing_script/scripts/run_local.py --job gitt
.venv/bin/python post_processing_script/scripts/run_local.py --job cycles_long
```

### Run all tests
```bash
.venv/bin/python post_processing_script/scripts/run_local.py --job all
```

### Custom input/output paths
```bash
.venv/bin/python post_processing_script/scripts/run_local.py \
    --job hppc \
    --input  'Data/HPPC/EVE_*.csv' \
    --output /tmp/eve_only_pulses
```

## Verifying correctness

The HPPC transform is a 1:1 port of the working VKC implementation at
[characterization_results/_hppc_pulse_id.py](../characterization_results/_hppc_pulse_id.py).
Same constants (`FLAT_THRESHOLD_FRACTION=0.008`, `FLAT_WINDOW=5`), same SoC
convention (`pulse N → N × 10 % SoC`), same R0/R1/R2/C1/C2 formulas.

Smoke-test output against the existing reference CSV:

```bash
.venv/bin/python -c "
import pandas as pd
from pathlib import Path
new = pd.concat([pd.read_parquet(p) for p in
                 Path('post_processing_script/output/HPPC').glob('**/*.parquet')])
ref = pd.read_csv('characterization_results/outputs/hppc_consolidated_vkc_soc.csv')
print(f'rows: pyspark={len(new)} reference={len(ref)}')
"
```

Both should print **99 rows** for the current 11-cell HPPC dataset.

## Deploying to AWS Glue

The package is structured so that swap-in to Glue is mechanical:

### 1. Zip the package
```bash
cd post_processing_script
zip -r post_processing.zip post_processing
```

### 2. Upload zip + entrypoint to S3
```bash
aws s3 cp post_processing.zip            s3://<lake>/glue/libs/
aws s3 cp scripts/glue_main.py           s3://<lake>/glue/jobs/
```

### 3. Create a Glue 4.0 job

| Setting               | Value                                              |
|-----------------------|----------------------------------------------------|
| Type                  | Spark                                              |
| Glue version          | 4.0 (Spark 3.3 / Python 3.10 / Java 17)            |
| Script path           | `s3://<lake>/glue/jobs/glue_main.py`               |
| `--extra-py-files`    | `s3://<lake>/glue/libs/post_processing.zip`        |
| `--job`               | `hppc`  (or `ocv` / `dcir` / `cycles_long`)        |
| `--input`             | `s3://<lake>/raw/HPPC/*.parquet`                   |
| `--output`            | `s3://<lake>/processed/HPPC/`                      |
| `--input-fmt`         | `parquet`                                          |
| Worker type           | G.1X (4 vCPU / 16 GB)                              |
| Number of workers     | 2 for HPPC; 10 for a longterm sweep                |

### 4. Schedule via EventBridge → Glue trigger
A `rate(1 day)` trigger fired by EventBridge re-processes the prior day's
raw exports — partition overwrite mode means only the touched
(`make`, `batch`) partitions are rewritten.

## Key design decisions

| Decision                          | Reason                                                                                       |
|-----------------------------------|----------------------------------------------------------------------------------------------|
| `applyInPandas` over Spark SQL UDF | HPPC pulse detection is per-cell stateful (cumulative pulse counter, V-fast search) — pandas-UDF gives each cell its own DataFrame |
| Partition by `(make, batch)`       | Matches our cohort/batch query pattern; small file counts; Athena partition-prune works      |
| `snappy` compression               | Glue default; cheap CPU; ~3× smaller than CSV                                                |
| `partitionOverwriteMode=dynamic`   | Re-running one batch doesn't clobber sibling batches                                         |
| Pin `PYSPARK_PYTHON` locally       | Workers need same numpy/pandas/pyarrow as the driver — venv interpreter is the obvious choice |
| Java 17 `--add-opens` flags        | Arrow needs internal-module access on JDK 17+; matches Glue 4.0 runtime                      |
| `_JAVA_OPTIONS` (not `extraJavaOptions`) | Local-mode driver JVM starts before SparkConf is read; only env vars apply in time      |

## What's intentionally NOT here

- No `boto3` / `awswrangler` — we never call AWS at runtime from this package
- No S3 paths in code (only in docstrings as examples)
- No state mutation, no DB writes — outputs are file-only
- No interactive notebooks — those live in `characterization_results/notebooks/`

## Smoke-test results (current Data/)

The latest end-to-end `--job all` run against the cells in `Data/`:

| Test            | Rows written | Notes                                                      |
|-----------------|-------------:|------------------------------------------------------------|
| HPPC            | 99           | 9 pulses × 11 cells; matches existing VKC reference CSV    |
| OCV             | 176          | 8 cells × 11 SoC × 2 directions                            |
| DCIR            | 8            | 1 anchor per cell (single-discharge protocol)              |
| GITT            | 626          | per-cell pulse counts vary; ordinal SoC labelling          |
| RateCapability  | 42           |                                                            |
| SelfDischarge   | 4            | 4 cells with completed self-discharge runs                 |
| PeakPower       | 8            |                                                            |
| ConstantPower   | 36           |                                                            |
| Longterm        | 5,242        | dominant parquet by row count                              |
| RPT             | 40           |                                                            |

## What still needs to be added

- A `tests/` directory with pytest fixtures that run a tiny synthetic
  dataset through every job (currently the smoke test is the real Data/)
- Cohort-aware partition keys (see "S3 layout" thread — `cohort=` vs `make=`)
