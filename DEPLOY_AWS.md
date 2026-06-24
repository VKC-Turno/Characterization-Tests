# Deploying Characterization_Tests_RD on AWS Glue

End-to-end recipe to run the PySpark characterisation pipeline as a managed
AWS Glue job. The same script that runs locally via `scripts/run_local.py`
runs on Glue via `scripts/glue_main.py` — no code changes needed.

## Prerequisites

- An AWS account with permissions to create IAM roles, S3 buckets, Glue jobs,
  and (optionally) EventBridge rules
- AWS CLI installed and configured (`aws configure` with a profile that has
  the above permissions)
- The Characterization_Tests_RD repo checked out locally

## Architecture at a glance

```
┌────────────────┐    ┌──────────────────┐    ┌────────────────────┐
│ Athena pullers │───▶│  Raw CSV in S3   │───▶│  Glue Spark job    │
│ (scripts/_pull*)│    │  s3://<lake>/raw │    │  scripts/glue_main │
└────────────────┘    └──────────────────┘    └─────────┬──────────┘
                                                        │
                                                        ▼
                                         ┌──────────────────────────────────┐
                                         │ Hive-partitioned parquet in S3   │
                                         │ s3://<lake>/processed/<TEST>/    │
                                         │   make=<X>/batch=<Y>/*.parquet   │
                                         └──────────────────────────────────┘
                                                        │
                                       ┌────────────────┼─────────────┐
                                       ▼                              ▼
                              ┌────────────────┐         ┌──────────────────────┐
                              │ Athena queries │         │ Dashboard (Streamlit)│
                              └────────────────┘         │ pointed at s3://     │
                                                         └──────────────────────┘
```

Throughout this guide, replace `<lake>` with your S3 bucket name (e.g. `turno-lab`).

---

## Step 1 — Create the S3 bucket and layout

If the lake bucket doesn't exist yet:

```bash
aws s3 mb s3://<lake> --region <region>
```

Conventional folder layout used by this pipeline:

```
s3://<lake>/
├── raw/                    ← input CSVs (uploaded by Athena pullers)
│   ├── HPPC/
│   ├── OCVSOC/
│   └── ...                 ← one folder per test
├── processed/              ← output parquet, written by Glue
│   ├── HPPC/
│   │   ├── make=EVE/batch=1/*.snappy.parquet
│   │   └── make=REPT/batch=1/*.snappy.parquet
│   └── ...
└── glue/
    ├── jobs/glue_main.py   ← deployed entry-point
    └── libs/post_processing.zip ← deployed package
```

## Step 2 — Create the IAM role for Glue

Glue jobs need an IAM role with:
- `AWSGlueServiceRole` managed policy
- Read/write access to `s3://<lake>/`
- (If reading from Athena pullers' CSV drop) read on `s3://<lake>/raw/`
- CloudWatch Logs write (for job logs)

Quick setup via CLI:

```bash
ROLE_NAME=GlueCharacterizationRole

# Trust policy: allow Glue to assume the role
cat > /tmp/glue-trust.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
  --role-name ${ROLE_NAME} \
  --assume-role-policy-document file:///tmp/glue-trust.json

aws iam attach-role-policy --role-name ${ROLE_NAME} \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Bucket-scoped S3 policy
cat > /tmp/glue-s3.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
    "Resource": ["arn:aws:s3:::<lake>","arn:aws:s3:::<lake>/*"]
  }]
}
EOF

aws iam put-role-policy --role-name ${ROLE_NAME} \
  --policy-name LakeAccess --policy-document file:///tmp/glue-s3.json
```

## Step 3 — Package the code and upload to S3

The PySpark transforms ship as a single zip that Glue mounts via `--extra-py-files`.

```bash
cd Characterization_Tests_RD

# 1. Zip the importable package (NOT the whole repo — just `post_processing/`)
zip -r post_processing.zip post_processing

# 2. Upload package + entry-point
aws s3 cp post_processing.zip            s3://<lake>/glue/libs/
aws s3 cp scripts/glue_main.py           s3://<lake>/glue/jobs/
```

Re-run these two `aws s3 cp` commands whenever you edit code — Glue refetches them on each job invocation.

## Step 4 — Create the Glue job (one per test)

You'll create one Glue job per `--job` value. Below is the recipe for HPPC; repeat with the right `--job` name for the other 9 tests.

### Via AWS Console
1. Glue → Jobs → Add Job
2. **Name:** `characterization-hppc`
3. **IAM role:** `GlueCharacterizationRole` (from Step 2)
4. **Type:** Spark
5. **Glue version:** 4.0 (Spark 3.3, Python 3.10, Java 17)
6. **Worker type:** G.1X (4 vCPU / 16 GB)
7. **Number of workers:** 2 (HPPC is small; use 10 for `cycles_long`)
8. **Script path:** `s3://<lake>/glue/jobs/glue_main.py`
9. **Job parameters** (Advanced properties → Job parameters):

   | Key | Value |
   |---|---|
   | `--extra-py-files` | `s3://<lake>/glue/libs/post_processing.zip` |
   | `--job` | `hppc` |
   | `--input` | `s3://<lake>/raw/HPPC/*.csv` |
   | `--output` | `s3://<lake>/processed/HPPC/` |
   | `--input-fmt` | `csv` (or `parquet` if your raw drop is parquet) |

### Via AWS CLI

```bash
JOB_NAME=characterization-hppc
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws glue create-job --name ${JOB_NAME} \
  --role arn:aws:iam::${ACCOUNT_ID}:role/GlueCharacterizationRole \
  --command "Name=glueetl,ScriptLocation=s3://<lake>/glue/jobs/glue_main.py,PythonVersion=3" \
  --glue-version 4.0 \
  --number-of-workers 2 --worker-type G.1X \
  --default-arguments '{
    "--extra-py-files": "s3://<lake>/glue/libs/post_processing.zip",
    "--job":             "hppc",
    "--input":           "s3://<lake>/raw/HPPC/*.csv",
    "--output":          "s3://<lake>/processed/HPPC/",
    "--input-fmt":       "csv",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-metrics":  "true"
  }'
```

### Sizing guide per test

| `--job`           | Worker type | # workers | Approx wall-time |
|-------------------|-------------|-----------|------------------|
| `hppc`            | G.1X        | 2         | < 2 min          |
| `ocv` / `dcir` / `gitt` | G.1X  | 2         | < 2 min          |
| `rate_cap` / `self_discharge` / `peak_power` / `constant_power` | G.1X | 2 | < 2 min |
| `cycles_rpt`      | G.1X        | 2         | < 2 min          |
| `cycles_long`     | G.1X        | 10        | 5–10 min         |

Start small; scale up only if you hit OOMs in the worker logs.

## Step 5 — Run a job and watch it

```bash
JOB_NAME=characterization-hppc

# Start
RUN_ID=$(aws glue start-job-run --job-name ${JOB_NAME} --query JobRunId --output text)
echo "Started run: ${RUN_ID}"

# Poll until done
while true; do
  STATE=$(aws glue get-job-run --job-name ${JOB_NAME} --run-id ${RUN_ID} \
          --query JobRun.JobRunState --output text)
  echo "$(date +%H:%M:%S)  ${STATE}"
  [[ "${STATE}" =~ ^(SUCCEEDED|FAILED|STOPPED|TIMEOUT)$ ]] && break
  sleep 30
done

# Tail logs
aws glue get-job-run --job-name ${JOB_NAME} --run-id ${RUN_ID} \
  --query 'JobRun.[StartedOn,CompletedOn,ExecutionTime,ErrorMessage]' --output table
```

Job-level logs land in CloudWatch under `/aws-glue/jobs/output`; executor stderr is in `/aws-glue/jobs/error`.

## Step 6 — Verify the output partition

```bash
aws s3 ls s3://<lake>/processed/HPPC/ --recursive | head -20
```

You should see Hive-partitioned files:
```
processed/HPPC/make=CALB/batch=2/part-00000-...snappy.parquet
processed/HPPC/make=EVE/batch=1/part-00000-...snappy.parquet
processed/HPPC/make=REPT/batch=1/part-00000-...snappy.parquet
processed/HPPC/make=REPT/batch=2/part-00000-...snappy.parquet
```

## Step 7 — (Optional) Register in Athena for SQL queries

The Hive-style partitions are Athena-native. Run once in the Athena query editor:

```sql
CREATE EXTERNAL TABLE rd_ts_cell_database.hppc_pulses (
  cell_no    string,
  cycle_no   int,
  pulse_idx  int,
  step_no    int,
  duration_s double,
  i_step     double,
  soc_start  double,
  v_pre      double,
  v_post     double,
  v_fast     double,
  v_end      double,
  tau1_s     double,
  tau2_s     double,
  r0_mohm    double,
  r1_mohm    double,
  r2_mohm    double,
  c1_f       double,
  c2_f       double,
  r_30s_mohm double
)
PARTITIONED BY (make string, batch string)
STORED AS PARQUET
LOCATION 's3://<lake>/processed/HPPC/';

MSCK REPAIR TABLE rd_ts_cell_database.hppc_pulses;
```

Then query like any table:

```sql
SELECT make, batch, cell_no, AVG(r0_mohm) AS r0_avg_mohm
FROM   rd_ts_cell_database.hppc_pulses
WHERE  make = 'EVE'
GROUP  BY make, batch, cell_no
ORDER  BY cell_no;
```

Repeat this DDL for each test's table (`ocv_curves`, `dcir_anchors`, `gitt_pulses`, …); see `post_processing/config.py` for each table's column list.

## Step 8 — Schedule the daily run

Glue has a built-in scheduler called **Triggers** that's the simplest way to run a
job on a schedule. One trigger can fire one or many jobs; the cron syntax matches
EventBridge. No extra IAM setup needed — Glue uses the job's own role.

### Option A — One trigger per job (granular control, retry independently)

```bash
JOB_NAME=characterization-hppc

aws glue create-trigger \
  --name  daily-${JOB_NAME} \
  --type  SCHEDULED \
  --schedule "cron(0 2 * * ? *)" \
  --actions "JobName=${JOB_NAME}" \
  --start-on-creation
```

The cron format is AWS-specific (6 fields, day-of-week uses `?` as wildcard):
`cron(<min> <hour> <day-of-month> <month> <day-of-week> <year>)`. Examples:
- `cron(0 2 * * ? *)`     — every day at 02:00 UTC
- `cron(0 2 ? * MON-FRI *)` — weekdays at 02:00 UTC
- `cron(0 */6 * * ? *)`   — every 6 hours

### Option B — One trigger fires all 10 jobs (easier ops, single schedule to manage)

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws glue create-trigger \
  --name  daily-characterization-suite \
  --type  SCHEDULED \
  --schedule "cron(0 2 * * ? *)" \
  --actions \
    'JobName=characterization-hppc' \
    'JobName=characterization-ocv' \
    'JobName=characterization-dcir' \
    'JobName=characterization-gitt' \
    'JobName=characterization-rate_cap' \
    'JobName=characterization-self_discharge' \
    'JobName=characterization-peak_power' \
    'JobName=characterization-constant_power' \
    'JobName=characterization-cycles_rpt' \
    'JobName=characterization-cycles_long' \
  --start-on-creation
```

All 10 actions fire in parallel when the cron condition matches. They run
independently — if one fails, the others still run.

### Inspect, pause, or delete

```bash
# List all schedule triggers
aws glue get-triggers --query 'Triggers[?Type==`SCHEDULED`].[Name,State,Schedule]' --output table

# Pause without deleting (state goes to DEACTIVATED)
aws glue stop-trigger --name daily-characterization-suite

# Resume
aws glue start-trigger --name daily-characterization-suite

# Delete
aws glue delete-trigger --name daily-characterization-suite
```

You can also create/edit triggers via the Glue console: **Glue → Triggers → Add Trigger**.

### Optional: chained triggers (job B runs after job A succeeds)

If you want `cycles_long` to run only after `hppc` succeeds (e.g. to chain on shared
upstream input freshness), use a `CONDITIONAL` trigger instead:

```bash
aws glue create-trigger --name after-hppc-run-cycles \
  --type CONDITIONAL \
  --predicate 'Logical=AND,Conditions=[{JobName=characterization-hppc,LogicalOperator=EQUALS,State=SUCCEEDED}]' \
  --actions 'JobName=characterization-cycles_long' \
  --start-on-creation
```

For the typical "refresh everything nightly" pattern, the parallel scheduled
trigger (Option B) is enough.

### Why this works idempotently

Glue's `partitionOverwriteMode = dynamic` (set in `spark_session.py`) means daily
re-runs only rewrite the `(make, batch)` partitions that the source data actually
touched — old partitions are preserved. Safe to re-run; safe to backfill.

### When to use EventBridge instead

Glue Triggers handle every cron-based schedule. Use EventBridge only when:
- You want to fire the Glue job from a **non-time event** — e.g. "S3 object created
  in `s3://<lake>/raw/`"
- You want to chain across services Glue Triggers can't reach (Step Functions,
  Lambda, SNS, etc.)

In both cases EventBridge → Glue job is one rule + one target, no IAM gymnastics.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'post_processing'` | Forgot `--extra-py-files` | Add the zip path to job parameters |
| `Function 'equal' has no kernel matching input types (int32, string)` | Reading old partitioned parquet with mismatched type | Drop & re-create the Athena table with the new column types |
| Empty output parquet | `--input` glob doesn't match any objects | Check `aws s3 ls <input-path>` |
| Job runs forever on `cycles_long` | Too few workers; trying to shuffle 5M+ rows | Bump `--number-of-workers` to 10+; consider G.2X |
| `java.lang.OutOfMemoryError: GC overhead limit exceeded` | One cell partition is huge; per-cell `applyInPandas` UDF runs out of memory | Switch worker type to `G.2X` or split input by date |
| Cost suddenly spikes | Long-running `cycles_long` re-processing the full Longterm dataset | Use partitioned overwrite by date or filter input to recent days only |

## Cost notes

Glue 4.0 G.1X = ~$0.44/DPU-hour, billed per second with a 1-min minimum. Typical run costs:

| Job | DPU-hours/run | Cost (USD) |
|---|---|---|
| Any of `hppc`, `ocv`, `dcir`, `gitt`, `rate_cap`, `self_discharge`, `peak_power`, `constant_power`, `cycles_rpt` | 2 workers × ~2 min = 0.07 DPU-h | ~$0.03 |
| `cycles_long` | 10 workers × ~7 min = 1.2 DPU-h | ~$0.50 |
| **All 10 jobs / day** | ~1.9 DPU-h | **~$0.80/day** |

Athena scan cost on the output table is ~$5 per TB scanned — partition prune on `make`+`batch` keeps each interactive query cheap (typically < 1 GB scanned).

## What's not in this recipe

- Glue workflows / triggers chaining multiple jobs (use `aws glue create-workflow`)
- Glue catalog vs Athena's data catalog (they're the same; the DDL above registers in both)
- Step Functions orchestration if you need cross-account or cross-region fan-out
- VPC-attached Glue (only needed if your S3 is behind a VPC endpoint)

For those, the AWS docs are the single source of truth — this guide stops at the point where the pipeline is producing partitioned parquet that downstream consumers can read.
