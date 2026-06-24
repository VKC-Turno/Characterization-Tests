# post_processing_dashboard — Streamlit dashboard

Interactive viewer for the parquet outputs of
[`post_processing_script/`](../post_processing_script/). Works against either
the local output directory or an S3 path once Glue is wired up.

## Layout

```
post_processing_dashboard/
├── app.py                          # Streamlit entry-point (page router + sidebar)
├── data_loader.py                  # pyarrow.dataset reader with partition prune
├── views/
│   ├── hppc_view.py                # R0/R1/R2/C1/C2 vs SoC; cohort compare
│   ├── ocv_view.py                 # OCV(SoC) charge / discharge overlay
│   ├── dcir_view.py                # R0 anchors per cell
│   ├── gitt_view.py                # R_pulse / τ_diff / V_inf vs SoC
│   ├── rate_cap_view.py            # Q + energy vs C-rate
│   ├── self_discharge_view.py      # drift rate + retention bars
│   ├── peak_power_view.py          # P_peak vs SoC, V at peak
│   ├── constant_power_view.py      # energy + time-to-cutoff vs P
│   └── cycles_view.py              # SoH trajectory, capacity, CE
├── requirements.txt
└── README.md
```

## Install

```bash
cd /home/hj/Desktop/PINNs
.venv/bin/python -m pip install -r post_processing_dashboard/requirements.txt
```

## Run

From the repo root:

```bash
.venv/bin/python -m streamlit run post_processing_dashboard/app.py
```

Open <http://localhost:8501>. Use the sidebar's **Data root** field to point at
a different output directory; defaults to `post_processing_script/output/`.

Override the default via env var:

```bash
POST_PROCESSING_OUTPUT=/tmp/eve_only \
  .venv/bin/python -m streamlit run post_processing_dashboard/app.py
```

## Views

| View            | What it shows                                                                                  |
|-----------------|------------------------------------------------------------------------------------------------|
| Summary         | One row per test with cells / makes / batches present in the parquet                           |
| HPPC            | (a) R0/R1/R2/C1/C2/R30s vs SoC for one cell, (b) cohort-compare one param, (c) raw pulse table |
| OCV             | V_OC vs SoC, charge & discharge dashed; multi-cell overlay                                     |
| DCIR            | R0 vs SoC anchors, per cell                                                                    |
| GITT            | R_pulse, τ_diff, V_inf vs SoC — three side-by-side panels                                      |
| Rate cap.       | Q + energy vs C-rate; charge & discharge dashed                                                |
| Self-discharge  | Drift rate (mV/h) + capacity retention bars per cell                                           |
| Peak power      | P_peak + V at peak vs SoC, per direction                                                       |
| Constant power  | Energy + time-to-cutoff vs power set-point, per direction                                      |
| Per-cycle       | SoH trajectory with EOL=0.80 reference, discharge capacity, coulombic efficiency               |

## Pointing at S3

Once the Glue job lands parquet at, e.g.,
`s3://turno-lab/processed/`, set the data root to that URI. `pyarrow.dataset`
handles S3 natively when `pyarrow` is built with S3 support and AWS creds are
available (same `AWS_PROFILE=battery-turno` you already use for Athena pulls).

```bash
AWS_PROFILE=battery-turno \
POST_PROCESSING_OUTPUT=s3://turno-lab/processed \
  .venv/bin/python -m streamlit run post_processing_dashboard/app.py
```

No code changes needed.

## Cache behaviour

`data_loader.py` decorates the parquet readers with `@st.cache_data`, so
repeat selections of the same `(make, batch, cell)` reuse the in-memory
table. Re-run a job (which overwrites the parquet) → click "Rerun" or
press `R` in the browser to bust the cache.

## What's intentionally NOT here

- No write paths — the dashboard is read-only by design
- No PySpark dependency — the dashboard reads parquet with pyarrow only,
  so you can run it on a laptop without setting up a JVM
- No tight coupling to `post_processing_script` imports — only file-layout
  contract (Hive partitions + schemas in `config.py`)
