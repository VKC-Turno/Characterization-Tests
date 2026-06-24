"""Pull HPPC raw cycling data from Athena for any (make, batch, cell).

Generalises the earlier `_pull_rept_hppc.py`. Adds the native cycler `"step no"`
column so pulse identification can use it directly (matching VKC's algorithm
exactly) instead of deriving step boundaries from name transitions.

Output: Data/HPPC/<MAKE>_HPPC_cell_<id>.csv (overwrites if exists).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _setup_aws() -> None:
    for d in (Path.home() / ".aws", Path.home() / "Desktop" / "AWS"):
        if (d / "credentials").exists():
            os.environ.setdefault("AWS_CONFIG_FILE", str(d / "config"))
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", str(d / "credentials"))
            return


def pull_cell(make: str, batch: str, cell_id: str, out_dir: Path,
              profile: str = "battery-turno", region: str = "ap-south-1") -> Path | None:
    import boto3
    import awswrangler as wr

    sess = boto3.Session(profile_name=profile, region_name=region)
    sql = """
        SELECT
            cell                  AS cell_no,
            "cycle no"            AS cycle_no,
            "step no"             AS cycler_step_no,
            "step name"           AS step_name,
            "absolute time"       AS absolute_time,
            "volt(v)"             AS volt_v,
            "current(a)"          AS current_a,
            "capacity(ah)"        AS capacity_ah,
            crate, drate, dod, max_cap, test, make, batch
        FROM detail
        WHERE make = :make
          AND test = 'HPPC'
          AND batch = :batch
          AND cell = :cell_no
        ORDER BY "absolute time"
    """
    df = wr.athena.read_sql_query(
        sql=sql, database="rd_ts_cell_database",
        boto3_session=sess,
        params={"make": make, "batch": batch, "cell_no": cell_id},
        paramstyle="named",
    )
    if df.empty:
        print(f"  ! {make}/{cell_id}/batch={batch}: 0 rows (skipped)")
        return None

    # Coerce arrow-backed types to numpy-friendly ones for downstream consumers
    df["step_name"] = df["step_name"].astype(str)
    df["cycler_step_no"] = df["cycler_step_no"].astype("Int64")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{make}_HPPC_cell_{cell_id}.csv"
    df.to_csv(out_path, index=False)
    print(f"  ✓ {make}_{cell_id} batch={batch}: {len(df):,} rows, "
          f"{df['cycler_step_no'].nunique()} cycler steps "
          f"-> {out_path.name} ({out_path.stat().st_size / 1e6:.1f} MB)")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--make", required=True)
    ap.add_argument("--batch", required=True)
    ap.add_argument("--cell", required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("Data/HPPC"))
    args = ap.parse_args()

    _setup_aws()
    pull_cell(args.make, args.batch, args.cell, args.out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
