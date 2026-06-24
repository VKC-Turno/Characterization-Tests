"""Pull REPT batch-1 HPPC raw data from Athena, mirroring the EVE schema.

Runs from the repo root. AWS creds are auto-discovered from ~/Desktop/AWS/.

Output: Data/HPPC/REPT_HPPC_cell_<id>.csv with columns:
  cell_no, cycle_no, step_name, absolute_time, volt_v, current_a,
  capacity_ah, crate, drate, dod, max_cap, test, make, batch
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


REPT_BATCH1_CELLS = [
    "0001", "0003", "0004", "0007", "0011", "0012", "0025", "0034",
    "0040", "0043", "0046", "0049", "0050", "0056", "0057", "0065",
    "0074", "0078", "0080", "0087", "0090",
]


def _setup_aws() -> None:
    for d in (Path.home() / ".aws", Path.home() / "Desktop" / "AWS"):
        if (d / "credentials").exists():
            os.environ.setdefault("AWS_CONFIG_FILE", str(d / "config"))
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", str(d / "credentials"))
            return


def pull_cell(cell_id: str, out_dir: Path, profile: str = "battery-turno",
              region: str = "ap-south-1") -> Path | None:
    import boto3
    import awswrangler as wr

    sess = boto3.Session(profile_name=profile, region_name=region)
    sql = """
        SELECT
            cell                AS cell_no,
            "cycle no"          AS cycle_no,
            "step name"         AS step_name,
            "absolute time"     AS absolute_time,
            "volt(v)"           AS volt_v,
            "current(a)"        AS current_a,
            "capacity(ah)"      AS capacity_ah,
            crate, drate, dod, max_cap, test, make, batch
        FROM detail
        WHERE make = 'REPT'
          AND test = 'HPPC'
          AND batch = '1'
          AND cell = :cell_no
        ORDER BY "absolute time"
    """
    df = wr.athena.read_sql_query(
        sql=sql, database="rd_ts_cell_database",
        boto3_session=sess,
        params={"cell_no": cell_id}, paramstyle="named",
    )
    if df.empty:
        print(f"  ! {cell_id}: 0 rows returned (skipping)")
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"REPT_HPPC_cell_{cell_id}.csv"
    df.to_csv(out_path, index=False)
    print(f"  ✓ {cell_id}: {len(df):,} rows -> {out_path.name} "
          f"({out_path.stat().st_size / 1e6:.1f} MB)")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cells", nargs="*", default=None,
                    help="Cell IDs to pull (default: all 21 batch-1 cells)")
    ap.add_argument("--out-dir", type=Path, default=Path("Data/HPPC"))
    args = ap.parse_args()

    _setup_aws()
    cells = args.cells or REPT_BATCH1_CELLS
    print(f"Pulling {len(cells)} REPT batch-1 HPPC cells -> {args.out_dir}")
    total_mb = 0.0
    for cell_id in cells:
        p = pull_cell(cell_id, args.out_dir)
        if p is not None:
            total_mb += p.stat().st_size / 1e6
    print(f"\nDone. Total written: {total_mb:.0f} MB across {len(cells)} cells.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
