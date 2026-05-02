"""One-off cleanup: remove DART buoy contamination from ioc_sea_level.

Background
----------
Before the Stage 2.A fix (PR introducing sensor allow-list filtering in
fetch_ioc_sealevel.py), the IOC SLSMF stationlist API was being read without
filtering by sensor type. DART buoys (sensor="prt") share the IOC station
namespace with coastal tide gauges and were being inserted into
ioc_sea_level.sea_level_m as if they were tide-gauge readings — but they
actually report ocean bottom pressure as water column height (slevel ~5779 m
for dtok), which is physically incompatible with coastal sea level (~1 m).

This contamination corrupted the ioc_sealevel_anomaly feature (computed as
AVG(sea_level_m) GROUP BY DATE without per-station weighting in
load_phase13_ioc_sealevel), so daily averages were dominated by the OBP rows.

What this script does
---------------------
1. Optionally lists affected rows in sqlite ioc_sea_level + BigQuery mirror
2. Deletes ioc_sealevel_failed_dates entries for the targeted station codes
   FIRST so a future cron will not re-attempt them
3. Deletes the contaminated ioc_sea_level rows in sqlite
4. Issues a matching DELETE in BigQuery (data-platform-490901.geohazard.ioc_sea_level)
5. Reports row counts before / after for both stores

Usage
-----
Default: dry-run, default station list (the 6 known DART stations in the
ioc_sea_level table as of 2026-05-02 BQ snapshot).

    python scripts/cleanup_ioc_sealevel_dart.py                 # dry run
    python scripts/cleanup_ioc_sealevel_dart.py --yes           # execute
    python scripts/cleanup_ioc_sealevel_dart.py --yes \\
        --station-codes dtok,dtok2 \\
        --min-date 2011-01-01 --max-date 2026-12-31

Run ONCE after the Stage 2.A PR is merged. Subsequent ml_prediction.py runs
re-derive anomaly features from the database directly, so the corrupted
features will self-heal on the next analysis cycle (no separate model
re-train required).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Repo path bootstrap to import db_connect and config the same way the
# fetcher modules do.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))

from db_connect import safe_connect  # type: ignore  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Default DART station codes observed in ioc_sea_level as of the 2026-05-02
# audit. Override with --station-codes for future incidents.
DEFAULT_DART_CODES = ["dtok", "dtok2", "dryu", "dryu2", "dsen", "drus"]

BQ_PROJECT = "data-platform-490901"
BQ_DATASET = "geohazard"
BQ_TABLE_DATA = "ioc_sea_level"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "One-off cleanup: remove DART buoy contamination from "
            "ioc_sea_level (sqlite + BigQuery). Default is dry-run; pass "
            "--yes to actually delete. See module docstring for context."
        ),
    )
    p.add_argument(
        "--yes", action="store_true",
        help="Actually execute DELETE. Without this flag the script is dry-run.",
    )
    p.add_argument(
        "--station-codes", default=",".join(DEFAULT_DART_CODES),
        help="Comma-separated IOC station codes to remove (default: known DART codes).",
    )
    p.add_argument(
        "--min-date", default=None,
        help="Restrict deletion to observed_at >= this YYYY-MM-DD (optional).",
    )
    p.add_argument(
        "--max-date", default=None,
        help="Restrict deletion to observed_at <= this YYYY-MM-DD (optional).",
    )
    p.add_argument(
        "--skip-bq", action="store_true",
        help="Skip BigQuery DELETE (sqlite-only cleanup).",
    )
    return p.parse_args()


def _build_sqlite_where(codes: list[str], min_date: str | None,
                        max_date: str | None) -> tuple[str, list]:
    where = "station_code IN ({})".format(",".join(["?"] * len(codes)))
    params: list = list(codes)
    if min_date:
        where += " AND observed_at >= ?"
        params.append(min_date)
    if max_date:
        where += " AND observed_at <= ?"
        params.append(max_date + "T23:59:59")
    return where, params


def _build_bq_where(codes: list[str], min_date: str | None,
                    max_date: str | None) -> str:
    quoted = ",".join(f"'{c}'" for c in codes)
    parts = [f"station_code IN ({quoted})"]
    if min_date:
        parts.append(f"observed_at >= '{min_date}'")
    if max_date:
        parts.append(f"observed_at <= '{max_date}T23:59:59'")
    return " AND ".join(parts)


async def _sqlite_inspect(codes: list[str], min_date: str | None,
                          max_date: str | None) -> dict:
    where, params = _build_sqlite_where(codes, min_date, max_date)
    async with safe_connect() as db:
        rows = await db.execute_fetchall(
            f"SELECT station_code, COUNT(*) FROM ioc_sea_level "
            f"WHERE {where} GROUP BY station_code ORDER BY station_code",
            params,
        )
        failed_rows = await db.execute_fetchall(
            f"SELECT station_code, COUNT(*) FROM ioc_sealevel_failed_dates "
            f"WHERE station_code IN ({','.join(['?'] * len(codes))}) "
            f"GROUP BY station_code",
            list(codes),
        )
    return {
        "data_per_station": [(r[0], r[1]) for r in rows],
        "failed_per_station": [(r[0], r[1]) for r in failed_rows],
    }


async def _sqlite_delete(codes: list[str], min_date: str | None,
                         max_date: str | None) -> tuple[int, int]:
    where, params = _build_sqlite_where(codes, min_date, max_date)
    async with safe_connect() as db:
        # failed_dates first so a concurrent fetcher cannot re-issue the
        # request between the two DELETEs.
        cur1 = await db.execute(
            f"DELETE FROM ioc_sealevel_failed_dates "
            f"WHERE station_code IN ({','.join(['?'] * len(codes))})",
            list(codes),
        )
        deleted_failed = cur1.rowcount
        cur2 = await db.execute(
            f"DELETE FROM ioc_sea_level WHERE {where}", params,
        )
        deleted_data = cur2.rowcount
        await db.commit()
    return deleted_data, deleted_failed


def _bq_delete(codes: list[str], min_date: str | None,
               max_date: str | None) -> int:
    """Issue BigQuery DELETE. Returns affected row count from the job stats."""
    import subprocess
    where = _build_bq_where(codes, min_date, max_date)
    sql = (
        f"DELETE FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_DATA}` "
        f"WHERE {where}"
    )
    logger.info("BigQuery DELETE SQL: %s", sql)
    cmd = [
        "bq", "query", "--nouse_legacy_sql", "--format=json",
        "--max_rows=1", sql,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=240)
    if proc.returncode != 0:
        logger.error("BigQuery DELETE failed: %s", proc.stderr)
        raise RuntimeError(f"bq query exited {proc.returncode}")
    logger.info("BigQuery DELETE stdout: %s", proc.stdout.strip()[:200])
    # `bq query` for DML returns no row count in stdout JSON, return 0
    # placeholder; operator should re-run an SELECT COUNT(*) afterwards.
    return 0


async def _amain() -> int:
    args = _parse_args()
    codes = [c.strip() for c in args.station_codes.split(",") if c.strip()]
    if not codes:
        logger.error("No station codes specified")
        return 2

    logger.info(
        "Targeting station_codes=%s, min_date=%s, max_date=%s, "
        "yes=%s, skip_bq=%s",
        codes, args.min_date, args.max_date, args.yes, args.skip_bq,
    )

    pre = await _sqlite_inspect(codes, args.min_date, args.max_date)
    pre_data_total = sum(c for _, c in pre["data_per_station"])
    pre_failed_total = sum(c for _, c in pre["failed_per_station"])
    logger.info(
        "[sqlite pre] ioc_sea_level rows targeted: %d (per-station: %s)",
        pre_data_total, pre["data_per_station"],
    )
    logger.info(
        "[sqlite pre] ioc_sealevel_failed_dates rows targeted: %d (per-station: %s)",
        pre_failed_total, pre["failed_per_station"],
    )

    if not args.yes:
        logger.warning("DRY RUN — pass --yes to actually delete.")
        return 0

    deleted_data, deleted_failed = await _sqlite_delete(
        codes, args.min_date, args.max_date,
    )
    logger.info(
        "[sqlite] deleted: %d data rows, %d failed_dates rows",
        deleted_data, deleted_failed,
    )

    if not args.skip_bq:
        _bq_delete(codes, args.min_date, args.max_date)
        logger.info("BigQuery DELETE issued. Verify with: "
                    "bq query \"SELECT station_code, COUNT(*) FROM "
                    "`%s.%s.%s` WHERE station_code IN (%s) "
                    "GROUP BY station_code\"",
                    BQ_PROJECT, BQ_DATASET, BQ_TABLE_DATA,
                    ",".join(f"'{c}'" for c in codes))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_amain()))
