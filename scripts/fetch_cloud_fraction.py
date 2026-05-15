"""Fetch cloud fraction data from NASA MODIS for earthquake cloud analysis.

"Earthquake clouds" -- unusual linear cloud formations along fault lines --
have been reported before major earthquakes. While controversial, the
physical mechanism is plausible (LAIC: Lithosphere-Atmosphere-Ionosphere
Coupling): crustal stress -> radon/ion release -> atmospheric ionization
-> water vapor condensation nuclei -> linear cloud formation along fault.

Data source: NASA MODIS Terra L3 daily (1deg global grid)
    - Product: MOD08_D3 (Terra, Cloud_Fraction_Mean)
    - Auth: NASA Earthdata Login via earthaccess library

Auth history (2026-05-15):
    Original OPeNDAP path returned HTTP 200 + HTML (URS OAuth login page)
    because LAADS DAAC's /opendap/ and /archive/ paths do NOT honor
    Bearer tokens directly -- they require the full OAuth interactive
    flow with cookies. The earthaccess library handles this end-to-end
    via username/password credentials in EARTHDATA_USERNAME/EARTHDATA_PASSWORD.

References:
    - Guangmeng & Jie (2013) Nat. Hazards Earth Syst. Sci. 13:927-934
    - Shou (2006) Terr. Atmos. Ocean. Sci. 17:395-414
"""

import asyncio
import logging
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import earthaccess
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SHORT_NAME = "MOD08_D3"
VERSION = "61"

JAPAN_BBOX = (122.0, 24.0, 150.0, 46.0)

LAT_START = 114
LAT_END = 136
LON_START = 302
LON_END = 330

START_YEAR = 2000


async def init_cloud_table():
    import sqlite3 as _sqlite3
    async with safe_connect() as db:
        try:
            await db.execute("SELECT COUNT(*) FROM cloud_fraction")
        except _sqlite3.OperationalError as e:
            if "no such table" not in str(e).lower():
                raise
        except _sqlite3.DatabaseError as e:
            msg = str(e).lower()
            if (
                "malformed" in msg
                or "database disk image" in msg
                or "not a database" in msg
                or "corrupt" in msg
            ):
                logger.warning("cloud_fraction unreadable (%s) -- dropping to recover", e)
                await db.execute("DROP TABLE IF EXISTS cloud_fraction")
                await db.commit()
            else:
                raise
        await db.execute("""
            CREATE TABLE IF NOT EXISTS cloud_fraction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                cloud_frac REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cloud_time ON cloud_fraction(observed_at)
        """)
        await db.commit()


def _parse_cloud_hdf4(hdf_path: str, date_str: str) -> list:
    from pyhdf.SD import SD, SDC
    try:
        hdf = SD(hdf_path, SDC.READ)
        ds = hdf.select("Cloud_Fraction_Mean")
        data = ds[:]
        attrs = ds.attributes()
        scale = float(attrs.get("scale_factor", 0.0001))
        offset = float(attrs.get("add_offset", 0.0))
        fill = int(attrs.get("_FillValue", -9999))
        ds.endaccess()
        hdf.end()
    except Exception as e:
        logger.warning("Cloud %s: HDF parse failed: %s: %s", date_str, type(e).__name__, e)
        return []

    n_rows, n_cols = data.shape
    rows = []
    for row_idx in range(LAT_START, min(LAT_END + 1, n_rows)):
        lat = -89.5 + row_idx
        for lon_idx in range(LON_START, min(LON_END + 1, n_cols)):
            raw = int(data[row_idx, lon_idx])
            if raw == fill or raw < 0:
                continue
            val = (raw - offset) * scale
            if val > 1.1:
                continue
            lon = -179.5 + lon_idx
            rows.append({
                "date": date_str,
                "lat": round(lat, 1),
                "lon": round(lon, 1),
                "cloud_frac": round(val, 4),
            })
    return rows


def _date_from_filename(fname: str):
    if not fname.startswith("MOD08_D3.A"):
        return None
    try:
        yy = int(fname[10:14])
        doy = int(fname[14:17])
        return (datetime(yy, 1, 1) + timedelta(days=doy - 1)).date()
    except (ValueError, IndexError):
        return None


async def main():
    await init_cloud_table()
    await init_db()

    if not (os.environ.get("EARTHDATA_USERNAME") and os.environ.get("EARTHDATA_PASSWORD")):
        logger.info(
            "Cloud fraction fetch: EARTHDATA_USERNAME/PASSWORD not set; skipping. "
            "Cloud features will be excluded via dynamic feature selection."
        )
        return

    auth = earthaccess.login(strategy="environment")
    if not getattr(auth, "authenticated", False):
        logger.error("earthaccess login failed; aborting")
        return
    logger.info("earthaccess login OK")

    now = datetime.now(timezone.utc).isoformat()

    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM cloud_fraction"
        )
    existing_dates = set(r[0] for r in existing) if existing else set()

    today = datetime.now(timezone.utc).date()
    d = datetime(START_YEAR, 1, 1).date()
    target_dates = []
    while d < today:
        ds = d.strftime("%Y-%m-%d")
        if ds not in existing_dates:
            target_dates.append(d)
        d += timedelta(days=1)

    analysis_dates = [dt for dt in target_dates if dt.year >= 2011]
    backfill_dates = [dt for dt in target_dates if dt.year < 2011]
    max_dates = int(os.environ.get("CLOUD_MAX_DATES", "60"))
    dates_to_fetch = (analysis_dates + backfill_dates)[:max_dates]

    if not dates_to_fetch:
        logger.info("All cloud fraction target dates already fetched")
        return

    logger.info("Cloud fraction: %d dates to fetch (max=%d)", len(dates_to_fetch), max_dates)

    by_month = defaultdict(list)
    for dt in dates_to_fetch:
        by_month[(dt.year, dt.month)].append(dt)

    total_records = 0
    for (year, month), dates in sorted(by_month.items()):
        date_set = {d.strftime("%Y-%m-%d") for d in dates}
        start = min(dates).strftime("%Y-%m-%d")
        end = max(dates).strftime("%Y-%m-%d")
        try:
            granules = earthaccess.search_data(
                short_name=SHORT_NAME,
                version=VERSION,
                temporal=(start, end),
                bounding_box=JAPAN_BBOX,
            )
        except Exception as e:
            logger.warning("Cloud search %d-%02d failed: %s: %s", year, month, type(e).__name__, e)
            continue

        if not granules:
            logger.info("Cloud %d-%02d: no granules found", year, month)
            continue

        with tempfile.TemporaryDirectory(prefix="modcloud_") as tmpdir:
            try:
                paths = earthaccess.download(granules, tmpdir)
            except Exception as e:
                logger.warning("Cloud download %d-%02d failed: %s: %s", year, month, type(e).__name__, e)
                continue

            month_records = 0
            for p in paths:
                p_str = str(p)
                fname = os.path.basename(p_str)
                file_date = _date_from_filename(fname)
                if file_date is None:
                    continue
                date_str = file_date.strftime("%Y-%m-%d")
                if date_str not in date_set:
                    continue

                rows = _parse_cloud_hdf4(p_str, date_str)
                if rows:
                    async with safe_connect() as db:
                        await db.executemany(
                            """INSERT OR IGNORE INTO cloud_fraction
                               (observed_at, cell_lat, cell_lon, cloud_frac, received_at)
                               VALUES (?, ?, ?, ?, ?)""",
                            [(r["date"], r["lat"], r["lon"], r["cloud_frac"], now) for r in rows],
                        )
                        await db.commit()
                    month_records += len(rows)

                try:
                    os.unlink(p_str)
                except OSError:
                    pass

            total_records += month_records
            logger.info("Cloud %d-%02d: %d files, %d records (cumulative %d)",
                        year, month, len(paths), month_records, total_records)

    logger.info("Cloud fraction fetch complete: %d records across %d months",
                total_records, len(by_month))


if __name__ == "__main__":
    asyncio.run(main())
