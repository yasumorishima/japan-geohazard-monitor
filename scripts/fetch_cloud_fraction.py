"""Fetch cloud fraction data from NASA MODIS for earthquake cloud analysis.

"Earthquake clouds" — unusual linear cloud formations along fault lines —
have been reported before major earthquakes. While controversial, the
physical mechanism is plausible:

Physical mechanism:
    Crustal stress → radon/ion release from faults → atmospheric ionization
    → water vapor condensation nuclei → linear cloud formation along
    fault trace. This is part of the LAIC (Lithosphere-Atmosphere-Ionosphere
    Coupling) model.

    Statistical analysis with satellite cloud fraction data can objectively
    test whether cloud patterns anomalies occur before earthquakes.

Data source: NASA MODIS Terra/Aqua Level 3 daily (1° global grid)
    - Product: MOD08_D3 (Terra) / MYD08_D3 (Aqua) — Cloud_Fraction_Mean
    - OPeNDAP access via LAADS DAAC
    - Requires Earthdata authentication

Target features:
    - cloud_fraction_anomaly: cloud cover deviation from 30-day baseline (σ)

References:
    - Guangmeng & Jie (2013) Nat. Hazards Earth Syst. Sci. 13:927-934
    - Shou (2006) Terr. Atmos. Ocean. Sci. 17:395-414
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH
from earthdata_auth import get_earthdata_session, earthdata_fetch, EARTHDATA_TOKEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# LAADS DAAC OPeNDAP for MOD08_D3 (MODIS Terra daily L3 atmosphere)
# Path includes RemoteResources/laads/ prefix (verified 2026-03-20)
LAADS_OPENDAP = "https://ladsweb.modaps.eosdis.nasa.gov/opendap/RemoteResources/laads/allData/61/MOD08_D3"

# Japan bbox in 1° grid
# Lat: -90 to 90 (180 cells), Lon: -180 to 180 (360 cells)
# Japan: lat 24-46 → indices 114 to 136
# Japan: lon 122-150 → indices 302 to 330
LAT_START = 114
LAT_END = 136
LON_START = 302
LON_END = 330

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)
START_YEAR = 2000


async def init_cloud_table():
    """Create cloud fraction table."""
    async with safe_connect() as db:
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
            CREATE INDEX IF NOT EXISTS idx_cloud_time
            ON cloud_fraction(observed_at)
        """)
        await db.commit()


async def _resolve_filename(session: aiohttp.ClientSession, year: int, doy: int) -> str | None:
    """Resolve MOD08_D3 filename from LAADS DAAC directory listing."""
    dir_url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD08_D3/{year}/{doy:03d}.json"
    try:
        headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"} if EARTHDATA_TOKEN else {}
        async with session.get(dir_url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
            # LAADS returns {"content": [file objects]}
            items = data.get("content", data) if isinstance(data, dict) else data
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = item.get("name", "")
                if name.startswith("MOD08_D3") and name.endswith(".hdf"):
                    return name
    except Exception:
        pass
    return None


async def fetch_cloud_day(session: aiohttp.ClientSession, date: datetime) -> list[dict]:
    """Fetch cloud fraction for one day via LAADS OPeNDAP."""
    if not EARTHDATA_TOKEN:
        return []

    year = date.year
    doy = date.timetuple().tm_yday
    date_str = date.strftime("%Y-%m-%d")

    # Resolve actual filename (contains processing date)
    filename = await _resolve_filename(session, year, doy)
    if not filename:
        return []

    # OPeNDAP ASCII — fetch full array (LAADS HDF4 doesn't support subsetting)
    # Python-side slicing for Japan bbox is fast (~65K values per day)
    url = f"{LAADS_OPENDAP}/{year}/{doy:03d}/{filename}.ascii?Cloud_Fraction_Mean"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            status, text = await earthdata_fetch(session, url, timeout=TIMEOUT)
            if status == 200:
                if not text or "<html" in text[:200].lower():
                    return []
                return _parse_cloud_ascii(text, date_str)
            elif status in (401, 403):
                logger.info("Cloud fraction requires Earthdata auth")
                return []
            elif status == 404:
                return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("Cloud %s: %s", date_str, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_cloud_ascii(text: str, date_str: str) -> list[dict]:
    """Parse OPeNDAP ASCII cloud fraction (full 180x360 grid).

    Format from LAADS OPeNDAP:
        Dataset: MOD08_D3...
        Cloud_Fraction_Mean[0], 2800, 4564, ...
        Cloud_Fraction_Mean[1], 3100, 4200, ...
    Values are Int16 scaled by 10000 (divide to get 0-1 fraction).
    We extract only the Japan bbox rows/columns.
    """
    rows = []
    in_data = False
    SCALE = 10000.0
    FILL_VALUE = -9999  # typical HDF4 fill

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Data lines start with variable name or [index]
        if line.startswith("Cloud_Fraction_Mean["):
            in_data = True
            # Extract row index: "Cloud_Fraction_Mean[42], ..."
            bracket_end = line.index("]")
            row_idx = int(line[len("Cloud_Fraction_Mean["):bracket_end])

            # Only process Japan lat rows (LAT_START to LAT_END inclusive)
            if row_idx < LAT_START or row_idx > LAT_END:
                continue

            lat = -89.5 + row_idx  # 1° grid center

            # Values after the first comma
            first_comma = line.index(",")
            val_parts = line[first_comma + 1:].split(",")

            # Extract Japan lon columns only
            for lon_idx in range(LON_START, min(LON_END + 1, len(val_parts) + LON_START)):
                arr_idx = lon_idx  # val_parts covers all 360 columns
                if arr_idx >= len(val_parts):
                    break
                try:
                    raw = int(val_parts[arr_idx].strip())
                    if raw <= FILL_VALUE or raw < 0:
                        continue
                    val = raw / SCALE
                    if val > 1.1:
                        continue
                    lon = -179.5 + lon_idx
                    rows.append({
                        "date": date_str,
                        "lat": round(lat, 1),
                        "lon": round(lon, 1),
                        "cloud_frac": round(val, 4),
                    })
                except (ValueError, IndexError):
                    continue

            continue

        # Stop after data section
        if in_data and not line.startswith("Cloud_Fraction_Mean"):
            break

    return rows


async def main():
    await init_db()
    await init_cloud_table()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN:
        logger.info(
            "Cloud fraction fetch: EARTHDATA_TOKEN not set. "
            "Cloud features will be excluded via dynamic selection."
        )
        return

    # Continuous daily fetch — ML anomaly detection requires full baselines
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM cloud_fraction"
        )
    existing_dates = set(r[0] for r in existing) if existing else set()

    # Generate all dates from START_YEAR to yesterday
    today = datetime.now(timezone.utc).date()
    d = datetime(START_YEAR, 1, 1).date()
    target_dates = []
    while d < today:
        ds = d.strftime("%Y-%m-%d")
        if ds not in existing_dates:
            target_dates.append(datetime(d.year, d.month, d.day))
        d += timedelta(days=1)

    # Prioritize analysis period (2011+) first, then backfill pre-2011
    analysis_dates = [dt for dt in target_dates if dt.year >= 2011]
    backfill_dates = [dt for dt in target_dates if dt.year < 2011]
    dates_to_fetch = (analysis_dates + backfill_dates)[:600]

    if not dates_to_fetch:
        logger.info("All cloud fraction target dates already fetched")
        return

    logger.info("Cloud fraction: %d dates to fetch", len(dates_to_fetch))

    total_records = 0
    session = await get_earthdata_session()
    try:
        for i, date in enumerate(dates_to_fetch):
            rows = await fetch_cloud_day(session, date)
            if rows:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO cloud_fraction
                           (observed_at, cell_lat, cell_lon, cloud_frac, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["date"], r["lat"], r["lon"], r["cloud_frac"], now) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 20 == 0:
                logger.info("Cloud: %d/%d dates, %d records",
                            i + 1, len(dates_to_fetch), total_records)
            await asyncio.sleep(1.0)
    finally:
        await session.close()

    logger.info("Cloud fraction fetch complete: %d records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
