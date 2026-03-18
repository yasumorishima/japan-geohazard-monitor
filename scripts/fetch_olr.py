"""Fetch Outgoing Longwave Radiation (OLR) daily data for Japan.

OLR measures thermal infrared radiation escaping Earth's atmosphere. Pre-seismic
thermal anomalies have been reported 7-10 days before M6+ earthquakes in Japan,
attributed to the LAIC (Lithosphere-Atmosphere-Ionosphere Coupling) model:
    crustal stress → radon release → ionization → aerosol nucleation
    → cloud/thermal anomaly → OLR change

Unlike MODIS LST (point measurements at epicenters), OLR captures broad-scale
thermal anomalies over the entire Japan region at 2.5-degree resolution.

Data source: NOAA PSL (Physical Sciences Laboratory) Uninterpolated OLR Daily
    - Derived from NOAA satellite observations
    - 2.5-degree global grid, daily, 1974-present
    - Single dataset file covering all years
    - No authentication required
    - PSL THREDDS NCSS endpoint returns CSV

Target features:
    - olr_anomaly: deviation from 30-day rolling mean (in σ units)

References:
    - Ouzounov et al. (2007) Tectonophysics 431:211-220
    - Xiong et al. (2010) Nat. Hazards Earth Syst. Sci. 10:2169-2178
"""

import asyncio
import csv
import io
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NOAA PSL THREDDS NCSS endpoint for uninterpolated OLR daily
# Single dataset covering 1974-present (no per-year files, no version guessing)
PSL_NCSS_BASE = (
    "https://psl.noaa.gov/thredds/ncss/grid/Datasets/uninterp_OLR/olr.day.mean.nc"
)

# Japan bounding box
NORTH = 46.0
SOUTH = 24.0
WEST = 122.0
EAST = 150.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)

START_YEAR = 2011


async def init_olr_table():
    """Create OLR table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS olr (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                olr_wm2 REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_olr_time
            ON olr(observed_at)
        """)
        await db.commit()


async def fetch_olr_year(session: aiohttp.ClientSession, year: int,
                          start_date: str = None, end_date: str = None) -> list[dict]:
    """Fetch OLR data for a year from NOAA PSL THREDDS NCSS.

    The PSL dataset is a single file covering 1974-present, so we subset
    by time_start/time_end parameters rather than guessing filenames.

    Returns list of {date, lat, lon, olr} dicts.
    """
    if start_date is None:
        start_date = f"{year}-01-01T00:00:00Z"
    if end_date is None:
        end_date = f"{year}-12-31T23:59:59Z"

    url = (
        f"{PSL_NCSS_BASE}"
        f"?var=olr"
        f"&north={NORTH}&south={SOUTH}&west={WEST}&east={EAST}"
        f"&time_start={start_date}&time_end={end_date}"
        f"&accept=csv"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return _parse_ncss_csv(text)
                else:
                    logger.warning("OLR %d attempt %d: HTTP %d", year, attempt, resp.status)
                    if attempt == MAX_RETRIES:
                        body_preview = (await resp.text())[:200]
                        logger.warning("OLR %d: final failure, response: %s", year, body_preview)
                        return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug("OLR %d attempt %d: %s", year, attempt, type(e).__name__)
            if attempt == MAX_RETRIES:
                logger.warning("OLR %d: all retries exhausted (%s)", year, type(e).__name__)
                return []
        await asyncio.sleep(2 ** attempt)

    return []


def _parse_ncss_csv(text: str) -> list[dict]:
    """Parse THREDDS NCSS CSV response.

    Expected format (header + data rows):
        date,lat[unit="degrees_north"],lon[unit="degrees_east"],olr[unit="W/m^2"]
        2011-01-01T00:00:00Z,25.0,122.5,234.567
    """
    rows = []
    reader = csv.reader(io.StringIO(text))

    header = None
    for line in reader:
        if not line:
            continue
        if header is None:
            header = line
            continue

        try:
            # Find column indices (header names vary)
            if len(line) < 4:
                continue

            date_str = line[0].strip()
            lat = float(line[1])
            lon = float(line[2])
            olr = float(line[3])

            # Validate
            if olr < 50 or olr > 400:
                continue

            # Normalize date to YYYY-MM-DD
            if "T" in date_str:
                date_str = date_str[:10]

            rows.append({
                "date": date_str,
                "lat": round(lat, 1),
                "lon": round(lon, 1),
                "olr": round(olr, 2),
            })
        except (ValueError, IndexError):
            continue

    return rows


async def main():
    await init_db()
    await init_olr_table()

    now = datetime.now(timezone.utc).isoformat()
    current_year = datetime.now(timezone.utc).year

    # Check existing data range
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MIN(observed_at), MAX(observed_at), COUNT(DISTINCT observed_at) FROM olr"
        )
    if existing and existing[0][2]:
        logger.info("OLR existing: %s to %s (%d dates)",
                     existing[0][0], existing[0][1], existing[0][2])
        # Find the last year we have complete data for
        last_date = existing[0][1]
        last_year = int(last_date[:4]) if last_date else START_YEAR
        # Fetch from last_year onwards (re-fetch partial year)
        start_year = last_year
    else:
        start_year = START_YEAR

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for year in range(start_year, current_year + 1):
            logger.info("Fetching OLR %d...", year)

            rows = await fetch_olr_year(session, year)
            if not rows:
                continue

            # Store in DB
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO olr
                       (observed_at, cell_lat, cell_lon, olr_wm2, received_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    [(r["date"], r["lat"], r["lon"], r["olr"], now) for r in rows],
                )
                await db.commit()

            total_records += len(rows)
            logger.info("OLR %d: %d records", year, len(rows))

            # Rate limit between years
            await asyncio.sleep(2.0)

    logger.info("OLR fetch complete: %d total records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
