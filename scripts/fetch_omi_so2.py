"""Fetch atmospheric SO2 column data from NASA OMI.

SO2 (sulfur dioxide) in the atmosphere primarily comes from volcanic
degassing and anthropogenic sources. In subduction zones like Japan,
changes in volcanic SO2 emission rates may correlate with regional
tectonic stress changes.

Physical mechanism:
    Tectonic stress changes → altered permeability in volcanic conduits
    → modified degassing rates → atmospheric SO2 column changes.
    Japan has 111 active volcanoes along the subduction zone; changes
    in their collective degassing pattern could indicate broad-scale
    stress redistribution.

Data source: NASA OMI OMSO2e (Level 3 daily gridded SO2)
    - 0.25° global grid, daily, 2004-present
    - Requires Earthdata login (free) for OPeNDAP access
    - Alternative: NASA SO2 monitoring portal for summary data

Target features:
    - so2_column_anomaly: deviation from seasonal baseline (DU)

References:
    - Carn et al. (2016) J. Volcanol. Geotherm. Res. 327:50-66
    - Noguchi et al. (2011) Remote Sensing 3:1820-1834
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

EARTHDATA_TOKEN = os.environ.get("EARTHDATA_TOKEN")

# GES DISC OPeNDAP base for OMSO2e (Level 3 daily gridded)
GESDISC_OPENDAP = "https://measures.gesdisc.eosdis.nasa.gov/opendap/SO2/OMSO2e.003"

# Japan bbox in grid indices (0.25° resolution)
# Lat: -89.875 to 89.875 (720 cells), Lon: -179.875 to 179.875 (1440 cells)
# Japan: lat 24-46 → indices (24+89.875)/0.25 = 455 to 543
# Japan: lon 122-150 → indices (122+179.875)/0.25 = 1207 to 1319
LAT_START = 455
LAT_END = 543
LON_START = 1207
LON_END = 1319

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)
START_YEAR = 2011


async def init_so2_table():
    """Create SO2 column table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS so2_column (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                so2_du REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_so2_time
            ON so2_column(observed_at)
        """)
        await db.commit()


async def fetch_so2_day(session: aiohttp.ClientSession, date: datetime) -> list[dict]:
    """Fetch SO2 column data for one day via GES DISC OPeNDAP.

    Uses ASCII output from OPeNDAP to avoid netCDF dependency.
    """
    if not EARTHDATA_TOKEN:
        return []

    date_str = date.strftime("%Y-%m-%d")
    doy = date.timetuple().tm_yday
    year = date.year

    # OMSO2e filename pattern: OMI-Aura_L3-OMSO2e_YYYY-MM-DD_v003-YYYY...
    # OPeNDAP URL with subsetting
    filename = f"OMI-Aura_L3-OMSO2e_{date_str}m{doy:03d}_v003"

    # Try to get PBL (planetary boundary layer) SO2 column
    url = (
        f"{GESDISC_OPENDAP}/{year}/{filename}.he5.ascii"
        f"?ColumnAmountSO2_PBL"
        f"[{LAT_START}:1:{LAT_END}]"
        f"[{LON_START}:1:{LON_END}]"
    )

    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT, headers=headers,
                                    allow_redirects=True) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    if "html" in content_type.lower():
                        logger.debug("SO2 %s: returned HTML (auth redirect)", date_str)
                        return []
                    text = await resp.text()
                    return _parse_opendap_ascii(text, date_str)
                elif resp.status in (401, 403):
                    logger.info("SO2 requires Earthdata auth (HTTP %d)", resp.status)
                    return []
                elif resp.status == 404:
                    return []  # No data for this date
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning("SO2 %s: HTTP %d", date_str, resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("SO2 %s: %s", date_str, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_opendap_ascii(text: str, date_str: str) -> list[dict]:
    """Parse OPeNDAP ASCII grid response.

    Format is typically:
        Dataset {
            Float32 ColumnAmountSO2_PBL[nLat][nLon];
        }
        ColumnAmountSO2_PBL[89][113]
        [0], value, value, value, ...
        [1], value, value, value, ...
    """
    rows = []
    in_data = False
    lat_idx = 0

    for line in text.split("\n"):
        line = line.strip()

        if "ColumnAmountSO2" in line and "[" in line:
            in_data = True
            continue

        if not in_data:
            continue

        if line.startswith("["):
            # Parse row: [lat_idx], val, val, val, ...
            parts = line.split(",")
            if len(parts) < 2:
                continue

            try:
                # Extract lat index from [N]
                idx_str = parts[0].strip()
                lat_idx = int(idx_str.strip("[] "))
                lat = -89.875 + (LAT_START + lat_idx) * 0.25

                for lon_offset, val_str in enumerate(parts[1:]):
                    val = float(val_str.strip())
                    if val < -999 or val > 1000:
                        continue  # Missing/fill value
                    lon = -179.875 + (LON_START + lon_offset) * 0.25

                    if val > 0:  # Only store positive detections
                        rows.append({
                            "date": date_str,
                            "lat": round(lat, 3),
                            "lon": round(lon, 3),
                            "so2": round(val, 4),
                        })
            except (ValueError, IndexError):
                continue

    return rows


async def main():
    await init_db()
    await init_so2_table()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN:
        logger.info(
            "SO2 fetch: EARTHDATA_TOKEN not set. "
            "Set EARTHDATA_TOKEN env var for OMI SO2 data access. "
            "Generate token at https://urs.earthdata.nasa.gov/ "
            "SO2 features will be excluded via dynamic selection."
        )
        return

    # Check existing
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(DISTINCT observed_at) FROM so2_column"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    n_dates = existing[0][1] if existing else 0
    logger.info("SO2 existing: %d dates (latest: %s)", n_dates, last_date)

    # Determine dates to fetch (only dates around M6+ earthquakes ±7 days)
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at) FROM earthquakes "
            "WHERE magnitude >= 6.0 AND DATE(occurred_at) >= '2011-01-01' "
            "ORDER BY occurred_at"
        )
    existing_dates = set()
    if last_date:
        async with aiosqlite.connect(DB_PATH) as db:
            ed_rows = await db.execute_fetchall(
                "SELECT DISTINCT observed_at FROM so2_column"
            )
        existing_dates = set(r[0] for r in ed_rows)

    target_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-7, 8):
            target_dates.add(d + timedelta(days=offset))

    dates_to_fetch = sorted(
        d for d in target_dates
        if d.strftime("%Y-%m-%d") not in existing_dates
        and d.year >= START_YEAR
    )[:200]  # Cap per run

    if not dates_to_fetch:
        logger.info("All SO2 target dates already fetched")
        return

    logger.info("SO2: %d dates to fetch", len(dates_to_fetch))

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for i, date in enumerate(dates_to_fetch):
            rows = await fetch_so2_day(session, date)
            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO so2_column
                           (observed_at, cell_lat, cell_lon, so2_du, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["date"], r["lat"], r["lon"], r["so2"], now) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 20 == 0:
                logger.info("SO2: %d/%d dates, %d records", i + 1, len(dates_to_fetch), total_records)

            await asyncio.sleep(1.0)

    logger.info("SO2 fetch complete: %d total records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
