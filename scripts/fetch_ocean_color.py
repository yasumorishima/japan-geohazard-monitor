"""Fetch ocean color (chlorophyll-a) data from NASA OB.DAAC.

Ocean color satellites measure chlorophyll-a concentration, which serves
as a proxy for phytoplankton biomass. Anomalous changes in ocean color
near subduction zones may indicate:

Physical mechanism:
    1. Submarine hydrothermal/volcanic activity → nutrient upwelling
       → phytoplankton bloom → chlorophyll spike
    2. Submarine landslides → turbidity increase → color change
    3. Seafloor gas seeps (methane, CO2) → localized ocean chemistry
       changes → biological response

Japan's subduction zones (Nankai Trough, Japan Trench, Izu-Bonin) have
active submarine volcanism. Changes in ocean color could indicate
subsurface tectonic activity not detectable on land.

Data source: NASA MODIS Aqua Level 3 Mapped Daily (4km)
    - Product: chlor_a (mg/m³)
    - OPeNDAP access via OB.DAAC
    - Requires Earthdata authentication

Target features:
    - ocean_color_anomaly: chlorophyll-a deviation from 30-day baseline (σ)

References:
    - Escalera-Reyes et al. (2019) Remote Sensing 11:2405
    - Yang et al. (2022) Geophysical Research Letters 49:e2022GL098939
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

# NASA OB.DAAC OPeNDAP for MODIS Aqua L3 daily mapped chlor_a
# Product: MODIS_AQUA_L3m_CHL_4km_daily
OBDAAC_OPENDAP = "https://oceandata.sci.gsfc.nasa.gov/opendap/MODISA/L3SMI"

# Japan ocean bbox (wider than land to cover subduction zones)
# Lat: 20-50, Lon: 120-155 → in 4km grid (~0.04°)
# Lat indices: (20+90)/0.04167 = 2640 to (50+90)/0.04167 = 3360
# Lon indices: (120+180)/0.04167 = 7200 to (155+180)/0.04167 = 8040
LAT_START = 2640
LAT_END = 3360
LON_START = 7200
LON_END = 8040

# Aggregate to 2° cells for compatibility with prediction grid
CELL_DEG = 2.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)
START_YEAR = 2011


async def init_ocean_color_table():
    """Create ocean color table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ocean_color (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                chlor_a_mg_m3 REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_time
            ON ocean_color(observed_at)
        """)
        await db.commit()


async def fetch_chlor_day(session: aiohttp.ClientSession, date: datetime) -> list[dict]:
    """Fetch chlorophyll-a for one day via OB.DAAC OPeNDAP."""
    if not EARTHDATA_TOKEN:
        return []

    date_str = date.strftime("%Y%m%d")
    year = date.year
    doy = date.timetuple().tm_yday

    # OB.DAAC filename pattern
    filename = f"AQUA_MODIS.{date.strftime('%Y%m%d')}.L3m.DAY.CHL.chlor_a.4km.nc"
    url = (
        f"{OBDAAC_OPENDAP}/{year}/{doy:03d}/{filename}.ascii"
        f"?chlor_a[{LAT_START}:10:{LAT_END}][{LON_START}:10:{LON_END}]"
    )

    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT, headers=headers,
                                    allow_redirects=True) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    if "html" in content_type.lower():
                        return []
                    text = await resp.text()
                    return _parse_chlor_ascii(text, date.strftime("%Y-%m-%d"))
                elif resp.status in (401, 403):
                    logger.info("Ocean color requires Earthdata auth (HTTP %d)", resp.status)
                    return []
                elif resp.status == 404:
                    return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("Ocean color %s: %s", date_str, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_chlor_ascii(text: str, date_str: str) -> list[dict]:
    """Parse OPeNDAP ASCII chlorophyll grid and aggregate to 2° cells."""
    import math
    from collections import defaultdict

    cell_values = defaultdict(list)
    in_data = False

    for line in text.split("\n"):
        line = line.strip()
        if "chlor_a" in line and "[" in line:
            in_data = True
            continue
        if not in_data:
            continue

        if line.startswith("["):
            parts = line.split(",")
            if len(parts) < 2:
                continue
            try:
                lat_idx = int(parts[0].strip("[] "))
                lat = -90.0 + (LAT_START + lat_idx * 10) * 0.04167

                for lon_offset, val_str in enumerate(parts[1:]):
                    val = float(val_str.strip())
                    if val < 0 or val > 100:
                        continue  # Fill/invalid
                    lon = -180.0 + (LON_START + lon_offset * 10) * 0.04167

                    # Aggregate to 2° cell
                    cell_lat = math.floor(lat / CELL_DEG) * CELL_DEG + CELL_DEG / 2
                    cell_lon = math.floor(lon / CELL_DEG) * CELL_DEG + CELL_DEG / 2
                    cell_values[(cell_lat, cell_lon)].append(val)
            except (ValueError, IndexError):
                continue

    rows = []
    for (lat, lon), vals in cell_values.items():
        if 20 <= lat <= 50 and 120 <= lon <= 155:
            rows.append({
                "date": date_str,
                "lat": round(lat, 1),
                "lon": round(lon, 1),
                "chlor_a": round(sum(vals) / len(vals), 4),
            })
    return rows


async def main():
    await init_db()
    await init_ocean_color_table()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN:
        logger.info(
            "Ocean color fetch: EARTHDATA_TOKEN not set. "
            "Ocean color features will be excluded via dynamic selection."
        )
        return

    # Fetch dates around M6+ earthquakes (±7 days)
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at) FROM earthquakes "
            "WHERE magnitude >= 6.0 AND DATE(occurred_at) >= '2011-01-01'"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM ocean_color"
        )
    existing_dates = set(r[0] for r in existing) if existing else set()

    target_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-7, 8):
            target_dates.add(d + timedelta(days=offset))

    dates_to_fetch = sorted(
        d for d in target_dates
        if d.strftime("%Y-%m-%d") not in existing_dates
        and d.year >= START_YEAR
    )[:150]

    if not dates_to_fetch:
        logger.info("All ocean color target dates already fetched")
        return

    logger.info("Ocean color: %d dates to fetch", len(dates_to_fetch))

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for i, date in enumerate(dates_to_fetch):
            rows = await fetch_chlor_day(session, date)
            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO ocean_color
                           (observed_at, cell_lat, cell_lon, chlor_a_mg_m3, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["date"], r["lat"], r["lon"], r["chlor_a"], now) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 20 == 0:
                logger.info("Ocean color: %d/%d dates, %d records",
                            i + 1, len(dates_to_fetch), total_records)
            await asyncio.sleep(1.0)

    logger.info("Ocean color fetch complete: %d records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
