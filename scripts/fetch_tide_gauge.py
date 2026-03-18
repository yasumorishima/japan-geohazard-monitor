"""Fetch tide gauge sea level data from UHSLC (University of Hawaii).

Tide gauges measure hourly sea level with sub-mm precision. After removing
tidal components, the residual reveals non-tidal signals including:
    - Slow-slip events (seafloor deformation → coastal sea level change)
    - Submarine volcanic activity
    - Crustal loading changes

Japan has dense coastal tide gauge coverage along subduction zones.
The UHSLC Research Quality dataset provides hourly values with quality
control, available as simple text files without authentication.

Physical mechanism:
    Pre-seismic slow slip → seafloor vertical displacement (mm-cm)
    → sea level anomaly at nearby coast → detectable in tide residual.
    Documented for Cascadia, Nankai, and other subduction zones.

Target features:
    - tide_residual_anomaly: sea level residual deviation from 30-day mean (σ)

Data source: University of Hawaii Sea Level Center (UHSLC)
    - Research Quality hourly data
    - Simple text (UHSLC format), no authentication
    - Stations around Japan coast

References:
    - Ito et al. (2013) Science 339:1206-1209 (slow slip + tide gauge)
    - Bürgmann (2018) Nature 553:1-2 (slow slip review)
"""

import asyncio
import logging
import re
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

# UHSLC Research Quality hourly data
# Format: https://uhslc.soest.hawaii.edu/data/netcdf/rqds/pacific/
UHSLC_BASE = "https://uhslc.soest.hawaii.edu/data/csv/rqds/pacific/hourly"

# Japan tide gauge stations (UHSLC IDs)
# Selected stations along Japan coast covering major subduction zones
JAPAN_STATIONS = {
    "h326": {"name": "Aburatsu", "lat": 31.58, "lon": 131.42},
    "h327": {"name": "Naha", "lat": 26.22, "lon": 127.67},
    "h328": {"name": "Mera", "lat": 34.92, "lon": 139.83},
    "h329": {"name": "Kushimoto", "lat": 33.47, "lon": 135.78},
    "h330": {"name": "Hamada", "lat": 34.90, "lon": 132.07},
    "h344": {"name": "Toyama", "lat": 36.77, "lon": 137.22},
    "h355": {"name": "Ishigaki", "lat": 24.33, "lon": 124.15},
    "h362": {"name": "Chichijima", "lat": 27.10, "lon": 142.18},
    "h681": {"name": "Wakkanai", "lat": 45.40, "lon": 141.68},
}

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)


async def init_tide_table():
    """Create tide gauge table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tide_gauge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                sea_level_mm REAL NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(station_id, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_tide_time
            ON tide_gauge(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_tide_station
            ON tide_gauge(station_id)
        """)
        await db.commit()


def parse_uhslc_csv(text: str, station_id: str) -> list[dict]:
    """Parse UHSLC CSV format hourly data.

    Format varies but typically:
        date, sea_level_mm
    or
        year, month, day, hour, sea_level_mm

    Missing values are typically -32767 or 9999.
    """
    rows = []
    for line in text.split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("Date"):
            continue

        parts = line.split(",")
        if len(parts) < 2:
            continue

        try:
            # Try date,value format first
            if len(parts) == 2:
                date_str = parts[0].strip()
                value = float(parts[1].strip())
            elif len(parts) >= 5:
                # year,month,day,hour,value format
                year = int(parts[0].strip())
                month = int(parts[1].strip())
                day = int(parts[2].strip())
                hour = int(parts[3].strip())
                value = float(parts[4].strip())
                date_str = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:00:00"
            else:
                continue

            # Skip missing values
            if value < -9000 or value > 90000:
                continue

            # Normalize date format
            if "T" not in date_str and len(date_str) >= 10:
                date_str = date_str[:10] + "T00:00:00"

            rows.append({
                "observed_at": date_str,
                "sea_level_mm": value,
            })
        except (ValueError, IndexError):
            continue

    return rows


async def fetch_station(session: aiohttp.ClientSession,
                         station_id: str) -> list[dict]:
    """Fetch hourly data for one UHSLC station."""
    url = f"{UHSLC_BASE}/{station_id}.csv"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return parse_uhslc_csv(text, station_id)
                elif resp.status == 404:
                    logger.info("UHSLC %s: not available (404)", station_id)
                    return []
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning("UHSLC %s: HTTP %d", station_id, resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("UHSLC %s: %s", station_id, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


async def main():
    await init_db()
    await init_tide_table()

    now = datetime.now(timezone.utc).isoformat()

    # Check existing
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT station_id, COUNT(*), MAX(observed_at) FROM tide_gauge GROUP BY station_id"
        )
    existing_summary = {r[0]: (r[1], r[2]) for r in existing} if existing else {}
    logger.info("Tide gauge existing: %d stations", len(existing_summary))

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for station_id, info in JAPAN_STATIONS.items():
            logger.info("Fetching %s (%s)...", station_id, info["name"])

            rows = await fetch_station(session, station_id)
            if not rows:
                continue

            # Filter to 2011+ only
            rows = [r for r in rows if r["observed_at"] >= "2011"]

            if not rows:
                logger.info("  %s: no data from 2011+", station_id)
                continue

            # Store
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO tide_gauge
                       (station_id, observed_at, sea_level_mm, latitude, longitude, received_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    [(station_id, r["observed_at"], r["sea_level_mm"],
                      info["lat"], info["lon"], now) for r in rows],
                )
                await db.commit()

            total_records += len(rows)
            logger.info("  %s: %d records", info["name"], len(rows))
            await asyncio.sleep(1.0)

    logger.info("Tide gauge fetch complete: %d total records from %d stations",
                total_records, len(JAPAN_STATIONS))


if __name__ == "__main__":
    asyncio.run(main())
