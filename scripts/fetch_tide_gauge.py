"""Fetch tide gauge sea level data from UHSLC ERDDAP.

Tide gauges measure hourly sea level with sub-mm precision. After removing
tidal components, the residual reveals non-tidal signals including:
    - Slow-slip events (seafloor deformation → coastal sea level change)
    - Submarine volcanic activity
    - Crustal loading changes

Japan has dense coastal tide gauge coverage along subduction zones.
The UHSLC Fast Delivery dataset provides hourly values with quality
control, available via ERDDAP REST API without authentication.

Physical mechanism:
    Pre-seismic slow slip → seafloor vertical displacement (mm-cm)
    → sea level anomaly at nearby coast → detectable in tide residual.
    Documented for Cascadia, Nankai, and other subduction zones.

Target features:
    - tide_residual_anomaly: sea level residual deviation from 30-day mean (σ)

Data source: University of Hawaii Sea Level Center (UHSLC)
    - ERDDAP: https://uhslc.soest.hawaii.edu/erddap/tabledap/global_hourly_fast
    - CSV output, no authentication
    - 19 stations around Japan coast (lat 24-46, lon 122-156)

References:
    - Ito et al. (2013) Science 339:1206-1209 (slow slip + tide gauge)
    - Bürgmann (2018) Nature 553:1-2 (slow slip review)
"""

import asyncio
import csv
import io
import logging
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

# UHSLC ERDDAP endpoint for Fast Delivery hourly data
ERDDAP_BASE = "https://uhslc.soest.hawaii.edu/erddap/tabledap/global_hourly_fast"

# Japan bounding box
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 156.0

# Fetch 90 days per request to keep response size manageable
CHUNK_DAYS = 90

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=600, connect=60)

START_YEAR = 2011


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


async def discover_japan_stations(session: aiohttp.ClientSession) -> list[dict]:
    """Discover all UHSLC stations in the Japan region via ERDDAP."""
    url = (
        f"{ERDDAP_BASE}.csv"
        f"?station_name,uhslc_id,latitude,longitude"
        f"&latitude>={JAPAN_LAT_MIN}&latitude<={JAPAN_LAT_MAX}"
        f"&longitude>={JAPAN_LON_MIN}&longitude<={JAPAN_LON_MAX}"
        f"&distinct()"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return _parse_station_csv(text)
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning("UHSLC station discovery: HTTP %d", resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("UHSLC station discovery failed: %s", type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_station_csv(text: str) -> list[dict]:
    """Parse ERDDAP station list CSV (2 header rows + data)."""
    stations = []
    reader = csv.reader(io.StringIO(text))

    header = next(reader, None)  # column names
    units = next(reader, None)   # units row

    for row in reader:
        if len(row) < 4:
            continue
        try:
            stations.append({
                "name": row[0].strip(),
                "uhslc_id": int(row[1]),
                "lat": float(row[2]),
                "lon": float(row[3]),
            })
        except (ValueError, IndexError):
            continue

    return stations


async def fetch_station_data(session: aiohttp.ClientSession,
                              uhslc_id: int,
                              time_start: str,
                              time_end: str) -> list[dict]:
    """Fetch hourly sea level data for one station via ERDDAP CSV."""
    url = (
        f"{ERDDAP_BASE}.csv"
        f"?time,sea_level,latitude,longitude"
        f"&uhslc_id={uhslc_id}"
        f"&time>={time_start}"
        f"&time<={time_end}"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return _parse_data_csv(text)
                elif resp.status == 404:
                    return []  # No data for this time range
                else:
                    if attempt == MAX_RETRIES:
                        logger.debug("UHSLC %d: HTTP %d", uhslc_id, resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("UHSLC %d: %s", uhslc_id, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_data_csv(text: str) -> list[dict]:
    """Parse ERDDAP hourly data CSV (2 header rows + data)."""
    rows = []
    reader = csv.reader(io.StringIO(text))

    header = next(reader, None)
    units = next(reader, None)  # skip units row

    for row in reader:
        if len(row) < 4:
            continue
        try:
            time_str = row[0].strip()
            sea_level = float(row[1])
            lat = float(row[2])
            lon = float(row[3])

            # Skip NaN or extreme values
            if sea_level < -9000 or sea_level > 90000:
                continue

            # Normalize timestamp: "2024-06-01T00:00:00Z" -> "2024-06-01T00:00:00"
            observed_at = time_str.replace("Z", "")

            rows.append({
                "observed_at": observed_at,
                "sea_level_mm": sea_level,
                "lat": lat,
                "lon": lon,
            })
        except (ValueError, IndexError):
            continue

    return rows


async def main():
    await init_db()
    await init_tide_table()

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # Check existing data
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT station_id, COUNT(*), MAX(observed_at) FROM tide_gauge GROUP BY station_id"
        )
    existing_summary = {r[0]: (r[1], r[2]) for r in existing} if existing else {}
    logger.info("Tide gauge existing: %d stations", len(existing_summary))

    # Discover Japan stations from ERDDAP
    async with aiohttp.ClientSession() as session:
        stations = await discover_japan_stations(session)

    if not stations:
        logger.warning("No UHSLC stations found in Japan region")
        return

    logger.info("Found %d UHSLC stations in Japan region", len(stations))

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for station in stations:
            station_key = str(station["uhslc_id"])
            name = station["name"]

            # Determine start time (must be timezone-aware to compare with now)
            if station_key in existing_summary:
                last_date = existing_summary[station_key][1]
                # Start from last known date
                try:
                    start_dt = datetime.fromisoformat(
                        last_date.replace("Z", "+00:00")
                    )
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    start_dt = datetime(START_YEAR, 1, 1, tzinfo=timezone.utc)
            else:
                start_dt = datetime(START_YEAR, 1, 1, tzinfo=timezone.utc)

            logger.info("Fetching %s (ID=%s, %.2f°N %.2f°E) from %s...",
                        name, station_key, station["lat"], station["lon"],
                        start_dt.strftime("%Y-%m-%d"))

            station_total = 0
            current = start_dt

            while current < now:
                chunk_end = min(current + timedelta(days=CHUNK_DAYS), now)
                time_start = current.strftime("%Y-%m-%dT00:00:00Z")
                time_end = chunk_end.strftime("%Y-%m-%dT23:59:59Z")

                rows = await fetch_station_data(
                    session, station["uhslc_id"], time_start, time_end
                )

                if rows:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.executemany(
                            """INSERT OR IGNORE INTO tide_gauge
                               (station_id, observed_at, sea_level_mm,
                                latitude, longitude, received_at)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            [(station_key, r["observed_at"], r["sea_level_mm"],
                              r["lat"], r["lon"], now_iso) for r in rows],
                        )
                        await db.commit()
                    station_total += len(rows)

                current = chunk_end + timedelta(days=1)
                await asyncio.sleep(0.5)  # Rate limit compliance

            if station_total > 0:
                total_records += station_total
                logger.info("  %s: %d records", name, station_total)

    logger.info("Tide gauge fetch complete: %d total records from %d stations",
                total_records, len(stations))


if __name__ == "__main__":
    asyncio.run(main())
