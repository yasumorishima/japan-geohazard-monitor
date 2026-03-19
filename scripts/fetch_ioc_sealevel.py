"""Fetch near-real-time sea level data from IOC Sea Level Station Monitoring.

Coastal and near-shore sea level stations operated by IOC member states capture
tidal and non-tidal sea level signals. Slow-slip events on subduction faults
cause seafloor vertical displacement that propagates as a pressure signal to
nearby coastal stations. Unlike UHSLC (already in our pipeline) which provides
historical research-quality data, IOC SLSMF provides near-real-time monitoring
data from additional stations.

Physical mechanism:
    Pre-seismic slow slip on the plate interface → seafloor vertical
    displacement (mm to cm scale) → pressure change propagates through
    water column → detected as sea level anomaly at nearby coastal gauges.
    The signal is separable from tidal components via harmonic analysis
    and from meteorological surge via atmospheric pressure correction.

Target features:
    - ioc_sealevel_anomaly: sea level deviation from 45-day rolling baseline (σ)

Data sources:
    1. IOC/UNESCO Sea Level Station Monitoring Facility (SLSMF)
       - Near-real-time data from global tide gauge network
       - JSON API: https://www.ioc-sealevelmonitoring.org/service.php
       - Rate limit: ~1 request per minute recommended
    2. PSMSL Ocean Bottom Pressure (reference/future integration)
       - https://psmsl.org/data/bottom_pressure/

References:
    - IOC/UNESCO Sea Level Monitoring Facility
    - Bürgmann (2018) Nature 553:1-2 (slow slip review)
    - Ito et al. (2013) Science 339:1206-1209 (slow slip + tide gauge)
"""

import asyncio
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

# IOC Sea Level Station Monitoring Facility API
IOC_BASE = "https://www.ioc-sealevelmonitoring.org/service.php"

# Japan bounding box for station filtering
JAPAN_LAT_MIN = 20.0
JAPAN_LAT_MAX = 50.0
JAPAN_LON_MIN = 120.0
JAPAN_LON_MAX = 155.0

# How many days of recent data to fetch per station
FETCH_DAYS = 45

# Maximum number of stations to process (time budget constraint)
MAX_STATIONS = 30

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)

# Delay between station data requests (rate limit compliance)
REQUEST_DELAY_SEC = 1.0


async def init_ioc_sealevel_table():
    """Create IOC sea level table and indices."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ioc_sea_level (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_code TEXT NOT NULL,
                station_name TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                sea_level_m REAL NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(station_code, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_ioc_sealevel_time
            ON ioc_sea_level(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_ioc_sealevel_station
            ON ioc_sea_level(station_code)
        """)
        await db.commit()


async def fetch_station_list(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch IOC station list and filter to Japan area.

    Returns list of dicts with keys: code, name, lat, lon.
    """
    params = {
        "query": "stationlist",
        "showall": "all",
        "format": "json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(IOC_BASE, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    break
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning("IOC station list: HTTP %d", resp.status)
                        return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("IOC station list: %s", type(e).__name__)
                return []
            await asyncio.sleep(2 ** attempt)
    else:
        return []

    if not data:
        logger.warning("IOC station list: empty response")
        return []

    # Filter to Japan area
    japan_stations = []
    for station in data:
        try:
            lat = float(station.get("lat", 0))
            lon = float(station.get("lon", 0))
        except (ValueError, TypeError):
            continue

        if (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
            code = station.get("code", "").strip()
            name = station.get("name", "").strip()
            if code:
                japan_stations.append({
                    "code": code,
                    "name": name or code,
                    "lat": lat,
                    "lon": lon,
                })

    logger.info("IOC stations in Japan area: %d (of %d total)",
                len(japan_stations), len(data))

    # Cap to MAX_STATIONS, preferring stations sorted by code for reproducibility
    japan_stations.sort(key=lambda s: s["code"])
    if len(japan_stations) > MAX_STATIONS:
        japan_stations = japan_stations[:MAX_STATIONS]
        logger.info("Capped to %d stations", MAX_STATIONS)

    return japan_stations


def parse_ioc_data(data: list, station: dict) -> list[dict]:
    """Parse IOC data API response into records.

    IOC data format (JSON array of objects):
        {"stime": "2024-01-01 00:00:00", "slevel": "1.234", ...}

    Returns list of dicts with keys: observed_at, sea_level_m.
    """
    rows = []
    for entry in data:
        try:
            time_str = entry.get("stime", "").strip()
            level_str = entry.get("slevel", "")

            if not time_str or level_str is None or level_str == "":
                continue

            sea_level = float(level_str)

            # Normalise timestamp to ISO format
            # IOC format: "YYYY-MM-DD HH:MM:SS"
            observed_at = time_str.replace(" ", "T")

            rows.append({
                "observed_at": observed_at,
                "sea_level_m": sea_level,
            })
        except (ValueError, TypeError, AttributeError):
            continue

    return rows


async def fetch_station_data(session: aiohttp.ClientSession,
                              station: dict,
                              time_start: str,
                              time_stop: str) -> list[dict]:
    """Fetch sea level data for one IOC station.

    Args:
        session: aiohttp session.
        station: dict with code, name, lat, lon.
        time_start: start time string (YYYY-MM-DD HH:MM:SS).
        time_stop: end time string (YYYY-MM-DD HH:MM:SS).

    Returns list of parsed records.
    """
    params = {
        "query": "data",
        "code": station["code"],
        "timestart": time_start,
        "timestop": time_stop,
        "format": "json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(IOC_BASE, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Handle empty or non-JSON responses
                    if not text.strip() or text.strip().startswith("<"):
                        logger.info("  %s (%s): empty or HTML response",
                                    station["code"], station["name"])
                        return []
                    import json
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        logger.info("  %s (%s): invalid JSON",
                                    station["code"], station["name"])
                        return []
                    if not isinstance(data, list):
                        logger.info("  %s (%s): unexpected format",
                                    station["code"], station["name"])
                        return []
                    return parse_ioc_data(data, station)
                elif resp.status == 404:
                    logger.info("  %s (%s): not available (404)",
                                station["code"], station["name"])
                    return []
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning("  %s (%s): HTTP %d",
                                       station["code"], station["name"], resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("  %s (%s): %s",
                               station["code"], station["name"], type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


async def main():
    await init_db()
    await init_ioc_sealevel_table()

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # Time window for data fetch
    time_start = (now - timedelta(days=FETCH_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    time_stop = now.strftime("%Y-%m-%d %H:%M:%S")

    # Check existing data summary
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT station_code, COUNT(*), MAX(observed_at) "
            "FROM ioc_sea_level GROUP BY station_code"
        )
    existing_summary = {r[0]: (r[1], r[2]) for r in existing} if existing else {}
    logger.info("IOC sea level existing: %d stations", len(existing_summary))

    # Fetch station list
    async with aiohttp.ClientSession() as session:
        stations = await fetch_station_list(session)

    if not stations:
        logger.warning("No IOC stations found in Japan area; aborting")
        return

    # Fetch data for each station
    total_records = 0
    stations_with_data = 0

    async with aiohttp.ClientSession() as session:
        for station in stations:
            logger.info("Fetching %s (%s, %.2f°N %.2f°E)...",
                        station["code"], station["name"],
                        station["lat"], station["lon"])

            rows = await fetch_station_data(
                session, station, time_start, time_stop
            )

            if not rows:
                logger.info("  %s: no data returned", station["code"])
                await asyncio.sleep(REQUEST_DELAY_SEC)
                continue

            # Store in database
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO ioc_sea_level
                       (station_code, station_name, observed_at,
                        sea_level_m, latitude, longitude, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [(station["code"], station["name"], r["observed_at"],
                      r["sea_level_m"], station["lat"], station["lon"],
                      now_iso) for r in rows],
                )
                await db.commit()

            total_records += len(rows)
            stations_with_data += 1
            logger.info("  %s: %d records (%.2f - %.2f m)",
                        station["name"], len(rows),
                        min(r["sea_level_m"] for r in rows),
                        max(r["sea_level_m"] for r in rows))

            await asyncio.sleep(REQUEST_DELAY_SEC)

    logger.info("IOC sea level fetch complete: %d records from %d/%d stations",
                total_records, stations_with_data, len(stations))


if __name__ == "__main__":
    asyncio.run(main())
