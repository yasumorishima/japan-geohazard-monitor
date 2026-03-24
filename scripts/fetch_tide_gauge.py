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
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# UHSLC ERDDAP endpoint for Fast Delivery hourly data
ERDDAP_BASE = "https://uhslc.soest.hawaii.edu/erddap/tabledap/global_hourly_fast"

# UHSLC direct CSV (fallback when ERDDAP times out)
UHSLC_CSV_BASE = "https://uhslc.soest.hawaii.edu/data/csv/fast/hourly"

# Japan bounding box
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 156.0

# Fetch 90 days per request to keep response size manageable
CHUNK_DAYS = 90

MAX_RETRIES = 5
TIMEOUT = aiohttp.ClientTimeout(total=900, connect=120)

START_YEAR = 2011

# Known Japan UHSLC stations (fallback when ERDDAP discovery fails)
JAPAN_STATIONS_FALLBACK = [
    {"name": "Abashiri", "uhslc_id": 347, "lat": 44.017, "lon": 144.283},
    {"name": "Aburatsu", "uhslc_id": 354, "lat": 31.567, "lon": 131.417},
    {"name": "Chichijima", "uhslc_id": 47, "lat": 27.1, "lon": 142.183},
    {"name": "Hakodate", "uhslc_id": 364, "lat": 41.783, "lon": 140.733},
    {"name": "Hamada", "uhslc_id": 348, "lat": 34.9, "lon": 132.067},
    {"name": "Ishigaki", "uhslc_id": 365, "lat": 24.333, "lon": 124.15},
    {"name": "Kushimoto", "uhslc_id": 353, "lat": 33.467, "lon": 135.783},
    {"name": "Kushiro", "uhslc_id": 350, "lat": 42.967, "lon": 144.383},
    {"name": "Maisaka", "uhslc_id": 356, "lat": 34.683, "lon": 137.617},
    {"name": "Mera", "uhslc_id": 352, "lat": 34.917, "lon": 139.833},
    {"name": "Minamitorishima", "uhslc_id": 49, "lat": 24.3, "lon": 153.967},
    {"name": "Nagasaki", "uhslc_id": 362, "lat": 32.733, "lon": 129.867},
    {"name": "Naha", "uhslc_id": 355, "lat": 26.217, "lon": 127.667},
    {"name": "Nakano Shima", "uhslc_id": 345, "lat": 29.833, "lon": 129.85},
    {"name": "Naze", "uhslc_id": 359, "lat": 28.378, "lon": 129.498},
    {"name": "Nishinoomote", "uhslc_id": 363, "lat": 30.732, "lon": 130.995},
    {"name": "Ofunato", "uhslc_id": 351, "lat": 39.067, "lon": 141.717},
    {"name": "Toyama", "uhslc_id": 349, "lat": 36.767, "lon": 137.217},
    {"name": "Wakkanai", "uhslc_id": 360, "lat": 45.4, "lon": 141.683},
]


async def init_tide_table():
    """Create tide gauge table."""
    async with safe_connect() as db:
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


async def fetch_station_csv(session: aiohttp.ClientSession,
                             uhslc_id: int,
                             start_year: int) -> list[dict]:
    """Fetch hourly sea level data via direct UHSLC CSV (fallback).

    CSV format: year,month,day,hour,sea_level_mm (no header).
    Missing values are encoded as -32767.
    """
    url = f"{UHSLC_CSV_BASE}/h{uhslc_id:03d}.csv"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return _parse_direct_csv(text, start_year)
                elif resp.status == 404:
                    return []
                else:
                    if attempt == MAX_RETRIES:
                        logger.debug("UHSLC CSV %d: HTTP %d", uhslc_id, resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("UHSLC CSV %d: %s", uhslc_id, type(e).__name__)
            await asyncio.sleep(2 ** attempt)
    return []


def _parse_direct_csv(text: str, start_year: int) -> list[dict]:
    """Parse UHSLC direct CSV: year,month,day,hour,sea_level_mm."""
    rows = []
    for line in text.strip().split("\n"):
        parts = line.strip().split(",")
        if len(parts) < 5:
            continue
        try:
            year = int(parts[0])
            if year < start_year:
                continue
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3])
            sea_level = float(parts[4])

            if sea_level <= -9000 or sea_level > 90000:
                continue  # Missing or invalid

            observed_at = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:00:00"
            rows.append({
                "observed_at": observed_at,
                "sea_level_mm": sea_level,
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
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT station_id, COUNT(*), MAX(observed_at) FROM tide_gauge GROUP BY station_id"
        )
    existing_summary = {r[0]: (r[1], r[2]) for r in existing} if existing else {}
    logger.info("Tide gauge existing: %d stations", len(existing_summary))

    # Discover Japan stations from ERDDAP
    async with aiohttp.ClientSession() as session:
        stations = await discover_japan_stations(session)

    use_csv_fallback = False
    if not stations:
        logger.warning("ERDDAP discovery failed, using fallback station list + direct CSV")
        stations = JAPAN_STATIONS_FALLBACK
        use_csv_fallback = True
    else:
        logger.info("Found %d UHSLC stations via ERDDAP", len(stations))

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for station in stations:
            station_key = str(station["uhslc_id"])
            name = station["name"]

            # Determine start time (must be timezone-aware to compare with now)
            if station_key in existing_summary:
                last_date = existing_summary[station_key][1]
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

            logger.info("Fetching %s (ID=%s, %.2f°N %.2f°E) from %s%s",
                        name, station_key, station["lat"], station["lon"],
                        start_dt.strftime("%Y-%m-%d"),
                        " [CSV fallback]" if use_csv_fallback else "")

            station_total = 0

            if use_csv_fallback:
                # Direct CSV: download entire file, filter by start year
                rows = await fetch_station_csv(
                    session, station["uhslc_id"], start_dt.year
                )
                # Filter out already-existing records
                if station_key in existing_summary:
                    last_obs = existing_summary[station_key][1]
                    rows = [r for r in rows if r["observed_at"] > last_obs]

                if rows:
                    async with safe_connect() as db:
                        await db.executemany(
                            """INSERT OR IGNORE INTO tide_gauge
                               (station_id, observed_at, sea_level_mm,
                                latitude, longitude, received_at)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            [(station_key, r["observed_at"], r["sea_level_mm"],
                              station["lat"], station["lon"], now_iso) for r in rows],
                        )
                        await db.commit()
                    station_total = len(rows)
            else:
                # ERDDAP: chunked time range queries
                current = start_dt
                while current < now:
                    chunk_end = min(current + timedelta(days=CHUNK_DAYS), now)
                    time_start = current.strftime("%Y-%m-%dT00:00:00Z")
                    time_end = chunk_end.strftime("%Y-%m-%dT23:59:59Z")

                    rows = await fetch_station_data(
                        session, station["uhslc_id"], time_start, time_end
                    )

                    if rows:
                        async with safe_connect() as db:
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
                    await asyncio.sleep(0.5)

            if station_total > 0:
                total_records += station_total
                logger.info("  %s: %d records", name, station_total)

            if use_csv_fallback:
                await asyncio.sleep(0.5)  # Rate limit for direct CSV

    logger.info("Tide gauge fetch complete: %d total records from %d stations",
                total_records, len(stations))


if __name__ == "__main__":
    asyncio.run(main())
