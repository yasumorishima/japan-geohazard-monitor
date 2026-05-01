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
import json
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# IOC Sea Level Station Monitoring Facility API
IOC_BASE = "https://www.ioc-sealevelmonitoring.org/service.php"

# Japan bounding box for station filtering
JAPAN_LAT_MIN = 20.0
JAPAN_LAT_MAX = 50.0
JAPAN_LON_MIN = 120.0
JAPAN_LON_MAX = 155.0

# Maximum number of stations to process (time budget constraint)
MAX_STATIONS = 30

# Backfill start date — IOC SLSMF historical data is broadly available from 2011
BACKFILL_START = datetime(2011, 1, 1)

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)

# Phase 2 (1) acceleration constants (gnss_tec PR #114 と同型)
MAX_RETRIES_BEFORE_SKIP = 3
FAILED_DATES_RETRY_AFTER_DAYS = 30
PARALLEL_FETCHES = int(os.environ.get("IOC_PARALLEL_FETCHES", "2"))
RATE_LIMIT_SLEEP = float(os.environ.get("IOC_RATE_LIMIT_SLEEP", "1.0"))
MAX_FETCHES = int(os.environ.get("IOC_MAX_FETCHES", "200"))


async def init_ioc_sealevel_table():
    """Create IOC sea level data and failure-tracking tables and indices."""
    async with safe_connect() as db:
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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ioc_sealevel_failed_dates (
                station_code TEXT NOT NULL,
                date_str TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_failed_at TEXT NOT NULL,
                PRIMARY KEY (station_code, date_str)
            )
        """)
        await db.commit()


async def get_failed_pairs() -> set[tuple[str, str]]:
    """Return (station_code, date_str) pairs to skip on this run.

    Pairs whose last_failed_at is older than FAILED_DATES_RETRY_AFTER_DAYS roll
    out of the skip set so previously-empty dates that become available later
    can be re-fetched without manual intervention.
    """
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(days=FAILED_DATES_RETRY_AFTER_DAYS)
    ).isoformat()
    async with safe_connect() as db:
        rows = await db.execute_fetchall(
            "SELECT station_code, date_str FROM ioc_sealevel_failed_dates "
            "WHERE retry_count >= ? AND last_failed_at > ?",
            (MAX_RETRIES_BEFORE_SKIP, cutoff_iso),
        )
    return {(r[0], r[1]) for r in rows}


async def mark_failed_pair(station_code: str, date_str: str) -> None:
    """Record a 0-record fetch for (station_code, date_str); increment retry_count."""
    now_iso = datetime.now(timezone.utc).isoformat()
    async with safe_connect() as db:
        await db.execute(
            "INSERT INTO ioc_sealevel_failed_dates "
            "(station_code, date_str, retry_count, last_failed_at) "
            "VALUES (?, ?, 1, ?) "
            "ON CONFLICT(station_code, date_str) DO UPDATE SET "
            "retry_count = retry_count + 1, "
            "last_failed_at = excluded.last_failed_at",
            (station_code, date_str, now_iso),
        )
        await db.commit()


async def get_existing_dates_per_station() -> dict[str, set[str]]:
    """Return {station_code: {date_str, ...}} computed from existing rows."""
    async with safe_connect() as db:
        rows = await db.execute_fetchall(
            "SELECT station_code, DATE(observed_at) AS d "
            "FROM ioc_sea_level GROUP BY station_code, d"
        )
    existing: dict[str, set[str]] = {}
    for code, d in rows:
        if d:
            existing.setdefault(code, set()).add(d)
    return existing


async def fetch_station_list(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch IOC station list and filter to Japan area.

    Returns list of dicts with keys: code, name, lat, lon.
    """
    params = {
        "query": "stationlist",
        "showall": "all",
        "format": "json",
    }

    data = None
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

    if not data:
        logger.warning("IOC station list: empty response")
        return []

    # Handle both list and dict response formats
    if isinstance(data, dict):
        # API might wrap stations in a key
        for key in ("stations", "data", "results"):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
        else:
            logger.warning("IOC station list: unexpected dict format (keys: %s)",
                          list(data.keys())[:10])
            return []

    if not isinstance(data, list):
        logger.warning("IOC station list: unexpected type %s", type(data).__name__)
        return []

    # Filter to Japan area
    japan_stations = []
    for station in data:
        if not isinstance(station, dict):
            continue
        try:
            lat = float(station.get("lat") or station.get("Lat") or 0)
            lon = float(station.get("lon") or station.get("Lon") or 0)
        except (ValueError, TypeError):
            continue

        if (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
            # Handle None/non-string code values safely
            raw_code = station.get("code") or station.get("Code") or ""
            code = str(raw_code).strip() if raw_code else ""
            raw_name = station.get("name") or station.get("Location") or ""
            name = str(raw_name).strip() if raw_name else ""
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
    """Fetch sea level data for one IOC station within a time range.

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
                        return []
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        return []
                    if not isinstance(data, list):
                        return []
                    return parse_ioc_data(data, station)
                elif resp.status == 404:
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


def build_target_pairs(
    all_dates: list[datetime],
    stations: list[dict],
    existing_per_station: dict[str, set[str]],
    failed_pairs: set[tuple[str, str]],
    max_fetches: int,
) -> list[tuple[datetime, dict]]:
    """Compute oldest-first (date, station) pairs to fetch this run.

    Iterates dates outermost so all stations advance together rather than one
    station racing ahead. Skips pairs already in existing rows or in the
    failed-dates retry-skip set.
    """
    target: list[tuple[datetime, dict]] = []
    for date in all_dates:
        date_str = date.strftime("%Y-%m-%d")
        for station in stations:
            code = station["code"]
            if date_str in existing_per_station.get(code, set()):
                continue
            if (code, date_str) in failed_pairs:
                continue
            target.append((date, station))
            if len(target) >= max_fetches:
                return target
    return target


async def main():
    """Backfill IOC sea level data oldest-first from 2011 across all stations."""
    await init_db()
    await init_ioc_sealevel_table()

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # Fetch station list
    async with aiohttp.ClientSession() as session:
        stations = await fetch_station_list(session)

    if not stations:
        logger.warning("No IOC stations found in Japan area; aborting")
        return

    existing_per_station = await get_existing_dates_per_station()
    failed_pairs = await get_failed_pairs()

    # Build target date list (BACKFILL_START .. yesterday UTC)
    end_date = (
        datetime.now(timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        - timedelta(days=1)
    )
    total_days = (end_date - BACKFILL_START).days + 1
    all_dates = [BACKFILL_START + timedelta(days=i) for i in range(total_days)]

    target_pairs = build_target_pairs(
        all_dates, stations, existing_per_station, failed_pairs, MAX_FETCHES,
    )

    total_existing_pairs = sum(len(s) for s in existing_per_station.values())
    logger.info(
        "IOC sea level: %d stations, %d existing (station, date) pairs, %d failed-skip pairs",
        len(stations), total_existing_pairs, len(failed_pairs),
    )
    logger.info(
        "Fetching %d (date, station) pairs with parallelism=%d, rate_limit_sleep=%.2fs",
        len(target_pairs), PARALLEL_FETCHES, RATE_LIMIT_SLEEP,
    )

    if not target_pairs:
        logger.info("No new (date, station) pairs to fetch")
        return

    sem = asyncio.Semaphore(PARALLEL_FETCHES)

    async def fetch_one(session: aiohttp.ClientSession,
                         date: datetime, station: dict):
        async with sem:
            date_str = date.strftime("%Y-%m-%d")
            time_start = f"{date_str} 00:00:00"
            time_stop = f"{date_str} 23:59:59"
            rows = await fetch_station_data(
                session, station, time_start, time_stop,
            )
            # Per-fetch rate-limit sleep stays inside semaphore so concurrent
            # workers each pace at RATE_LIMIT_SLEEP rather than burst-and-stop.
            await asyncio.sleep(RATE_LIMIT_SLEEP)
            return date, station, rows

    total_records = 0
    inserted_pairs = 0
    failed_count = 0

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, d, s) for d, s in target_pairs]
        for coro in asyncio.as_completed(tasks):
            date, station, rows = await coro
            date_str = date.strftime("%Y-%m-%d")
            code = station["code"]
            if rows:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO ioc_sea_level
                           (station_code, station_name, observed_at,
                            sea_level_m, latitude, longitude, received_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        [(code, station["name"], r["observed_at"],
                          r["sea_level_m"], station["lat"], station["lon"],
                          now_iso) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)
                inserted_pairs += 1
                if inserted_pairs % 20 == 0 or inserted_pairs <= 5:
                    logger.info(
                        "  %s/%s: %d records (cumulative: %d records / %d pairs)",
                        code, date_str, len(rows), total_records, inserted_pairs,
                    )
            else:
                await mark_failed_pair(code, date_str)
                failed_count += 1
                if failed_count % 20 == 0 or failed_count <= 5:
                    logger.info(
                        "  %s/%s: 0 records (marked failed, cumulative failed: %d)",
                        code, date_str, failed_count,
                    )

    logger.info(
        "IOC sea level fetch complete: %d records / %d pairs inserted, %d pairs failed",
        total_records, inserted_pairs, failed_count,
    )


if __name__ == "__main__":
    asyncio.run(main())
