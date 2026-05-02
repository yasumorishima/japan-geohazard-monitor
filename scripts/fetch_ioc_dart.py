"""Fetch deep-ocean DART ocean bottom pressure (OBP) via IOC SLSMF.

NOAA NDBC's DART realtime feed (fetch_dart_pressure.py) only exposes the
last ~45 days. The IOC Sea Level Station Monitoring Facility republishes
several DART buoys under their own 4-letter codes ("dtok", "dtok2",
"dryu", "dryu2") with `sensor="prt"` and the same physical observable —
water column height in metres — going back to 2011. This fetcher
backfills `dart_pressure` from IOC for the historical range while NDBC
keeps owning the realtime 45-day window.

Stage 2.B (Phase 2 (3) `dart_pressure` IOC integration):
    - Coexists with fetch_dart_pressure.py (NDBC). The two sources use
      disjoint station_id namespaces (NDBC = numeric "21413" / IOC = 4
      letters "dtok"), so UNIQUE(station_id, observed_at) cannot collide.
    - Same Phase 2 (1) acceleration shape as fetch_ioc_sealevel.py:
      Semaphore parallelism + oldest-first iteration + per-pair
      retry-after via dart_pressure_failed_dates + TRANSIENT_FAILURE
      sentinel for transient HTTP failures.
    - sensor allow-list is `frozenset({"prt"})` only, with defense-in-depth
      at both station_list and per-record level.

Schema reuse:
    - dart_pressure.water_height_m  <- IOC `slevel`
    - dart_pressure.station_id      <- IOC `code` ("dtok" etc.)
    - dart_pressure.measurement_type = 1 (15-min, IOC SLSMF DART grain)

References:
    - IOC/UNESCO Sea Level Station Monitoring Facility (SLSMF)
    - Stage 2.A: PR #119 introduced the sensor allow-list in
      fetch_ioc_sealevel.py that excluded `prt` from sea-level data.
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

IOC_BASE = "https://www.ioc-sealevelmonitoring.org/service.php"

# Japan + western Pacific bounding box covering known IOC DART buoys
# (dtok ≈ 30N 152E, dryu ≈ 29N 135E). The wider band picks up future
# additions like Kuril / Philippine basin DARTs without code changes.
JAPAN_LAT_MIN = 20.0
JAPAN_LAT_MAX = 50.0
JAPAN_LON_MIN = 120.0
JAPAN_LON_MAX = 165.0

# Time-budget cap on stations per run. Today only 4 IOC DART buoys have
# usable codes (dtok, dtok2, dryu, dryu2); the cap leaves head room for
# future additions without uncapped fan-out.
MAX_STATIONS = 20

# Backfill from 2011-01-01 (matches BACKFILL_START in fetch_ioc_sealevel).
BACKFILL_START = datetime(2011, 1, 1)

# Allow-list intentionally `prt` only. fetch_ioc_sealevel.py owns the
# coastal tide-gauge sensors; this fetcher must NEVER write a non-DART
# sensor into dart_pressure (per-record check below is defense-in-depth
# in case IOC merges multi-sensor streams under one code).
ALLOWED_SENSORS = frozenset({"prt"})

# IOC SLSMF DART grain is 15-min; record this constant so the column
# meaning matches NDBC's measurement_type=1 convention.
DART_MEASUREMENT_TYPE = 1

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)

# Phase 2 (1) acceleration constants (mirror fetch_ioc_sealevel.py).
MAX_RETRIES_BEFORE_SKIP = 3
FAILED_DATES_RETRY_AFTER_DAYS = 30
PARALLEL_FETCHES = int(os.environ.get("IOC_DART_PARALLEL_FETCHES", "2"))
RATE_LIMIT_SLEEP = float(os.environ.get("IOC_DART_RATE_LIMIT_SLEEP", "1.0"))
MAX_FETCHES = int(os.environ.get("IOC_DART_MAX_FETCHES", "200"))


class _TransientFailure:
    """Sentinel for transient HTTP failures (5xx, timeouts, conn errors,
    HTML error pages, JSON decode errors). Distinguished from an empty
    list (definitive 200-OK no-data / 404) so the main loop can avoid
    burning a retry_count slot on a transient.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<TRANSIENT_FAILURE>"


TRANSIENT_FAILURE = _TransientFailure()


async def init_dart_tables():
    """Ensure dart_pressure + dart_pressure_failed_dates tables exist.

    `dart_pressure` is the same schema used by fetch_dart_pressure.py
    (NDBC); we only `CREATE TABLE IF NOT EXISTS` so a coexisting NDBC
    fetcher's schema wins on first init.
    """
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS dart_pressure (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                water_height_m REAL NOT NULL,
                measurement_type INTEGER,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(station_id, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dart_pressure_time
            ON dart_pressure(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dart_pressure_station
            ON dart_pressure(station_id)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS dart_pressure_failed_dates (
                station_id TEXT NOT NULL,
                date_str TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_failed_at TEXT NOT NULL,
                PRIMARY KEY (station_id, date_str)
            )
        """)
        await db.commit()


async def get_failed_pairs() -> set[tuple[str, str]]:
    """Return (station_id, date_str) pairs to skip on this run.

    Pairs whose last_failed_at is older than FAILED_DATES_RETRY_AFTER_DAYS
    roll out of the skip set so previously-empty dates that become
    available later can be re-fetched without manual intervention.
    """
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(days=FAILED_DATES_RETRY_AFTER_DAYS)
    ).isoformat()
    async with safe_connect() as db:
        rows = await db.execute_fetchall(
            "SELECT station_id, date_str FROM dart_pressure_failed_dates "
            "WHERE retry_count >= ? AND last_failed_at > ?",
            (MAX_RETRIES_BEFORE_SKIP, cutoff_iso),
        )
    return {(r[0], r[1]) for r in rows}


async def mark_failed_pair(station_id: str, date_str: str) -> None:
    """Record a 0-record fetch for (station_id, date_str); increment retry_count."""
    now_iso = datetime.now(timezone.utc).isoformat()
    async with safe_connect() as db:
        await db.execute(
            "INSERT INTO dart_pressure_failed_dates "
            "(station_id, date_str, retry_count, last_failed_at) "
            "VALUES (?, ?, 1, ?) "
            "ON CONFLICT(station_id, date_str) DO UPDATE SET "
            "retry_count = retry_count + 1, "
            "last_failed_at = excluded.last_failed_at",
            (station_id, date_str, now_iso),
        )
        await db.commit()


async def get_existing_dates_per_station() -> dict[str, set[str]]:
    """Return {station_id: {date_str, ...}} for IOC-style codes already
    written to dart_pressure.

    NDBC numeric station_ids (e.g. "21413") share the same column but use
    a disjoint namespace from IOC 4-letter codes; we still return them
    so the build_target_pairs skip logic correctly excludes any cross
    namespace dates if a future migration unifies the IDs.
    """
    async with safe_connect() as db:
        rows = await db.execute_fetchall(
            "SELECT station_id, DATE(observed_at) AS d "
            "FROM dart_pressure GROUP BY station_id, d"
        )
    existing: dict[str, set[str]] = {}
    for sid, d in rows:
        if d:
            existing.setdefault(sid, set()).add(d)
    return existing


async def fetch_station_list(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch IOC station list and filter to Japan area + sensor=prt.

    Returns list of dicts with keys: code, name, lat, lon, sensor.
    Stations without a `code` field are skipped (IOC sometimes lists
    unmapped buoys with sensor data but no operator-assigned code, which
    cannot be used as a stable station_id).
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

    if isinstance(data, dict):
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

    japan_stations: list[dict] = []
    excluded_by_sensor = 0
    excluded_no_code = 0
    for station in data:
        if not isinstance(station, dict):
            continue
        try:
            lat = float(station.get("lat") or station.get("Lat") or 0)
            lon = float(station.get("lon") or station.get("Lon") or 0)
        except (ValueError, TypeError):
            continue
        if not (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
            continue

        raw_sensor = station.get("sensor")
        sensor = str(raw_sensor).strip().lower() if raw_sensor else ""
        if sensor not in ALLOWED_SENSORS:
            excluded_by_sensor += 1
            continue

        raw_code = station.get("code") or station.get("Code") or ""
        code = str(raw_code).strip() if raw_code else ""
        if not code:
            excluded_no_code += 1
            continue
        raw_name = station.get("name") or station.get("Location") or ""
        name = str(raw_name).strip() if raw_name else ""
        japan_stations.append({
            "code": code,
            "name": name or code,
            "lat": lat,
            "lon": lon,
            "sensor": sensor,
        })

    logger.info(
        "IOC DART stations in Japan area: %d allowed (of %d total, "
        "excluded: %d by sensor, %d by missing code)",
        len(japan_stations), len(data), excluded_by_sensor, excluded_no_code,
    )

    japan_stations.sort(key=lambda s: s["code"])
    if len(japan_stations) > MAX_STATIONS:
        japan_stations = japan_stations[:MAX_STATIONS]
        logger.info("Capped to %d stations", MAX_STATIONS)

    return japan_stations


def parse_ioc_data(data: list, station: dict) -> list[dict]:
    """Parse IOC data API response into DART OBP records.

    IOC payload entry: {"slevel": 5779.78, "stime": "2026-04-15 00:00:00", "sensor": "prt"}

    Defense-in-depth: skip records whose per-record `sensor` field is NOT
    in ALLOWED_SENSORS. Without this, a buoy that intermittently reports
    a coastal sensor under the same code would leak non-OBP values into
    dart_pressure.
    """
    rows = []
    for entry in data:
        try:
            time_str = entry.get("stime", "").strip()
            level_str = entry.get("slevel", "")

            if not time_str or level_str is None or level_str == "":
                continue

            entry_sensor = entry.get("sensor")
            if entry_sensor is not None:
                entry_sensor_norm = str(entry_sensor).strip().lower()
                if entry_sensor_norm and entry_sensor_norm not in ALLOWED_SENSORS:
                    continue

            water_height = float(level_str)

            observed_at = time_str.replace(" ", "T")

            rows.append({
                "observed_at": observed_at,
                "water_height_m": water_height,
            })
        except (ValueError, TypeError, AttributeError):
            continue

    return rows


async def fetch_station_data(
    session: aiohttp.ClientSession,
    station: dict,
    time_start: str,
    time_stop: str,
) -> "list[dict] | _TransientFailure":
    """Fetch DART OBP records for one IOC station within a time range.

    Returns:
        list[dict]:        Parsed records (possibly empty for definitive
                           200-OK no-data or 404).
        TRANSIENT_FAILURE: 5xx / 429 / timeout / connection error after
                           MAX_RETRIES, or 200 OK with HTML error page /
                           JSON decode error / non-list payload.
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
                    stripped = text.strip()
                    if not stripped:
                        return []
                    if stripped.startswith("<"):
                        if attempt == MAX_RETRIES:
                            logger.warning(
                                "  %s (%s): 200 OK with HTML body (transient)",
                                station["code"], station["name"],
                            )
                            return TRANSIENT_FAILURE
                        await asyncio.sleep(2 ** attempt)
                        continue
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        if attempt == MAX_RETRIES:
                            logger.warning(
                                "  %s (%s): JSON decode error (transient)",
                                station["code"], station["name"],
                            )
                            return TRANSIENT_FAILURE
                        await asyncio.sleep(2 ** attempt)
                        continue
                    if not isinstance(data, list):
                        if attempt == MAX_RETRIES:
                            logger.warning(
                                "  %s (%s): non-list payload %s (transient)",
                                station["code"], station["name"],
                                type(data).__name__,
                            )
                            return TRANSIENT_FAILURE
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return parse_ioc_data(data, station)
                elif resp.status == 404:
                    return []
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning("  %s (%s): HTTP %d (transient)",
                                       station["code"], station["name"], resp.status)
                        return TRANSIENT_FAILURE
                    await asyncio.sleep(2 ** attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("  %s (%s): %s (transient)",
                               station["code"], station["name"], type(e).__name__)
                return TRANSIENT_FAILURE
            await asyncio.sleep(2 ** attempt)

    return TRANSIENT_FAILURE


def build_target_pairs(
    all_dates: list[datetime],
    stations: list[dict],
    existing_per_station: dict[str, set[str]],
    failed_pairs: set[tuple[str, str]],
    max_fetches: int,
) -> list[tuple[datetime, dict]]:
    """Compute oldest-first (date, station) pairs to fetch this run."""
    if max_fetches <= 0:
        return []
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
    """Backfill DART OBP from IOC oldest-first from 2011 across all stations."""
    await init_db()
    await init_dart_tables()

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    async with aiohttp.ClientSession() as session:
        stations = await fetch_station_list(session)

    if not stations:
        logger.warning("No IOC DART stations found in Japan area; aborting")
        return

    existing_per_station = await get_existing_dates_per_station()
    failed_pairs = await get_failed_pairs()

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
        "IOC DART: %d stations, %d existing (station, date) pairs, %d failed-skip pairs",
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
            await asyncio.sleep(RATE_LIMIT_SLEEP)
            return date, station, rows

    total_records = 0
    inserted_pairs = 0
    failed_count = 0
    transient_skipped = 0

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, d, s) for d, s in target_pairs]
        for coro in asyncio.as_completed(tasks):
            date, station, rows = await coro
            date_str = date.strftime("%Y-%m-%d")
            code = station["code"]
            if rows is TRANSIENT_FAILURE:
                transient_skipped += 1
                if transient_skipped <= 5 or transient_skipped % 20 == 0:
                    logger.info(
                        "  %s/%s: transient failure (not marked, cumulative: %d)",
                        code, date_str, transient_skipped,
                    )
            elif rows:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO dart_pressure
                           (station_id, observed_at, water_height_m,
                            measurement_type, latitude, longitude, received_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        [(code, r["observed_at"], r["water_height_m"],
                          DART_MEASUREMENT_TYPE,
                          station["lat"], station["lon"],
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
        "IOC DART fetch complete: %d records / %d pairs inserted, "
        "%d pairs failed (definitive), %d pairs transient-skipped",
        total_records, inserted_pairs, failed_count, transient_skipped,
    )


if __name__ == "__main__":
    asyncio.run(main())
