"""Fetch high-resolution GNSS-TEC data from Nagoya University ISEE.

Downloads 0.25°×0.25° absolute TEC (VTEC) and detrended TEC (dTEC) grid data
over Japan from the Nagoya University Space Environment Database (ISEE).
This is 25x higher spatial resolution than CODE IONEX (2.5°×5°) used in Phase 1.

Data source: https://stdb2.isee.nagoya-u.ac.jp/GPS/GPS-TEC/
Confirmed working URL patterns (2026-03-16):
    VTEC: /GPS/shinbori/AGRID2/nc/{year}/{doy}/{YYYYMMDD}{HH}_atec.nc
    dTEC: /GPS/shinbori/GRID2/nc/{year}/{doy}/{YYYYMMDD}{HH}_dtec.nc
    ROTI: /GPS/shinbori/RGRID2/nc/{year}/{doy}/{YYYYMMDD}{HH}_roti.nc

Resolution: 0.25° spatial, 1-hour temporal
Coverage: 1993-present (atec), 2019-present (dtec), near-real-time updates
File size: ~12MB per hour per product
No authentication required.

Strategy: fetch 1 representative hour per date (12 UT = 21 JST nighttime,
or 03 UT = 12 JST daytime) to manage data volume. Priority: VTEC for
absolute values, dTEC for detrended anomaly detection.

Phase 2 (1) acceleration (2026-04-30):
    - GNSS_TEC_MAX_DATES default 30 -> 200 (6.7x throughput)
    - Parallel HTTP fetch via asyncio.Semaphore (4 in-flight dates)
    - Per-date rate-limit sleep 2.0s -> 0.5s
    - gnss_tec_failed_dates table to skip 0-record dates after 3 retries
    - 30-day retry reset so archive backfill is not permanently blocked
"""

import asyncio
import io
import logging
import os as _os
import struct
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

# Nagoya University ISEE GNSS-TEC archive (confirmed working 2026-03-16)
NAGOYA_BASE = "https://stdb2.isee.nagoya-u.ac.jp/GPS/shinbori"

# Japan bounding box for filtering
JAPAN_BBOX = {"min_lat": 25.0, "max_lat": 46.0, "min_lon": 125.0, "max_lon": 150.0}

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)

# Hours to fetch per day (UT). 03 UT = 12 JST (daytime), 12 UT = 21 JST (nighttime)
# Fetch both for day/night comparison
FETCH_HOURS = [3, 12]

# Phase 2 (1) acceleration constants
MAX_RETRIES_BEFORE_SKIP = 3
FAILED_DATES_RETRY_AFTER_DAYS = 30
PARALLEL_DATES = int(_os.environ.get("GNSS_TEC_PARALLEL_DATES", "4"))
RATE_LIMIT_SLEEP = float(_os.environ.get("GNSS_TEC_RATE_LIMIT_SLEEP", "0.5"))


async def init_gnss_tec_table():
    """Create GNSS-TEC data and failure-tracking tables if not exist."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS gnss_tec (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                tec_tecu REAL NOT NULL,
                dtec_tecu REAL,
                roti REAL,
                epoch TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'nagoya',
                received_at TEXT NOT NULL,
                UNIQUE(latitude, longitude, epoch, source)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_gnss_tec_epoch
            ON gnss_tec(epoch)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_gnss_tec_location
            ON gnss_tec(latitude, longitude)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS gnss_tec_failed_dates (
                date_str TEXT PRIMARY KEY,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_failed_at TEXT NOT NULL
            )
        """)
        await db.commit()


async def get_failed_dates() -> set[str]:
    """Return date strings to skip (retry_count >= threshold AND recent failure).

    Dates whose last_failed_at is older than FAILED_DATES_RETRY_AFTER_DAYS roll
    out of the skip set so archive coverage that becomes available later can be
    re-fetched without manual intervention.
    """
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(days=FAILED_DATES_RETRY_AFTER_DAYS)
    ).isoformat()
    async with safe_connect() as db:
        rows = await db.execute_fetchall(
            "SELECT date_str FROM gnss_tec_failed_dates "
            "WHERE retry_count >= ? AND last_failed_at > ?",
            (MAX_RETRIES_BEFORE_SKIP, cutoff_iso),
        )
    return {r[0] for r in rows}


async def mark_failed_date(date_str: str) -> None:
    """Record a 0-record fetch for date_str; increment retry_count on conflict."""
    now_iso = datetime.now(timezone.utc).isoformat()
    async with safe_connect() as db:
        await db.execute(
            "INSERT INTO gnss_tec_failed_dates (date_str, retry_count, last_failed_at) "
            "VALUES (?, 1, ?) "
            "ON CONFLICT(date_str) DO UPDATE SET "
            "retry_count = retry_count + 1, "
            "last_failed_at = excluded.last_failed_at",
            (date_str, now_iso),
        )
        await db.commit()


async def try_fetch(session: aiohttp.ClientSession, url: str) -> bytes | None:
    """Try to fetch a URL, return None on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.read()
                elif resp.status == 404:
                    return None
                else:
                    logger.debug("HTTP %d for %s", resp.status, url.split("/")[-1])
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("Failed: %s", e)
                return None
            await asyncio.sleep(2 ** attempt)
    return None


def parse_netcdf_simple(data: bytes, epoch: str) -> list[tuple]:
    """Parse netCDF3 (classic format) without external dependencies.

    netCDF3 classic format starts with 'CDF\\x01'.
    We extract lat/lon/tec arrays from the binary structure.

    Falls back to netCDF4 library if available.
    """
    # Try netCDF4 library first (most reliable)
    try:
        import netCDF4
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as f:
            f.write(data)
            tmp_path = f.name

        try:
            ds = netCDF4.Dataset(tmp_path, "r")
            rows = _extract_from_dataset(ds, epoch)
            ds.close()
            return rows
        finally:
            os.unlink(tmp_path)
    except ImportError:
        pass

    # Try scipy.io.netcdf
    try:
        from scipy.io import netcdf_file

        buf = io.BytesIO(data)
        ds = netcdf_file(buf, "r", mmap=False)
        rows = _extract_from_scipy(ds, epoch)
        ds.close()
        return rows
    except ImportError:
        pass

    logger.warning("Neither netCDF4 nor scipy available. Cannot parse netCDF data.")
    return []


def _extract_from_dataset(ds, epoch: str) -> list[tuple]:
    """Extract TEC data from a netCDF4.Dataset object."""
    # Find variable names (may vary by product)
    lat_var = lon_var = tec_var = None
    for name in ds.variables:
        lower = name.lower()
        if "lat" in lower:
            lat_var = name
        elif "lon" in lower:
            lon_var = name
        elif "tec" in lower or "vtec" in lower or "atec" in lower:
            tec_var = name

    if not all([lat_var, lon_var, tec_var]):
        logger.debug("Variables found: %s", list(ds.variables.keys()))
        return []

    lats = ds.variables[lat_var][:]
    lons = ds.variables[lon_var][:]
    tec_data = ds.variables[tec_var][:]

    return _grid_to_rows(lats, lons, tec_data, epoch)


def _extract_from_scipy(ds, epoch: str) -> list[tuple]:
    """Extract TEC data from a scipy netcdf_file object."""
    lat_var = lon_var = tec_var = None
    for name in ds.variables:
        lower = name.lower()
        if "lat" in lower:
            lat_var = name
        elif "lon" in lower:
            lon_var = name
        elif "tec" in lower or "vtec" in lower or "atec" in lower:
            tec_var = name

    if not all([lat_var, lon_var, tec_var]):
        logger.debug("Variables found: %s", list(ds.variables.keys()))
        return []

    lats = ds.variables[lat_var].data.copy()
    lons = ds.variables[lon_var].data.copy()
    tec_data = ds.variables[tec_var].data.copy()

    return _grid_to_rows(lats, lons, tec_data, epoch)


def _grid_to_rows(lats, lons, tec_data, epoch: str) -> list[tuple]:
    """Convert lat/lon/tec grids to row tuples, filtered to Japan bbox.

    Nagoya GNSS-TEC netCDF structure:
        atec shape = (lat=360, lon=721, time=12)
        Dimensions: (latitude, longitude, time)
        Resolution: 0.5° spatial, 5-min temporal (12 steps per hour)
        Missing value: 999.0

    Subsample to 1.0° to manage data volume (every 2nd point from 0.5° grid).
    Take time-averaged TEC (mean over 12 five-minute bins in the hour).
    """
    rows = []
    stride = 2  # 0.5° × 2 = 1.0° effective resolution
    MISSING = 999.0

    if tec_data.ndim == 2:
        # (lat, lon) — simple 2D grid
        for i in range(0, len(lats), stride):
            lat = float(lats[i])
            if lat < JAPAN_BBOX["min_lat"] or lat > JAPAN_BBOX["max_lat"]:
                continue
            for j in range(0, len(lons), stride):
                lon = float(lons[j])
                if lon < JAPAN_BBOX["min_lon"] or lon > JAPAN_BBOX["max_lon"]:
                    continue
                tec = float(tec_data[i, j])
                if tec >= MISSING or tec < -100 or tec != tec or tec > 200:
                    continue
                rows.append((lat, lon, tec, None, epoch))
    elif tec_data.ndim == 3:
        # Nagoya format: (lat, lon, time) — NOT (time, lat, lon)
        # Average over time axis (axis=2) for a single hourly value
        for i in range(0, len(lats), stride):
            lat = float(lats[i])
            if lat < JAPAN_BBOX["min_lat"] or lat > JAPAN_BBOX["max_lat"]:
                continue
            for j in range(0, len(lons), stride):
                lon = float(lons[j])
                if lon < JAPAN_BBOX["min_lon"] or lon > JAPAN_BBOX["max_lon"]:
                    continue
                # Average over time steps, excluding missing values
                vals = []
                for t in range(tec_data.shape[2]):
                    v = float(tec_data[i, j, t])
                    if v < MISSING and v > -100 and v == v and v <= 200:
                        vals.append(v)
                if vals:
                    tec_mean = sum(vals) / len(vals)
                    rows.append((lat, lon, round(tec_mean, 2), None, epoch))

    return rows


async def fetch_date(session: aiohttp.ClientSession, date: datetime,
                     hours: list[int] | None = None) -> list[tuple]:
    """Fetch GNSS-TEC data for a specific date.

    Tries VTEC (AGRID2) first, then dTEC (GRID2) as fallback.
    """
    if hours is None:
        hours = FETCH_HOURS

    year = date.strftime("%Y")
    doy = date.strftime("%j")
    ymd = date.strftime("%Y%m%d")

    all_rows = []

    for hour in hours:
        hh = f"{hour:02d}"
        epoch = f"{date.strftime('%Y-%m-%d')} {hh}:00:00"

        # Try VTEC (absolute TEC) — available from 1993
        url = f"{NAGOYA_BASE}/AGRID2/nc/{year}/{doy}/{ymd}{hh}_atec.nc"
        data = await try_fetch(session, url)
        if data is not None and len(data) > 100:
            rows = parse_netcdf_simple(data, epoch)
            if rows:
                all_rows.extend(rows)
                logger.info("  %s %s UT: %d VTEC records", date.strftime("%Y-%m-%d"), hh, len(rows))
                continue

        # Fallback: dTEC (detrended) — available from 2019
        url = f"{NAGOYA_BASE}/GRID2/nc/{year}/{doy}/{ymd}{hh}_dtec.nc"
        data = await try_fetch(session, url)
        if data is not None and len(data) > 100:
            rows = parse_netcdf_simple(data, epoch)
            if rows:
                all_rows.extend(rows)
                logger.info("  %s %s UT: %d dTEC records", date.strftime("%Y-%m-%d"), hh, len(rows))

    return all_rows


async def main():
    """Fetch GNSS-TEC data continuously for 2011-now."""
    await init_db()
    await init_gnss_tec_table()

    now = datetime.now(timezone.utc).isoformat()

    # Continuous strategy: all dates 2011-01-01 to yesterday, fetch missing.
    # Oldest-first fill so archive coverage gap closes from 2011 forward.
    start_date = datetime(2011, 1, 1)
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    total_days = (end_date - start_date).days + 1
    all_dates = [start_date + timedelta(days=i) for i in range(total_days)]

    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT DISTINCT DATE(epoch) FROM gnss_tec"
        )

    existing_date_strs = {r[0] for r in existing if r[0]}
    failed_skip = await get_failed_dates()

    # Combined skip set: dates that already have data OR have hit retry limit
    # within the last FAILED_DATES_RETRY_AFTER_DAYS days.
    skip_date_strs = existing_date_strs | failed_skip

    # Oldest-first for GNSS-TEC: Nagoya ISEE has sporadic 404s in the last 1-6
    # months (publication lag). 2011-early-2026 archive is mostly populated, so
    # filling from oldest fastest catches up without wasting cron budget on
    # recent missing URLs (those eventually become covered).
    dates_to_fetch = sorted(
        d for d in all_dates if d.strftime("%Y-%m-%d") not in skip_date_strs
    )

    logger.info(
        "GNSS-TEC: %d missing dates (%d total, %d existing, %d failed-skip)",
        len(dates_to_fetch), total_days, len(existing_date_strs), len(failed_skip),
    )

    if not dates_to_fetch:
        logger.info("No new dates to fetch")
        return

    max_dates = int(_os.environ.get("GNSS_TEC_MAX_DATES", "200"))
    target_dates = dates_to_fetch[:max_dates]
    logger.info(
        "Fetching %d dates with parallelism=%d, rate_limit_sleep=%.2fs",
        len(target_dates), PARALLEL_DATES, RATE_LIMIT_SLEEP,
    )

    sem = asyncio.Semaphore(PARALLEL_DATES)

    async def fetch_one(session: aiohttp.ClientSession, date: datetime):
        async with sem:
            rows = await fetch_date(session, date)
            # Per-date rate-limit sleep stays inside semaphore so concurrent
            # workers each pace at RATE_LIMIT_SLEEP rather than burst-and-stop.
            await asyncio.sleep(RATE_LIMIT_SLEEP)
            return date, rows

    total_records = 0
    inserted_dates = 0
    failed_dates_count = 0

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, d) for d in target_dates]
        # as_completed lets us write to SQLite incrementally as fetches finish
        # (HTTP parallel, SQLite sequential to avoid lock contention).
        for coro in asyncio.as_completed(tasks):
            date, rows = await coro
            date_str = date.strftime("%Y-%m-%d")
            if rows:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO gnss_tec
                           (latitude, longitude, tec_tecu, dtec_tecu, epoch, source, received_at)
                           VALUES (?, ?, ?, ?, ?, 'nagoya', ?)""",
                        [(lat, lon, tec, dtec, ep, now) for lat, lon, tec, dtec, ep in rows],
                    )
                    await db.commit()
                total_records += len(rows)
                inserted_dates += 1
                logger.info("  %s: %d records (cumulative: %d records / %d dates)",
                            date_str, len(rows), total_records, inserted_dates)
            else:
                await mark_failed_date(date_str)
                failed_dates_count += 1
                logger.info("  %s: 0 records (marked failed, cumulative failed: %d)",
                            date_str, failed_dates_count)

    logger.info(
        "GNSS-TEC fetch complete: %d records / %d dates inserted, %d dates failed",
        total_records, inserted_dates, failed_dates_count,
    )


if __name__ == "__main__":
    asyncio.run(main())
