"""Fetch high-resolution GNSS-TEC data from Nagoya University ISEE.

Downloads 0.25°×0.25° detrended TEC (dTEC) grid data over Japan
from the Nagoya University GNSS-TEC database. This is 25x higher
spatial resolution than the CODE IONEX (2.5°×5°) used in Phase 1.

Data source: https://stdb2.isee.nagoya-u.ac.jp/GPS/GPS-TEC/
Format: netCDF or ASCII grid
Resolution: 0.25° spatial, 10-minute temporal
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

# Nagoya University GNSS-TEC archive
# Data is available as daily files
# URL pattern needs to be verified — using known structure
NAGOYA_BASE = "https://stdb2.isee.nagoya-u.ac.jp/GPS/GPS-TEC/data"

# Japan bounding box for filtering
JAPAN_BBOX = {"min_lat": 25.0, "max_lat": 46.0, "min_lon": 125.0, "max_lon": 150.0}

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=60, connect=30)


async def init_gnss_tec_table():
    """Create GNSS-TEC table if not exists."""
    async with aiosqlite.connect(DB_PATH) as db:
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
                    logger.warning("HTTP %d for %s", resp.status, url)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("Failed after %d attempts: %s", MAX_RETRIES, e)
                return None
            await asyncio.sleep(2 ** attempt)
    return None


def parse_gnss_tec_ascii(data: str, epoch: str) -> list[tuple]:
    """Parse ASCII grid format GNSS-TEC data.

    Expected format: rows of (lat, lon, tec_value) or grid format.
    Actual format depends on the source — this handles common cases.
    """
    rows = []
    for line in data.split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("%"):
            continue
        parts = line.split()
        if len(parts) >= 3:
            try:
                lat = float(parts[0])
                lon = float(parts[1])
                tec = float(parts[2])
                if (JAPAN_BBOX["min_lat"] <= lat <= JAPAN_BBOX["max_lat"] and
                        JAPAN_BBOX["min_lon"] <= lon <= JAPAN_BBOX["max_lon"]):
                    dtec = float(parts[3]) if len(parts) > 3 else None
                    rows.append((lat, lon, tec, dtec, epoch))
            except ValueError:
                continue
    return rows


async def fetch_date(session: aiohttp.ClientSession, date: datetime) -> list[tuple]:
    """Fetch GNSS-TEC data for a specific date.

    Tries multiple URL patterns since the exact format may vary.
    """
    year = date.strftime("%Y")
    doy = date.strftime("%j")
    date_str = date.strftime("%Y%m%d")

    # Try various URL patterns
    url_patterns = [
        # netCDF format
        f"{NAGOYA_BASE}/vtec/{year}/gps_vtec_{date_str}.nc",
        f"{NAGOYA_BASE}/dtec/{year}/gps_dtec_{date_str}.nc",
        # ASCII format
        f"{NAGOYA_BASE}/vtec/{year}/gps_vtec_{date_str}.dat",
        f"{NAGOYA_BASE}/vtec/{year}/{doy}/vtec.dat",
        # Alternative paths
        f"{NAGOYA_BASE}/{year}/{doy}/vtec_0.25x0.25.dat",
    ]

    for url in url_patterns:
        data = await try_fetch(session, url)
        if data is not None:
            logger.info("  Found data at: %s (%d bytes)", url.split("/")[-1], len(data))
            try:
                # Try to decode as text
                text = data.decode("utf-8", errors="replace")
                epoch = date.strftime("%Y-%m-%d 00:00:00")
                rows = parse_gnss_tec_ascii(text, epoch)
                if rows:
                    return rows
            except Exception as e:
                logger.debug("Parse failed for %s: %s", url, e)

            # If it's netCDF, we need netCDF4 library
            # Check if data starts with netCDF magic bytes
            if data[:4] in (b'\x89HDF', b'CDF\x01', b'CDF\x02'):
                logger.info("  netCDF file detected, needs netCDF4 library")
                try:
                    return await parse_netcdf(data, date)
                except ImportError:
                    logger.warning("  netCDF4 not installed, skipping")
                    return []

    return []


async def parse_netcdf(data: bytes, date: datetime) -> list[tuple]:
    """Parse netCDF GNSS-TEC data."""
    import tempfile
    import os

    try:
        import netCDF4
    except ImportError:
        logger.warning("netCDF4 not available")
        return []

    # Write to temp file for netCDF4 to read
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as f:
        f.write(data)
        tmp_path = f.name

    try:
        ds = netCDF4.Dataset(tmp_path, "r")
        # Extract lat/lon/tec variables (names may vary)
        lat_var = None
        lon_var = None
        tec_var = None

        for name in ds.variables:
            lower = name.lower()
            if "lat" in lower:
                lat_var = name
            elif "lon" in lower:
                lon_var = name
            elif "tec" in lower or "vtec" in lower:
                tec_var = name

        if not all([lat_var, lon_var, tec_var]):
            logger.warning("Could not identify variables: %s", list(ds.variables.keys()))
            ds.close()
            return []

        lats = ds.variables[lat_var][:]
        lons = ds.variables[lon_var][:]
        tec_data = ds.variables[tec_var][:]

        rows = []
        epoch = date.strftime("%Y-%m-%d 00:00:00")

        # Handle different array shapes
        if tec_data.ndim == 2:
            for i, lat in enumerate(lats):
                for j, lon in enumerate(lons):
                    if (JAPAN_BBOX["min_lat"] <= float(lat) <= JAPAN_BBOX["max_lat"] and
                            JAPAN_BBOX["min_lon"] <= float(lon) <= JAPAN_BBOX["max_lon"]):
                        tec = float(tec_data[i, j])
                        if not (tec < -900 or tec != tec):  # Skip fill values and NaN
                            rows.append((float(lat), float(lon), tec, None, epoch))
        elif tec_data.ndim == 3:  # (time, lat, lon)
            for t_idx in range(min(tec_data.shape[0], 144)):  # Max 144 (10-min intervals)
                ep = (date + timedelta(minutes=10 * t_idx)).strftime("%Y-%m-%d %H:%M:00")
                for i, lat in enumerate(lats):
                    for j, lon in enumerate(lons):
                        if (JAPAN_BBOX["min_lat"] <= float(lat) <= JAPAN_BBOX["max_lat"] and
                                JAPAN_BBOX["min_lon"] <= float(lon) <= JAPAN_BBOX["max_lon"]):
                            tec = float(tec_data[t_idx, i, j])
                            if not (tec < -900 or tec != tec):
                                rows.append((float(lat), float(lon), tec, None, ep))

        ds.close()
        logger.info("  Parsed %d GNSS-TEC records from netCDF", len(rows))
        return rows
    finally:
        os.unlink(tmp_path)


async def main():
    """Fetch GNSS-TEC data for dates around major earthquakes."""
    await init_db()
    await init_gnss_tec_table()

    now = datetime.now(timezone.utc).isoformat()

    # Get dates around M6.5+ earthquakes (±3 days)
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at) FROM earthquakes "
            "WHERE magnitude >= 6.5 ORDER BY occurred_at"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT DATE(epoch) FROM gnss_tec"
        )

    eq_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-3, 4):
            eq_dates.add(d + timedelta(days=offset))

    existing_dates = set(datetime.strptime(r[0], "%Y-%m-%d") for r in existing if r[0])
    dates_to_fetch = sorted(eq_dates - existing_dates)

    logger.info("GNSS-TEC: %d dates to fetch (%d earthquake dates, %d already in DB)",
                len(dates_to_fetch), len(eq_dates), len(existing_dates))

    if not dates_to_fetch:
        logger.info("No new dates to fetch")
        return

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for i, date in enumerate(dates_to_fetch[:100]):  # Limit for initial run
            rows = await fetch_date(session, date)
            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO gnss_tec
                           (latitude, longitude, tec_tecu, dtec_tecu, epoch, source, received_at)
                           VALUES (?, ?, ?, ?, ?, 'nagoya', ?)""",
                        [(lat, lon, tec, dtec, ep, now) for lat, lon, tec, dtec, ep in rows],
                    )
                    await db.commit()
                total_records += len(rows)
                logger.info("  %s: %d records", date.strftime("%Y-%m-%d"), len(rows))
            else:
                logger.debug("  %s: no data found", date.strftime("%Y-%m-%d"))

            if (i + 1) % 20 == 0:
                logger.info("  Progress: %d/%d dates, %d records total",
                            i + 1, len(dates_to_fetch), total_records)

            # Rate limiting
            await asyncio.sleep(0.5)

    logger.info("GNSS-TEC fetch complete: %d records from %d dates", total_records, len(dates_to_fetch))


if __name__ == "__main__":
    asyncio.run(main())
