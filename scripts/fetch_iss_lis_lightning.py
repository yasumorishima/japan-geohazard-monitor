"""Fetch ISS LIS (Lightning Imaging Sensor) data for Japan region.

The LIS instrument on the International Space Station observed lightning
from March 2017 to November 2023, covering latitudes ±54°. Japan is
within coverage. Each orbit pass captures flash locations and optical
energy, providing a space-based lightning climatology.

Physical mechanism:
    Pre-seismic radon emission → atmospheric ionization → electric field
    changes → anomalous lightning activity near fault zones.
    The LAIC (Lithosphere-Atmosphere-Ionosphere Coupling) model predicts
    detectable lightning anomalies 1-15 days before M5+ events.

Data source:
    NASA GHRC DAAC - Quality Controlled ISS LIS Science Data V2
    - Granules: individual orbit files (HDF/NetCDF)
    - Coverage: 2017-03 to 2023-11
    - Spatial: ±54° latitude, global
    - Requires Earthdata authentication
    - CMR API for granule discovery (no auth needed)

Target features:
    - lightning_count_7d: flash count in 2° cell over 7 days
    - lightning_anomaly: deviation from seasonal baseline

References:
    - Pulinets & Ouzounov (2011) NHESS 11:3247
    - Blakeslee et al. (2020) JGR 125:e2020JD032918
"""

import asyncio
import csv
import io
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
from earthdata_auth import (
    get_earthdata_session, earthdata_fetch_bytes,
    EARTHDATA_USERNAME, EARTHDATA_PASSWORD, EARTHDATA_TOKEN,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# CMR API for granule discovery (no auth required)
CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
COLLECTION_SHORT_NAME = "isslis_v2_fin"

# Japan bounding box
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 155.0

# Aggregate to 2° cells
CELL_DEG = 2.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)

# Max granules per run to stay within Actions time budget
# ISS LIS is accumulative (skips existing data), so process more per run
MAX_GRANULES_PER_RUN = 700  # ~1.4s/granule × 700 ≈ 16min (fits in 20min timeout)


async def init_iss_lis_table():
    """Create ISS LIS lightning table (separate from Blitzortung lightning)."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS iss_lis_lightning (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                flash_count INTEGER NOT NULL,
                mean_radiance REAL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_iss_lis_lightning_time
            ON iss_lis_lightning(observed_at)
        """)
        await db.commit()


def snap_to_cell(lat, lon):
    """Snap coordinates to 2° grid cell center."""
    return (
        round(lat / CELL_DEG) * CELL_DEG,
        round(lon / CELL_DEG) * CELL_DEG,
    )


async def search_granules(session, start_date, end_date, page_size=200):
    """Search CMR for ISS LIS granules overlapping Japan.

    CMR API is public (no auth needed).
    Returns list of {url, filename, time_start, time_end}.
    """
    params = {
        "short_name": COLLECTION_SHORT_NAME,
        "bounding_box": f"{JAPAN_LON_MIN},{JAPAN_LAT_MIN},{JAPAN_LON_MAX},{JAPAN_LAT_MAX}",
        "temporal": f"{start_date},{end_date}",
        "page_size": str(page_size),
        "sort_key": "start_date",
    }

    granules = []
    page = 1

    while True:
        params["page_num"] = str(page)
        try:
            async with session.get(CMR_GRANULES_URL, params=params,
                                    timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    logger.warning("CMR search: HTTP %d", resp.status)
                    break

                data = await resp.json()
                entries = data.get("feed", {}).get("entry", [])

                if not entries:
                    break

                for entry in entries:
                    # Find NetCDF download URL
                    nc_url = None
                    for link in entry.get("links", []):
                        href = link.get("href", "")
                        if href.endswith(".nc") and "ghrcw-protected" in href:
                            nc_url = href
                            break

                    if nc_url:
                        granules.append({
                            "url": nc_url,
                            "title": entry.get("title", ""),
                            "time_start": entry.get("time_start", ""),
                            "time_end": entry.get("time_end", ""),
                        })

                if len(entries) < page_size:
                    break
                page += 1

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("CMR search error: %s", e)
            break

    return granules


def parse_lis_netcdf(data_bytes):
    """Parse ISS LIS NetCDF file to extract flash events in Japan.

    Uses netCDF4 if available, otherwise falls back to simple text scan.
    Returns list of {lat, lon, time_iso, radiance}.
    """
    flashes = []

    try:
        import netCDF4
        import tempfile

        # Write to temp file for netCDF4
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmp.write(data_bytes)
            tmp_path = tmp.name

        try:
            ds = netCDF4.Dataset(tmp_path, "r")

            # ISS LIS variables
            if "lightning_flash_TAI93_time" in ds.variables:
                flash_lats = ds.variables.get("lightning_flash_lat", None)
                flash_lons = ds.variables.get("lightning_flash_lon", None)
                flash_times = ds.variables.get("lightning_flash_TAI93_time", None)
                flash_radiances = ds.variables.get("lightning_flash_radiance", None)

                if flash_lats is not None and flash_lons is not None:
                    n_flashes = len(flash_lats)
                    for i in range(n_flashes):
                        lat = float(flash_lats[i])
                        lon = float(flash_lons[i])

                        # Filter to Japan region
                        if not (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                                and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
                            continue

                        # Convert TAI93 time to ISO
                        tai93 = float(flash_times[i]) if flash_times is not None else 0
                        # TAI93 = seconds since 1993-01-01 00:00:00 TAI
                        dt = datetime(1993, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=tai93)
                        time_iso = dt.strftime("%Y-%m-%dT%H:%M:%S")

                        radiance = None
                        if flash_radiances is not None:
                            try:
                                radiance = float(flash_radiances[i])
                            except (ValueError, IndexError):
                                pass

                        flashes.append({
                            "lat": lat,
                            "lon": lon,
                            "time_iso": time_iso,
                            "radiance": radiance,
                        })

            ds.close()
        finally:
            os.unlink(tmp_path)

    except ImportError:
        logger.warning("netCDF4 not available, skipping ISS LIS file parsing")
    except Exception as e:
        logger.warning("ISS LIS NetCDF parse error: %s", e)

    return flashes


def aggregate_daily_cells(flashes):
    """Aggregate flash events to daily 2° grid cells.

    Returns list of {date, cell_lat, cell_lon, flash_count, mean_radiance}.
    """
    cells = {}  # (date, cell_lat, cell_lon) -> {count, radiance_sum}

    for f in flashes:
        date = f["time_iso"][:10]  # YYYY-MM-DD
        clat, clon = snap_to_cell(f["lat"], f["lon"])
        key = (date, clat, clon)

        if key not in cells:
            cells[key] = {"count": 0, "radiance_sum": 0.0, "n_radiance": 0}

        cells[key]["count"] += 1
        if f["radiance"] is not None:
            cells[key]["radiance_sum"] += f["radiance"]
            cells[key]["n_radiance"] += 1

    result = []
    for (date, clat, clon), info in cells.items():
        mean_rad = (info["radiance_sum"] / info["n_radiance"]
                    if info["n_radiance"] > 0 else None)
        result.append({
            "date": date,
            "cell_lat": clat,
            "cell_lon": clon,
            "flash_count": info["count"],
            "mean_radiance": mean_rad,
        })

    return result


async def main():
    await init_db()
    await init_iss_lis_table()

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    has_auth = (EARTHDATA_USERNAME and EARTHDATA_PASSWORD) or EARTHDATA_TOKEN
    if not has_auth:
        logger.info("ISS LIS: no Earthdata credentials, skipping")
        return

    # Check existing data
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(*) FROM iss_lis_lightning"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    n_existing = existing[0][1] if existing else 0

    if last_date:
        logger.info("ISS LIS existing: %d records (latest: %s)", n_existing, last_date)
        start_date = last_date[:10]
    else:
        start_date = "2017-03-01"

    # ISS LIS ended 2023-11-16
    end_date = "2023-11-17"

    if start_date >= end_date:
        logger.info("ISS LIS: all available data already fetched")
        return

    # Search for granules
    logger.info("ISS LIS: searching CMR for granules (%s to %s)...", start_date, end_date)
    async with aiohttp.ClientSession() as session:
        granules = await search_granules(session, start_date, end_date)

    logger.info("ISS LIS: found %d granules overlapping Japan", len(granules))

    if not granules:
        return

    # Cap to avoid timeout
    if len(granules) > MAX_GRANULES_PER_RUN:
        logger.info("ISS LIS: capping to %d granules (of %d)", MAX_GRANULES_PER_RUN, len(granules))
        granules = granules[:MAX_GRANULES_PER_RUN]

    # Download and parse each granule
    total_flashes = 0
    total_records = 0
    session = await get_earthdata_session()

    try:
        for i, granule in enumerate(granules):
            if (i + 1) % 50 == 0:
                logger.info("ISS LIS: processing granule %d/%d...", i + 1, len(granules))

            # Download NetCDF
            status, data = await earthdata_fetch_bytes(
                session, granule["url"], timeout=TIMEOUT)

            if status != 200 or not data:
                continue

            # Parse flashes in Japan
            flashes = parse_lis_netcdf(data)
            if not flashes:
                continue

            total_flashes += len(flashes)

            # Aggregate to daily cells
            daily_cells = aggregate_daily_cells(flashes)

            if daily_cells:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO iss_lis_lightning
                           (observed_at, cell_lat, cell_lon, flash_count,
                            mean_radiance, received_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [(r["date"], r["cell_lat"], r["cell_lon"],
                          r["flash_count"], r["mean_radiance"], now_iso)
                         for r in daily_cells],
                    )
                    await db.commit()
                total_records += len(daily_cells)

            await asyncio.sleep(0.3)  # Rate limit

    finally:
        await session.close()

    logger.info("ISS LIS fetch complete: %d flashes → %d daily cell records from %d granules",
                total_flashes, total_records, len(granules))


if __name__ == "__main__":
    asyncio.run(main())
