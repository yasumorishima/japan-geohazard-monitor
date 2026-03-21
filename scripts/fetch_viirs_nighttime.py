"""Fetch VIIRS nighttime light/airglow data from NASA LAADS DAAC.

The VIIRS Day/Night Band (DNB) detects very faint light emissions.
Pre-seismic acoustic-gravity waves from ground motion can modulate
airglow at ~90 km altitude, producing detectable radiance anomalies.

Data source: VNP46A4 (Annual VIIRS Nighttime Lights)
    - Annual composites per tile (~150 MB HDF5)
    - Japan tiles: h29v05 (northern), h29v06 (southern)
    - LAADS DAAC: Earthdata authentication required
    - Coverage: 2012-present (annual)

Target features:
    - nightlight_anomaly: radiance deviation from 6-month baseline (σ)

References:
    - Ouzounov et al. (2022) "Pre-earthquake processes" Chapter 12
    - Román et al. (2018) Remote Sensing 10:1395 (VNP46 suite)
"""

import asyncio
import json
import logging
import math
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH
from earthdata_auth import (
    get_earthdata_session, earthdata_fetch_bytes,
    EARTHDATA_USERNAME, EARTHDATA_PASSWORD, EARTHDATA_TOKEN,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# LAADS DAAC API for file discovery (no auth for catalog)
LAADS_API = "https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content"
# Use Earthdata Cloud URL (avoids LAADS EULA redirect issue)
EARTHDATA_CLOUD = "https://data.laadsdaac.earthdatacloud.nasa.gov/prod-lads/VNP46A4"

# Japan MODIS/VIIRS sinusoidal tiles
JAPAN_TILES = ["h29v05", "h29v06"]

# Japan bounding box
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 155.0

CELL_DEG = 2.0
START_YEAR = 2012
MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=600, connect=60)


async def init_nightlight_table():
    """Create nightlight table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS nightlight (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                radiance_nwcm2sr REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_nightlight_time
            ON nightlight(observed_at)
        """)
        await db.commit()


def sinusoidal_to_latlon(row, col, h, v, nrows=2400, ncols=2400):
    """Convert MODIS sinusoidal tile row/col to lat/lon."""
    tile_size = 1111950.0  # meters per tile edge
    R = 6371007.181  # Earth radius meters

    x_ul = (h - 18) * tile_size
    y_ul = (9 - v) * tile_size
    pixel_size = tile_size / nrows

    x = x_ul + (col + 0.5) * pixel_size
    y = y_ul - (row + 0.5) * pixel_size

    lat = math.degrees(y / R)
    if abs(lat) < 89.9:
        lon = math.degrees(x / (R * math.cos(math.radians(lat))))
    else:
        lon = 0.0
    return lat, lon


def parse_vnp46a4_h5(filepath, h, v):
    """Extract mean radiance from VNP46A4 HDF5 file for Japan pixels."""
    pixels = []
    try:
        import h5py
        with h5py.File(filepath, "r") as f:
            grids = f.get("HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields")
            if grids is None:
                logger.warning("VNP46A4: expected HDF5 structure not found")
                return []

            dataset = None
            for var_name in ["AllAngle_Composite_Snow_Free",
                             "NearNadir_Composite_Snow_Free",
                             "AllAngle_Composite"]:
                if var_name in grids:
                    dataset = grids[var_name]
                    break

            if dataset is None:
                logger.warning("VNP46A4: no radiance dataset found")
                return []

            data = dataset[:]
            fill_value = dataset.attrs.get("_FillValue", 65535)
            scale_factor = float(dataset.attrs.get("scale_factor", 0.1))
            add_offset = float(dataset.attrs.get("add_offset", 0.0))
            nrows, ncols = data.shape

            # Subsample every 24 pixels (~10km)
            step = 24
            for row in range(0, nrows, step):
                for col in range(0, ncols, step):
                    val = data[row, col]
                    if val == fill_value or val <= 0:
                        continue
                    radiance = float(val) * scale_factor + add_offset
                    if radiance <= 0 or radiance > 1000:
                        continue
                    lat, lon = sinusoidal_to_latlon(row, col, h, v, nrows, ncols)
                    if not (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                            and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
                        continue
                    pixels.append({"lat": lat, "lon": lon, "radiance": radiance})

    except ImportError:
        logger.warning("h5py not available, skipping VNP46A4 parsing")
    except Exception as e:
        logger.warning("VNP46A4 parse error: %s", e)
    return pixels


def aggregate_to_cells(pixels, year):
    """Aggregate pixel radiances to 2° grid cells."""
    cells = {}
    for p in pixels:
        clat = round(p["lat"] / CELL_DEG) * CELL_DEG
        clon = round(p["lon"] / CELL_DEG) * CELL_DEG
        key = (clat, clon)
        if key not in cells:
            cells[key] = {"sum": 0.0, "count": 0}
        cells[key]["sum"] += p["radiance"]
        cells[key]["count"] += 1

    return [{
        "observed_at": f"{year}-07-01",
        "cell_lat": clat,
        "cell_lon": clon,
        "radiance": round(info["sum"] / info["count"], 4),
    } for (clat, clon), info in cells.items()]


async def main():
    await init_db()
    await init_nightlight_table()

    now_iso = datetime.now(timezone.utc).isoformat()
    current_year = datetime.now(timezone.utc).year

    has_auth = (EARTHDATA_USERNAME and EARTHDATA_PASSWORD) or EARTHDATA_TOKEN
    if not has_auth:
        logger.info("VIIRS nightlight: no Earthdata credentials, skipping")
        return

    # Check existing
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT DISTINCT substr(observed_at, 1, 4) FROM nightlight"
        )
    existing_years = {r[0] for r in existing} if existing else set()
    logger.info("VIIRS nightlight existing: %d years", len(existing_years))

    session = await get_earthdata_session()
    total_records = 0

    try:
        for year in range(START_YEAR, current_year):
            if str(year) in existing_years:
                continue

            logger.info("Fetching VIIRS VNP46A4 for %d...", year)
            year_pixels = []

            for tile in JAPAN_TILES:
                h = int(tile[1:3])
                v = int(tile[4:6])

                # Find file via LAADS API (no auth needed for catalog)
                api_url = f"{LAADS_API}/details/allData/5200/VNP46A4/{year}/001/"
                try:
                    async with aiohttp.ClientSession() as api_session:
                        async with api_session.get(api_url, timeout=TIMEOUT) as resp:
                            if resp.status != 200:
                                continue
                            catalog = await resp.json()
                except Exception:
                    continue

                target_file = None
                file_size = 0
                for item in catalog.get("content", []):
                    name = item.get("name", "")
                    if tile in name and name.endswith(".h5"):
                        target_file = name
                        file_size = item.get("size", 0)
                        break

                if not target_file:
                    logger.info("  %s %d: no file found", tile, year)
                    continue

                # Download via Earthdata Cloud (avoids LAADS EULA redirect)
                dl_url = f"{EARTHDATA_CLOUD}/{target_file}"
                logger.info("  Downloading %s (%.0f MB) from Earthdata Cloud...", tile, file_size / 1024 / 1024)

                status, file_bytes = await earthdata_fetch_bytes(
                    session, dl_url,
                    timeout=aiohttp.ClientTimeout(total=1200))

                if status != 200 or not file_bytes or len(file_bytes) < 1000:
                    logger.warning("  %s %d: download failed (HTTP %d)", tile, year, status)
                    continue

                # Validate HDF5 magic bytes before parsing
                HDF5_MAGIC = b'\x89HDF\r\n\x1a\n'
                if file_bytes[:8] != HDF5_MAGIC:
                    if file_bytes[:1] == b'<':
                        logger.warning("  %s %d: got HTML instead of HDF5 (auth/EULA issue)", tile, year)
                    else:
                        logger.warning("  %s %d: invalid file (not HDF5, first 20 bytes: %r)",
                                       tile, year, file_bytes[:20])
                    continue

                # Parse
                with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
                    tmp.write(file_bytes)
                    tmp_path = tmp.name
                try:
                    pixels = parse_vnp46a4_h5(tmp_path, h, v)
                    year_pixels.extend(pixels)
                    logger.info("  %s %d: %d Japan pixels", tile, year, len(pixels))
                finally:
                    os.unlink(tmp_path)

                await asyncio.sleep(1.0)

            if year_pixels:
                daily_cells = aggregate_to_cells(year_pixels, year)
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO nightlight
                           (observed_at, cell_lat, cell_lon, radiance_nwcm2sr, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["observed_at"], r["cell_lat"], r["cell_lon"],
                          r["radiance"], now_iso) for r in daily_cells],
                    )
                    await db.commit()
                total_records += len(daily_cells)
                logger.info("  %d: %d cell records", year, len(daily_cells))

    finally:
        await session.close()

    logger.info("VIIRS nightlight fetch complete: %d cell records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
