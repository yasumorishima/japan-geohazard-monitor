"""Fetch Outgoing Longwave Radiation (OLR) daily data for Japan.

OLR measures thermal infrared radiation escaping Earth's atmosphere. Pre-seismic
thermal anomalies have been reported 7-10 days before M6+ earthquakes in Japan,
attributed to the LAIC (Lithosphere-Atmosphere-Ionosphere Coupling) model:
    crustal stress → radon release → ionization → aerosol nucleation
    → cloud/thermal anomaly → OLR change

Unlike MODIS LST (point measurements at epicenters), OLR captures broad-scale
thermal anomalies over the entire Japan region at 2.5-degree resolution.

Data source: NOAA NCEI CDR (Climate Data Record) OLR Daily
    - Derived from NOAA satellite observations
    - 2.5-degree global grid, daily, 1979-present (~2-day lag)
    - Per-year NetCDF files + preliminary file for current year
    - No authentication required
    - URL: https://www.ncei.noaa.gov/data/outgoing-longwave-radiation-daily/access/

Target features:
    - olr_anomaly: deviation from 30-day rolling mean (in σ units)

References:
    - Ouzounov et al. (2007) Tectonophysics 431:211-220
    - Xiong et al. (2010) Nat. Hazards Earth Syst. Sci. 10:2169-2178
"""

import asyncio
import logging
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NCEI CDR OLR directory listing
NCEI_BASE_URL = "https://www.ncei.noaa.gov/data/outgoing-longwave-radiation-daily/access/"

# Japan bounding box
NORTH = 46.0
SOUTH = 24.0
WEST = 122.0
EAST = 150.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=600, connect=30)

START_YEAR = 2011

# Pattern for yearly CDR OLR files
# Final: olr-daily_v01r02_YYYYMMDD_YYYYMMDD.nc
# Preliminary: olr-daily_v01r02-preliminary_YYYYMMDD_YYYYMMDD.nc
FILE_PATTERN = re.compile(
    r'olr-daily_v01r02(?:-preliminary)?_(\d{4})\d{4}_(\d{4})\d{4}\.nc'
)


async def init_olr_table():
    """Create OLR table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS olr (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                olr_wm2 REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_olr_time
            ON olr(observed_at)
        """)
        await db.commit()


async def list_available_files(session: aiohttp.ClientSession) -> dict[int, str]:
    """Parse NCEI directory listing to find available OLR NetCDF files.

    Returns dict mapping year -> filename. For years with both final and
    preliminary files, the preliminary file is preferred for the current year
    (more recent data), and the final file is preferred for past years.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(NCEI_BASE_URL, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    logger.warning("Directory listing attempt %d: HTTP %d", attempt, resp.status)
                    if attempt == MAX_RETRIES:
                        return {}
                    await asyncio.sleep(2 ** attempt)
                    continue
                html = await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Directory listing attempt %d: %s", attempt, type(e).__name__)
            if attempt == MAX_RETRIES:
                return {}
            await asyncio.sleep(2 ** attempt)
            continue

        # Parse filenames from HTML directory listing
        # Links look like: <a href="olr-daily_v01r02_20110101_20111231.nc">
        files_by_year: dict[int, dict[str, str]] = {}
        for match in FILE_PATTERN.finditer(html):
            filename = match.group(0)
            start_year = int(match.group(1))
            is_preliminary = "-preliminary" in filename

            if start_year not in files_by_year:
                files_by_year[start_year] = {}

            if is_preliminary:
                files_by_year[start_year]["preliminary"] = filename
            else:
                files_by_year[start_year]["final"] = filename

        # Select best file per year
        current_year = datetime.now(timezone.utc).year
        result: dict[int, str] = {}
        for year, variants in files_by_year.items():
            if year == current_year:
                # Prefer preliminary for current year (more recent data)
                result[year] = variants.get("preliminary", variants.get("final", ""))
            else:
                # Prefer final for past years
                result[year] = variants.get("final", variants.get("preliminary", ""))

        logger.info("Found %d year files on NCEI (years %s-%s)",
                     len(result),
                     min(result.keys()) if result else "?",
                     max(result.keys()) if result else "?")
        return result

    return {}


async def download_netcdf(session: aiohttp.ClientSession, filename: str,
                           dest_path: Path) -> bool:
    """Download a single NetCDF file from NCEI."""
    url = NCEI_BASE_URL + filename
    logger.info("Downloading %s (~95MB)...", filename)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    logger.warning("Download %s attempt %d: HTTP %d",
                                   filename, attempt, resp.status)
                    if attempt == MAX_RETRIES:
                        return False
                    await asyncio.sleep(2 ** attempt)
                    continue

                # Stream to disk to avoid holding ~95MB in memory
                with open(dest_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(1024 * 1024):
                        f.write(chunk)

                file_size_mb = dest_path.stat().st_size / (1024 * 1024)
                logger.info("Downloaded %s (%.1f MB)", filename, file_size_mb)
                return True

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Download %s attempt %d: %s", filename, attempt, type(e).__name__)
            if attempt == MAX_RETRIES:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


def extract_japan_olr(nc_path: Path) -> list[dict]:
    """Extract Japan-region OLR data from a NetCDF file.

    Uses netCDF4 to read the file. Extracts grid cells within the Japan
    bounding box (lat 24-46, lon 122-150).

    Returns list of {date, lat, lon, olr} dicts.
    """
    import netCDF4

    rows = []
    try:
        ds = netCDF4.Dataset(str(nc_path), "r")
    except Exception as e:
        logger.error("Failed to open NetCDF %s: %s", nc_path.name, e)
        return []

    try:
        # Read coordinate arrays
        lats = ds.variables["lat"][:]
        lons = ds.variables["lon"][:]
        time_var = ds.variables["time"]
        times = netCDF4.num2date(time_var[:], units=time_var.units,
                                  calendar=getattr(time_var, "calendar", "standard"))

        # Find indices for Japan bounding box
        lat_mask = (lats >= SOUTH) & (lats <= NORTH)
        lon_mask = (lons >= WEST) & (lons <= EAST)

        lat_indices = np.where(lat_mask)[0]
        lon_indices = np.where(lon_mask)[0]

        if len(lat_indices) == 0 or len(lon_indices) == 0:
            logger.warning("No grid cells found in Japan bbox for %s", nc_path.name)
            return []

        japan_lats = lats[lat_indices]
        japan_lons = lons[lon_indices]

        logger.info("Extracting Japan region: %d lats x %d lons x %d days from %s",
                     len(japan_lats), len(japan_lons), len(times), nc_path.name)

        # Read OLR variable — subset spatially to reduce memory
        # olr shape is typically (time, lat, lon)
        olr_var = ds.variables["olr"]

        # Read the Japan spatial subset for all times
        # netCDF4 supports numpy-style indexing
        lat_slice = slice(int(lat_indices[0]), int(lat_indices[-1]) + 1)
        lon_slice = slice(int(lon_indices[0]), int(lon_indices[-1]) + 1)
        olr_data = olr_var[:, lat_slice, lon_slice]

        # Handle fill values / masked arrays
        if hasattr(olr_data, "filled"):
            olr_data = olr_data.filled(np.nan)

        for t_idx in range(len(times)):
            date_str = times[t_idx].strftime("%Y-%m-%d")

            for i, lat_val in enumerate(japan_lats):
                for j, lon_val in enumerate(japan_lons):
                    olr_val = float(olr_data[t_idx, i, j])

                    # Skip NaN / fill values and physically unreasonable values
                    if np.isnan(olr_val) or olr_val < 50 or olr_val > 400:
                        continue

                    rows.append({
                        "date": date_str,
                        "lat": round(float(lat_val), 1),
                        "lon": round(float(lon_val), 1),
                        "olr": round(olr_val, 2),
                    })

    except Exception as e:
        logger.error("Error extracting OLR from %s: %s", nc_path.name, e)
    finally:
        ds.close()

    return rows


async def main():
    await init_db()
    await init_olr_table()

    now = datetime.now(timezone.utc).isoformat()
    current_year = datetime.now(timezone.utc).year

    # Check existing data range to determine which years to fetch
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MIN(observed_at), MAX(observed_at), COUNT(DISTINCT observed_at) FROM olr"
        )
    if existing and existing[0][2]:
        logger.info("OLR existing: %s to %s (%d dates)",
                     existing[0][0], existing[0][1], existing[0][2])
        last_date = existing[0][1]
        last_year = int(last_date[:4]) if last_date else START_YEAR
        # Re-fetch from last_year (may be partial) and current year
        fetch_years = list(range(last_year, current_year + 1))
    else:
        fetch_years = list(range(START_YEAR, current_year + 1))

    # List available files on NCEI
    async with aiohttp.ClientSession() as session:
        available_files = await list_available_files(session)
        if not available_files:
            logger.error("Could not list NCEI files — aborting OLR fetch")
            return

        total_records = 0

        for year in fetch_years:
            if year not in available_files:
                logger.warning("No NCEI file available for year %d", year)
                continue

            filename = available_files[year]
            logger.info("Processing OLR %d (%s)...", year, filename)

            # Download to a temp file
            with tempfile.TemporaryDirectory() as tmpdir:
                nc_path = Path(tmpdir) / filename
                success = await download_netcdf(session, filename, nc_path)
                if not success:
                    logger.warning("Failed to download %s — skipping year %d", filename, year)
                    continue

                # Extract Japan region data (runs synchronously — CPU-bound)
                rows = await asyncio.get_event_loop().run_in_executor(
                    None, extract_japan_olr, nc_path
                )

            if not rows:
                logger.warning("No valid OLR data extracted for year %d", year)
                continue

            # Store in DB
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO olr
                       (observed_at, cell_lat, cell_lon, olr_wm2, received_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    [(r["date"], r["lat"], r["lon"], r["olr"], now) for r in rows],
                )
                await db.commit()

            total_records += len(rows)
            logger.info("OLR %d: %d records stored", year, len(rows))

            # Rate limit between years
            await asyncio.sleep(2.0)

    logger.info("OLR fetch complete: %d total records across %d year(s)",
                total_records, len(fetch_years))


if __name__ == "__main__":
    asyncio.run(main())
