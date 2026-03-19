"""Fetch GRACE/GRACE-FO gravity anomaly data from GFZ GravIS (public, no auth).

GRACE (2002-2017) and GRACE-FO (2018-present) twin satellites measure
Earth's gravity field changes caused by mass redistribution. Monthly
solutions detect water mass, ice mass, and — critically — tectonic
mass changes at plate boundaries.

Physical mechanism:
    Pre-seismic fluid migration in the crust causes detectable gravity
    changes. Documented gravity anomalies were observed before the 2011
    Tohoku M9.0 earthquake (Matsuo & Heki, 2011). Monthly resolution
    limits detection of short-term precursors, but captures slow strain
    accumulation over subduction zones.

Data source: GFZ GravIS RL06 Level-3 Terrestrial Water Storage (TWS) Anomalies
    - Monthly equivalent water height anomalies (cm), 1-degree global grid
    - GRACE (2002/04-2017/06) + GRACE-FO (2018/06-present) combined
    - Single NetCDF file from GFZ ISDC (public HTTPS, no authentication)
    - Gap: June 2017 - May 2018 (between GRACE and GRACE-FO)
    - Anomalies relative to 2002/04-2020/03 time-mean
    - TWS = gravity-based total water storage ≈ LWE thickness

Target features:
    - gravity_anomaly_rate: month-to-month LWE change rate per cell (cm/month)

Citation:
    Boergens, E., Dobslaw, H., Dill, R. (2019):
    GFZ GravIS RL06 Continental Water Storage Anomalies.
    V. 0006. GFZ Data Services. https://doi.org/10.5880/GFZ.GRAVIS_06_L3_TWS

References:
    - Matsuo & Heki (2011) Geophys. Res. Lett. 38:L17312
    - Panet et al. (2018) Nat. Geosci. 11:611-615
"""

import asyncio
import logging
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# GFZ ISDC public HTTPS endpoint (no authentication required)
# Switched from FTP to HTTPS in June 2025; FTP shut down March 2, 2026.
GFZ_ISDC_TWS_URL = (
    "https://isdc-data.gfz.de/grace/GravIS/GFZ/Level-3/TWS/"
    "GRAVIS-3_GFZOP_0600_TWS_GRID_GFZ_0006.nc"
)

# Cache directory for the downloaded NetCDF (avoid re-downloading 496MB every run)
CACHE_DIR = Path(os.environ.get("GRACE_CACHE_DIR", tempfile.gettempdir())) / "grace_cache"

# Japan bbox (1-degree grid subset)
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 150.0

# GRACE/GRACE-FO gap period (no data available)
GRACE_GAP_START = "2017-06"
GRACE_GAP_END = "2018-05"

DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=1800, connect=60)  # 30 min for 496MB
CHUNK_SIZE = 1024 * 1024  # 1MB chunks


async def init_gravity_table():
    """Create gravity anomaly table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS gravity_mascon (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                lwe_thickness_cm REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_gravity_time
            ON gravity_mascon(observed_at)
        """)
        await db.commit()


async def download_netcdf(session: aiohttp.ClientSession) -> Path | None:
    """Download GFZ GravIS TWS NetCDF with conditional GET (If-Modified-Since).

    Returns the path to the cached NetCDF file, or None on failure.
    Uses If-Modified-Since to avoid re-downloading if the file hasn't changed.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    nc_path = CACHE_DIR / "GFZ_TWS_v0006.nc"
    last_modified_path = CACHE_DIR / "GFZ_TWS_v0006.last_modified"

    headers = {}
    if nc_path.exists() and last_modified_path.exists():
        lm_str = last_modified_path.read_text().strip()
        if lm_str:
            headers["If-Modified-Since"] = lm_str
            logger.info("Checking if GFZ TWS file has been updated (If-Modified-Since: %s)", lm_str)

    try:
        async with session.get(GFZ_ISDC_TWS_URL, headers=headers, timeout=DOWNLOAD_TIMEOUT) as resp:
            if resp.status == 304:
                logger.info("GFZ TWS file not modified, using cached version (%s)", nc_path)
                return nc_path

            if resp.status != 200:
                logger.error("GFZ ISDC returned HTTP %d for TWS NetCDF", resp.status)
                # Fall back to cache if available
                if nc_path.exists():
                    logger.info("Using previously cached file despite HTTP error")
                    return nc_path
                return None

            total_size = resp.content_length or 0
            logger.info(
                "Downloading GFZ GravIS TWS NetCDF (%.1f MB) ...",
                total_size / (1024 * 1024) if total_size else 0,
            )

            # Stream to temp file, then rename atomically
            tmp_path = nc_path.with_suffix(".tmp")
            downloaded = 0
            last_log_pct = 0
            with open(tmp_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        pct = int(downloaded * 100 / total_size)
                        if pct >= last_log_pct + 20:
                            logger.info("  Download progress: %d%%", pct)
                            last_log_pct = pct

            # Rename atomically
            tmp_path.replace(nc_path)
            logger.info("Download complete: %.1f MB", downloaded / (1024 * 1024))

            # Save Last-Modified for conditional GET next time
            lm = resp.headers.get("Last-Modified", "")
            if lm:
                last_modified_path.write_text(lm)

            return nc_path

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error("Download failed: %s: %s", type(e).__name__, e)
        if nc_path.exists():
            logger.info("Using previously cached file despite download error")
            return nc_path
        return None


def extract_japan_tws(nc_path: Path) -> list[dict]:
    """Extract Japan-region TWS anomaly data from GFZ GravIS NetCDF.

    The NetCDF contains:
        - tws(time, lat, lon): gravity-based TWS anomaly in cm EWH
        - time: days since reference epoch
        - lat: 1-degree grid latitudes
        - lon: 1-degree grid longitudes

    Returns list of dicts with date, lat, lon, lwe (cm).
    """
    try:
        import netCDF4
    except ImportError:
        logger.error(
            "netCDF4 is required to read GRACE data. "
            "Install with: pip install netCDF4"
        )
        return []

    try:
        ds = netCDF4.Dataset(str(nc_path), "r")
    except Exception as e:
        logger.error("Failed to open NetCDF: %s", e)
        return []

    try:
        # Read coordinate arrays
        lats = ds.variables["lat"][:]
        lons = ds.variables["lon"][:]
        time_var = ds.variables["time"]
        tws_var = ds.variables["tws"]  # (time, lat, lon) in cm EWH

        # Find Japan region indices
        lat_mask = (lats >= JAPAN_LAT_MIN) & (lats <= JAPAN_LAT_MAX)
        lon_mask = (lons >= JAPAN_LON_MIN) & (lons <= JAPAN_LON_MAX)

        lat_indices = [i for i, m in enumerate(lat_mask) if m]
        lon_indices = [i for i, m in enumerate(lon_mask) if m]

        if not lat_indices or not lon_indices:
            logger.error(
                "No grid cells found in Japan bbox. "
                "Lat range: %.1f-%.1f, Lon range: %.1f-%.1f",
                lats.min(), lats.max(), lons.min(), lons.max(),
            )
            return []

        japan_lats = lats[lat_indices]
        japan_lons = lons[lon_indices]

        logger.info(
            "Japan region: %d lat x %d lon cells (%.1f-%.1f N, %.1f-%.1f E)",
            len(japan_lats), len(japan_lons),
            japan_lats.min(), japan_lats.max(),
            japan_lons.min(), japan_lons.max(),
        )

        # Convert time to dates
        try:
            times = netCDF4.num2date(
                time_var[:],
                units=time_var.units,
                calendar=getattr(time_var, "calendar", "standard"),
            )
        except Exception:
            # Fallback: try cftime
            import cftime
            times = cftime.num2date(
                time_var[:],
                units=time_var.units,
                calendar=getattr(time_var, "calendar", "standard"),
            )

        # Extract Japan subset
        # Use numpy slicing for efficiency: tws[time, lat_slice, lon_slice]
        import numpy as np
        lat_start, lat_end = lat_indices[0], lat_indices[-1] + 1
        lon_start, lon_end = lon_indices[0], lon_indices[-1] + 1

        tws_japan = tws_var[:, lat_start:lat_end, lon_start:lon_end]

        # Handle masked values (ocean/missing data)
        if hasattr(tws_japan, "filled"):
            fill_value = getattr(tws_var, "_FillValue", None) or 1e20
            tws_japan = tws_japan.filled(fill_value=np.nan)

        rows = []
        n_times = len(times)
        n_skipped_gap = 0
        n_skipped_nan = 0

        for t_idx in range(n_times):
            dt = times[t_idx]
            # Format as YYYY-MM-DD (first day of month)
            if hasattr(dt, "strftime"):
                date_str = dt.strftime("%Y-%m-%d")
                month_str = dt.strftime("%Y-%m")
            else:
                date_str = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
                month_str = f"{dt.year:04d}-{dt.month:02d}"

            # Skip GRACE/GRACE-FO gap period
            if GRACE_GAP_START <= month_str <= GRACE_GAP_END:
                n_skipped_gap += 1
                continue

            for li, lat_idx in enumerate(range(lat_end - lat_start)):
                for lo, lon_idx in enumerate(range(lon_end - lon_start)):
                    val = float(tws_japan[t_idx, lat_idx, lon_idx])
                    if np.isnan(val) or abs(val) > 1e10:
                        n_skipped_nan += 1
                        continue

                    rows.append({
                        "date": date_str,
                        "lat": round(float(japan_lats[li]), 2),
                        "lon": round(float(japan_lons[lo]), 2),
                        "lwe": round(val, 4),
                    })

        logger.info(
            "Extracted %d records (%d months, skipped %d gap months, %d NaN cells)",
            len(rows), n_times - n_skipped_gap, n_skipped_gap, n_skipped_nan,
        )
        return rows

    except Exception as e:
        logger.error("Error extracting TWS data: %s", e, exc_info=True)
        return []
    finally:
        ds.close()


async def main():
    await init_db()
    await init_gravity_table()

    now = datetime.now(timezone.utc).isoformat()

    async with aiohttp.ClientSession() as session:
        nc_path = await download_netcdf(session)

    if not nc_path:
        logger.info(
            "GRACE TWS NetCDF not available. "
            "Monthly gravity anomaly features will be excluded via dynamic selection."
        )
        return

    # Extract Japan-region data (runs synchronously — CPU-bound NetCDF parsing)
    rows = extract_japan_tws(nc_path)

    if not rows:
        logger.info(
            "No Japan-region TWS data extracted from NetCDF. "
            "Monthly gravity anomaly features will be excluded via dynamic selection."
        )
        return

    # Store in database (batch insert with INSERT OR IGNORE for idempotency)
    async with aiosqlite.connect(DB_PATH) as db:
        # Insert in batches to avoid huge parameter lists
        BATCH_SIZE = 5000
        total_inserted = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            await db.executemany(
                """INSERT OR IGNORE INTO gravity_mascon
                   (observed_at, cell_lat, cell_lon, lwe_thickness_cm, received_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [(r["date"], r["lat"], r["lon"], r["lwe"], now) for r in batch],
            )
            total_inserted += len(batch)
        await db.commit()

    logger.info("GRACE fetch complete: %d records stored", total_inserted)


if __name__ == "__main__":
    asyncio.run(main())
