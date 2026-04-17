"""Fetch LIS/OTD Monthly Climatology Time Series (LRMTS) for Japan region.

The LIS/OTD Low Resolution Monthly Climatology Time Series (LRMTS) combines
measurements from the NASA Optical Transient Detector (OTD, 1995-2000) and
the Lightning Imaging Sensor (LIS) aboard TRMM (1998-2014) into a 2.5°
global grid of monthly flash-rate values. Unlike `lohrmc` (0.5° climatology
averaged across years) this dataset preserves year-by-year values from
1995-05 to 2014-12 — the only NASA-distributed gridded lightning dataset
that covers the 2011-03-11 Tohoku earthquake window.

Physical mechanism:
    Pre-seismic radon emission → atmospheric ionization → electric field
    changes → anomalous lightning activity near fault zones. Monthly
    flash-rate anomalies (current month vs. climatological baseline) are
    robust long-baseline proxies for pre-earthquake atmospheric coupling.

Data source:
    NASA GHRC DAAC - LIS/OTD 2.5° Low Resolution Monthly Climatology
    Time Series V2.3.2015 (short_name: lolrmts)
    - Single NetCDF granule: LISOTD_LRMTS_V2.3.2015.nc
    - Coverage: 1995-05 to 2015-04, global 2.5°
    - Requires Earthdata authentication (Bearer token or basic auth)

Storage:
    Source 2.5° grid is coarser than our 2° target. We snap each 2° output
    cell to its containing 2.5° source cell (nearest-neighbor), so multiple
    2° cells may map to the same underlying 2.5° measurement — acceptable
    for earthquake-precursor feature extraction where the regional flash
    rate is what matters.

References:
    - Cecil, Buechler, Blakeslee (2014) Atmos. Res. 135-136:404-414
    - https://ghrc.earthdata.nasa.gov (search lolrmts)
"""

import asyncio
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from earthdata_auth import (
    get_earthdata_session, earthdata_fetch_bytes,
    EARTHDATA_USERNAME, EARTHDATA_PASSWORD, EARTHDATA_TOKEN,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
COLLECTION_SHORT_NAME = "lolrmts"

# Japan bounding box (matches fetch_wwlln_thunder_hour.py)
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 155.0

# Aggregate to 2° cells (matches lightning and iss_lis_lightning)
CELL_DEG = 2.0

TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)

# Dataset start. The time dim is labelled "Month_since_Jan_95" so idx=0
# corresponds to 1995-01 (240 months → last = 2014-12).
DATASET_START_YEAR = 1995
DATASET_START_MONTH = 1


async def init_table():
    """Create lightning_lis_otd table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lightning_lis_otd (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                flash_rate REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_lisotd_time
            ON lightning_lis_otd(observed_at)
        """)
        await db.commit()


async def search_granule(session: aiohttp.ClientSession) -> dict | None:
    """Search CMR for lolrmts NetCDF granule. Returns {url, title} or None."""
    params = {
        "short_name": COLLECTION_SHORT_NAME,
        "page_size": "10",
    }
    try:
        async with session.get(CMR_GRANULES_URL, params=params, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                logger.warning("CMR search HTTP %d", resp.status)
                return None
            data = await resp.json()
            entries = data.get("feed", {}).get("entry", [])
            for entry in entries:
                title = entry.get("title", "")
                if not title.endswith(".nc"):
                    continue
                for link in entry.get("links", []):
                    href = link.get("href", "")
                    if href.endswith(".nc") and ("ghrcw-protected" in href or "ghrc.earthdata" in href):
                        return {"url": href, "title": title}
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning("CMR search error: %s", e)
    return None


def month_index_to_date(idx: int) -> str:
    """Convert 0-based month index from dataset start to YYYY-MM-01."""
    total_months = DATASET_START_MONTH - 1 + idx
    year = DATASET_START_YEAR + total_months // 12
    month = (total_months % 12) + 1
    return f"{year}-{month:02d}-01"


def parse_lis_otd_netcdf(data_bytes: bytes) -> list[dict]:
    """Parse LIS/OTD LRMTS NetCDF file, aggregate to 2° cell-months.

    Returns list of {date, cell_lat, cell_lon, flash_rate}.
    """
    try:
        import netCDF4
        import numpy as np
    except ImportError:
        logger.warning("netCDF4/numpy not available, cannot parse LIS/OTD file")
        return []

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp.write(data_bytes)
        tmp_path = tmp.name

    results: list[dict] = []
    try:
        ds = netCDF4.Dataset(tmp_path, "r")
        var_names = list(ds.variables.keys())

        # Diagnostic dump (same pattern that helped WWLLN fetcher debugging)
        for vn in var_names:
            v = ds.variables[vn]
            fv = getattr(v, "_FillValue", None)
            missing = getattr(v, "missing_value", None)
            logger.info(
                "LISOTD diag: var=%s dims=%s shape=%s dtype=%s fill=%s missing=%s",
                vn, v.dimensions, v.shape, v.dtype, fv, missing,
            )

        # Locate the flash-rate variable. V2.3 LRMTS name is typically
        # "LRMTS_COM_FR" (combined flash rate). Try a few defensively.
        fr_var = None
        fr_name = None
        for cand in ("LRMTS_COM_FR", "LRMTS_LIS_FR", "COM_FR", "flash_rate",
                     "LRMTS_COM_THRFR", "flashes", "FR"):
            if cand in ds.variables:
                fr_var = ds.variables[cand]
                fr_name = cand
                break
        if fr_var is None:
            # Fallback: pick the 3D variable with largest size
            candidates = [
                (vn, ds.variables[vn]) for vn in var_names
                if len(ds.variables[vn].dimensions) == 3
            ]
            if candidates:
                candidates.sort(key=lambda x: np.prod(x[1].shape), reverse=True)
                fr_name, fr_var = candidates[0]
                logger.warning("LISOTD: no known flash-rate var found, "
                               "falling back to largest 3D var=%s", fr_name)
        if fr_var is None:
            logger.warning("LISOTD: no flash-rate variable in NetCDF (vars: %s)",
                           var_names)
            return []
        logger.info("LISOTD diag: selected fr_var=%s dims=%s shape=%s",
                    fr_name, fr_var.dimensions, fr_var.shape)

        # Locate lat/lon/time dims dynamically (learned from WWLLN axis fix).
        # Use lowercase substring matching so variants like "Month_since_Jan_95"
        # still resolve to the time axis.
        dim_names = fr_var.dimensions
        def _match(d: str, keywords) -> bool:
            dl = d.lower()
            return any(k in dl for k in keywords)
        lat_kw = ("lat",)
        lon_kw = ("lon",)
        time_kw = ("month", "time", "nmon", "mon_", "since_jan", "since_")
        lat_axis = next((i for i, d in enumerate(dim_names) if _match(d, lat_kw)), None)
        lon_axis = next((i for i, d in enumerate(dim_names) if _match(d, lon_kw)), None)
        time_axis = next((i for i, d in enumerate(dim_names) if _match(d, time_kw)), None)
        if lat_axis is None or lon_axis is None or time_axis is None:
            logger.warning("LISOTD: could not identify axes in dims=%s", dim_names)
            return []

        # Locate lat/lon coordinate arrays
        lat = lon = None
        for cand in ("Latitude", "latitude", "lat", "Lat"):
            if cand in ds.variables:
                lat = ds.variables[cand][:]
                break
        for cand in ("Longitude", "longitude", "lon", "Lon"):
            if cand in ds.variables:
                lon = ds.variables[cand][:]
                break
        if lat is None or lon is None:
            logger.warning("LISOTD: no lat/lon variable in NetCDF (vars: %s)",
                           var_names)
            return []

        raw = fr_var[:]
        data = np.transpose(raw, (time_axis, lat_axis, lon_axis))
        n_months = data.shape[0]
        logger.info(
            "LISOTD diag: after transpose shape=%s dtype=%s (time=%d lat=%d lon=%d)",
            data.shape, data.dtype, time_axis, lat_axis, lon_axis,
        )

        lat_arr = np.asarray(lat, dtype=np.float64)
        lon_arr = np.asarray(lon, dtype=np.float64)
        # LIS/OTD uses 0-360 longitude in some V2 products; normalize to [-180,180]
        if lon_arr.max() > 180.0:
            lon_arr = ((lon_arr + 180.0) % 360.0) - 180.0
            logger.info("LISOTD diag: normalized lon 0-360 → -180-180")
        logger.info(
            "LISOTD diag: lat range [%.3f,%.3f] n=%d, lon range [%.3f,%.3f] n=%d, n_months=%d",
            float(lat_arr.min()), float(lat_arr.max()), len(lat_arr),
            float(lon_arr.min()), float(lon_arr.max()), len(lon_arr), n_months,
        )

        lat_mask = (lat_arr >= JAPAN_LAT_MIN) & (lat_arr <= JAPAN_LAT_MAX)
        lon_mask = (lon_arr >= JAPAN_LON_MIN) & (lon_arr <= JAPAN_LON_MAX)
        lat_idx = np.where(lat_mask)[0]
        lon_idx = np.where(lon_mask)[0]
        if len(lat_idx) == 0 or len(lon_idx) == 0:
            logger.warning("LISOTD: Japan region not in grid")
            return []

        # With lon possibly reordered after 0-360 normalization, still pick
        # contiguous slice by index ranges (lon array order preserved).
        lat_window = lat_arr[lat_idx[0]:lat_idx[-1] + 1]
        lon_window = lon_arr[lon_idx[0]:lon_idx[-1] + 1]
        window = data[:, lat_idx[0]:lat_idx[-1] + 1, lon_idx[0]:lon_idx[-1] + 1]

        # 2° cell buckets (max-reduce over native 2.5° subcells)
        lat_bucket = np.round(lat_window / CELL_DEG).astype(np.int64)
        lon_bucket = np.round(lon_window / CELL_DEG).astype(np.int64)
        lat_bucket_min = int(lat_bucket.min())
        lon_bucket_min = int(lon_bucket.min())
        lat_idx_2d = lat_bucket - lat_bucket_min
        lon_idx_2d = lon_bucket - lon_bucket_min
        n_clats = int(lat_idx_2d.max()) + 1
        n_clons = int(lon_idx_2d.max()) + 1
        ci_grid, cj_grid = np.meshgrid(lat_idx_2d, lon_idx_2d, indexing="ij")

        for month_idx in range(n_months):
            date = month_index_to_date(month_idx)
            raw_slab = window[month_idx]
            if hasattr(raw_slab, "mask"):
                slab = raw_slab.astype(np.float64).filled(np.nan)
            else:
                slab = np.asarray(raw_slab, dtype=np.float64)
            # Flash-rate is non-negative; fill values typically -9999.
            # Keep values in sane physical range (< 1000 fl/km²/yr).
            valid = np.isfinite(slab) & (slab >= 0) & (slab < 1e4)
            # idx=194 is 2011-03 (Tohoku earthquake month) — log it for verification.
            if month_idx < 2 or month_idx in (193, 194, 195) or not valid.any():
                finite = slab[np.isfinite(slab)]
                logger.info(
                    "LISOTD diag month=%03d (%s): shape=%s valid=%d finite_min=%s finite_max=%s sample=%s",
                    month_idx, date, slab.shape, int(valid.sum()),
                    float(finite.min()) if finite.size else None,
                    float(finite.max()) if finite.size else None,
                    finite[:5].tolist() if finite.size else [],
                )
            if not valid.any():
                continue

            cell_max = np.full((n_clats, n_clons), -np.inf, dtype=np.float64)
            np.maximum.at(cell_max, (ci_grid[valid], cj_grid[valid]), slab[valid])

            cell_rows, cell_cols = np.where(np.isfinite(cell_max))
            for cr, cc in zip(cell_rows.tolist(), cell_cols.tolist()):
                clat = (cr + lat_bucket_min) * CELL_DEG
                clon = (cc + lon_bucket_min) * CELL_DEG
                results.append({
                    "date": date,
                    "cell_lat": float(clat),
                    "cell_lon": float(clon),
                    "flash_rate": float(cell_max[cr, cc]),
                })

        ds.close()
    except Exception as e:
        logger.warning("LISOTD NetCDF parse error: %s", e)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return results


async def main():
    await init_db()
    await init_table()

    has_auth = (EARTHDATA_USERNAME and EARTHDATA_PASSWORD) or EARTHDATA_TOKEN
    if not has_auth:
        logger.info("LISOTD: no Earthdata credentials, skipping")
        return

    # If already populated (single-granule dataset, monotone import),
    # skip unless INGEST_ALL override is set.
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT COUNT(*) FROM lightning_lis_otd"
        )
    existing_n = existing[0][0] if existing else 0
    if existing_n > 0 and not os.environ.get("LISOTD_FORCE"):
        logger.info("LISOTD: already populated (%d rows), skipping. "
                    "Set LISOTD_FORCE=1 to reimport.", existing_n)
        return

    async with aiohttp.ClientSession() as public_session:
        granule = await search_granule(public_session)
    if not granule:
        logger.warning("LISOTD: no granule found via CMR")
        return
    logger.info("LISOTD: found granule %s", granule["title"])

    session = await get_earthdata_session()
    try:
        logger.info("LISOTD: downloading %s", granule["url"])
        status, data = await earthdata_fetch_bytes(session, granule["url"],
                                                   timeout=TIMEOUT)
        if status != 200 or not data:
            logger.warning("LISOTD: fetch failed (HTTP %d, %d bytes)",
                           status, len(data) if data else 0)
            return
        logger.info("LISOTD: parsing %d bytes", len(data))
        rows = parse_lis_otd_netcdf(data)
        if not rows:
            logger.warning("LISOTD: no rows produced")
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        async with safe_connect() as db:
            await db.executemany(
                """INSERT OR IGNORE INTO lightning_lis_otd
                   (observed_at, cell_lat, cell_lon, flash_rate, received_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [(r["date"], r["cell_lat"], r["cell_lon"], r["flash_rate"], now_iso)
                 for r in rows],
            )
            await db.commit()
        logger.info("LISOTD: inserted %d cell-month rows "
                    "(range %s → %s)", len(rows), rows[0]["date"], rows[-1]["date"])
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
