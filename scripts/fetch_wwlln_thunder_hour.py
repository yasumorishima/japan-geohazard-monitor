"""Fetch WWLLN Monthly Thunder Hour data for Japan region.

The World Wide Lightning Location Network (WWLLN) Monthly Thunder Hour
dataset provides the number of hours per month during which at least two
WWLLN strokes were observed within 15 km of each grid point. Coverage
spans 2013-01 to 2025-12 at 0.05° resolution, distributed as one
NetCDF file per year.

Physical mechanism:
    Pre-seismic radon emission → atmospheric ionization → electric field
    changes → anomalous lightning activity near fault zones.
    Thunder-hour aggregates are less sensitive to detection-network
    geometry than raw stroke counts, useful as a long-baseline anomaly
    feature complementing the daily Blitzortung/ISS LIS streams.

Data source:
    NASA GHRC DAAC - WWLLN Monthly Thunder Hour Data (wwllnmth)
    - Granules: WWLLN_th_YYYY.nc (one file per year, 12 monthly layers)
    - Coverage: 2013-01 to 2025-12, global 0.05°
    - Requires Earthdata authentication (Bearer token or basic auth)
    - CMR API for granule discovery (no auth needed)

Storage:
    Aggregates 0.05° native grid to 2° cells (matching `lightning` and
    `iss_lis_lightning` resolution). Per cell-month, stores the **max**
    thunder-hours across the ~40×40 native subcells inside — preserves
    the peak-activity signal that an averaging over mostly-empty ocean
    subcells would dilute. Thunder-hour is already a per-15km-radius
    integration so max is the physically meaningful 2°-cell summary.

References:
    - Thunder Hour dataset doc: https://ghrc.earthdata.nasa.gov (search wwllnmth)
    - Holzworth et al. (2021) JGR 126:e2020JD033884
    - Pulinets & Ouzounov (2011) Adv. Space Res. 47:413-424
"""

import asyncio
import logging
import os
import re
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
COLLECTION_SHORT_NAME = "wwllnmth"

# Japan bounding box (matches fetch_iss_lis_lightning.py)
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 155.0

# Aggregate to 2° cells (matches lightning and iss_lis_lightning)
CELL_DEG = 2.0

TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)

# Each annual file is ~620 MB and parses in a few seconds; 13 years
# of historical data would blow the step timeout + bandwidth cap.
# Override via WWLLN_TH_MAX_FILES for debug runs.
MAX_FILES_PER_RUN = int(os.environ.get("WWLLN_TH_MAX_FILES", "5"))


async def init_table():
    """Create lightning_thunder_hour table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lightning_thunder_hour (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                thunder_hours REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_lth_time
            ON lightning_thunder_hour(observed_at)
        """)
        await db.commit()


def snap_to_cell(lat: float, lon: float) -> tuple[float, float]:
    """Snap coordinates to 2° grid cell center (matches iss_lis_lightning)."""
    return (
        round(lat / CELL_DEG) * CELL_DEG,
        round(lon / CELL_DEG) * CELL_DEG,
    )


async def search_granules(session: aiohttp.ClientSession) -> list[dict]:
    """Search CMR for wwllnmth annual granules.

    CMR is public (no auth needed). Returns list of {url, title, year}.
    """
    params = {
        "short_name": COLLECTION_SHORT_NAME,
        "page_size": "100",
        "sort_key": "start_date",
    }
    granules: list[dict] = []
    try:
        async with session.get(CMR_GRANULES_URL, params=params, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                logger.warning("CMR search HTTP %d", resp.status)
                return []
            data = await resp.json()
            entries = data.get("feed", {}).get("entry", [])
            for entry in entries:
                title = entry.get("title", "")
                m = re.search(r"(\d{4})", title)
                if not m:
                    continue
                year = int(m.group(1))
                nc_url = None
                for link in entry.get("links", []):
                    href = link.get("href", "")
                    if href.endswith(".nc") and ("ghrcw-protected" in href or "ghrc.earthdata" in href):
                        nc_url = href
                        break
                if nc_url:
                    granules.append({"url": nc_url, "title": title, "year": year})
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning("CMR search error: %s", e)
    return granules


def parse_wwlln_netcdf(data_bytes: bytes, year: int) -> list[dict]:
    """Parse a WWLLN annual NetCDF file, aggregate to 2° cell-months.

    Returns list of {date, cell_lat, cell_lon, thunder_hours}.
    """
    try:
        import netCDF4
        import numpy as np
    except ImportError:
        logger.warning("netCDF4/numpy not available, cannot parse WWLLN file")
        return []

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp.write(data_bytes)
        tmp_path = tmp.name

    results: list[dict] = []
    try:
        ds = netCDF4.Dataset(tmp_path, "r")
        var_names = list(ds.variables.keys())

        # Diagnostic dump: every variable's shape/dtype/_FillValue and dims
        for vn in var_names:
            v = ds.variables[vn]
            fv = getattr(v, "_FillValue", None)
            missing = getattr(v, "missing_value", None)
            logger.info(
                "WWLLN %d diag: var=%s dims=%s shape=%s dtype=%s fill=%s missing=%s",
                year, vn, v.dimensions, v.shape, v.dtype, fv, missing,
            )

        # Locate thunder_hours variable defensively
        th_var = None
        th_name = None
        for cand in ("thunder_hours", "thunder_hour", "TH", "th", "ThunderHours"):
            if cand in ds.variables:
                th_var = ds.variables[cand]
                th_name = cand
                break
        if th_var is None:
            logger.warning("No thunder_hours variable in NetCDF (vars: %s)", var_names)
            return []
        logger.info("WWLLN %d diag: selected th_var=%s dims=%s shape=%s",
                    year, th_name, th_var.dimensions, th_var.shape)

        # Locate lat/lon
        lat = None
        lon = None
        for cand in ("latitude", "lat", "Latitude"):
            if cand in ds.variables:
                lat = ds.variables[cand][:]
                break
        for cand in ("longitude", "lon", "Longitude"):
            if cand in ds.variables:
                lon = ds.variables[cand][:]
                break
        if lat is None or lon is None:
            logger.warning("No lat/lon variable in NetCDF (vars: %s)", var_names)
            return []

        # Observed layout in wwllnmth: dims=('nlon','nlat','nmon'), shape=(7200,3600,12).
        # Normalize to (time, lat, lon) regardless of source order so the rest
        # of this function can assume that shape.
        dim_names = th_var.dimensions
        lat_dim_cands = {"nlat", "lat", "latitude", "Latitude"}
        lon_dim_cands = {"nlon", "lon", "longitude", "Longitude"}
        time_dim_cands = {"nmon", "mon", "month", "time", "Time"}
        lat_axis = next((i for i, d in enumerate(dim_names) if d in lat_dim_cands), None)
        lon_axis = next((i for i, d in enumerate(dim_names) if d in lon_dim_cands), None)
        time_axis = next((i for i, d in enumerate(dim_names) if d in time_dim_cands), None)
        if lat_axis is None or lon_axis is None:
            logger.warning("WWLLN %d: could not identify lat/lon axes in dims=%s",
                           year, dim_names)
            return []

        th_raw = th_var[:]
        if time_axis is not None:
            th_data = np.transpose(th_raw, (time_axis, lat_axis, lon_axis))
            n_months = th_data.shape[0]
        else:
            th_data = np.transpose(th_raw, (lat_axis, lon_axis))[np.newaxis, ...]
            n_months = 1
        logger.info(
            "WWLLN %d diag: after transpose th_data shape=%s dtype=%s (time_axis=%s lat_axis=%s lon_axis=%s)",
            year, th_data.shape, th_data.dtype, time_axis, lat_axis, lon_axis,
        )

        lat_arr = np.asarray(lat)
        lon_arr = np.asarray(lon)
        lat_mask = (lat_arr >= JAPAN_LAT_MIN) & (lat_arr <= JAPAN_LAT_MAX)
        lon_mask = (lon_arr >= JAPAN_LON_MIN) & (lon_arr <= JAPAN_LON_MAX)
        lat_idx = np.where(lat_mask)[0]
        lon_idx = np.where(lon_mask)[0]
        if len(lat_idx) == 0 or len(lon_idx) == 0:
            logger.warning("WWLLN %d: Japan region not in grid", year)
            return []

        window = th_data[:, lat_idx[0]:lat_idx[-1] + 1, lon_idx[0]:lon_idx[-1] + 1]

        lat_window = lat_arr[lat_idx[0]:lat_idx[-1] + 1]
        lon_window = lon_arr[lon_idx[0]:lon_idx[-1] + 1]

        # Vectorized 2° bucket assignment per native row/col.
        # ~440×660 = 290K subcells × 12 months would TLE in Python loops,
        # so use np.maximum.at to reduce in C.
        lat_bucket = np.round(lat_window / CELL_DEG).astype(np.int64)
        lon_bucket = np.round(lon_window / CELL_DEG).astype(np.int64)
        lat_bucket_min = int(lat_bucket.min())
        lon_bucket_min = int(lon_bucket.min())
        lat_idx_2d = lat_bucket - lat_bucket_min
        lon_idx_2d = lon_bucket - lon_bucket_min
        n_clats = int(lat_idx_2d.max()) + 1
        n_clons = int(lon_idx_2d.max()) + 1
        # Per-element bucket index grids (broadcast at use site)
        ci_grid, cj_grid = np.meshgrid(lat_idx_2d, lon_idx_2d, indexing="ij")

        for month_idx in range(n_months):
            month = month_idx + 1
            date = f"{year}-{month:02d}-01"
            raw_slab = window[month_idx]
            if hasattr(raw_slab, "mask"):
                slab = raw_slab.astype(np.float64).filled(np.nan)
            else:
                slab = np.asarray(raw_slab, dtype=np.float64)
            finite = np.isfinite(slab)
            valid = finite & (slab >= 0) & (slab < 1e6)
            if month_idx < 2 or not valid.any():
                finite_vals = slab[finite] if finite.any() else np.array([])
                logger.info(
                    "WWLLN %d diag month=%02d: slab shape=%s finite=%d valid=%d min=%s max=%s sample=%s",
                    year, month, slab.shape, int(finite.sum()), int(valid.sum()),
                    float(finite_vals.min()) if finite_vals.size else None,
                    float(finite_vals.max()) if finite_vals.size else None,
                    finite_vals[:5].tolist() if finite_vals.size else [],
                )
            if not valid.any():
                continue

            # Max-reduce native subcells into 2° buckets in C.
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
                    "thunder_hours": float(cell_max[cr, cc]),
                })

        ds.close()
    except Exception as e:
        logger.warning("WWLLN NetCDF parse error for %d: %s", year, e)
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
        logger.info("WWLLN: no Earthdata credentials, skipping")
        return

    # Already-imported years (skip those entirely)
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT DISTINCT substr(observed_at, 1, 4) FROM lightning_thunder_hour"
        )
    done_years = {r[0] for r in existing}
    if done_years:
        logger.info("WWLLN existing: %d years (%s)", len(done_years), sorted(done_years))

    async with aiohttp.ClientSession() as session:
        granules = await search_granules(session)
    logger.info("WWLLN: CMR returned %d annual granules", len(granules))

    pending = [g for g in granules if str(g["year"]) not in done_years]
    if not pending:
        logger.info("WWLLN: all available years already imported")
        return

    pending.sort(key=lambda g: g["year"])
    pending = pending[:MAX_FILES_PER_RUN]
    logger.info("WWLLN: processing %d years this run: %s",
                len(pending), [g["year"] for g in pending])

    now_iso = datetime.now(timezone.utc).isoformat()
    session = await get_earthdata_session()
    total_rows = 0
    try:
        for g in pending:
            year = g["year"]
            logger.info("WWLLN %d: downloading %s", year, g["title"])
            status, data = await earthdata_fetch_bytes(session, g["url"], timeout=TIMEOUT)
            if status != 200 or not data:
                logger.warning("WWLLN %d: fetch failed (HTTP %d, %d bytes)",
                               year, status, len(data) if data else 0)
                continue
            logger.info("WWLLN %d: parsing %d bytes", year, len(data))
            rows = parse_wwlln_netcdf(data, year)
            if not rows:
                logger.warning("WWLLN %d: no rows produced", year)
                continue
            async with safe_connect() as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO lightning_thunder_hour
                       (observed_at, cell_lat, cell_lon, thunder_hours, received_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    [(r["date"], r["cell_lat"], r["cell_lon"], r["thunder_hours"], now_iso)
                     for r in rows],
                )
                await db.commit()
            total_rows += len(rows)
            logger.info("WWLLN %d: inserted %d cell-month rows", year, len(rows))
            await asyncio.sleep(1.0)
    finally:
        await session.close()

    logger.info("WWLLN fetch complete: %d total rows across %d years",
                total_rows, len(pending))


if __name__ == "__main__":
    asyncio.run(main())
