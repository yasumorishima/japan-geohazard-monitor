"""Fetch ocean color (chlorophyll-a) data from NOAA CoastWatch ERDDAP.

Ocean color satellites measure chlorophyll-a concentration, which serves
as a proxy for phytoplankton biomass. Anomalous changes in ocean color
near subduction zones may indicate:

Physical mechanism:
    1. Submarine hydrothermal/volcanic activity → nutrient upwelling
       → phytoplankton bloom → chlorophyll spike
    2. Submarine landslides → turbidity increase → color change
    3. Seafloor gas seeps (methane, CO2) → localized ocean chemistry
       changes → biological response

Japan's subduction zones (Nankai Trough, Japan Trench, Izu-Bonin) have
active submarine volcanism. Changes in ocean color could indicate
subsurface tectonic activity not detectable on land.

Data source: NOAA CoastWatch ERDDAP - MODIS Aqua chlorophyll-a monthly
    - Dataset: erdMH1chlamday (Level 3 Mapped Monthly, 4km)
    - Variable: chlorophyll (mg/m³)
    - No authentication required
    - CSV output with spatial/temporal subsetting

Target features:
    - ocean_color_anomaly: chlorophyll-a deviation from 30-day baseline (σ)

References:
    - Escalera-Reyes et al. (2019) Remote Sensing 11:2405
    - Yang et al. (2022) Geophysical Research Letters 49:e2022GL098939
"""

import asyncio
import csv
import io
import logging
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NOAA CoastWatch ERDDAP - MODIS Aqua monthly chlorophyll-a
ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
DATASET_ID = "erdMH1chlamday"

# Japan ocean bbox (wider than land to cover subduction zones)
LAT_MIN = 24.0
LAT_MAX = 46.0
LON_MIN = 122.0
LON_MAX = 150.0

# Aggregate to 2° cells for compatibility with prediction grid
CELL_DEG = 2.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)
START_YEAR = 2011


def _build_erddap_url(time_iso: str) -> str:
    """Build ERDDAP griddap CSV URL for a single monthly time step.

    The erdMH1chlamday dataset has monthly composites. The time
    parameter selects the nearest available month.
    """
    return (
        f"{ERDDAP_BASE}/{DATASET_ID}.csv"
        f"?chlorophyll[({time_iso})]"
        f"[({LAT_MIN}):({LAT_MAX})]"
        f"[({LON_MIN}):({LON_MAX})]"
    )


async def init_ocean_color_table():
    """Create ocean color table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ocean_color (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                chlor_a_mg_m3 REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_time
            ON ocean_color(observed_at)
        """)
        await db.commit()


def _parse_erddap_csv(text: str) -> list[dict]:
    """Parse ERDDAP CSV response into row dicts with 2° cell aggregation.

    ERDDAP CSV has two header rows:
        Row 1: column names (time, latitude, longitude, chlorophyll)
        Row 2: units (UTC, degrees_north, degrees_east, mg m-3)
    Followed by data rows.
    """
    cell_values: dict[tuple[str, float, float], list[float]] = defaultdict(list)

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)  # column names
        _ = next(reader)       # units row
    except StopIteration:
        logger.warning("ERDDAP CSV response has no header rows")
        return []

    # Find column indices
    col_map = {name.strip().lower(): i for i, name in enumerate(header)}
    time_idx = col_map.get("time")
    lat_idx = col_map.get("latitude")
    lon_idx = col_map.get("longitude")
    chl_idx = col_map.get("chlorophyll")

    if any(idx is None for idx in (time_idx, lat_idx, lon_idx, chl_idx)):
        logger.warning("ERDDAP CSV missing expected columns: %s", header)
        return []

    for row in reader:
        if len(row) <= max(time_idx, lat_idx, lon_idx, chl_idx):
            continue
        try:
            time_str = row[time_idx].strip()
            lat = float(row[lat_idx])
            lon = float(row[lon_idx])
            chl_val = float(row[chl_idx])
        except (ValueError, IndexError):
            continue

        # Filter invalid / fill values
        if chl_val < 0 or chl_val > 100 or math.isnan(chl_val):
            continue

        # Extract date (YYYY-MM-DD) from ISO timestamp
        date_str = time_str[:10]

        # Aggregate to 2° cell
        cell_lat = math.floor(lat / CELL_DEG) * CELL_DEG + CELL_DEG / 2
        cell_lon = math.floor(lon / CELL_DEG) * CELL_DEG + CELL_DEG / 2

        cell_values[(date_str, cell_lat, cell_lon)].append(chl_val)

    rows = []
    for (date_str, clat, clon), vals in cell_values.items():
        rows.append({
            "date": date_str,
            "lat": round(clat, 1),
            "lon": round(clon, 1),
            "chlor_a": round(sum(vals) / len(vals), 4),
        })
    return rows


async def fetch_chlor_month(
    session: aiohttp.ClientSession, year: int, month: int
) -> list[dict]:
    """Fetch monthly chlorophyll-a from CoastWatch ERDDAP for one month."""
    # Use mid-month date for the ERDDAP time query
    time_iso = f"{year:04d}-{month:02d}-16T00:00:00Z"
    url = _build_erddap_url(time_iso)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    if not text or "<html" in text[:200].lower():
                        logger.debug("ERDDAP returned HTML for %04d-%02d", year, month)
                        return []
                    return _parse_erddap_csv(text)
                elif resp.status == 404:
                    logger.debug("No ERDDAP data for %04d-%02d (404)", year, month)
                    return []
                else:
                    body = await resp.text()
                    logger.debug(
                        "ERDDAP HTTP %d for %04d-%02d: %s",
                        resp.status, year, month, body[:200],
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug(
                    "ERDDAP %04d-%02d failed after %d retries: %s",
                    year, month, MAX_RETRIES, type(e).__name__,
                )
            await asyncio.sleep(2 ** attempt)

    return []


def _generate_month_list(start_year: int) -> list[tuple[int, int]]:
    """Generate (year, month) tuples from start_year to current month."""
    now = datetime.now(timezone.utc)
    months = []
    for y in range(start_year, now.year + 1):
        for m in range(1, 13):
            if y == now.year and m > now.month:
                break
            months.append((y, m))
    return months


async def main():
    await init_db()
    await init_ocean_color_table()

    now_iso = datetime.now(timezone.utc).isoformat()

    # Determine which months we already have in the DB
    async with aiosqlite.connect(DB_PATH) as db:
        existing_rows = await db.execute_fetchall(
            "SELECT DISTINCT substr(observed_at, 1, 7) FROM ocean_color"
        )
    existing_months = set(r[0] for r in existing_rows) if existing_rows else set()

    # Build list of months to fetch
    all_months = _generate_month_list(START_YEAR)
    months_to_fetch = [
        (y, m) for y, m in all_months
        if f"{y:04d}-{m:02d}" not in existing_months
    ]

    if not months_to_fetch:
        logger.info("All ocean color months already fetched (up to current month)")
        return

    logger.info("Ocean color: %d months to fetch via CoastWatch ERDDAP", len(months_to_fetch))

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for i, (year, month) in enumerate(months_to_fetch):
            rows = await fetch_chlor_month(session, year, month)
            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO ocean_color
                           (observed_at, cell_lat, cell_lon, chlor_a_mg_m3, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [
                            (r["date"], r["lat"], r["lon"], r["chlor_a"], now_iso)
                            for r in rows
                        ],
                    )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 10 == 0:
                logger.info(
                    "Ocean color: %d/%d months, %d records",
                    i + 1, len(months_to_fetch), total_records,
                )
            # Be polite to ERDDAP server
            await asyncio.sleep(1.0)

    logger.info("Ocean color fetch complete: %d records from %d months", total_records, len(months_to_fetch))


if __name__ == "__main__":
    asyncio.run(main())
