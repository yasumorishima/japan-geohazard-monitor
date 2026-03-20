"""Fetch soil moisture data from NOAA ERDDAP (public, no auth required).

Soil moisture is a potential earthquake precursor signal: crustal strain
changes pore pressure in shallow rock and soil, altering soil moisture
content near active faults. This is the same mechanism as groundwater
level changes (a classic earthquake precursor), but measurable from
space without ground stations.

Data sources (both via ERDDAP griddap, CSV output, no authentication):

1. NOAA SMOPS CDR (primary) — daily, 0.25°, 2017-present
   Dataset: noaacwSMcdrDaily on coastwatch.noaa.gov/erddap
   NOAA Soil Moisture Products System blends AMSR2/SMOS/ASCAT/SMAP
   into a single daily global product at 25 km resolution.

2. NOAA CPC Soil Moisture v2 (backfill) — monthly, 0.5°, 1948-present
   Dataset: noaa_psl_bec3_56fa_c395 on upwell.pfeg.noaa.gov/erddap
   Model-based soil moisture using observed precipitation and temperature.
   Used for 2011-2016 before SMOPS CDR coverage begins.

Target features:
    - soil_moisture_anomaly: deviation from 30-day baseline (σ)

References:
    - Nissen et al. (2014) Geophys. Res. Lett. 41:6621-6628
    - Wegnüller et al. (2020) Remote Sensing 12:2895
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

# ── ERDDAP endpoints (public, no auth) ──────────────────────────────
# Primary: NOAA CPC Soil Moisture v2 — monthly model reanalysis (1948-present)
# SMOPS CDR ended at 2022-12, so CPC is now the primary source.
CPC_ERDDAP_BASE = "https://upwell.pfeg.noaa.gov/erddap/griddap"
CPC_DATASET_ID = "noaa_psl_bec3_56fa_c395"

# Fallback: NOAA SMOPS CDR — daily blended satellite (2017-2022 only)
SMOPS_ERDDAP_BASE = "https://coastwatch.noaa.gov/erddap/griddap"
SMOPS_DATASET_ID = "noaacwSMcdrDaily"

# Japan bbox
LAT_MIN = 24.0
LAT_MAX = 46.0
LON_MIN = 122.0
LON_MAX = 150.0

# Aggregate to 2° cells for compatibility with prediction grid
CELL_DEG = 2.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)

# SMOPS CDR starts ~2017-03; CPC covers 1948-present (monthly)
SMOPS_START_YEAR = 2017
BACKFILL_START_YEAR = 2011


async def init_soil_moisture_table():
    """Create soil moisture table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS soil_moisture (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                sm_m3m3 REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sm_time
            ON soil_moisture(observed_at)
        """)
        await db.commit()


# ── ERDDAP CSV parsing (shared) ─────────────────────────────────────

def _parse_erddap_csv(text: str, value_col: str,
                       min_val: float = 0.0,
                       max_val: float = 1.0,
                       scale: float = 1.0) -> list[dict]:
    """Parse ERDDAP CSV response into row dicts with 2° cell aggregation.

    ERDDAP CSV has two header rows:
        Row 1: column names (time, latitude, longitude, <variable>)
        Row 2: units row
    Followed by data rows.

    Args:
        text: raw CSV text from ERDDAP
        value_col: column name for the soil moisture variable
        min_val: minimum valid value (after scaling)
        max_val: maximum valid value (after scaling)
        scale: multiply raw value by this to convert to m³/m³
    """
    cell_values: dict[tuple[str, float, float], list[float]] = defaultdict(list)

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)  # column names
        _ = next(reader)       # units row
    except StopIteration:
        logger.warning("ERDDAP CSV response has no header rows")
        return []

    col_map = {name.strip().lower(): i for i, name in enumerate(header)}
    time_idx = col_map.get("time")
    lat_idx = col_map.get("latitude")
    lon_idx = col_map.get("longitude")
    val_idx = col_map.get(value_col.lower())

    if any(idx is None for idx in (time_idx, lat_idx, lon_idx, val_idx)):
        logger.warning("ERDDAP CSV missing expected columns: %s (wanted %s)",
                        header, value_col)
        return []

    for row in reader:
        if len(row) <= max(time_idx, lat_idx, lon_idx, val_idx):
            continue
        try:
            time_str = row[time_idx].strip()
            lat = float(row[lat_idx])
            lon = float(row[lon_idx])
            raw_val = float(row[val_idx])
        except (ValueError, IndexError):
            continue

        sm_val = raw_val * scale

        # Filter invalid / fill / NaN values
        if math.isnan(sm_val) or sm_val < min_val or sm_val > max_val:
            continue

        date_str = time_str[:10]

        # Aggregate to 2° cell
        cell_lat = math.floor(lat / CELL_DEG) * CELL_DEG + CELL_DEG / 2
        cell_lon = math.floor(lon / CELL_DEG) * CELL_DEG + CELL_DEG / 2

        cell_values[(date_str, cell_lat, cell_lon)].append(sm_val)

    rows = []
    for (date_str, clat, clon), vals in cell_values.items():
        rows.append({
            "date": date_str,
            "lat": round(clat, 1),
            "lon": round(clon, 1),
            "sm": round(sum(vals) / len(vals), 4),
        })
    return rows


# ── SMOPS CDR daily fetch (2017-present) ─────────────────────────────

def _build_smops_url(date_iso: str) -> str:
    """Build ERDDAP griddap CSV URL for a single day of SMOPS data.

    The SMOPS CDR variable name is 'sm' (soil moisture, m³/m³).
    """
    return (
        f"{SMOPS_ERDDAP_BASE}/{SMOPS_DATASET_ID}.csv"
        f"?sm[({date_iso}T00:00:00Z)]"
        f"[({LAT_MIN}):({LAT_MAX})]"
        f"[({LON_MIN}):({LON_MAX})]"
    )


async def fetch_smops_day(
    session: aiohttp.ClientSession, date_str: str
) -> list[dict]:
    """Fetch one day of SMOPS soil moisture from CoastWatch ERDDAP."""
    url = _build_smops_url(date_str)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    if not text or "<html" in text[:200].lower():
                        logger.debug("SMOPS ERDDAP returned HTML for %s", date_str)
                        return []
                    return _parse_erddap_csv(text, value_col="sm",
                                              min_val=0.0, max_val=1.0)
                elif resp.status == 404:
                    logger.debug("No SMOPS data for %s (404)", date_str)
                    return []
                else:
                    body = await resp.text()
                    logger.debug("SMOPS ERDDAP HTTP %d for %s: %s",
                                  resp.status, date_str, body[:200])
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("SMOPS %s failed after %d retries: %s",
                              date_str, MAX_RETRIES, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _generate_date_list(start_year: int, start_month: int = 3,
                         start_day: int = 1) -> list[str]:
    """Generate YYYY-MM-DD strings at weekly intervals from start to now.

    We fetch one sample per week (Mondays) to keep requests manageable.
    Daily data can fill gaps if needed later.
    """
    from datetime import date, timedelta

    now = date.today()
    d = date(start_year, start_month, start_day)
    dates = []
    while d <= now:
        dates.append(d.isoformat())
        d += timedelta(days=7)  # Weekly sampling
    return dates


# ── CPC monthly fetch (2011–2016 backfill) ──────────────────────────

def _build_cpc_url(time_iso: str) -> str:
    """Build ERDDAP griddap CSV URL for CPC monthly soil moisture.

    CPC variable name is 'soilw' (soil moisture, kg/m²).
    For the top layer (~1.6m column), typical range is 0-800 kg/m².
    We convert to approximate volumetric m³/m³ by dividing by 1600
    (column depth ~1.6m × 1000 kg/m³ water density).
    """
    return (
        f"{CPC_ERDDAP_BASE}/{CPC_DATASET_ID}.csv"
        f"?soilw[({time_iso})]"
        f"[({LAT_MIN}):({LAT_MAX})]"
        f"[({LON_MIN}):({LON_MAX})]"
    )


async def fetch_cpc_month(
    session: aiohttp.ClientSession, year: int, month: int
) -> list[dict]:
    """Fetch monthly CPC soil moisture from upwell ERDDAP."""
    time_iso = f"{year:04d}-{month:02d}-01T00:00:00Z"
    url = _build_cpc_url(time_iso)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    if not text or "<html" in text[:200].lower():
                        logger.debug("CPC ERDDAP returned HTML for %04d-%02d",
                                      year, month)
                        return []
                    # CPC soilw is in kg/m², convert to approx m³/m³
                    # Typical range 0-800 kg/m² for ~1.6m column
                    return _parse_erddap_csv(text, value_col="soilw",
                                              min_val=0.0, max_val=1.0,
                                              scale=1.0 / 1600.0)
                elif resp.status == 404:
                    logger.debug("No CPC data for %04d-%02d (404)", year, month)
                    return []
                else:
                    body = await resp.text()
                    logger.debug("CPC ERDDAP HTTP %d for %04d-%02d: %s",
                                  resp.status, year, month, body[:200])
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("CPC %04d-%02d failed after %d retries: %s",
                              year, month, MAX_RETRIES, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _generate_month_list(start_year: int, end_year: int) -> list[tuple[int, int]]:
    """Generate (year, month) tuples from start_year to end_year (inclusive)."""
    months = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            months.append((y, m))
    return months


# ── Main ─────────────────────────────────────────────────────────────

async def store_rows(rows: list[dict], now_iso: str):
    """Insert soil moisture rows into DB."""
    if not rows:
        return 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """INSERT OR IGNORE INTO soil_moisture
               (observed_at, cell_lat, cell_lon, sm_m3m3, received_at)
               VALUES (?, ?, ?, ?, ?)""",
            [(r["date"], r["lat"], r["lon"], r["sm"], now_iso) for r in rows],
        )
        await db.commit()
    return len(rows)


async def main():
    await init_db()
    await init_soil_moisture_table()

    now_iso = datetime.now(timezone.utc).isoformat()

    # Determine which dates/months we already have
    async with aiosqlite.connect(DB_PATH) as db:
        existing_rows = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM soil_moisture"
        )
    existing_dates = set(r[0] for r in existing_rows) if existing_rows else set()

    # Also track existing year-months for CPC monthly data
    existing_months = set()
    for d in existing_dates:
        if len(d) >= 7:
            existing_months.add(d[:7])

    logger.info("Soil moisture: %d existing dates in DB", len(existing_dates))

    total_records = 0

    async with aiohttp.ClientSession() as session:
        # ── Phase 1: CPC monthly backfill (2011–2016) ────────────
        cpc_months = _generate_month_list(BACKFILL_START_YEAR,
                                           SMOPS_START_YEAR - 1)
        cpc_to_fetch = [
            (y, m) for y, m in cpc_months
            if f"{y:04d}-{m:02d}" not in existing_months
        ]

        if cpc_to_fetch:
            logger.info("CPC backfill: %d months to fetch (2011-2016)",
                         len(cpc_to_fetch))
            for i, (year, month) in enumerate(cpc_to_fetch):
                rows = await fetch_cpc_month(session, year, month)
                if rows:
                    stored = await store_rows(rows, now_iso)
                    total_records += stored

                if (i + 1) % 12 == 0:
                    logger.info("CPC backfill: %d/%d months, %d records",
                                 i + 1, len(cpc_to_fetch), total_records)
                # Be polite to ERDDAP server
                await asyncio.sleep(1.0)
        else:
            logger.info("CPC backfill: all months already present")

        # ── Phase 2: SMOPS daily (2017–present, weekly sampling) ──
        smops_dates = _generate_date_list(SMOPS_START_YEAR)
        smops_to_fetch = [d for d in smops_dates if d not in existing_dates]

        if smops_to_fetch:
            logger.info("SMOPS daily: %d dates to fetch (2017-present, weekly)",
                         len(smops_to_fetch))
            for i, date_str in enumerate(smops_to_fetch):
                rows = await fetch_smops_day(session, date_str)
                if rows:
                    stored = await store_rows(rows, now_iso)
                    total_records += stored

                if (i + 1) % 20 == 0:
                    logger.info("SMOPS daily: %d/%d dates, %d records",
                                 i + 1, len(smops_to_fetch), total_records)
                # Be polite to ERDDAP server
                await asyncio.sleep(1.0)
        else:
            logger.info("SMOPS daily: all dates already present")

    logger.info("Soil moisture fetch complete: %d total records stored",
                 total_records)


if __name__ == "__main__":
    asyncio.run(main())
