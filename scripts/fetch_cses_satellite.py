"""Fetch satellite electromagnetic and continuous hourly geomagnetic data.

Downloads two complementary datasets for earthquake precursor analysis:

1. INTERMAGNET hourly geomagnetic data (primary, no auth required)
   - Continuous hourly-resolution H/D/Z/F for KAK, MMB, KNY (2011-2026)
   - Unlike fetch_kakioka_ulf.py (±7 days around M6+), this fetches ALL days
   - Enables spectral analysis (Fourier, wavelet) at 0.01-0.1 Hz ULF band
   - Source: BGS GIN REST API (SamplesPerDay=24)

2. CSES-01 (Zhangheng-1) satellite data (best effort, may require auth)
   - Launched Feb 2018 by CNSA/INFN, sun-synchronous orbit ~507 km
   - EFD (Electric Field Detector): DC-3.5 MHz
   - SCM (Search Coil Magnetometer): 10 Hz-20 kHz
   - PAP (Plasma Analyzer Package): electron density, ion temperature
   - Source: CSES-Limadou Italian portal (limadou.ssdc.asi.it)

Physical basis:
    CSES-01 and its predecessor DEMETER (2004-2010) detected ionospheric
    electromagnetic anomalies before major earthquakes. DEMETER found ELF
    (1-2 kHz) intensity decreases hours before shocks. Ground-based
    INTERMAGNET geomagnetic data complements satellite observations by
    providing continuous local ULF monitoring. Continuous hourly data
    enables spectral analysis to detect pre-seismic ULF power changes.

    Kyoto University (2024) proposed that water in clay minerals reaching
    supercritical state under tectonic stress explains pre-seismic
    ionospheric anomalies via electrokinetic coupling.

References:
    - Parrot (2011) Nat. Hazards Earth Syst. Sci. (DEMETER results)
    - Zhima et al. (2020) Space Weather (CSES results)
    - Hattori (2004) Nat. Hazards Earth Syst. Sci. (ULF precursors)
    - Kyoto University (2024) - Water in clay minerals supercritical state
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

STATIONS = {
    "KAK": {"lat": 36.23, "lon": 140.19, "name": "Kakioka"},
    "MMB": {"lat": 43.91, "lon": 144.19, "name": "Memambetsu"},
    "KNY": {"lat": 31.42, "lon": 130.88, "name": "Kanoya"},
}

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)

# INTERMAGNET via BGS Edinburgh GIN
INTERMAGNET_API = "https://imag-data.bgs.ac.uk/GIN_V1/GINServices"

# CSES-Limadou (Italian portal)
CSES_LIMADOU_BASE = "https://limadou.ssdc.asi.it"

# Rate limiting (seconds)
INTERMAGNET_DELAY = 0.5
CSES_DELAY = 2.0

# Date range for continuous hourly fetch
HOURLY_START_YEAR = 2011
HOURLY_END_YEAR = 2026

# Max days per station per run — increased from 500 to 2000 for faster backfill.
# 3 stations × 2000 days ÷ 7-day batches ≈ 860 requests at 2s delay ≈ 30 min.
# Well within GitHub Actions 6h limit.
MAX_DAYS_PER_STATION = 2000


async def init_satellite_tables():
    """Create satellite_em and geomag_hourly tables."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS satellite_em (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                elf_power_db REAL,
                vlf_power_db REAL,
                electron_density REAL,
                ion_temperature REAL,
                received_at TEXT NOT NULL,
                UNIQUE(source, observed_at, latitude, longitude)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sat_em_time
            ON satellite_em(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sat_em_source
            ON satellite_em(source)
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS geomag_hourly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                h_nt REAL,
                d_nt REAL,
                z_nt REAL,
                f_nt REAL,
                received_at TEXT NOT NULL,
                UNIQUE(station, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_geomag_hourly_time
            ON geomag_hourly(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_geomag_hourly_station
            ON geomag_hourly(station)
        """)
        await db.commit()


def parse_iaga2002_minute(text: str, station: str) -> list[tuple]:
    """Parse IAGA-2002 format minute magnetic data and downsample to hourly.

    BGS GIN returns 1440 rows/day (1-minute cadence). We parse all minute rows,
    then average every 60 values to produce 24 hourly rows per day.

    Returns list of (station, observed_at, H, D, Z, F) tuples at hourly cadence.
    Missing values (99999/88888) are excluded from averaging.
    """
    # First pass: collect all minute-level rows
    minute_rows = []
    in_data = False
    for line in text.split("\n"):
        if line.startswith("DATE"):
            in_data = True
            continue
        if not in_data or not line.strip():
            continue
        parts = line.split()
        if len(parts) < 7:
            continue
        try:
            date_str = parts[0]
            time_str = parts[1]

            h = float(parts[3])
            d = float(parts[4])
            z = float(parts[5])
            f_val = float(parts[6])

            # 99999 or 88888 = missing → None
            h = None if abs(h) > 90000 else h
            d = None if abs(d) > 90000 else d
            z = None if abs(z) > 90000 else z
            f_val = None if abs(f_val) > 90000 else f_val

            # Extract hour for grouping
            hour = int(time_str[:2])
            minute_rows.append((date_str, hour, h, d, z, f_val))
        except (ValueError, IndexError):
            continue

    if not minute_rows:
        return []

    # Second pass: average per hour
    from collections import defaultdict
    hourly_buckets = defaultdict(lambda: {"h": [], "d": [], "z": [], "f": []})
    for date_str, hour, h, d, z, f_val in minute_rows:
        key = (date_str, hour)
        if h is not None:
            hourly_buckets[key]["h"].append(h)
        if d is not None:
            hourly_buckets[key]["d"].append(d)
        if z is not None:
            hourly_buckets[key]["z"].append(z)
        if f_val is not None:
            hourly_buckets[key]["f"].append(f_val)

    rows = []
    for (date_str, hour), vals in sorted(hourly_buckets.items()):
        observed_at = f"{date_str}T{hour:02d}:00:00"
        avg_h = sum(vals["h"]) / len(vals["h"]) if vals["h"] else None
        avg_d = sum(vals["d"]) / len(vals["d"]) if vals["d"] else None
        avg_z = sum(vals["z"]) / len(vals["z"]) if vals["z"] else None
        avg_f = sum(vals["f"]) / len(vals["f"]) if vals["f"] else None
        rows.append((station, observed_at, avg_h, avg_d, avg_z, avg_f))

    return rows


async def fetch_intermagnet_hourly_day(session: aiohttp.ClientSession,
                                        station: str, date: datetime,
                                        duration_days: int = 7) -> list[tuple]:
    """Fetch minute data from INTERMAGNET BGS GIN API & downsample to hourly.

    BGS GIN API only supports samplesPerDay=1440 (minute) or 86400 (second).
    There is no hourly (24) option — requesting SamplesPerDay=24 returns HTTP 400.
    We fetch minute data and average every 60 rows to get hourly resolution.

    Fetches `duration_days` at once (default 7) to reduce request count.
    API max is 366 days for minute data.

    API endpoint: GINServices?Request=GetData&format=iaga2002
    Date format: yyyy-mm-dd only (no time/timezone suffix).
    publicationState: best-avail (not adj-or-rep which is invalid).
    """
    start = date.strftime("%Y-%m-%d")

    url = (f"{INTERMAGNET_API}?Request=GetData&observatoryIagaCode={station}"
           f"&samplesPerDay=1440&dataStartDate={start}&dataDuration={duration_days}"
           f"&publicationState=best-avail&format=iaga2002")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    if "DATE" in text:
                        rows = parse_iaga2002_minute(text, station)
                        if rows:
                            return rows
                elif resp.status == 204:
                    return []  # No data available
                elif resp.status == 404:
                    return []  # Station/date not available
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning(
                            "INTERMAGNET hourly %s %s: HTTP %d after %d attempts",
                            station, date.strftime("%Y-%m-%d"), resp.status, attempt,
                        )
                        return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning(
                    "INTERMAGNET hourly %s %s: %s after %d attempts",
                    station, date.strftime("%Y-%m-%d"), type(e).__name__, attempt,
                )
                return []
            await asyncio.sleep(2 ** attempt)

    return []


async def fetch_cses_data(session: aiohttp.ClientSession,
                           start_date: datetime,
                           end_date: datetime) -> list[tuple]:
    """Attempt to fetch CSES-Limadou satellite data.

    CSES data access typically requires registration at limadou.ssdc.asi.it.
    This function attempts known API patterns; if authentication is required,
    it logs clearly and returns empty.

    Returns list of (source, observed_at, lat, lon, elf_db, vlf_db,
                     electron_density, ion_temp) tuples.
    """
    # Try known CSES API endpoints
    # The Limadou portal may expose data catalogs or summary products
    api_endpoints = [
        # Catalog/metadata endpoint (common pattern for ASI/SSDC services)
        (f"{CSES_LIMADOU_BASE}/api/v1/data/catalog"
         f"?start={start_date.strftime('%Y-%m-%dT00:00:00Z')}"
         f"&end={end_date.strftime('%Y-%m-%dT00:00:00Z')}"
         f"&product=EFD_L2"),
        # Alternative: REST data access
        (f"{CSES_LIMADOU_BASE}/data/products"
         f"?from={start_date.strftime('%Y-%m-%d')}"
         f"&to={end_date.strftime('%Y-%m-%d')}"
         f"&type=SCM"),
    ]

    for url in api_endpoints:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        content_type = resp.headers.get("Content-Type", "")
                        if "json" in content_type:
                            data = await resp.json()
                            rows = _parse_cses_json(data)
                            if rows:
                                logger.info("CSES: retrieved %d records", len(rows))
                                return rows
                        elif "html" in content_type:
                            # Likely a login page redirect
                            logger.info(
                                "CSES endpoint returned HTML (likely auth required): %s",
                                url.split("?")[0],
                            )
                            break
                    elif resp.status == 401 or resp.status == 403:
                        logger.info(
                            "CSES access denied (registration required): HTTP %d",
                            resp.status,
                        )
                        return []
                    elif resp.status == 404:
                        break  # Try next endpoint
                    else:
                        if attempt == MAX_RETRIES:
                            break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == MAX_RETRIES:
                    logger.warning("CSES fetch failed: %s", type(e).__name__)
                    break
                await asyncio.sleep(2 ** attempt)

    logger.info(
        "CSES data not available via public API. "
        "Registration at %s may be required for direct data access.",
        CSES_LIMADOU_BASE,
    )
    return []


def _parse_cses_json(data: dict | list) -> list[tuple]:
    """Parse CSES JSON response into satellite_em rows.

    Expected structure varies by endpoint. This handles common patterns
    from ASI/SSDC-style data services.
    """
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    # Handle list of records
    records = data if isinstance(data, list) else data.get("data", data.get("records", []))
    if not isinstance(records, list):
        return []

    for rec in records:
        try:
            observed_at = rec.get("time") or rec.get("datetime") or rec.get("timestamp")
            if not observed_at:
                continue

            lat = rec.get("latitude") or rec.get("lat")
            lon = rec.get("longitude") or rec.get("lon")

            # EM field data (various possible key names)
            elf_power = rec.get("elf_power_db") or rec.get("elf_power") or rec.get("efd_elf")
            vlf_power = rec.get("vlf_power_db") or rec.get("vlf_power") or rec.get("scm_vlf")
            electron_density = (rec.get("electron_density") or rec.get("ne")
                                or rec.get("plasma_density"))
            ion_temp = (rec.get("ion_temperature") or rec.get("ti")
                        or rec.get("ion_temp"))

            # Convert to float, allowing None
            lat = float(lat) if lat is not None else None
            lon = float(lon) if lon is not None else None
            elf_power = float(elf_power) if elf_power is not None else None
            vlf_power = float(vlf_power) if vlf_power is not None else None
            electron_density = float(electron_density) if electron_density is not None else None
            ion_temp = float(ion_temp) if ion_temp is not None else None

            rows.append((
                "CSES", str(observed_at), lat, lon,
                elf_power, vlf_power, electron_density, ion_temp,
            ))
        except (ValueError, TypeError, AttributeError):
            continue

    return rows


def _generate_all_dates(start_year: int, end_year: int) -> list[datetime]:
    """Generate all dates from start_year-01-01 to end_year-12-31 or today."""
    today = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    # Don't go beyond yesterday (today's data may not be available yet)
    end = min(
        datetime(end_year, 12, 31, tzinfo=timezone.utc),
        today - timedelta(days=1),
    )

    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


async def fetch_intermagnet_hourly(session: aiohttp.ClientSession, now: str):
    """Fetch continuous hourly INTERMAGNET data for all stations, 2011-2026.

    Processes in monthly batches with progress logging. Skips dates that
    already exist in geomag_hourly table.
    """
    all_dates = _generate_all_dates(HOURLY_START_YEAR, HOURLY_END_YEAR)
    logger.info(
        "INTERMAGNET hourly: %d total dates in range %d-%d",
        len(all_dates), HOURLY_START_YEAR, HOURLY_END_YEAR,
    )

    # Get existing date-station pairs to skip
    async with aiosqlite.connect(DB_PATH) as db:
        existing_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(observed_at), station FROM geomag_hourly"
        )
    existing_set = set((r[0], r[1]) for r in existing_rows)
    logger.info("INTERMAGNET hourly: %d existing date-station pairs", len(existing_set))

    total_records = 0
    total_fetched = 0
    total_skipped = 0
    total_failed = 0

    for station in ["KAK", "MMB", "KNY"]:
        dates_to_fetch = sorted(
            d for d in all_dates
            if (d.strftime("%Y-%m-%d"), station) not in existing_set
        )

        if not dates_to_fetch:
            logger.info("%s hourly: all dates already fetched", station)
            continue

        # Cap per-station per-run to avoid extremely long runs
        if len(dates_to_fetch) > MAX_DAYS_PER_STATION:
            logger.info(
                "%s hourly: %d dates pending, capping at %d for this run",
                station, len(dates_to_fetch), MAX_DAYS_PER_STATION,
            )
            dates_to_fetch = dates_to_fetch[:MAX_DAYS_PER_STATION]

        logger.info(
            "%s hourly: %d dates to fetch (total range: %s to %s)",
            station, len(dates_to_fetch),
            dates_to_fetch[0].strftime("%Y-%m-%d"),
            dates_to_fetch[-1].strftime("%Y-%m-%d"),
        )

        station_records = 0
        station_fetched = 0
        station_failed = 0
        current_month = None

        # Batch dates into 7-day chunks to reduce API requests (~1/7 the calls)
        BATCH_SIZE = 7
        i = 0
        while i < len(dates_to_fetch):
            batch_start = dates_to_fetch[i]
            # Find contiguous dates within this batch window
            batch_end_idx = i
            while (batch_end_idx < min(i + BATCH_SIZE, len(dates_to_fetch))
                   and (dates_to_fetch[batch_end_idx] - batch_start).days < BATCH_SIZE):
                batch_end_idx += 1
            batch_count = batch_end_idx - i
            actual_duration = (dates_to_fetch[batch_end_idx - 1] - batch_start).days + 1

            # Monthly progress logging
            month_key = batch_start.strftime("%Y-%m")
            if month_key != current_month:
                if current_month is not None:
                    logger.info(
                        "  %s month %s complete: %d records so far",
                        station, current_month, station_records,
                    )
                current_month = month_key

            rows = await fetch_intermagnet_hourly_day(
                session, station, batch_start, duration_days=actual_duration)
            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO geomag_hourly
                           (station, observed_at, h_nt, d_nt, z_nt, f_nt, received_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        [(s, t, h, d, z, f, now) for s, t, h, d, z, f in rows],
                    )
                    await db.commit()
                station_records += len(rows)
                station_fetched += batch_count
            else:
                station_failed += batch_count

            # Progress log every ~50 dates
            if station_fetched % 50 < BATCH_SIZE:
                logger.info(
                    "  %s: ~%d/%d dates processed, %d records, %d failed",
                    station, station_fetched + station_failed, len(dates_to_fetch),
                    station_records, station_failed,
                )

            i = batch_end_idx
            await asyncio.sleep(INTERMAGNET_DELAY)

        total_records += station_records
        total_fetched += station_fetched
        total_failed += station_failed
        logger.info(
            "%s hourly complete: %d records from %d days (%d failed)",
            station, station_records, station_fetched, station_failed,
        )

    logger.info(
        "INTERMAGNET hourly total: %d records, %d days fetched, %d failed",
        total_records, total_fetched, total_failed,
    )
    return total_records


async def fetch_cses_satellite(session: aiohttp.ClientSession, now: str):
    """Fetch CSES satellite electromagnetic data (best effort).

    CSES-01 (Zhangheng-1) operates since Feb 2018. Data access may
    require registration at limadou.ssdc.asi.it. This function attempts
    public API access and gracefully handles auth requirements.
    """
    # CSES data starts from Feb 2018
    cses_start = datetime(2018, 2, 1, tzinfo=timezone.utc)
    today = datetime.now(timezone.utc)

    # Check what we already have
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at) FROM satellite_em WHERE source = 'CSES'"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None

    if last_date:
        start_date = datetime.fromisoformat(last_date.replace("Z", "+00:00"))
        logger.info("CSES: resuming from %s", last_date)
    else:
        start_date = cses_start
        logger.info("CSES: starting from %s", start_date.strftime("%Y-%m-%d"))

    # Try fetching in monthly batches
    total_records = 0
    current = start_date
    consecutive_failures = 0
    max_consecutive_failures = 3

    while current < today and consecutive_failures < max_consecutive_failures:
        batch_end = min(current + timedelta(days=30), today)

        rows = await fetch_cses_data(session, current, batch_end)
        if rows:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO satellite_em
                       (source, observed_at, latitude, longitude,
                        elf_power_db, vlf_power_db, electron_density,
                        ion_temperature, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(*row, now) for row in rows],
                )
                await db.commit()
            total_records += len(rows)
            consecutive_failures = 0
        else:
            consecutive_failures += 1

        current = batch_end
        await asyncio.sleep(CSES_DELAY)

    if consecutive_failures >= max_consecutive_failures:
        logger.info(
            "CSES: stopped after %d consecutive failures "
            "(registration likely required at %s)",
            max_consecutive_failures, CSES_LIMADOU_BASE,
        )

    logger.info("CSES satellite total: %d records", total_records)
    return total_records


async def main():
    await init_db()
    await init_satellite_tables()

    now = datetime.now(timezone.utc).isoformat()

    async with aiohttp.ClientSession() as session:
        # 1. Primary: INTERMAGNET continuous hourly data (guaranteed to work)
        logger.info("=" * 60)
        logger.info("Phase 1: INTERMAGNET continuous hourly geomagnetic data")
        logger.info("=" * 60)
        hourly_records = await fetch_intermagnet_hourly(session, now)

        # 2. Secondary: CSES satellite data (best effort)
        logger.info("=" * 60)
        logger.info("Phase 2: CSES satellite electromagnetic data (best effort)")
        logger.info("=" * 60)
        cses_records = await fetch_cses_satellite(session, now)

    # Summary
    logger.info("=" * 60)
    logger.info("Fetch complete:")
    logger.info("  INTERMAGNET hourly: %d records", hourly_records)
    logger.info("  CSES satellite: %d records", cses_records)
    logger.info("=" * 60)

    # Report table sizes
    async with aiosqlite.connect(DB_PATH) as db:
        for table in ["geomag_hourly", "satellite_em"]:
            row = await db.execute_fetchall(f"SELECT COUNT(*) FROM {table}")
            logger.info("  Table %s: %d total rows", table, row[0][0])

        # Date range coverage for geomag_hourly
        coverage = await db.execute_fetchall("""
            SELECT station, MIN(observed_at), MAX(observed_at), COUNT(*)
            FROM geomag_hourly GROUP BY station
        """)
        for station, min_dt, max_dt, count in coverage:
            logger.info(
                "  %s hourly coverage: %s to %s (%d records)",
                station, min_dt[:10] if min_dt else "N/A",
                max_dt[:10] if max_dt else "N/A", count,
            )


if __name__ == "__main__":
    asyncio.run(main())
