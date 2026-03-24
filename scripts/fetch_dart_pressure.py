"""Fetch NOAA DART ocean bottom pressure data for stations near Japan.

Deep-ocean Assessment and Reporting of Tsunamis (DART) buoys measure
ocean bottom pressure (OBP) via seafloor-mounted pressure sensors.
The water column height derived from OBP records seafloor vertical
displacement and ocean loading changes with sub-mm precision.

Physical mechanism:
    Pre-seismic slow slip on subduction faults causes measurable
    pressure changes at nearby DART stations. The seafloor deformation
    from aseismic slip alters the water column height above the sensor.
    DART buoys near Japan sit on or near the Pacific Plate boundary,
    making them sensitive to pre-seismic deformation of the Japan Trench.
    Additionally, pre-seismic fluid migration and crustal dilation can
    produce detectable OBP anomalies days to weeks before rupture.

Target features:
    - dart_pressure_anomaly: daily mean water height deviation from
      30-day running mean per station (σ)
    - dart_pressure_range: daily max-min water height (m), sensitive
      to transient deformation events

Data source: NOAA National Data Buoy Center (NDBC)
    - Realtime (last 45 days): https://www.ndbc.noaa.gov/data/realtime2/{ID}.dart
    - Historical (annual text, gzipped):
      https://www.ndbc.noaa.gov/view_text_file.php?filename={ID}h{YEAR}.txt.gz&dir=data/historical/dart/
    - DART text format columns: #YY MM DD hh mm T TYPE HEIGHT
      TYPE: 1=15-min, 2=1-min, 3=15-sec
      HEIGHT: water column height in meters

Stations near Japan (Pacific Plate boundary):
    21413 (30.53°N, 152.13°E) - Izu-Bonin Trench
    21418 (38.73°N, 148.65°E) - Japan Trench (offshore Tohoku)
    21419 (44.44°N, 155.72°E) - Kuril-Kamchatka Trench
    21416 (48.12°N, 163.33°E) - Kuril-Kamchatka Trench (north)
    52404 (20.63°N, 132.14°E) - Philippine Sea / Ryukyu Trench

References:
    - Baba et al. (2020) Science 367:6478
    - Hino et al. (2014) Earth Planet. Sci. Lett. 396:248-259
"""

import asyncio
import gzip
import io
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NOAA NDBC DART endpoints
REALTIME_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station_id}.dart"
HISTORICAL_URL = (
    "https://www.ndbc.noaa.gov/view_text_file.php"
    "?filename={station_id}h{year}.txt.gz&dir=data/historical/dart/"
)

# DART stations near Japan on/near Pacific Plate boundary
DART_STATIONS = {
    "21413": {"name": "Izu-Bonin Trench", "lat": 30.53, "lon": 152.13},
    "21418": {"name": "Japan Trench (Tohoku)", "lat": 38.73, "lon": 148.65},
    "21419": {"name": "Kuril-Kamchatka Trench", "lat": 44.44, "lon": 155.72},
    "21416": {"name": "Kuril-Kamchatka Trench N", "lat": 48.12, "lon": 163.33},
    "52404": {"name": "Philippine Sea / Ryukyu", "lat": 20.63, "lon": 132.14},
}

# Fetch years 2011 to present
HISTORICAL_START_YEAR = 2011

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)


async def init_dart_table():
    """Create DART pressure table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS dart_pressure (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                water_height_m REAL NOT NULL,
                measurement_type INTEGER,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(station_id, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dart_time
            ON dart_pressure(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dart_station
            ON dart_pressure(station_id)
        """)
        await db.commit()


def parse_dart_text(text: str) -> list[dict]:
    """Parse DART text format (realtime or historical).

    DART text columns:
        #YY  MM DD hh mm   T   TYPE   HEIGHT
    Where:
        T = data category (not used for filtering here)
        TYPE: 1=15-min, 2=1-min, 3=15-sec
        HEIGHT: water column height in meters

    Missing/bad values are typically 9999.000 or absent.
    """
    rows = []
    for line in text.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split()
        if len(parts) < 8:
            continue

        try:
            yy = int(parts[0])
            mm = int(parts[1])
            dd = int(parts[2])
            hh = int(parts[3])
            mi = int(parts[4])
            # parts[5] = T (data category)
            mtype = int(parts[6])
            height = float(parts[7])

            # Convert 2-digit year to 4-digit
            year = yy if yy >= 100 else (2000 + yy if yy < 80 else 1900 + yy)

            # Skip missing/sentinel values
            if height > 9000 or height < 0:
                continue

            observed_at = f"{year:04d}-{mm:02d}-{dd:02d}T{hh:02d}:{mi:02d}:00"

            rows.append({
                "observed_at": observed_at,
                "water_height_m": height,
                "measurement_type": mtype,
            })
        except (ValueError, IndexError):
            continue

    return rows


async def fetch_with_retry(session: aiohttp.ClientSession, url: str,
                           label: str, decompress_gzip: bool = False) -> str | None:
    """Fetch URL content with retries and optional gzip decompression."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    if decompress_gzip:
                        raw = await resp.read()
                        try:
                            decompressed = gzip.decompress(raw)
                            return decompressed.decode("utf-8", errors="replace")
                        except (gzip.BadGzipFile, OSError):
                            # Server may return uncompressed despite .gz extension
                            return raw.decode("utf-8", errors="replace")
                    else:
                        return await resp.text()
                elif resp.status == 404:
                    logger.debug("%s: not available (404)", label)
                    return None
                else:
                    if attempt == MAX_RETRIES:
                        logger.warning("%s: HTTP %d after %d attempts",
                                       label, resp.status, MAX_RETRIES)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("%s: %s after %d attempts",
                               label, type(e).__name__, MAX_RETRIES)
            await asyncio.sleep(2 ** attempt)

    return None


async def fetch_station_historical(session: aiohttp.ClientSession,
                                   station_id: str,
                                   existing_max_at: str | None) -> list[dict]:
    """Fetch historical annual DART files for one station (2011 to last year)."""
    current_year = datetime.now(timezone.utc).year
    all_rows = []

    for year in range(HISTORICAL_START_YEAR, current_year):
        # Skip years we already have complete data for
        if existing_max_at and existing_max_at >= f"{year + 1}-01-01":
            logger.debug("  %s/%d: already have data, skipping", station_id, year)
            continue

        url = HISTORICAL_URL.format(station_id=station_id, year=year)
        label = f"DART {station_id}/{year}"
        text = await fetch_with_retry(session, url, label, decompress_gzip=True)

        if text:
            rows = parse_dart_text(text)
            if rows:
                logger.info("  %s/%d: %d records", station_id, year, len(rows))
                all_rows.extend(rows)
            else:
                logger.debug("  %s/%d: no parseable records", station_id, year)
        else:
            logger.debug("  %s/%d: no data available", station_id, year)

        # Be polite to NOAA servers
        await asyncio.sleep(1.0)

    return all_rows


async def fetch_station_realtime(session: aiohttp.ClientSession,
                                 station_id: str) -> list[dict]:
    """Fetch realtime DART data (last 45 days) for one station."""
    url = REALTIME_URL.format(station_id=station_id)
    label = f"DART {station_id}/realtime"
    text = await fetch_with_retry(session, url, label)

    if not text:
        return []

    rows = parse_dart_text(text)
    logger.info("  %s realtime: %d records", station_id, len(rows))
    return rows


async def store_records(station_id: str, info: dict,
                        rows: list[dict], now: str) -> int:
    """Store parsed records into SQLite, returning count of new inserts."""
    if not rows:
        return 0

    async with safe_connect() as db:
        await db.executemany(
            """INSERT OR IGNORE INTO dart_pressure
               (station_id, observed_at, water_height_m, measurement_type,
                latitude, longitude, received_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [(station_id, r["observed_at"], r["water_height_m"],
              r["measurement_type"], info["lat"], info["lon"], now)
             for r in rows],
        )
        changes = db.total_changes
        await db.commit()

    return changes


async def log_daily_stats():
    """Output aggregated daily statistics per station."""
    async with safe_connect() as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT
                station_id,
                DATE(observed_at) AS obs_date,
                AVG(water_height_m) AS mean_height,
                MIN(water_height_m) AS min_height,
                MAX(water_height_m) AS max_height,
                COUNT(*) AS n_obs
            FROM dart_pressure
            GROUP BY station_id, DATE(observed_at)
            ORDER BY station_id, obs_date DESC
            LIMIT 50
        """)
        rows = await cursor.fetchall()

    if not rows:
        logger.info("No daily stats available yet")
        return

    logger.info("=== Recent daily stats (last entries per station) ===")
    logger.info("%-8s  %-12s  %10s  %10s  %10s  %5s",
                "Station", "Date", "Mean(m)", "Min(m)", "Max(m)", "N")
    for r in rows:
        logger.info("%-8s  %-12s  %10.3f  %10.3f  %10.3f  %5d",
                     r["station_id"], r["obs_date"],
                     r["mean_height"], r["min_height"],
                     r["max_height"], r["n_obs"])


async def main():
    await init_db()
    await init_dart_table()

    now = datetime.now(timezone.utc).isoformat()

    # Check existing data per station
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT station_id, COUNT(*), MAX(observed_at) "
            "FROM dart_pressure GROUP BY station_id"
        )
    existing_summary = {r[0]: (r[1], r[2]) for r in existing} if existing else {}
    logger.info("DART existing: %d stations, %s",
                len(existing_summary),
                {k: v[0] for k, v in existing_summary.items()} if existing_summary else "{}")

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for station_id, info in DART_STATIONS.items():
            logger.info("Fetching DART %s (%s, %.2f°N %.2f°E)...",
                        station_id, info["name"], info["lat"], info["lon"])

            existing_max_at = existing_summary.get(station_id, (0, None))[1]

            # Fetch historical annual files
            hist_rows = await fetch_station_historical(
                session, station_id, existing_max_at
            )

            # Fetch realtime (last 45 days, overlaps with current year)
            rt_rows = await fetch_station_realtime(session, station_id)

            # Merge: realtime overwrites historical for overlapping timestamps
            all_rows = hist_rows + rt_rows

            # Filter to 2011+ only
            all_rows = [r for r in all_rows if r["observed_at"] >= "2011"]

            if not all_rows:
                logger.info("  %s: no data from 2011+", station_id)
                continue

            await store_records(station_id, info, all_rows, now)
            total_records += len(all_rows)
            logger.info("  %s total: %d records ingested", station_id, len(all_rows))

    # Final summary
    async with safe_connect() as db:
        row = await db.execute_fetchall(
            "SELECT COUNT(*), COUNT(DISTINCT station_id), "
            "MIN(observed_at), MAX(observed_at) FROM dart_pressure"
        )
    if row and row[0][0]:
        logger.info("DART pressure total: %d records, %d stations, range %s to %s",
                     row[0][0], row[0][1], row[0][2], row[0][3])

    # Output daily aggregated stats
    await log_daily_stats()

    logger.info("DART pressure fetch complete: %d records ingested this run", total_records)


if __name__ == "__main__":
    asyncio.run(main())
