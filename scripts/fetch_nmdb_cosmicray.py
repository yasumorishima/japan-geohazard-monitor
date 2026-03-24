"""Fetch cosmic ray neutron monitor data from NMDB for earthquake precursors.

Downloads daily corrected count rates from the Neutron Monitor Database (NMDB).
No authentication required.

Physical basis: Homola et al. (2023, J. Atmos. Sol.-Terr. Phys. 247:106068)
found >6σ correlation between cosmic ray intensity variations and global
summed earthquake magnitude (M>=4) with ~15-day time lag (cosmic rays leading).
Mechanism: crustal stress changes alter geomagnetic field → changes cosmic ray
deflection patterns. Use daily rate anomalies (deviation from 27-day solar
rotation mean) as precursor features.

Stations:
    IRKT (Irkutsk, Russia, 52.47°N, 104.03°E) - closest to Japan, 3.64 GV
    OULU (Oulu, Finland, 65.05°N, 25.47°E) - reference, longest record, 0.81 GV
    PSNM (Doi Inthanon, Thailand, 18.59°N, 98.49°E) - equatorial, 16.8 GV

References:
    - Homola et al. (2023) J. Atmos. Sol.-Terr. Phys. 247:106068
    - arXiv: 2204.12310
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STATIONS = {
    "IRKT": {"lat": 52.47, "lon": 104.03, "name": "Irkutsk", "rigidity_gv": 3.64},
    "OULU": {"lat": 65.05, "lon": 25.47, "name": "Oulu", "rigidity_gv": 0.81},
    "PSNM": {"lat": 18.59, "lon": 98.49, "name": "Doi Inthanon", "rigidity_gv": 16.8},
}

NMDB_API = "https://www.nmdb.eu/nest/draw_graph.php"
MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)


async def init_cosmic_ray_table():
    """Create cosmic_ray table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS cosmic_ray (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                counts_per_sec REAL,
                received_at TEXT NOT NULL,
                UNIQUE(station, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cr_time
            ON cosmic_ray(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cr_station
            ON cosmic_ray(station)
        """)
        await db.commit()


def parse_nmdb_ascii(text: str) -> list[tuple]:
    """Parse NMDB ASCII response (semicolon-separated).

    Format:
        start_date_time;MCORR_E
        2024-01-01 12:00:00;100.107
    Returns list of (date_str, counts_per_sec).
    """
    rows = []
    in_data = False
    for line in text.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("start_date_time"):
            in_data = True
            continue
        if not in_data:
            continue
        parts = line.split(";")
        if len(parts) < 2:
            continue
        try:
            dt_str = parts[0].strip()
            val_str = parts[1].strip()
            if not val_str or val_str == "null":
                continue
            counts = float(val_str)
            # Extract date only (daily resolution)
            date_str = dt_str[:10]
            rows.append((date_str, counts))
        except (ValueError, IndexError):
            continue
    return rows


async def fetch_station_month(session: aiohttp.ClientSession,
                               station: str, year: int, month: int) -> list[tuple]:
    """Fetch one month of daily data for a station from NMDB."""
    start_day = 1
    if month == 12:
        end_year, end_month = year + 1, 1
    else:
        end_year, end_month = year, month + 1
    end_day = 1

    params = {
        "wget": "1",
        "stations[]": station,
        "output": "ascii",
        "tabchoice": "revori",
        "dtype": "corr_for_efficiency",
        "tresolution": "1440",
        "date_choice": "bydate",
        "start_year": str(year),
        "start_month": str(month),
        "start_day": str(start_day),
        "start_hour": "0",
        "start_min": "0",
        "end_year": str(end_year),
        "end_month": str(end_month),
        "end_day": str(end_day),
        "end_hour": "0",
        "end_min": "0",
        "yunits": "0",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(NMDB_API, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    rows = parse_nmdb_ascii(text)
                    if rows:
                        return [(station, d, c) for d, c in rows]
                    return []
                elif resp.status in (429, 503):
                    logger.warning("  %s %d-%02d: rate limited (HTTP %d), retry %d",
                                   station, year, month, resp.status, attempt)
                    await asyncio.sleep(5 * attempt)
                elif resp.status == 204:
                    return []
                else:
                    logger.warning("  %s %d-%02d: HTTP %d", station, year, month, resp.status)
                    if attempt == MAX_RETRIES:
                        return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("  %s %d-%02d: %s after %d retries",
                               station, year, month, type(e).__name__, MAX_RETRIES)
                return []
            await asyncio.sleep(2 ** attempt)

    return []


async def main():
    await init_db()
    await init_cosmic_ray_table()

    now = datetime.now(timezone.utc).isoformat()

    # Check existing data
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT station, MIN(observed_at), MAX(observed_at), COUNT(*) "
            "FROM cosmic_ray GROUP BY station"
        )
        existing_keys = set()
        rows_all = await db.execute_fetchall(
            "SELECT station, observed_at FROM cosmic_ray"
        )
        for r in rows_all:
            existing_keys.add((r[0], r[1]))

    for row in existing:
        logger.info("Existing: %s: %d records (%s to %s)", row[0], row[3], row[1], row[2])

    # Generate months to fetch: 2011-01 to current month
    today = datetime.now(timezone.utc)
    months_to_fetch = []
    for year in range(2011, today.year + 1):
        for month in range(1, 13):
            if year == today.year and month > today.month:
                break
            months_to_fetch.append((year, month))

    total_records = 0
    total_fetched = 0

    async with aiohttp.ClientSession() as session:
        for station in STATIONS:
            station_records = 0
            skip_count = 0

            for year, month in months_to_fetch:
                # Quick check: if first day of month exists, skip
                check_key = (station, f"{year:04d}-{month:02d}-01")
                if check_key in existing_keys:
                    skip_count += 1
                    continue

                rows = await fetch_station_month(session, station, year, month)
                if rows:
                    new_rows = [(s, d, c) for s, d, c in rows
                                if (s, d) not in existing_keys]
                    if new_rows:
                        async with safe_connect() as db:
                            await db.executemany(
                                """INSERT OR IGNORE INTO cosmic_ray
                                   (station, observed_at, counts_per_sec, received_at)
                                   VALUES (?, ?, ?, ?)""",
                                [(s, d, c, now) for s, d, c in new_rows],
                            )
                            await db.commit()
                        station_records += len(new_rows)
                        for s, d, c in new_rows:
                            existing_keys.add((s, d))

                total_fetched += 1
                if total_fetched % 20 == 0:
                    logger.info("  %s: %d months fetched, %d new records",
                                station, total_fetched, station_records)

                await asyncio.sleep(1.0)  # Rate limit

            total_records += station_records
            logger.info("%s complete: %d new records (%d months skipped)",
                        station, station_records, skip_count)
            total_fetched = 0  # Reset per station

    logger.info("Cosmic ray fetch complete: %d total new records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
