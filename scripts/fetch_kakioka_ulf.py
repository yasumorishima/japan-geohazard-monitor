"""Fetch ULF magnetic field data from Kakioka Magnetic Observatory (JMA).

Downloads 1-minute geomagnetic data from the World Data Centre for
Geomagnetism, Kyoto (WDC Kyoto), which hosts data from Kakioka (KAK),
Memambetsu (MMB), and Kanoya (KNY) observatories in Japan.

Physical basis: Stress changes in the crust can generate ULF (0.01-10 Hz)
electromagnetic emissions via piezoelectric, electrokinetic, and
microfracturing processes (Hayakawa et al., 2007).

Data source: WDC Kyoto (https://wdc.kugi.kyoto-u.ac.jp/)
Format: IAGA-2002 format, 1-minute values
Stations: KAK (Kakioka, 36.23°N, 140.19°E), MMB (Memambetsu, 43.91°N, 144.19°E),
          KNY (Kanoya, 31.42°N, 130.88°E)
"""

import asyncio
import logging
import re
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

# WDC Kyoto data download URL
# Format: IAGA-2002 1-minute definitive data
WDC_URL = "https://wdc.kugi.kyoto-u.ac.jp/cgi-bin/catdata.cgi"

# Japanese magnetic observatories
STATIONS = {
    "KAK": {"lat": 36.23, "lon": 140.19, "name": "Kakioka"},
    "MMB": {"lat": 43.91, "lon": 144.19, "name": "Memambetsu"},
    "KNY": {"lat": 31.42, "lon": 130.88, "name": "Kanoya"},
}

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=60, connect=30)


async def init_ulf_table():
    """Create ULF magnetic field table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ulf_magnetic (
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
            CREATE INDEX IF NOT EXISTS idx_ulf_time
            ON ulf_magnetic(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_ulf_station
            ON ulf_magnetic(station)
        """)
        await db.commit()


def parse_iaga2002(text: str, station: str) -> list[tuple]:
    """Parse IAGA-2002 format magnetic data.

    Returns list of (station, observed_at, H, D, Z, F) tuples.
    Missing values (99999) are stored as None.
    """
    rows = []
    in_data = False
    for line in text.split("\n"):
        if line.startswith("DATE"):
            in_data = True
            continue
        if not in_data or not line.strip():
            continue
        # IAGA-2002 data line format:
        # DATE       TIME         DOY     KAKH      KAKD      KAKZ      KAKF
        # 2024-01-01 00:00:00.000 001     29515.40  -6543.20  35432.10  46423.50
        parts = line.split()
        if len(parts) < 7:
            continue
        try:
            date_str = parts[0]
            time_str = parts[1]
            observed_at = f"{date_str}T{time_str[:8]}"

            h = float(parts[3])
            d = float(parts[4])
            z = float(parts[5])
            f = float(parts[6])

            # 99999 or 88888 = missing
            h = None if abs(h) > 90000 else h
            d = None if abs(d) > 90000 else d
            z = None if abs(z) > 90000 else z
            f = None if abs(f) > 90000 else f

            rows.append((station, observed_at, h, d, z, f))
        except (ValueError, IndexError):
            continue
    return rows


async def fetch_station_day(session: aiohttp.ClientSession,
                            station: str, date: datetime) -> list[tuple]:
    """Fetch 1-minute data for a station and date from WDC Kyoto."""
    # WDC Kyoto CGI parameters
    params = {
        "site": station.lower(),
        "year": date.strftime("%Y"),
        "month": date.strftime("%m"),
        "day": date.strftime("%d"),
        "output": "iaga2002",
        "type": "definitive",
        "resolution": "minute",
    }

    # Try multiple URL patterns
    urls = [
        f"https://wdc.kugi.kyoto-u.ac.jp/cgi-bin/catdata.cgi",
        f"https://wdc.kugi.kyoto-u.ac.jp/mdpub/min/{date.strftime('%Y%m')}/{station.lower()}{date.strftime('%Y%m%d')}min.dat",
    ]

    for url in urls:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if "cgi-bin" in url:
                    async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            rows = parse_iaga2002(text, station)
                            if rows:
                                return rows
                else:
                    async with session.get(url, timeout=TIMEOUT) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            rows = parse_iaga2002(text, station)
                            if rows:
                                return rows
                        elif resp.status == 404:
                            break  # Try next URL
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == MAX_RETRIES:
                    break
                await asyncio.sleep(2 ** attempt)

    return []


async def main():
    await init_db()
    await init_ulf_table()

    now = datetime.now(timezone.utc).isoformat()

    # Get dates around M6.5+ earthquakes (±3 days)
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at) FROM earthquakes "
            "WHERE magnitude >= 6.5 ORDER BY occurred_at"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT DATE(observed_at), station FROM ulf_magnetic"
        )

    eq_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-3, 4):
            eq_dates.add(d + timedelta(days=offset))

    existing_set = set((r[0], r[1]) for r in existing)

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for station in ["KAK"]:  # Start with Kakioka (closest to Tokyo/Kanto seismic zone)
            dates_to_fetch = sorted(
                d for d in eq_dates
                if (d.strftime("%Y-%m-%d"), station) not in existing_set
            )
            logger.info("%s: %d dates to fetch (%d total eq dates, %d existing)",
                        station, len(dates_to_fetch), len(eq_dates), len(existing_set))

            for i, date in enumerate(dates_to_fetch[:50]):  # Limit for initial run
                rows = await fetch_station_day(session, station, date)
                if rows:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.executemany(
                            """INSERT OR IGNORE INTO ulf_magnetic
                               (station, observed_at, h_nt, d_nt, z_nt, f_nt, received_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            [(s, t, h, d, z, f, now) for s, t, h, d, z, f in rows],
                        )
                        await db.commit()
                    total_records += len(rows)
                    logger.info("  %s %s: %d records", station, date.strftime("%Y-%m-%d"), len(rows))

                if (i + 1) % 10 == 0:
                    logger.info("  Progress: %d/%d dates, %d records",
                                i + 1, len(dates_to_fetch), total_records)

                await asyncio.sleep(0.5)  # Rate limit

    logger.info("ULF fetch complete: %d records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
