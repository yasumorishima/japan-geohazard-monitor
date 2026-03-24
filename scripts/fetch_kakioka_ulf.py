"""Fetch ULF magnetic field data from Japanese observatories.

Downloads 1-minute geomagnetic data from multiple sources:
1. INTERMAGNET via BGS GINServices REST API (primary, no auth)
2. WDC Kyoto direct file downloads (fallback)

Physical basis: Stress changes in the crust can generate ULF (0.01-10 Hz)
electromagnetic emissions via piezoelectric, electrokinetic, and
microfracturing processes (Hayakawa et al., 2007; Hattori, 2004).

Key precursor signatures:
    - ULF power increase (0.01-0.1 Hz) 1-30 days before M6+ earthquakes
    - Polarization ratio Sz/Sh increase (vertical/horizontal)
    - Fractal dimension change of ULF time series

Stations:
    KAK (Kakioka, 36.23°N, 140.19°E) - closest to Kanto seismic zone
    MMB (Memambetsu, 43.91°N, 144.19°E) - Hokkaido
    KNY (Kanoya, 31.42°N, 130.88°E) - Kyushu

References:
    - Hayakawa et al. (2007) J. Atmos. Sol.-Terr. Phys.
    - Hattori (2004) Nat. Hazards Earth Syst. Sci.
    - Fraser-Smith et al. (1990) GRL 17:1465-1468 (Loma Prieta)
"""

import asyncio
import logging
import re
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
    "KAK": {"lat": 36.23, "lon": 140.19, "name": "Kakioka"},
    "MMB": {"lat": 43.91, "lon": 144.19, "name": "Memambetsu"},
    "KNY": {"lat": 31.42, "lon": 130.88, "name": "Kanoya"},
}

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)

# INTERMAGNET via BGS Edinburgh GIN
# REST API providing IAGA-2002 formatted data
INTERMAGNET_API = "https://imag-data.bgs.ac.uk/GIN_V1/GINServices"


async def init_ulf_table():
    """Create ULF magnetic field table."""
    async with safe_connect() as db:
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

    Returns list of (station, observed_at, comp1, comp2, Z, F) tuples.
    INTERMAGNET returns XYZG (X≈H north, Y≈D east, Z vertical, G≈F total).
    WDC Kyoto returns HDZF. Both map to the same DB columns.
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


async def fetch_intermagnet_day(session: aiohttp.ClientSession,
                                 station: str, date: datetime) -> list[tuple]:
    """Fetch 1-minute data from INTERMAGNET BGS GIN API.

    API endpoint: GINServices?Request=GetData&format=iaga2002
    Documented at: https://imag-data.bgs.ac.uk/GIN/GINFederated
    """
    start = date.strftime("%Y-%m-%dT00:00:00Z")
    end = (date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    # Try multiple API URL patterns
    urls_params = [
        # Pattern 1: GIN V1 API (confirmed working 2026-03-16)
        (f"{INTERMAGNET_API}?Request=GetData&observatoryIagaCode={station}"
         f"&SamplesPerDay=1440&dataStartDate={start}&dataDuration=1"
         f"&publicationState=adj-or-rep&format=iaga2002", None),
    ]

    for url, params in urls_params:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        # INTERMAGNET returns IAGA-2002 with DATE header line
                        if "DATE" in text:
                            rows = parse_iaga2002(text, station)
                            if rows:
                                return rows
                    elif resp.status == 204:
                        return []  # No data available
                    elif resp.status == 404:
                        break  # Try next URL
                    else:
                        if attempt == MAX_RETRIES:
                            break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == MAX_RETRIES:
                    break
                await asyncio.sleep(2 ** attempt)

    return []


async def fetch_wdc_kyoto_day(session: aiohttp.ClientSession,
                               station: str, date: datetime) -> list[tuple]:
    """Fetch from WDC Kyoto (fallback).

    Multiple URL patterns tried based on WDC data structure.
    """
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    ym = date.strftime("%Y%m")
    ymd = date.strftime("%Y%m%d")
    stn = station.lower()

    urls = [
        # Definitive 1-minute data
        f"https://wdc.kugi.kyoto-u.ac.jp/mdpub/min/{ym}/{stn}{ymd}min.dat",
        # Quasi-definitive
        f"https://wdc.kugi.kyoto-u.ac.jp/mdpub/min/{ym}/{stn}{ymd}qmin.dat",
        # CGI interface
        f"https://wdc.kugi.kyoto-u.ac.jp/cgi-bin/catdata.cgi"
        f"?site={stn}&year={year}&month={month}&day={day}"
        f"&output=iaga2002&type=definitive&resolution=minute",
    ]

    for url in urls:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        rows = parse_iaga2002(text, station)
                        if rows:
                            return rows
                    elif resp.status == 404:
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == MAX_RETRIES:
                    break
                await asyncio.sleep(2 ** attempt)

    return []


async def fetch_station_day(session: aiohttp.ClientSession,
                             station: str, date: datetime) -> list[tuple]:
    """Fetch day of data, trying INTERMAGNET first, WDC Kyoto fallback."""
    rows = await fetch_intermagnet_day(session, station, date)
    if rows:
        return rows
    return await fetch_wdc_kyoto_day(session, station, date)


async def main():
    await init_db()
    await init_ulf_table()

    now = datetime.now(timezone.utc).isoformat()

    # Get dates around M6+ earthquakes (±7 days for better temporal coverage)
    async with safe_connect() as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at), latitude, longitude, magnitude "
            "FROM earthquakes "
            "WHERE magnitude >= 6.0 ORDER BY occurred_at"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT DATE(observed_at), station FROM ulf_magnetic"
        )

    eq_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-7, 8):
            eq_dates.add(d + timedelta(days=offset))

    existing_set = set((r[0], r[1]) for r in existing)

    total_records = 0
    total_fetched = 0
    total_failed = 0

    async with aiohttp.ClientSession() as session:
        # Try all three stations but prioritize KAK (most relevant for Tokyo/Kanto)
        for station in ["KAK", "MMB", "KNY"]:
            dates_to_fetch = sorted(
                d for d in eq_dates
                if (d.strftime("%Y-%m-%d"), station) not in existing_set
                and d.year >= 2011  # INTERMAGNET has good coverage from 2011+
            )

            if not dates_to_fetch:
                logger.info("%s: all dates already fetched", station)
                continue

            logger.info("%s: %d dates to fetch (%d total eq dates, %d existing)",
                        station, len(dates_to_fetch), len(eq_dates), len(existing_set))

            # Process in batches — increased limit for full temporal coverage
            # INTERMAGNET GIN V1 is reliable, 0.5s between requests
            max_per_station = 300
            batch = dates_to_fetch[:max_per_station]
            station_records = 0

            for i, date in enumerate(batch):
                rows = await fetch_station_day(session, station, date)
                if rows:
                    async with safe_connect() as db:
                        await db.executemany(
                            """INSERT OR IGNORE INTO ulf_magnetic
                               (station, observed_at, h_nt, d_nt, z_nt, f_nt, received_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            [(s, t, h, d, z, f, now) for s, t, h, d, z, f in rows],
                        )
                        await db.commit()
                    station_records += len(rows)
                    total_records += len(rows)
                    total_fetched += 1
                else:
                    total_failed += 1

                if (i + 1) % 20 == 0:
                    logger.info("  %s: %d/%d dates, %d records (total %d)",
                                station, i + 1, len(batch), station_records, total_records)

                await asyncio.sleep(0.5)  # Rate limit

            logger.info("%s complete: %d records from %d dates", station, station_records, len(batch))

    logger.info("ULF fetch complete: %d records from %d fetches (%d failed)",
                total_records, total_fetched, total_failed)


if __name__ == "__main__":
    asyncio.run(main())
