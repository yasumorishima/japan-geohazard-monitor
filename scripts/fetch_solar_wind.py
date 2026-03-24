"""Fetch solar wind parameters from NASA OMNIWeb.

Solar wind measurements from ACE/DSCOVR/Wind spacecraft at L1 point,
combined into the OMNI2 hourly dataset. Much richer than Kp index
(which is a 3-hour global average derived from ground stations).

Physical mechanism:
    Solar wind → magnetospheric compression → geomagnetically induced
    currents (GIC) in Earth's crust → piezoelectric/electrokinetic
    stress modulation at fault zones. Bz southward turning events
    cause the strongest magnetospheric response.

    The Dst index (ring current intensity) reflects energy input from
    solar wind and correlates with telluric current magnitude.

Target features:
    - sw_bz_min_24h: minimum IMF Bz in 24 hours (nT, negative = geoeffective)
    - sw_pressure_max_24h: max dynamic pressure in 24h (nPa)
    - dst_min_24h: minimum Dst in 24h (nT, negative = storm)

Data source: NASA OMNIWeb Low-Resolution (hourly) OMNI2
    - FTP: https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/
    - Format: fixed-width text, one file per year
    - No authentication required

References:
    - Sobolev & Zakrzhevskaya (2020) Pure Appl. Geophys. 177:629-640
    - Urata et al. (2018) Nat. Hazards Earth Syst. Sci. 18:2897-2909
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

# NASA SPDF FTP (HTTP accessible)
OMNI_URL = "https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/omni2_{year}.dat"

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)
START_YEAR = 2011


async def init_solar_wind_table():
    """Create solar wind table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS solar_wind (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL UNIQUE,
                bz_gsm_nt REAL,
                speed_kms REAL,
                density_cm3 REAL,
                pressure_npa REAL,
                dst_nt REAL,
                received_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sw_time
            ON solar_wind(observed_at)
        """)
        await db.commit()


def parse_omni2(text: str, year: int) -> list[dict]:
    """Parse OMNI2 hourly data file.

    Column layout (1-indexed, space-separated):
        1: Year
        2: Day of Year (1-366)
        3: Hour (0-23)
        14: IMF Bz GSM (nT) — missing: 9999.99
        24: Flow speed (km/s) — missing: 9999.9
        25: Proton density (n/cc) — missing: 999.9
        28: Flow pressure (nPa) — missing: 99.99
        41: Dst index (nT) — missing: 99999

    Full format doc: https://omniweb.gsfc.nasa.gov/html/ow_data.html
    """
    rows = []
    for line in text.split("\n"):
        parts = line.split()
        if len(parts) < 41:
            continue

        try:
            yr = int(parts[0])
            doy = int(parts[1])
            hour = int(parts[2])

            if yr != year:
                continue

            # Construct datetime
            dt = datetime(yr, 1, 1, hour, 0, 0, tzinfo=timezone.utc)
            dt += timedelta(days=doy - 1)
            observed_at = dt.strftime("%Y-%m-%dT%H:%M:%S")

            # Parse values with missing value detection
            bz = float(parts[13])
            speed = float(parts[23])
            density = float(parts[24])
            pressure = float(parts[27])
            dst = float(parts[40])

            # Replace missing values with None
            bz = None if abs(bz) > 999 else bz
            speed = None if speed > 9000 else speed
            density = None if density > 900 else density
            pressure = None if pressure > 90 else pressure
            dst = None if abs(dst) > 9000 else dst

            rows.append({
                "observed_at": observed_at,
                "bz": bz,
                "speed": speed,
                "density": density,
                "pressure": pressure,
                "dst": dst,
            })
        except (ValueError, IndexError):
            continue

    return rows


async def main():
    await init_db()
    await init_solar_wind_table()

    now = datetime.now(timezone.utc).isoformat()
    current_year = datetime.now(timezone.utc).year

    # Check existing data
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(*) FROM solar_wind"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    n_existing = existing[0][1] if existing else 0

    if last_date:
        start_year = int(last_date[:4])
        logger.info("Solar wind existing: %d records (latest: %s)", n_existing, last_date)
    else:
        start_year = START_YEAR
        logger.info("Solar wind: no existing data, starting from %d", start_year)

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for year in range(start_year, current_year + 1):
            url = OMNI_URL.format(year=year)
            text = None

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    async with session.get(url, timeout=TIMEOUT) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            break
                        elif resp.status == 404:
                            logger.info("OMNI2 %d: not yet available (404)", year)
                            break
                        else:
                            logger.warning("OMNI2 %d: HTTP %d (attempt %d)", year, resp.status, attempt)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == MAX_RETRIES:
                        logger.warning("OMNI2 %d: %s", year, type(e).__name__)
                    await asyncio.sleep(2 ** attempt)

            if not text:
                continue

            rows = parse_omni2(text, year)
            if not rows:
                continue

            # Store in DB
            async with safe_connect() as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO solar_wind
                       (observed_at, bz_gsm_nt, speed_kms, density_cm3,
                        pressure_npa, dst_nt, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [(r["observed_at"], r["bz"], r["speed"], r["density"],
                      r["pressure"], r["dst"], now) for r in rows],
                )
                await db.commit()

            total_records += len(rows)
            logger.info("OMNI2 %d: %d hourly records", year, len(rows))
            await asyncio.sleep(1.0)

    logger.info("Solar wind fetch complete: %d total records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
