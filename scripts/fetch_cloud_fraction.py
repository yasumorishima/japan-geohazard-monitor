"""Fetch cloud fraction data from NASA MODIS for earthquake cloud analysis.

"Earthquake clouds" — unusual linear cloud formations along fault lines —
have been reported before major earthquakes. While controversial, the
physical mechanism is plausible:

Physical mechanism:
    Crustal stress → radon/ion release from faults → atmospheric ionization
    → water vapor condensation nuclei → linear cloud formation along
    fault trace. This is part of the LAIC (Lithosphere-Atmosphere-Ionosphere
    Coupling) model.

    Statistical analysis with satellite cloud fraction data can objectively
    test whether cloud patterns anomalies occur before earthquakes.

Data source: NASA MODIS Terra/Aqua Level 3 daily (1° global grid)
    - Product: MOD08_D3 (Terra) / MYD08_D3 (Aqua) — Cloud_Fraction_Mean
    - OPeNDAP access via LAADS DAAC
    - Requires Earthdata authentication

Target features:
    - cloud_fraction_anomaly: cloud cover deviation from 30-day baseline (σ)

References:
    - Guangmeng & Jie (2013) Nat. Hazards Earth Syst. Sci. 13:927-934
    - Shou (2006) Terr. Atmos. Ocean. Sci. 17:395-414
"""

import asyncio
import logging
import os
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

EARTHDATA_TOKEN = os.environ.get("EARTHDATA_TOKEN")

# LAADS DAAC OPeNDAP for MOD08_D3 (MODIS Terra daily L3 atmosphere)
LAADS_OPENDAP = "https://ladsweb.modaps.eosdis.nasa.gov/opendap/allData/61/MOD08_D3"

# Japan bbox in 1° grid
# Lat: -90 to 90 (180 cells), Lon: -180 to 180 (360 cells)
# Japan: lat 24-46 → indices 114 to 136
# Japan: lon 122-150 → indices 302 to 330
LAT_START = 114
LAT_END = 136
LON_START = 302
LON_END = 330

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)
START_YEAR = 2011


async def init_cloud_table():
    """Create cloud fraction table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS cloud_fraction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                cloud_frac REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cloud_time
            ON cloud_fraction(observed_at)
        """)
        await db.commit()


async def fetch_cloud_day(session: aiohttp.ClientSession, date: datetime) -> list[dict]:
    """Fetch cloud fraction for one day via LAADS OPeNDAP."""
    if not EARTHDATA_TOKEN:
        return []

    year = date.year
    doy = date.timetuple().tm_yday
    date_str = date.strftime("%Y-%m-%d")

    # MOD08_D3 filename pattern
    filename = f"MOD08_D3.A{year}{doy:03d}.061.*.hdf"

    # OPeNDAP with subsetting for Cloud_Fraction_Mean
    url = (
        f"{LAADS_OPENDAP}/{year}/{doy:03d}/"
        f"?Cloud_Fraction_Mean_Mean"
        f"[{LAT_START}:1:{LAT_END}]"
        f"[{LON_START}:1:{LON_END}]"
    )

    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT, headers=headers,
                                    allow_redirects=True) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    if "html" in content_type.lower():
                        return []
                    text = await resp.text()
                    return _parse_cloud_ascii(text, date_str)
                elif resp.status in (401, 403):
                    logger.info("Cloud fraction requires Earthdata auth")
                    return []
                elif resp.status == 404:
                    return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("Cloud %s: %s", date_str, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_cloud_ascii(text: str, date_str: str) -> list[dict]:
    """Parse OPeNDAP ASCII cloud fraction grid."""
    rows = []
    in_data = False

    for line in text.split("\n"):
        line = line.strip()
        if "Cloud_Fraction" in line and "[" in line:
            in_data = True
            continue
        if not in_data:
            continue

        if line.startswith("["):
            parts = line.split(",")
            if len(parts) < 2:
                continue
            try:
                lat_idx = int(parts[0].strip("[] "))
                lat = -89.5 + (LAT_START + lat_idx)  # 1° grid, center

                for lon_offset, val_str in enumerate(parts[1:]):
                    val = float(val_str.strip())
                    if val < 0 or val > 1.1:
                        continue  # Fill value
                    lon = -179.5 + (LON_START + lon_offset)

                    rows.append({
                        "date": date_str,
                        "lat": round(lat, 1),
                        "lon": round(lon, 1),
                        "cloud_frac": round(val, 4),
                    })
            except (ValueError, IndexError):
                continue

    return rows


async def main():
    await init_db()
    await init_cloud_table()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN:
        logger.info(
            "Cloud fraction fetch: EARTHDATA_TOKEN not set. "
            "Cloud features will be excluded via dynamic selection."
        )
        return

    # Fetch dates around M6+ earthquakes
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at) FROM earthquakes "
            "WHERE magnitude >= 6.0 AND DATE(occurred_at) >= '2011-01-01'"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM cloud_fraction"
        )
    existing_dates = set(r[0] for r in existing) if existing else set()

    target_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-7, 8):
            target_dates.add(d + timedelta(days=offset))

    dates_to_fetch = sorted(
        d for d in target_dates
        if d.strftime("%Y-%m-%d") not in existing_dates
        and d.year >= START_YEAR
    )[:200]

    if not dates_to_fetch:
        logger.info("All cloud fraction target dates already fetched")
        return

    logger.info("Cloud fraction: %d dates to fetch", len(dates_to_fetch))

    total_records = 0
    async with aiohttp.ClientSession() as session:
        for i, date in enumerate(dates_to_fetch):
            rows = await fetch_cloud_day(session, date)
            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO cloud_fraction
                           (observed_at, cell_lat, cell_lon, cloud_frac, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["date"], r["lat"], r["lon"], r["cloud_frac"], now) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 20 == 0:
                logger.info("Cloud: %d/%d dates, %d records",
                            i + 1, len(dates_to_fetch), total_records)
            await asyncio.sleep(1.0)

    logger.info("Cloud fraction fetch complete: %d records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
