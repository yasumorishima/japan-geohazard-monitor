"""Fetch GOES proton flux data for earthquake precursor analysis.

Solar Energetic Particle (SEP) events deliver high-energy protons (>10 MeV)
that penetrate to Earth's stratosphere and below. These create ionization
cascades in the atmosphere, modifying the global electric circuit. Enhanced
proton flux -> increased atmospheric conductivity -> modified telluric
currents -> potential stress modulation at fault zones. Major SEP events
(>100 pfu) are rare (~10/solar cycle) but deliver enormous energy.

Target features:
    - proton_10mev_max: max daily >=10 MeV flux (pfu)
    - proton_60mev_max: max daily >=60 MeV flux (pfu)

Data sources:
    1. NASA OMNIWeb OMNI2 hourly files (historical, 2011-present)
       - Column 28 (0-indexed 27): Proton flux > 10 MeV (1/(SEc-cm2-Ster))
       - Column 30 (0-indexed 29): Proton flux > 60 MeV
       - Missing value: 999.99
       - URL: https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/omni2_{year}.dat
    2. NOAA SWPC JSON (recent 7-day, for near-real-time updates)
       - URL: https://services.swpc.noaa.gov/json/goes/primary/integral-protons-7-day.json

References:
    - Ouzounov et al. (2018) Pre-Earthquake Processes, AGU Monograph
    - Pulinets & Ouzounov (2011) Adv. Space Res. 47(3):413-431
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

# NASA SPDF FTP (HTTP accessible) — same source as fetch_solar_wind.py
OMNI_URL = "https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/omni2_{year}.dat"

# NOAA SWPC recent 7-day proton flux (JSON)
SWPC_PROTON_URL = "https://services.swpc.noaa.gov/json/goes/primary/integral-protons-7-day.json"

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)
START_YEAR = 2011

# OMNI2 missing value threshold for proton flux columns
MISSING_PROTON = 900.0


async def init_goes_proton_table():
    """Create goes_proton table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS goes_proton (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                proton_10mev_max REAL,
                proton_60mev_max REAL,
                UNIQUE(observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_gp_time
            ON goes_proton(observed_at)
        """)
        await db.commit()


def parse_omni2_proton(text: str, year: int) -> list[dict]:
    """Parse OMNI2 hourly data file for proton flux columns.

    Column layout (0-indexed, space-separated):
        0: Year
        1: Day of Year (1-366)
        2: Hour (0-23)
        27: Proton flux > 10 MeV (1/(SEc-cm2-Ster)) -- missing: 999.99
        29: Proton flux > 60 MeV -- missing: 999.99

    Full format doc: https://omniweb.gsfc.nasa.gov/html/ow_data.html
    """
    rows = []
    for line in text.split("\n"):
        parts = line.split()
        if len(parts) < 31:
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
            # OMNI2 year files allocate slots for the full year. Skip future
            # slots so unpopulated placeholders don't widen the date range.
            if dt > datetime.now(timezone.utc):
                continue
            observed_at = dt.strftime("%Y-%m-%dT%H:%M:%S")

            # Parse proton flux values
            p10 = float(parts[27])
            p60 = float(parts[29])

            # Replace missing values with None
            p10 = None if p10 > MISSING_PROTON else p10
            p60 = None if p60 > MISSING_PROTON else p60

            rows.append({
                "observed_at": observed_at,
                "p10": p10,
                "p60": p60,
            })
        except (ValueError, IndexError):
            continue

    return rows


def aggregate_daily_max(hourly_rows: list[dict]) -> list[dict]:
    """Aggregate hourly proton flux to daily max.

    For SEP event detection, peak flux matters more than average.
    """
    daily: dict[str, dict] = {}
    for r in hourly_rows:
        date_str = r["observed_at"][:10]  # YYYY-MM-DD
        if date_str not in daily:
            daily[date_str] = {"p10": None, "p60": None}

        d = daily[date_str]
        if r["p10"] is not None:
            d["p10"] = max(d["p10"], r["p10"]) if d["p10"] is not None else r["p10"]
        if r["p60"] is not None:
            d["p60"] = max(d["p60"], r["p60"]) if d["p60"] is not None else r["p60"]

    return [
        {
            "observed_at": f"{date}T00:00:00",
            "p10": v["p10"],
            "p60": v["p60"],
        }
        for date, v in sorted(daily.items())
    ]


async def fetch_swpc_recent(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch recent 7-day proton flux from NOAA SWPC JSON.

    Returns daily-max aggregated records for >=10 MeV and >=100 MeV
    (mapped to p10 and p60 respectively, since >=100 MeV is the closest
    available energy band in this endpoint).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(SWPC_PROTON_URL, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    break
                else:
                    logger.warning("SWPC proton: HTTP %d (attempt %d)", resp.status, attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("SWPC proton: %s", type(e).__name__)
                return []
            await asyncio.sleep(2 ** attempt)
    else:
        return []

    # Parse SWPC JSON: group by energy band, aggregate daily max
    # Energy bands in the JSON: ">=10 MeV", ">=50 MeV", ">=100 MeV", etc.
    hourly: dict[str, dict] = {}
    for entry in data:
        time_tag = entry.get("time_tag", "")
        energy = entry.get("energy", "")
        flux = entry.get("flux")

        if not time_tag or flux is None:
            continue

        date_str = time_tag[:10]  # YYYY-MM-DD
        if date_str not in hourly:
            hourly[date_str] = {"p10": None, "p60": None}

        d = hourly[date_str]
        if ">=10" in energy:
            d["p10"] = max(d["p10"], flux) if d["p10"] is not None else flux
        elif ">=100" in energy:
            # >=100 MeV is stricter than >=60 MeV; use as proxy for high-energy band
            d["p60"] = max(d["p60"], flux) if d["p60"] is not None else flux

    return [
        {
            "observed_at": f"{date}T00:00:00",
            "p10": v["p10"],
            "p60": v["p60"],
        }
        for date, v in sorted(hourly.items())
    ]


async def main():
    await init_db()
    await init_goes_proton_table()

    current_year = datetime.now(timezone.utc).year
    now_iso = datetime.now(timezone.utc).isoformat()

    # One-time purge: OMNI2 yearly files ship full-year placeholder slots,
    # so runs before 2026-04-14 inserted future-dated rows. Delete anything
    # beyond now so the time range stays honest.
    async with safe_connect() as db:
        cur = await db.execute(
            "DELETE FROM goes_proton WHERE observed_at > ?", (now_iso,)
        )
        deleted = cur.rowcount if cur else 0
        await db.commit()
    if deleted:
        logger.warning("GOES proton: purged %d future-dated placeholder rows", deleted)

    # Check existing data
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(*) FROM goes_proton"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    n_existing = existing[0][1] if existing else 0

    if last_date:
        start_year = int(last_date[:4])
        logger.info("GOES proton existing: %d records (latest: %s)", n_existing, last_date)
    else:
        start_year = START_YEAR
        logger.info("GOES proton: no existing data, starting from %d", start_year)

    total_records = 0
    async with aiohttp.ClientSession() as session:
        # --- Phase 1: Historical data from OMNI2 ---
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

            hourly_rows = parse_omni2_proton(text, year)
            if not hourly_rows:
                continue

            daily_rows = aggregate_daily_max(hourly_rows)

            # Store in DB
            async with safe_connect() as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO goes_proton
                       (observed_at, proton_10mev_max, proton_60mev_max)
                       VALUES (?, ?, ?)""",
                    [(r["observed_at"], r["p10"], r["p60"]) for r in daily_rows],
                )
                await db.commit()

            total_records += len(daily_rows)
            logger.info("OMNI2 %d: %d hourly -> %d daily proton records",
                        year, len(hourly_rows), len(daily_rows))
            await asyncio.sleep(1.0)

        # --- Phase 2: Recent 7-day from SWPC (fills gap between OMNI2 and today) ---
        logger.info("Fetching SWPC 7-day proton flux...")
        swpc_rows = await fetch_swpc_recent(session)
        if swpc_rows:
            async with safe_connect() as db:
                await db.executemany(
                    """INSERT OR REPLACE INTO goes_proton
                       (observed_at, proton_10mev_max, proton_60mev_max)
                       VALUES (?, ?, ?)""",
                    [(r["observed_at"], r["p10"], r["p60"]) for r in swpc_rows],
                )
                await db.commit()
            total_records += len(swpc_rows)
            logger.info("SWPC 7-day: %d daily proton records", len(swpc_rows))

    logger.info("GOES proton fetch complete: %d total daily records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
