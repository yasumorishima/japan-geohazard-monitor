"""Fetch Earth Orientation Parameters (EOP) from IERS.

Earth Orientation Parameters capture tiny variations in Earth's rotation:
    - LOD (Length of Day): deviations from 86400 SI seconds (~ms scale)
    - Polar motion (x, y): wobble of rotation axis (~arcsec scale)
    - ΔUT1: difference between UTC and UT1

Physical mechanism (speculative, largely untested in earthquake ML):
    - LOD changes reflect angular momentum exchange between solid Earth,
      atmosphere, and ocean. Large earthquakes redistribute mass and
      measurably change LOD (2011 Tohoku shortened day by 1.8 μs).
    - The INVERSE question is unexplored: do LOD rate-of-change or polar
      motion velocity anomalies PRECEDE large earthquakes?
    - Polar motion affects latitude-dependent tidal loading, which
      modulates stress on plate boundaries.

This is a novel feature for earthquake ML — virtually no prior work
uses EOP as predictive features.

Data sources (tried in order):
    1. OBSPM (Paris Observatory) eopc04: daily EOP, updated daily
    2. USNO finals2000A: daily EOP (backup, may be stale)

Target features:
    - lod_rate: day-to-day LOD change rate (ms/day)
    - polar_motion_speed: polar motion velocity (arcsec/day)

References:
    - Chao & Gross (1987) Geophys. J. R. Astr. Soc. 91:569-596
    - Gross (2007) Treatise on Geophysics, Vol. 3
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# OBSPM (Paris Observatory) eopc04 — primary source, updated daily
OBSPM_EOPC04_URL = "https://hpiers.obspm.fr/iers/eop/eopc04/eopc04.1962-now"

# USNO finals2000A — backup (may be stale but format is well-known)
USNO_FINALS_URL = "https://maia.usno.navy.mil/ser7/finals2000A.data"

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)


async def init_eop_table():
    """Create Earth Orientation Parameters table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS earth_rotation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL UNIQUE,
                x_arcsec REAL,
                y_arcsec REAL,
                dut1_s REAL,
                lod_ms REAL,
                received_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_eop_time
            ON earth_rotation(observed_at)
        """)
        await db.commit()


def parse_eopc04(text: str) -> list[dict]:
    """Parse OBSPM eopc04.1962-now space-separated format.

    Format (space-separated columns):
        Year  Month  Day  MJD  x(")  y(")  UT1-UTC(s)  LOD(s)  dX(")  dY(")  ...

    Header lines start with '#' or contain non-numeric first fields.
    LOD is in SECONDS in eopc04 (convert to milliseconds for DB).
    Only use data from 2011 onwards.
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
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            # parts[3] = MJD (skip)
            x = float(parts[4])
            y = float(parts[5])
            dut1 = float(parts[6])
            lod_s = float(parts[7])

            if year < 2011:
                continue

            date_str = f"{year:04d}-{month:02d}-{day:02d}"

            # Convert LOD from seconds to milliseconds
            lod_ms = lod_s * 1000.0

            rows.append({
                "date": date_str,
                "x": x,
                "y": y,
                "dut1": dut1,
                "lod": lod_ms,
            })
        except (ValueError, IndexError):
            continue

    return rows


def parse_finals2000a(text: str) -> list[dict]:
    """Parse IERS finals2000A.data fixed-width format.

    Column layout (0-indexed character positions):
        0-1:   year (2-digit)
        2-3:   month
        4-5:   day
        7:     IERS bulletin type (I=IERS, P=predicted)
        18-27: x pole (arcsec)
        37-46: y pole (arcsec)
        58-68: UT1-UTC (seconds)
        79-86: LOD (milliseconds)

    Only use 'I' (observed) values, not 'P' (predicted).
    """
    rows = []
    for line in text.split("\n"):
        if len(line) < 80:
            continue

        try:
            # Parse date
            yy = int(line[0:2].strip())
            mm = int(line[2:4].strip())
            dd = int(line[4:6].strip())

            # 2-digit year: 70-99 = 1970-1999, 00-69 = 2000-2069
            year = 1900 + yy if yy >= 70 else 2000 + yy

            if year < 2011:
                continue

            # Bulletin type
            bulletin = line[16:17].strip()
            if bulletin == "P":
                continue  # Skip predicted values

            date_str = f"{year:04d}-{mm:02d}-{dd:02d}"

            # Parse values (may be blank)
            x_str = line[18:27].strip()
            y_str = line[37:46].strip()
            dut1_str = line[58:68].strip()
            lod_str = line[79:86].strip()

            x = float(x_str) if x_str else None
            y = float(y_str) if y_str else None
            dut1 = float(dut1_str) if dut1_str else None
            lod = float(lod_str) if lod_str else None

            rows.append({
                "date": date_str,
                "x": x,
                "y": y,
                "dut1": dut1,
                "lod": lod,
            })
        except (ValueError, IndexError):
            continue

    return rows


async def main():
    await init_db()
    await init_eop_table()

    now = datetime.now(timezone.utc).isoformat()

    # Check existing data
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(*) FROM earth_rotation"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    n_existing = existing[0][1] if existing else 0
    logger.info("EOP existing: %d records (latest: %s)", n_existing, last_date)

    # Try sources in order: OBSPM eopc04 (primary), USNO finals2000A (backup)
    sources = [
        (OBSPM_EOPC04_URL, parse_eopc04, "OBSPM eopc04"),
        (USNO_FINALS_URL, parse_finals2000a, "USNO finals2000A"),
    ]

    rows = []
    async with aiohttp.ClientSession() as session:
        for url, parser, label in sources:
            text = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    logger.info("Fetching EOP from %s (%s)...", label, url.split("/")[2])
                    async with session.get(url, timeout=TIMEOUT) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            logger.info("EOP data fetched from %s: %.1f KB",
                                        label, len(text) / 1024)
                            break
                        else:
                            logger.warning("EOP HTTP %d from %s", resp.status, label)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == MAX_RETRIES:
                        logger.warning("EOP fetch failed from %s: %s",
                                       label, type(e).__name__)
                    await asyncio.sleep(2 ** attempt)

            if text:
                rows = parser(text)
                if rows:
                    logger.info("Parsed %d EOP records (2011+) from %s", len(rows), label)
                    break
                else:
                    logger.warning("No parseable EOP records from %s, trying next source",
                                   label)

    if not rows:
        logger.error("Could not fetch EOP data from any source")
        return

    # Store in DB
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """INSERT OR IGNORE INTO earth_rotation
               (observed_at, x_arcsec, y_arcsec, dut1_s, lod_ms, received_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [(r["date"], r["x"], r["y"], r["dut1"], r["lod"], now) for r in rows],
        )
        await db.commit()

    new_count = len(rows) - n_existing
    logger.info("EOP fetch complete: %d total records (%d new)", len(rows), max(new_count, 0))


if __name__ == "__main__":
    asyncio.run(main())
