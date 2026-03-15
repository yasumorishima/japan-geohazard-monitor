"""Fetch MODIS Land Surface Temperature data from NASA LAADS DAAC.

Downloads MOD11A1 (Terra) daily 1km LST for Japan region around
M6.5+ earthquake dates (±7 days) for thermal anomaly analysis.

Physical basis: Stress-induced micro-fracturing → radon/gas release
→ surface heating → detectable thermal IR anomaly before earthquakes
(Tronin 2006, Ouzounov & Freund 2004).

Data source: NASA LAADS DAAC (https://ladsweb.modaps.eosdis.nasa.gov)
Product: MOD11A1 v061 (Terra MODIS LST, daily, 1km, 2000-present)
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

# NASA LAADS DAAC CMR API for granule discovery
CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
COLLECTION_ID = "C1621389631-LPDAAC_ETS"  # MOD11A1 v061

# Japan bounding box
JAPAN_BBOX = "120,20,155,50"  # W,S,E,N

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)


async def init_lst_table():
    """Create LST table if not exists."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS modis_lst (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                lst_kelvin REAL NOT NULL,
                quality INTEGER,
                observed_date TEXT NOT NULL,
                granule_id TEXT,
                received_at TEXT NOT NULL,
                UNIQUE(latitude, longitude, observed_date)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_modis_lst_date
            ON modis_lst(observed_date)
        """)
        await db.commit()


async def search_granules(session, date_str, bbox=JAPAN_BBOX):
    """Search CMR for MOD11A1 granules covering Japan on a date."""
    params = {
        "collection_concept_id": COLLECTION_ID,
        "temporal": f"{date_str}T00:00:00Z,{date_str}T23:59:59Z",
        "bounding_box": bbox,
        "page_size": 20,
        "sort_key": "-start_date",
    }
    try:
        async with session.get(CMR_URL, params=params, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            entries = data.get("feed", {}).get("entry", [])
            granules = []
            for e in entries:
                links = e.get("links", [])
                data_url = next(
                    (l["href"] for l in links
                     if l.get("rel") == "http://esipfed.org/ns/fedsearch/1.1/data#"
                     and l["href"].endswith(".hdf")),
                    None
                )
                if data_url:
                    granules.append({
                        "id": e.get("id", ""),
                        "title": e.get("title", ""),
                        "url": data_url,
                        "time_start": e.get("time_start", ""),
                    })
            return granules
    except Exception as ex:
        logger.warning("CMR search failed for %s: %s", date_str, ex)
        return []


async def main():
    await init_db()
    await init_lst_table()

    now = datetime.now(timezone.utc).isoformat()

    # Get dates around M6.5+ earthquakes (±7 days)
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at) FROM earthquakes "
            "WHERE magnitude >= 6.5 ORDER BY occurred_at"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT observed_date FROM modis_lst"
        )

    eq_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-7, 8):
            eq_dates.add(d + timedelta(days=offset))

    existing_dates = set(r[0] for r in existing if r[0])
    dates_to_fetch = sorted(d for d in eq_dates if d.strftime("%Y-%m-%d") not in existing_dates)

    logger.info("MODIS LST: %d dates to search (%d eq dates, %d existing)",
                len(dates_to_fetch), len(eq_dates), len(existing_dates))

    if not dates_to_fetch:
        logger.info("No new dates to fetch")
        return

    # Search CMR for granules (discovery phase only — actual HDF download
    # requires NASA Earthdata auth token which is not configured yet)
    n_found = 0
    async with aiohttp.ClientSession() as session:
        for i, date in enumerate(dates_to_fetch[:50]):  # Limit for initial discovery
            date_str = date.strftime("%Y-%m-%d")
            granules = await search_granules(session, date_str)
            if granules:
                n_found += len(granules)
                logger.info("  %s: %d granules found", date_str, len(granules))
                # Store granule metadata for later download
                async with aiosqlite.connect(DB_PATH) as db:
                    for g in granules:
                        await db.execute(
                            """INSERT OR IGNORE INTO modis_lst
                               (latitude, longitude, lst_kelvin, observed_date, granule_id, received_at)
                               VALUES (0, 0, 0, ?, ?, ?)""",
                            (date_str, g["id"], now),
                        )
                    await db.commit()
            else:
                logger.debug("  %s: no granules", date_str)

            if (i + 1) % 10 == 0:
                logger.info("  Progress: %d/%d dates, %d granules found",
                            i + 1, len(dates_to_fetch), n_found)

            await asyncio.sleep(0.3)  # Rate limit

    logger.info("MODIS LST discovery: %d granules found for %d dates", n_found, len(dates_to_fetch))
    if n_found == 0:
        logger.info("Note: Actual data download requires NASA Earthdata authentication")
        logger.info("  Register at: https://urs.earthdata.nasa.gov/")
        logger.info("  Set EARTHDATA_TOKEN environment variable")


if __name__ == "__main__":
    asyncio.run(main())
