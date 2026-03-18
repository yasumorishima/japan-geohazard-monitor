"""Fetch soil moisture data from NASA SMAP.

SMAP (Soil Moisture Active Passive) measures surface soil moisture
using an L-band (1.4 GHz) radiometer at ~9 km resolution.

Physical mechanism:
    Crustal strain changes pore pressure in shallow rock and soil,
    altering soil moisture content near active faults. This is the
    same mechanism as groundwater level changes (a classic earthquake
    precursor), but measurable from space without ground stations.

    Liquefaction-prone areas may show anomalous moisture content before
    earthquakes due to pore pressure build-up. Japan's alluvial plains
    along fault zones are ideal test areas.

Data source: NASA SMAP L3 Radiometer Global Daily Soil Moisture (SPL3SMP)
    - 36 km EASE-Grid 2.0, daily, 2015-present
    - Requires Earthdata login (free)
    - Alternative: ESA CCI Soil Moisture (1979-present, 0.25°)

Target features:
    - soil_moisture_anomaly: deviation from 30-day baseline (σ)

References:
    - Nissen et al. (2014) Geophys. Res. Lett. 41:6621-6628
    - Wegnüller et al. (2020) Remote Sensing 12:2895
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

# ESA CCI Soil Moisture (alternative, longer history)
# Available as NetCDF from Climate Data Store or OPeNDAP
ESA_CCI_OPENDAP = "https://www.esa-soilmoisture-cci.org/data"

# NASA AppEEARS API (RESTful, returns JSON/CSV, no HDF5 needed)
APPEEARS_API = "https://appeears.earthdatacloud.nasa.gov/api"

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)

# Japan bbox
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 150.0


async def init_soil_moisture_table():
    """Create soil moisture table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS soil_moisture (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                sm_m3m3 REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sm_time
            ON soil_moisture(observed_at)
        """)
        await db.commit()


async def submit_appeears_task(session: aiohttp.ClientSession,
                                 start_date: str, end_date: str) -> str | None:
    """Submit an AppEEARS area extraction task for SMAP soil moisture.

    AppEEARS is NASA's application for extracting and exploring
    remote sensing data. It handles the HDF5 processing server-side
    and returns CSV/JSON.

    Returns task_id if successful, None otherwise.
    """
    if not EARTHDATA_TOKEN:
        return None

    headers = {
        "Authorization": f"Bearer {EARTHDATA_TOKEN}",
        "Content-Type": "application/json",
    }

    # Task payload for SMAP L3 soil moisture
    task = {
        "task_type": "area",
        "task_name": f"geohazard_smap_{start_date}_{end_date}",
        "params": {
            "dates": [{"startDate": start_date, "endDate": end_date}],
            "layers": [
                {
                    "product": "SPL3SMP.009",
                    "layer": "Soil_Moisture_Retrieval_Data_AM_soil_moisture",
                }
            ],
            "geo": {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [JAPAN_LON_MIN, JAPAN_LAT_MIN],
                        [JAPAN_LON_MAX, JAPAN_LAT_MIN],
                        [JAPAN_LON_MAX, JAPAN_LAT_MAX],
                        [JAPAN_LON_MIN, JAPAN_LAT_MAX],
                        [JAPAN_LON_MIN, JAPAN_LAT_MIN],
                    ]],
                },
            },
            "output": {"format": {"type": "csv"}},
        },
    }

    try:
        async with session.post(f"{APPEEARS_API}/task", json=task,
                                 headers=headers, timeout=TIMEOUT) as resp:
            if resp.status in (200, 201, 202):
                data = await resp.json()
                task_id = data.get("task_id")
                logger.info("AppEEARS task submitted: %s", task_id)
                return task_id
            elif resp.status in (401, 403):
                logger.info("AppEEARS requires valid Earthdata token (HTTP %d)", resp.status)
            else:
                logger.warning("AppEEARS submit HTTP %d", resp.status)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning("AppEEARS submit failed: %s", type(e).__name__)

    return None


async def check_appeears_task(session: aiohttp.ClientSession, task_id: str) -> str:
    """Check AppEEARS task status.

    Returns: 'processing', 'done', 'error', or 'unknown'.
    """
    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
    try:
        async with session.get(f"{APPEEARS_API}/task/{task_id}",
                                headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("status", "unknown")
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass
    return "unknown"


async def download_appeears_result(session: aiohttp.ClientSession,
                                     task_id: str) -> list[dict]:
    """Download completed AppEEARS task results as CSV."""
    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
    rows = []

    try:
        # List files
        async with session.get(f"{APPEEARS_API}/bundle/{task_id}",
                                headers=headers, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                return []
            bundle = await resp.json()

        # Download CSV files
        for file_info in bundle.get("files", []):
            if not file_info.get("file_name", "").endswith(".csv"):
                continue

            file_id = file_info["file_id"]
            async with session.get(
                f"{APPEEARS_API}/bundle/{task_id}/{file_id}",
                headers=headers, timeout=TIMEOUT,
            ) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    rows.extend(_parse_appeears_csv(text))

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning("AppEEARS download failed: %s", type(e).__name__)

    return rows


def _parse_appeears_csv(text: str) -> list[dict]:
    """Parse AppEEARS CSV output."""
    import csv
    import io

    rows = []
    reader = csv.DictReader(io.StringIO(text))

    for row in reader:
        try:
            lat = float(row.get("Latitude", 0))
            lon = float(row.get("Longitude", 0))
            sm = float(row.get("Soil_Moisture_Retrieval_Data_AM_soil_moisture", -9999))
            date_str = row.get("Date", "")

            if sm < 0 or sm > 1:
                continue  # Invalid/fill value

            rows.append({
                "date": date_str[:10],
                "lat": round(lat, 3),
                "lon": round(lon, 3),
                "sm": round(sm, 4),
            })
        except (ValueError, TypeError):
            continue

    return rows


async def main():
    await init_db()
    await init_soil_moisture_table()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN:
        logger.info(
            "SMAP fetch: EARTHDATA_TOKEN not set. "
            "Set EARTHDATA_TOKEN env var for SMAP soil moisture access. "
            "Generate token at https://urs.earthdata.nasa.gov/ "
            "Soil moisture features will be excluded via dynamic selection."
        )
        return

    # Check existing
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(DISTINCT observed_at) FROM soil_moisture"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    logger.info("Soil moisture existing: latest=%s", last_date)

    # Submit AppEEARS task for recent data
    start = "2015-04-01" if not last_date else last_date
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async with aiohttp.ClientSession() as session:
        task_id = await submit_appeears_task(session, start, end)

        if not task_id:
            logger.info(
                "Could not submit AppEEARS task. "
                "SMAP soil moisture will be excluded via dynamic selection."
            )
            return

        # Check status (AppEEARS tasks are async, may take minutes to hours)
        # We just submit and check — if not ready, next run picks it up
        await asyncio.sleep(5)
        status = await check_appeears_task(session, task_id)
        logger.info("AppEEARS task %s status: %s", task_id, status)

        if status == "done":
            rows = await download_appeears_result(session, task_id)
            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO soil_moisture
                           (observed_at, cell_lat, cell_lon, sm_m3m3, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["date"], r["lat"], r["lon"], r["sm"], now) for r in rows],
                    )
                    await db.commit()
                logger.info("SMAP: stored %d records", len(rows))
            else:
                logger.info("SMAP: no records in AppEEARS result")
        else:
            logger.info(
                "AppEEARS task submitted but not yet complete (status=%s). "
                "Results will be available in next run.", status
            )


if __name__ == "__main__":
    asyncio.run(main())
