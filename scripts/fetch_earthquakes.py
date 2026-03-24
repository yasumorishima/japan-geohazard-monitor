"""Fetch all M3+ earthquakes in Japan region (2011-2026) from USGS."""

import asyncio
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

JAPAN_BBOX = "minlatitude=20&maxlatitude=50&minlongitude=120&maxlongitude=155"


MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=60, connect=30)


async def fetch_json_with_retry(session: aiohttp.ClientSession, url: str):
    """Fetch JSON with exponential backoff retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    logger.warning("HTTP %d for %s", resp.status, url[:80])
                    if attempt == MAX_RETRIES:
                        return None
                    await asyncio.sleep(2 ** attempt)
                    continue
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.error("Failed after %d attempts: %s", MAX_RETRIES, e)
                return None
            wait = 2 ** attempt
            logger.warning("Attempt %d/%d failed (%s), retrying in %ds...", attempt, MAX_RETRIES, e, wait)
            await asyncio.sleep(wait)
    return None


async def main():
    await init_db()
    now = datetime.now(timezone.utc).isoformat()
    total = 0

    async with aiohttp.ClientSession() as session:
        for year in range(2011, datetime.now().year + 1):
            start = f"{year}-01-01"
            end = f"{year + 1}-01-01" if year < datetime.now().year else datetime.now().strftime("%Y-%m-%d")

            for min_m, max_m in [(3.0, 5.0), (5.0, 10.0)]:
                url = (
                    f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
                    f"&starttime={start}&endtime={end}"
                    f"&minmagnitude={min_m}&maxmagnitude={max_m}"
                    f"&{JAPAN_BBOX}&orderby=time&limit=20000"
                )
                data = await fetch_json_with_retry(session, url)
                if data is None:
                    continue

                rows = []
                for f in data["features"]:
                    p = f["properties"]
                    c = f["geometry"]["coordinates"]
                    rows.append((
                        "usgs", p.get("code", f["id"]),
                        datetime.fromtimestamp(p["time"] / 1000, tz=timezone.utc).isoformat(),
                        c[1], c[0], c[2] if len(c) > 2 else None,
                        p.get("mag"), p.get("magType"), None, None, p.get("place"), now,
                    ))

                if rows:
                    async with safe_connect() as db:
                        await db.executemany(
                            """INSERT OR IGNORE INTO earthquakes
                               (source, event_id, occurred_at, latitude, longitude, depth_km,
                                magnitude, magnitude_type, max_intensity,
                                location_ja, location_en, received_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            rows,
                        )
                        await db.commit()
                    total += len(rows)

            logger.info("%d: done", year)

    logger.info("Total earthquakes fetched: %d", total)


if __name__ == "__main__":
    asyncio.run(main())
