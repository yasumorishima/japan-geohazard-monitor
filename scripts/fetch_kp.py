"""Fetch Kp index from GFZ Potsdam (1932-present, full history)."""

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

GFZ_URL = "https://kp.gfz.de/app/files/Kp_ap_Ap_SN_F107_since_1932.txt"
HOURS = [0, 3, 6, 9, 12, 15, 18, 21]
START_YEAR = 2011


MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=60, connect=30)


async def fetch_with_retry(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch URL with exponential backoff retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                resp.raise_for_status()
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt
            logger.warning("Attempt %d/%d failed (%s), retrying in %ds...", attempt, MAX_RETRIES, e, wait)
            await asyncio.sleep(wait)
    raise RuntimeError("Unreachable")


async def main():
    await init_db()
    now = datetime.now(timezone.utc).isoformat()

    async with aiohttp.ClientSession() as session:
        text = await fetch_with_retry(session, GFZ_URL)

    rows = []
    for line in text.split("\n"):
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        if len(parts) < 15:
            continue
        try:
            year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
        except ValueError:
            continue
        if year < START_YEAR:
            continue
        for i, h in enumerate(HOURS):
            try:
                kp = float(parts[7 + i])
                time_tag = f"{year:04d}-{month:02d}-{day:02d} {h:02d}:00:00.000"
                rows.append((time_tag, kp, None, None, now))
            except (ValueError, IndexError):
                continue

    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """INSERT OR IGNORE INTO geomag_kp
               (time_tag, kp, a_running, station_count, received_at)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
        await db.commit()

    logger.info("Kp fetched: %d records (%d-%d)", len(rows), START_YEAR, datetime.now().year)


if __name__ == "__main__":
    asyncio.run(main())
