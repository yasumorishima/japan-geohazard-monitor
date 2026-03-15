"""Fetch MODIS Land Surface Temperature via ORNL DAAC TESViS REST API.

Extracts point LST values at earthquake epicenters for ±7 days using
the ORNL DAAC subset API (no authentication required, synchronous).

Physical basis: Stress-induced micro-fracturing → radon/gas release
→ surface heating → detectable thermal IR anomaly before earthquakes
(Tronin 2006, Ouzounov & Freund 2004).

API: https://modis.ornl.gov/rst/api/v1/
Product: MOD11A2 (8-day composite) or MOD11A1 (daily) if available
"""

import asyncio
import json
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

ORNL_API_BASE = "https://modis.ornl.gov/rst/api/v1"
TIMEOUT = aiohttp.ClientTimeout(total=60, connect=30)
MAX_RETRIES = 3


async def init_lst_table():
    """Create LST table if not exists."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS modis_lst (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                lst_kelvin REAL NOT NULL,
                lst_day_kelvin REAL,
                lst_night_kelvin REAL,
                quality INTEGER,
                observed_date TEXT NOT NULL,
                product TEXT,
                received_at TEXT NOT NULL,
                UNIQUE(latitude, longitude, observed_date)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_modis_lst_date
            ON modis_lst(observed_date)
        """)
        await db.commit()


def date_to_modis(dt: datetime) -> str:
    """Convert datetime to MODIS date format (AYYYY DDD)."""
    return f"A{dt.strftime('%Y%j')}"


def modis_to_date(modis_date: str) -> str:
    """Convert MODIS date (AYYYY DDD) to YYYY-MM-DD."""
    year = int(modis_date[1:5])
    doy = int(modis_date[5:])
    dt = datetime(year, 1, 1) + timedelta(days=doy - 1)
    return dt.strftime("%Y-%m-%d")


async def fetch_lst_point(session: aiohttp.ClientSession,
                          product: str, lat: float, lon: float,
                          start_date: datetime, end_date: datetime) -> list[dict]:
    """Fetch LST for a single point from ORNL DAAC API."""
    url = f"{ORNL_API_BASE}/{product}/subset"
    params = {
        "latitude": lat,
        "longitude": lon,
        "startDate": date_to_modis(start_date),
        "endDate": date_to_modis(end_date),
        "kmAboveBelow": 0,
        "kmLeftRight": 0,
    }
    headers = {"Accept": "application/json"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, params=params, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = []
                    for subset in data.get("subset", []):
                        band = subset.get("band", "")
                        for i, val in enumerate(subset.get("data", [])):
                            if val is not None and val > 0 and val < 65535:
                                # LST is stored as K * 50 (scale factor 0.02)
                                lst_k = val * 0.02
                                calendar_date = subset.get("calendar_date", "")
                                if calendar_date:
                                    results.append({
                                        "date": calendar_date,
                                        "band": band,
                                        "lst_kelvin": round(lst_k, 2),
                                    })
                    return results
                elif resp.status == 404:
                    return []
                else:
                    text = await resp.text()
                    logger.warning("HTTP %d for %s: %s", resp.status, url, text[:100])
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("Failed: %s", e)
                return []
            await asyncio.sleep(2 ** attempt)
    return []


async def main():
    await init_db()
    await init_lst_table()
    now = datetime.now(timezone.utc).isoformat()

    # Check which products are available
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{ORNL_API_BASE}/products", timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    products = await resp.json()
                    product_ids = [p.get("product", "") for p in products.get("products", [])]
                    has_daily = "MOD11A1" in product_ids
                    has_8day = "MOD11A2" in product_ids
                    logger.info("Available: MOD11A1(daily)=%s, MOD11A2(8-day)=%s", has_daily, has_8day)
                else:
                    logger.warning("Products endpoint returned %d", resp.status)
                    has_daily = False
                    has_8day = True  # Assume 8-day is available
        except Exception as e:
            logger.warning("Products check failed: %s", e)
            has_daily = False
            has_8day = True

    product = "MOD11A1" if has_daily else "MOD11A2"
    logger.info("Using product: %s", product)

    # Get M6.5+ earthquake epicenters
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, latitude, longitude, magnitude "
            "FROM earthquakes WHERE magnitude >= 6.5 "
            "ORDER BY occurred_at"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT latitude, longitude, observed_date FROM modis_lst "
            "WHERE lst_kelvin > 0"
        )

    existing_set = set((round(r[0], 2), round(r[1], 2), r[2]) for r in existing)
    logger.info("M6.5+ events: %d, Existing LST records: %d", len(eq_rows), len(existing_set))

    # Fetch LST for each epicenter ±7 days
    total_records = 0
    async with aiohttp.ClientSession() as session:
        for i, eq in enumerate(eq_rows[:30]):  # Limit for initial run
            try:
                eq_time = datetime.fromisoformat(eq[0].replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            lat, lon, mag = eq[1], eq[2], eq[3]
            start = eq_time - timedelta(days=7)
            end = eq_time + timedelta(days=7)

            # Skip if we already have data for this location/period
            check_date = eq_time.strftime("%Y-%m-%d")
            if (round(lat, 2), round(lon, 2), check_date) in existing_set:
                continue

            results = await fetch_lst_point(session, product, lat, lon, start, end)
            if results:
                async with aiosqlite.connect(DB_PATH) as db:
                    for r in results:
                        await db.execute(
                            """INSERT OR IGNORE INTO modis_lst
                               (latitude, longitude, lst_kelvin, observed_date, product, received_at)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            (lat, lon, r["lst_kelvin"], r["date"], product, now),
                        )
                    await db.commit()
                total_records += len(results)
                logger.info("  M%.1f (%s): %d LST records", mag, eq_time.strftime("%Y-%m-%d"), len(results))

            if (i + 1) % 10 == 0:
                logger.info("  Progress: %d/%d events, %d records", i + 1, len(eq_rows), total_records)

            await asyncio.sleep(1.0)  # Rate limit (ORNL asks for reasonable usage)

    logger.info("MODIS LST fetch complete: %d records from %d events", total_records, len(eq_rows))


if __name__ == "__main__":
    asyncio.run(main())
