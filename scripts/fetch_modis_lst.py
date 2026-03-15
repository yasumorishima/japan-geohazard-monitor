"""Fetch MODIS Land Surface Temperature via ORNL DAAC TESViS REST API.

Extracts point LST values at earthquake epicenters AND random control
locations for ±14 days using the ORNL DAAC subset API (no auth required).

Physical basis: Stress-induced micro-fracturing → radon/CO2 release
→ surface heating → detectable thermal IR anomaly before earthquakes
(Tronin 2006, Ouzounov & Freund 2004).

API: https://modis.ornl.gov/rst/api/v1/
Product: MOD11A1 (daily) preferred, MOD11A2 (8-day composite) fallback.

Data strategy:
    1. ALL M5.5+ earthquakes on land (陸域) → epicenter ±14 days
    2. Random control locations (same lat/lon distribution, shifted ±60-180 days)
    3. Extended ±14 days to get robust seasonal baseline for anomaly detection
    4. Rate limit: 1 req/sec (ORNL fair usage)
"""

import asyncio
import json
import logging
import math
import random
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

# Japan land areas (rough polygon to skip ocean epicenters)
# MODIS LST is NaN over ocean, so only land epicenters are useful
JAPAN_LAND_BOXES = [
    (30.0, 45.6, 129.0, 146.0),  # Main islands (broad)
    (24.0, 28.0, 123.0, 131.5),  # Ryukyu / Okinawa
    (41.5, 45.5, 140.0, 145.5),  # Hokkaido
]


def is_on_land(lat: float, lon: float) -> bool:
    """Rough check if point is on Japanese land (not ocean)."""
    for min_lat, max_lat, min_lon, max_lon in JAPAN_LAND_BOXES:
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return True
    return False


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
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_modis_lst_location
            ON modis_lst(latitude, longitude)
        """)
        await db.commit()


def date_to_modis(dt: datetime) -> str:
    """Convert datetime to MODIS date format (AYYYY DDD)."""
    return f"A{dt.strftime('%Y%j')}"


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
                        # Only use LST_Day_1km or LST_Night_1km bands
                        if "LST" not in band and "lst" not in band.lower():
                            continue
                        for val in subset.get("data", []):
                            if val is not None and val > 0 and val < 65535:
                                # LST is stored as K * 50 (scale factor 0.02)
                                lst_k = val * 0.02
                                # Filter physically reasonable range (200K-350K)
                                if 200 < lst_k < 350:
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
                    logger.warning("HTTP %d for (%.2f, %.2f): %s", resp.status, lat, lon, text[:100])
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("Failed (%.2f, %.2f): %s", lat, lon, e)
                return []
            await asyncio.sleep(2 ** attempt)
    return []


async def store_results(results: list[dict], lat: float, lon: float,
                        product: str, now: str):
    """Store LST results in database."""
    if not results:
        return 0
    async with aiosqlite.connect(DB_PATH) as db:
        for r in results:
            await db.execute(
                """INSERT OR IGNORE INTO modis_lst
                   (latitude, longitude, lst_kelvin, observed_date, product, received_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (lat, lon, r["lst_kelvin"], r["date"], product, now),
            )
        await db.commit()
    return len(results)


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
                    has_daily = False
                    has_8day = True
        except Exception as e:
            logger.warning("Products check failed: %s", e)
            has_daily = False
            has_8day = True

    product = "MOD11A1" if has_daily else "MOD11A2"
    logger.info("Using product: %s", product)

    # ---------------------------------------------------------------
    # Phase 1: Earthquake epicenters (M5.5+ on land, ±14 days)
    # ---------------------------------------------------------------
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, latitude, longitude, magnitude "
            "FROM earthquakes WHERE magnitude >= 5.5 "
            "ORDER BY occurred_at"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT latitude, longitude, observed_date FROM modis_lst "
            "WHERE lst_kelvin > 0"
        )

    existing_set = set((round(r[0], 2), round(r[1], 2), r[2]) for r in existing)

    # Filter to land-based earthquakes
    land_eqs = []
    ocean_skip = 0
    for eq in eq_rows:
        lat, lon = eq[1], eq[2]
        if is_on_land(lat, lon):
            land_eqs.append(eq)
        else:
            ocean_skip += 1

    logger.info("M5.5+ total: %d, On land: %d, Ocean (skipped): %d, Existing LST: %d",
                len(eq_rows), len(land_eqs), ocean_skip, len(existing_set))

    # Fetch LST for earthquake epicenters
    eq_records = 0
    eq_fetched = 0
    async with aiohttp.ClientSession() as session:
        for i, eq in enumerate(land_eqs):
            try:
                eq_time = datetime.fromisoformat(eq[0].replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            lat, lon, mag = eq[1], eq[2], eq[3]
            start = eq_time - timedelta(days=14)
            end = eq_time + timedelta(days=14)

            # Skip if already fetched
            check_date = eq_time.strftime("%Y-%m-%d")
            if (round(lat, 2), round(lon, 2), check_date) in existing_set:
                continue

            results = await fetch_lst_point(session, product, lat, lon, start, end)
            n = await store_results(results, lat, lon, product, now)
            eq_records += n
            eq_fetched += 1

            if n > 0:
                logger.info("  [EQ] M%.1f %s (%.2f, %.2f): %d records",
                            mag, eq_time.strftime("%Y-%m-%d"), lat, lon, n)

            if (i + 1) % 20 == 0:
                logger.info("  Progress: %d/%d land events, %d fetched, %d records",
                            i + 1, len(land_eqs), eq_fetched, eq_records)

            await asyncio.sleep(1.0)  # Rate limit

    logger.info("Earthquake LST complete: %d records from %d fetches (%d land events)",
                eq_records, eq_fetched, len(land_eqs))

    # ---------------------------------------------------------------
    # Phase 2: Random control locations (same epicenters, shifted ±60-180 days)
    # ---------------------------------------------------------------
    logger.info("--- Fetching random control LST data ---")

    random.seed(42)
    ctrl_records = 0
    ctrl_fetched = 0

    # Sample up to 100 control points from the same epicenter locations
    # but shifted in time (±60 to ±180 days) to avoid contamination
    sample_eqs = land_eqs if len(land_eqs) <= 100 else random.sample(land_eqs, 100)

    async with aiohttp.ClientSession() as session:
        for i, eq in enumerate(sample_eqs):
            try:
                eq_time = datetime.fromisoformat(eq[0].replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            lat, lon = eq[1], eq[2]

            # Random time shift: 60-180 days before or after
            shift_days = random.choice([-1, 1]) * random.randint(60, 180)
            ctrl_time = eq_time + timedelta(days=shift_days)

            # Ensure within MODIS data range (2000-present)
            if ctrl_time.year < 2002 or ctrl_time.year > 2025:
                continue

            check_date = ctrl_time.strftime("%Y-%m-%d")
            if (round(lat, 2), round(lon, 2), check_date) in existing_set:
                continue

            start = ctrl_time - timedelta(days=14)
            end = ctrl_time + timedelta(days=14)

            results = await fetch_lst_point(session, product, lat, lon, start, end)
            n = await store_results(results, lat, lon, product, now)
            ctrl_records += n
            ctrl_fetched += 1

            if n > 0:
                logger.info("  [CTRL] %s (%.2f, %.2f): %d records",
                            ctrl_time.strftime("%Y-%m-%d"), lat, lon, n)

            if (i + 1) % 20 == 0:
                logger.info("  Control progress: %d/%d, %d fetched, %d records",
                            i + 1, len(sample_eqs), ctrl_fetched, ctrl_records)

            await asyncio.sleep(1.0)

    logger.info("Control LST complete: %d records from %d fetches", ctrl_records, ctrl_fetched)
    logger.info("TOTAL: %d earthquake + %d control = %d LST records",
                eq_records, ctrl_records, eq_records + ctrl_records)


if __name__ == "__main__":
    asyncio.run(main())
