"""Fetch GRACE/GRACE-FO gravity anomaly data.

GRACE (2002-2017) and GRACE-FO (2018-present) twin satellites measure
Earth's gravity field changes caused by mass redistribution. Monthly
solutions detect water mass, ice mass, and — critically — tectonic
mass changes at plate boundaries.

Physical mechanism:
    Pre-seismic fluid migration in the crust causes detectable gravity
    changes. Documented gravity anomalies were observed before the 2011
    Tohoku M9.0 earthquake (Matsuo & Heki, 2011). Monthly resolution
    limits detection of short-term precursors, but captures slow strain
    accumulation over subduction zones.

Data source: JPL GRACE/GRACE-FO Mascon (RL06.3v04)
    - Monthly liquid water equivalent (LWE) thickness
    - 0.5-degree global grid
    - Available via PO.DAAC OPeNDAP (Earthdata auth) or JPL TELLUS
    - Gap: June 2017 - May 2018 (between GRACE and GRACE-FO)

Target features:
    - gravity_anomaly_rate: month-to-month LWE change rate per cell (cm/month)

References:
    - Matsuo & Heki (2011) Geophys. Res. Lett. 38:L17312
    - Panet et al. (2018) Nat. Geosci. 11:611-615
"""

import asyncio
import logging
import os
import re
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

# JPL TELLUS Mascon via PO.DAAC OPeNDAP
# Returns ASCII when .ascii is appended
GRACE_OPENDAP_BASE = "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections"

# Alternative: direct download from JPL TELLUS (requires Earthdata login)
GRACE_TELLUS_URL = (
    "https://podaac-tools.jpl.nasa.gov/drive/files/allData/"
    "tellus/L3/mascon/RL06.3/v04/CRI/"
)

EARTHDATA_TOKEN = os.environ.get("EARTHDATA_TOKEN")

# Japan bbox (0.5-degree grid subset)
# Lat: 24-46, Lon: 122-150
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 150.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=600, connect=60)


async def init_gravity_table():
    """Create gravity anomaly table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS gravity_mascon (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                lwe_thickness_cm REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_gravity_time
            ON gravity_mascon(observed_at)
        """)
        await db.commit()


async def fetch_grace_via_tellus_ascii(session: aiohttp.ClientSession) -> list[dict]:
    """Attempt to fetch GRACE data via JPL TELLUS text summary.

    JPL provides monthly gravity change summaries. If the main OPeNDAP
    approach fails, we try the simpler TELLUS API.
    """
    # JPL GRACE mascon data — try the simple REST endpoint
    url = (
        "https://podaac-tools.jpl.nasa.gov/drive/files/allData/"
        "tellus/L3/mascon/RL06.3/v04/CRI/mascon_summary.txt"
    )
    headers = {}
    if EARTHDATA_TOKEN:
        headers["Authorization"] = f"Bearer {EARTHDATA_TOKEN}"

    try:
        async with session.get(url, timeout=TIMEOUT, headers=headers) as resp:
            if resp.status == 200:
                text = await resp.text()
                return _parse_mascon_summary(text)
            elif resp.status in (401, 403):
                logger.info("GRACE TELLUS requires Earthdata auth (HTTP %d)", resp.status)
            else:
                logger.warning("GRACE TELLUS HTTP %d", resp.status)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning("GRACE TELLUS failed: %s", type(e).__name__)

    return []


def _parse_mascon_summary(text: str) -> list[dict]:
    """Parse GRACE mascon summary text (if available)."""
    # Format varies — attempt common patterns
    rows = []
    for line in text.split("\n"):
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            lat = float(parts[0])
            lon = float(parts[1])
            date_str = parts[2]
            lwe = float(parts[3])

            if (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                    and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
                rows.append({
                    "date": date_str,
                    "lat": round(lat, 2),
                    "lon": round(lon, 2),
                    "lwe": round(lwe, 4),
                })
        except (ValueError, IndexError):
            continue
    return rows


async def fetch_grace_monthly_json(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch GRACE monthly solutions via GFZ ICGEM or alternative JSON API.

    Uses the GFZ Helmholtz Centre ISDC (Information System and Data Center)
    which provides GRACE/GRACE-FO solutions in various formats.
    """
    # GFZ ISDC catalog for GRACE-FO Level-3 mascon
    catalog_url = (
        "https://isdc.gfz-potsdam.de/grace-fo-isdc/grace-fo-gravity-field/"
    )

    # Try simple approach: JPL GRACE Tellus REST endpoint
    # These endpoints return monthly average LWE for specific regions
    region_url = (
        "https://grace.jpl.nasa.gov/data/get-data/"
        "monthly-mass-grids-land/ascii/"
    )

    headers = {}
    if EARTHDATA_TOKEN:
        headers["Authorization"] = f"Bearer {EARTHDATA_TOKEN}"

    for url in [region_url, catalog_url]:
        try:
            async with session.get(url, timeout=TIMEOUT, headers=headers,
                                    allow_redirects=True) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    if "json" in content_type:
                        data = await resp.json()
                        return _parse_grace_json(data)
                    elif "text" in content_type:
                        text = await resp.text()
                        # Check for redirects to login page
                        if "<html" in text.lower()[:200]:
                            logger.info("GRACE endpoint returned HTML (auth page)")
                            continue
                        return _parse_mascon_summary(text)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            continue

    return []


def _parse_grace_json(data) -> list[dict]:
    """Parse GRACE JSON response."""
    rows = []
    if isinstance(data, list):
        for item in data:
            try:
                lat = float(item.get("lat", item.get("latitude", 0)))
                lon = float(item.get("lon", item.get("longitude", 0)))
                lwe = float(item.get("lwe", item.get("lwe_thickness", 0)))
                date_str = item.get("time", item.get("date", ""))

                if (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                        and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
                    rows.append({
                        "date": str(date_str)[:10],
                        "lat": round(lat, 2),
                        "lon": round(lon, 2),
                        "lwe": round(lwe, 4),
                    })
            except (ValueError, TypeError):
                continue
    return rows


async def main():
    await init_db()
    await init_gravity_table()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN:
        logger.info(
            "GRACE fetch: EARTHDATA_TOKEN not set. "
            "Set EARTHDATA_TOKEN env var for GRACE/GRACE-FO data access. "
            "Generate token at https://urs.earthdata.nasa.gov/"
        )

    async with aiohttp.ClientSession() as session:
        # Try multiple approaches
        rows = await fetch_grace_via_tellus_ascii(session)

        if not rows:
            rows = await fetch_grace_monthly_json(session)

        if not rows:
            logger.info(
                "GRACE data not retrieved. This is expected without EARTHDATA_TOKEN. "
                "Monthly gravity anomaly features will be excluded via dynamic selection."
            )
            return

        # Store
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executemany(
                """INSERT OR IGNORE INTO gravity_mascon
                   (observed_at, cell_lat, cell_lon, lwe_thickness_cm, received_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [(r["date"], r["lat"], r["lon"], r["lwe"], now) for r in rows],
            )
            await db.commit()

        logger.info("GRACE fetch complete: %d records stored", len(rows))


if __name__ == "__main__":
    asyncio.run(main())
