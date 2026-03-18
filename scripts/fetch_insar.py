"""Fetch InSAR ground deformation data from COMET LiCSAR.

InSAR (Interferometric Synthetic Aperture Radar) measures ground
deformation at mm-scale precision using satellite radar. Unlike GEONET
GPS (point measurements at 1,300 stations), InSAR provides continuous
spatial coverage of the entire Japan region.

Physical mechanism:
    Pre-seismic slow slip, aseismic creep, and strain accumulation
    produce subtle ground deformation (mm-cm) detectable by InSAR.
    GEONET GPS captures this at station locations, but InSAR fills
    the gaps between stations — especially offshore areas near
    subduction zones.

Data source: COMET LiCSAR (Looking Into Continents from Space with
    Synthetic Aperture Radar)
    - Pre-processed Sentinel-1 interferograms for tectonic regions
    - Free access, no authentication required
    - Covers Japan since Sentinel-1 launch (2014)

    Alternative: ASF DAAC (Alaska Satellite Facility) for raw Sentinel-1
    - Requires Earthdata login for SLC products
    - Processing requires ISCE/SNAP (heavy computation)

Target features:
    - insar_deformation_rate: LOS velocity anomaly per cell (mm/year deviation)

References:
    - Bürgmann et al. (2000) Ann. Rev. Earth Planet. Sci. 28:169-209
    - Lazecký et al. (2020) Remote Sensing 12:2430 (LiCSAR system)
"""

import asyncio
import logging
import os
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

# COMET LiCSAR portal
LICSAR_BASE = "https://comet.nerc.ac.uk/comet-lics-portal"
LICSAR_API = "https://gws-access.jasmin.ac.uk/public/nceo_geohazards/LiCSAR_products"

# Japan frames (Sentinel-1 tracks covering Japan)
# Major descending tracks over Japan: 046D, 047D, 048D, etc.
# These track IDs need verification from LiCSAR catalog
JAPAN_TRACKS = ["046D", "047D", "048D", "127A", "128A", "129A"]

# Japan bbox
JAPAN_LAT_MIN = 24.0
JAPAN_LAT_MAX = 46.0
JAPAN_LON_MIN = 122.0
JAPAN_LON_MAX = 150.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)


async def init_insar_table():
    """Create InSAR deformation table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS insar_deformation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                frame_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                los_velocity_mm_yr REAL,
                coherence REAL,
                received_at TEXT NOT NULL,
                UNIQUE(frame_id, observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_insar_time
            ON insar_deformation(observed_at)
        """)
        await db.commit()


async def fetch_licsar_catalog(session: aiohttp.ClientSession) -> list[dict]:
    """Query LiCSAR catalog for Japan frames.

    LiCSAR provides pre-processed interferograms organized by frame ID.
    Each frame covers ~250km x 250km.
    """
    # Try LiCSAR portal API
    catalog_urls = [
        f"{LICSAR_BASE}/api/frames?minlat={JAPAN_LAT_MIN}&maxlat={JAPAN_LAT_MAX}"
        f"&minlon={JAPAN_LON_MIN}&maxlon={JAPAN_LON_MAX}",
        f"{LICSAR_API}/",
    ]

    for url in catalog_urls:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        content_type = resp.headers.get("Content-Type", "")
                        if "json" in content_type:
                            data = await resp.json()
                            frames = _parse_licsar_frames(data)
                            if frames:
                                return frames
                        elif "html" in content_type:
                            text = await resp.text()
                            frames = _parse_licsar_html(text)
                            if frames:
                                return frames
                    elif resp.status == 404:
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == MAX_RETRIES:
                    logger.debug("LiCSAR catalog: %s", type(e).__name__)
                await asyncio.sleep(2 ** attempt)

    return []


def _parse_licsar_frames(data) -> list[dict]:
    """Parse LiCSAR JSON frame catalog."""
    frames = []
    if isinstance(data, list):
        for item in data:
            frame_id = item.get("frame_id", item.get("id", ""))
            if frame_id:
                frames.append({
                    "frame_id": frame_id,
                    "track": item.get("track", ""),
                    "direction": item.get("direction", ""),
                })
    elif isinstance(data, dict):
        for key, val in data.items():
            if isinstance(val, dict):
                frames.append({"frame_id": key, **val})
    return frames


def _parse_licsar_html(text: str) -> list[dict]:
    """Parse LiCSAR HTML directory listing for frame IDs."""
    import re
    frames = []
    # Look for frame IDs in directory listing
    for match in re.finditer(r'href="(\d{3}[AD]_\d{5}_\d{6})/"', text):
        frames.append({"frame_id": match.group(1)})
    return frames


async def fetch_frame_velocity(session: aiohttp.ClientSession,
                                 frame_id: str) -> list[dict]:
    """Fetch cumulative velocity map for a LiCSAR frame.

    LiCSAR velocity maps are GeoTIFF files — heavy to process without
    GDAL. We try to get metadata/summary data instead.
    """
    # Try getting the velocity metadata
    url = f"{LICSAR_API}/{frame_id}/metadata/vel.csv"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return _parse_velocity_csv(text, frame_id)
                elif resp.status == 404:
                    return []
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == MAX_RETRIES:
                return []
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_velocity_csv(text: str, frame_id: str) -> list[dict]:
    """Parse LiCSAR velocity CSV (if available)."""
    rows = []
    for line in text.split("\n"):
        parts = line.split(",")
        if len(parts) < 3:
            continue
        try:
            lon = float(parts[0])
            lat = float(parts[1])
            vel = float(parts[2])

            if (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX
                    and JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
                rows.append({
                    "frame_id": frame_id,
                    "lat": round(lat, 3),
                    "lon": round(lon, 3),
                    "velocity": round(vel, 2),
                })
        except (ValueError, IndexError):
            continue
    return rows


async def main():
    await init_db()
    await init_insar_table()

    now = datetime.now(timezone.utc).isoformat()

    logger.info("=== InSAR Deformation Fetch (LiCSAR) ===")

    async with aiohttp.ClientSession() as session:
        # Query catalog for Japan frames
        frames = await fetch_licsar_catalog(session)

        if not frames:
            logger.info(
                "LiCSAR catalog query returned no Japan frames. "
                "InSAR data may require manual frame ID identification. "
                "InSAR features will be excluded via dynamic selection."
            )
            return

        logger.info("Found %d LiCSAR frames for Japan", len(frames))

        total_records = 0
        for frame_info in frames[:20]:  # Cap per run
            frame_id = frame_info["frame_id"]
            rows = await fetch_frame_velocity(session, frame_id)

            if rows:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO insar_deformation
                           (frame_id, observed_at, cell_lat, cell_lon,
                            los_velocity_mm_yr, coherence, received_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        [(r["frame_id"], now[:10], r["lat"], r["lon"],
                          r["velocity"], None, now) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)
                logger.info("  %s: %d records", frame_id, len(rows))

            await asyncio.sleep(1.0)

        logger.info("InSAR fetch complete: %d records from %d frames",
                    total_records, len(frames))


if __name__ == "__main__":
    asyncio.run(main())
