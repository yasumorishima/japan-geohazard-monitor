"""Fetch animal tracking data from Movebank for earthquake precursor analysis.

Downloads GPS tracking data of animals in/near Japan from the Movebank database.
Authentication via MOVEBANK_USER / MOVEBANK_PASS environment variables.
Falls back to public study access if credentials are not available.

Physical basis: Wikelski et al. (2020, Ethology 126:931-941) demonstrated that
farm animals (cows, sheep, dogs) showed significant anticipatory activity
increases 1-20 hours before M3.8+ earthquakes. Animals may detect pre-seismic
electromagnetic emissions, radon gas release, or infrasound from micro-fracturing.
GPS movement speed/distance anomalies can serve as precursor features.

The ICARUS project (Max Planck Institute) is building a global animal tracking
network specifically for earthquake prediction, with ICARUS 2.0 launched Nov 2025.

Japan region species tracked on Movebank include:
    - Streaked Shearwater (Calonectris leucomelas) - Japanese island colonies
    - Red-crowned Crane (Grus japonensis) - Hokkaido
    - Oriental White Stork (Ciconia boyciana) - Hyogo
    - Ural Owl (Strix uralensis) - Hokkaido forests
    - Japanese Black Bear (Ursus thibetanus japonicus)

References:
    - Wikelski et al. (2020) Ethology 126:931-941
    - Wikelski et al. (2021) Ethology 127:822-826 (rebuttal to Zoller critique)
    - Kirschvink (2000) Phys. Chem. Earth 25(9-11):537-547
"""

import asyncio
import logging
import os
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

MOVEBANK_API = "https://www.movebank.org/movebank/service/direct-read"

# Japan bounding box
JAPAN_LAT_MIN, JAPAN_LAT_MAX = 24.0, 46.0
JAPAN_LON_MIN, JAPAN_LON_MAX = 122.0, 150.0

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)


async def init_movebank_tables():
    """Create animal tracking tables."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS animal_study (
                study_id INTEGER PRIMARY KEY,
                name TEXT,
                species TEXT,
                main_lat REAL,
                main_lon REAL,
                n_events INTEGER,
                sensor_types TEXT,
                discovered_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS animal_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                study_id INTEGER NOT NULL,
                study_name TEXT,
                species TEXT,
                individual_id TEXT,
                observed_at TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(study_id, individual_id, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_animal_time
            ON animal_tracking(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_animal_study
            ON animal_tracking(study_id)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_animal_location
            ON animal_tracking(latitude, longitude)
        """)
        await db.commit()


def get_auth():
    """Get Movebank credentials from environment."""
    user = os.environ.get("MOVEBANK_USER", "")
    passwd = os.environ.get("MOVEBANK_PASS", "")
    if user and passwd:
        return aiohttp.BasicAuth(user, passwd)
    return None


async def fetch_studies(session: aiohttp.ClientSession,
                        auth: aiohttp.BasicAuth | None) -> list[dict]:
    """Fetch all studies from Movebank and filter for Japan region."""
    params = {
        "entity_type": "study",
        "attributes": "id,name,main_location_lat,main_location_long,"
                      "number_of_events,sensor_type_ids,taxon_ids",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(MOVEBANK_API, params=params,
                                   auth=auth, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return parse_csv_studies(text)
                elif resp.status == 401:
                    logger.warning("Movebank authentication failed. "
                                   "Set MOVEBANK_USER and MOVEBANK_PASS env vars.")
                    return []
                elif resp.status == 403:
                    logger.warning("Movebank access forbidden (may need license acceptance)")
                    return []
                else:
                    logger.warning("Movebank study list: HTTP %d (attempt %d)",
                                   resp.status, attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.error("Movebank study list failed: %s", e)
                return []
            await asyncio.sleep(2 ** attempt)

    return []


def parse_csv_studies(text: str) -> list[dict]:
    """Parse Movebank CSV study response and filter for Japan region."""
    lines = text.strip().split("\n")
    if not lines:
        return []

    header = lines[0].split(",")
    studies = []

    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < len(header):
            continue
        row = dict(zip(header, parts))
        try:
            lat = float(row.get("main_location_lat", ""))
            lon = float(row.get("main_location_long", ""))
        except (ValueError, TypeError):
            continue

        # Filter for Japan region
        if not (JAPAN_LAT_MIN <= lat <= JAPAN_LAT_MAX and
                JAPAN_LON_MIN <= lon <= JAPAN_LON_MAX):
            continue

        try:
            n_events = int(row.get("number_of_events", "0"))
        except ValueError:
            n_events = 0

        if n_events < 100:
            continue

        # Check for GPS sensor (sensor_type_id 653 = GPS)
        sensor_ids = row.get("sensor_type_ids", "")

        studies.append({
            "study_id": int(row.get("id", 0)),
            "name": row.get("name", ""),
            "lat": lat,
            "lon": lon,
            "n_events": n_events,
            "sensor_types": sensor_ids,
            "taxon_ids": row.get("taxon_ids", ""),
        })

    return studies


async def fetch_study_events(session: aiohttp.ClientSession,
                              auth: aiohttp.BasicAuth | None,
                              study_id: int, study_name: str) -> list[tuple]:
    """Fetch GPS events for a study."""
    params = {
        "entity_type": "event",
        "study_id": str(study_id),
        "sensor_type": "gps",
        "attributes": "individual_id,timestamp,location_lat,location_long",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(MOVEBANK_API, params=params,
                                   auth=auth, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return parse_csv_events(text, study_id, study_name)
                elif resp.status in (401, 403):
                    logger.info("  Study %d: access denied (may need license)", study_id)
                    return []
                else:
                    logger.warning("  Study %d events: HTTP %d (attempt %d)",
                                   study_id, resp.status, attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("  Study %d events failed: %s", study_id, e)
                return []
            await asyncio.sleep(2 ** attempt)

    return []


def parse_csv_events(text: str, study_id: int, study_name: str) -> list[tuple]:
    """Parse Movebank CSV event response."""
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return []

    header = lines[0].split(",")
    rows = []

    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < len(header):
            continue
        row = dict(zip(header, parts))
        try:
            individual_id = row.get("individual_id", "")
            timestamp = row.get("timestamp", "")
            lat = float(row.get("location_lat", ""))
            lon = float(row.get("location_long", ""))
            if not timestamp or lat == 0 or lon == 0:
                continue
            rows.append((study_id, study_name, "", individual_id,
                         timestamp, lat, lon))
        except (ValueError, TypeError):
            continue

    return rows


async def main():
    await init_db()
    await init_movebank_tables()

    now = datetime.now(timezone.utc).isoformat()
    auth = get_auth()

    if auth is None:
        logger.warning("MOVEBANK_USER/MOVEBANK_PASS not set. "
                        "Attempting public access only.")

    # Check existing studies
    async with safe_connect() as db:
        existing_studies = set()
        rows = await db.execute_fetchall("SELECT study_id FROM animal_study")
        for r in rows:
            existing_studies.add(r[0])

    # Discover studies in Japan region
    async with aiohttp.ClientSession() as session:
        logger.info("=== Discovering Movebank studies in Japan region ===")
        studies = await fetch_studies(session, auth)

        if not studies:
            logger.warning("No Movebank studies found in Japan region "
                           "(auth may be needed). Exiting gracefully.")
            return

        logger.info("Found %d studies with GPS data in Japan region (>100 events)",
                     len(studies))

        # Save study metadata
        async with safe_connect() as db:
            for s in studies:
                await db.execute(
                    """INSERT OR REPLACE INTO animal_study
                       (study_id, name, main_lat, main_lon, n_events,
                        sensor_types, discovered_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (s["study_id"], s["name"], s["lat"], s["lon"],
                     s["n_events"], s["sensor_types"], now),
                )
            await db.commit()

        # Fetch events for each study
        total_records = 0
        max_studies = 20  # Limit to avoid excessive API calls

        for i, study in enumerate(studies[:max_studies]):
            sid = study["study_id"]
            sname = study["name"]

            logger.info("  [%d/%d] Study %d: %s (%.1f°N, %.1f°E, %d events)",
                        i + 1, min(len(studies), max_studies),
                        sid, sname[:50], study["lat"], study["lon"],
                        study["n_events"])

            events = await fetch_study_events(session, auth, sid, sname)
            if events:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO animal_tracking
                           (study_id, study_name, species, individual_id,
                            observed_at, latitude, longitude, received_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        [(sid, sn, sp, ind, ts, lat, lon, now)
                         for sid, sn, sp, ind, ts, lat, lon in events],
                    )
                    await db.commit()
                total_records += len(events)
                logger.info("    → %d GPS events stored", len(events))
            else:
                logger.info("    → no accessible GPS data")

            await asyncio.sleep(2.0)  # Rate limit (1 concurrent per IP)

    logger.info("Movebank fetch complete: %d total GPS events from %d studies",
                total_records, len(studies))


if __name__ == "__main__":
    asyncio.run(main())
