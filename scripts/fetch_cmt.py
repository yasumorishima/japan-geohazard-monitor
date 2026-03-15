"""Fetch focal mechanism data from Global CMT catalog (NDK format).

Downloads GCMT solutions for Japan region (2011-present), extracts
strike/dip/rake and stores in the focal_mechanisms table.
"""

import asyncio
import logging
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

# GCMT NDK bulk file URLs
GCMT_URLS = [
    # Historical catalog 1976-2017
    "https://www.ldeo.columbia.edu/~gcmt/projects/CMT/catalog/jan76_dec17.ndk",
    # Monthly files for 2018-present (iterate by year/month)
]

# Japan bounding box (same as config.py)
JAPAN_BBOX = {"min_lat": 20.0, "max_lat": 50.0, "min_lon": 120.0, "max_lon": 155.0}
START_YEAR = 2011

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=120, connect=30)


def parse_ndk(text: str) -> list[dict]:
    """Parse GCMT NDK format into list of focal mechanism dicts.

    NDK format: 5 lines per event.
    Line 1: Hypocenter info (PDE/ISC catalog)
    Line 2: CMT info (centroid location, half-duration, etc.)
    Line 3: Moment tensor components
    Line 4: Eigenvalues/eigenvectors
    Line 5: Nodal planes (strike/dip/rake for both planes)
    """
    lines = text.strip().split("\n")
    events = []

    i = 0
    while i + 4 < len(lines):
        try:
            line1 = lines[i]      # Hypocenter reference (PDE catalog)
            line2 = lines[i + 1]  # CMT event name + data used
            line3 = lines[i + 2]  # CENTROID: time_shift, lat, lon, depth
            line4 = lines[i + 3]  # Moment tensor (exponent + 6 components)
            line5 = lines[i + 4]  # Eigenvalues + nodal planes
            i += 5

            # Line 1: hypocenter reference
            parts1 = line1.split()
            if len(parts1) < 6:
                continue

            # Line 2: CMT event name
            parts2 = line2.split()
            if len(parts2) < 2:
                continue
            event_id = parts2[0].strip()

            # Line 3: CENTROID parameters
            # Format: CENTROID: time_shift err lat err lon err depth err type timestamp
            # Example: CENTROID:     -0.3 0.9  13.76 0.06  -89.08 0.09 162.8 12.5 FREE S-...
            parts3 = line3.split()
            if len(parts3) < 8:
                continue
            try:
                # Skip "CENTROID:" prefix if present
                offset = 1 if parts3[0].upper().startswith("CENTROID") else 0
                cent_lat = float(parts3[offset + 2])   # lat
                cent_lon = float(parts3[offset + 4])   # lon
                cent_depth = float(parts3[offset + 6])  # depth
            except (ValueError, IndexError):
                continue

            # Line 4: Moment tensor exponent
            parts4 = line4.split()
            if len(parts4) < 2:
                continue
            try:
                mt_exponent = int(parts4[0])
            except ValueError:
                continue

            # Line 5: Eigenvalues + scalar moment + nodal planes
            # Format: V10 T_val T_plg T_azm N_val N_plg N_azm P_val P_plg P_azm
            #         scalar_moment NP1_strike NP1_dip NP1_rake NP2_strike NP2_dip NP2_rake
            # Indices: [0]  [1-3]      [4-6]      [7-9]
            #          [10]         [11-13]              [14-16]
            parts5 = line5.split()
            if len(parts5) < 17:
                continue

            try:
                scalar_moment_mantissa = float(parts5[10])
                strike1 = float(parts5[11])
                dip1 = float(parts5[12])
                rake1 = float(parts5[13])
                strike2 = float(parts5[14])
                dip2 = float(parts5[15])
                rake2 = float(parts5[16])

                # Validate: dip should be 0-90
                if not (0 <= dip1 <= 90 and 0 <= dip2 <= 90):
                    continue

                # Scalar moment uses same exponent as moment tensor (line 4)
                moment_dyncm = scalar_moment_mantissa * (10 ** mt_exponent)
                moment_nm = moment_dyncm * 1e-7  # dyne-cm to N-m

                import math
                mw = (2.0 / 3.0) * math.log10(max(moment_nm, 1e10)) - 6.07
            except (ValueError, IndexError):
                continue

            # Parse date from line 1
            try:
                date_str = parts1[1]  # YYYY/MM/DD
                time_str = parts1[2]  # HH:MM:SS.S
                if "/" in date_str:
                    year, month, day = date_str.split("/")
                else:
                    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
                hour, minute = time_str.split(":")[:2]
                sec_parts = time_str.split(":")[2] if len(time_str.split(":")) > 2 else "0"
                sec = int(float(sec_parts))
                occurred_at = f"{int(year):04d}-{int(month):02d}-{int(day):02d}T{int(hour):02d}:{int(minute):02d}:{sec:02d}"
            except (ValueError, IndexError):
                continue

            # Filter by year
            if int(year) < START_YEAR:
                continue

            # Filter by Japan bounding box (use centroid location)
            if not (JAPAN_BBOX["min_lat"] <= cent_lat <= JAPAN_BBOX["max_lat"] and
                    JAPAN_BBOX["min_lon"] <= cent_lon <= JAPAN_BBOX["max_lon"]):
                continue

            events.append({
                "source": "gcmt",
                "event_id": event_id,
                "occurred_at": occurred_at,
                "latitude": cent_lat,
                "longitude": cent_lon,
                "depth_km": cent_depth,
                "magnitude": round(mw, 1),
                "strike1": strike1,
                "dip1": dip1,
                "rake1": rake1,
                "strike2": strike2,
                "dip2": dip2,
                "rake2": rake2,
                "moment_nm": moment_nm,
            })

        except Exception as e:
            logger.debug("Skipping NDK block at line %d: %s", i, e)
            i += 1
            continue

    return events


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
    total = 0

    async with aiohttp.ClientSession() as session:
        # Fetch historical catalog (1976-2017)
        logger.info("Fetching GCMT historical catalog...")
        try:
            text = await fetch_with_retry(session, GCMT_URLS[0])
            events = parse_ndk(text)
            logger.info("Parsed %d events from historical catalog (Japan, %d+)", len(events), START_YEAR)
        except Exception as e:
            logger.error("Failed to fetch historical catalog: %s", e)
            events = []

        # Fetch monthly catalogs for 2018-present
        current_year = datetime.now().year
        for year in range(2018, current_year + 1):
            for month in range(1, 13):
                if year == current_year and month > datetime.now().month:
                    break
                month_name = datetime(year, month, 1).strftime("%b").lower()
                url = f"https://www.ldeo.columbia.edu/~gcmt/projects/CMT/catalog/NEW_MONTHLY/{year}/{month_name}{str(year)[2:]}.ndk"
                try:
                    text = await fetch_with_retry(session, url)
                    monthly_events = parse_ndk(text)
                    events.extend(monthly_events)
                    if monthly_events:
                        logger.info("  %s %d: %d events", month_name, year, len(monthly_events))
                except Exception:
                    # Some months may not have files yet
                    pass

    # Deduplicate by event_id
    seen = set()
    unique_events = []
    for e in events:
        if e["event_id"] not in seen:
            seen.add(e["event_id"])
            unique_events.append(e)

    logger.info("Total unique events: %d", len(unique_events))

    # Store in database
    async with aiosqlite.connect(DB_PATH) as db:
        rows = [
            (e["source"], e["event_id"], e["occurred_at"],
             e["latitude"], e["longitude"], e["depth_km"],
             e["magnitude"], e["strike1"], e["dip1"], e["rake1"],
             e["strike2"], e["dip2"], e["rake2"], e["moment_nm"], now)
            for e in unique_events
        ]
        await db.executemany(
            """INSERT OR IGNORE INTO focal_mechanisms
               (source, event_id, occurred_at, latitude, longitude, depth_km,
                magnitude, strike1, dip1, rake1, strike2, dip2, rake2,
                moment_nm, received_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        await db.commit()
        total = len(rows)

    logger.info("CMT data stored: %d focal mechanisms (Japan, %d-%d)", total, START_YEAR, current_year)


if __name__ == "__main__":
    asyncio.run(main())
