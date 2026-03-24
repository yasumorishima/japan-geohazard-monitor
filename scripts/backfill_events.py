"""Backfill historical data around major Japanese earthquakes.

Downloads TEC (IONEX), earthquake, and Kp data for ±7 days around
each M6.5+ event in Japan (2020-2026). Run inside Docker container
or directly on RPi5.

Usage:
    python scripts/backfill_events.py
"""

import asyncio
import gzip
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import DB_PATH
from db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Key M6.5+ earthquakes in/near Japan
EVENTS = [
    ("2020-02-13", 7.0, "Kuril Islands"),
    ("2021-02-13", 7.1, "Fukushima"),
    ("2021-03-20", 7.0, "Miyagi"),
    ("2021-05-01", 6.9, "Miyagi"),
    ("2022-03-16", 7.3, "Fukushima"),
    ("2024-01-01", 7.5, "Noto Peninsula"),
    ("2024-04-02", 7.4, "Taiwan Hualien"),
    ("2024-08-08", 7.1, "Hyuganada Sea"),
    ("2025-01-13", 6.8, "Miyazaki"),
    ("2025-11-09", 6.8, "Iwate"),
    ("2025-12-08", 7.6, "Aomori Prefecture"),
    ("2025-12-12", 6.7, "Kuji"),
]

WINDOW_DAYS = 7  # ±7 days around each event

# Japan bounding box
JAPAN_BBOX = "minlatitude=20&maxlatitude=50&minlongitude=120&maxlongitude=155"

# TEC Japan region filter
TEC_LAT_MIN, TEC_LAT_MAX = 25.0, 45.0
TEC_LON_MIN, TEC_LON_MAX = 125.0, 150.0


async def backfill_earthquakes(session: aiohttp.ClientSession, db: aiosqlite.Connection):
    """Backfill earthquake data from USGS for each event window."""
    logger.info("=== Backfilling earthquakes ===")
    total = 0

    for date_str, mag, place in EVENTS:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        start = (dt - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=WINDOW_DAYS + 1)).strftime("%Y-%m-%d")

        url = (
            f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
            f"&starttime={start}&endtime={end}&{JAPAN_BBOX}&orderby=time"
        )

        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("USGS failed for %s: %d", date_str, resp.status)
                continue
            data = await resp.json()

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for f in data["features"]:
            p = f["properties"]
            c = f["geometry"]["coordinates"]
            rows.append((
                "usgs", p.get("code", f["id"]),
                datetime.fromtimestamp(p["time"] / 1000, tz=timezone.utc).isoformat(),
                c[1], c[0], c[2] if len(c) > 2 else None,
                p.get("mag"), p.get("magType"), None,
                None, p.get("place"), now,
            ))

        if rows:
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
            logger.info("  M%.1f %s (%s): %d earthquakes", mag, date_str, place, len(rows))

    logger.info("Earthquakes backfilled: %d total", total)


async def backfill_tec(session: aiohttp.ClientSession, db: aiosqlite.Connection):
    """Backfill TEC data from CODE IONEX files for each event window."""
    logger.info("=== Backfilling TEC (IONEX) ===")

    # Collect unique (year, doy) pairs
    file_dates = set()
    for date_str, _, _ in EVENTS:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        for offset in range(-WINDOW_DAYS, WINDOW_DAYS + 1):
            d = dt + timedelta(days=offset)
            file_dates.add((d.year, d.timetuple().tm_yday, d))

    total_records = 0

    for year, doy, d in sorted(file_dates):
        # Try final product first, then predicted
        url = (
            f"http://ftp.aiub.unibe.ch/CODE/{year}/"
            f"COD0OPSFIN_{year}{doy:03d}0000_01D_01H_GIM.INX.gz"
        )

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.debug("  TEC %s (DOY %03d): not found", d.strftime("%Y-%m-%d"), doy)
                    continue
                compressed = await resp.read()
        except Exception as e:
            logger.debug("  TEC %s: %s", d.strftime("%Y-%m-%d"), e)
            continue

        content = gzip.decompress(compressed).decode("ascii", errors="ignore")
        records = _parse_ionex_japan(content)

        if records:
            now = datetime.now(timezone.utc).isoformat()
            rows = [
                (r["lat"], r["lon"], r["tec"], r["epoch"], "final", now)
                for r in records
            ]
            await db.executemany(
                """INSERT OR IGNORE INTO tec
                   (latitude, longitude, tec_tecu, epoch, product_type, received_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                rows,
            )
            await db.commit()
            total_records += len(rows)
            logger.info("  TEC %s (DOY %03d): %d points", d.strftime("%Y-%m-%d"), doy, len(rows))

    logger.info("TEC backfilled: %d total records", total_records)


async def backfill_kp(session: aiohttp.ClientSession, db: aiosqlite.Connection):
    """Backfill Kp index from GFZ Potsdam for each event window."""
    logger.info("=== Backfilling Kp (GFZ Potsdam) ===")

    total = 0
    for date_str, mag, place in EVENTS:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        start = (dt - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=WINDOW_DAYS + 1)).strftime("%Y-%m-%d")

        # GFZ Kp index API
        url = (
            f"https://kp.gfz-potsdam.de/app/json/"
            f"?start={start}T00:00:00Z&end={end}T00:00:00Z"
        )

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("  Kp %s: HTTP %d", date_str, resp.status)
                    continue
                data = await resp.json()
        except Exception as e:
            logger.warning("  Kp %s: %s", date_str, e)
            continue

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        datetimes = data.get("datetime", [])
        kp_values = data.get("Kp", [])

        for t, kp in zip(datetimes, kp_values):
            if kp is not None:
                rows.append((t, float(kp), None, None, now))

        if rows:
            await db.executemany(
                """INSERT OR IGNORE INTO geomag_kp
                   (time_tag, kp, a_running, station_count, received_at)
                   VALUES (?, ?, ?, ?, ?)""",
                rows,
            )
            await db.commit()
            total += len(rows)
            logger.info("  Kp M%.1f %s: %d records", mag, date_str, len(rows))

    logger.info("Kp backfilled: %d total records", total)


def _parse_ionex_japan(content: str) -> list[dict]:
    """Parse IONEX and extract Japan region TEC grid."""
    records = []
    lines = content.split("\n")

    exponent = -1
    for line in lines:
        if "EXPONENT" in line:
            try:
                exponent = int(line[:6].strip())
            except ValueError:
                pass
        if "END OF HEADER" in line:
            break

    scale = 10 ** exponent

    i = 0
    while i < len(lines):
        line = lines[i]
        if "START OF TEC MAP" not in line:
            i += 1
            continue

        i += 1
        epoch_line = lines[i]
        try:
            parts = epoch_line.split()
            epoch = datetime(
                int(parts[0]), int(parts[1]), int(parts[2]),
                int(parts[3]), int(parts[4]), int(parts[5]),
                tzinfo=timezone.utc,
            ).isoformat()
        except (ValueError, IndexError):
            i += 1
            continue

        i += 1
        while i < len(lines) and "END OF TEC MAP" not in lines[i]:
            if "LAT/LON1/LON2/DLON/H" in lines[i]:
                header = lines[i]
                try:
                    lat = float(header[:8].strip())
                    lon1 = float(header[8:14].strip())
                    lon2 = float(header[14:20].strip())
                    dlon = float(header[20:26].strip())
                except ValueError:
                    i += 1
                    continue

                if lat < TEC_LAT_MIN or lat > TEC_LAT_MAX:
                    i += 1
                    continue

                n_lons = int((lon2 - lon1) / dlon) + 1
                values = []
                i += 1
                while len(values) < n_lons and i < len(lines):
                    values.extend(int(v) for v in lines[i].split())
                    i += 1

                for j, val in enumerate(values):
                    lon = lon1 + j * dlon
                    if TEC_LON_MIN <= lon <= TEC_LON_MAX and val != 9999:
                        records.append({
                            "lat": lat, "lon": lon,
                            "tec": val * scale, "epoch": epoch,
                        })
            else:
                i += 1
        i += 1

    return records


async def main():
    await init_db()

    async with aiohttp.ClientSession() as session:
        async with safe_connect() as db:
            await backfill_earthquakes(session, db)
            await backfill_tec(session, db)
            await backfill_kp(session, db)

    logger.info("=== Backfill complete ===")


if __name__ == "__main__":
    asyncio.run(main())
