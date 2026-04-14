"""Fetch lightning stroke data for earthquake precursor analysis.

Downloads daily lightning stroke counts aggregated into 2-degree grid cells
covering the Japan region. Multiple data sources are attempted in order:

1. Blitzortung.org historical JSON archive (community lightning network)
2. University of Bonn sferics archive (VLF radio measurements)
3. Blitzortung live API (last ~2 hours, for recent/real-time data only)

Physical basis: Pre-seismic electromagnetic emissions (EMP) from crustal
micro-fracturing can manifest as anomalous VLF/ELF signals. Hayakawa &
Molchanov (2002) documented ULF/ELF anomalies before major earthquakes.
Lightning activity changes near fault zones may correlate with stress changes
through atmospheric electric field perturbations (Pulinets & Ouzounov, 2011).

Target features:
    - Daily stroke count per 2-degree grid cell
    - Mean peak current intensity (kA) per cell
    - Anomalies relative to seasonal baseline

Grid cells: 2-degree resolution covering Japan bbox (20-50N, 120-155E),
yielding 15 lat x 18 lon = 270 cells. Cell coordinates refer to cell centers
(e.g., 21.0, 121.0 for the cell spanning 20-22N, 120-122E).

References:
    - Hayakawa, M. & Molchanov, O.A. (2002) Seismo Electromagnetics.
    - Pulinets, S. & Ouzounov, D. (2011) Adv. Space Res. 47:413-424.
    - Betz, H.D. et al. (2009) Nat. Hazards Earth Syst. Sci. 9:1033-1039.
"""

import asyncio
import gzip
import json
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH, JAPAN_BBOX

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=180, connect=30)

# Japan bounding box from config
MIN_LAT = JAPAN_BBOX["min_lat"]  # 20.0
MAX_LAT = JAPAN_BBOX["max_lat"]  # 50.0
MIN_LON = JAPAN_BBOX["min_lon"]  # 120.0
MAX_LON = JAPAN_BBOX["max_lon"]  # 155.0

# Grid resolution (degrees)
GRID_DEG = 2.0

# Blitzortung historical archive (gzipped JSON, monthly)
BLITZORTUNG_ARCHIVE_URL = (
    "https://data.blitzortung.org/Strokes/Month/{year}/{month:02d}/"
    "strokes_{year}_{month:02d}.json.gz"
)

# Blitzortung live GeoJSON endpoint (last ~2 hours)
BLITZORTUNG_LIVE_URL = "https://map.blitzortung.org/GEOjson/Data/Strokes/All/{timestamp}"

# University of Bonn sferics archive (VLF measurements)
SFERICS_BONN_URL = (
    "https://sferics.uni-bonn.de/data/{year}/{month:02d}/{day:02d}/"
    "strokes_{year}{month:02d}{day:02d}.json"
)


def snap_to_grid(lat: float, lon: float) -> tuple[float, float]:
    """Snap a coordinate to the center of its 2-degree grid cell.

    E.g., (35.7, 139.8) -> (35.0, 139.0) for the cell [34-36, 138-140].
    """
    cell_lat = math.floor(lat / GRID_DEG) * GRID_DEG + GRID_DEG / 2
    cell_lon = math.floor(lon / GRID_DEG) * GRID_DEG + GRID_DEG / 2
    return cell_lat, cell_lon


def in_japan_bbox(lat: float, lon: float) -> bool:
    """Check if coordinate falls within the Japan bounding box."""
    return MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON


async def init_lightning_table():
    """Create lightning stroke table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lightning (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                stroke_count INTEGER,
                mean_intensity REAL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_lightning_time
            ON lightning(observed_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_lightning_cell
            ON lightning(cell_lat, cell_lon)
        """)
        await db.commit()


def aggregate_strokes_to_grid(
    strokes: list[dict],
) -> dict[tuple[str, float, float], dict]:
    """Aggregate individual strokes into daily 2-degree grid cells.

    Each stroke dict must have: lat, lon, time (ISO or epoch), intensity (kA).

    Returns dict keyed by (date_str, cell_lat, cell_lon) with values:
        {"count": int, "total_intensity": float}
    """
    cells: dict[tuple[str, float, float], dict] = {}

    for stroke in strokes:
        lat = stroke.get("lat")
        lon = stroke.get("lon")
        if lat is None or lon is None:
            continue
        if not in_japan_bbox(lat, lon):
            continue

        # Parse time
        t = stroke.get("time")
        if isinstance(t, (int, float)):
            # Epoch nanoseconds (Blitzortung) or seconds
            if t > 1e15:
                t = t / 1e9  # nanoseconds to seconds
            elif t > 1e12:
                t = t / 1e6  # milliseconds to seconds
            dt = datetime.fromtimestamp(t, tz=timezone.utc)
        elif isinstance(t, str):
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        else:
            continue

        date_str = dt.strftime("%Y-%m-%d")
        cell_lat, cell_lon = snap_to_grid(lat, lon)
        key = (date_str, cell_lat, cell_lon)

        intensity = stroke.get("intensity", 0.0)
        if isinstance(intensity, str):
            try:
                intensity = float(intensity)
            except ValueError:
                intensity = 0.0

        if key not in cells:
            cells[key] = {"count": 0, "total_intensity": 0.0}
        cells[key]["count"] += 1
        cells[key]["total_intensity"] += abs(intensity) if intensity else 0.0

    return cells


async def fetch_blitzortung_archive_month(
    session: aiohttp.ClientSession, year: int, month: int
) -> list[dict] | None:
    """Fetch monthly stroke archive from Blitzortung.org (gzipped JSON).

    Returns list of stroke dicts with keys: lat, lon, time, intensity.
    Returns None when the response indicates access restriction (HTML
    block page or 403) — caller should stop trying further months.
    """
    url = BLITZORTUNG_ARCHIVE_URL.format(year=year, month=month)
    logger.info("Trying Blitzortung archive: %s", url)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get("Content-Type", "")
                    # Blitzortung may return HTML login/block page with 200
                    if "html" in content_type.lower():
                        logger.warning(
                            "Blitzortung archive returned HTML (access restricted) "
                            "for %04d-%02d", year, month,
                        )
                        return None

                    raw = await resp.read()
                    try:
                        text = gzip.decompress(raw).decode("utf-8")
                    except (gzip.BadGzipFile, OSError):
                        # Not gzipped, try raw
                        text = raw.decode("utf-8")

                    # Detect HTML even if Content-Type was wrong
                    if text.lstrip().startswith(("<!DOCTYPE", "<html", "<HTML")):
                        logger.warning(
                            "Blitzortung archive returned HTML body "
                            "for %04d-%02d (access restricted)", year, month,
                        )
                        return None

                    data = json.loads(text)
                    if isinstance(data, list):
                        strokes = []
                        for entry in data:
                            stroke = {
                                "lat": entry.get("lat"),
                                "lon": entry.get("lon"),
                                "time": entry.get("time", entry.get("t")),
                                "intensity": entry.get(
                                    "intensity", entry.get("sig", entry.get("amp", 0))
                                ),
                            }
                            strokes.append(stroke)
                        logger.info(
                            "Blitzortung archive: %d total strokes for %04d-%02d",
                            len(strokes), year, month,
                        )
                        return strokes
                elif resp.status == 403:
                    logger.warning(
                        "Blitzortung archive returned 403 (access restricted) for %04d-%02d — archive blocked, short-circuiting",
                        year, month,
                    )
                    return None
                elif resp.status == 404:
                    logger.info(
                        "Blitzortung archive not available for %04d-%02d (404)",
                        year, month,
                    )
                    return []
                else:
                    logger.warning(
                        "Blitzortung archive HTTP %d for %04d-%02d (attempt %d/%d)",
                        resp.status, year, month, attempt, MAX_RETRIES,
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                "Blitzortung archive error for %04d-%02d (attempt %d/%d): %s",
                year, month, attempt, MAX_RETRIES, e,
            )
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)

    return []


async def fetch_sferics_bonn_day(
    session: aiohttp.ClientSession, date: datetime
) -> list[dict]:
    """Fetch daily VLF sferics data from University of Bonn archive.

    Returns list of stroke dicts with keys: lat, lon, time, intensity.
    """
    url = SFERICS_BONN_URL.format(
        year=date.year, month=date.month, day=date.day
    )
    logger.debug("Trying Sferics Bonn: %s", url)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    data = json.loads(text)
                    if isinstance(data, list):
                        strokes = []
                        for entry in data:
                            stroke = {
                                "lat": entry.get("lat"),
                                "lon": entry.get("lon"),
                                "time": entry.get("time", entry.get("t")),
                                "intensity": entry.get(
                                    "intensity", entry.get("amp", 0)
                                ),
                            }
                            strokes.append(stroke)
                        return strokes
                elif resp.status in (403, 404):
                    return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("Sferics Bonn failed for %s: %s", date.strftime("%Y-%m-%d"), e)
                return []
            await asyncio.sleep(2 ** attempt)

    return []


async def fetch_blitzortung_live(
    session: aiohttp.ClientSession,
) -> list[dict]:
    """Fetch recent strokes from Blitzortung live GeoJSON (last ~2 hours).

    Only useful for near-real-time data. Returns stroke dicts.
    """
    timestamp = int(datetime.now(timezone.utc).timestamp())
    url = BLITZORTUNG_LIVE_URL.format(timestamp=timestamp)
    logger.info("Trying Blitzortung live API: %s", url)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    data = json.loads(text)

                    strokes = []
                    # GeoJSON FeatureCollection
                    features = data.get("features", [])
                    if not features and isinstance(data, list):
                        features = data

                    for feat in features:
                        if isinstance(feat, dict) and "geometry" in feat:
                            coords = feat["geometry"].get("coordinates", [])
                            props = feat.get("properties", {})
                            if len(coords) >= 2:
                                stroke = {
                                    "lon": coords[0],
                                    "lat": coords[1],
                                    "time": props.get("time", timestamp),
                                    "intensity": props.get(
                                        "intensity", props.get("amp", 0)
                                    ),
                                }
                                strokes.append(stroke)
                        elif isinstance(feat, dict):
                            # Plain JSON array format
                            stroke = {
                                "lat": feat.get("lat"),
                                "lon": feat.get("lon"),
                                "time": feat.get("time", feat.get("t")),
                                "intensity": feat.get(
                                    "intensity", feat.get("sig", feat.get("amp", 0))
                                ),
                            }
                            strokes.append(stroke)

                    logger.info("Blitzortung live: %d strokes", len(strokes))
                    return strokes
                elif resp.status in (403, 404):
                    logger.info("Blitzortung live API returned %d", resp.status)
                    return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("Blitzortung live API failed: %s", e)
                return []
            await asyncio.sleep(2 ** attempt)

    return []


async def store_grid_cells(
    cells: dict[tuple[str, float, float], dict], now: str
) -> int:
    """Insert aggregated grid cells into the lightning table.

    Returns number of rows inserted.
    """
    if not cells:
        return 0

    rows = []
    for (date_str, cell_lat, cell_lon), agg in cells.items():
        count = agg["count"]
        mean_intensity = (
            agg["total_intensity"] / count if count > 0 else None
        )
        rows.append((date_str, cell_lat, cell_lon, count, mean_intensity, now))

    async with safe_connect() as db:
        await db.executemany(
            """INSERT OR IGNORE INTO lightning
               (observed_at, cell_lat, cell_lon, stroke_count, mean_intensity, received_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows,
        )
        await db.commit()

    return len(rows)


async def main():
    await init_db()
    await init_lightning_table()

    now = datetime.now(timezone.utc).isoformat()

    # Determine target dates: days around M6+ earthquakes (+-7 days)
    async with safe_connect() as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DISTINCT DATE(occurred_at), latitude, longitude, magnitude "
            "FROM earthquakes "
            "WHERE magnitude >= 6.0 ORDER BY occurred_at"
        )
        existing = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM lightning"
        )

    target_dates = set()
    for r in eq_rows:
        d = datetime.strptime(r[0], "%Y-%m-%d")
        for offset in range(-7, 8):
            target_dates.add(d + timedelta(days=offset))

    existing_dates = set(r[0] for r in existing)

    dates_to_fetch = sorted(
        d for d in target_dates
        if d.strftime("%Y-%m-%d") not in existing_dates
        and d.year >= 2011
    )

    if not dates_to_fetch:
        logger.info("All target dates already fetched")
        return

    logger.info(
        "%d dates to fetch (%d total target dates, %d existing)",
        len(dates_to_fetch), len(target_dates), len(existing_dates),
    )

    total_records = 0
    total_fetched_days = 0
    total_failed_days = 0
    source_stats: dict[str, int] = {}

    async with aiohttp.ClientSession() as session:
        # --- Source 1: Blitzortung monthly archive ---
        # Group target dates by month to minimize requests
        months_needed: dict[tuple[int, int], list[datetime]] = {}
        for d in dates_to_fetch:
            key = (d.year, d.month)
            months_needed.setdefault(key, []).append(d)

        # Limit to 60 months per run to be respectful of the server
        month_keys = sorted(months_needed.keys())[:60]

        for year, month in month_keys:
            strokes = await fetch_blitzortung_archive_month(session, year, month)
            if strokes is None:
                logger.warning(
                    "Blitzortung archive is access-restricted — "
                    "skipping remaining %d months, falling back to Sferics Bonn",
                    len(month_keys) - month_keys.index((year, month)) - 1,
                )
                break
            if strokes:
                cells = aggregate_strokes_to_grid(strokes)
                # Filter to only target dates in this month
                target_date_strs = set(
                    d.strftime("%Y-%m-%d") for d in months_needed[(year, month)]
                )
                filtered_cells = {
                    k: v for k, v in cells.items() if k[0] in target_date_strs
                }
                inserted = await store_grid_cells(filtered_cells, now)
                if inserted > 0:
                    total_records += inserted
                    days_covered = len(set(k[0] for k in filtered_cells))
                    total_fetched_days += days_covered
                    source_stats["blitzortung_archive"] = (
                        source_stats.get("blitzortung_archive", 0) + days_covered
                    )
                    logger.info(
                        "Blitzortung archive %04d-%02d: %d cells for %d days",
                        year, month, inserted, days_covered,
                    )
                    # Remove fetched dates from the remaining list
                    for date_str in target_date_strs:
                        existing_dates.add(date_str)

            # Rate limit between monthly fetches
            await asyncio.sleep(1.0)

        # --- Source 2: Sferics Bonn (daily, fallback) ---
        remaining_dates = sorted(
            d for d in dates_to_fetch
            if d.strftime("%Y-%m-%d") not in existing_dates
        )

        if remaining_dates:
            logger.info(
                "Trying Sferics Bonn for %d remaining dates", len(remaining_dates)
            )
            # If Blitzortung archive failed entirely, increase Sferics Bonn batch
            blitz_fetched = source_stats.get("blitzortung_archive", 0)
            bonn_limit = 500 if blitz_fetched == 0 else 200
            bonn_batch = remaining_dates[:bonn_limit]

            for i, date in enumerate(bonn_batch):
                strokes = await fetch_sferics_bonn_day(session, date)
                if strokes:
                    date_str = date.strftime("%Y-%m-%d")
                    cells = aggregate_strokes_to_grid(strokes)
                    # Filter to this specific date
                    day_cells = {
                        k: v for k, v in cells.items() if k[0] == date_str
                    }
                    inserted = await store_grid_cells(day_cells, now)
                    if inserted > 0:
                        total_records += inserted
                        total_fetched_days += 1
                        source_stats["sferics_bonn"] = (
                            source_stats.get("sferics_bonn", 0) + 1
                        )
                        existing_dates.add(date_str)
                else:
                    total_failed_days += 1

                if (i + 1) % 30 == 0:
                    logger.info(
                        "Sferics Bonn: %d/%d dates processed, %d records so far",
                        i + 1, len(bonn_batch), total_records,
                    )

                # Rate limit
                await asyncio.sleep(0.5)

        # --- Source 3: Blitzortung live (near-real-time only) ---
        # Only useful if we need data from the last ~2 hours
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today_str not in existing_dates and any(
            d.strftime("%Y-%m-%d") == today_str for d in dates_to_fetch
        ):
            strokes = await fetch_blitzortung_live(session)
            if strokes:
                cells = aggregate_strokes_to_grid(strokes)
                inserted = await store_grid_cells(cells, now)
                if inserted > 0:
                    total_records += inserted
                    total_fetched_days += 1
                    source_stats["blitzortung_live"] = 1
                    logger.info("Blitzortung live: %d cells for today", inserted)

    # Summary
    logger.info(
        "Lightning fetch complete: %d grid-cell records, %d days fetched, %d days failed",
        total_records, total_fetched_days, total_failed_days,
    )
    for source, count in sorted(source_stats.items()):
        logger.info("  Source %s: %d days", source, count)

    if total_records == 0:
        logger.warning(
            "No lightning data could be retrieved from any source. "
            "Blitzortung historical archive may require authentication. "
            "Sferics Bonn archive may not cover the requested period. "
            "Consider manual data acquisition or alternative sources "
            "(WWLLN institutional access, JMA LIDEN)."
        )


if __name__ == "__main__":
    asyncio.run(main())
