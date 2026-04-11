"""Fetch atmospheric SO2 column data from NASA OMI.

SO2 (sulfur dioxide) in the atmosphere primarily comes from volcanic
degassing and anthropogenic sources. In subduction zones like Japan,
changes in volcanic SO2 emission rates may correlate with regional
tectonic stress changes.

Physical mechanism:
    Tectonic stress changes → altered permeability in volcanic conduits
    → modified degassing rates → atmospheric SO2 column changes.
    Japan has 111 active volcanoes along the subduction zone; changes
    in their collective degassing pattern could indicate broad-scale
    stress redistribution.

Data source: NASA OMI OMSO2e (Level 3 daily gridded SO2)
    - 0.25° global grid, daily, 2004-present
    - Requires Earthdata login (free) for OPeNDAP access
    - Alternative: NASA SO2 monitoring portal for summary data

Target features:
    - so2_column_anomaly: deviation from seasonal baseline (DU)

References:
    - Carn et al. (2016) J. Volcanol. Geotherm. Res. 327:50-66
    - Noguchi et al. (2011) Remote Sensing 3:1820-1834
"""

import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH
from earthdata_auth import (
    get_earthdata_session, earthdata_fetch,
    EARTHDATA_TOKEN, EARTHDATA_USERNAME, EARTHDATA_PASSWORD,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# GES DISC OPeNDAP for OMSO2G (OMI Level 2G daily gridded, 0.25° grid)
# OMSO2e was removed from GES DISC; OMSO2G is the replacement (2004-present)
# Note: GES DISC requires URS redirect + Basic Auth (Bearer returns 401)
GESDISC_OPENDAP = "https://acdisc.gesdisc.eosdis.nasa.gov/opendap/HDF-EOS5/Aura_OMI_Level2G/OMSO2G.003"

# Japan bbox in grid indices (0.25° resolution)
# Lat: -89.875 to 89.875 (720 cells), Lon: -179.875 to 179.875 (1440 cells)
# Japan: lat 24-46 → indices (24+89.875)/0.25 = 455 to 543
# Japan: lon 122-150 → indices (122+179.875)/0.25 = 1207 to 1319
LAT_START = 455
LAT_END = 543
LON_START = 1207
LON_END = 1319

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)
START_YEAR = 2004


async def init_so2_table():
    """Create SO2 column table."""
    async with safe_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS so2_column (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                so2_du REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_so2_time
            ON so2_column(observed_at)
        """)
        await db.commit()


# Year-level cache for contents.html (same year = same listing)
_contents_cache: dict[int, str] = {}


async def _get_contents_html(year: int) -> str | None:
    """Fetch and cache contents.html for a given year (public, no auth)."""
    if year in _contents_cache:
        return _contents_cache[year]

    contents_url = f"{GESDISC_OPENDAP}/{year}/contents.html"
    try:
        async with aiohttp.ClientSession() as plain_session:
            async with plain_session.get(contents_url, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    logger.info("SO2 contents %d: HTTP %d (expected 200)", year, resp.status)
                    _contents_cache[year] = ""
                    return None
                text = await resp.text()
        if not text:
            logger.info("SO2 contents %d: empty response", year)
            _contents_cache[year] = ""
            return None
        _contents_cache[year] = text
        logger.info("SO2 contents %d: cached (%d bytes)", year, len(text))
        return text
    except Exception as e:
        logger.info("SO2 contents %d: %s: %s", year, type(e).__name__, e)
        return None


async def _resolve_so2_filename(session: aiohttp.ClientSession, year: int, date_str: str) -> str | None:
    """Resolve OMSO2G filename from OPeNDAP contents listing.

    OMSO2G files are stored directly under year/ (no DOY subdirectory).
    Filename: OMI-Aura_L2G-OMSO2G_{YYYY}m{MMDD}_v003-{revision}.he5
    """

    mmdd = date_str[5:7] + date_str[8:10]  # "2024-01-15" -> "0115"
    pattern_str = f"OMI-Aura_L2G-OMSO2G_{year}m{mmdd}"

    text = await _get_contents_html(year)
    if not text:
        return None
    match = re.search(rf'({re.escape(pattern_str)}[^"<\s]*\.he5)', text)
    if match:
        return match.group(1)

    # Diagnostic — when the regex misses, dump enough info to see why.
    # Emitted at most once per year to keep logs reasonable.
    flag = f"_diag_resolve_{year}"
    if not getattr(_resolve_so2_filename, flag, False):
        setattr(_resolve_so2_filename, flag, True)
        he5_count = text.count(".he5")
        # Find any OMSO2G file in this year to see the actual naming convention
        any_match = re.search(r'(OMI-Aura_L2G-OMSO2G_\d+m\d+[^"<\s]*\.he5)', text)
        sample = any_match.group(1) if any_match else "(none)"
        # Find a prefix match (same year + month) to see if only the day mismatches
        mm = date_str[5:7]
        month_match = re.search(
            rf'(OMI-Aura_L2G-OMSO2G_{year}m{mm}\d+[^"<\s]*\.he5)', text,
        )
        month_sample = month_match.group(1) if month_match else "(none)"
        logger.warning(
            "SO2 %s: no match for pattern=%s. contents.html has %d .he5 files. "
            "any OMSO2G sample=%s. same-year-month sample=%s. len(text)=%d",
            date_str, pattern_str, he5_count, sample, month_sample, len(text),
        )
    else:
        logger.info("SO2 %s: no filename match for pattern %s", date_str, pattern_str)
    return None


async def fetch_so2_day(session: aiohttp.ClientSession, date: datetime) -> list[dict]:
    """Fetch SO2 column data for one day via GES DISC OPeNDAP.

    Uses OMSO2G (Level 2G gridded, 0.25° grid, nCandidate=15).
    nCandidate[0] = best pixel per grid cell.
    GES DISC requires URS Basic Auth redirect (Bearer returns 401).
    """
    has_auth = (EARTHDATA_USERNAME and EARTHDATA_PASSWORD) or EARTHDATA_TOKEN
    if not has_auth:
        logger.debug("SO2 %s: no Earthdata credentials available", date.strftime("%Y-%m-%d"))
        return []

    date_str = date.strftime("%Y-%m-%d")
    year = date.year

    # Resolve filename (contains variable revision timestamp)
    exact_filename = await _resolve_so2_filename(session, year, date_str)
    if not exact_filename:
        return []

    # OPeNDAP ASCII: fetch best pixel (nCandidate=0) for Japan bbox
    # OMSO2G is 3D: [nCandidate=15][YDim=720][XDim=1440]
    url = (
        f"{GESDISC_OPENDAP}/{year}/{exact_filename}.ascii"
        f"?ColumnAmountSO2_PBL"
        f"[0:0][{LAT_START}:{LAT_END}][{LON_START}:{LON_END}]"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            status, text = await earthdata_fetch(session, url, timeout=TIMEOUT)
            if status == 200:
                if not text or "<html" in text[:200].lower():
                    # Log first occurrence at INFO for diagnosis
                    if not hasattr(fetch_so2_day, '_html_warned'):
                        fetch_so2_day._html_warned = True
                        logger.info("SO2 %s: got HTML instead of ASCII (len=%d, preview=%s)",
                                    date_str, len(text) if text else 0, repr((text or "")[:100]))
                    return []
                rows = _parse_opendap_ascii(text, date_str)
                if not rows and not hasattr(fetch_so2_day, '_empty_warned'):
                    fetch_so2_day._empty_warned = True
                    logger.info("SO2 %s: ASCII parsed but 0 positive values (text len=%d, first 200 chars=%s)",
                                date_str, len(text), repr(text[:200]))
                return rows
            elif status in (401, 403):
                if not hasattr(fetch_so2_day, '_auth_warned'):
                    fetch_so2_day._auth_warned = True
                    logger.info("SO2 %s: auth failed (HTTP %d) — credentials may be invalid for GES DISC",
                                date_str, status)
                return []
            elif status == 404:
                return []  # No data for this date
            else:
                if attempt == MAX_RETRIES:
                    logger.warning("SO2 %s: HTTP %d", date_str, status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.info("SO2 %s: %s", date_str, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_opendap_ascii(text: str, date_str: str) -> list[dict]:
    """Parse OPeNDAP ASCII grid response for OMSO2G.

    OMSO2G is 3D [nCandidate][YDim][XDim], subsetted to [0:0][lat][lon].
    OPeNDAP ASCII format for 3D with first dim fixed:
        ColumnAmountSO2_PBL.ColumnAmountSO2_PBL[1][88][113]
        [0][0], val, val, val, ...
        [0][1], val, val, val, ...
    Or for 2D (if server collapses singleton dim):
        [0], val, val, val, ...

    Values are in Dobson Units (DU). Fill value = -1.2676506e+30.
    """
    rows = []
    in_data = False

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Skip the "Dataset:" header line
        if line.startswith("Dataset:"):
            continue

        # Data lines contain variable name + indices + values, e.g.:
        # ColumnAmountSO2_PBL[0][5], -0.26, -0.05, ...
        # OR just indices (older format):
        # [0][5], -0.26, -0.05, ...
        if "ColumnAmountSO2" in line and "[" in line and "," in line:
            # This IS a data line (variable name prefix + indices + values)
            in_data = True
            # Fall through to parse it (don't skip!)
        elif "ColumnAmountSO2" in line and "," not in line:
            # Pure header line like "ColumnAmountSO2_PBL.ColumnAmountSO2_PBL[1][88][113]"
            in_data = True
            continue

        if not in_data:
            continue

        # Data lines: "ColumnAmountSO2_PBL[0][5], val, val, ..." or "[0][5], val, val, ..."
        if not ("[" in line and "," in line):
            break  # End of data section

        first_comma = line.find(",")
        if first_comma < 0:
            continue

        try:
            idx_part = line[:first_comma].strip()
            # Extract lat index: last bracketed number
            # "ColumnAmountSO2_PBL[0][5]" -> 5, "[0][5]" -> 5, "[5]" -> 5

            bracket_nums = re.findall(r'\[(\d+)\]', idx_part)
            if not bracket_nums:
                continue
            lat_idx = int(bracket_nums[-1])  # Last index = lat row
            lat = -89.875 + (LAT_START + lat_idx) * 0.25

            val_parts = line[first_comma + 1:].split(",")
            for lon_offset, val_str in enumerate(val_parts):
                val_str = val_str.strip()
                if not val_str:
                    continue
                val = float(val_str)
                if val < -999 or val > 1000:
                    continue  # Fill value
                lon = -179.875 + (LON_START + lon_offset) * 0.25

                # Store all valid values (not just positive).
                # Background SO2 in Japan is typically -2 to +2 DU (noise).
                # ML pipeline computes per-cell baseline and anomaly detection,
                # so it needs the full distribution including negatives.
                rows.append({
                    "date": date_str,
                    "lat": round(lat, 3),
                    "lon": round(lon, 3),
                    "so2": round(val, 4),
                })
        except (ValueError, IndexError):
            continue

    return rows


async def main():
    await init_db()
    await init_so2_table()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN and not (EARTHDATA_USERNAME and EARTHDATA_PASSWORD):
        logger.info(
            "SO2 fetch: no Earthdata credentials. "
            "Set EARTHDATA_TOKEN or EARTHDATA_USERNAME+EARTHDATA_PASSWORD. "
            "Generate at https://urs.earthdata.nasa.gov/ "
            "SO2 features will be excluded via dynamic selection."
        )
        return

    # Check existing
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(DISTINCT observed_at) FROM so2_column"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    n_dates = existing[0][1] if existing else 0
    logger.info("SO2 existing: %d dates (latest: %s)", n_dates, last_date)

    # Determine dates to fetch — continuous daily coverage for proper baselines
    # ML anomaly detection requires continuous time series, not just event windows
    existing_dates = set()
    async with safe_connect() as db:
        ed_rows = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM so2_column"
        )
    existing_dates = set(r[0] for r in ed_rows)

    # Generate all dates from START_YEAR to yesterday
    today = datetime.now(timezone.utc).date()
    d = datetime(START_YEAR, 1, 1).date()
    target_dates = []
    while d < today:
        ds = d.strftime("%Y-%m-%d")
        if ds not in existing_dates:
            target_dates.append(datetime(d.year, d.month, d.day))
        d += timedelta(days=1)

    # Prioritize recent gaps first (most impactful for test period), then backfill
    # Split: 2011+ dates first (analysis period), then pre-2011 (baseline building)
    analysis_dates = [dt for dt in target_dates if dt.year >= 2011]
    backfill_dates = [dt for dt in target_dates if dt.year < 2011]
    dates_to_fetch = (analysis_dates + backfill_dates)[:600]  # 600/run fits 60-min timeout

    if not dates_to_fetch:
        logger.info("All SO2 target dates already fetched")
        return

    logger.info("SO2: %d dates to fetch", len(dates_to_fetch))

    total_records = 0
    auth_fail_count = 0
    html_fail_count = 0
    no_file_count = 0
    session = await get_earthdata_session()
    try:
        for i, date in enumerate(dates_to_fetch):
            rows = await fetch_so2_day(session, date)
            if rows:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO so2_column
                           (observed_at, cell_lat, cell_lon, so2_du, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["date"], r["lat"], r["lon"], r["so2"], now) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 20 == 0:
                logger.info("SO2: %d/%d dates, %d records", i + 1, len(dates_to_fetch), total_records)

            await asyncio.sleep(1.0)
    finally:
        await session.close()

    # Diagnostic summary: why 0 records?
    if total_records == 0 and dates_to_fetch:
        logger.info(
            "SO2 fetch complete: 0 records from %d dates. "
            "Check logs above for root cause: "
            "Bearer→401 + BasicAuth→HTML = credentials invalid for GES DISC data access "
            "(EULA not accepted or app not approved at urs.earthdata.nasa.gov)",
            len(dates_to_fetch),
        )
    else:
        logger.info("SO2 fetch complete: %d total records from %d dates",
                     total_records, len(dates_to_fetch))


if __name__ == "__main__":
    asyncio.run(main())
