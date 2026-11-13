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

Data source: NASA OMI OMSO2G v004 (Level 2G, 0.25° grid, candidate 0)
    - 0.25° global grid, daily, 2004-present, ColumnAmountSO2_PBL
    - Requires Earthdata login (free) for OPeNDAP access
    - OMSO2G.004 stops at 2026-06-08 in CMR while OMI keeps observing.
      For dates without an OMSO2G granule the same cells are rebuilt from
      the OMSO2 v004 Level 2 orbits (see grid_l2_orbits).  Measured on
      2026-11-13 for 2026-06-08, 2020-06-01 and 2011-01-05: candidate 0 of
      OMSO2G is, for every cell, the first L2 pixel (orbit, line, scene
      order) whose centre falls in the cell, and its value is the L2 value
      rounded to 0.01 DU -- 5,241 of 5,241 Japan cells matched exactly.
      OMSO2e v004 is not used: it has no PBL column (only the GEOS-5
      profile ColumnAmountSO2), a different quantity.
    - Every finished date is written to so2_fetch_log; dates that came back
      empty (no granule, or every Japan cell is fill) are skipped on later
      runs once they are SETTLE_DAYS old.  Before this, ~660 such dates were
      re-requested on every run (~16 min, run 43360558222).

Target features:
    - so2_column_anomaly: deviation from seasonal baseline (DU)

References:
    - Carn et al. (2016) J. Volcanol. Geotherm. Res. 327:50-66
    - Noguchi et al. (2011) Remote Sensing 3:1820-1834
"""

import asyncio
import io
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH
from earthdata_auth import (
    get_earthdata_session, earthdata_fetch, earthdata_fetch_bytes,
    EARTHDATA_TOKEN, EARTHDATA_USERNAME, EARTHDATA_PASSWORD,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# OMSO2G (OMI Level 2G daily gridded, 0.25 deg grid, 2004-present)
# OMSO2e was removed from GES DISC; OMSO2G is the replacement (2004-present)
# V003 archive froze ~2025-06; V004 is the live continuation to present
# (identical 0.25-deg 720x1440 grid + ColumnAmountSO2 variable)
#
# GES DISC retired its on-premises OPeNDAP (acdisc.gesdisc.eosdis.nasa.gov/opendap/
# answers HTTP 410 "Service Permanently Retired", seen 2026-11-13). The same granules
# are served by Earthdata Cloud OPeNDAP, which accepts the Bearer token. Granule URLs
# are not guessable (the file name carries a reprocessing timestamp), so they are
# looked up in CMR (public, no auth): RelatedUrls Type "USE SERVICE API",
# Subtype "OPENDAP DATA".
CMR_GRANULES = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
OMSO2G_COLLECTION = "C3685693295-GES_DISC"  # OMSO2G.004 (GES_DISC, cloud)
# Cloud OPeNDAP keeps the HDF-EOS5 group path; the on-prem server flattened it.
SO2_VAR = "/HDFEOS/GRIDS/OMI_Total_Column_Amount_SO2/Data_Fields/ColumnAmountSO2_PBL"

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

# OMSO2 v004 Level 2 orbits (HTTPS download only: no cloud OPeNDAP, and the
# on-prem aura.gesdisc OPeNDAP answers 410). ~16 MB each, 3-5 orbits/day touch
# the Japan box.
OMSO2_L2_COLLECTION = "C3454342607-GES_DISC"  # OMSO2.004 (GES_DISC)
L2_GROUP = "HDFEOS/SWATHS/OMI Total Column Amount SO2"
# Outer edges of the LAT_START..LAT_END x LON_START..LON_END cells
JAPAN_BBOX = (
    f"{-180 + LON_START * 0.25},{-90 + LAT_START * 0.25},"
    f"{-180 + (LON_END + 1) * 0.25},{-90 + (LAT_END + 1) * 0.25}"
)
_ORBIT = re.compile(r"-o(\d+)_")

# Dates younger than this are never marked done in so2_fetch_log (granules can
# still be published or reprocessed); older empty dates are skipped next run.
SETTLE_DAYS = 30
# L2 orbits younger than this are not used (each orbit appears in CMR ~5-12 h
# after observation; a partial set would fix the wrong candidate 0 for good).
L2_SETTLE_DAYS = 3
# Stop starting new dates after this many minutes; so2_column and
# so2_fetch_log keep the progress for the next run. The workflow sets it from
# the time left in the job (restore can take most of the 120-min job).
TIME_BUDGET_S = float(os.environ.get("SO2_TIME_BUDGET_MIN", "75")) * 60


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
        # One row per finished date. kind: 'rows' (cells stored), 'empty'
        # (granule exists, every Japan cell is fill), 'no_granule' (neither
        # OMSO2G nor OMSO2 L2 has a granule). Errors are not logged.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS so2_fetch_log (
                observed_at TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                n_records INTEGER NOT NULL,
                fetched_at TEXT NOT NULL
            )
        """)
        await db.commit()


# Year-level cache: date "YYYY-MM-DD" -> cloud OPeNDAP granule URL
_granule_cache: dict[int, dict[str, str]] = {}
_GRANULE_DATE = re.compile(r"OMSO2G_(\d{4})m(\d{2})(\d{2})_")


async def _get_year_granules(year: int) -> dict[str, str] | None:
    """Look up one year of OMSO2G granules in CMR (public, no auth) and cache them."""
    if year in _granule_cache:
        return _granule_cache[year] or None

    params = {
        "collection_concept_id": OMSO2G_COLLECTION,
        "temporal": f"{year}-01-01T00:00:00Z,{year}-12-31T23:59:59Z",
        "page_size": "2000",
    }
    found: dict[str, tuple[str, str]] = {}  # date -> (GranuleUR, url)
    headers: dict[str, str] = {}
    try:
        async with aiohttp.ClientSession() as plain_session:
            while True:
                async with plain_session.get(
                    CMR_GRANULES, params=params, headers=headers, timeout=TIMEOUT,
                ) as resp:
                    if resp.status != 200:
                        logger.info("SO2 CMR %d: HTTP %d (expected 200)", year, resp.status)
                        return None  # not cached: a transient CMR error is retried next date
                    data = await resp.json(content_type=None)
                    search_after = resp.headers.get("CMR-Search-After")
                items = data.get("items") or []
                for item in items:
                    umm = item.get("umm", {})
                    ur = umm.get("GranuleUR", "")
                    m = _GRANULE_DATE.search(ur)
                    if not m:
                        continue
                    url = next(
                        (r.get("URL") for r in umm.get("RelatedUrls", [])
                         if r.get("Type") == "USE SERVICE API"
                         and r.get("Subtype") == "OPENDAP DATA"
                         and str(r.get("URL", "")).startswith("https://opendap.earthdata.nasa.gov/")),
                        None,
                    )
                    if not url:
                        continue
                    d = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                    # Several revisions of one day: keep the latest (UR sorts by revision stamp)
                    if d not in found or ur > found[d][0]:
                        found[d] = (ur, url)
                if not search_after or len(items) < int(params["page_size"]):
                    break
                headers = {"CMR-Search-After": search_after}
    except Exception as e:
        logger.info("SO2 CMR %d: %s: %s", year, type(e).__name__, e)
        return None

    _granule_cache[year] = {d: v[1] for d, v in found.items()}
    if found:
        logger.info("SO2 CMR %d: %d granules with cloud OPeNDAP URLs", year, len(found))
    else:
        logger.warning("SO2 CMR %d: 0 granules with cloud OPeNDAP URLs in %s "
                       "(collection retired or moved?)", year, OMSO2G_COLLECTION)
    return _granule_cache[year] or None


async def _resolve_so2_url(year: int, date_str: str) -> tuple[str | None, bool]:
    """Return (cloud OPeNDAP URL of the OMSO2G granule for date_str or None, cmr_answered).

    cmr_answered is False when the CMR lookup itself failed (transient), so the
    caller must not conclude that the granule does not exist.
    """
    granules = await _get_year_granules(year)
    if year not in _granule_cache:
        return None, False
    url = (granules or {}).get(date_str)
    if not url:
        logger.info("SO2 %s: no OMSO2G granule in CMR", date_str)
    return url, True


async def fetch_so2_day(session: aiohttp.ClientSession, date: datetime) -> tuple[list[dict], str, str]:
    """Fetch SO2 column data for one day.

    Returns (rows, kind, source). kind is 'rows', 'empty', 'no_granule' or
    'error' (transient: auth, HTTP, CMR failure -- the date is retried).

    Uses OMSO2G (Level 2G gridded, 0.25° grid, nCandidate=15) via Earthdata
    Cloud OPeNDAP, nCandidate[0] for the Japan box. Cloud OPeNDAP accepts the
    Bearer token (earthdata_fetch tries it first). Dates without an OMSO2G
    granule are rebuilt from the OMSO2 v004 Level 2 orbits (fetch_so2_day_l2).
    """
    has_auth = (EARTHDATA_USERNAME and EARTHDATA_PASSWORD) or EARTHDATA_TOKEN
    if not has_auth:
        logger.debug("SO2 %s: no Earthdata credentials available", date.strftime("%Y-%m-%d"))
        return [], "error", "none"

    date_str = date.strftime("%Y-%m-%d")
    year = date.year

    # Resolve the granule URL (file name carries a reprocessing timestamp)
    granule_url, cmr_ok = await _resolve_so2_url(year, date_str)
    if not cmr_ok:
        return [], "error", "OMSO2G"
    if not granule_url:
        return await fetch_so2_day_l2(session, date)

    # OPeNDAP ASCII: fetch best pixel (nCandidate=0) for Japan bbox
    # OMSO2G is 3D: [nCandidate=15][YDim=720][XDim=1440]
    url = (
        f"{granule_url}.ascii"
        f"?{SO2_VAR}"
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
                    return [], "error", "OMSO2G"
                fetch_so2_day.auth_failures = 0
                shape = _ascii_shape(text)
                if shape != (LAT_END - LAT_START + 1, LON_END - LON_START + 1):
                    # A DAP error after the header or a cut-off body is not "empty":
                    # it must not be stored or logged, so the date is retried.
                    logger.info("SO2 %s: OPeNDAP ASCII has shape %s, expected %s -- retry next run",
                                date_str, shape, (LAT_END - LAT_START + 1, LON_END - LON_START + 1))
                    return [], "error", "OMSO2G"
                rows = _parse_opendap_ascii(text, date_str)
                if not rows and not hasattr(fetch_so2_day, '_empty_warned'):
                    fetch_so2_day._empty_warned = True
                    logger.info("SO2 %s: ASCII parsed but 0 valid values -- every Japan cell is "
                                "fill in candidate 0 (text len=%d, first 200 chars=%s)",
                                date_str, len(text), repr(text[:200]))
                return rows, ("rows" if rows else "empty"), "OMSO2G"
            elif status in (401, 403):
                # Cloud OPeNDAP needs a working Bearer token: the Basic-auth
                # fallback ends in 401 there (measured 2026-11-13).
                _note_auth_failure(date_str, status)
                return [], "error", "OMSO2G"
            elif status == 404:
                return [], "error", "OMSO2G"  # CMR listed it; retry next run
            else:
                if attempt == MAX_RETRIES:
                    logger.warning("SO2 %s: HTTP %d", date_str, status)
                else:
                    await asyncio.sleep(2 ** attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.info("SO2 %s: %s", date_str, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return [], "error", "OMSO2G"


def _ascii_shape(text: str) -> tuple[int, int] | None:
    """(data lines, values per line) of an OPeNDAP ASCII grid answer, or None if ragged.

    Counts every value, fill included, so a complete Japan-box answer is
    (LAT_END-LAT_START+1, LON_END-LON_START+1) whatever the cloud cover.
    """
    widths = []
    for line in text.split("\n"):
        line = line.strip()
        if "ColumnAmountSO2" in line and "[" in line and "," in line:
            widths.append(len(line.split(",")) - 1)
    if not widths or len(set(widths)) != 1:
        return None if widths else (0, 0)
    return len(widths), widths[0]


def _note_auth_failure(date_str: str, status: int) -> None:
    fetch_so2_day.auth_failures = getattr(fetch_so2_day, "auth_failures", 0) + 1
    if not hasattr(fetch_so2_day, '_auth_warned'):
        fetch_so2_day._auth_warned = True
        logger.info("SO2 %s: auth failed (HTTP %d) — credentials may be invalid for GES DISC",
                    date_str, status)


_l2_year_hits: dict[int, int] = {}


async def _cmr_get(params: dict, what: str) -> tuple[dict, int] | None:
    """GET CMR granules (public, no auth). Returns (json, CMR-Hits) or None on failure."""
    try:
        async with aiohttp.ClientSession() as plain_session:
            async with plain_session.get(CMR_GRANULES, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    logger.info("SO2 L2 CMR %s: HTTP %d (expected 200)", what, resp.status)
                    return None
                hits = int(resp.headers.get("CMR-Hits", "-1"))
                return await resp.json(content_type=None), hits
    except Exception as e:
        logger.info("SO2 L2 CMR %s: %s: %s", what, type(e).__name__, e)
        return None


async def _l2_collection_alive(year: int) -> bool | None:
    """Does OMSO2_L2_COLLECTION have any granule in `year`? None if CMR failed.

    CMR answers 200 with no items for a retired or moved concept id, which
    would otherwise turn every fallback date into a permanent 'no_granule'.
    """
    if year not in _l2_year_hits:
        got = await _cmr_get({
            "collection_concept_id": OMSO2_L2_COLLECTION,
            "temporal": f"{year}-01-01T00:00:00Z,{year}-12-31T23:59:59Z",
            "page_size": "1",
        }, f"year {year}")
        if got is None or got[1] < 0:
            return None
        _l2_year_hits[year] = got[1]
        if got[1] == 0:
            logger.warning("SO2 L2 CMR: 0 granules in %s for %d (collection retired or moved?) "
                           "-- dates are not logged as no_granule", OMSO2_L2_COLLECTION, year)
    return _l2_year_hits[year] > 0


async def _l2_orbits_for_date(date_str: str) -> list[tuple[int, str]] | None:
    """CMR lookup (public) of the OMSO2 v004 L2 orbits that touch the Japan box.

    Returns [(orbit number, HTTPS download URL)] sorted by orbit, [] when the
    collection has no orbit over Japan that day, or None when the answer cannot
    be trusted (CMR failure, collection without granules that year).
    All orbits of the UTC day are listed; an orbit is kept when CMR's bbox
    search returns it or when it has no spatial metadata (so a granule with
    missing geometry cannot silently drop out and change candidate 0).
    """
    temporal = f"{date_str}T00:00:00Z,{date_str}T23:59:59Z"
    day = await _cmr_get({"collection_concept_id": OMSO2_L2_COLLECTION,
                          "temporal": temporal, "page_size": "100"}, date_str)
    boxed = await _cmr_get({"collection_concept_id": OMSO2_L2_COLLECTION,
                            "temporal": temporal, "bounding_box": JAPAN_BBOX,
                            "page_size": "100"}, f"{date_str} bbox")
    if day is None or boxed is None:
        return None
    day_items = day[0].get("items") or []
    if day[1] > len(day_items):
        logger.info("SO2 L2 CMR %s: %d hits but %d items returned", date_str, day[1], len(day_items))
        return None
    if not day_items:
        alive = await _l2_collection_alive(int(date_str[:4]))
        return [] if alive else None
    in_box = {(it.get("umm") or {}).get("GranuleUR", "") for it in boxed[0].get("items") or []}

    best: dict[int, tuple[str, str]] = {}  # orbit -> (GranuleUR, url)
    for item in day_items:
        umm = item.get("umm", {})
        ur = umm.get("GranuleUR", "")
        geom = ((umm.get("SpatialExtent") or {}).get("HorizontalSpatialDomain") or {}).get("Geometry")
        if ur not in in_box:
            if geom:
                continue
            logger.info("SO2 L2 %s: %s has no spatial metadata -- downloaded anyway", date_str, ur)
        m = _ORBIT.search(ur)
        url = next(
            (r.get("URL") for r in umm.get("RelatedUrls", [])
             if r.get("Type") == "GET DATA" and str(r.get("URL", "")).startswith("https://")),
            None,
        )
        if not m or not url:
            logger.info("SO2 L2 %s: %s has no orbit number or download URL", date_str, ur)
            return None  # an unreadable orbit could hold candidate 0 of some cells
        orb = int(m.group(1))
        # Several revisions of one orbit: keep the latest (UR sorts by production stamp)
        if orb not in best or ur > best[orb][0]:
            best[orb] = (ur, url)
    return [(orb, best[orb][1]) for orb in sorted(best)]


def grid_l2_orbits(orbits: list[tuple[int, dict]], date_str: str) -> tuple[list[dict], int]:
    """Rebuild OMSO2G candidate 0 for the Japan box from OMSO2 L2 orbits.

    orbits: [(orbit number, {"lat", "lon", "pbl"} 2-D arrays)] in any order.
    For each 0.25° cell the first pixel -- by orbit, then line, then scene --
    whose centre falls in the cell is candidate 0, even when its value is
    fill (OMSO2G does the same: cells whose candidate 0 is fill stay fill
    although later candidates are valid). Values are rounded to 0.01 DU as in
    OMSO2G. Returns (rows, number of cells that had a candidate).
    """
    import numpy as np

    lat_parts, lon_parts, val_parts = [], [], []
    for _, d in sorted(orbits, key=lambda x: x[0]):
        lat_parts.append(np.asarray(d["lat"], dtype=np.float64).ravel())
        lon_parts.append(np.asarray(d["lon"], dtype=np.float64).ravel())
        val_parts.append(np.asarray(d["pbl"], dtype=np.float64).ravel())
    if not lat_parts:
        return [], 0
    lat = np.concatenate(lat_parts)
    lon = np.concatenate(lon_parts)
    val = np.concatenate(val_parts)
    ok = np.isfinite(lat) & np.isfinite(lon) & (np.abs(lat) <= 90) & (np.abs(lon) <= 180)
    yi = np.full(lat.shape, -1, dtype=np.int64)
    xi = np.full(lon.shape, -1, dtype=np.int64)
    yi[ok] = np.floor((lat[ok] + 90.0) / 0.25).astype(np.int64)
    xi[ok] = np.floor((lon[ok] + 180.0) / 0.25).astype(np.int64)
    inbox = ok & (yi >= LAT_START) & (yi <= LAT_END) & (xi >= LON_START) & (xi <= LON_END)
    pos = np.nonzero(inbox)[0]  # flattened order = orbit, line, scene
    if pos.size == 0:
        return [], 0
    cell = yi[pos] * 1440 + xi[pos]
    _, first = np.unique(cell, return_index=True)  # first occurrence per cell
    pick = pos[first]
    rows = []
    for p in pick:
        v = val[p]
        if not np.isfinite(v) or v < -999 or v > 1000:
            continue  # fill (-1.2676506e+30), as in _parse_opendap_ascii
        rows.append({
            "date": date_str,
            "lat": round(-89.875 + int(yi[p]) * 0.25, 3),
            "lon": round(-179.875 + int(xi[p]) * 0.25, 3),
            "so2": round(round(float(v), 2), 4),
        })
    return rows, int(pick.size)


def _read_l2_orbit(data: bytes) -> dict:
    import h5py

    with h5py.File(io.BytesIO(data), "r") as f:
        return {
            "lat": f[f"{L2_GROUP}/Geolocation Fields/Latitude"][...],
            "lon": f[f"{L2_GROUP}/Geolocation Fields/Longitude"][...],
            "pbl": f[f"{L2_GROUP}/Data Fields/ColumnAmountSO2_PBL"][...],
        }


async def fetch_so2_day_l2(session: aiohttp.ClientSession, date: datetime) -> tuple[list[dict], str, str]:
    """Japan-box SO2 for a date without an OMSO2G granule, from OMSO2 v004 L2 orbits."""
    date_str = date.strftime("%Y-%m-%d")
    if not EARTHDATA_TOKEN:
        if not hasattr(fetch_so2_day_l2, "_notoken_warned"):
            fetch_so2_day_l2._notoken_warned = True
            logger.info("SO2 L2: no EARTHDATA_TOKEN -- L2 fallback needs the Bearer token")
        return [], "error", "OMSO2-L2"
    # L2 orbits are published one by one: a date fetched too early would be
    # stored from a partial orbit set (wrong candidate 0) and never fetched again.
    if date.date() >= datetime.now(timezone.utc).date() - timedelta(days=L2_SETTLE_DAYS):
        if not hasattr(fetch_so2_day_l2, "_young_noted"):
            fetch_so2_day_l2._young_noted = True
            logger.info("SO2 %s: younger than %d days -- L2 orbits may still be arriving, "
                        "left for a later run", date_str, L2_SETTLE_DAYS)
        return [], "error", "OMSO2-L2"
    orbits = await _l2_orbits_for_date(date_str)
    if orbits is None:
        return [], "error", "OMSO2-L2"
    if not orbits:
        logger.info("SO2 %s: no OMSO2 L2 orbit over Japan in CMR either", date_str)
        return [], "no_granule", "OMSO2-L2"

    loaded: list[tuple[int, dict]] = []
    for orb, url in orbits:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                status, data = await earthdata_fetch_bytes(session, url, timeout=TIMEOUT)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                status, data = 0, b""
                logger.info("SO2 %s L2 orbit %d: %s", date_str, orb, type(e).__name__)
            if status == 200 and data[:8] == b"\x89HDF\r\n\x1a\n":
                break
            if status in (401, 403):
                _note_auth_failure(date_str, status)
                return [], "error", "OMSO2-L2"
            if attempt == MAX_RETRIES:
                logger.warning("SO2 %s L2 orbit %d: HTTP %d (%d bytes, not HDF5)",
                               date_str, orb, status, len(data))
                return [], "error", "OMSO2-L2"  # every orbit is needed for candidate order
            await asyncio.sleep(2 ** attempt)
        try:
            loaded.append((orb, _read_l2_orbit(data)))
        except Exception as e:  # any bad file: retry the date next run, never crash main()
            logger.warning("SO2 %s L2 orbit %d: unreadable (%s: %s)", date_str, orb, type(e).__name__, e)
            return [], "error", "OMSO2-L2"
    fetch_so2_day.auth_failures = 0
    rows, n_cells = grid_l2_orbits(loaded, date_str)
    logger.info("SO2 %s: rebuilt from %d L2 orbits -> %d cells, %d valid",
                date_str, len(loaded), n_cells, len(rows))
    return rows, ("rows" if rows else "empty"), "OMSO2-L2"


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

    # Dates that finished empty on an earlier run (no granule / all fill).
    # SO2_RECHECK=1 ignores the log, e.g. after a reprocessing.
    async with safe_connect() as db:
        log_rows = await db.execute_fetchall(
            "SELECT observed_at FROM so2_fetch_log WHERE kind IN ('empty', 'no_granule')"
        )
    logged_empty = set() if os.environ.get("SO2_RECHECK") == "1" else set(r[0] for r in log_rows)
    logger.info("SO2 fetch log: %d dates finished empty on earlier runs%s",
                len(log_rows), " (ignored: SO2_RECHECK=1)" if not logged_empty and log_rows else "")

    # Generate all dates from START_YEAR to yesterday
    today = datetime.now(timezone.utc).date()
    settle_cut = today - timedelta(days=SETTLE_DAYS)
    d = datetime(START_YEAR, 1, 1).date()
    target_dates = []
    skipped_logged = 0
    while d < today:
        ds = d.strftime("%Y-%m-%d")
        if ds in logged_empty:
            skipped_logged += 1
        elif ds not in existing_dates:
            target_dates.append(datetime(d.year, d.month, d.day))
        d += timedelta(days=1)
    if skipped_logged:
        logger.info("SO2: skipping %d dates logged empty", skipped_logged)

    # Prioritize recent gaps first (most impactful for test period), then backfill
    # Split: 2011+ dates first (analysis period), then pre-2011 (baseline building)
    analysis_dates = [dt for dt in target_dates if dt.year >= 2011]
    backfill_dates = [dt for dt in target_dates if dt.year < 2011]
    max_dates = int(os.environ.get("SO2_MAX_DATES", "600"))
    dates_to_fetch = (analysis_dates + backfill_dates)[:max_dates]

    if not dates_to_fetch:
        logger.info("All SO2 target dates already fetched")
        return

    logger.info("SO2: %d dates to fetch", len(dates_to_fetch))

    total_records = 0
    kinds: dict[str, int] = {}
    l2_dates = 0
    t0 = time.monotonic()
    session = await get_earthdata_session()
    try:
        for i, date in enumerate(dates_to_fetch):
            if time.monotonic() - t0 > TIME_BUDGET_S:
                logger.info("SO2: time budget (%.0f min) reached after %d/%d dates -- "
                            "the next run continues", TIME_BUDGET_S / 60, i, len(dates_to_fetch))
                break
            rows, kind, source = await fetch_so2_day(session, date)
            kinds[kind] = kinds.get(kind, 0) + 1
            if source == "OMSO2-L2" and kind == "rows":
                l2_dates += 1
            if getattr(fetch_so2_day, "auth_failures", 0) >= 3:
                logger.warning(
                    "SO2: stopping after 3 consecutive auth failures (HTTP 401/403; a "
                    "Bearer timeout also ends here, via the Basic fallback). "
                    "Earthdata Cloud OPeNDAP needs a working EARTHDATA_TOKEN; if the "
                    "test-earthdata-auth run shows the token is live, this was slowness "
                    "and the next run continues. Renew at https://urs.earthdata.nasa.gov/profile"
                )
                break
            date_str = date.strftime("%Y-%m-%d")
            settled = date.date() < settle_cut
            if rows or (kind in ("empty", "no_granule") and settled):
                async with safe_connect() as db:
                    if rows:
                        await db.executemany(
                            """INSERT OR IGNORE INTO so2_column
                               (observed_at, cell_lat, cell_lon, so2_du, received_at)
                               VALUES (?, ?, ?, ?, ?)""",
                            [(r["date"], r["lat"], r["lon"], r["so2"], now) for r in rows],
                        )
                    # Empty dates younger than SETTLE_DAYS are not logged, so they are retried.
                    if kind != "error" and (rows or settled):
                        await db.execute(
                            """INSERT OR REPLACE INTO so2_fetch_log
                               (observed_at, source, kind, n_records, fetched_at)
                               VALUES (?, ?, ?, ?, ?)""",
                            (date_str, source, kind, len(rows), now),
                        )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 20 == 0:
                logger.info("SO2: %d/%d dates, %d records", i + 1, len(dates_to_fetch), total_records)

            await asyncio.sleep(1.0)
    finally:
        await session.close()

    # Diagnostic summary: why 0 records?
    summary = ", ".join(f"{k} {v}" for k, v in sorted(kinds.items()))
    if total_records == 0 and dates_to_fetch:
        logger.info(
            "SO2 fetch complete: 0 records from %d dates (%s). "
            "'empty' = every Japan cell is fill, 'no_granule' = neither OMSO2G nor "
            "OMSO2 L2 has a granule, 'error' = auth/HTTP/CMR failure (see the INFO "
            "lines above; 'auth failed (HTTP 401/403)' means the token).",
            len(dates_to_fetch), summary,
        )
    else:
        logger.info("SO2 fetch complete: %d total records from %d dates (%s; %d dates rebuilt from L2)",
                     total_records, len(dates_to_fetch), summary, l2_dates)


if __name__ == "__main__":
    asyncio.run(main())
