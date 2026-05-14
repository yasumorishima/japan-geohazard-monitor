"""Fetch cloud fraction data from NASA MODIS for earthquake cloud analysis.

"Earthquake clouds" — unusual linear cloud formations along fault lines —
have been reported before major earthquakes. While controversial, the
physical mechanism is plausible:

Physical mechanism:
    Crustal stress → radon/ion release from faults → atmospheric ionization
    → water vapor condensation nuclei → linear cloud formation along
    fault trace. This is part of the LAIC (Lithosphere-Atmosphere-Ionosphere
    Coupling) model.

    Statistical analysis with satellite cloud fraction data can objectively
    test whether cloud patterns anomalies occur before earthquakes.

Data source: NASA MODIS Terra/Aqua Level 3 daily (1° global grid)
    - Product: MOD08_D3 (Terra) / MYD08_D3 (Aqua) — Cloud_Fraction_Mean
    - OPeNDAP access via LAADS DAAC
    - Requires Earthdata authentication

Target features:
    - cloud_fraction_anomaly: cloud cover deviation from 30-day baseline (σ)

References:
    - Guangmeng & Jie (2013) Nat. Hazards Earth Syst. Sci. 13:927-934
    - Shou (2006) Terr. Atmos. Ocean. Sci. 17:395-414
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH
from earthdata_auth import get_earthdata_session, EARTHDATA_TOKEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# LAADS DAAC direct HDF4 download for MOD08_D3 (MODIS Terra daily L3 atmosphere).
# Switched from OPeNDAP `.ascii?Cloud_Fraction_Mean` to direct .hdf download because
# LAADS OPeNDAP does NOT honor Bearer tokens (diagnosis 2026-05-14): every OPeNDAP
# request returns 302 -> /oauth/login regardless of Authorization header, ultimately
# delivering an HTTP 200 HTML OAuth landing page (len ~11062). NASA's
# "Download Files Using EDL Tokens" doc only documents Bearer for /archive/... paths.
LAADS_ARCHIVE = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD08_D3"

# Japan bbox in 1° grid
# Lat: -90 to 90 (180 cells), Lon: -180 to 180 (360 cells)
# Japan: lat 24-46 → indices 114 to 136
# Japan: lon 122-150 → indices 302 to 330
LAT_START = 114
LAT_END = 136
LON_START = 302
LON_END = 330

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)
START_YEAR = 2000


async def init_cloud_table():
    """Create cloud fraction table.

    If the existing cloud_fraction table is unreadable (page-level
    corruption — root cause of the 2026-04-12 "database disk image is
    malformed" incident), drop it so the CREATE below repopulates
    cleanly. BQ retains ~123K rows loaded pre-corruption, so the drop
    is recoverable.
    """
    import sqlite3 as _sqlite3  # narrow exception scope
    async with safe_connect() as db:
        try:
            await db.execute("SELECT COUNT(*) FROM cloud_fraction")
        except _sqlite3.OperationalError as e:
            # "no such table" is the expected path on a fresh DB; swallow.
            # Must come BEFORE DatabaseError (OperationalError is a subclass).
            if "no such table" not in str(e).lower():
                raise
        except _sqlite3.DatabaseError as e:
            # Only the specific corruption signatures; re-raise anything else
            # (UNIQUE constraint, locked, schema change, etc.) so real errors
            # aren't silently swallowed.
            msg = str(e).lower()
            if (
                "malformed" in msg
                or "database disk image" in msg
                or "not a database" in msg
                or "corrupt" in msg
            ):
                logger.warning(
                    "cloud_fraction unreadable (%s) — dropping to recover", e
                )
                await db.execute("DROP TABLE IF EXISTS cloud_fraction")
                await db.commit()
            else:
                raise
        await db.execute("""
            CREATE TABLE IF NOT EXISTS cloud_fraction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                cell_lat REAL NOT NULL,
                cell_lon REAL NOT NULL,
                cloud_frac REAL NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at, cell_lat, cell_lon)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_cloud_time
            ON cloud_fraction(observed_at)
        """)
        await db.commit()


async def _resolve_filename(session: aiohttp.ClientSession, year: int, doy: int) -> str | None:
    """Resolve MOD08_D3 filename from LAADS DAAC directory listing."""
    dir_url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD08_D3/{year}/{doy:03d}.json"
    # Emit one diagnostic line per (year, doy) miss, up to 3 per year.
    flag = f"_diag_resolve_{year}"
    diag_count = getattr(_resolve_filename, flag, 0)
    try:
        headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"} if EARTHDATA_TOKEN else {}
        async with session.get(dir_url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                if diag_count < 3:
                    body_preview = (await resp.text())[:300] if resp.status != 404 else "(404)"
                    logger.warning(
                        "Cloud resolve %d/%03d: HTTP %d. token=%s. body=%r",
                        year, doy, resp.status,
                        "present" if EARTHDATA_TOKEN else "MISSING", body_preview,
                    )
                    setattr(_resolve_filename, flag, diag_count + 1)
                return None
            data = await resp.json(content_type=None)
            # LAADS returns {"content": [file objects]}
            items = data.get("content", data) if isinstance(data, dict) else data
            listed_names: list[str] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = item.get("name", "")
                if name.startswith("MOD08_D3") and name.endswith(".hdf"):
                    return name
                if name:
                    listed_names.append(name)
            # Reached here = no MOD08_D3 .hdf file present in the listing.
            if diag_count < 3:
                sample = listed_names[:5] if listed_names else ["(empty listing)"]
                logger.warning(
                    "Cloud resolve %d/%03d: MOD08_D3 .hdf not present. "
                    "listing has %d non-matching items. sample=%s",
                    year, doy, len(listed_names), sample,
                )
                setattr(_resolve_filename, flag, diag_count + 1)
    except Exception as exc:
        if diag_count < 3:
            logger.warning(
                "Cloud resolve %d/%03d: exception %s: %s",
                year, doy, type(exc).__name__, exc,
            )
            setattr(_resolve_filename, flag, diag_count + 1)
    return None


async def fetch_cloud_day(session: aiohttp.ClientSession, date: datetime) -> list[dict]:
    """Fetch cloud fraction for one day via LAADS direct HDF4 download.

    Uses /archive/... path with Bearer token (the only LAADS path where Bearer
    is officially supported). The HDF4 file is downloaded to memory and parsed
    via pyhdf. Japan bbox subsetting is done after parsing (HDF4 does not
    support server-side subsetting on this endpoint).
    """
    if not EARTHDATA_TOKEN:
        return []

    year = date.year
    doy = date.timetuple().tm_yday
    date_str = date.strftime("%Y-%m-%d")

    filename = await _resolve_filename(session, year, doy)
    if not filename:
        return []

    url = f"{LAADS_ARCHIVE}/{year}/{doy:03d}/{filename}"
    headers = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(
                url, headers=headers, timeout=TIMEOUT, allow_redirects=True
            ) as resp:
                if resp.status in (401, 403):
                    logger.info(
                        "Cloud fraction requires Earthdata auth (HTTP %d)", resp.status
                    )
                    return []
                if resp.status == 404:
                    return []
                if resp.status != 200:
                    if attempt == MAX_RETRIES:
                        body_head = (await resp.text(errors="replace"))[:200]
                        logger.warning(
                            "Cloud %s: HTTP %d on .hdf download preview=%r",
                            date_str, resp.status, body_head,
                        )
                    await asyncio.sleep(2 ** attempt)
                    continue

                content_bytes = await resp.read()
                if not content_bytes or len(content_bytes) < 1000:
                    diag_count = getattr(fetch_cloud_day, "_diag_empty", 0)
                    if diag_count < 5:
                        logger.warning(
                            "Cloud %s: empty/tiny .hdf body (len=%d)",
                            date_str, len(content_bytes),
                        )
                        fetch_cloud_day._diag_empty = diag_count + 1
                    return []
                # HDF4 magic header bytes
                if content_bytes[:4] != b"":
                    diag_count = getattr(fetch_cloud_day, "_diag_nothdf", 0)
                    if diag_count < 5:
                        preview = content_bytes[:200]
                        logger.warning(
                            "Cloud %s: not HDF4 magic (auth/redirect issue?) "
                            "len=%d preview=%r",
                            date_str, len(content_bytes), preview,
                        )
                        fetch_cloud_day._diag_nothdf = diag_count + 1
                    return []
                return _parse_cloud_hdf4(content_bytes, date_str)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.debug("Cloud %s: %s", date_str, type(e).__name__)
            await asyncio.sleep(2 ** attempt)

    return []


def _parse_cloud_hdf4(content_bytes: bytes, date_str: str) -> list[dict]:
    """Parse a MOD08_D3 HDF4 file (bytes) and extract Cloud_Fraction_Mean.

    MOD08_D3 collection 61 layout:
        - Dataset: Cloud_Fraction_Mean  (Int16, 180x360)
        - YDim row 0   = -89.5 deg lat (south first), row 179 = +89.5 deg
        - XDim col 0   = -179.5 deg lon, col 359 = +179.5 deg
        - scale_factor (HDF attr): 0.0001  -> raw * scale = cloud fraction in [0, 1]
        - _FillValue   (HDF attr): -9999
    Japan bbox slicing matches the previous OPeNDAP code path.
    """
    import os as _os
    import tempfile as _tempfile

    from pyhdf.SD import SD, SDC  # local import: pyhdf only loaded on the fetch path

    tmp = _tempfile.NamedTemporaryFile(suffix=".hdf", delete=False)
    tmp_path = tmp.name
    try:
        tmp.write(content_bytes)
        tmp.close()
        try:
            hdf = SD(tmp_path, SDC.READ)
            ds = hdf.select("Cloud_Fraction_Mean")
            data = ds[:]
            attrs = ds.attributes()
            scale = float(attrs.get("scale_factor", 0.0001))
            offset = float(attrs.get("add_offset", 0.0))
            fill = int(attrs.get("_FillValue", -9999))
            ds.endaccess()
            hdf.end()
        except Exception as e:
            diag_count = getattr(_parse_cloud_hdf4, "_diag_parse", 0)
            if diag_count < 5:
                logger.warning(
                    "Cloud %s: HDF parse failed: %s: %s",
                    date_str, type(e).__name__, e,
                )
                _parse_cloud_hdf4._diag_parse = diag_count + 1
            return []
    finally:
        try:
            _os.unlink(tmp_path)
        except OSError:
            pass

    n_rows, n_cols = data.shape
    rows: list[dict] = []
    for row_idx in range(LAT_START, min(LAT_END + 1, n_rows)):
        lat = -89.5 + row_idx
        for lon_idx in range(LON_START, min(LON_END + 1, n_cols)):
            raw = int(data[row_idx, lon_idx])
            if raw == fill or raw < 0:
                continue
            val = (raw - offset) * scale
            if val > 1.1:
                continue
            lon = -179.5 + lon_idx
            rows.append({
                "date": date_str,
                "lat": round(lat, 1),
                "lon": round(lon, 1),
                "cloud_frac": round(val, 4),
            })
    return rows


async def main():
    # Drop/recreate cloud_fraction BEFORE init_db so a corrupt cloud_fraction
    # page can't poison any subsequent CREATE inside init_db. (init_db is
    # idempotent CREATE IF NOT EXISTS, so running it second is harmless.)
    await init_cloud_table()
    await init_db()

    now = datetime.now(timezone.utc).isoformat()

    if not EARTHDATA_TOKEN:
        logger.info(
            "Cloud fraction fetch: EARTHDATA_TOKEN not set. "
            "Cloud features will be excluded via dynamic selection."
        )
        return

    # Continuous daily fetch — ML anomaly detection requires full baselines
    async with safe_connect() as db:
        existing = await db.execute_fetchall(
            "SELECT DISTINCT observed_at FROM cloud_fraction"
        )
    existing_dates = set(r[0] for r in existing) if existing else set()

    # Generate all dates from START_YEAR to yesterday
    today = datetime.now(timezone.utc).date()
    d = datetime(START_YEAR, 1, 1).date()
    target_dates = []
    while d < today:
        ds = d.strftime("%Y-%m-%d")
        if ds not in existing_dates:
            target_dates.append(datetime(d.year, d.month, d.day))
        d += timedelta(days=1)

    # Prioritize analysis period (2011+) first, then backfill pre-2011
    analysis_dates = [dt for dt in target_dates if dt.year >= 2011]
    backfill_dates = [dt for dt in target_dates if dt.year < 2011]
    max_dates = int(os.environ.get("CLOUD_MAX_DATES", "600"))
    dates_to_fetch = (analysis_dates + backfill_dates)[:max_dates]

    if not dates_to_fetch:
        logger.info("All cloud fraction target dates already fetched")
        return

    logger.info("Cloud fraction: %d dates to fetch", len(dates_to_fetch))

    total_records = 0
    session = await get_earthdata_session()
    try:
        for i, date in enumerate(dates_to_fetch):
            rows = await fetch_cloud_day(session, date)
            if rows:
                async with safe_connect() as db:
                    await db.executemany(
                        """INSERT OR IGNORE INTO cloud_fraction
                           (observed_at, cell_lat, cell_lon, cloud_frac, received_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        [(r["date"], r["lat"], r["lon"], r["cloud_frac"], now) for r in rows],
                    )
                    await db.commit()
                total_records += len(rows)

            if (i + 1) % 20 == 0:
                logger.info("Cloud: %d/%d dates, %d records",
                            i + 1, len(dates_to_fetch), total_records)
            await asyncio.sleep(1.0)
    finally:
        await session.close()

    logger.info("Cloud fraction fetch complete: %d records", total_records)


if __name__ == "__main__":
    asyncio.run(main())
