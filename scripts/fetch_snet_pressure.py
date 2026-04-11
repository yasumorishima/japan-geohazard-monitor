"""Fetch S-net seafloor water pressure data via NIED Hi-net portal using HinetPy.

S-net (Seafloor Observation Network for Earthquakes and Tsunamis along the
Japan Trench) consists of 150 ocean bottom stations connected by fiber-optic
cables. Each station includes an accelerometer, velocity seismometer, and water
pressure gauge. The pressure gauges measure absolute water column pressure with
sub-Pa precision at 10 Hz. Pre-seismic slow-slip events cause vertical seafloor
displacement of mm-cm, detectable as sustained pressure changes of ~10 Pa/mm.
S-net's position directly above the Japan Trench subduction zone makes it the
highest-sensitivity network for detecting pre-seismic deformation.

This script:
  1. Checks HINET_USER / HINET_PASS environment variables (from GitHub Secrets)
  2. Connects to NIED Hi-net via HinetPy
  3. Fetches station metadata for S-net pressure/acceleration network (code 0120A)
  4. Downloads a small continuous waveform segment (5 min) for the previous day
  5. Decodes WIN32 binary format and computes daily aggregated pressure statistics
  6. Stores results in SQLite table ``snet_pressure``

HinetPy session limits:
  - Max 5 minutes per request, max ~50 MB per session
  - Data available after ~2 hours delay from real time

References:
    Aoi et al. (2020) Earth Planets Space 72:126 (S-net overview)
    Tanaka et al. (2019) EPS 71:120 (S-net pressure data)
"""

import asyncio
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH
from db import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# S-net pressure/acceleration network code in HinetPy
SNET_PRESSURE_CODE = "0120A"

# Duration per request in minutes (HinetPy limit: max 5 min for high-rate data)
REQUEST_DURATION_MIN = 5

# Segment counts per fetched day. Recent days get more segments for robust
# rolling statistics; backfill days get fewer to stretch the daily quota.
SEGMENTS_RECENT = 6
SEGMENTS_BACKFILL = 2

# How many most-recent days are always re-checked (even if some records exist).
RECENT_DAYS = 7

# NIED Hi-net enforces a per-account daily download quota (~200 req/day).
# Budget this run conservatively so that both recent refresh and backfill fit
# in the same day without tripping the quota. Overridable for smoke tests.
MAX_REQUESTS_PER_RUN = int(os.environ.get("SNET_PRESSURE_MAX_REQUESTS", "120"))

# S-net first station installation: 2016-05 (final segment commissioned FY2016).
# No seafloor pressure data physically exists before this date.
SNET_EARLIEST_DATE = datetime(2016, 5, 1)

# S-net station approximate locations along the Japan Trench
# Stations are named N.Snnnn where nnnn is the station number (0101-0653)
# Latitude/longitude are approximate centroids per cable segment
SNET_CABLE_SEGMENTS = {
    "S1": {"lat_range": (39.5, 42.0), "lon_range": (142.5, 145.5), "desc": "Off Tokachi"},
    "S2": {"lat_range": (38.0, 40.0), "lon_range": (142.0, 144.5), "desc": "Off Sanriku"},
    "S3": {"lat_range": (36.5, 38.5), "lon_range": (141.5, 144.0), "desc": "Off Miyagi"},
    "S4": {"lat_range": (35.0, 37.0), "lon_range": (140.5, 143.0), "desc": "Off Fukushima"},
    "S5": {"lat_range": (34.0, 36.0), "lon_range": (140.0, 142.5), "desc": "Off Boso"},
    "S6": {"lat_range": (33.0, 35.0), "lon_range": (139.0, 141.5), "desc": "Off Tokai"},
}


async def ensure_table(db: aiosqlite.Connection) -> None:
    """Create snet_pressure table if it does not exist."""
    await db.execute("""
        CREATE TABLE IF NOT EXISTS snet_pressure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            pressure_mean_hpa REAL,
            pressure_std_hpa REAL,
            latitude REAL,
            longitude REAL,
            n_samples INTEGER,
            received_at TEXT NOT NULL,
            UNIQUE(station_id, observed_at)
        )
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_snet_pressure_time
        ON snet_pressure(observed_at)
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_snet_pressure_station
        ON snet_pressure(station_id)
    """)
    await db.commit()


def _check_credentials() -> tuple[str, str] | None:
    """Return (user, password) from env vars, or None if not set."""
    user = os.environ.get("HINET_USER", "").strip()
    password = os.environ.get("HINET_PASS", "").strip()
    if not user or not password:
        return None
    return user, password


def _fetch_snet_data(
    user: str,
    password: str,
    target_date: datetime,
    n_segments: int,
) -> list[dict]:
    """Synchronous function to fetch S-net data via HinetPy.

    HinetPy is synchronous (uses requests internally), so this runs in a
    thread executor from the async context.

    Args:
        user, password: NIED Hi-net credentials.
        target_date: naive datetime (UTC) for the day to fetch.
        n_segments: number of 5-minute segments to spread across the day.
            Each segment consumes one HinetPy request from the daily quota.

    Returns a list of dicts with keys:
        station_id, observed_at, pressure_mean_hpa, pressure_std_hpa,
        latitude, longitude, n_samples
    """
    try:
        from HinetPy import Client
    except ImportError:
        logger.error(
            "HinetPy is not installed. Install with: pip install HinetPy"
        )
        return []

    results = []
    work_dir = tempfile.mkdtemp(prefix="snet_")
    logger.info("Working directory: %s", work_dir)

    try:
        client = Client(user, password)
        logger.info("Authenticated to NIED Hi-net successfully")
    except Exception as exc:
        logger.error("Hi-net authentication failed: %s", exc)
        return []

    # Fetch station metadata
    station_coords = {}
    try:
        stations = client.get_station_list(SNET_PRESSURE_CODE)
        if stations is not None:
            for st in stations:
                # HinetPy station list: .code is network (0120A for all),
                # .name is station ID (N.S1N01, etc.)
                sid = getattr(st, "name", None) or getattr(st, "code", None)
                lat = getattr(st, "latitude", None)
                lon = getattr(st, "longitude", None)
                if sid and lat is not None and lon is not None:
                    station_coords[str(sid)] = (float(lat), float(lon))
            logger.info(
                "Retrieved metadata for %d S-net pressure stations", len(station_coords)
            )
        else:
            logger.warning("get_station_list returned None; station coordinates unavailable")
    except Exception as exc:
        logger.warning("Could not fetch station metadata: %s", exc)

    # Spread `n_segments` evenly across the 24-hour day so each segment
    # samples a different local-time window (dawn / midday / dusk / night).
    if n_segments <= 0:
        segment_hours: list[int] = []
    else:
        step = max(24 // n_segments, 1)
        segment_hours = [h for h in range(0, 24, step)][:n_segments]

    for hour in segment_hours:
        start = target_date.replace(hour=hour, minute=0, second=0, microsecond=0)
        logger.info(
            "Requesting %d-min segment starting %s for network %s",
            REQUEST_DURATION_MIN,
            start.strftime("%Y-%m-%d %H:%M"),
            SNET_PRESSURE_CODE,
        )

        try:
            data = client.get_continuous_waveform(
                SNET_PRESSURE_CODE,
                start,
                REQUEST_DURATION_MIN,
                outdir=work_dir,
            )
            if data is None:
                logger.warning("No data returned for segment starting %s", start)
                continue

            # data is (win32_file, channel_table_file) or similar
            if isinstance(data, tuple) and len(data) == 2:
                win32_file, ch_table = data
            else:
                logger.warning("Unexpected return format from get_continuous_waveform: %s", type(data))
                continue

            if win32_file is None:
                logger.warning("WIN32 file is None for segment starting %s", start)
                continue

            logger.info("Decoding WIN32 file: %s", win32_file)

            # Decode WIN32 → SAC files (HinetPy v0.10+: use win32.extract_sac)
            try:
                from HinetPy import win32 as hinetwin32
                sac_files = hinetwin32.extract_sac(win32_file, ch_table, outdir=work_dir)
            except Exception as exc:
                logger.warning("Failed to decode WIN32 data: %s", exc)
                continue

            if not sac_files:
                logger.warning("No SAC files produced from decoding")
                continue

            # Process SAC files — look for pressure channels
            # S-net pressure channels typically have component code 'U' (up = pressure)
            # or channel names containing 'pressure' / 'P'
            _process_sac_files(
                sac_files, start, station_coords, results, work_dir
            )

        except Exception as exc:
            exc_str = str(exc).lower()
            if "quota" in exc_str or "limit" in exc_str:
                logger.error("Hi-net data quota exceeded: %s", exc)
                break
            elif "auth" in exc_str or "login" in exc_str or "401" in exc_str:
                logger.error("Hi-net authentication error: %s", exc)
                break
            else:
                logger.warning("Error fetching segment at %s: %s", start, exc)
                continue

    # Clean up temp files
    try:
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)
    except Exception:
        pass

    return results


def _process_sac_files(
    sac_files: list,
    segment_start: datetime,
    station_coords: dict[str, tuple[float, float]],
    results: list[dict],
    work_dir: str,
) -> None:
    """Parse decoded SAC files and extract pressure statistics.

    S-net pressure channels in WIN32/SAC format:
    - Channel component 'U' or 'P' typically indicates pressure gauge
    - Station IDs follow pattern like N.S0101, N.S0102, etc.
    """
    try:
        import numpy as np
    except ImportError:
        logger.error("numpy is required for SAC file processing")
        return

    observed_at = segment_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    processed_stations = set()

    for sac_path in sac_files:
        sac_path = str(sac_path)
        # SAC filename convention from HinetPy: NETWORK.STATION.LOCATION.CHANNEL.SAC
        basename = Path(sac_path).stem
        parts = basename.split(".")
        if len(parts) < 4:
            continue

        station_id = parts[1] if len(parts) > 1 else basename
        channel = parts[3] if len(parts) > 3 else parts[-1]

        # Filter for pressure channels — typically component 'U' (vertical/pressure)
        # S-net pressure gauge channels end with 'U' or contain 'BPR' (Bottom Pressure Recorder)
        is_pressure = (
            channel.endswith("U")
            or "BPR" in channel.upper()
            or "PRESSURE" in channel.upper()
        )
        if not is_pressure:
            continue

        if station_id in processed_stations:
            continue

        try:
            # Read SAC binary file
            data = _read_sac_data(sac_path)
            if data is None or len(data) == 0:
                continue

            # Convert raw counts to approximate pressure (hPa)
            # S-net pressure gauges: raw count scaling varies by station
            # Typical sensitivity: ~0.01 Pa per count
            # For now, store raw statistics and note the unit assumption
            mean_val = float(np.mean(data))
            std_val = float(np.std(data))
            n_samples = len(data)

            # Convert from Pa to hPa (1 hPa = 100 Pa)
            # Raw SAC values from S-net pressure gauges are typically in Pa
            pressure_mean_hpa = mean_val / 100.0
            pressure_std_hpa = std_val / 100.0

            lat, lon = station_coords.get(station_id, (None, None))

            results.append({
                "station_id": station_id,
                "observed_at": observed_at,
                "pressure_mean_hpa": pressure_mean_hpa,
                "pressure_std_hpa": pressure_std_hpa,
                "latitude": lat,
                "longitude": lon,
                "n_samples": n_samples,
            })
            processed_stations.add(station_id)

        except Exception as exc:
            logger.warning("Error processing SAC file %s: %s", sac_path, exc)


def _read_sac_data(filepath: str) -> "np.ndarray | None":
    """Read data section from a SAC binary file.

    SAC binary format:
    - Header: 632 bytes (70 floats + 40 ints + 24 strings)
    - Data: NPTS float32 values starting at byte 632
    """
    import struct

    import numpy as np

    try:
        with open(filepath, "rb") as f:
            header = f.read(632)
            if len(header) < 632:
                return None

            # NPTS is at int header position 9 (float header = 280 bytes, int offset 9*4 = 36)
            # Float section: 70 * 4 = 280 bytes
            # Int section starts at 280; NPTS is the 10th int (index 9), at 280 + 9*4 = 316
            npts = struct.unpack_from("<i", header, 316)[0]
            if npts <= 0 or npts > 10_000_000:
                # Try big-endian
                npts = struct.unpack_from(">i", header, 316)[0]
                if npts <= 0 or npts > 10_000_000:
                    return None
                data = np.frombuffer(f.read(npts * 4), dtype=">f4")
            else:
                data = np.frombuffer(f.read(npts * 4), dtype="<f4")

            if len(data) != npts:
                return None
            return data

    except Exception as exc:
        logger.warning("Failed to read SAC file %s: %s", filepath, exc)
        return None


async def _store_records(records: list[dict], target_date_str: str) -> int:
    """Insert one day's records into snet_pressure. Returns inserted row count."""
    if not records:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    async with safe_connect() as db:
        for rec in records:
            try:
                await db.execute(
                    """INSERT OR IGNORE INTO snet_pressure
                       (station_id, observed_at, pressure_mean_hpa, pressure_std_hpa,
                        latitude, longitude, n_samples, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        rec["station_id"],
                        rec["observed_at"],
                        rec["pressure_mean_hpa"],
                        rec["pressure_std_hpa"],
                        rec["latitude"],
                        rec["longitude"],
                        rec["n_samples"],
                        now,
                    ),
                )
                inserted += 1
            except Exception as exc:
                logger.warning(
                    "Failed to insert record for %s on %s: %s",
                    rec.get("station_id", "?"), target_date_str, exc,
                )
        await db.commit()
    return inserted


async def _load_existing_dates() -> set[str]:
    """Return the set of YYYY-MM-DD strings already present in snet_pressure."""
    async with safe_connect() as db:
        cursor = await db.execute(
            "SELECT DISTINCT substr(observed_at, 1, 10) FROM snet_pressure"
        )
        rows = await cursor.fetchall()
    return {r[0] for r in rows if r and r[0]}


def _plan_target_dates(
    existing: set[str],
    today: datetime,
) -> tuple[list[datetime], list[datetime]]:
    """Return (recent_dates, backfill_dates) to attempt this run.

    Recent: the last RECENT_DAYS days excluding today (data has ~2h delay,
    so yesterday is the freshest reliable day). Always re-checked so we
    catch late-arriving or partial data.

    Backfill: all dates from SNET_EARLIEST_DATE up to (today - RECENT_DAYS),
    oldest-first, excluding any date already in the DB. Oldest-first builds
    a contiguous block forward from 2016-05 which aligns with the
    'continuous 2011-today' coverage goal.
    """
    today_midnight = today.replace(hour=0, minute=0, second=0, microsecond=0)

    recent_dates: list[datetime] = []
    for i in range(1, RECENT_DAYS + 1):
        d = today_midnight - timedelta(days=i)
        recent_dates.append(d)

    backfill_boundary = today_midnight - timedelta(days=RECENT_DAYS + 1)
    backfill_dates: list[datetime] = []
    cursor = SNET_EARLIEST_DATE
    while cursor <= backfill_boundary:
        if cursor.strftime("%Y-%m-%d") not in existing:
            backfill_dates.append(cursor)
        cursor += timedelta(days=1)

    return recent_dates, backfill_dates


async def main() -> None:
    """Fetch S-net pressure data with recent-refresh + oldest-first backfill."""
    credentials = _check_credentials()
    if credentials is None:
        logger.warning(
            "HINET_USER and/or HINET_PASS environment variables not set. "
            "S-net data requires NIED Hi-net registration "
            "(https://hinetwww11.bosai.go.jp/nied/registration/). "
            "Exiting gracefully."
        )
        return

    user, password = credentials
    logger.info("NIED Hi-net credentials found, starting S-net pressure data fetch")

    await init_db()
    async with safe_connect() as db:
        await ensure_table(db)

    existing = await _load_existing_dates()
    logger.info("snet_pressure: %d existing dates in DB", len(existing))

    today = datetime.utcnow()
    recent_dates, backfill_dates = _plan_target_dates(existing, today)

    logger.info(
        "Plan: %d recent days (re-refresh) + %d backfill days available (from %s)",
        len(recent_dates),
        len(backfill_dates),
        SNET_EARLIEST_DATE.strftime("%Y-%m-%d"),
    )
    logger.info(
        "Budget: %d requests this run (recent=%d seg/day, backfill=%d seg/day)",
        MAX_REQUESTS_PER_RUN, SEGMENTS_RECENT, SEGMENTS_BACKFILL,
    )

    loop = asyncio.get_event_loop()
    requests_used = 0
    total_inserted = 0
    days_fetched = 0
    days_empty = 0

    async def fetch_one(d: datetime, n_segments: int) -> None:
        nonlocal requests_used, total_inserted, days_fetched, days_empty
        date_str = d.strftime("%Y-%m-%d")
        # Skip recent days that already have enough data. We still re-check
        # if the count is suspiciously low (<50 rows) since partial days are
        # common when HinetPy decoding fails on a subset of channels.
        async with safe_connect() as db:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM snet_pressure WHERE observed_at LIKE ?",
                (f"{date_str}%",),
            )
            row = await cursor.fetchone()
            have = row[0] if row else 0
        if have >= 50:
            logger.info("%s: already %d rows, skipping", date_str, have)
            return

        logger.info(
            "Fetching %s (%d segments, budget %d/%d used)",
            date_str, n_segments, requests_used, MAX_REQUESTS_PER_RUN,
        )
        records = await loop.run_in_executor(
            None, _fetch_snet_data, user, password, d, n_segments
        )
        # Each segment attempted consumes one quota slot even if it failed.
        requests_used += n_segments
        if not records:
            logger.warning("%s: no records retrieved", date_str)
            days_empty += 1
            return
        inserted = await _store_records(records, date_str)
        total_inserted += inserted
        days_fetched += 1
        logger.info("%s: inserted %d rows", date_str, inserted)

    # Recent days first — always refresh the freshest week.
    for d in recent_dates:
        if requests_used + SEGMENTS_RECENT > MAX_REQUESTS_PER_RUN:
            logger.info("Budget exhausted before recent refresh complete")
            break
        await fetch_one(d, SEGMENTS_RECENT)

    # Backfill oldest-first with remaining budget.
    for d in backfill_dates:
        if requests_used + SEGMENTS_BACKFILL > MAX_REQUESTS_PER_RUN:
            logger.info("Budget exhausted during backfill")
            break
        await fetch_one(d, SEGMENTS_BACKFILL)

    # Coverage summary
    existing_after = await _load_existing_dates()
    total_available = (today - SNET_EARLIEST_DATE).days
    covered = sum(1 for d in existing_after if d >= SNET_EARLIEST_DATE.strftime("%Y-%m-%d"))
    coverage_pct = round(100.0 * covered / total_available, 1) if total_available > 0 else 0.0

    logger.info(
        "S-net pressure run complete: "
        "%d rows inserted across %d days (%d empty), %d/%d requests used. "
        "Coverage: %d/%d days (%.1f%%) since %s",
        total_inserted, days_fetched, days_empty,
        requests_used, MAX_REQUESTS_PER_RUN,
        covered, total_available, coverage_pct,
        SNET_EARLIEST_DATE.strftime("%Y-%m-%d"),
    )


if __name__ == "__main__":
    asyncio.run(main())
