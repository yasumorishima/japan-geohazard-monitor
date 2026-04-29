"""Fetch F-net waveform features from NIED — Phase 1 Step 5c.

F-net (Full Range Seismograph Network of Japan) is NIED's nationwide
broadband seismometer network, operated since August 1995. 73 land-based
stations across Japan with 3-component STS-1/STS-2 broadband sensors,
sampled at 100 Hz.

Network code: 0103 (single sensor type — broadband velocity).

Features extracted per station per day per segment (identical schema to
snet_waveform for cross-source ML feature parity):
    - RMS (vertical + horizontal)
    - H/V spectral ratio (Nakamura site response indicator)
    - Band-specific power: LF (0.1–1 Hz), HF (1–10 Hz)
    - Spectral slope (noise source characterization)
    - VLF power (0.01–0.1 Hz), VLF H/V ratio (broadband-specific)

Backfill strategy:
    - Initial 15 stations geographically distributed (env override FNET_STATIONS)
    - Recent 7 days (4 segments/day) + backfill gaps (1 segment/day)
    - HinetPy session limits: shared ~150 budget with S-net (split 60/60)
    - Reports per-region coverage via Discord

References:
    Okada et al. (2004) Earth Planets Space 56 (F-net description)
    Obara (2002) Nonvolcanic deep tremor — F-net first detection
    Ide et al. (2007) Slow earthquakes — F-net long-period waveforms
"""

import asyncio
import json
import logging
import math
import os
import struct
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH
from db import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FNET_NETWORK_CODE = "0103"
FNET_SENSOR_TYPE = "broadband"
# Backfill window starts at the practical operational era (2000-01-01)
# rather than the 1995 network inception, to keep gap_days realistic.
FNET_START_STR = "2000-01-01"

# HinetPy constraints — shared NIED account quota with S-net.
# Split: snet 60 + fnet 60 = 120 per run (out of NIED ~150 daily budget).
REQUEST_DURATION_MIN = 5
SEGMENTS_RECENT = 4
SEGMENTS_BACKFILL = 1
RECENT_DAYS = 7
MAX_BACKFILL_DAYS_PER_RUN = 5
QUOTA_COOLDOWN_SEC = 2
MAX_REQUESTS_PER_RUN = int(os.environ.get("FNET_MAX_REQUESTS", "60"))

# Phase D3: graceful partial-save before SIGTERM. The GHA step is killed at
# timeout-minutes (default 75 = 4500 s); we proactively stop ~5 min earlier
# so per-item saves and the artifact upload step can run cleanly.
STEP_BUDGET_SEC = int(os.environ.get("FNET_STEP_BUDGET_SEC", "4200"))
DEADLINE_MARGIN_SEC = int(os.environ.get("FNET_DEADLINE_MARGIN_SEC", "300"))

# Initial active station count (gradual rollout: 15 → 30 → 73).
# Geographic stratified sample by latitude (N→S) over all available stations.
MAX_ACTIVE_STATIONS = int(os.environ.get("FNET_MAX_STATIONS", "15"))

# Optional explicit station list (comma-separated, e.g. "N.HID,N.FUK,N.ASO").
# When set, overrides MAX_ACTIVE_STATIONS auto-selection.
# Set to "ALL" to fetch every station with no filter.
FNET_STATIONS_ENV = os.environ.get("FNET_STATIONS", "").strip()

# F-net geographic regions for spatial analysis (instead of S-net cable segments).
# Bounds chosen to partition the Japan archipelago including Ryukyu/Bonin chains.
FNET_REGIONS = {
    # Boundaries are half-open [lo, hi) on lat to avoid overlap; classify_region
    # iterates in dict insertion order which matches "order" field for ties.
    "F-HKD":   {"lat_range": (41.5, 46.0), "lon_range": (139.5, 146.0), "desc": "Hokkaido",          "order": 0},
    "F-TOH-N": {"lat_range": (39.5, 41.5), "lon_range": (139.0, 142.5), "desc": "Tohoku North",      "order": 1},
    "F-TOH-S": {"lat_range": (37.0, 39.5), "lon_range": (139.0, 142.0), "desc": "Tohoku South",      "order": 2},
    "F-KNT":   {"lat_range": (35.0, 37.0), "lon_range": (138.5, 141.5), "desc": "Kanto",             "order": 3},
    "F-IZU":   {"lat_range": (28.0, 35.0), "lon_range": (138.5, 142.5), "desc": "Izu/Bonin",         "order": 4},
    "F-CHB":   {"lat_range": (35.5, 37.5), "lon_range": (135.5, 138.5), "desc": "Chubu",             "order": 5},
    "F-KSI":   {"lat_range": (33.5, 35.5), "lon_range": (134.5, 137.0), "desc": "Kansai",            "order": 6},
    "F-CHG":   {"lat_range": (33.5, 36.0), "lon_range": (130.5, 134.5), "desc": "Chugoku",           "order": 7},
    "F-SKK":   {"lat_range": (32.5, 34.5), "lon_range": (132.5, 135.0), "desc": "Shikoku",           "order": 8},
    "F-KYU-N": {"lat_range": (32.0, 34.0), "lon_range": (129.5, 132.5), "desc": "Kyushu North",      "order": 9},
    "F-KYU-S": {"lat_range": (30.5, 32.5), "lon_range": (129.5, 132.0), "desc": "Kyushu South",      "order": 10},
    "F-OKN":   {"lat_range": (24.0, 28.0), "lon_range": (122.0, 131.0), "desc": "Okinawa/Ryukyu",    "order": 11},
}

# Spectral analysis bands (Hz) — identical to snet for feature parity.
BAND_VLF = (0.01, 0.1)
BAND_LF = (0.1, 1.0)
BAND_HF = (1.0, 10.0)
BAND_FULL = (0.1, 10.0)

# FFT window sizes — identical to snet velocity branch (F-net is broadband).
FFT_WINDOW_SEC = 10
VLF_FFT_WINDOW_SEC = 200

EXPECTED_FS = 100.0
MAX_RETRIES_BEFORE_SKIP = 3


class HinetQuotaError(Exception):
    """Raised when HinetPy reports daily quota exceeded.

    Carries partial_results so callers can persist any records that were
    successfully fetched in earlier segments before the quota hit.
    """

    def __init__(self, message: str, partial_results: list[dict] | None = None):
        """Capture the exception message and any records fetched before the cap."""
        super().__init__(message)
        self.partial_results = partial_results or []


class HinetAuthError(Exception):
    """Raised when HinetPy reports an authentication failure mid-run."""

    def __init__(self, message: str, partial_results: list[dict] | None = None):
        """Capture the exception message and any records fetched before auth failed."""
        super().__init__(message)
        self.partial_results = partial_results or []


# ---------------------------------------------------------------------------
# Discord notification helper
# ---------------------------------------------------------------------------

def send_discord(title: str, description: str, fields: list[dict] = None,
                 color: int = 3447003) -> None:
    """Send a Discord embed notification via curl. Non-blocking, fail-silent."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        return
    try:
        embed = {"title": title, "description": description, "color": color}
        if fields:
            embed["fields"] = fields
        embed["footer"] = {"text": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
        payload = json.dumps({"embeds": [embed]})
        result = subprocess.run(
            ["curl", "-sS", "-w", "\nHTTP %{http_code}\n",
             "-X", "POST", webhook_url,
             "-H", "Content-Type: application/json",
             "-d", payload],
            capture_output=True, text=True, timeout=15,
        )
        if "HTTP 2" not in result.stdout and "HTTP 2" not in result.stderr:
            logger.warning("Discord curl: %s %s", result.stdout.strip(), result.stderr.strip())
    except Exception as exc:
        logger.warning("Discord notification failed: %s", exc)


# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS fnet_waveform (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    date_str TEXT NOT NULL,
    segment_hour INTEGER NOT NULL,
    rms_z REAL,
    rms_h REAL,
    hv_ratio REAL,
    lf_power REAL,
    hf_power REAL,
    spectral_slope REAL,
    vlf_power REAL,
    vlf_hv_ratio REAL,
    n_samples INTEGER,
    latitude REAL,
    longitude REAL,
    region TEXT,
    received_at TEXT NOT NULL,
    UNIQUE(station_id, date_str, segment_hour)
);
"""

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_fnet_wf_date ON fnet_waveform(date_str);",
    "CREATE INDEX IF NOT EXISTS idx_fnet_wf_station ON fnet_waveform(station_id);",
    "CREATE INDEX IF NOT EXISTS idx_fnet_wf_region ON fnet_waveform(region);",
]

FAILED_DATES_DDL = (
    "CREATE TABLE IF NOT EXISTS fnet_failed_dates ("
    "  date_str TEXT NOT NULL PRIMARY KEY, "
    "  last_failed_at TEXT NOT NULL, "
    "  retry_count INTEGER NOT NULL DEFAULT 1, "
    "  reason TEXT"
    ")"
)


async def ensure_table(db: aiosqlite.Connection) -> None:
    """Create fnet_waveform + fnet_failed_dates tables and indexes."""
    await db.execute(TABLE_DDL)
    for idx in INDEX_DDL:
        await db.execute(idx)
    await db.execute(FAILED_DATES_DDL)
    await db.commit()


# ---------------------------------------------------------------------------
# Coverage analysis
# ---------------------------------------------------------------------------

async def get_existing_dates(db: aiosqlite.Connection) -> set[str]:
    """Return set of date_str values already in fnet_waveform."""
    rows = await db.execute_fetchall(
        "SELECT DISTINCT date_str FROM fnet_waveform"
    )
    return {r[0] for r in rows}


async def get_failed_dates(
    db: aiosqlite.Connection, threshold: int = MAX_RETRIES_BEFORE_SKIP
) -> set[str]:
    """Return date_str values that have hit the skip threshold.

    To re-investigate a date that was wrongly skipped (e.g. NIED maintenance
    coincided with consecutive runs), reset its retry_count:

        UPDATE fnet_failed_dates SET retry_count = 0 WHERE date_str = '2018-09-15';

    or wipe the marker entirely:

        DELETE FROM fnet_failed_dates WHERE date_str = '2018-09-15';
    """
    rows = await db.execute_fetchall(
        "SELECT date_str FROM fnet_failed_dates WHERE retry_count >= ?",
        (threshold,),
    )
    return {r[0] for r in rows}


def _mark_failed_sync(conn, date_str: str, reason: str = "no_records") -> None:
    """Increment retry_count for a date that returned zero records.

    Uses synchronous sqlite3 connection because callers run inside the executor
    thread used by HinetPy. INSERT OR IGNORE inserts a fresh row at retry_count=0;
    the UPDATE then bumps it to 1 (covers fresh-insert and pre-existing cases).
    """
    now_str = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO fnet_failed_dates "
        "(date_str, last_failed_at, retry_count, reason) "
        "VALUES (?, ?, 0, ?)",
        (date_str, now_str, reason),
    )
    conn.execute(
        "UPDATE fnet_failed_dates "
        "SET retry_count = retry_count + 1, last_failed_at = ?, reason = ? "
        "WHERE date_str = ?",
        (now_str, reason, date_str),
    )
    conn.commit()


async def get_coverage_report(db: aiosqlite.Connection) -> dict:
    """Generate detailed coverage report with per-region breakdown."""
    total_rows = (await db.execute_fetchall(
        "SELECT COUNT(*) FROM fnet_waveform"
    ))[0][0]

    dates = await db.execute_fetchall(
        "SELECT date_str, COUNT(DISTINCT station_id), COUNT(*) "
        "FROM fnet_waveform GROUP BY date_str ORDER BY date_str"
    )

    regions = await db.execute_fetchall(
        "SELECT region, COUNT(DISTINCT station_id), COUNT(DISTINCT date_str) "
        "FROM fnet_waveform WHERE region IS NOT NULL "
        "GROUP BY region ORDER BY region"
    )

    fnet_start_dt = datetime.strptime(FNET_START_STR, "%Y-%m-%d")
    if dates:
        all_dates = sorted([d[0] for d in dates])
        last_date = datetime.strptime(all_dates[-1], "%Y-%m-%d")
        expected_days = (last_date - fnet_start_dt).days + 1
        actual_days = len(all_dates)
        gap_days = expected_days - actual_days

        gaps = []
        for i in range(1, len(all_dates)):
            d1 = datetime.strptime(all_dates[i - 1], "%Y-%m-%d")
            d2 = datetime.strptime(all_dates[i], "%Y-%m-%d")
            gap = (d2 - d1).days - 1
            if gap > 0:
                gaps.append((all_dates[i - 1], all_dates[i], gap))
        gaps.sort(key=lambda x: -x[2])
    else:
        expected_days = actual_days = gap_days = 0
        gaps = []

    return {
        "total_rows": total_rows,
        "total_dates": actual_days,
        "expected_dates": expected_days,
        "gap_days": gap_days,
        "coverage_pct": round(actual_days / expected_days * 100, 1) if expected_days > 0 else 0,
        "first_date": all_dates[0] if dates else None,
        "last_date": all_dates[-1] if dates else None,
        "stations_per_date": {d[0]: d[1] for d in dates[-7:]} if dates else {},
        "regions": {r[0]: {"stations": r[1], "dates": r[2]} for r in regions},
        "top_gaps": gaps[:5],
    }


# ---------------------------------------------------------------------------
# SAC binary reader (identical algorithm to snet — copied for standalone build)
# ---------------------------------------------------------------------------

def read_sac_data(filepath: str) -> tuple:
    """Read SAC binary file, return (data_array, header_dict) or (None, None).

    SAC format: 632-byte header (70 floats + 40 ints + 24 strings) + float32 data.
    """
    try:
        import numpy as np
    except ImportError:
        return None, None

    try:
        with open(filepath, "rb") as f:
            header = f.read(632)
            if len(header) < 632:
                return None, None

            npts_le = struct.unpack_from("<i", header, 316)[0]
            npts_be = struct.unpack_from(">i", header, 316)[0]

            if 0 < npts_le <= 10_000_000:
                endian = "<"
                npts = npts_le
            elif 0 < npts_be <= 10_000_000:
                endian = ">"
                npts = npts_be
            else:
                return None, None

            delta = struct.unpack_from(f"{endian}f", header, 0)[0]
            fs = 1.0 / delta if delta > 0 else EXPECTED_FS

            stla = struct.unpack_from(f"{endian}f", header, 31 * 4)[0]
            stlo = struct.unpack_from(f"{endian}f", header, 32 * 4)[0]

            raw = f.read(npts * 4)
            data = np.frombuffer(raw, dtype=f"{endian}f4")
            if len(data) != npts:
                return None, None

            info = {"npts": npts, "fs": fs, "stla": stla, "stlo": stlo}
            return data, info

    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# Spectral analysis (identical to snet)
# ---------------------------------------------------------------------------

def _compute_psd(z, x, y, fs: float, window_sec: float):
    """Welch-style PSD for 3-component data using a Hann window.

    Returns (psd_z, psd_x, psd_y, freqs, n_windows) or None when the input
    is shorter than one full window.
    """
    import numpy as np

    n = len(z)
    win_samples = int(fs * window_sec)
    if n < win_samples:
        return None

    hop = win_samples // 2
    n_windows = max(1, (n - win_samples) // hop + 1)

    hann = np.hanning(win_samples)
    hann_norm = np.sum(hann ** 2)

    psd_z = np.zeros(win_samples // 2 + 1)
    psd_x = np.zeros_like(psd_z)
    psd_y = np.zeros_like(psd_z)

    for i in range(n_windows):
        start = i * hop
        end = start + win_samples
        if end > n:
            break
        seg_z = z[start:end] * hann
        seg_x = x[start:end] * hann
        seg_y = y[start:end] * hann
        psd_z += np.abs(np.fft.rfft(seg_z)) ** 2
        psd_x += np.abs(np.fft.rfft(seg_x)) ** 2
        psd_y += np.abs(np.fft.rfft(seg_y)) ** 2

    scale = 2.0 / (fs * hann_norm * n_windows) if n_windows > 0 else 1.0
    psd_z *= scale
    psd_x *= scale
    psd_y *= scale

    freqs = np.fft.rfftfreq(win_samples, d=1.0 / fs)
    return psd_z, psd_x, psd_y, freqs, n_windows


def _band_power(psd, freqs, f_low, f_high):
    """Return log10 of integrated PSD power in [f_low, f_high) Hz."""
    import numpy as np
    mask = (freqs >= f_low) & (freqs < f_high)
    if not np.any(mask):
        return 0.0
    _trapz = getattr(np, "trapezoid", None) or np.trapz
    power = float(_trapz(psd[mask], freqs[mask]))
    return math.log10(max(power, 1e-30))


def _hv_ratio(psd_x, psd_y, psd_z, freqs, f_low, f_high):
    """Mean horizontal-to-vertical spectral ratio in the [f_low, f_high] band."""
    import numpy as np
    psd_h = psd_x + psd_y
    safe_z = np.where(psd_z > 0, psd_z, 1e-30)
    hv_spectrum = np.sqrt(psd_h / safe_z)
    mask = (freqs >= f_low) & (freqs <= f_high)
    if np.any(mask):
        return float(np.mean(hv_spectrum[mask]))
    return 1.0


def _spectral_slope(psd_total, freqs, f_low, f_high):
    """Spectral slope (beta) by log-log linear fit over [f_low, f_high].

    Returns -2.0 (Brownian default) when there are too few valid bins to fit.
    """
    import numpy as np
    mask = (freqs >= f_low) & (freqs <= f_high) & (psd_total > 0)
    if np.sum(mask) > 5:
        log_f = np.log10(freqs[mask])
        log_p = np.log10(psd_total[mask])
        mean_f = np.mean(log_f)
        mean_p = np.mean(log_p)
        cov = np.sum((log_f - mean_f) * (log_p - mean_p))
        var_f = np.sum((log_f - mean_f) ** 2)
        return float(cov / var_f) if var_f > 0 else -2.0
    return -2.0


def compute_waveform_features(data_z, data_x, data_y, fs: float) -> dict | None:
    """Compute waveform features from 3-component data.

    F-net is always broadband, so VLF analysis is always performed.
    """
    import numpy as np

    min_samples = int(fs * FFT_WINDOW_SEC)
    if data_z is None or len(data_z) < min_samples:
        return None
    if data_x is None or len(data_x) < min_samples:
        return None
    if data_y is None or len(data_y) < min_samples:
        return None

    n = min(len(data_z), len(data_x), len(data_y))
    z = data_z[:n].astype(np.float64)
    x = data_x[:n].astype(np.float64)
    y = data_y[:n].astype(np.float64)

    z -= np.mean(z)
    x -= np.mean(x)
    y -= np.mean(y)

    if np.all(z == 0) or np.all(x == 0) or np.all(y == 0):
        return None

    rms_z = float(np.sqrt(np.mean(z ** 2)))
    rms_x = float(np.sqrt(np.mean(x ** 2)))
    rms_y = float(np.sqrt(np.mean(y ** 2)))
    rms_h = math.sqrt(rms_x ** 2 + rms_y ** 2)

    psd_result = _compute_psd(z, x, y, fs, FFT_WINDOW_SEC)
    if psd_result is None:
        return None
    psd_z, psd_x, psd_y, freqs, _ = psd_result

    hv_ratio = _hv_ratio(psd_x, psd_y, psd_z, freqs, 0.1, 10.0)
    psd_total = psd_z + psd_x + psd_y
    lf_power = _band_power(psd_total, freqs, *BAND_LF)
    hf_power = _band_power(psd_total, freqs, *BAND_HF)
    slope = _spectral_slope(psd_total, freqs, *BAND_FULL)

    result = {
        "rms_z": rms_z,
        "rms_h": rms_h,
        "hv_ratio": hv_ratio,
        "lf_power": lf_power,
        "hf_power": hf_power,
        "spectral_slope": slope,
        "vlf_power": None,
        "vlf_hv_ratio": None,
    }

    vlf_result = _compute_psd(z, x, y, fs, VLF_FFT_WINDOW_SEC)
    if vlf_result is not None:
        vpsd_z, vpsd_x, vpsd_y, vfreqs, _ = vlf_result
        vpsd_total = vpsd_z + vpsd_x + vpsd_y
        result["vlf_power"] = _band_power(vpsd_total, vfreqs, *BAND_VLF)
        result["vlf_hv_ratio"] = _hv_ratio(vpsd_x, vpsd_y, vpsd_z, vfreqs,
                                            BAND_VLF[0], BAND_VLF[1])

    return result


# ---------------------------------------------------------------------------
# Region classification
# ---------------------------------------------------------------------------

def classify_region(lat: float, lon: float) -> str | None:
    """Classify station into geographic region by coordinates.

    Boundaries are half-open [lo, hi) so adjacent regions don't double-claim
    a point on a shared edge. Iteration follows dict insertion order which
    matches FNET_REGIONS' "order" field, giving deterministic tiebreaks
    for points outside any half-open window (the upper-most edge of the
    last region uses an inclusive check below).
    """
    if lat is None or lon is None:
        return None
    for region, info in FNET_REGIONS.items():
        lat_lo, lat_hi = info["lat_range"]
        lon_lo, lon_hi = info["lon_range"]
        if lat_lo <= lat < lat_hi and lon_lo <= lon < lon_hi:
            return region
    # Fallback for points exactly on the outermost upper edges
    for region, info in FNET_REGIONS.items():
        lat_lo, lat_hi = info["lat_range"]
        lon_lo, lon_hi = info["lon_range"]
        if lat_lo <= lat <= lat_hi and lon_lo <= lon <= lon_hi:
            return region
    return None


# ---------------------------------------------------------------------------
# Station selection (geographic stratified sample)
# ---------------------------------------------------------------------------

def select_active_stations(client, max_stations: int) -> tuple[list[str], dict]:
    """Select F-net stations for fetching.

    Returns (station_codes, station_coords_map). When FNET_STATIONS env is
    set explicitly, that list is used. When set to "ALL", no filter applied.
    Otherwise, max_stations are picked via stratified latitude sample (N→S)
    so geographic spread is preserved as the active set grows.
    """
    try:
        all_stations = client.get_station_list(FNET_NETWORK_CODE)
    except Exception as exc:
        logger.warning("F-net station list fetch failed: %s", exc)
        return [], {}

    if not all_stations:
        return [], {}

    coords_map: dict = {}
    for st in all_stations:
        sid = getattr(st, "name", None) or getattr(st, "code", None)
        lat = getattr(st, "latitude", None)
        lon = getattr(st, "longitude", None)
        if sid and lat is not None and lon is not None:
            coords_map[str(sid)] = (float(lat), float(lon))

    logger.info("F-net network reports %d stations total", len(coords_map))

    if FNET_STATIONS_ENV.upper() == "ALL":
        logger.info("FNET_STATIONS=ALL → no station filter")
        return list(coords_map.keys()), coords_map

    if FNET_STATIONS_ENV:
        explicit = [s.strip() for s in FNET_STATIONS_ENV.split(",") if s.strip()]
        logger.info("FNET_STATIONS env override: %d explicit stations", len(explicit))
        return explicit, coords_map

    if len(coords_map) <= max_stations:
        return list(coords_map.keys()), coords_map

    sorted_st = sorted(coords_map.items(), key=lambda kv: -kv[1][0])  # north → south
    step = len(sorted_st) / max_stations
    selected = [sorted_st[int(i * step)][0] for i in range(max_stations)]
    logger.info(
        "Stratified-sample %d/%d stations (lat-spaced N→S)",
        len(selected), len(coords_map),
    )
    return selected, coords_map


# ---------------------------------------------------------------------------
# Credentials check
# ---------------------------------------------------------------------------

def _check_credentials() -> tuple[str, str] | None:
    """Return (user, password) from env or None when either is unset."""
    user = os.environ.get("HINET_USER", "").strip()
    password = os.environ.get("HINET_PASS", "").strip()
    if not user or not password:
        return None
    return user, password


# ---------------------------------------------------------------------------
# Core fetch logic (synchronous — runs in thread executor)
# ---------------------------------------------------------------------------

def _fetch_day(
    client, station_coords: dict, target_date: datetime, n_segments: int,
    deadline: float | None = None,
) -> list[dict]:
    """Fetch and process one day's F-net waveform data.

    If ``deadline`` (a ``time.monotonic()`` value) is provided, the segment
    loop exits early once it is reached, returning whatever results have been
    accumulated so far so the caller can persist them before SIGTERM.
    """
    import shutil
    results = []
    date_str = target_date.strftime("%Y-%m-%d")
    segment_hours = [h for h in range(0, 24, max(1, 24 // n_segments))][:n_segments]
    requested_segments = len(segment_hours)
    fetched_segments = 0

    for hour in segment_hours:
        if deadline is not None and time.monotonic() >= deadline:
            logger.warning(
                "  Step budget exhausted mid-day at %s %02d:00 — fetched %d/%d "
                "segments, %d records pending caller persist.",
                date_str, hour, fetched_segments, requested_segments, len(results),
            )
            break
        fetched_segments += 1
        start = target_date.replace(hour=hour, minute=0, second=0, microsecond=0)
        work_dir = tempfile.mkdtemp(prefix=f"fnet_{date_str}_{hour:02d}_")

        try:
            logger.info("Requesting %s %02d:00 (%d min)",
                        date_str, hour, REQUEST_DURATION_MIN)

            data = client.get_continuous_waveform(
                FNET_NETWORK_CODE, start, REQUEST_DURATION_MIN, outdir=work_dir,
            )
            if data is None or not isinstance(data, tuple) or len(data) != 2:
                logger.warning("No data for %s %02d:00", date_str, hour)
                continue

            win32_file, ch_table = data
            if win32_file is None:
                continue

            from HinetPy import win32 as hinetwin32
            sac_files = hinetwin32.extract_sac(win32_file, ch_table, outdir=work_dir)
            if not sac_files:
                sac_files = list(Path(work_dir).glob("*.SAC"))
            if not sac_files:
                logger.warning("No SAC files decoded for %s %02d:00", date_str, hour)
                continue

            sample_names = [Path(p).name for p in sac_files[:2]]
            logger.info("  extract_sac -> %d SAC files, sample: %s", len(sac_files), sample_names)

            station_files: dict = {}
            for sac_path in sac_files:
                basename = Path(sac_path).stem
                parts = basename.split(".")
                if len(parts) < 3:
                    continue
                station_id = parts[1]
                channel = parts[3] if len(parts) > 3 else parts[-1]

                # F-net SAC component is 2-char (verified via PR #104 debug log,
                # 2026-04-28): second char 'B' = broadband 100Hz (target), 'A' =
                # long-period 1Hz (reject to avoid fs mismatch in PSD path).
                # Sample filenames observed: N.ISIF.NB.SAC, N.ADMF.EB.SAC.
                ch_up = channel.upper() if channel else ""
                comp = {"UB": "Z", "NB": "X", "EB": "Y"}.get(ch_up)
                if comp is None:
                    continue
                station_files.setdefault(station_id, {})[comp] = str(sac_path)

            if station_files:
                first_st = next(iter(station_files))
                logger.info("  station_files: %d stations, first=%s comps=%s",
                            len(station_files), first_st, sorted(station_files[first_st].keys()))
            else:
                logger.info("  station_files: 0 stations (channel filter rejected all SACs)")

            for station_id, comps in station_files.items():
                if len(comps) < 3:
                    continue

                data_z, info_z = read_sac_data(comps.get("Z", ""))
                data_x, info_x = read_sac_data(comps.get("X", ""))
                data_y, info_y = read_sac_data(comps.get("Y", ""))

                if data_z is None or data_x is None or data_y is None:
                    continue

                # fs consistency check: HinetPy normalizes per (station, component) to
                # one SAC, but a mixed-rate triplet (broadband 100Hz vs long-period 1Hz)
                # would silently corrupt PSD — reject if any pair disagrees.
                fs_z = info_z.get("fs", EXPECTED_FS) if info_z else EXPECTED_FS
                fs_x = info_x.get("fs", EXPECTED_FS) if info_x else EXPECTED_FS
                fs_y = info_y.get("fs", EXPECTED_FS) if info_y else EXPECTED_FS
                if abs(fs_z - fs_x) > 0.01 or abs(fs_z - fs_y) > 0.01:
                    logger.warning(
                        "fs mismatch for %s (%s): z=%.2f x=%.2f y=%.2f — skipping",
                        station_id, date_str, fs_z, fs_x, fs_y,
                    )
                    continue
                fs = fs_z

                features = compute_waveform_features(data_z, data_x, data_y, fs)
                if features is None:
                    continue

                full_id = f"N.{station_id}" if not station_id.startswith("N.") else station_id
                lat, lon = station_coords.get(full_id, (None, None))
                if lat is None:
                    lat, lon = station_coords.get(station_id, (None, None))
                if lat is None and info_z:
                    stla = info_z.get("stla", -12345.0)
                    stlo = info_z.get("stlo", -12345.0)
                    if stla != -12345.0 and stlo != -12345.0:
                        lat, lon = stla, stlo

                region = classify_region(lat, lon) if lat is not None else None

                results.append({
                    "station_id": full_id,
                    "date_str": date_str,
                    "segment_hour": hour,
                    "rms_z": features["rms_z"],
                    "rms_h": features["rms_h"],
                    "hv_ratio": features["hv_ratio"],
                    "lf_power": features["lf_power"],
                    "hf_power": features["hf_power"],
                    "spectral_slope": features["spectral_slope"],
                    "vlf_power": features.get("vlf_power"),
                    "vlf_hv_ratio": features.get("vlf_hv_ratio"),
                    "n_samples": info_z.get("npts", 0),
                    "latitude": lat,
                    "longitude": lon,
                    "region": region,
                })

            n_seg = len([r for r in results
                         if r["date_str"] == date_str and r["segment_hour"] == hour])
            logger.info("  %s %02d:00 → %d stations processed", date_str, hour, n_seg)

        except Exception as exc:
            exc_str = str(exc).lower()
            if "quota" in exc_str or "limit" in exc_str:
                logger.error("Hi-net quota exceeded: %s", exc)
                raise HinetQuotaError(str(exc), partial_results=results) from exc
            elif "auth" in exc_str or "login" in exc_str or "401" in exc_str:
                logger.error("Hi-net authentication error: %s", exc)
                raise HinetAuthError(str(exc), partial_results=results) from exc
            else:
                logger.warning("Error fetching %s %02d:00: %s", date_str, hour, exc)
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

        # Skip cooldown if it would push us past the deadline (cooldown is
        # ~QUOTA_COOLDOWN_SEC; SIGTERM during sleep would lose accumulated state).
        if deadline is not None and time.monotonic() + QUOTA_COOLDOWN_SEC >= deadline:
            continue
        time.sleep(QUOTA_COOLDOWN_SEC)

    return results


def _safe_connect_sync(db_path: str = None):
    """Open sqlite3 connection with safety PRAGMAs (executor thread variant)."""
    import sqlite3
    path = db_path or os.environ.get("GEOHAZARD_DB_PATH", "/app/data/geohazard.db")
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _ensure_table_sync(conn) -> None:
    """Synchronous variant of ensure_table for use inside the executor thread."""
    conn.execute(TABLE_DDL)
    for idx in INDEX_DDL:
        conn.execute(idx)
    conn.execute(FAILED_DATES_DDL)
    conn.commit()


def _save_records_sync(conn, records: list[dict], now_str: str) -> tuple[int, int]:
    """Insert records into fnet_waveform with INSERT OR IGNORE.

    Returns (inserted, skipped) where inserted counts only rows that actually
    landed (cursor.rowcount == 1) and skipped covers both UNIQUE-conflict
    duplicates and rows that raised during execute.
    """
    inserted = skipped = 0
    for rec in records:
        try:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO fnet_waveform
                   (station_id, date_str, segment_hour,
                    rms_z, rms_h, hv_ratio, lf_power, hf_power,
                    spectral_slope, vlf_power, vlf_hv_ratio,
                    n_samples, latitude, longitude,
                    region, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rec["station_id"], rec["date_str"], rec["segment_hour"],
                    rec["rms_z"], rec["rms_h"], rec["hv_ratio"],
                    rec["lf_power"], rec["hf_power"], rec["spectral_slope"],
                    rec.get("vlf_power"), rec.get("vlf_hv_ratio"),
                    rec["n_samples"], rec["latitude"], rec["longitude"],
                    rec["region"], now_str,
                ),
            )
            # INSERT OR IGNORE returns rowcount=0 when the row was a duplicate
            # of the UNIQUE(station_id, date_str, segment_hour) key, so the
            # original counter would over-report inserts on resume runs.
            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
        except Exception as exc:
            logger.warning("Insert failed for %s/%s: %s",
                           rec["station_id"], rec["date_str"], exc)
            skipped += 1
    conn.commit()
    return inserted, skipped


def _fetch_and_save(
    user: str, password: str,
    dates_to_fetch: list[tuple[datetime, int]],
) -> tuple[int, int]:
    """Fetch waveform data and save each item to DB immediately."""
    try:
        from HinetPy import Client
    except ImportError:
        logger.error("HinetPy not installed")
        return 0, 0

    try:
        client = Client(user, password)
        logger.info("Authenticated to NIED Hi-net")
    except Exception as exc:
        logger.error("Authentication failed: %s", exc)
        return 0, 0

    selected, station_coords = select_active_stations(client, MAX_ACTIVE_STATIONS)
    logger.info("Active F-net stations: %d", len(selected))

    if selected and FNET_STATIONS_ENV.upper() != "ALL":
        # HinetPy station code naming for select_stations is not 100% documented:
        # try the codes as returned by get_station_list first, and on failure
        # retry with the "N." prefix stripped (Opus review C2).
        try:
            client.select_stations(FNET_NETWORK_CODE, selected)
            logger.info("client.select_stations: %d stations registered (as-is)", len(selected))
        except Exception as exc:
            logger.warning("select_stations as-is failed (%s); retrying without N. prefix", exc)
            try:
                stripped = [s[2:] if s.startswith("N.") else s for s in selected]
                client.select_stations(FNET_NETWORK_CODE, stripped)
                logger.info("client.select_stations: %d stations registered (stripped)", len(stripped))
            except Exception as exc2:
                logger.warning(
                    "client.select_stations failed both ways (continuing unfiltered, "
                    "expect 73-station fetches): %s", exc2,
                )

    conn = _safe_connect_sync()
    _ensure_table_sync(conn)
    now_str = datetime.now(timezone.utc).isoformat()

    total_inserted = 0
    total_skipped = 0
    total_items = len(dates_to_fetch)
    request_count = 0

    # Phase D3: graceful exit before SIGTERM. Per-item saves below mean any
    # data already persisted survives; this loop just stops queueing new items
    # and the segment loop also bails out via the deadline argument.
    start_time = time.monotonic()
    deadline = start_time + STEP_BUDGET_SEC - DEADLINE_MARGIN_SEC

    try:
        for i, (target_date, n_segments) in enumerate(dates_to_fetch):
            now = time.monotonic()
            if now >= deadline:
                logger.warning(
                    "Step budget exhausted at item %d/%d (elapsed %.0fs / "
                    "budget %ds, margin %ds). Breaking to allow graceful "
                    "artifact upload.",
                    i, total_items, now - start_time,
                    STEP_BUDGET_SEC, DEADLINE_MARGIN_SEC,
                )
                break

            date_str = target_date.strftime("%Y-%m-%d")

            if request_count + n_segments > MAX_REQUESTS_PER_RUN:
                logger.warning(
                    "Quota limit approaching (%d/%d). Stopping after %d items.",
                    request_count, MAX_REQUESTS_PER_RUN, i,
                )
                break

            logger.info(
                "=== Item %d/%d: %s (%d segments) ===",
                i + 1, total_items, date_str, n_segments,
            )

            try:
                day_records = _fetch_day(
                    client, station_coords, target_date, n_segments,
                    deadline=deadline,
                )
            except HinetQuotaError as exc:
                request_count += n_segments
                if exc.partial_results:
                    ins, skip = _save_records_sync(conn, exc.partial_results, now_str)
                    total_inserted += ins
                    total_skipped += skip
                    logger.info("  Saved %d partial records before quota hit", ins)
                logger.error("Quota hit mid-fetch, stopping run: %s", exc)
                break
            except HinetAuthError as exc:
                request_count += n_segments
                if exc.partial_results:
                    ins, skip = _save_records_sync(conn, exc.partial_results, now_str)
                    total_inserted += ins
                    total_skipped += skip
                logger.error("Auth failure mid-fetch, stopping run: %s", exc)
                break

            request_count += n_segments

            if day_records:
                ins, skip = _save_records_sync(conn, day_records, now_str)
                total_inserted += ins
                total_skipped += skip
                logger.info("  Saved %d records for %s", ins, date_str)
            else:
                _mark_failed_sync(conn, date_str, reason="no_records")
                logger.info("  Marked %s as failed (no records returned)", date_str)

            # Skip cooldown if it would push us past the deadline.
            if time.monotonic() + QUOTA_COOLDOWN_SEC >= deadline:
                continue
            time.sleep(QUOTA_COOLDOWN_SEC)
    finally:
        conn.close()

    return total_inserted, total_skipped


# ---------------------------------------------------------------------------
# Main async entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Fetch F-net waveform features with incremental backfill."""
    credentials = _check_credentials()
    if credentials is None:
        logger.warning(
            "HINET_USER/HINET_PASS not set. F-net waveform fetch requires "
            "NIED Hi-net registration. Exiting gracefully."
        )
        return

    user, password = credentials
    logger.info("Starting F-net broadband waveform extraction (Phase 1 Step 5c)")
    logger.info("Network code: %s, max active stations: %d, max requests: %d",
                FNET_NETWORK_CODE, MAX_ACTIVE_STATIONS, MAX_REQUESTS_PER_RUN)

    await init_db()

    async with safe_connect() as db:
        await ensure_table(db)
        existing = await get_existing_dates(db)
        failed = await get_failed_dates(db)

    skip_dates = existing | failed

    logger.info(
        "Existing coverage: %d dates, failed-out: %d dates, skip total: %d",
        len(existing), len(failed), len(skip_dates),
    )

    now_utc = datetime.now(timezone.utc)
    fnet_start = datetime.strptime(FNET_START_STR, "%Y-%m-%d")

    recent_dates = []
    for days_ago in range(1, RECENT_DAYS + 1):
        target = (now_utc - timedelta(days=days_ago)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        recent_dates.append(target)

    backfill_dates = []
    current = (now_utc - timedelta(days=RECENT_DAYS + 1)).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
    )
    while current >= fnet_start and len(backfill_dates) < MAX_BACKFILL_DAYS_PER_RUN:
        backfill_dates.append(current)
        current -= timedelta(days=1)

    dates_to_fetch = []
    for target in recent_dates:
        date_str = target.strftime("%Y-%m-%d")
        if date_str not in skip_dates:
            dates_to_fetch.append((target, SEGMENTS_RECENT))

    for target in backfill_dates:
        date_str = target.strftime("%Y-%m-%d")
        if date_str not in skip_dates:
            dates_to_fetch.append((target, SEGMENTS_BACKFILL))

    if not dates_to_fetch:
        logger.info("Window scan found nothing. Checking full history for gaps...")
        async with safe_connect() as db:
            report = await get_coverage_report(db)
        _log_coverage_report(report)
        pct = report['coverage_pct']

        if pct >= 100.0:
            logger.info("Coverage is 100%%. Backfill complete.")
            return

        cutoff = (now_utc - timedelta(days=RECENT_DAYS + 1)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        scan_date = fnet_start
        while scan_date <= cutoff:
            date_str = scan_date.strftime("%Y-%m-%d")
            if date_str not in skip_dates:
                dates_to_fetch.append((scan_date, SEGMENTS_BACKFILL))
            scan_date += timedelta(days=1)

        if not dates_to_fetch:
            logger.info("No historical gaps found outside recent window.")
            send_discord(
                "⚠️ F-net Waveform — Stalled",
                f"No gaps found in full history scan. Coverage: {pct}%\n"
                f"{report['total_dates']} dates, {report['total_rows']:,} rows",
                color=15105570,
            )
            return

        logger.info(
            "Full scan found %d missing dates. Fetching oldest-first (budget: %d requests).",
            len(dates_to_fetch), MAX_REQUESTS_PER_RUN,
        )

    n_recent = len([d for d in dates_to_fetch if d[1] == SEGMENTS_RECENT])
    n_backfill = len([d for d in dates_to_fetch if d[1] == SEGMENTS_BACKFILL])
    est_requests = sum(d[1] for d in dates_to_fetch)

    logger.info(
        "Fetch schedule: %d recent + %d backfill = %d items (~%d requests, budget %d)",
        n_recent, n_backfill, len(dates_to_fetch), est_requests, MAX_REQUESTS_PER_RUN,
    )

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None, _fetch_and_save, user, password, dates_to_fetch,
    )
    inserted, skipped = result

    if inserted == 0:
        logger.warning("No waveform records retrieved")
        send_discord(
            "⚠️ F-net Waveform — No Data",
            "Fetch completed but no records were retrieved. "
            "Check Hi-net credentials and quota.",
            color=15158332,
        )
        return

    logger.info("Inserted %d records (%d skipped/duplicate)", inserted, skipped)

    async with safe_connect() as db:
        report = await get_coverage_report(db)

    _log_coverage_report(report)

    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    coverage_path = results_dir / "fnet_coverage.json"
    coverage_path.write_text(json.dumps(report, indent=2, default=str))
    logger.info("Coverage report saved to %s", coverage_path)

    logger.info(
        "Fetch complete: inserted %d records (coverage %.1f%%)",
        inserted, report["coverage_pct"],
    )


def _log_coverage_report(report: dict) -> None:
    """Print the coverage report dict (from get_coverage_report) to logger.info."""
    logger.info("=== F-net Waveform Coverage ===")
    logger.info("  Date range: %s → %s", report.get("first_date"), report.get("last_date"))
    logger.info("  Coverage: %s%% (%d/%d days)",
                report["coverage_pct"], report["total_dates"], report["expected_dates"])
    logger.info("  Gap days: %d", report["gap_days"])
    logger.info("  Total rows: %d", report["total_rows"])
    if report["top_gaps"]:
        logger.info("  Top gaps:")
        for g in report["top_gaps"][:5]:
            logger.info("    %s → %s (%d days)", g[0], g[1], g[2])
    if report["regions"]:
        logger.info("  Per region:")
        for region, info in sorted(report["regions"].items()):
            logger.info("    %s: %d stations, %d dates", region, info["stations"], info["dates"])
    if report.get("stations_per_date"):
        logger.info("  Recent stations/date:")
        for d, n in sorted(report["stations_per_date"].items()):
            logger.info("    %s: %d stations", d, n)


if __name__ == "__main__":
    asyncio.run(main())
