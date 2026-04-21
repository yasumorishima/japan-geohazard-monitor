"""Fetch S-net waveform features from NIED Hi-net — Phase 19 (multi-sensor).

S-net (Seafloor Observation Network for Earthquakes and Tsunamis along the
Japan Trench) has 150 ocean bottom stations with multiple sensor types:
    - 0120  (VX/VY/VZ):    Broadband velocity — tremor/SSE detection (0.01–0.1 Hz)
    - 0120A (A1X/A1Y/A1Z): Acceleration — strong motion, microseismicity
    - 0120C (A2HX/A2HY/A2HZ): High-gain acceleration — micro-amplitude signals

Features extracted per station per day per sensor:
    - RMS (vertical + horizontal)
    - H/V spectral ratio (site response indicator)
    - Band-specific power: LF (0.1–1 Hz), HF (1–10 Hz)
    - Spectral slope (noise source characterization)
    - [Velocity only] VLF power (0.01–0.1 Hz), VLF H/V ratio

Aggregated ML features (15 total):
    Acceleration (7):
        snet_rms/hv_ratio/lf_power/hf_power/spectral_slope _anomaly
        snet_spatial_gradient, snet_segment_max_anomaly
    Velocity (5):
        snet_vlf_power/vlf_hv_ratio/velocity_rms _anomaly
        snet_vlf_spatial_gradient, snet_spectral_slope_velocity
    Cross-sensor (2):
        snet_vlf_hf_ratio, snet_accel_velocity_coherence
    High-gain (1):
        snet_highgain_snr_anomaly

Backfill strategy:
    - Checks existing coverage per sensor_type in DB
    - Recent 7 days (4 segments/day) + backfill gaps (1 segment/day)
    - Priority: velocity (0120) > acceleration (0120A) > high-gain (0120C)
    - HinetPy session limits: ~200 req/day → budget ~160 for 3 codes
    - Reports per-sensor coverage via Discord

References:
    Aoi et al. (2020) Earth Planets Space 72:126 (S-net overview)
    Nakamura & Katao (2005) H/V ratio for site characterization
    Obara (2002) Nonvolcanic deep tremor detection via spectral analysis
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

# S-net multi-sensor configuration (Phase 19)
# Priority determines fetch order within each date (lower = first)
SNET_SENSORS = {
    "0120":  {"sensor_type": "velocity",  "priority": 1, "vlf_analysis": True},
    "0120A": {"sensor_type": "accel",     "priority": 2, "vlf_analysis": False},
    "0120C": {"sensor_type": "accel_hg",  "priority": 3, "vlf_analysis": False},
}
# 0120B (low-gain accel) skipped — redundant with 0120A, same physical quantity

# HinetPy constraints (budget: ~200 req/day across 3 sensor codes)
# Override MAX_REQUESTS_PER_RUN via env var for smoke tests (e.g. SNET_MAX_REQUESTS=2)
REQUEST_DURATION_MIN = 5       # Max 5 min per request
SEGMENTS_RECENT = 4            # 4 segments for recent days (every 6h)
SEGMENTS_BACKFILL = 1          # 1 segment for backfill days (daily average)
RECENT_DAYS = 7                # Last 7 days get high-resolution sampling
MAX_BACKFILL_DAYS_PER_RUN = 5  # 5 days × 3 codes = 15 backfill requests (reduced to fit 6h job limit)
QUOTA_COOLDOWN_SEC = 2         # Pause between requests to respect quota
MAX_REQUESTS_PER_RUN = int(os.environ.get("SNET_MAX_REQUESTS", "120"))

# S-net cable segments for spatial analysis
SNET_CABLE_SEGMENTS = {
    "S1": {"lat_range": (39.5, 42.0), "lon_range": (142.5, 145.5), "desc": "Off Tokachi",    "order": 0},
    "S2": {"lat_range": (38.0, 40.0), "lon_range": (142.0, 144.5), "desc": "Off Sanriku",    "order": 1},
    "S3": {"lat_range": (36.5, 38.5), "lon_range": (141.5, 144.0), "desc": "Off Miyagi",     "order": 2},
    "S4": {"lat_range": (35.0, 37.0), "lon_range": (140.5, 143.0), "desc": "Off Fukushima",  "order": 3},
    "S5": {"lat_range": (34.0, 36.0), "lon_range": (140.0, 142.5), "desc": "Off Boso",       "order": 4},
    "S6": {"lat_range": (33.0, 35.0), "lon_range": (139.0, 141.5), "desc": "Off Tokai",      "order": 5},
}

# Spectral analysis bands (Hz)
BAND_VLF = (0.01, 0.1)  # Very low frequency — SSE/tremor (velocity only)
BAND_LF = (0.1, 1.0)    # Low frequency — slow-slip, tremor
BAND_HF = (1.0, 10.0)   # High frequency — local seismicity, microseisms
BAND_FULL = (0.1, 10.0)  # Full band for spectral slope estimation

# FFT window sizes
FFT_WINDOW_SEC = 10      # Standard window for acceleration (0.1 Hz resolution)
VLF_FFT_WINDOW_SEC = 200 # Long window for velocity VLF (0.005 Hz resolution)

# Sampling rate from test: 100 Hz
EXPECTED_FS = 100.0


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
        import subprocess
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
CREATE TABLE IF NOT EXISTS snet_waveform (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    date_str TEXT NOT NULL,
    segment_hour INTEGER NOT NULL,
    sensor_type TEXT NOT NULL DEFAULT '0120A',
    -- Per-station waveform features
    rms_z REAL,              -- Vertical RMS (counts)
    rms_h REAL,              -- Horizontal RMS = sqrt(rms_x² + rms_y²)
    hv_ratio REAL,           -- H/V spectral ratio (Nakano method)
    lf_power REAL,           -- Log10 power in 0.1–1 Hz band
    hf_power REAL,           -- Log10 power in 1–10 Hz band
    spectral_slope REAL,     -- β in log-log PSD (0.1–10 Hz)
    vlf_power REAL,          -- Log10 power in 0.01–0.1 Hz band (velocity only)
    vlf_hv_ratio REAL,       -- H/V ratio in VLF band (velocity only)
    n_samples INTEGER,       -- Number of valid samples
    latitude REAL,
    longitude REAL,
    cable_segment TEXT,      -- S1..S6
    received_at TEXT NOT NULL,
    UNIQUE(station_id, date_str, segment_hour, sensor_type)
);
"""

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_snet_wf_date ON snet_waveform(date_str);",
    "CREATE INDEX IF NOT EXISTS idx_snet_wf_station ON snet_waveform(station_id);",
    "CREATE INDEX IF NOT EXISTS idx_snet_wf_segment ON snet_waveform(cable_segment);",
    "CREATE INDEX IF NOT EXISTS idx_snet_wf_sensor ON snet_waveform(sensor_type);",
]


async def ensure_table(db: aiosqlite.Connection) -> None:
    """Create snet_waveform table and indexes, with migration for Phase 18 → 19."""
    await db.execute(TABLE_DDL)
    for idx in INDEX_DDL:
        await db.execute(idx)

    # Migration: add columns if upgrading from Phase 18 schema
    columns = {row[1] for row in await db.execute_fetchall(
        "PRAGMA table_info(snet_waveform)"
    )}

    migrated = False
    for col, ddl in [
        ("sensor_type", "ALTER TABLE snet_waveform ADD COLUMN sensor_type TEXT NOT NULL DEFAULT '0120A'"),
        ("vlf_power", "ALTER TABLE snet_waveform ADD COLUMN vlf_power REAL"),
        ("vlf_hv_ratio", "ALTER TABLE snet_waveform ADD COLUMN vlf_hv_ratio REAL"),
    ]:
        if col not in columns:
            await db.execute(ddl)
            migrated = True
            logger.info("Migration: added column %s to snet_waveform", col)

    if migrated:
        # Recreate unique index to include sensor_type
        await db.execute("DROP INDEX IF EXISTS sqlite_autoindex_snet_waveform_1")
        # SQLite cannot ALTER UNIQUE constraint, but new rows will use the new DDL's constraint.
        # For existing data, create a unique index explicitly.
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_snet_wf_unique "
            "ON snet_waveform(station_id, date_str, segment_hour, sensor_type)"
        )
        logger.info("Migration: unique index updated to include sensor_type")

    await db.commit()


# ---------------------------------------------------------------------------
# Coverage analysis
# ---------------------------------------------------------------------------

async def get_existing_dates(db: aiosqlite.Connection) -> set[tuple[str, str]]:
    """Return set of (date_str, sensor_type) tuples already in snet_waveform."""
    rows = await db.execute_fetchall(
        "SELECT DISTINCT date_str, sensor_type FROM snet_waveform"
    )
    return {(r[0], r[1]) for r in rows}


async def get_coverage_report(db: aiosqlite.Connection) -> dict:
    """Generate detailed coverage report with per-sensor breakdown."""
    total_rows = (await db.execute_fetchall(
        "SELECT COUNT(*) FROM snet_waveform"
    ))[0][0]

    # Per-sensor type coverage
    sensor_coverage = {}
    sensor_rows = await db.execute_fetchall(
        "SELECT sensor_type, COUNT(*), COUNT(DISTINCT date_str) "
        "FROM snet_waveform GROUP BY sensor_type"
    )
    for sr in sensor_rows:
        sensor_coverage[sr[0]] = {"rows": sr[1], "dates": sr[2]}

    dates = await db.execute_fetchall(
        "SELECT date_str, COUNT(DISTINCT station_id), COUNT(*) "
        "FROM snet_waveform GROUP BY date_str ORDER BY date_str"
    )

    segments = await db.execute_fetchall(
        "SELECT cable_segment, COUNT(DISTINCT station_id), COUNT(DISTINCT date_str) "
        "FROM snet_waveform WHERE cable_segment IS NOT NULL "
        "GROUP BY cable_segment ORDER BY cable_segment"
    )

    # Gap analysis
    if dates:
        all_dates = sorted([d[0] for d in dates])
        first_date = datetime.strptime(all_dates[0], "%Y-%m-%d")
        last_date = datetime.strptime(all_dates[-1], "%Y-%m-%d")
        expected_days = (last_date - first_date).days + 1
        actual_days = len(all_dates)
        gap_days = expected_days - actual_days

        # Find largest gaps
        gaps = []
        for i in range(1, len(all_dates)):
            d1 = datetime.strptime(all_dates[i - 1], "%Y-%m-%d")
            d2 = datetime.strptime(all_dates[i], "%Y-%m-%d")
            gap = (d2 - d1).days - 1
            if gap > 0:
                gaps.append((all_dates[i - 1], all_dates[i], gap))
        gaps.sort(key=lambda x: -x[2])
    else:
        first_date = last_date = None
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
        "stations_per_date": {d[0]: d[1] for d in dates[-7:]} if dates else {},  # last 7 days
        "segments": {s[0]: {"stations": s[1], "dates": s[2]} for s in segments},
        "sensor_coverage": sensor_coverage,
        "top_gaps": gaps[:5],
    }


# ---------------------------------------------------------------------------
# SAC binary reader
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

            # Determine endianness from NPTS (int header index 9, offset 316)
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

            # Read DELTA (sample interval) at float offset 0
            delta = struct.unpack_from(f"{endian}f", header, 0)[0]
            fs = 1.0 / delta if delta > 0 else EXPECTED_FS

            # Read station latitude (float offset 31) and longitude (float offset 32)
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
# Spectral analysis (pure NumPy — no scipy dependency)
# ---------------------------------------------------------------------------

def _compute_psd(z, x, y, fs: float, window_sec: float):
    """Compute Welch-like PSD for 3-component data.

    Args:
        z, x, y: Detrended, zero-mean component arrays (float64)
        fs: Sampling frequency (Hz)
        window_sec: FFT window length in seconds

    Returns:
        (psd_z, psd_x, psd_y, freqs, n_windows) or None if insufficient data.
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
    """Compute log10 integrated power in a frequency band."""
    import numpy as np
    mask = (freqs >= f_low) & (freqs < f_high)
    if not np.any(mask):
        return 0.0
    _trapz = getattr(np, "trapezoid", None) or np.trapz
    power = float(_trapz(psd[mask], freqs[mask]))
    return math.log10(max(power, 1e-30))


def _hv_ratio(psd_x, psd_y, psd_z, freqs, f_low, f_high):
    """Compute average H/V spectral ratio in a frequency band."""
    import numpy as np
    psd_h = psd_x + psd_y
    safe_z = np.where(psd_z > 0, psd_z, 1e-30)
    hv_spectrum = np.sqrt(psd_h / safe_z)
    mask = (freqs >= f_low) & (freqs <= f_high)
    if np.any(mask):
        return float(np.mean(hv_spectrum[mask]))
    return 1.0


def _spectral_slope(psd_total, freqs, f_low, f_high):
    """Compute spectral slope (beta) via log-log linear fit."""
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
    return -2.0  # Default: Brownian noise


def compute_waveform_features(
    data_z, data_x, data_y, fs: float, vlf_analysis: bool = False
) -> dict | None:
    """Compute waveform features from 3-component data.

    Args:
        data_z: Vertical component (numpy array)
        data_x: X (North-South) component
        data_y: Y (East-West) component
        fs: Sampling frequency (Hz)
        vlf_analysis: If True, compute VLF features using 200s FFT windows
                      (for velocity/broadband sensors)

    Returns:
        Dict with features, or None if data is insufficient.
        Standard: rms_z, rms_h, hv_ratio, lf_power, hf_power, spectral_slope
        VLF (when vlf_analysis=True): vlf_power, vlf_hv_ratio
    """
    import numpy as np

    min_samples = int(fs * FFT_WINDOW_SEC)  # At least one standard window
    if data_z is None or len(data_z) < min_samples:
        return None
    if data_x is None or len(data_x) < min_samples:
        return None
    if data_y is None or len(data_y) < min_samples:
        return None

    # Use common length
    n = min(len(data_z), len(data_x), len(data_y))
    z = data_z[:n].astype(np.float64)
    x = data_x[:n].astype(np.float64)
    y = data_y[:n].astype(np.float64)

    # Remove mean (detrend)
    z -= np.mean(z)
    x -= np.mean(x)
    y -= np.mean(y)

    # Skip if all zeros (dead channel)
    if np.all(z == 0) or np.all(x == 0) or np.all(y == 0):
        return None

    # --- RMS ---
    rms_z = float(np.sqrt(np.mean(z ** 2)))
    rms_x = float(np.sqrt(np.mean(x ** 2)))
    rms_y = float(np.sqrt(np.mean(y ** 2)))
    rms_h = math.sqrt(rms_x ** 2 + rms_y ** 2)

    # --- Standard PSD (10-second windows) ---
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

    # --- VLF analysis (velocity sensors: 200-second windows) ---
    if vlf_analysis:
        vlf_result = _compute_psd(z, x, y, fs, VLF_FFT_WINDOW_SEC)
        if vlf_result is not None:
            vpsd_z, vpsd_x, vpsd_y, vfreqs, _ = vlf_result
            vpsd_total = vpsd_z + vpsd_x + vpsd_y
            result["vlf_power"] = _band_power(vpsd_total, vfreqs, *BAND_VLF)
            result["vlf_hv_ratio"] = _hv_ratio(vpsd_x, vpsd_y, vpsd_z, vfreqs,
                                                BAND_VLF[0], BAND_VLF[1])
        else:
            logger.debug("VLF analysis: data too short for %ds window", VLF_FFT_WINDOW_SEC)

    return result


# ---------------------------------------------------------------------------
# Station → cable segment mapping
# ---------------------------------------------------------------------------

def classify_cable_segment(station_name: str) -> str | None:
    """Map station name (e.g. 'N.S1N01') to cable segment ('S1'..'S6')."""
    # Station names follow pattern N.SxNnn where x is segment number
    if not station_name:
        return None
    # Try to extract from station name
    for seg in SNET_CABLE_SEGMENTS:
        if seg in station_name:
            return seg
    return None


def classify_cable_segment_by_coords(lat: float, lon: float) -> str | None:
    """Classify station into cable segment by coordinates."""
    if lat is None or lon is None:
        return None
    for seg, info in SNET_CABLE_SEGMENTS.items():
        lat_lo, lat_hi = info["lat_range"]
        lon_lo, lon_hi = info["lon_range"]
        if lat_lo <= lat <= lat_hi and lon_lo <= lon <= lon_hi:
            return seg
    return None


# ---------------------------------------------------------------------------
# Credentials check
# ---------------------------------------------------------------------------

def _check_credentials() -> tuple[str, str] | None:
    """Return (user, password) or None."""
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
    network_code: str = "0120A", sensor_config: dict = None,
) -> list[dict]:
    """Fetch and process one day's S-net waveform data for a single sensor code.

    Returns list of per-station feature dicts for each segment.
    """
    import numpy as np

    if sensor_config is None:
        sensor_config = SNET_SENSORS.get(network_code, {"sensor_type": "accel", "vlf_analysis": False})
    sensor_type = sensor_config["sensor_type"]
    vlf_analysis = sensor_config.get("vlf_analysis", False)

    results = []
    date_str = target_date.strftime("%Y-%m-%d")
    segment_hours = [h for h in range(0, 24, max(1, 24 // n_segments))][:n_segments]

    for hour in segment_hours:
        start = target_date.replace(hour=hour, minute=0, second=0, microsecond=0)
        work_dir = tempfile.mkdtemp(prefix=f"snet_{network_code}_{date_str}_{hour:02d}_")

        try:
            logger.info("Requesting %s [%s] %02d:00 (%d min)",
                        date_str, network_code, hour, REQUEST_DURATION_MIN)

            data = client.get_continuous_waveform(
                network_code, start, REQUEST_DURATION_MIN, outdir=work_dir,
            )
            if data is None or not isinstance(data, tuple) or len(data) != 2:
                logger.warning("No data for %s [%s] %02d:00", date_str, network_code, hour)
                continue

            win32_file, ch_table = data
            if win32_file is None:
                continue

            # Decode WIN32 → SAC
            from HinetPy import win32 as hinetwin32
            sac_files = hinetwin32.extract_sac(win32_file, ch_table, outdir=work_dir)
            if not sac_files:
                sac_files = list(Path(work_dir).glob("*.SAC"))
            if not sac_files:
                logger.warning("No SAC files decoded for %s [%s] %02d:00",
                               date_str, network_code, hour)
                continue

            # Group SAC files by station
            station_files = {}  # station_id -> {component: sac_path}
            for sac_path in sac_files:
                basename = Path(sac_path).stem
                parts = basename.split(".")
                if len(parts) < 3:
                    continue
                station_id = parts[1]
                # Handle both 4-part (N.STA.LOC.CHA) and 3-part (N.STA.CHA) formats
                channel = parts[3] if len(parts) > 3 else parts[-1]

                suffix = channel[-1].upper() if channel else ""
                if suffix in ("Z", "X", "Y"):
                    station_files.setdefault(station_id, {})[suffix] = str(sac_path)

            # Process each station with all 3 components
            for station_id, comps in station_files.items():
                if len(comps) < 3:
                    continue

                data_z, info_z = read_sac_data(comps.get("Z", ""))
                data_x, info_x = read_sac_data(comps.get("X", ""))
                data_y, info_y = read_sac_data(comps.get("Y", ""))

                if data_z is None or data_x is None or data_y is None:
                    continue

                fs = info_z.get("fs", EXPECTED_FS) if info_z else EXPECTED_FS

                features = compute_waveform_features(
                    data_z, data_x, data_y, fs, vlf_analysis=vlf_analysis)
                if features is None:
                    continue

                # Get coordinates
                lat, lon = station_coords.get(station_id, (None, None))
                if lat is None and info_z:
                    stla = info_z.get("stla", -12345.0)
                    stlo = info_z.get("stlo", -12345.0)
                    if stla != -12345.0 and stlo != -12345.0:
                        lat, lon = stla, stlo

                cable_seg = classify_cable_segment(station_id)
                if cable_seg is None and lat is not None:
                    cable_seg = classify_cable_segment_by_coords(lat, lon)

                results.append({
                    "station_id": station_id,
                    "date_str": date_str,
                    "segment_hour": hour,
                    "sensor_type": sensor_type,
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
                    "cable_segment": cable_seg,
                })

            logger.info(
                "  %s %02d:00 → %d stations processed",
                date_str, hour, len([r for r in results if r["date_str"] == date_str and r["segment_hour"] == hour]),
            )

        except Exception as exc:
            exc_str = str(exc).lower()
            if "quota" in exc_str or "limit" in exc_str:
                logger.error("Hi-net quota exceeded: %s", exc)
                return results  # Return what we have
            elif "auth" in exc_str or "login" in exc_str or "401" in exc_str:
                logger.error("Hi-net authentication error: %s", exc)
                return results
            else:
                logger.warning("Error fetching %s %02d:00: %s", date_str, hour, exc)
        finally:
            # Clean up temp files
            try:
                import shutil
                shutil.rmtree(work_dir, ignore_errors=True)
            except Exception:
                pass

        # Rate limiting
        time.sleep(QUOTA_COOLDOWN_SEC)

    return results


def _safe_connect_sync(db_path: str = None):
    """Open a sqlite3 connection with the same safety PRAGMAs as db_connect.safe_connect."""
    import sqlite3
    path = db_path or os.environ.get("GEOHAZARD_DB_PATH", "/app/data/geohazard.db")
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _ensure_table_sync(conn) -> None:
    """Create snet_waveform table (sync version for use in executor thread)."""
    conn.execute(TABLE_DDL)
    for idx in INDEX_DDL:
        conn.execute(idx)
    # Migration: add columns if upgrading from Phase 18
    columns = {row[1] for row in conn.execute("PRAGMA table_info(snet_waveform)").fetchall()}
    for col, ddl in [
        ("sensor_type", "ALTER TABLE snet_waveform ADD COLUMN sensor_type TEXT NOT NULL DEFAULT '0120A'"),
        ("vlf_power", "ALTER TABLE snet_waveform ADD COLUMN vlf_power REAL"),
        ("vlf_hv_ratio", "ALTER TABLE snet_waveform ADD COLUMN vlf_hv_ratio REAL"),
    ]:
        if col not in columns:
            conn.execute(ddl)
            logger.info("Migration: added column %s", col)
    conn.commit()


def _save_records_sync(conn, records: list[dict], now_str: str) -> tuple[int, int]:
    """Insert records into snet_waveform. Returns (inserted, skipped)."""
    inserted = skipped = 0
    for rec in records:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO snet_waveform
                   (station_id, date_str, segment_hour, sensor_type,
                    rms_z, rms_h, hv_ratio, lf_power, hf_power,
                    spectral_slope, vlf_power, vlf_hv_ratio,
                    n_samples, latitude, longitude,
                    cable_segment, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rec["station_id"], rec["date_str"], rec["segment_hour"],
                    rec["sensor_type"],
                    rec["rms_z"], rec["rms_h"], rec["hv_ratio"],
                    rec["lf_power"], rec["hf_power"], rec["spectral_slope"],
                    rec.get("vlf_power"), rec.get("vlf_hv_ratio"),
                    rec["n_samples"], rec["latitude"], rec["longitude"],
                    rec["cable_segment"], now_str,
                ),
            )
            inserted += 1
        except Exception as exc:
            logger.warning("Insert failed for %s/%s/%s: %s",
                           rec["station_id"], rec["date_str"],
                           rec.get("sensor_type"), exc)
            skipped += 1
    conn.commit()
    return inserted, skipped


def _fetch_and_save(
    user: str, password: str,
    dates_to_fetch: list[tuple[datetime, int, str, dict]],
) -> tuple[int, int]:
    """Fetch waveform data and save each item to DB immediately.

    Runs entirely in one thread (HinetPy Client + sqlite3).
    Each item is committed right after fetch so data survives timeout kills.

    Returns (total_inserted, total_skipped).
    """
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

    # Fetch station metadata once
    station_coords = {}
    try:
        stations = client.get_station_list("0120A")
        if stations:
            for st in stations:
                sid = getattr(st, "name", None) or getattr(st, "code", None)
                lat = getattr(st, "latitude", None)
                lon = getattr(st, "longitude", None)
                if sid and lat is not None and lon is not None:
                    station_coords[str(sid)] = (float(lat), float(lon))
            logger.info("Station metadata: %d stations", len(station_coords))
    except Exception as exc:
        logger.warning("Station metadata fetch failed: %s", exc)

    conn = _safe_connect_sync()
    _ensure_table_sync(conn)
    now_str = datetime.now(timezone.utc).isoformat()

    total_inserted = 0
    total_skipped = 0
    total_items = len(dates_to_fetch)
    request_count = 0

    try:
        for i, (target_date, n_segments, network_code, sensor_config) in enumerate(dates_to_fetch):
            date_str = target_date.strftime("%Y-%m-%d")

            # Quota check
            if request_count + n_segments > MAX_REQUESTS_PER_RUN:
                logger.warning(
                    "Quota limit approaching (%d/%d). Stopping after %d items.",
                    request_count, MAX_REQUESTS_PER_RUN, i,
                )
                send_discord(
                    "⚠️ S-net Quota Limit Reached",
                    f"Stopped at {request_count} requests ({i}/{total_items} items)",
                    color=16776960,
                )
                break

            logger.info(
                "=== Item %d/%d: %s [%s] (%d segments) ===",
                i + 1, total_items, date_str, network_code, n_segments,
            )

            day_records = _fetch_day(
                client, station_coords, target_date, n_segments,
                network_code=network_code, sensor_config=sensor_config,
            )
            request_count += n_segments

            # Save immediately — survives timeout kills
            if day_records:
                ins, skip = _save_records_sync(conn, day_records, now_str)
                total_inserted += ins
                total_skipped += skip
                logger.info("  Saved %d records for %s [%s]", ins, date_str, network_code)

            # Progress notification every 20 items or at end
            if (i + 1) % 20 == 0 or i + 1 == total_items:
                send_discord(
                    "🌊 S-net Multi-Sensor Fetch Progress",
                    f"Item {i + 1}/{total_items}: {date_str} [{network_code}]",
                    fields=[
                        {"name": "Requests used", "value": f"{request_count}/{MAX_REQUESTS_PER_RUN}", "inline": True},
                        {"name": "Inserted so far", "value": f"{total_inserted:,}", "inline": True},
                    ],
                    color=3447003,
                )

            time.sleep(QUOTA_COOLDOWN_SEC)
    finally:
        conn.close()

    return total_inserted, total_skipped


# ---------------------------------------------------------------------------
# Main async entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Fetch S-net waveform features with incremental backfill (multi-sensor)."""
    credentials = _check_credentials()
    if credentials is None:
        logger.warning(
            "HINET_USER/HINET_PASS not set. S-net waveform fetch requires "
            "NIED Hi-net registration. Exiting gracefully."
        )
        return

    user, password = credentials
    logger.info("Starting S-net multi-sensor waveform extraction (Phase 19)")
    logger.info("Sensor codes: %s", ", ".join(SNET_SENSORS.keys()))

    await init_db()

    async with safe_connect() as db:
        await ensure_table(db)
        existing = await get_existing_dates(db)

    logger.info("Existing coverage: %d (date, sensor_type) pairs in DB", len(existing))

    # Build fetch schedule: for each sensor code, build recent + backfill list
    now_utc = datetime.now(timezone.utc)
    snet_start = datetime(2016, 8, 15)

    # Sort sensor configs by priority
    sorted_sensors = sorted(SNET_SENSORS.items(), key=lambda x: x[1]["priority"])

    # Interleave: for each date, fetch all needed codes before moving to next date
    # This ensures cross-sensor features have matching date coverage
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
    while current >= snet_start and len(backfill_dates) < MAX_BACKFILL_DAYS_PER_RUN * 3:
        backfill_dates.append(current)
        current -= timedelta(days=1)
    # Most recent gaps first
    backfill_dates = backfill_dates[:MAX_BACKFILL_DAYS_PER_RUN]

    # Build interleaved fetch list: (date, n_segments, code, config)
    dates_to_fetch = []

    # Recent: all codes for each date
    for target in recent_dates:
        date_str = target.strftime("%Y-%m-%d")
        for code, config in sorted_sensors:
            if (date_str, config["sensor_type"]) not in existing:
                dates_to_fetch.append((target, SEGMENTS_RECENT, code, config))

    # Backfill: all codes for each date
    for target in backfill_dates:
        date_str = target.strftime("%Y-%m-%d")
        for code, config in sorted_sensors:
            if (date_str, config["sensor_type"]) not in existing:
                dates_to_fetch.append((target, SEGMENTS_BACKFILL, code, config))

    if not dates_to_fetch:
        logger.info("All dates/sensors already covered. Nothing to fetch.")
        async with safe_connect() as db:
            report = await get_coverage_report(db)
        _log_coverage_report(report)
        pct = report['coverage_pct']
        if pct < 100:
            send_discord(
                "⚠️ S-net Multi-Sensor — Stalled",
                f"No new dates fetched. Coverage stuck at {pct}%\n"
                f"{report['total_dates']} dates, {report['total_rows']:,} rows",
                color=15105570,  # orange
            )
            _create_stall_issue(report, backfill_dates, recent_dates)
        return

    # Count by type
    n_recent = len([d for d in dates_to_fetch if d[1] == SEGMENTS_RECENT])
    n_backfill = len([d for d in dates_to_fetch if d[1] == SEGMENTS_BACKFILL])
    code_counts = {}
    for _, _, code, _ in dates_to_fetch:
        code_counts[code] = code_counts.get(code, 0) + 1
    est_requests = sum(d[1] for d in dates_to_fetch)

    logger.info(
        "Fetch schedule: %d recent + %d backfill = %d items (~%d requests)",
        n_recent, n_backfill, len(dates_to_fetch), est_requests,
    )
    logger.info("  Per code: %s", ", ".join(f"{k}: {v}" for k, v in sorted(code_counts.items())))

    send_discord(
        "🌊 S-net Multi-Sensor Fetch Starting",
        f"{len(dates_to_fetch)} items (~{est_requests} requests)",
        fields=[
            {"name": "Recent", "value": str(n_recent), "inline": True},
            {"name": "Backfill", "value": str(n_backfill), "inline": True},
            {"name": "Codes", "value": ", ".join(f"{k}: {v}" for k, v in sorted(code_counts.items())), "inline": False},
        ],
    )

    # Incremental fetch + immediate DB save.
    # All HinetPy + DB work runs synchronously in an executor thread.
    # Each item's records are committed immediately so data survives timeout kills.

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, _fetch_and_save, user, password, dates_to_fetch,
    )
    inserted, skipped = result

    if inserted == 0:
        logger.warning("No waveform records retrieved")
        send_discord(
            "⚠️ S-net Waveform — No Data",
            "Fetch completed but no records were retrieved. "
            "Check Hi-net credentials and quota.",
            color=15158332,
        )
        return

    logger.info("Inserted %d records (%d skipped/duplicate)", inserted, skipped)

    # Generate and report coverage
    async with safe_connect() as db:
        report = await get_coverage_report(db)

    _log_coverage_report(report)

    # Save coverage report as JSON artifact
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    coverage_path = results_dir / "snet_coverage.json"
    coverage_path.write_text(json.dumps(report, indent=2, default=str))
    logger.info("Coverage report saved to %s", coverage_path)

    # Final Discord notification with full coverage
    gap_text = "None" if not report["top_gaps"] else "\n".join(
        f"  {g[0]} → {g[1]} ({g[2]}d)" for g in report["top_gaps"][:3]
    )
    segment_text = "\n".join(
        f"  {seg}: {info['stations']} stn, {info['dates']} days"
        for seg, info in sorted(report["segments"].items())
    ) if report["segments"] else "No segment data"
    sensor_text = "\n".join(
        f"  {st}: {info['rows']:,} rows, {info['dates']} days"
        for st, info in sorted(report.get("sensor_coverage", {}).items())
    ) or "—"

    send_discord(
        "🌊 S-net Multi-Sensor Fetch Complete",
        f"Inserted {inserted:,} records",
        fields=[
            {"name": "Coverage", "value": f"{report['coverage_pct']}% ({report['total_dates']}/{report['expected_dates']} days)", "inline": True},
            {"name": "Date range", "value": f"{report['first_date']} → {report['last_date']}", "inline": True},
            {"name": "Total rows", "value": f"{report['total_rows']:,}", "inline": True},
            {"name": "Sensors", "value": sensor_text, "inline": False},
            {"name": "Top gaps", "value": gap_text, "inline": False},
        ],
        color=5763719 if report["coverage_pct"] > 80 else 16776960,
    )


def _create_stall_issue(report: dict, backfill_dates: list, recent_dates: list) -> None:
    """Create a GitHub Issue when S-net fetch window is all covered but overall coverage < 100%."""
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not repo:
        logger.info("GITHUB_REPOSITORY not set; skipping stall issue creation")
        return

    pct = report["coverage_pct"]
    window_dates = recent_dates + backfill_dates
    window_start = min(d.strftime("%Y-%m-%d") for d in window_dates) if window_dates else "unknown"
    window_end = max(d.strftime("%Y-%m-%d") for d in window_dates) if window_dates else "unknown"

    gaps_text = ""
    if report.get("top_gaps"):
        gaps_text = "\n### Top coverage gaps in DB\n"
        for g_start, g_end, g_days in report["top_gaps"]:
            gaps_text += f"- `{g_start}` → `{g_end}`: {g_days} missing days\n"

    body = (
        f"## S-net Waveform Fetch Stalled\n\n"
        f"**Coverage**: {pct}% ({report['total_dates']}/{report['expected_dates']} days,"
        f" {report['total_rows']:,} rows)\n"
        f"**DB date range**: {report['first_date']} → {report['last_date']}\n"
        f"**Checked window this run**: {window_start} → {window_end}\n\n"
        f"All dates in the current fetch window are already in the DB, "
        f"but overall coverage is {pct}% (< 100%). "
        f"Missing dates are likely outside the current backfill window or failed to decode.\n"
        f"{gaps_text}\n"
        f"_Auto-created by `fetch_snet_waveform.py`_"
    )
    title = f"S-net waveform stalled at {pct}%"

    # Avoid creating duplicate issues
    check = subprocess.run(
        ["gh", "issue", "list", "--repo", repo, "--state", "open",
         "--label", "backfill", "--search", "S-net waveform stalled",
         "--json", "number", "--limit", "1"],
        capture_output=True, text=True,
    )
    if check.returncode == 0:
        try:
            existing = json.loads(check.stdout or "[]")
            if existing:
                logger.info("Stall issue already open (#%s); skipping", existing[0]["number"])
                return
        except Exception:
            pass

    result = subprocess.run(
        ["gh", "issue", "create", "--repo", repo,
         "--title", title, "--body", body,
         "--label", "bug", "--label", "backfill"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Stall issue created: %s", result.stdout.strip())
    else:
        logger.warning("Failed to create stall issue: %s", result.stderr.strip())


def _log_coverage_report(report: dict) -> None:
    """Log coverage report to console."""
    logger.info("=== S-net Waveform Coverage ===")
    logger.info("  Date range: %s → %s", report.get("first_date"), report.get("last_date"))
    logger.info("  Coverage: %s%% (%d/%d days)",
                report["coverage_pct"], report["total_dates"], report["expected_dates"])
    logger.info("  Gap days: %d", report["gap_days"])
    logger.info("  Total rows: %d", report["total_rows"])
    if report["top_gaps"]:
        logger.info("  Top gaps:")
        for g in report["top_gaps"][:5]:
            logger.info("    %s → %s (%d days)", g[0], g[1], g[2])
    if report.get("sensor_coverage"):
        logger.info("  Per sensor type:")
        for st, info in sorted(report["sensor_coverage"].items()):
            logger.info("    %s: %d rows, %d dates", st, info["rows"], info["dates"])
    if report["segments"]:
        logger.info("  Per segment:")
        for seg, info in sorted(report["segments"].items()):
            logger.info("    %s: %d stations, %d dates", seg, info["stations"], info["dates"])
    if report.get("stations_per_date"):
        logger.info("  Recent stations/date:")
        for d, n in sorted(report["stations_per_date"].items()):
            logger.info("    %s: %d stations", d, n)


if __name__ == "__main__":
    asyncio.run(main())
