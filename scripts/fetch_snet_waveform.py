"""Fetch S-net waveform features from NIED Hi-net — Phase 18.

S-net (Seafloor Observation Network for Earthquakes and Tsunamis along the
Japan Trench) has 150 ocean bottom stations with 3-component accelerometers.
This script extracts multi-scale waveform features for earthquake prediction,
replacing the single-feature pressure approach (Phase 13).

Features extracted per station per day:
    - RMS acceleration (vertical + horizontal)
    - H/V spectral ratio (site response indicator)
    - Band-specific power: low-freq (0.1–1 Hz), high-freq (1–10 Hz)
    - Spectral slope (noise source characterization)

Aggregated ML features (7 total):
    1. snet_rms_anomaly        — overall seismic noise level vs 30-day baseline
    2. snet_hv_ratio_anomaly   — H/V ratio change (structural/coupling change)
    3. snet_lf_power_anomaly   — low-freq power anomaly (slow-slip proxy)
    4. snet_hf_power_anomaly   — high-freq power anomaly (microseismicity)
    5. snet_spectral_slope_anomaly — noise source characterization change
    6. snet_spatial_gradient    — along-trench RMS gradient (stress migration)
    7. snet_segment_max_anomaly — max per-segment anomaly (localized precursor)

Backfill strategy:
    - Checks existing coverage in DB
    - Fills recent 7 days (6 segments/day) + backfill older gaps (1 segment/day)
    - HinetPy session limits: ~50 MB, ~200 req/day
    - Reports coverage & gaps via Discord webhook at each milestone

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
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH
from db import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# S-net network codes — 0120A confirmed as 3-component acceleration (100 Hz)
# TODO Phase 19: investigate 0120 (velocity?), 0120B, 0120C for additional features
SNET_NETWORK_CODE = "0120A"

# HinetPy constraints
REQUEST_DURATION_MIN = 5       # Max 5 min per request
SEGMENTS_RECENT = 6            # 6 segments for recent days (every 4h)
SEGMENTS_BACKFILL = 1          # 1 segment for backfill days (daily average)
RECENT_DAYS = 7                # Last 7 days get high-resolution sampling
MAX_BACKFILL_DAYS_PER_RUN = 60 # Conservative: ~60 days backfill per CI run
QUOTA_COOLDOWN_SEC = 2         # Pause between requests to respect quota

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
BAND_LF = (0.1, 1.0)    # Low frequency — slow-slip, tremor
BAND_HF = (1.0, 10.0)   # High frequency — local seismicity, microseisms
BAND_FULL = (0.1, 10.0)  # Full band for spectral slope estimation

# Sampling rate from test: 100 Hz
EXPECTED_FS = 100.0


# ---------------------------------------------------------------------------
# Discord notification helper
# ---------------------------------------------------------------------------

def send_discord(title: str, description: str, fields: list[dict] = None,
                 color: int = 3447003) -> None:
    """Send a Discord embed notification. Non-blocking, fail-silent."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        return
    try:
        import urllib.request
        embed = {"title": title, "description": description, "color": color}
        if fields:
            embed["fields"] = fields
        embed["footer"] = {"text": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
        payload = json.dumps({"embeds": [embed]}).encode()
        req = urllib.request.Request(
            webhook_url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
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
    -- Per-station waveform features
    rms_z REAL,              -- Vertical RMS acceleration (counts)
    rms_h REAL,              -- Horizontal RMS = sqrt(rms_x² + rms_y²)
    hv_ratio REAL,           -- H/V spectral ratio (Nakano method)
    lf_power REAL,           -- Log10 power in 0.1–1 Hz band
    hf_power REAL,           -- Log10 power in 1–10 Hz band
    spectral_slope REAL,     -- β in log-log PSD (0.1–10 Hz)
    n_samples INTEGER,       -- Number of valid samples
    latitude REAL,
    longitude REAL,
    cable_segment TEXT,      -- S1..S6
    received_at TEXT NOT NULL,
    UNIQUE(station_id, date_str, segment_hour)
);
"""

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_snet_wf_date ON snet_waveform(date_str);",
    "CREATE INDEX IF NOT EXISTS idx_snet_wf_station ON snet_waveform(station_id);",
    "CREATE INDEX IF NOT EXISTS idx_snet_wf_segment ON snet_waveform(cable_segment);",
]


async def ensure_table(db: aiosqlite.Connection) -> None:
    """Create snet_waveform table and indexes."""
    await db.execute(TABLE_DDL)
    for idx in INDEX_DDL:
        await db.execute(idx)
    await db.commit()


# ---------------------------------------------------------------------------
# Coverage analysis
# ---------------------------------------------------------------------------

async def get_existing_dates(db: aiosqlite.Connection) -> set[str]:
    """Return set of date_str values already in snet_waveform."""
    rows = await db.execute_fetchall(
        "SELECT DISTINCT date_str FROM snet_waveform"
    )
    return {r[0] for r in rows}


async def get_coverage_report(db: aiosqlite.Connection) -> dict:
    """Generate detailed coverage report."""
    total_rows = (await db.execute_fetchall(
        "SELECT COUNT(*) FROM snet_waveform"
    ))[0][0]

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

def compute_waveform_features(
    data_z, data_x, data_y, fs: float
) -> dict | None:
    """Compute waveform features from 3-component acceleration data.

    Args:
        data_z: Vertical component (numpy array)
        data_x: X (North-South) component
        data_y: Y (East-West) component
        fs: Sampling frequency (Hz)

    Returns:
        Dict with rms_z, rms_h, hv_ratio, lf_power, hf_power, spectral_slope
        or None if data is insufficient.
    """
    import numpy as np

    min_samples = int(fs * 10)  # Need at least 10 seconds
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

    # --- FFT-based PSD (Welch-like: segmented + averaged) ---
    # Use 10-second windows with 50% overlap for stable spectral estimates
    win_samples = int(fs * 10)  # 10-second window
    hop = win_samples // 2
    n_windows = max(1, (n - win_samples) // hop + 1)

    # Hanning window
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

        fft_z = np.fft.rfft(seg_z)
        fft_x = np.fft.rfft(seg_x)
        fft_y = np.fft.rfft(seg_y)

        psd_z += np.abs(fft_z) ** 2
        psd_x += np.abs(fft_x) ** 2
        psd_y += np.abs(fft_y) ** 2

    # Normalize
    scale = 2.0 / (fs * hann_norm * n_windows) if n_windows > 0 else 1.0
    psd_z *= scale
    psd_x *= scale
    psd_y *= scale

    freqs = np.fft.rfftfreq(win_samples, d=1.0 / fs)

    # --- H/V spectral ratio (Nakamura method) ---
    psd_h = psd_x + psd_y
    # Avoid division by zero
    safe_z = np.where(psd_z > 0, psd_z, 1e-30)
    hv_spectrum = np.sqrt(psd_h / safe_z)

    # Average H/V in 0.1–10 Hz band
    mask_hv = (freqs >= 0.1) & (freqs <= 10.0)
    if np.any(mask_hv):
        hv_ratio = float(np.mean(hv_spectrum[mask_hv]))
    else:
        hv_ratio = 1.0

    # --- Band power (log10) ---
    def band_power(psd, f_low, f_high):
        mask = (freqs >= f_low) & (freqs < f_high)
        if not np.any(mask):
            return 0.0
        # np.trapezoid (NumPy 2.0+) / np.trapz (legacy)
        _trapz = getattr(np, "trapezoid", None) or np.trapz
        power = float(_trapz(psd[mask], freqs[mask]))
        return math.log10(max(power, 1e-30))

    # Combined (all 3 components) for band power
    psd_total = psd_z + psd_x + psd_y
    lf_power = band_power(psd_total, *BAND_LF)
    hf_power = band_power(psd_total, *BAND_HF)

    # --- Spectral slope (linear fit in log-log space) ---
    mask_slope = (freqs >= BAND_FULL[0]) & (freqs <= BAND_FULL[1]) & (psd_total > 0)
    if np.sum(mask_slope) > 5:
        log_f = np.log10(freqs[mask_slope])
        log_p = np.log10(psd_total[mask_slope])
        # Least-squares linear fit
        n_pts = len(log_f)
        mean_f = np.mean(log_f)
        mean_p = np.mean(log_p)
        cov = np.sum((log_f - mean_f) * (log_p - mean_p))
        var_f = np.sum((log_f - mean_f) ** 2)
        spectral_slope = float(cov / var_f) if var_f > 0 else -2.0
    else:
        spectral_slope = -2.0  # Default: Brownian noise

    return {
        "rms_z": rms_z,
        "rms_h": rms_h,
        "hv_ratio": hv_ratio,
        "lf_power": lf_power,
        "hf_power": hf_power,
        "spectral_slope": spectral_slope,
    }


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
    client, station_coords: dict, target_date: datetime, n_segments: int
) -> list[dict]:
    """Fetch and process one day's S-net waveform data.

    Returns list of per-station feature dicts for each segment.
    """
    import numpy as np

    results = []
    date_str = target_date.strftime("%Y-%m-%d")
    segment_hours = [h for h in range(0, 24, max(1, 24 // n_segments))][:n_segments]

    for hour in segment_hours:
        start = target_date.replace(hour=hour, minute=0, second=0, microsecond=0)
        work_dir = tempfile.mkdtemp(prefix=f"snet_{date_str}_{hour:02d}_")

        try:
            logger.info("Requesting %s %02d:00 (%d min)", date_str, hour, REQUEST_DURATION_MIN)

            data = client.get_continuous_waveform(
                SNET_NETWORK_CODE, start, REQUEST_DURATION_MIN, outdir=work_dir,
            )
            if data is None or not isinstance(data, tuple) or len(data) != 2:
                logger.warning("No data for %s %02d:00", date_str, hour)
                continue

            win32_file, ch_table = data
            if win32_file is None:
                continue

            # Decode WIN32 → SAC
            from HinetPy import win32 as hinetwin32
            sac_files = hinetwin32.extract_sac(win32_file, ch_table, outdir=work_dir)
            if not sac_files:
                # HinetPy sometimes returns empty but files exist on disk
                sac_files = list(Path(work_dir).glob("*.SAC"))
            if not sac_files:
                logger.warning("No SAC files decoded for %s %02d:00", date_str, hour)
                continue

            # Group SAC files by station
            station_files = {}  # station_id -> {component: sac_path}
            for sac_path in sac_files:
                basename = Path(sac_path).stem
                parts = basename.split(".")
                if len(parts) < 4:
                    continue
                station_id = parts[1]
                channel = parts[3] if len(parts) > 3 else parts[-1]

                # Classify component by suffix
                suffix = channel[-1].upper() if channel else ""
                if suffix == "Z":
                    comp = "Z"
                elif suffix == "X":
                    comp = "X"
                elif suffix == "Y":
                    comp = "Y"
                else:
                    continue

                if station_id not in station_files:
                    station_files[station_id] = {}
                station_files[station_id][comp] = str(sac_path)

            # Process each station with all 3 components
            for station_id, comps in station_files.items():
                if len(comps) < 3:
                    continue  # Need all 3 components for H/V ratio

                data_z, info_z = read_sac_data(comps.get("Z", ""))
                data_x, info_x = read_sac_data(comps.get("X", ""))
                data_y, info_y = read_sac_data(comps.get("Y", ""))

                if data_z is None or data_x is None or data_y is None:
                    continue

                fs = info_z.get("fs", EXPECTED_FS) if info_z else EXPECTED_FS

                features = compute_waveform_features(data_z, data_x, data_y, fs)
                if features is None:
                    continue

                # Get coordinates
                lat, lon = station_coords.get(station_id, (None, None))
                if lat is None and info_z:
                    stla = info_z.get("stla", -12345.0)
                    stlo = info_z.get("stlo", -12345.0)
                    if stla != -12345.0 and stlo != -12345.0:
                        lat, lon = stla, stlo

                # Classify cable segment
                cable_seg = classify_cable_segment(station_id)
                if cable_seg is None and lat is not None:
                    cable_seg = classify_cable_segment_by_coords(lat, lon)

                results.append({
                    "station_id": station_id,
                    "date_str": date_str,
                    "segment_hour": hour,
                    "rms_z": features["rms_z"],
                    "rms_h": features["rms_h"],
                    "hv_ratio": features["hv_ratio"],
                    "lf_power": features["lf_power"],
                    "hf_power": features["hf_power"],
                    "spectral_slope": features["spectral_slope"],
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


def _fetch_multiple_days(
    user: str, password: str,
    dates_to_fetch: list[tuple[datetime, int]],
    station_coords_out: dict,
) -> list[dict]:
    """Fetch waveform data for multiple days.

    Args:
        dates_to_fetch: list of (date, n_segments) tuples
        station_coords_out: dict to populate with station coordinates

    Returns list of all per-station feature records.
    """
    try:
        from HinetPy import Client
    except ImportError:
        logger.error("HinetPy not installed")
        return []

    try:
        client = Client(user, password)
        logger.info("Authenticated to NIED Hi-net")
    except Exception as exc:
        logger.error("Authentication failed: %s", exc)
        return []

    # Fetch station metadata once
    try:
        stations = client.get_station_list(SNET_NETWORK_CODE)
        if stations:
            for st in stations:
                sid = getattr(st, "name", None) or getattr(st, "code", None)
                lat = getattr(st, "latitude", None)
                lon = getattr(st, "longitude", None)
                if sid and lat is not None and lon is not None:
                    station_coords_out[str(sid)] = (float(lat), float(lon))
            logger.info("Station metadata: %d stations", len(station_coords_out))
    except Exception as exc:
        logger.warning("Station metadata fetch failed: %s", exc)

    all_results = []
    total_days = len(dates_to_fetch)

    for i, (target_date, n_segments) in enumerate(dates_to_fetch):
        date_str = target_date.strftime("%Y-%m-%d")
        logger.info(
            "=== Day %d/%d: %s (%d segments) ===",
            i + 1, total_days, date_str, n_segments,
        )

        day_results = _fetch_day(client, station_coords_out, target_date, n_segments)
        all_results.extend(day_results)

        # Progress notification every 10 days or at end
        if (i + 1) % 10 == 0 or i + 1 == total_days:
            n_stations = len(set(r["station_id"] for r in day_results)) if day_results else 0
            send_discord(
                "🌊 S-net Waveform Fetch Progress",
                f"Day {i + 1}/{total_days}: {date_str}",
                fields=[
                    {"name": "Stations today", "value": str(n_stations), "inline": True},
                    {"name": "Total records", "value": f"{len(all_results):,}", "inline": True},
                    {"name": "Days remaining", "value": str(total_days - i - 1), "inline": True},
                ],
                color=3447003,
            )

    return all_results


# ---------------------------------------------------------------------------
# Main async entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Fetch S-net waveform features with incremental backfill."""
    credentials = _check_credentials()
    if credentials is None:
        logger.warning(
            "HINET_USER/HINET_PASS not set. S-net waveform fetch requires "
            "NIED Hi-net registration. Exiting gracefully."
        )
        return

    user, password = credentials
    logger.info("Starting S-net waveform feature extraction (Phase 18)")

    await init_db()

    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_table(db)
        existing_dates = await get_existing_dates(db)

    logger.info("Existing coverage: %d dates in DB", len(existing_dates))

    # Build fetch schedule
    now_utc = datetime.now(timezone.utc)
    dates_to_fetch = []

    # 1. Recent days (high resolution: 6 segments)
    for days_ago in range(1, RECENT_DAYS + 1):
        target = (now_utc - timedelta(days=days_ago)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        date_str = target.strftime("%Y-%m-%d")
        if date_str not in existing_dates:
            dates_to_fetch.append((target, SEGMENTS_RECENT))

    # 2. Backfill: oldest gaps first (1 segment per day)
    # S-net data available from ~2016-08
    snet_start = datetime(2016, 8, 15)
    backfill_candidates = []

    current = (now_utc - timedelta(days=RECENT_DAYS + 1)).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
    )
    while current >= snet_start and len(backfill_candidates) < MAX_BACKFILL_DAYS_PER_RUN * 3:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in existing_dates:
            backfill_candidates.append((current, SEGMENTS_BACKFILL))
        current -= timedelta(days=1)

    # Prioritize: most recent gaps first (more useful for ML training window)
    backfill_candidates.sort(key=lambda x: x[0], reverse=True)
    backfill_candidates = backfill_candidates[:MAX_BACKFILL_DAYS_PER_RUN]

    dates_to_fetch.extend(backfill_candidates)

    if not dates_to_fetch:
        logger.info("All dates already covered. Nothing to fetch.")
        # Still generate coverage report
        async with aiosqlite.connect(DB_PATH) as db:
            report = await get_coverage_report(db)
        _log_coverage_report(report)
        send_discord(
            "🌊 S-net Waveform — All Covered",
            f"{report['total_dates']} dates, {report['total_rows']:,} rows, "
            f"coverage {report['coverage_pct']}%",
            color=5763719,  # green
        )
        return

    logger.info(
        "Fetch schedule: %d recent + %d backfill = %d days",
        len([d for d in dates_to_fetch if d[1] == SEGMENTS_RECENT]),
        len([d for d in dates_to_fetch if d[1] == SEGMENTS_BACKFILL]),
        len(dates_to_fetch),
    )

    send_discord(
        "🌊 S-net Waveform Fetch Starting",
        f"Fetching {len(dates_to_fetch)} days "
        f"({len(existing_dates)} already in DB)",
        fields=[
            {"name": "Recent (6 seg)", "value": str(len([d for d in dates_to_fetch if d[1] == SEGMENTS_RECENT])), "inline": True},
            {"name": "Backfill (1 seg)", "value": str(len([d for d in dates_to_fetch if d[1] == SEGMENTS_BACKFILL])), "inline": True},
        ],
    )

    # Run synchronous HinetPy fetch in thread executor
    station_coords = {}
    loop = asyncio.get_event_loop()
    records = await loop.run_in_executor(
        None, _fetch_multiple_days, user, password, dates_to_fetch, station_coords,
    )

    if not records:
        logger.warning("No waveform records retrieved")
        send_discord(
            "⚠️ S-net Waveform — No Data",
            "Fetch completed but no records were retrieved. "
            "Check Hi-net credentials and quota.",
            color=15158332,  # red
        )
        return

    # Store in database
    now_str = datetime.now(timezone.utc).isoformat()
    inserted = 0
    skipped = 0

    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_table(db)
        for rec in records:
            try:
                await db.execute(
                    """INSERT OR IGNORE INTO snet_waveform
                       (station_id, date_str, segment_hour,
                        rms_z, rms_h, hv_ratio, lf_power, hf_power,
                        spectral_slope, n_samples, latitude, longitude,
                        cable_segment, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        rec["station_id"], rec["date_str"], rec["segment_hour"],
                        rec["rms_z"], rec["rms_h"], rec["hv_ratio"],
                        rec["lf_power"], rec["hf_power"], rec["spectral_slope"],
                        rec["n_samples"], rec["latitude"], rec["longitude"],
                        rec["cable_segment"], now_str,
                    ),
                )
                inserted += 1
            except Exception as exc:
                logger.warning("Insert failed for %s/%s: %s", rec["station_id"], rec["date_str"], exc)
                skipped += 1
        await db.commit()

    logger.info("Inserted %d records (%d skipped/duplicate)", inserted, skipped)

    # Generate and report coverage
    async with aiosqlite.connect(DB_PATH) as db:
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

    send_discord(
        "🌊 S-net Waveform Fetch Complete",
        f"Inserted {inserted:,} records for {len(dates_to_fetch)} days",
        fields=[
            {"name": "Coverage", "value": f"{report['coverage_pct']}% ({report['total_dates']}/{report['expected_dates']} days)", "inline": True},
            {"name": "Date range", "value": f"{report['first_date']} → {report['last_date']}", "inline": True},
            {"name": "Total rows", "value": f"{report['total_rows']:,}", "inline": True},
            {"name": "Gap days", "value": str(report["gap_days"]), "inline": True},
            {"name": "Top gaps", "value": gap_text, "inline": False},
            {"name": "Segments", "value": segment_text, "inline": False},
        ],
        color=5763719 if report["coverage_pct"] > 80 else 16776960,
    )


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
