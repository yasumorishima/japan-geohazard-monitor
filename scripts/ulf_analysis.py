"""ULF magnetic anomaly analysis for earthquake precursors.

Analyzes geomagnetic 1-minute data from Kakioka (KAK), Memambetsu (MMB),
and Kanoya (KNY) for ULF precursor signatures before M5+ earthquakes.

Implemented methods:

1. **ULF Power Spectral Density** (Hayakawa et al., 2007)
   - Compute power in 0.01-0.1 Hz (Pc1-2) band using FFT of 1-hour windows
   - Compare pre-earthquake vs baseline power levels
   - Hypothesis: microfracturing → piezomagnetic/electrokinetic emission

2. **Polarization Ratio Sz/Sh** (Hattori, 2004)
   - Vertical (Z) vs horizontal (H) spectral power ratio
   - Ratio increase = source below ground (not ionospheric)
   - Ionospheric signals have Sh > Sz; lithospheric have Sz > Sh

3. **Fractal Dimension** (Gotoh et al., 2004)
   - Higuchi method applied to Z-component time series
   - Decrease in D before earthquakes = signal becomes more regular

4. **Nighttime-only analysis** (Hattori 2004)
   - 0-6 LT only: avoids human-made electromagnetic noise
   - Essential for ULF precursor detection in Japan

Physical distance relevance:
   - KAK: 36.23°N → sensitive to Kanto/Tokai region (≤300km)
   - MMB: 43.91°N → sensitive to Hokkaido
   - KNY: 31.42°N → sensitive to Kyushu/Nankai
"""

import asyncio
import json
import logging
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"
DEG_TO_KM = 111.32

STATIONS = {
    "KAK": {"lat": 36.23, "lon": 140.19},
    "MMB": {"lat": 43.91, "lon": 144.19},
    "KNY": {"lat": 31.42, "lon": 130.88},
}


# ---------------------------------------------------------------------------
# Signal processing (pure Python, no numpy/scipy dependency)
# ---------------------------------------------------------------------------

def fft_power(values: list[float], sample_rate_hz: float = 1 / 60.0,
              freq_lo: float = 0.001, freq_hi: float = 0.1) -> float | None:
    """Compute spectral power in [freq_lo, freq_hi] Hz band using DFT.

    Uses a simple radix-2 DFT on 1-minute sampled data.
    For 60 samples (1 hour), frequency resolution = 1/3600 Hz ≈ 0.00028 Hz.
    """
    n = len(values)
    if n < 16:
        return None

    # Pad to power of 2
    n_fft = 1
    while n_fft < n:
        n_fft *= 2

    # Zero-pad and remove mean
    mean_val = sum(values) / n
    padded = [(v - mean_val) for v in values] + [0.0] * (n_fft - n)

    # DFT (direct computation for small N, O(N²) but N≤128 so fast enough)
    power_sum = 0.0
    n_bins = 0

    for k in range(n_fft // 2):
        freq = k * sample_rate_hz / n_fft
        if freq < freq_lo or freq > freq_hi:
            continue

        real = sum(padded[j] * math.cos(2 * math.pi * k * j / n_fft) for j in range(n_fft))
        imag = sum(padded[j] * math.sin(2 * math.pi * k * j / n_fft) for j in range(n_fft))
        power = (real ** 2 + imag ** 2) / n_fft
        power_sum += power
        n_bins += 1

    return power_sum / max(n_bins, 1) if n_bins > 0 else None


def polarization_ratio(z_values: list[float], h_values: list[float],
                       sample_rate_hz: float = 1 / 60.0) -> float | None:
    """Compute Sz/Sh polarization ratio in ULF band.

    Sz = spectral power of Z component
    Sh = spectral power of H component
    Ratio > 1 suggests lithospheric origin.
    """
    sz = fft_power(z_values, sample_rate_hz)
    sh = fft_power(h_values, sample_rate_hz)
    if sz is None or sh is None or sh < 1e-10:
        return None
    return sz / sh


def higuchi_fractal_dimension(values: list[float], k_max: int = 10) -> float | None:
    """Compute fractal dimension using Higuchi (1988) method.

    D ≈ 1.5 for random walk (Brownian motion)
    D ≈ 2.0 for white noise
    D decrease → signal becomes more regular (possible precursor)
    """
    n = len(values)
    if n < k_max * 4:
        return None

    log_k = []
    log_l = []

    for k in range(1, k_max + 1):
        l_sum = 0.0
        n_m = 0
        for m in range(1, k + 1):
            curve_len = 0.0
            n_points = int((n - m) / k)
            if n_points < 2:
                continue
            for i in range(1, n_points):
                idx1 = m + i * k - 1
                idx0 = m + (i - 1) * k - 1
                if idx1 < n and idx0 < n:
                    curve_len += abs(values[idx1] - values[idx0])
            norm = (n - 1) / (k * n_points * k)
            l_sum += curve_len * norm
            n_m += 1

        if n_m > 0:
            l_avg = l_sum / n_m
            if l_avg > 0:
                log_k.append(math.log(1 / k))
                log_l.append(math.log(l_avg))

    if len(log_k) < 3:
        return None

    # Linear regression slope = fractal dimension
    n_pts = len(log_k)
    mean_x = sum(log_k) / n_pts
    mean_y = sum(log_l) / n_pts
    num = sum((log_k[i] - mean_x) * (log_l[i] - mean_y) for i in range(n_pts))
    den = sum((log_k[i] - mean_x) ** 2 for i in range(n_pts))
    if abs(den) < 1e-15:
        return None
    return num / den


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

async def run_ulf_analysis(min_mag: float = 5.0):
    logger.info("=== ULF Magnetic Anomaly Analysis (min_mag=%.1f) ===", min_mag)

    async with aiosqlite.connect(DB_PATH) as db:
        # Check ULF data availability
        ulf_count = await db.execute_fetchall(
            "SELECT station, COUNT(*), MIN(observed_at), MAX(observed_at) "
            "FROM ulf_magnetic GROUP BY station"
        )
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= ? AND magnitude IS NOT NULL "
            "ORDER BY occurred_at",
            (min_mag,),
        )

    if not ulf_count:
        logger.warning("No ULF data available. Run fetch_kakioka_ulf.py first.")
        return {"error": "no_ulf_data"}

    station_info = {}
    for row in ulf_count:
        station_info[row[0]] = {
            "n_records": row[1],
            "date_range": f"{row[2]} to {row[3]}",
        }
        logger.info("  %s: %d records (%s to %s)", row[0], row[1], row[2], row[3])

    # Parse earthquakes
    targets = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            targets.append({
                "time": t, "mag": r[1], "lat": r[2], "lon": r[3],
                "depth": r[4] if r[4] else 10.0,
            })
        except (ValueError, TypeError):
            continue

    logger.info("  Target events (M%.1f+): %d", min_mag, len(targets))

    # For each station, analyze ULF anomalies around earthquakes
    results_by_station = {}

    for station, stn_loc in STATIONS.items():
        if station not in station_info:
            continue

        logger.info("  --- Station %s (%.2f°N, %.2f°E) ---", station, stn_loc["lat"], stn_loc["lon"])

        # Find nearby earthquakes (within 300km)
        nearby_targets = []
        for e in targets:
            dist_km = math.sqrt(
                ((e["lat"] - stn_loc["lat"]) * DEG_TO_KM) ** 2
                + ((e["lon"] - stn_loc["lon"]) * DEG_TO_KM * math.cos(math.radians(stn_loc["lat"]))) ** 2
            )
            if dist_km <= 300:
                nearby_targets.append({**e, "dist_km": round(dist_km, 1)})

        logger.info("    Nearby M%.1f+ events (≤300km): %d", min_mag, len(nearby_targets))

        if not nearby_targets:
            results_by_station[station] = {"n_nearby": 0, "status": "no_nearby_events"}
            continue

        # Analyze each nearby earthquake
        event_results = []

        for e in nearby_targets:
            async with aiosqlite.connect(DB_PATH) as db:
                # Get ULF data ±7 days around earthquake
                eq_date = e["time"].strftime("%Y-%m-%d")
                start = (e["time"] - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")
                end = (e["time"] + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59")

                rows = await db.execute_fetchall(
                    "SELECT observed_at, h_nt, d_nt, z_nt, f_nt "
                    "FROM ulf_magnetic "
                    "WHERE station = ? AND observed_at >= ? AND observed_at <= ? "
                    "ORDER BY observed_at",
                    (station, start, end),
                )

            if len(rows) < 60:  # Need at least 1 hour of data
                continue

            # Split into pre-event (nighttime 0-6 LT, days -7 to -1)
            # and baseline (days +1 to +7)
            pre_h, pre_z = [], []
            post_h, post_z = [], []

            for r in rows:
                try:
                    t = datetime.fromisoformat(r[0])
                    hour = t.hour
                    # Nighttime only: 0-6 JST (= 15-21 UTC previous day or 0-6 UTC +9h)
                    # Since data is in UTC, nighttime JST = 15:00-21:00 UTC
                    is_night = 15 <= hour <= 21

                    dt_days = (t - e["time"].replace(tzinfo=None)).total_seconds() / 86400

                    h_val = r[1]
                    z_val = r[3]

                    if h_val is None or z_val is None:
                        continue

                    if is_night and -7 <= dt_days < 0:
                        pre_h.append(h_val)
                        pre_z.append(z_val)
                    elif is_night and 0 < dt_days <= 7:
                        post_h.append(h_val)
                        post_z.append(z_val)
                except (ValueError, TypeError):
                    continue

            if len(pre_h) < 30 or len(post_h) < 30:
                continue

            # Compute metrics
            pre_power_z = fft_power(pre_z[:60])
            post_power_z = fft_power(post_z[:60])
            pre_polar = polarization_ratio(pre_z[:60], pre_h[:60])
            post_polar = polarization_ratio(post_z[:60], post_h[:60])
            pre_fd = higuchi_fractal_dimension(pre_z[:120])
            post_fd = higuchi_fractal_dimension(post_z[:120])

            event_results.append({
                "time": e["time"].isoformat(),
                "mag": e["mag"],
                "dist_km": e["dist_km"],
                "n_pre": len(pre_h),
                "n_post": len(post_h),
                "pre_power_z": round(pre_power_z, 4) if pre_power_z else None,
                "post_power_z": round(post_power_z, 4) if post_power_z else None,
                "power_ratio_pre_post": round(pre_power_z / post_power_z, 3) if pre_power_z and post_power_z and post_power_z > 0 else None,
                "pre_polarization_sz_sh": round(pre_polar, 3) if pre_polar else None,
                "post_polarization_sz_sh": round(post_polar, 3) if post_polar else None,
                "pre_fractal_dim": round(pre_fd, 3) if pre_fd else None,
                "post_fractal_dim": round(post_fd, 3) if post_fd else None,
            })

        logger.info("    Analyzed events with sufficient ULF data: %d", len(event_results))

        # Aggregate statistics
        if event_results:
            power_ratios = [e["power_ratio_pre_post"] for e in event_results
                           if e["power_ratio_pre_post"] is not None]
            pre_polars = [e["pre_polarization_sz_sh"] for e in event_results
                         if e["pre_polarization_sz_sh"] is not None]
            post_polars = [e["post_polarization_sz_sh"] for e in event_results
                          if e["post_polarization_sz_sh"] is not None]
            pre_fds = [e["pre_fractal_dim"] for e in event_results
                      if e["pre_fractal_dim"] is not None]
            post_fds = [e["post_fractal_dim"] for e in event_results
                       if e["post_fractal_dim"] is not None]

            def safe_mean(vals):
                return round(sum(vals) / len(vals), 3) if vals else None

            results_by_station[station] = {
                "n_nearby": len(nearby_targets),
                "n_analyzed": len(event_results),
                "power_ratio_pre_post": {
                    "n": len(power_ratios),
                    "mean": safe_mean(power_ratios),
                    "gt_1_5_pct": round(sum(1 for v in power_ratios if v > 1.5) / max(len(power_ratios), 1) * 100, 1),
                    "gt_2_pct": round(sum(1 for v in power_ratios if v > 2.0) / max(len(power_ratios), 1) * 100, 1),
                    "interpretation": "ratio>1 = pre-EQ power higher (precursor signal)",
                },
                "polarization": {
                    "pre_mean": safe_mean(pre_polars),
                    "post_mean": safe_mean(post_polars),
                    "pre_gt_1_pct": round(sum(1 for v in pre_polars if v > 1.0) / max(len(pre_polars), 1) * 100, 1),
                    "interpretation": "Sz/Sh > 1 = lithospheric origin",
                },
                "fractal_dimension": {
                    "pre_mean": safe_mean(pre_fds),
                    "post_mean": safe_mean(post_fds),
                    "interpretation": "pre < post = regularization before EQ",
                },
                "event_details": event_results[:50],  # Cap for JSON size
            }
        else:
            results_by_station[station] = {
                "n_nearby": len(nearby_targets),
                "n_analyzed": 0,
                "status": "insufficient_ulf_data_for_events",
            }

    results = {
        "metadata": {
            "min_mag": min_mag,
            "stations": station_info,
            "n_targets": len(targets),
        },
        "by_station": results_by_station,
        "interpretation": {
            "expected_precursor_signatures": [
                "ULF Z-power increase 1-7 days before M6+ (power_ratio > 1.5)",
                "Polarization Sz/Sh > 1.0 before EQ (lithospheric origin)",
                "Fractal dimension decrease before EQ",
                "Signal strongest for shallow M6+ within 100-200km",
            ],
            "key_references": [
                "Hayakawa et al. 2007 - ULF anomalies before M6.8 Iwate-Miyagi Nairiku",
                "Hattori 2004 - ULF geomagnetic anomalies review",
                "Fraser-Smith et al. 1990 - Loma Prieta ULF precursor",
            ],
        },
    }

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_ulf_analysis()

    out_path = RESULTS_DIR / f"ulf_analysis_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
