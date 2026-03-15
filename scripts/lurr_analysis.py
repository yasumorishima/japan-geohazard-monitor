"""LURR (Load-Unload Response Ratio) analysis.

Tests whether the crust responds asymmetrically to tidal loading
near failure. LURR >> 1 indicates approaching criticality.

Method:
  1. Compute tidal phase at each M3+ earthquake time/location
  2. Classify as loading (tidal stress increasing on fault) or unloading
  3. LURR = Σ 10^(α*M) during loading / Σ 10^(α*M) during unloading
  4. Compare LURR before M5+ events vs random periods

Reference: Yin et al. (2006) Pure Appl. Geophys.
"""

import asyncio
import bisect
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
ALPHA = 1.0  # Energy weighting exponent (standard: α=1.0 for Benioff strain)


def lunar_hour_angle(t: datetime, lon: float) -> float:
    """Compute lunar hour angle at a given time and longitude.

    Simplified astronomical calculation using mean lunar elements.
    Accuracy ~1° (sufficient for tidal phase classification).
    """
    # Julian centuries from J2000.0
    jd = (t - datetime(2000, 1, 1, 12, tzinfo=timezone.utc)).total_seconds() / 86400 + 2451545.0
    T = (jd - 2451545.0) / 36525.0

    # Moon's mean longitude (deg)
    L_moon = (218.3165 + 481267.8813 * T) % 360

    # Greenwich Mean Sidereal Time (deg)
    gmst = (280.46061837 + 360.98564736629 * (jd - 2451545.0)) % 360

    # Local lunar hour angle
    lha = (gmst + lon - L_moon) % 360
    return lha


def tidal_loading(t: datetime, lat: float, lon: float,
                  strike: float, dip: float) -> bool:
    """Determine if tidal stress is loading a fault at given time/location.

    Uses the M2 (lunar semidiurnal) tide as dominant component.
    Loading = tidal Coulomb stress is positive (promotes failure).

    For a thrust fault, loading occurs when the tidal potential creates
    compression perpendicular to the fault, increasing shear stress.
    Simplified: loading when tidal phase is in the promoting quadrant.
    """
    lha = lunar_hour_angle(t, lon)

    # M2 tidal phase = 2 * lunar hour angle
    phase = (2 * lha) % 360

    # For thrust faults (dominant in Japan subduction):
    # Loading when tidal compression is roughly perpendicular to fault strike
    # Simplified: loading during rising phase of tidal stress
    # Phase 0-90° and 180-270° are "rising" (loading)
    # Phase 90-180° and 270-360° are "falling" (unloading)
    # Adjust for fault strike
    adjusted = (phase - strike) % 360
    return adjusted < 90 or (180 <= adjusted < 270)


def default_mechanism(lat, lon, depth):
    """Regional default strike/dip for Japan."""
    if depth > 70:
        return 200.0, 45.0
    elif lon > 142 and lat > 35:
        return 200.0, 25.0
    elif lon < 137 and lat < 35:
        return 240.0, 15.0
    else:
        return 200.0, 35.0


async def run_lurr_analysis(min_mag_target: float = 5.0):
    logger.info("=== LURR Analysis (target M%.1f+) ===", min_mag_target)

    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
            "ORDER BY occurred_at"
        )

    events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events.append({"time": t, "mag": r[1], "lat": r[2], "lon": r[3],
                           "depth": r[4] if r[4] else 10.0})
        except (ValueError, TypeError):
            continue

    t0 = events[0]["time"]
    for e in events:
        e["t_days"] = (e["time"] - t0).total_seconds() / 86400
    all_times = [e["t_days"] for e in events]
    T_total = all_times[-1]

    # Classify each M3+ event as loading or unloading
    logger.info("  Classifying %d events by tidal phase...", len(events))
    for e in events:
        strike, dip = default_mechanism(e["lat"], e["lon"], e["depth"])
        e["loading"] = tidal_loading(e["time"], e["lat"], e["lon"], strike, dip)

    n_loading = sum(1 for e in events if e["loading"])
    logger.info("  Loading: %d (%.1f%%), Unloading: %d",
                n_loading, 100 * n_loading / len(events), len(events) - n_loading)

    # ---------------------------------------------------------------
    # Compute LURR in windows before each M5+ event
    # ---------------------------------------------------------------
    targets = [e for e in events if e["mag"] >= min_mag_target]
    logger.info("  Targets: %d M%.1f+ events", len(targets), min_mag_target)

    def compute_lurr(center_lat, center_lon, t_end_days, window_days=90, radius_deg=2.0):
        """Compute LURR in a space-time window."""
        t_start = t_end_days - window_days
        idx_start = bisect.bisect_left(all_times, t_start)
        idx_end = bisect.bisect_left(all_times, t_end_days)

        load_sum = 0.0
        unload_sum = 0.0
        n_events = 0

        for i in range(idx_start, idx_end):
            e = events[i]
            if abs(e["lat"] - center_lat) > radius_deg or abs(e["lon"] - center_lon) > radius_deg:
                continue
            energy = 10 ** (ALPHA * e["mag"])
            if e["loading"]:
                load_sum += energy
            else:
                unload_sum += energy
            n_events += 1

        if unload_sum < 1e-10 or n_events < 10:
            return None, n_events
        return load_sum / unload_sum, n_events

    # LURR before each target event
    lurr_eq = []
    for te in targets:
        if te["t_days"] < 100:
            continue
        lurr, n = compute_lurr(te["lat"], te["lon"], te["t_days"])
        if lurr is not None:
            lurr_eq.append({
                "time": te["time"].isoformat()[:16],
                "mag": te["mag"],
                "lurr": round(lurr, 3),
                "n_events": n,
            })

    # LURR at random times/locations
    random.seed(42)
    lurr_rand = []
    for _ in range(500):
        rt = 100 + random.random() * (T_total - 200)
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25
        lurr, n = compute_lurr(rlat, rlon, rt)
        if lurr is not None:
            lurr_rand.append(round(lurr, 3))

    def lurr_stats(values):
        if not values:
            return {"n": 0}
        v = [x["lurr"] if isinstance(x, dict) else x for x in values]
        s = sorted(v)
        n = len(s)
        return {
            "n": n,
            "mean": round(sum(s) / n, 3),
            "median": round(s[n // 2], 3),
            "p10": round(s[int(n * 0.1)], 3),
            "p90": round(s[int(n * 0.9)], 3),
            "gt_1_pct": round(sum(1 for x in s if x > 1.0) / n * 100, 1),
            "gt_1_5_pct": round(sum(1 for x in s if x > 1.5) / n * 100, 1),
            "gt_2_pct": round(sum(1 for x in s if x > 2.0) / n * 100, 1),
        }

    eq_stats = lurr_stats(lurr_eq)
    rand_stats = lurr_stats(lurr_rand)

    # LURR > 1.5 lift
    lift_15 = eq_stats["gt_1_5_pct"] / max(rand_stats["gt_1_5_pct"], 0.1)

    # Multiple windows
    window_results = {}
    for w in [30, 60, 90, 180]:
        w_eq = []
        for te in targets:
            if te["t_days"] < w + 10:
                continue
            lurr, n = compute_lurr(te["lat"], te["lon"], te["t_days"], window_days=w)
            if lurr is not None:
                w_eq.append(lurr)
        w_rand = []
        random.seed(42 + w)
        for _ in range(300):
            rt = w + 10 + random.random() * (T_total - w - 20)
            rlat = 25 + random.random() * 20
            rlon = 125 + random.random() * 25
            lurr, n = compute_lurr(rlat, rlon, rt, window_days=w)
            if lurr is not None:
                w_rand.append(lurr)
        window_results[f"{w}d"] = {
            "earthquake": lurr_stats(w_eq),
            "random": lurr_stats(w_rand),
        }

    results = {
        "catalog_stats": {
            "n_events": len(events),
            "n_loading": n_loading,
            "loading_pct": round(100 * n_loading / len(events), 1),
            "n_targets": len(targets),
        },
        "lurr_90d": {
            "earthquake": eq_stats,
            "random": rand_stats,
            "lift_gt_1_5": round(lift_15, 2),
        },
        "by_window": window_results,
        "sample_events": lurr_eq[:20],
    }

    logger.info("  LURR 90d — EQ: mean=%.3f >1.5=%.1f%% | Rand: mean=%.3f >1.5=%.1f%% | Lift=%.2f",
                eq_stats["mean"], eq_stats["gt_1_5_pct"],
                rand_stats["mean"], rand_stats["gt_1_5_pct"], lift_15)

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = await run_lurr_analysis()
    out_path = RESULTS_DIR / f"lurr_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
