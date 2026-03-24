"""Seismicity rate anomaly analysis (model-free + constrained ETAS).

Two complementary approaches:
1. Model-free: Compare observed M3+ rate before M5+ events with
   the long-term regional average rate (no parameter fitting needed)
2. Constrained ETAS: Use literature values for Japan (Ogata 1998)
   to compute expected rate, then check residuals

Both test whether M5+ events are preceded by anomalous seismicity
(activation or quiescence) that isn't explained by baseline rates.
"""

import argparse
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
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

# Constrained ETAS parameters from Ogata (1998) / Ogata & Zhuang (2006)
# for Japan M3+ catalog
ETAS_PARAMS = {
    "mu": None,  # Estimated from data (long-term average - aftershock fraction)
    "K": 0.04,   # Aftershock productivity (literature range: 0.01-0.05)
    "alpha": 1.0, # Magnitude scaling (literature: 0.8-1.2)
    "c": 0.01,   # Omori offset in days (literature: 0.005-0.05)
    "p": 1.1,    # Omori exponent (literature: 1.0-1.3, must be >1 for convergence)
    "Mc": 3.0,
}

DEG_TO_KM = 111.32


async def run_etas_analysis(min_mag_target: float = 5.0) -> dict:
    """Run seismicity rate anomaly analysis."""
    logger.info("=== Seismicity Rate Anomaly Analysis (target M%.1f+) ===", min_mag_target)

    async with safe_connect() as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
            "ORDER BY occurred_at"
        )

    events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events.append({
                "time": t,
                "mag": r[1],
                "lat": r[2],
                "lon": r[3],
                "depth": r[4],
            })
        except (ValueError, TypeError):
            continue

    if len(events) < 100:
        return {"error": "Insufficient data", "n": len(events)}

    t0 = events[0]["time"]
    for e in events:
        e["t_days"] = (e["time"] - t0).total_seconds() / 86400

    T_total = events[-1]["t_days"] - events[0]["t_days"]
    target_events = [e for e in events if e["mag"] >= min_mag_target]

    logger.info("  Catalog: %d M3+ events over %.0f days, %d M%.1f+ targets",
                len(events), T_total, len(target_events), min_mag_target)

    # Pre-compute time index for fast lookups
    all_times = [e["t_days"] for e in events]

    # ---------------------------------------------------------------
    # Approach 1: Model-free regional rate comparison
    # ---------------------------------------------------------------
    logger.info("  --- Model-free rate analysis ---")

    # Long-term average M3+ rate (events/day, full catalog)
    global_rate = len(events) / T_total

    # For each M5+ event: count M3+ in preceding 7 and 30 days
    # Compare with long-term rate in same spatial region (2° box)
    def regional_rate(lat, lon, radius_deg=2.0):
        """Long-term M3+ rate in a spatial box (events/day)."""
        n = sum(1 for e in events
                if abs(e["lat"] - lat) <= radius_deg
                and abs(e["lon"] - lon) <= radius_deg)
        return n / T_total

    def observed_count(t_target, window_days, lat=None, lon=None, radius_deg=None):
        """Count M3+ events in [t-window, t]."""
        t_start = t_target - window_days
        idx_start = bisect.bisect_left(all_times, t_start)
        idx_end = bisect.bisect_left(all_times, t_target)
        if lat is None:
            return idx_end - idx_start
        # Spatial filter
        return sum(1 for i in range(idx_start, idx_end)
                   if abs(events[i]["lat"] - lat) <= radius_deg
                   and abs(events[i]["lon"] - lon) <= radius_deg)

    rate_results_7d = []
    rate_results_30d = []

    for te in target_events:
        if te["t_days"] < 35:  # Need at least 35 days of history
            continue

        # 7-day window, regional (2° box)
        obs_7d = observed_count(te["t_days"], 7, te["lat"], te["lon"], 2.0)
        exp_7d = regional_rate(te["lat"], te["lon"], 2.0) * 7
        ratio_7d = obs_7d / max(exp_7d, 0.1)

        # 30-day window, regional
        obs_30d = observed_count(te["t_days"], 30, te["lat"], te["lon"], 2.0)
        exp_30d = regional_rate(te["lat"], te["lon"], 2.0) * 30
        ratio_30d = obs_30d / max(exp_30d, 0.1)

        rate_results_7d.append({
            "time": te["time"].isoformat()[:16],
            "mag": te["mag"],
            "observed": obs_7d,
            "expected": round(exp_7d, 1),
            "ratio": round(ratio_7d, 3),
        })
        rate_results_30d.append({
            "ratio": round(ratio_30d, 3),
        })

    # Random baseline
    random.seed(42)
    rand_ratios_7d = []
    rand_ratios_30d = []
    for _ in range(500):
        rt = 35 + random.random() * (T_total - 70)
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25

        obs_7d = observed_count(rt, 7, rlat, rlon, 2.0)
        exp_7d = regional_rate(rlat, rlon, 2.0) * 7
        ratio_7d = obs_7d / max(exp_7d, 0.1)
        rand_ratios_7d.append(round(ratio_7d, 3))

        obs_30d = observed_count(rt, 30, rlat, rlon, 2.0)
        exp_30d = regional_rate(rlat, rlon, 2.0) * 30
        ratio_30d = obs_30d / max(exp_30d, 0.1)
        rand_ratios_30d.append(round(ratio_30d, 3))

    def ratio_stats(values):
        if not values:
            return {"n": 0}
        s = sorted(values)
        n = len(s)
        return {
            "n": n,
            "mean": round(sum(s) / n, 3),
            "median": round(s[n // 2], 3),
            "p10": round(s[int(n * 0.1)], 3),
            "p90": round(s[int(n * 0.9)], 3),
            "gt_2_pct": round(sum(1 for v in s if v > 2.0) / n * 100, 1),
            "gt_3_pct": round(sum(1 for v in s if v > 3.0) / n * 100, 1),
            "lt_05_pct": round(sum(1 for v in s if v < 0.5) / n * 100, 1),
            "lt_03_pct": round(sum(1 for v in s if v < 0.3) / n * 100, 1),
        }

    eq_7d_ratios = [r["ratio"] for r in rate_results_7d]
    eq_30d_ratios = [r["ratio"] for r in rate_results_30d]

    eq_7d_stats = ratio_stats(eq_7d_ratios)
    rand_7d_stats = ratio_stats(rand_ratios_7d)
    eq_30d_stats = ratio_stats(eq_30d_ratios)
    rand_30d_stats = ratio_stats(rand_ratios_30d)

    # Activation lift
    act_7d_lift = eq_7d_stats["gt_2_pct"] / max(rand_7d_stats["gt_2_pct"], 0.1)
    qui_7d_lift = eq_7d_stats["lt_05_pct"] / max(rand_7d_stats["lt_05_pct"], 0.1)

    logger.info("  7-day rate ratio — EQ: mean=%.2f >2=%.1f%% <0.5=%.1f%% | Rand: mean=%.2f >2=%.1f%% <0.5=%.1f%%",
                eq_7d_stats["mean"], eq_7d_stats["gt_2_pct"], eq_7d_stats["lt_05_pct"],
                rand_7d_stats["mean"], rand_7d_stats["gt_2_pct"], rand_7d_stats["lt_05_pct"])
    logger.info("  Activation lift (>2x): %.2f | Quiescence lift (<0.5x): %.2f", act_7d_lift, qui_7d_lift)

    # ---------------------------------------------------------------
    # Approach 2: Constrained ETAS residuals
    # ---------------------------------------------------------------
    logger.info("  --- Constrained ETAS analysis ---")

    # Estimate mu from data: total rate minus estimated aftershock fraction
    # Aftershock fraction ~60-70% for Japan (Ogata 1998)
    mu_est = global_rate * 0.35  # 35% background
    params = dict(ETAS_PARAMS)
    params["mu"] = mu_est

    events_td = [(e["t_days"], e["mag"]) for e in events]

    def etas_rate(t_days, recent_events):
        """Compute ETAS rate at time t from recent events."""
        rate = params["mu"]
        K, alpha, c, p, Mc = params["K"], params["alpha"], params["c"], params["p"], params["Mc"]
        for ti, mi in recent_events:
            dt = t_days - ti
            if dt <= 0:
                continue
            rate += K * math.exp(alpha * (mi - Mc)) / (dt + c) ** p
        return rate

    def etas_expected_7d(t_target, idx_in_catalog):
        """Expected M3+ count in 7 days before target using ETAS."""
        n_samples = 7
        dt = 7.0 / n_samples
        total = 0.0
        for k in range(n_samples):
            t_sample = t_target - 7 + (k + 0.5) * dt
            t_cutoff = t_sample - 90
            cutoff_idx = bisect.bisect_left(all_times, t_cutoff)
            # Only use events before t_sample
            sample_idx = bisect.bisect_left(all_times, t_sample)
            recent = events_td[cutoff_idx:min(sample_idx, idx_in_catalog)]
            rate = etas_rate(t_sample, recent)
            total += rate * dt
        return total

    etas_ratios = []
    for te in target_events:
        if te["t_days"] < 100:
            continue
        idx = bisect.bisect_left(all_times, te["t_days"])
        expected = etas_expected_7d(te["t_days"], idx)
        t_start = te["t_days"] - 7
        idx_start = bisect.bisect_left(all_times, t_start)
        observed = idx - idx_start
        ratio = observed / max(expected, 0.1)
        etas_ratios.append(round(ratio, 3))

    etas_rand_ratios = []
    random.seed(99)
    for _ in range(500):
        rt = 100 + random.random() * (T_total - 200)
        idx = bisect.bisect_left(all_times, rt)
        if idx < 50:
            continue
        expected = etas_expected_7d(rt, idx)
        t_start = rt - 7
        idx_start = bisect.bisect_left(all_times, t_start)
        observed = idx - idx_start
        ratio = observed / max(expected, 0.1)
        etas_rand_ratios.append(round(ratio, 3))

    etas_eq_stats = ratio_stats(etas_ratios)
    etas_rand_stats = ratio_stats(etas_rand_ratios)

    logger.info("  ETAS ratio — EQ: mean=%.2f >2=%.1f%% <0.5=%.1f%% | Rand: mean=%.2f >2=%.1f%% <0.5=%.1f%%",
                etas_eq_stats.get("mean", 0), etas_eq_stats.get("gt_2_pct", 0), etas_eq_stats.get("lt_05_pct", 0),
                etas_rand_stats.get("mean", 0), etas_rand_stats.get("gt_2_pct", 0), etas_rand_stats.get("lt_05_pct", 0))

    # ---------------------------------------------------------------
    # Magnitude dependence
    # ---------------------------------------------------------------
    mag_bins_7d = {}
    for r in rate_results_7d:
        label = f"M{int(r['mag'])}"
        mag_bins_7d.setdefault(label, []).append(r["ratio"])

    results = {
        "catalog_stats": {
            "n_m3_plus": len(events),
            "n_target": len(target_events),
            "duration_days": round(T_total, 1),
            "global_rate_per_day": round(global_rate, 3),
        },
        "model_free_7d": {
            "description": "Regional rate ratio: observed M3+ in 7d (2deg box) / long-term regional rate * 7d",
            "earthquake_locations": eq_7d_stats,
            "random_locations": rand_7d_stats,
            "activation_lift_gt2": round(act_7d_lift, 2),
            "quiescence_lift_lt05": round(qui_7d_lift, 2),
        },
        "model_free_30d": {
            "earthquake_locations": eq_30d_stats,
            "random_locations": rand_30d_stats,
        },
        "etas_constrained": {
            "params": params,
            "earthquake_locations": etas_eq_stats,
            "random_locations": etas_rand_stats,
        },
        "by_magnitude": {k: ratio_stats(v) for k, v in sorted(mag_bins_7d.items())},
        "sample_events": rate_results_7d[:20],
    }

    return results


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-mag", type=float, default=5.0)
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_etas_analysis(args.min_mag)

    out_path = RESULTS_DIR / f"etas_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
