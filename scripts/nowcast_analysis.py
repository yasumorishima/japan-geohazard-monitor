"""Earthquake Nowcasting analysis.

Estimates the "Earthquake Potential Score" (EPS) — the probability
that a region is due for a large earthquake based on the count of
small earthquakes since the last large event.

Method:
  1. For each spatial cell, count M3+ events since last M5+
  2. Compare with historical distribution of such counts
  3. EPS = percentile rank of current count (0-100%)
  4. Test: are M5+ events preceded by high EPS?

Reference: Rundle et al. (2016) Earth and Space Science
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
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"


async def run_nowcast_analysis(min_mag_target: float = 5.0):
    logger.info("=== Earthquake Nowcasting (target M%.1f+) ===", min_mag_target)

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
            events.append({"time": t, "mag": r[1], "lat": r[2], "lon": r[3],
                           "depth": r[4] if r[4] else 10.0})
        except (ValueError, TypeError):
            continue

    t0 = events[0]["time"]
    for e in events:
        e["t_days"] = (e["time"] - t0).total_seconds() / 86400
    all_times = [e["t_days"] for e in events]
    T_total = all_times[-1]

    targets = [e for e in events if e["mag"] >= min_mag_target]
    logger.info("  Catalog: %d M3+, %d M%.1f+, %.0f days",
                len(events), len(targets), min_mag_target, T_total)

    # ---------------------------------------------------------------
    # Build inter-event M3+ counts for each spatial cell
    # ---------------------------------------------------------------
    # Divide Japan into 2°×2° grid cells
    CELL_SIZE = 2.0
    LAT_RANGE = (20, 50)
    LON_RANGE = (120, 155)

    def cell_key(lat, lon):
        return (int((lat - LAT_RANGE[0]) / CELL_SIZE), int((lon - LON_RANGE[0]) / CELL_SIZE))

    # For each cell, record sequence of M3+ counts between M5+ events
    cell_events = {}
    for e in events:
        ck = cell_key(e["lat"], e["lon"])
        cell_events.setdefault(ck, []).append(e)

    # Historical cycle lengths (M3+ count between consecutive M5+)
    all_cycle_lengths = []
    cell_cycles = {}
    for ck, evts in cell_events.items():
        count_since_last = 0
        cycles = []
        for e in evts:
            if e["mag"] >= min_mag_target:
                if count_since_last > 0:
                    cycles.append(count_since_last)
                    all_cycle_lengths.append(count_since_last)
                count_since_last = 0
            else:
                count_since_last += 1
        cell_cycles[ck] = cycles

    if not all_cycle_lengths:
        return {"error": "No complete cycles found"}

    all_cycle_lengths.sort()
    n_cycles = len(all_cycle_lengths)
    logger.info("  Found %d complete M3+ cycles across %d cells", n_cycles, len(cell_cycles))
    logger.info("  Cycle lengths: mean=%.0f, median=%.0f, p90=%.0f",
                sum(all_cycle_lengths) / n_cycles,
                all_cycle_lengths[n_cycles // 2],
                all_cycle_lengths[int(n_cycles * 0.9)])

    # ---------------------------------------------------------------
    # Compute EPS at each target event time
    # ---------------------------------------------------------------
    def compute_eps(lat, lon, t_days):
        """Earthquake Potential Score: percentile of current M3+ count
        since last M5+ in this cell, relative to historical distribution."""
        ck = cell_key(lat, lon)
        evts = cell_events.get(ck, [])
        if not evts:
            return None

        # Count M3+ since last M5+ before t_days
        count = 0
        for e in reversed(evts):
            if e["t_days"] >= t_days:
                continue
            if e["mag"] >= min_mag_target:
                break
            count += 1

        if count == 0:
            return None

        # Use cell-specific cycles if available, else global
        cycles = cell_cycles.get(ck, [])
        ref = cycles if len(cycles) >= 5 else all_cycle_lengths

        # EPS = percentile rank
        rank = bisect.bisect_left(sorted(ref), count)
        eps = 100.0 * rank / len(ref)
        return min(eps, 100.0)

    eps_eq = []
    for te in targets:
        if te["t_days"] < 30:
            continue
        eps = compute_eps(te["lat"], te["lon"], te["t_days"])
        if eps is not None:
            eps_eq.append({
                "time": te["time"].isoformat()[:16],
                "mag": te["mag"],
                "eps": round(eps, 1),
            })

    random.seed(42)
    eps_rand = []
    for _ in range(500):
        rt = 30 + random.random() * (T_total - 60)
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25
        eps = compute_eps(rlat, rlon, rt)
        if eps is not None:
            eps_rand.append(round(eps, 1))

    def eps_stats(values):
        if not values:
            return {"n": 0}
        v = [x["eps"] if isinstance(x, dict) else x for x in values]
        s = sorted(v)
        n = len(s)
        return {
            "n": n,
            "mean": round(sum(s) / n, 1),
            "median": round(s[n // 2], 1),
            "p10": round(s[int(n * 0.1)], 1),
            "p90": round(s[int(n * 0.9)], 1),
            "gt_50_pct": round(sum(1 for x in s if x > 50) / n * 100, 1),
            "gt_70_pct": round(sum(1 for x in s if x > 70) / n * 100, 1),
            "gt_90_pct": round(sum(1 for x in s if x > 90) / n * 100, 1),
        }

    eq_stats = eps_stats(eps_eq)
    rand_stats = eps_stats(eps_rand)

    lift_gt70 = eq_stats["gt_70_pct"] / max(rand_stats["gt_70_pct"], 0.1)

    # Magnitude dependence
    mag_bins = {}
    for e in eps_eq:
        label = f"M{int(e['mag'])}"
        mag_bins.setdefault(label, []).append(e["eps"])

    results = {
        "cycle_stats": {
            "n_cycles": n_cycles,
            "mean_length": round(sum(all_cycle_lengths) / n_cycles, 1),
            "median_length": all_cycle_lengths[n_cycles // 2],
            "p90_length": all_cycle_lengths[int(n_cycles * 0.9)],
        },
        "eps_distribution": {
            "earthquake": eq_stats,
            "random": rand_stats,
            "lift_gt_70": round(lift_gt70, 2),
        },
        "by_magnitude": {k: eps_stats(v) for k, v in sorted(mag_bins.items())},
        "sample_events": eps_eq[:20],
    }

    logger.info("  EPS — EQ: mean=%.1f >70=%.1f%% | Rand: mean=%.1f >70=%.1f%% | Lift=%.2f",
                eq_stats["mean"], eq_stats["gt_70_pct"],
                rand_stats["mean"], rand_stats["gt_70_pct"], lift_gt70)

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = await run_nowcast_analysis()
    out_path = RESULTS_DIR / f"nowcast_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
