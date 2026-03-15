"""Natural Time Analysis for seismic criticality detection.

Natural time χ reindexes events by order number rather than clock time.
The variance κ1 = <χ²> - <χ>² approaches 0.070 when the system reaches
criticality (analogous to phase transitions in statistical mechanics).

Method:
  1. For each spatial region, define a sliding window of N recent M3+ events
  2. Compute κ1 = variance of natural time in the window
  3. Track κ1 evolution and check proximity to critical value 0.070
  4. Compare κ1 distribution before M5+ events vs random times

Reference: Varotsos et al. (2011) Natural Time Analysis: The New View of Time
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

KAPPA_CRITICAL = 0.070  # Critical value for seismicity (Varotsos et al.)


def compute_kappa1(magnitudes: list[float]) -> float | None:
    """Compute natural time variance κ1 for an event sequence.

    χ_k = k/N for event k in sequence of N events
    Q_k = 10^(1.5*M_k) (energy proxy)
    <χ> = Σ(χ_k * Q_k) / Σ(Q_k)
    <χ²> = Σ(χ_k² * Q_k) / Σ(Q_k)
    κ1 = <χ²> - <χ>²
    """
    N = len(magnitudes)
    if N < 6:
        return None

    # Energy weights
    Q = [10 ** (1.5 * m) for m in magnitudes]
    Q_total = sum(Q)
    if Q_total < 1e-10:
        return None

    # Natural time
    chi = [(k + 1) / N for k in range(N)]

    # Weighted moments
    chi_mean = sum(chi[k] * Q[k] for k in range(N)) / Q_total
    chi2_mean = sum(chi[k] ** 2 * Q[k] for k in range(N)) / Q_total

    kappa1 = chi2_mean - chi_mean ** 2
    return kappa1


async def run_natural_time_analysis(min_mag_target: float = 5.0):
    logger.info("=== Natural Time Analysis (target M%.1f+) ===", min_mag_target)

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

    targets = [e for e in events if e["mag"] >= min_mag_target]
    logger.info("  Catalog: %d M3+ events, %d M%.1f+ targets", len(events), len(targets), min_mag_target)

    # ---------------------------------------------------------------
    # Compute κ1 before each M5+ event using regional window
    # ---------------------------------------------------------------
    WINDOW_SIZES = [40, 70, 100, 200]  # Number of events in window

    def kappa_at(center_lat, center_lon, t_end_days, window_n=100, radius_deg=2.0):
        """Compute κ1 from the last window_n events in a spatial box before t_end."""
        idx_end = bisect.bisect_left(all_times, t_end_days)
        regional = []
        for i in range(idx_end - 1, max(idx_end - 10000, -1), -1):
            e = events[i]
            if abs(e["lat"] - center_lat) <= radius_deg and abs(e["lon"] - center_lon) <= radius_deg:
                regional.append(e["mag"])
            if len(regional) >= window_n:
                break

        regional.reverse()  # Chronological order
        return compute_kappa1(regional)

    results_by_window = {}
    for wn in WINDOW_SIZES:
        logger.info("  Window N=%d...", wn)

        kappa_eq = []
        for te in targets:
            if te["t_days"] < 30:
                continue
            k1 = kappa_at(te["lat"], te["lon"], te["t_days"], window_n=wn)
            if k1 is not None:
                kappa_eq.append({
                    "time": te["time"].isoformat()[:16],
                    "mag": te["mag"],
                    "kappa1": round(k1, 5),
                    "near_critical": abs(k1 - KAPPA_CRITICAL) < 0.005,
                })

        random.seed(42 + wn)
        kappa_rand = []
        for _ in range(500):
            rt = 30 + random.random() * (T_total - 60)
            rlat = 25 + random.random() * 20
            rlon = 125 + random.random() * 25
            k1 = kappa_at(rlat, rlon, rt, window_n=wn)
            if k1 is not None:
                kappa_rand.append(round(k1, 5))

        def kappa_stats(values):
            if not values:
                return {"n": 0}
            v = [x["kappa1"] if isinstance(x, dict) else x for x in values]
            s = sorted(v)
            n = len(s)
            near_crit = sum(1 for x in v if abs(x - KAPPA_CRITICAL) < 0.005)
            return {
                "n": n,
                "mean": round(sum(s) / n, 5),
                "median": round(s[n // 2], 5),
                "p10": round(s[int(n * 0.1)], 5),
                "p90": round(s[int(n * 0.9)], 5),
                "near_critical_pct": round(100 * near_crit / n, 1),
                "lt_0070_pct": round(sum(1 for x in v if x < KAPPA_CRITICAL) / n * 100, 1),
            }

        eq_stats = kappa_stats(kappa_eq)
        rand_stats = kappa_stats(kappa_rand)

        lift_near_crit = eq_stats["near_critical_pct"] / max(rand_stats["near_critical_pct"], 0.1)

        results_by_window[f"N{wn}"] = {
            "earthquake": eq_stats,
            "random": rand_stats,
            "lift_near_critical": round(lift_near_crit, 2),
        }

        logger.info("    N=%d: EQ κ1=%.5f near_crit=%.1f%% | Rand κ1=%.5f near_crit=%.1f%% | lift=%.2f",
                     wn, eq_stats["mean"], eq_stats["near_critical_pct"],
                     rand_stats["mean"], rand_stats["near_critical_pct"], lift_near_crit)

    results = {
        "kappa_critical": KAPPA_CRITICAL,
        "by_window_size": results_by_window,
        "sample_events": kappa_eq[:20] if kappa_eq else [],
    }

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = await run_natural_time_analysis()
    out_path = RESULTS_DIR / f"natural_time_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
