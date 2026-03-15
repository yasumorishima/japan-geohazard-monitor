"""ETAS (Epidemic Type Aftershock Sequence) analysis.

Fits an ETAS model to the earthquake catalog and tests whether large
earthquakes preferentially occur during periods of elevated seismicity
rate relative to the ETAS expectation (i.e., anomalous quiescence or
activation that the ETAS model doesn't explain).

Model: λ(t,x,y) = μ(x,y) + Σ K * exp(α(Mi - Mc)) / (t - ti + c)^p
  - μ: background rate
  - K, α, c, p: aftershock productivity parameters
  - Sum over all prior events i with ti < t

Reference: Ogata (1988, 1998), Zhuang et al. (2002)
"""

import argparse
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

# ETAS default parameters (Japan, Ogata 1998 estimates)
DEFAULT_PARAMS = {
    "mu": 0.5,      # background rate (events/day for M5+)
    "K": 0.04,      # aftershock productivity
    "alpha": 1.2,   # magnitude scaling
    "c": 0.01,      # time offset (days)
    "p": 1.1,       # temporal decay exponent (Omori)
    "Mc": 3.0,      # completeness magnitude
}

DEG_TO_KM = 111.32


def etas_rate(t_days: float, events_before: list, params: dict) -> float:
    """Compute ETAS conditional intensity at time t.

    events_before: list of (time_days, magnitude) for events before t.
    Returns: expected rate (events/day).
    """
    mu = params["mu"]
    K = params["K"]
    alpha = params["alpha"]
    c = params["c"]
    p = params["p"]
    Mc = params["Mc"]

    rate = mu
    for ti, mi in events_before:
        dt = t_days - ti
        if dt <= 0:
            continue
        rate += K * math.exp(alpha * (mi - Mc)) / (dt + c) ** p

    return rate


def fit_etas_mle(events: list, params0: dict, max_iter: int = 50) -> dict:
    """Fit ETAS parameters by approximate maximum likelihood.

    Uses grid search over key parameters (K, alpha, c, p) with
    fixed mu estimated from long-term average.

    events: list of (time_days, magnitude) sorted by time.
    """
    if len(events) < 50:
        return params0

    # Estimate mu from data: events in first/last quarters (less aftershock contamination)
    T = events[-1][0] - events[0][0]
    if T < 30:
        return params0

    # Count events in quiet periods (background estimate)
    quarter = T / 4
    n_q1 = sum(1 for t, m in events if t < events[0][0] + quarter)
    n_q4 = sum(1 for t, m in events if t > events[-1][0] - quarter)
    mu_est = (n_q1 + n_q4) / (2 * quarter)

    best_params = dict(params0)
    best_params["mu"] = max(mu_est, 0.01)
    best_ll = -1e18

    # Grid search over (K, alpha, p) — c is less sensitive
    K_grid = [0.01, 0.02, 0.04, 0.08, 0.15]
    alpha_grid = [0.8, 1.0, 1.2, 1.5, 2.0]
    p_grid = [0.9, 1.0, 1.1, 1.2, 1.3]

    for K in K_grid:
        for alpha in alpha_grid:
            for p in p_grid:
                params = {
                    "mu": best_params["mu"],
                    "K": K, "alpha": alpha,
                    "c": 0.01, "p": p,
                    "Mc": params0["Mc"],
                }
                # Log-likelihood (truncated for speed)
                ll = 0.0
                sample_step = max(1, len(events) // 200)
                for idx in range(10, len(events), sample_step):
                    ti, mi = events[idx]
                    rate = etas_rate(ti, events[:idx], params)
                    if rate > 0:
                        ll += math.log(rate)
                    else:
                        ll -= 100  # penalty

                if ll > best_ll:
                    best_ll = ll
                    best_params = dict(params)

    logger.info("  ETAS fit: mu=%.3f K=%.3f α=%.1f c=%.3f p=%.1f (LL=%.1f)",
                best_params["mu"], best_params["K"], best_params["alpha"],
                best_params["c"], best_params["p"], best_ll)
    return best_params


async def run_etas_analysis(min_mag_target: float = 5.0) -> dict:
    """Run ETAS analysis on earthquake catalog."""
    logger.info("=== ETAS Analysis (target M%.1f+) ===", min_mag_target)

    async with aiosqlite.connect(DB_PATH) as db:
        # Load all M3+ events for ETAS fitting
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
    # Convert to days since first event
    for e in events:
        e["t_days"] = (e["time"] - t0).total_seconds() / 86400

    events_td = [(e["t_days"], e["mag"]) for e in events]
    T_total = events_td[-1][0] - events_td[0][0]

    logger.info("  Catalog: %d M3+ events over %.0f days", len(events), T_total)

    # Fit ETAS parameters
    params = fit_etas_mle(events_td, DEFAULT_PARAMS)

    # ---------------------------------------------------------------
    # Compute ETAS rate and residuals at each M5+ event
    # ---------------------------------------------------------------
    target_events = [e for e in events if e["mag"] >= min_mag_target]
    logger.info("  Target events (M%.1f+): %d", min_mag_target, len(target_events))

    # For each target event, compute:
    # 1. ETAS expected count in preceding 7 days (integral of rate)
    # 2. Observed count in preceding 7 days
    # 3. Rate ratio = observed / expected

    def etas_expected_count(t_target, idx_in_catalog, window_days=7):
        """Approximate expected M3+ count in [t-window, t] by sampling ETAS rate."""
        n_samples = 14
        dt = window_days / n_samples
        total = 0.0
        for k in range(n_samples):
            t_sample = t_target - window_days + (k + 0.5) * dt
            # Only use events before the sample time (up to 90 days back for efficiency)
            t_cutoff = t_sample - 90
            recent = [(t, m) for t, m in events_td[:idx_in_catalog] if t > t_cutoff]
            rate = etas_rate(t_sample, recent, params)
            total += rate * dt
        return total

    rate_ratios = []
    for i, te in enumerate(target_events):
        idx_in_catalog = next(
            (j for j, e in enumerate(events) if e["time"] == te["time"]),
            None
        )
        if idx_in_catalog is None or idx_in_catalog < 50:
            continue

        # ETAS expected count in preceding 7 days
        expected = etas_expected_count(te["t_days"], idx_in_catalog)

        # Observed count: M3+ events in preceding 7 days
        t_start = te["t_days"] - 7
        observed = sum(1 for t, m in events_td if t_start <= t < te["t_days"])

        # Rate ratio (observed / expected counts)
        ratio = observed / max(expected, 0.1)

        rate_ratios.append({
            "time": te["time"].isoformat()[:16],
            "mag": te["mag"],
            "lat": te["lat"],
            "lon": te["lon"],
            "etas_expected_7d": round(expected, 1),
            "observed_7d": observed,
            "rate_ratio": round(ratio, 3),
            "log_ratio": round(math.log10(max(ratio, 0.001)), 3),
        })

        if (i + 1) % 500 == 0:
            logger.info("    Processed %d/%d target events", i + 1, len(target_events))

    if not rate_ratios:
        return {"error": "No rate ratios computed"}

    # ---------------------------------------------------------------
    # Random baseline: compute rate ratio at random times
    # ---------------------------------------------------------------
    random.seed(42)
    random_ratios = []
    for ri in range(500):
        t_rand = events_td[0][0] + 100 + random.random() * (T_total - 200)
        idx = next((j for j, (t, m) in enumerate(events_td) if t >= t_rand), len(events_td) - 1)
        if idx < 50:
            continue

        expected = etas_expected_count(t_rand, idx)
        t_start = t_rand - 7
        observed = sum(1 for t, m in events_td if t_start <= t < t_rand)
        ratio = observed / max(expected, 0.1)
        random_ratios.append(round(ratio, 3))

    # ---------------------------------------------------------------
    # Statistical comparison
    # ---------------------------------------------------------------
    eq_ratios = [r["rate_ratio"] for r in rate_ratios]

    def ratio_stats(values, label):
        if not values:
            return {"n": 0}
        s = sorted(values)
        n = len(s)
        mean = sum(s) / n
        return {
            "n": n,
            "mean": round(mean, 3),
            "median": round(s[n // 2], 3),
            "p10": round(s[int(n * 0.1)], 3),
            "p25": round(s[int(n * 0.25)], 3),
            "p75": round(s[int(n * 0.75)], 3),
            "p90": round(s[int(n * 0.9)], 3),
            "gt_1_pct": round(sum(1 for v in s if v > 1.0) / n * 100, 1),
            "gt_2_pct": round(sum(1 for v in s if v > 2.0) / n * 100, 1),
            "lt_05_pct": round(sum(1 for v in s if v < 0.5) / n * 100, 1),
        }

    eq_stats = ratio_stats(eq_ratios, "earthquake")
    rand_stats = ratio_stats(random_ratios, "random")

    # Quiescence test: do large earthquakes follow periods of unusual quiet?
    quiescence_eq = sum(1 for r in eq_ratios if r < 0.5) / len(eq_ratios) * 100
    quiescence_rand = sum(1 for r in random_ratios if r < 0.5) / len(random_ratios) * 100 if random_ratios else 0

    # Activation test: do large earthquakes follow periods of unusual activity?
    activation_eq = sum(1 for r in eq_ratios if r > 2.0) / len(eq_ratios) * 100
    activation_rand = sum(1 for r in random_ratios if r > 2.0) / len(random_ratios) * 100 if random_ratios else 0

    # Magnitude dependence
    mag_bins = {}
    for r in rate_ratios:
        bin_label = f"M{int(r['mag'])}"
        mag_bins.setdefault(bin_label, []).append(r["rate_ratio"])

    results = {
        "etas_params": params,
        "catalog_stats": {
            "n_m3_plus": len(events),
            "n_target": len(target_events),
            "duration_days": round(T_total, 1),
            "background_rate_per_day": round(params["mu"], 3),
        },
        "rate_ratio_earthquakes": eq_stats,
        "rate_ratio_random": rand_stats,
        "quiescence_test": {
            "description": "Fraction of events preceded by unusually low rate (ratio < 0.5)",
            "earthquake_pct": round(quiescence_eq, 1),
            "random_pct": round(quiescence_rand, 1),
            "lift": round(quiescence_eq / max(quiescence_rand, 0.1), 2),
        },
        "activation_test": {
            "description": "Fraction of events preceded by unusually high rate (ratio > 2.0)",
            "earthquake_pct": round(activation_eq, 1),
            "random_pct": round(activation_rand, 1),
            "lift": round(activation_eq / max(activation_rand, 0.1), 2),
        },
        "by_magnitude": {k: ratio_stats(v, k) for k, v in sorted(mag_bins.items())},
        "sample_events": rate_ratios[:30],
    }

    logger.info("  Rate ratio — EQ: mean=%.2f, >1=%.0f%%, <0.5=%.0f%% | Rand: mean=%.2f, >1=%.0f%%, <0.5=%.0f%%",
                eq_stats["mean"], eq_stats["gt_1_pct"], eq_stats["lt_05_pct"],
                rand_stats["mean"], rand_stats["gt_1_pct"], rand_stats["lt_05_pct"])
    logger.info("  Quiescence lift: %.2f | Activation lift: %.2f",
                results["quiescence_test"]["lift"], results["activation_test"]["lift"])

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
