"""CSEP benchmark: compare ML forecast against reference models.

Implements 4 reference models and CSEP-standard statistical tests:
    1. Uniform Poisson — spatially uniform rate
    2. Smoothed Seismicity (Helmstetter et al. 2007) — kernel-smoothed past rates
    3. Relative Intensity (RI, Rhoades & Evison 2004) — rate proportional to past
    4. Simple ETAS — parametric aftershock model

Tests:
    - N-test: observed count vs predicted (Poisson consistency)
    - L-test: log-likelihood comparison
    - T-test: paired log-likelihood ratio (Rhoades et al. 2011)
    - Molchan diagram: miss rate vs alarm fraction

References:
    - Schorlemmer et al. (2007) "Earthquake likelihood model testing"
    - Helmstetter et al. (2007) "High-resolution time-independent forecast"
    - Rhoades & Evison (2004) "Long-range earthquake forecasting"
    - Werner & Sornette (2008) "Magnitude uncertainties in CSEP"
"""

import json
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

# Grid (matches features.py / csep_format.py)
GRID_LAT_MIN, GRID_LAT_MAX = 26.0, 46.0
GRID_LON_MIN, GRID_LON_MAX = 128.0, 148.0
CELL_SIZE_DEG = 2.0


# ---------------------------------------------------------------------------
# Reference models
# ---------------------------------------------------------------------------

def uniform_poisson_forecast(total_rate, n_cells):
    """Uniform Poisson: same rate in every cell.

    Args:
        total_rate: total expected M5+ events per forecast window
        n_cells: number of spatial cells

    Returns:
        dict: {(lat, lon): rate_per_cell}
    """
    rate_per_cell = total_rate / max(n_cells, 1)
    forecast = {}
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        lon = GRID_LON_MIN
        while lon <= GRID_LON_MAX:
            forecast[(lat, lon)] = rate_per_cell
            lon += CELL_SIZE_DEG
        lat += CELL_SIZE_DEG
    return forecast


def smoothed_seismicity_forecast(events, test_start_days, window_days,
                                  min_mag=5.0, sigma_deg=2.0):
    """Helmstetter et al. (2007) smoothed seismicity.

    Kernel-smoothed past seismicity rate, using only training period data.

    Args:
        events: list of dicts with t_days, lat, lon, mag
        test_start_days: cutoff (only use events before this)
        window_days: forecast window
        min_mag: minimum magnitude for counting
        sigma_deg: Gaussian smoothing kernel width

    Returns:
        dict: {(lat, lon): expected_rate}
    """
    # Count past events per cell (training period only)
    train_events = [e for e in events if e["t_days"] < test_start_days and e["mag"] >= min_mag]
    total_days = test_start_days

    cell_counts = {}
    for e in train_events:
        clat = round(e["lat"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
        clon = round(e["lon"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
        ck = (clat, clon)
        cell_counts[ck] = cell_counts.get(ck, 0) + 1

    # Gaussian smoothing
    forecast = {}
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        lon = GRID_LON_MIN
        while lon <= GRID_LON_MAX:
            weighted_sum = 0.0
            total_weight = 0.0
            for (clat, clon), count in cell_counts.items():
                dlat = lat - clat
                dlon = lon - clon
                dist_sq = dlat ** 2 + dlon ** 2
                w = math.exp(-0.5 * dist_sq / sigma_deg ** 2)
                weighted_sum += w * count
                total_weight += w
            if total_weight > 0:
                smoothed_rate = weighted_sum / total_weight
                # Scale to forecast window
                forecast[(lat, lon)] = smoothed_rate / max(total_days, 1) * window_days
            else:
                forecast[(lat, lon)] = 0.0
            lon += CELL_SIZE_DEG
        lat += CELL_SIZE_DEG

    return forecast


def relative_intensity_forecast(events, test_start_days, window_days,
                                 min_mag_count=3.0, min_mag_target=5.0):
    """Relative Intensity (RI) forecast.

    Rate of M5+ proportional to historical M3+ rate in each cell.

    Args:
        events: list of dicts with t_days, lat, lon, mag
        test_start_days: training period cutoff
        window_days: forecast window
    """
    train = [e for e in events if e["t_days"] < test_start_days]
    train_target = [e for e in train if e["mag"] >= min_mag_target]
    total_days = test_start_days

    # M3+ count per cell
    cell_m3 = {}
    for e in train:
        if e["mag"] >= min_mag_count:
            clat = round(e["lat"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
            clon = round(e["lon"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
            ck = (clat, clon)
            cell_m3[ck] = cell_m3.get(ck, 0) + 1

    total_m3 = sum(cell_m3.values()) or 1
    total_m5_rate = len(train_target) / max(total_days, 1) * window_days

    forecast = {}
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        lon = GRID_LON_MIN
        while lon <= GRID_LON_MAX:
            ck = (lat, lon)
            frac = cell_m3.get(ck, 0) / total_m3
            forecast[ck] = frac * total_m5_rate
            lon += CELL_SIZE_DEG
        lat += CELL_SIZE_DEG

    return forecast


def simple_etas_forecast(events, test_start_days, window_days,
                          min_mag_target=5.0):
    """Simple ETAS-based forecast.

    Background rate + aftershock contributions from recent M5+ events.
    Uses fixed ETAS parameters (Ogata 1998 Japan estimates).
    """
    K, alpha, c, p, mc = 0.04, 1.0, 0.01, 1.1, 3.0
    train = [e for e in events if e["t_days"] < test_start_days]
    train_target = [e for e in train if e["mag"] >= min_mag_target]
    total_days = test_start_days

    # Background rate per cell
    cell_m5 = {}
    for e in train_target:
        clat = round(e["lat"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
        clon = round(e["lon"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
        ck = (clat, clon)
        cell_m5[ck] = cell_m5.get(ck, 0) + 1

    # ETAS triggered rate from events near test_start
    recent_cutoff = test_start_days - 365  # last year
    recent_events = [e for e in train if e["mag"] >= 4.0 and e["t_days"] > recent_cutoff]

    forecast = {}
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        lon = GRID_LON_MIN
        while lon <= GRID_LON_MAX:
            ck = (lat, lon)
            # Background
            bg_rate = cell_m5.get(ck, 0) / max(total_days, 1) * window_days

            # Aftershock contribution
            etas_rate = 0.0
            for e in recent_events:
                dlat = abs(e["lat"] - lat)
                dlon = abs(e["lon"] - lon)
                if dlat > 3 or dlon > 3:
                    continue
                dt = test_start_days - e["t_days"]
                if dt <= 0:
                    continue
                productivity = K * math.exp(alpha * (e["mag"] - mc))
                # Integrated kernel over [dt, dt + window_days]
                if abs(p - 1.0) < 0.01:
                    integral = productivity * (
                        math.log(dt + window_days + c) - math.log(dt + c))
                else:
                    integral = productivity / (1 - p) * (
                        (dt + window_days + c) ** (1 - p) - (dt + c) ** (1 - p))
                # Spatial decay (isotropic Gaussian, sigma=1 deg)
                dist_sq = dlat ** 2 + dlon ** 2
                spatial = math.exp(-0.5 * dist_sq)
                etas_rate += max(integral, 0) * spatial

            # Scale triggered rate: fraction that are M5+
            # Roughly: P(M>=5 | triggered) = 10^(-b*(5-mc)) ≈ 0.01
            etas_m5_rate = etas_rate * 0.01

            forecast[ck] = bg_rate + etas_m5_rate
            lon += CELL_SIZE_DEG
        lat += CELL_SIZE_DEG

    return forecast


# ---------------------------------------------------------------------------
# CSEP statistical tests
# ---------------------------------------------------------------------------

def n_test(forecast_rates, observed_counts, confidence=0.95):
    """N-test: is total observed count consistent with forecast?

    Tests if the total observed count falls within the Poisson
    confidence interval of the total forecast rate.

    Returns:
        dict with total_forecast, total_observed, p_value, pass
    """
    total_forecast = sum(forecast_rates.values())
    total_observed = sum(observed_counts.values())

    # Poisson CDF: P(X <= k | lambda)
    if total_forecast <= 0:
        return {
            "total_forecast": 0, "total_observed": total_observed,
            "p_value": 0.0, "pass": False,
            "ci_lower": 0, "ci_upper": 0,
        }

    # Compute P(X <= observed) under Poisson(total_forecast)
    p_value = _poisson_cdf(int(total_observed), total_forecast)

    # Two-sided: reject if p < alpha/2 or p > 1-alpha/2
    alpha = 1 - confidence
    passed = alpha / 2 <= p_value <= 1 - alpha / 2

    # 95% CI bounds
    ci_lower = _poisson_quantile(alpha / 2, total_forecast)
    ci_upper = _poisson_quantile(1 - alpha / 2, total_forecast)

    return {
        "total_forecast": round(total_forecast, 4),
        "total_observed": total_observed,
        "p_value": round(p_value, 4),
        "pass": passed,
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
    }


def l_test(forecast_rates, observed_counts):
    """L-test: joint log-likelihood of the forecast.

    log L = sum_i [n_i * log(lambda_i) - lambda_i - log(n_i!)]
    """
    log_l = 0.0
    n_cells = 0
    for ck in forecast_rates:
        rate = max(forecast_rates[ck], 1e-10)
        obs = observed_counts.get(ck, 0)
        log_l += obs * math.log(rate) - rate - _log_factorial(obs)
        n_cells += 1

    return {
        "log_likelihood": round(log_l, 4),
        "n_cells": n_cells,
        "mean_log_l_per_cell": round(log_l / max(n_cells, 1), 4),
    }


def t_test(forecast_a_rates, forecast_b_rates, observed_counts):
    """T-test: paired log-likelihood ratio between two forecasts.

    Tests whether model A is significantly better than model B.
    H0: both models have equal predictive power.

    Returns:
        dict with t_statistic, p_value, better_model
    """
    diffs = []
    for ck in forecast_a_rates:
        rate_a = max(forecast_a_rates[ck], 1e-10)
        rate_b = max(forecast_b_rates.get(ck, 1e-10), 1e-10)
        obs = observed_counts.get(ck, 0)

        ll_a = obs * math.log(rate_a) - rate_a
        ll_b = obs * math.log(rate_b) - rate_b
        diffs.append(ll_a - ll_b)

    if not diffs:
        return {"t_statistic": 0, "p_value": 1.0, "better_model": "neither"}

    n = len(diffs)
    mean_diff = sum(diffs) / n
    var_diff = sum((d - mean_diff) ** 2 for d in diffs) / max(n - 1, 1)
    se = math.sqrt(var_diff / n) if var_diff > 0 else 1e-10

    t_stat = mean_diff / se

    # Approximate p-value using normal distribution (n >> 30)
    p_value = 2 * (1 - _normal_cdf(abs(t_stat)))

    better = "A" if mean_diff > 0 else "B" if mean_diff < 0 else "neither"

    return {
        "t_statistic": round(t_stat, 4),
        "p_value": round(p_value, 6),
        "mean_ll_diff": round(mean_diff, 6),
        "n_cells": n,
        "better_model": better,
        "significant_at_005": p_value < 0.05,
    }


def molchan_test(forecast_rates, observed_counts, n_points=100):
    """Molchan diagram: alarm_fraction vs miss_rate.

    Cells ranked by forecast rate (descending). Sweep threshold
    to build the Molchan curve.

    Returns:
        dict with area_skill_score, diagram_points
    """
    total_obs = sum(observed_counts.values())
    if total_obs == 0:
        return {"area_skill_score": 0.0, "diagram_points": []}

    # Rank cells by forecast rate (descending)
    ranked = sorted(forecast_rates.items(), key=lambda x: -x[1])
    n_cells = len(ranked)

    points = [(0.0, 1.0)]  # (alarm_fraction=0, miss_rate=1)
    cumulative_obs = 0
    for i, (ck, rate) in enumerate(ranked):
        cumulative_obs += observed_counts.get(ck, 0)
        alarm_frac = (i + 1) / n_cells
        miss_rate = 1 - cumulative_obs / total_obs
        if i % max(1, n_cells // n_points) == 0 or i == n_cells - 1:
            points.append((round(alarm_frac, 4), round(miss_rate, 4)))
    points.append((1.0, 0.0))

    # Remove duplicates and sort
    points = sorted(set(points))

    # Area under Molchan curve
    area = 0.0
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        avg_y = (points[i][1] + points[i - 1][1]) / 2
        area += dx * avg_y

    skill = round(1 - 2 * area, 4)

    return {
        "area_skill_score": skill,
        "area_under_curve": round(area, 4),
        "diagram_points": points[:n_points],
    }


# ---------------------------------------------------------------------------
# Math utilities
# ---------------------------------------------------------------------------

def _poisson_cdf(k, lam):
    """P(X <= k) for Poisson(lam)."""
    if lam <= 0:
        return 1.0 if k >= 0 else 0.0
    total = 0.0
    log_pmf = -lam
    for i in range(k + 1):
        if i > 0:
            log_pmf += math.log(lam) - math.log(i)
        total += math.exp(log_pmf)
        if total > 1.0:
            return 1.0
    return min(total, 1.0)


def _poisson_quantile(p, lam):
    """Inverse Poisson CDF (smallest k such that P(X<=k) >= p)."""
    if lam <= 0:
        return 0
    k = 0
    while _poisson_cdf(k, lam) < p and k < lam * 10 + 100:
        k += 1
    return k


def _log_factorial(n):
    """log(n!) using Stirling's approximation for large n."""
    if n <= 1:
        return 0.0
    if n <= 20:
        return math.lgamma(n + 1)
    return math.lgamma(n + 1)


def _normal_cdf(x):
    """Standard normal CDF using error function."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def run_benchmark(events, test_start_days, window_days=7, ml_forecast=None):
    """Run full CSEP benchmark comparing ML vs reference models.

    Args:
        events: all events with t_days, lat, lon, mag
        test_start_days: training/test split in days from t0
        window_days: forecast window
        ml_forecast: dict {(lat,lon): rate} from ML model (optional)

    Returns:
        dict with all test results
    """
    logger.info("=== CSEP Benchmark ===")

    # Observed events in test period (first forecast window)
    test_events = [e for e in events
                   if test_start_days <= e["t_days"] < test_start_days + window_days
                   and e["mag"] >= 5.0]

    observed = {}
    for e in test_events:
        clat = round(e["lat"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
        clon = round(e["lon"] / CELL_SIZE_DEG) * CELL_SIZE_DEG
        ck = (clat, clon)
        observed[ck] = observed.get(ck, 0) + 1

    logger.info("  Test window: days [%.0f, %.0f), %d M5+ events",
                test_start_days, test_start_days + window_days, len(test_events))

    # Count cells
    n_cells = 0
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        lon = GRID_LON_MIN
        while lon <= GRID_LON_MAX:
            n_cells += 1
            lon += CELL_SIZE_DEG
        lat += CELL_SIZE_DEG

    # Train-period M5+ rate
    train_target = [e for e in events
                    if e["t_days"] < test_start_days and e["mag"] >= 5.0]
    total_rate = len(train_target) / max(test_start_days, 1) * window_days

    # Generate reference forecasts
    logger.info("  Generating reference forecasts...")
    ref_uniform = uniform_poisson_forecast(total_rate, n_cells)
    ref_smooth = smoothed_seismicity_forecast(events, test_start_days, window_days)
    ref_ri = relative_intensity_forecast(events, test_start_days, window_days)
    ref_etas = simple_etas_forecast(events, test_start_days, window_days)

    models = {
        "Uniform_Poisson": ref_uniform,
        "Smoothed_Seismicity": ref_smooth,
        "Relative_Intensity": ref_ri,
        "Simple_ETAS": ref_etas,
    }

    if ml_forecast:
        models["ML_HistGBT"] = ml_forecast

    results = {
        "test_period": {
            "start_days": test_start_days,
            "window_days": window_days,
            "n_observed_m5": len(test_events),
            "n_cells_with_events": len(observed),
        },
        "models": {},
    }

    # Run tests for each model
    for name, forecast in models.items():
        logger.info("  --- %s ---", name)

        n_result = n_test(forecast, observed)
        l_result = l_test(forecast, observed)
        m_result = molchan_test(forecast, observed)

        total_rate = sum(forecast.values())
        logger.info("    Total rate: %.4f, N-test: %s (p=%.4f), Molchan skill: %.4f",
                    total_rate, "PASS" if n_result["pass"] else "FAIL",
                    n_result["p_value"], m_result["area_skill_score"])

        results["models"][name] = {
            "total_forecast_rate": round(total_rate, 4),
            "n_test": n_result,
            "l_test": l_result,
            "molchan": m_result,
        }

    # Paired T-tests: ML vs each reference
    if ml_forecast:
        results["t_tests"] = {}
        for ref_name, ref_forecast in models.items():
            if ref_name == "ML_HistGBT":
                continue
            t_result = t_test(ml_forecast, ref_forecast, observed)
            results["t_tests"][f"ML_vs_{ref_name}"] = t_result
            logger.info("  T-test ML vs %s: t=%.4f p=%.6f (%s)",
                        ref_name, t_result["t_statistic"],
                        t_result["p_value"], t_result["better_model"])

    return results


def main():
    """Run benchmark using saved ML predictions and earthquake data."""
    import aiosqlite
    import asyncio

    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Load events
    async def load():
        from config import DB_PATH
        async with safe_connect() as db:
            rows = await db.execute_fetchall(
                "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
                "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
                "ORDER BY occurred_at"
            )
        events = []
        t0 = None
        for r in rows:
            try:
                t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
                if t0 is None:
                    t0 = t
                events.append({
                    "time": t,
                    "t_days": (t - t0).total_seconds() / 86400,
                    "mag": r[1], "lat": r[2], "lon": r[3],
                    "depth": r[4] if r[4] else 10.0,
                })
            except (ValueError, TypeError):
                continue
        return events, t0

    events, t0 = asyncio.run(load())
    logger.info("Loaded %d events", len(events))

    # Test split: 2019-01-01
    split_date = datetime(2019, 1, 1, tzinfo=timezone.utc)
    test_start_days = (split_date - t0).total_seconds() / 86400

    # Load ML level-0 predictions indexed by (cell, t_days)
    ml_preds_by_key = {}
    level0_file = RESULTS_DIR / "level0_predictions_M5plus.json"
    if level0_file.exists():
        try:
            with open(level0_file) as f:
                l0_data = json.load(f)

            for rec in l0_data.get("predictions", []):
                key = (rec["cell_lat"], rec["cell_lon"], rec["t_days"])
                ml_preds_by_key[key] = rec["prob"]
            logger.info("Loaded ML level-0 predictions: %d records", len(ml_preds_by_key))
        except Exception as e:
            logger.warning("Could not load ML predictions: %s", e)
    else:
        logger.warning("Level-0 file not found: %s", level0_file)

    from csep_format import csep_rate_from_probability

    # Run full benchmark over multiple test windows
    # Each window gets its own ML forecast based on predictions within that window
    window_days = 7
    all_results = []

    # Sample test windows (every 30 days in test period, up to 80 windows)
    test_end = events[-1]["t_days"]
    n_windows = 0
    t = test_start_days
    while t + window_days <= test_end and n_windows < 80:
        # Build ML forecast for this specific window
        # Average ML probabilities for predictions whose t_days falls in [t, t+window)
        ml_forecast = None
        if ml_preds_by_key:
            cell_probs = {}
            cell_counts = {}
            for (clat, clon, t_days), prob in ml_preds_by_key.items():
                if t <= t_days < t + window_days:
                    ck = (clat, clon)
                    cell_probs[ck] = cell_probs.get(ck, 0) + prob
                    cell_counts[ck] = cell_counts.get(ck, 0) + 1

            if cell_probs:
                ml_forecast = {}
                for ck, total_prob in cell_probs.items():
                    avg_prob = total_prob / cell_counts[ck]
                    rates = csep_rate_from_probability(avg_prob, window_days)
                    ml_forecast[ck] = sum(rates.values())

        result = run_benchmark(events, t, window_days, ml_forecast)
        all_results.append(result)
        t += 30
        n_windows += 1

    # Aggregate results across windows
    if all_results:
        agg = aggregate_benchmark_results(all_results)
    else:
        agg = {"error": "no_test_windows"}

    # Save
    out_path = RESULTS_DIR / f"csep_benchmark_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(agg, f, indent=2, ensure_ascii=False)
    logger.info("Benchmark results saved: %s", out_path)


def aggregate_benchmark_results(results_list):
    """Aggregate benchmark results across multiple test windows."""
    model_names = set()
    for r in results_list:
        model_names.update(r.get("models", {}).keys())

    aggregated = {
        "n_windows": len(results_list),
        "models": {},
        "t_tests": {},
    }

    for model in model_names:
        n_test_passes = 0
        molchan_skills = []
        log_likelihoods = []
        total_forecasts = []

        for r in results_list:
            m = r.get("models", {}).get(model, {})
            if not m:
                continue
            if m.get("n_test", {}).get("pass", False):
                n_test_passes += 1
            molchan_skills.append(m.get("molchan", {}).get("area_skill_score", 0))
            log_likelihoods.append(m.get("l_test", {}).get("log_likelihood", 0))
            total_forecasts.append(m.get("total_forecast_rate", 0))

        n = len(molchan_skills) or 1
        aggregated["models"][model] = {
            "n_test_pass_rate": round(n_test_passes / n, 4),
            "mean_molchan_skill": round(sum(molchan_skills) / n, 4),
            "mean_log_likelihood": round(sum(log_likelihoods) / n, 4),
            "mean_total_rate": round(sum(total_forecasts) / n, 4),
        }

    # Aggregate T-tests
    t_test_names = set()
    for r in results_list:
        t_test_names.update(r.get("t_tests", {}).keys())

    for tt_name in t_test_names:
        t_stats = []
        sig_count = 0
        for r in results_list:
            tt = r.get("t_tests", {}).get(tt_name, {})
            if tt:
                t_stats.append(tt.get("t_statistic", 0))
                if tt.get("significant_at_005", False):
                    sig_count += 1

        n = len(t_stats) or 1
        aggregated["t_tests"][tt_name] = {
            "mean_t_statistic": round(sum(t_stats) / n, 4),
            "fraction_significant": round(sig_count / n, 4),
        }

    return aggregated


if __name__ == "__main__":
    main()
