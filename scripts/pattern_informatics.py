"""Pattern Informatics (PI) analysis.

Detects spatial cells where seismicity patterns have changed relative
to a baseline period.  Cells with anomalously high PI scores indicate
regions where the system has deviated from its historical mean, which
statistical-mechanics theory links to approaching criticality.

Algorithm (Rundle et al. 2003):
  1. Grid Japan at 0.5° resolution
  2. For each cell, compute seismicity rate in sliding time windows
  3. Normalise the rate change: Δs_i = (s_i(t2) - s_i(t1)) / σ_i
  4. PI(x) = <Δs²> - <Δs>²  (variance of the normalised change)
  5. Evaluate: fraction of M5+ events falling in top-N% PI cells

References:
  - Rundle, J.B. et al. (2003) Rev. Geophys.
  - Tiampo, K.F. et al. (2002) Pure Appl. Geophys.
"""

import asyncio
import json
import logging
import math
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

# Grid parameters
LAT_MIN, LAT_MAX = 25.0, 46.0
LON_MIN, LON_MAX = 125.0, 150.0
CELL_SIZE = 0.5  # degrees

# Target magnitude threshold
MAG_MIN_CATALOG = 3.0
MAG_TARGET = 5.0


def _cell_index(lat: float, lon: float) -> tuple[int, int]:
    """Return (row, col) grid index for a coordinate."""
    row = int((lat - LAT_MIN) / CELL_SIZE)
    col = int((lon - LON_MIN) / CELL_SIZE)
    return (row, col)


def _cell_centre(row: int, col: int) -> tuple[float, float]:
    """Return (lat, lon) of a cell centre."""
    return (LAT_MIN + (row + 0.5) * CELL_SIZE,
            LON_MIN + (col + 0.5) * CELL_SIZE)


def _n_rows() -> int:
    return int((LAT_MAX - LAT_MIN) / CELL_SIZE)


def _n_cols() -> int:
    return int((LON_MAX - LON_MIN) / CELL_SIZE)


def _year_frac(dt: datetime) -> float:
    """Convert datetime to fractional year."""
    year_start = datetime(dt.year, 1, 1, tzinfo=timezone.utc)
    year_end = datetime(dt.year + 1, 1, 1, tzinfo=timezone.utc)
    return dt.year + (dt - year_start).total_seconds() / (year_end - year_start).total_seconds()


# ------------------------------------------------------------------
# Core PI computation
# ------------------------------------------------------------------

def _compute_cell_rates(
    events: list[dict],
    t_start: float,
    t_end: float,
    mode: str = "count",
) -> dict[tuple[int, int], float]:
    """Compute seismicity rate per cell for a time window.

    Args:
        events: list of event dicts with 'year_frac', 'mag', 'lat', 'lon'
        t_start: start of window (fractional year)
        t_end: end of window (fractional year)
        mode: 'count' for M3+ count rate, 'energy' for Benioff strain rate

    Returns:
        dict mapping (row, col) -> rate (events/year or energy/year)
    """
    duration = t_end - t_start
    if duration <= 0:
        return {}

    rates: dict[tuple[int, int], float] = {}
    for ev in events:
        if ev["year_frac"] < t_start or ev["year_frac"] >= t_end:
            continue
        key = _cell_index(ev["lat"], ev["lon"])
        if mode == "energy":
            # Benioff strain: sqrt(seismic energy) ∝ 10^(0.75*M)
            val = math.pow(10, 0.75 * ev["mag"])
        else:
            val = 1.0
        rates[key] = rates.get(key, 0.0) + val

    # Normalise to annual rate
    for k in rates:
        rates[k] /= duration

    return rates


def _compute_pi_map(
    events: list[dict],
    t_base_start: float,
    t_base_end: float,
    t_anomaly_start: float,
    t_anomaly_end: float,
    n_sub_windows: int = 5,
    mode: str = "count",
) -> dict[tuple[int, int], float]:
    """Compute PI scores for all cells.

    Uses multiple overlapping sub-windows within the base period to
    estimate the mean and variance of the normalised activity change.

    The anomaly period rate is compared to each sub-window's rate;
    PI(x) = Var(Δs) across these comparisons.

    Args:
        events: full event catalogue
        t_base_start, t_base_end: learning/base period (years)
        t_anomaly_start, t_anomaly_end: anomaly detection period
        n_sub_windows: number of sub-windows in the base period
        mode: 'count' or 'energy'

    Returns:
        dict mapping (row, col) -> PI score
    """
    # Collect all active cells
    all_cells: set[tuple[int, int]] = set()
    for ev in events:
        if t_base_start <= ev["year_frac"] < t_anomaly_end:
            all_cells.add(_cell_index(ev["lat"], ev["lon"]))

    if not all_cells:
        return {}

    # Anomaly period rate
    rate_anomaly = _compute_cell_rates(events, t_anomaly_start, t_anomaly_end, mode)

    # Compute sub-window rates in the base period
    base_duration = t_base_end - t_base_start
    sub_duration = base_duration / n_sub_windows
    sub_rates: list[dict[tuple[int, int], float]] = []
    for i in range(n_sub_windows):
        sw_start = t_base_start + i * sub_duration
        sw_end = sw_start + sub_duration
        sub_rates.append(_compute_cell_rates(events, sw_start, sw_end, mode))

    # For each cell, compute σ from the base sub-windows, then Δs for
    # each sub-window vs anomaly, and finally PI = Var(Δs).
    pi_scores: dict[tuple[int, int], float] = {}

    for cell in all_cells:
        # Base sub-window rates for this cell
        sw_vals = [sr.get(cell, 0.0) for sr in sub_rates]
        mean_base = sum(sw_vals) / len(sw_vals)
        var_base = sum((v - mean_base) ** 2 for v in sw_vals) / len(sw_vals)
        sigma = math.sqrt(var_base) if var_base > 0 else 0.0

        # Skip cells with zero variance (constant or empty)
        if sigma < 1e-12:
            # Fallback: use mean as scale if non-zero
            if mean_base > 1e-12:
                sigma = mean_base
            else:
                continue

        r_anom = rate_anomaly.get(cell, 0.0)

        # Normalised change for each sub-window vs anomaly
        delta_s = [(r_anom - sv) / sigma for sv in sw_vals]

        # PI = Var(Δs) = <Δs²> - <Δs>²
        mean_ds = sum(delta_s) / len(delta_s)
        mean_ds2 = sum(d * d for d in delta_s) / len(delta_s)
        pi = mean_ds2 - mean_ds * mean_ds

        if pi > 0:
            pi_scores[cell] = pi

    return pi_scores


def _percentile_rank(pi_map: dict[tuple[int, int], float], cell: tuple[int, int]) -> float | None:
    """Return the percentile rank (0-100) of a cell's PI score."""
    if cell not in pi_map:
        return None
    score = pi_map[cell]
    all_scores = sorted(pi_map.values())
    # Fraction of scores <= this score
    count_leq = sum(1 for s in all_scores if s <= score)
    return 100.0 * count_leq / len(all_scores)


# ------------------------------------------------------------------
# Evaluation
# ------------------------------------------------------------------

def _evaluate_pi(
    pi_map: dict[tuple[int, int], float],
    target_events: list[dict],
    thresholds: list[float] | None = None,
) -> dict:
    """Evaluate PI map against actual target events.

    For each threshold (top-N% of cells), compute:
    - hit rate: fraction of target events in hot cells
    - area fraction: fraction of active cells flagged
    - Molchan score: 1 - hit_rate + area_fraction (lower is better)

    Returns:
        dict with evaluation metrics and ROC points
    """
    if thresholds is None:
        thresholds = [5, 10, 15, 20, 25, 30, 40, 50]

    if not pi_map or not target_events:
        return {"error": "Empty PI map or no target events"}

    sorted_scores = sorted(pi_map.values(), reverse=True)
    n_cells = len(sorted_scores)

    roc_points = []
    for thr_pct in thresholds:
        n_hot = max(1, int(n_cells * thr_pct / 100.0))
        score_cutoff = sorted_scores[min(n_hot - 1, n_cells - 1)]

        hot_cells = {c for c, s in pi_map.items() if s >= score_cutoff}
        n_hit = sum(1 for ev in target_events if _cell_index(ev["lat"], ev["lon"]) in hot_cells)

        hit_rate = n_hit / len(target_events) if target_events else 0
        area_frac = len(hot_cells) / n_cells
        molchan = 1.0 - hit_rate + area_frac

        roc_points.append({
            "threshold_pct": thr_pct,
            "n_hot_cells": len(hot_cells),
            "n_hits": n_hit,
            "n_targets": len(target_events),
            "hit_rate": round(hit_rate, 4),
            "area_fraction": round(area_frac, 4),
            "molchan_score": round(molchan, 4),
        })

    # AUC of Molchan diagram (trapezoidal integration over area_frac vs miss_rate)
    # miss_rate = 1 - hit_rate; ideal forecast hugs the left axis
    pts = sorted(roc_points, key=lambda p: p["area_fraction"])
    auc = 0.0
    for i in range(1, len(pts)):
        dx = pts[i]["area_fraction"] - pts[i - 1]["area_fraction"]
        y_avg = ((1 - pts[i]["hit_rate"]) + (1 - pts[i - 1]["hit_rate"])) / 2
        auc += dx * y_avg
    # Normalise: random forecast AUC = 0.5, perfect = 0.0
    # Skill = 1 - 2*AUC (positive = better than random)

    return {
        "roc_points": roc_points,
        "molchan_auc": round(auc, 4),
    }


def _compare_pi_distributions(
    pi_map: dict[tuple[int, int], float],
    target_events: list[dict],
    n_random: int = 1000,
    seed: int = 42,
) -> dict:
    """Compare PI score distributions: target events vs random locations.

    Returns statistics for both distributions plus lift metrics.
    """
    # PI scores at target event locations
    eq_scores = []
    for ev in target_events:
        cell = _cell_index(ev["lat"], ev["lon"])
        if cell in pi_map:
            eq_scores.append(pi_map[cell])

    # PI scores at random active cells
    rng = random.Random(seed)
    active_cells = list(pi_map.keys())
    rand_scores = []
    if active_cells:
        for _ in range(n_random):
            cell = rng.choice(active_cells)
            rand_scores.append(pi_map[cell])

    def _stats(values: list[float], label: str) -> dict:
        if not values:
            return {"n": 0, "label": label}
        s = sorted(values)
        n = len(s)
        mean = sum(s) / n
        median = s[n // 2]
        p90 = s[int(n * 0.9)]
        # Fraction in top quartile of all PI scores
        all_sorted = sorted(pi_map.values())
        q75_cutoff = all_sorted[int(len(all_sorted) * 0.75)] if all_sorted else 0
        in_top25 = sum(1 for v in s if v >= q75_cutoff)
        return {
            "label": label,
            "n": n,
            "mean": round(mean, 4),
            "median": round(median, 4),
            "p90": round(p90, 4),
            "in_top_25pct": round(100 * in_top25 / n, 1),
        }

    eq_stats = _stats(eq_scores, "earthquake")
    rand_stats = _stats(rand_scores, "random")

    lift = None
    if eq_stats.get("in_top_25pct") and rand_stats.get("in_top_25pct"):
        lift = round(eq_stats["in_top_25pct"] / max(rand_stats["in_top_25pct"], 0.1), 2)

    return {
        "earthquake": eq_stats,
        "random": rand_stats,
        "lift_top25": lift,
    }


# ------------------------------------------------------------------
# Temporal stability
# ------------------------------------------------------------------

def _temporal_stability(
    events: list[dict],
    base_start: float,
    base_end: float,
    step_years: float = 1.0,
    window_years: float = 2.0,
    mode: str = "count",
) -> list[dict]:
    """Compute PI maps for sliding anomaly windows and measure stability.

    Returns per-window summary (n_hotspots, top-cell overlap with
    previous window).
    """
    results = []
    prev_top_cells: set[tuple[int, int]] | None = None
    t = base_end

    while t + window_years <= _year_frac(datetime(2026, 3, 1, tzinfo=timezone.utc)):
        pi_map = _compute_pi_map(events, base_start, base_end, t, t + window_years,
                                 n_sub_windows=5, mode=mode)
        if not pi_map:
            t += step_years
            continue

        sorted_scores = sorted(pi_map.values(), reverse=True)
        n_cells = len(sorted_scores)
        n_top = max(1, int(n_cells * 0.10))
        cutoff = sorted_scores[min(n_top - 1, n_cells - 1)]
        top_cells = {c for c, s in pi_map.items() if s >= cutoff}

        overlap = None
        if prev_top_cells is not None and prev_top_cells:
            intersection = top_cells & prev_top_cells
            union = top_cells | prev_top_cells
            overlap = round(len(intersection) / len(union), 3) if union else 0

        results.append({
            "anomaly_window": f"{t:.1f}-{t + window_years:.1f}",
            "n_active_cells": n_cells,
            "n_top10pct": len(top_cells),
            "jaccard_overlap_prev": overlap,
        })

        prev_top_cells = top_cells
        t += step_years

    return results


# ------------------------------------------------------------------
# Main analysis
# ------------------------------------------------------------------

async def run_pattern_informatics():
    """Run the full Pattern Informatics analysis."""
    logger.info("=== Pattern Informatics Analysis ===")

    # Load catalogue
    async with aiosqlite.connect(DB_PATH) as db:
        rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= ? AND magnitude IS NOT NULL "
            "ORDER BY occurred_at",
            (MAG_MIN_CATALOG,),
        )

    events: list[dict] = []
    for r in rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            yf = _year_frac(t)
            events.append({
                "time": t,
                "year_frac": yf,
                "mag": r[1],
                "lat": r[2],
                "lon": r[3],
                "depth": r[4] if r[4] else 10.0,
            })
        except (ValueError, TypeError):
            continue

    if not events:
        logger.error("No events in catalogue")
        return {"error": "Empty catalogue"}

    logger.info("  Catalogue: %d M%.1f+ events, %.1f - %.1f",
                len(events), MAG_MIN_CATALOG,
                events[0]["year_frac"], events[-1]["year_frac"])

    targets_all = [e for e in events if e["mag"] >= MAG_TARGET]
    logger.info("  Targets (M%.1f+): %d events", MAG_TARGET, len(targets_all))

    # ------------------------------------------------------------------
    # Test 1: Train 2011-2015, Validate 2016-2018
    # ------------------------------------------------------------------
    logger.info("--- Test 1: Train 2011-2015, Validate 2016-2018 ---")
    pi_map_tv = _compute_pi_map(
        events,
        t_base_start=2011.0, t_base_end=2014.0,
        t_anomaly_start=2014.0, t_anomaly_end=2015.0,
        n_sub_windows=6, mode="count",
    )
    targets_val = [e for e in targets_all if 2016.0 <= e["year_frac"] < 2019.0]
    logger.info("  PI map: %d active cells, %d validation targets",
                len(pi_map_tv), len(targets_val))

    eval_tv = _evaluate_pi(pi_map_tv, targets_val)
    dist_tv = _compare_pi_distributions(pi_map_tv, targets_val)

    if isinstance(eval_tv, dict) and "roc_points" in eval_tv:
        best = min(eval_tv["roc_points"], key=lambda p: p["molchan_score"])
        logger.info("  Best Molchan: %.4f at top-%d%% (hit=%.2f, area=%.2f)",
                    best["molchan_score"], best["threshold_pct"],
                    best["hit_rate"], best["area_fraction"])
    if dist_tv.get("lift_top25"):
        logger.info("  Lift (top-25%%): %.2f", dist_tv["lift_top25"])

    # ------------------------------------------------------------------
    # Test 2: Train 2011-2018, Prospective 2019-2026
    # ------------------------------------------------------------------
    logger.info("--- Test 2: Train 2011-2018, Prospective 2019-2026 ---")
    pi_map_pr = _compute_pi_map(
        events,
        t_base_start=2011.0, t_base_end=2017.0,
        t_anomaly_start=2017.0, t_anomaly_end=2019.0,
        n_sub_windows=6, mode="count",
    )
    targets_pr = [e for e in targets_all if 2019.0 <= e["year_frac"] < 2027.0]
    logger.info("  PI map: %d active cells, %d prospective targets",
                len(pi_map_pr), len(targets_pr))

    eval_pr = _evaluate_pi(pi_map_pr, targets_pr)
    dist_pr = _compare_pi_distributions(pi_map_pr, targets_pr)

    if isinstance(eval_pr, dict) and "roc_points" in eval_pr:
        best = min(eval_pr["roc_points"], key=lambda p: p["molchan_score"])
        logger.info("  Best Molchan: %.4f at top-%d%% (hit=%.2f, area=%.2f)",
                    best["molchan_score"], best["threshold_pct"],
                    best["hit_rate"], best["area_fraction"])
    if dist_pr.get("lift_top25"):
        logger.info("  Lift (top-25%%): %.2f", dist_pr["lift_top25"])

    # ------------------------------------------------------------------
    # Test 3: Energy-based PI (Benioff strain)
    # ------------------------------------------------------------------
    logger.info("--- Test 3: Energy-based PI (Benioff strain) ---")
    pi_map_en = _compute_pi_map(
        events,
        t_base_start=2011.0, t_base_end=2017.0,
        t_anomaly_start=2017.0, t_anomaly_end=2019.0,
        n_sub_windows=6, mode="energy",
    )
    eval_en = _evaluate_pi(pi_map_en, targets_pr)
    dist_en = _compare_pi_distributions(pi_map_en, targets_pr)

    if isinstance(eval_en, dict) and "roc_points" in eval_en:
        best = min(eval_en["roc_points"], key=lambda p: p["molchan_score"])
        logger.info("  Best Molchan: %.4f at top-%d%% (hit=%.2f, area=%.2f)",
                    best["molchan_score"], best["threshold_pct"],
                    best["hit_rate"], best["area_fraction"])
    if dist_en.get("lift_top25"):
        logger.info("  Lift (top-25%%): %.2f", dist_en["lift_top25"])

    # ------------------------------------------------------------------
    # Temporal stability
    # ------------------------------------------------------------------
    logger.info("--- Temporal stability ---")
    stability = _temporal_stability(
        events,
        base_start=2011.0, base_end=2015.0,
        step_years=1.0, window_years=2.0,
        mode="count",
    )
    if stability:
        overlaps = [s["jaccard_overlap_prev"] for s in stability
                    if s["jaccard_overlap_prev"] is not None]
        if overlaps:
            mean_overlap = sum(overlaps) / len(overlaps)
            logger.info("  %d windows, mean Jaccard overlap: %.3f", len(stability), mean_overlap)

    # ------------------------------------------------------------------
    # Top PI hotspot cells (prospective map)
    # ------------------------------------------------------------------
    top_hotspots = []
    if pi_map_pr:
        sorted_cells = sorted(pi_map_pr.items(), key=lambda x: x[1], reverse=True)
        for cell, score in sorted_cells[:20]:
            lat, lon = _cell_centre(*cell)
            pct = _percentile_rank(pi_map_pr, cell)
            top_hotspots.append({
                "lat": round(lat, 2),
                "lon": round(lon, 2),
                "pi_score": round(score, 4),
                "percentile": round(pct, 1) if pct is not None else None,
            })

    # ------------------------------------------------------------------
    # Assemble results
    # ------------------------------------------------------------------
    results = {
        "method": "Pattern Informatics (Rundle et al. 2003)",
        "grid": {
            "lat_range": [LAT_MIN, LAT_MAX],
            "lon_range": [LON_MIN, LON_MAX],
            "cell_size_deg": CELL_SIZE,
            "n_rows": _n_rows(),
            "n_cols": _n_cols(),
        },
        "catalogue": {
            "n_events_M3+": len(events),
            "n_targets_M5+": len(targets_all),
            "year_range": [round(events[0]["year_frac"], 2),
                           round(events[-1]["year_frac"], 2)],
        },
        "test1_train_validation": {
            "train": "2011-2015",
            "validate": "2016-2018",
            "n_active_cells": len(pi_map_tv),
            "n_targets": len(targets_val),
            "evaluation": eval_tv,
            "distribution_comparison": dist_tv,
        },
        "test2_prospective": {
            "train": "2011-2018",
            "test": "2019-2026",
            "n_active_cells": len(pi_map_pr),
            "n_targets": len(targets_pr),
            "evaluation": eval_pr,
            "distribution_comparison": dist_pr,
        },
        "test3_energy_based": {
            "mode": "benioff_strain",
            "train": "2011-2018",
            "test": "2019-2026",
            "n_active_cells": len(pi_map_en),
            "evaluation": eval_en,
            "distribution_comparison": dist_en,
        },
        "temporal_stability": stability,
        "top_hotspots_prospective": top_hotspots,
    }

    logger.info("=== Pattern Informatics complete ===")
    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = await run_pattern_informatics()
    out_path = RESULTS_DIR / f"pattern_informatics_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
