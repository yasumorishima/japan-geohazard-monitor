"""GNSS-TEC high-resolution ionospheric anomaly analysis for earthquake precursors.

Phase 1 used IONEX TEC (2.5 x 5.0 deg grid from CODE), which failed because
spatial averaging diluted any epicentral signal. This analysis uses Nagoya
University GNSS-TEC data at 0.5 deg resolution (gnss_tec table, ~31k records)
to re-examine the LAIC ionospheric coupling hypothesis:

    Stress -> microfracturing -> radon/charged aerosol release
    -> electric field perturbation -> ionospheric plasma redistribution
    -> TEC anomaly above epicenter

Key design choices:
    - Forward-looking only (no future data leakage)
    - Aftershock isolation filter applied throughout
    - Day/night separation (LAIC predicts stronger nighttime anomalies)
    - Alarm-based prospective evaluation with probability gain
    - Direct comparison with low-res IONEX to quantify resolution effect

References:
    - Heki (2011) Ionospheric electron enhancement preceding the 2011 Tohoku-Oki
    - Liu et al. (2006) Pre-earthquake ionospheric anomalies before M>=6.0
    - Pulinets & Ouzounov (2011) LAIC model - Natural Hazards Earth Syst Sci
    - He & Heki (2017) Ionospheric anomalies immediately before Mw 7.0+ EQs
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
DEG_TO_KM = 111.32


# ---------------------------------------------------------------------------
# Shared utilities (self-contained, matching existing script patterns)
# ---------------------------------------------------------------------------

def filter_isolated(target_events, all_events_sorted, days=3.0, degrees=1.5):
    """Remove aftershock-contaminated events.

    An event is considered isolated if no prior event of comparable
    magnitude (within mag - 0.5) occurred within *days* and *degrees*.
    """
    all_times = [e[0] for e in all_events_sorted]
    isolated = []
    for t, lat, lon, mag in target_events:
        t_min = t - timedelta(days=days)
        idx_start = bisect.bisect_left(all_times, t_min)
        idx_end = bisect.bisect_left(all_times, t)
        is_iso = True
        for i in range(idx_start, idx_end):
            te, late, lone, mage = all_events_sorted[i]
            if te >= t:
                break
            if abs(lat - late) <= degrees and abs(lon - lone) <= degrees and mage >= mag - 0.5:
                is_iso = False
                break
        if is_iso:
            isolated.append((t, lat, lon, mag))
    return isolated


def percentile(values, p):
    """Compute p-th percentile (0-100) with linear interpolation."""
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    d = k - f
    return s[f] + d * (s[c] - s[f])


def bootstrap_ci(values, n_boot=1000, ci=95):
    """Bootstrap confidence interval for the mean."""
    if not values:
        return {"mean": 0, "ci_lower": 0, "ci_upper": 0}
    rng = random.Random(42)
    means = []
    for _ in range(n_boot):
        sample = rng.choices(values, k=len(values))
        means.append(sum(sample) / len(sample))
    means.sort()
    alpha = (100 - ci) / 200
    lo = int(alpha * n_boot)
    hi = int((1 - alpha) * n_boot)
    return {
        "mean": round(sum(values) / len(values), 4),
        "ci_lower": round(means[lo], 4),
        "ci_upper": round(means[min(hi, n_boot - 1)], 4),
    }


def haversine_deg(lat1, lon1, lat2, lon2):
    """Great-circle distance in degrees (approximate)."""
    dlat = lat2 - lat1
    dlon = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.sqrt(dlat ** 2 + dlon ** 2)


# ---------------------------------------------------------------------------
# TEC Z-score computation
# ---------------------------------------------------------------------------

def compute_zscore(values, baseline_values):
    """Compute Z-score of mean(values) against baseline distribution.

    Returns None if baseline has insufficient data or near-zero std.
    """
    if not values or len(baseline_values) < 5:
        return None
    bl_mean = sum(baseline_values) / len(baseline_values)
    bl_var = sum((v - bl_mean) ** 2 for v in baseline_values) / len(baseline_values)
    bl_std = math.sqrt(bl_var)
    if bl_std < 0.01:
        return None
    obs_mean = sum(values) / len(values)
    return (obs_mean - bl_mean) / bl_std


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

async def load_data(db, min_mag=5.0):
    """Load earthquakes and GNSS-TEC data from DB."""
    eq_rows = await db.execute_fetchall(
        "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
        "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
        "ORDER BY occurred_at"
    )

    gnss_rows = await db.execute_fetchall(
        "SELECT latitude, longitude, tec_tecu, dtec_tecu, epoch "
        "FROM gnss_tec ORDER BY epoch"
    )

    ionex_rows = await db.execute_fetchall(
        "SELECT latitude, longitude, tec_tecu, epoch "
        "FROM tec ORDER BY epoch"
    )

    # Parse earthquakes
    all_events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            all_events.append({
                "time": t, "mag": r[1], "lat": r[2], "lon": r[3],
                "depth": r[4] if r[4] else 10.0,
            })
        except (ValueError, TypeError):
            continue

    targets = [e for e in all_events if e["mag"] >= min_mag]
    all_tuples = [(e["time"], e["lat"], e["lon"], e["mag"]) for e in all_events]
    target_tuples = [(e["time"], e["lat"], e["lon"], e["mag"]) for e in targets]
    isolated_set = set(
        (t.isoformat(), lat, lon)
        for t, lat, lon, mag in filter_isolated(target_tuples, all_tuples)
    )
    for e in targets:
        e["isolated"] = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set

    # Parse GNSS-TEC into spatial index: (lat, lon) -> [(epoch_dt, tec, dtec)]
    gnss_by_loc = {}
    for lat, lon, tec, dtec, epoch_str in gnss_rows:
        key = (round(lat, 2), round(lon, 2))
        try:
            ep = datetime.fromisoformat(epoch_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if key not in gnss_by_loc:
            gnss_by_loc[key] = []
        gnss_by_loc[key].append((ep, tec, dtec))

    # Sort each location's timeseries
    for key in gnss_by_loc:
        gnss_by_loc[key].sort(key=lambda x: x[0])

    # Parse IONEX similarly
    ionex_by_loc = {}
    for lat, lon, tec, epoch_str in ionex_rows:
        key = (round(lat, 1), round(lon, 1))
        try:
            ep = datetime.fromisoformat(epoch_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if key not in ionex_by_loc:
            ionex_by_loc[key] = []
        ionex_by_loc[key].append((ep, tec))

    for key in ionex_by_loc:
        ionex_by_loc[key].sort(key=lambda x: x[0])

    return all_events, targets, gnss_by_loc, ionex_by_loc


# ---------------------------------------------------------------------------
# Find nearest grid point
# ---------------------------------------------------------------------------

def find_nearest_grid(eq_lat, eq_lon, loc_dict, max_dist_deg=0.5):
    """Find nearest grid point within max_dist_deg. Return key or None."""
    best_key = None
    best_dist = float("inf")
    for key in loc_dict:
        d = haversine_deg(eq_lat, eq_lon, key[0], key[1])
        if d < best_dist and d <= max_dist_deg:
            best_dist = d
            best_key = key
    return best_key, best_dist


# ---------------------------------------------------------------------------
# Extract TEC windows around an event
# ---------------------------------------------------------------------------

def extract_tec_windows(timeseries, event_time, pre_days=3, baseline_days=30):
    """Extract TEC values in pre-event and baseline windows.

    Returns:
        pre_values: TEC values in [event - pre_days, event)
        baseline_values: TEC values in [event - baseline_days, event - pre_days)
    """
    pre_start = event_time - timedelta(days=pre_days)
    bl_start = event_time - timedelta(days=baseline_days)

    pre_values = []
    baseline_values = []

    # Use bisect on epoch times for efficiency
    epochs = [r[0] for r in timeseries]
    idx_bl = bisect.bisect_left(epochs, bl_start)
    idx_ev = bisect.bisect_left(epochs, event_time)

    for i in range(idx_bl, idx_ev):
        ep, tec = timeseries[i][0], timeseries[i][1]
        if ep >= pre_start:
            pre_values.append(tec)
        else:
            baseline_values.append(tec)

    return pre_values, baseline_values


def extract_tec_windows_7d(timeseries, event_time, baseline_days=30):
    """Same as extract_tec_windows but with 7-day pre-event window."""
    return extract_tec_windows(timeseries, event_time, pre_days=7, baseline_days=baseline_days)


def classify_hour_ut(epoch_dt):
    """Classify epoch into day (03 UT ~ 12 JST) or night (12 UT ~ 21 JST)."""
    h = epoch_dt.hour
    if 0 <= h <= 5:
        return "night"  # 09-14 JST (close enough to nighttime ionosphere)
    elif 6 <= h <= 11:
        return "day"
    elif 12 <= h <= 17:
        return "night"
    else:
        return "day"


def split_day_night(timeseries):
    """Split timeseries into daytime (03 UT) and nighttime (12 UT) subsets."""
    day_ts = [(ep, tec, dtec) for ep, tec, dtec in timeseries
              if classify_hour_ut(ep) == "day"]
    night_ts = [(ep, tec, dtec) for ep, tec, dtec in timeseries
                if classify_hour_ut(ep) == "night"]
    return day_ts, night_ts


# ---------------------------------------------------------------------------
# Analysis 1: Epicentral TEC anomaly detection
# ---------------------------------------------------------------------------

async def analysis_epicentral_zscore(targets, gnss_by_loc, all_events):
    """For each M5+ earthquake, compute TEC Z-score at nearest GNSS-TEC grid point."""
    logger.info("  --- Analysis 1: Epicentral TEC Z-score ---")

    results_3d = []
    results_7d = []
    matched = 0
    unmatched = 0

    for e in targets:
        key, dist = find_nearest_grid(e["lat"], e["lon"], gnss_by_loc, max_dist_deg=0.5)
        if key is None:
            unmatched += 1
            continue

        ts = gnss_by_loc[key]
        matched += 1

        # 3-day pre-event vs 30-day baseline
        pre3, bl3 = extract_tec_windows(ts, e["time"], pre_days=3, baseline_days=30)
        z3 = compute_zscore(pre3, bl3)

        # 7-day pre-event vs 30-day baseline
        pre7, bl7 = extract_tec_windows(ts, e["time"], pre_days=7, baseline_days=30)
        z7 = compute_zscore(pre7, bl7)

        entry = {
            "time": e["time"].isoformat(),
            "mag": e["mag"],
            "lat": e["lat"],
            "lon": e["lon"],
            "depth": e["depth"],
            "isolated": e["isolated"],
            "grid_lat": key[0],
            "grid_lon": key[1],
            "dist_deg": round(dist, 3),
            "n_pre3": len(pre3),
            "n_bl3": len(bl3),
            "zscore_3d": round(z3, 4) if z3 is not None else None,
            "n_pre7": len(pre7),
            "n_bl7": len(bl7),
            "zscore_7d": round(z7, 4) if z7 is not None else None,
        }

        if z3 is not None:
            results_3d.append(z3)
        if z7 is not None:
            results_7d.append(z7)

        e["zscore_3d"] = z3
        e["zscore_7d"] = z7
        e["_detail"] = entry

    logger.info("    Matched: %d, Unmatched: %d", matched, unmatched)

    # Random baseline: same procedure at random locations/times
    rng = random.Random(42)
    if not all_events:
        rand_z3 = []
        rand_z7 = []
    else:
        t_min = all_events[0]["time"]
        t_max = all_events[-1]["time"]
        t_span = (t_max - t_min).total_seconds()

        rand_z3 = []
        rand_z7 = []
        grid_keys = list(gnss_by_loc.keys())
        if grid_keys:
            for _ in range(500):
                rk = rng.choice(grid_keys)
                rt = t_min + timedelta(seconds=rng.random() * t_span)
                ts = gnss_by_loc[rk]
                pre3, bl3 = extract_tec_windows(ts, rt, pre_days=3, baseline_days=30)
                z3 = compute_zscore(pre3, bl3)
                if z3 is not None:
                    rand_z3.append(z3)
                pre7, bl7 = extract_tec_windows(ts, rt, pre_days=7, baseline_days=30)
                z7 = compute_zscore(pre7, bl7)
                if z7 is not None:
                    rand_z7.append(z7)

    def zscore_stats(values, label):
        if not values:
            return {"n": 0, "label": label}
        n = len(values)
        return {
            "n": n,
            "label": label,
            "mean": round(sum(values) / n, 4),
            "median": round(sorted(values)[n // 2], 4),
            "std": round((sum((v - sum(values) / n) ** 2 for v in values) / n) ** 0.5, 4),
            "gt_2_pct": round(sum(1 for v in values if v > 2.0) / n * 100, 1),
            "gt_3_pct": round(sum(1 for v in values if v > 3.0) / n * 100, 1),
            "lt_neg2_pct": round(sum(1 for v in values if v < -2.0) / n * 100, 1),
            "abs_gt_2_pct": round(sum(1 for v in values if abs(v) > 2.0) / n * 100, 1),
            "bootstrap_ci": bootstrap_ci(values),
            "p10": round(percentile(values, 10), 4),
            "p25": round(percentile(values, 25), 4),
            "p75": round(percentile(values, 75), 4),
            "p90": round(percentile(values, 90), 4),
        }

    eq_3d = zscore_stats(results_3d, "earthquake_pre3d")
    eq_7d = zscore_stats(results_7d, "earthquake_pre7d")
    rd_3d = zscore_stats(rand_z3, "random_pre3d")
    rd_7d = zscore_stats(rand_z7, "random_pre7d")

    # Lift: fraction exceeding |Z|>2 in EQ vs random
    def safe_lift(eq_pct, rand_pct):
        if rand_pct is None or rand_pct < 0.1:
            return None
        if eq_pct is None:
            return None
        return round(eq_pct / max(rand_pct, 0.1), 2)

    lift_3d = safe_lift(eq_3d.get("abs_gt_2_pct"), rd_3d.get("abs_gt_2_pct"))
    lift_7d = safe_lift(eq_7d.get("abs_gt_2_pct"), rd_7d.get("abs_gt_2_pct"))

    logger.info("    3d Z-score: EQ mean=%.4f (n=%d) | rand mean=%.4f (n=%d) | lift(|Z|>2)=%s",
                eq_3d.get("mean", 0), eq_3d.get("n", 0),
                rd_3d.get("mean", 0), rd_3d.get("n", 0), lift_3d)
    logger.info("    7d Z-score: EQ mean=%.4f (n=%d) | rand mean=%.4f (n=%d) | lift(|Z|>2)=%s",
                eq_7d.get("mean", 0), eq_7d.get("n", 0),
                rd_7d.get("mean", 0), rd_7d.get("n", 0), lift_7d)

    return {
        "n_matched": matched,
        "n_unmatched": unmatched,
        "earthquake_3d": eq_3d,
        "earthquake_7d": eq_7d,
        "random_3d": rd_3d,
        "random_7d": rd_7d,
        "lift_absZ_gt2_3d": lift_3d,
        "lift_absZ_gt2_7d": lift_7d,
    }


# ---------------------------------------------------------------------------
# Analysis 2: Isolated event verification
# ---------------------------------------------------------------------------

async def analysis_isolated(targets):
    """Check whether TEC anomaly survives aftershock filtering."""
    logger.info("  --- Analysis 2: Isolated event filter ---")

    iso_z3 = [e["zscore_3d"] for e in targets if e.get("zscore_3d") is not None and e["isolated"]]
    iso_z7 = [e["zscore_7d"] for e in targets if e.get("zscore_7d") is not None and e["isolated"]]
    non_z3 = [e["zscore_3d"] for e in targets if e.get("zscore_3d") is not None and not e["isolated"]]
    non_z7 = [e["zscore_7d"] for e in targets if e.get("zscore_7d") is not None and not e["isolated"]]

    def stats(values, label):
        if not values:
            return {"n": 0, "label": label}
        n = len(values)
        return {
            "n": n,
            "label": label,
            "mean": round(sum(values) / n, 4),
            "abs_gt_2_pct": round(sum(1 for v in values if abs(v) > 2.0) / n * 100, 1),
            "gt_2_pct": round(sum(1 for v in values if v > 2.0) / n * 100, 1),
            "lt_neg2_pct": round(sum(1 for v in values if v < -2.0) / n * 100, 1),
            "bootstrap_ci": bootstrap_ci(values),
        }

    iso_3d_s = stats(iso_z3, "isolated_3d")
    iso_7d_s = stats(iso_z7, "isolated_7d")
    non_3d_s = stats(non_z3, "non_isolated_3d")
    non_7d_s = stats(non_z7, "non_isolated_7d")

    logger.info("    Isolated 3d: n=%d mean=%.4f |Z|>2=%.1f%%",
                iso_3d_s.get("n", 0), iso_3d_s.get("mean", 0), iso_3d_s.get("abs_gt_2_pct", 0))
    logger.info("    Non-iso  3d: n=%d mean=%.4f |Z|>2=%.1f%%",
                non_3d_s.get("n", 0), non_3d_s.get("mean", 0), non_3d_s.get("abs_gt_2_pct", 0))

    return {
        "isolated_3d": iso_3d_s,
        "isolated_7d": iso_7d_s,
        "non_isolated_3d": non_3d_s,
        "non_isolated_7d": non_7d_s,
    }


# ---------------------------------------------------------------------------
# Analysis 3: Day/night separation
# ---------------------------------------------------------------------------

async def analysis_day_night(targets, gnss_by_loc):
    """Separate daytime (03 UT / 12 JST) and nighttime (12 UT / 21 JST) TEC."""
    logger.info("  --- Analysis 3: Day/night separation ---")

    day_z3 = []
    night_z3 = []
    day_z7 = []
    night_z7 = []

    for e in targets:
        key, dist = find_nearest_grid(e["lat"], e["lon"], gnss_by_loc, max_dist_deg=0.5)
        if key is None:
            continue

        ts = gnss_by_loc[key]
        day_ts, night_ts = split_day_night(ts)

        # Daytime Z-scores
        if day_ts:
            # Convert to (epoch, tec) for extract_tec_windows compatibility
            day_ts_2 = [(ep, tec, dtec) for ep, tec, dtec in day_ts]
            pre3, bl3 = extract_tec_windows(day_ts_2, e["time"], pre_days=3, baseline_days=30)
            z = compute_zscore(pre3, bl3)
            if z is not None:
                day_z3.append(z)
            pre7, bl7 = extract_tec_windows(day_ts_2, e["time"], pre_days=7, baseline_days=30)
            z = compute_zscore(pre7, bl7)
            if z is not None:
                day_z7.append(z)

        # Nighttime Z-scores
        if night_ts:
            night_ts_2 = [(ep, tec, dtec) for ep, tec, dtec in night_ts]
            pre3, bl3 = extract_tec_windows(night_ts_2, e["time"], pre_days=3, baseline_days=30)
            z = compute_zscore(pre3, bl3)
            if z is not None:
                night_z3.append(z)
            pre7, bl7 = extract_tec_windows(night_ts_2, e["time"], pre_days=7, baseline_days=30)
            z = compute_zscore(pre7, bl7)
            if z is not None:
                night_z7.append(z)

    def stats(values, label):
        if not values:
            return {"n": 0, "label": label}
        n = len(values)
        return {
            "n": n,
            "label": label,
            "mean": round(sum(values) / n, 4),
            "abs_gt_2_pct": round(sum(1 for v in values if abs(v) > 2.0) / n * 100, 1),
            "gt_2_pct": round(sum(1 for v in values if v > 2.0) / n * 100, 1),
            "lt_neg2_pct": round(sum(1 for v in values if v < -2.0) / n * 100, 1),
            "bootstrap_ci": bootstrap_ci(values),
            "p25": round(percentile(values, 25), 4),
            "p75": round(percentile(values, 75), 4),
        }

    day_3d_s = stats(day_z3, "daytime_03UT_3d")
    day_7d_s = stats(day_z7, "daytime_03UT_7d")
    night_3d_s = stats(night_z3, "nighttime_12UT_3d")
    night_7d_s = stats(night_z7, "nighttime_12UT_7d")

    # LAIC predicts night > day for anomaly magnitude
    night_day_ratio_3d = None
    if day_3d_s.get("n", 0) > 0 and night_3d_s.get("n", 0) > 0:
        day_abs = day_3d_s.get("abs_gt_2_pct", 0)
        night_abs = night_3d_s.get("abs_gt_2_pct", 0)
        if day_abs > 0.1:
            night_day_ratio_3d = round(night_abs / day_abs, 2)

    logger.info("    Day   3d: n=%d mean=%.4f |Z|>2=%.1f%%",
                day_3d_s.get("n", 0), day_3d_s.get("mean", 0), day_3d_s.get("abs_gt_2_pct", 0))
    logger.info("    Night 3d: n=%d mean=%.4f |Z|>2=%.1f%%",
                night_3d_s.get("n", 0), night_3d_s.get("mean", 0), night_3d_s.get("abs_gt_2_pct", 0))
    logger.info("    Night/Day ratio (|Z|>2): %s", night_day_ratio_3d)

    return {
        "daytime_3d": day_3d_s,
        "daytime_7d": day_7d_s,
        "nighttime_3d": night_3d_s,
        "nighttime_7d": night_7d_s,
        "night_day_ratio_absZ2_3d": night_day_ratio_3d,
        "laic_prediction": "nighttime anomalies should be stronger",
    }


# ---------------------------------------------------------------------------
# Analysis 4: Prospective alarm evaluation
# ---------------------------------------------------------------------------

async def analysis_prospective_alarm(targets, gnss_by_loc, all_events):
    """Evaluate TEC Z-score as a prospective alarm.

    An alarm is declared when |Z-score| > threshold at a grid cell.
    Success: M5+ earthquake occurs within 7 days and 1 degree.
    Metrics: precision, recall, probability_gain (using spatial base rate).
    """
    logger.info("  --- Analysis 4: Prospective alarm evaluation ---")

    # Build event lookup: for each day, list of M5+ events
    eq_by_day = {}
    for e in all_events:
        if e["mag"] < 5.0:
            continue
        day_key = e["time"].strftime("%Y-%m-%d")
        if day_key not in eq_by_day:
            eq_by_day[day_key] = []
        eq_by_day[day_key].append(e)

    # Compute spatial base rate per 1-degree cell
    # base_rate = (n_M5+ in cell) / (total_days * n_cells)
    if not all_events:
        return {"error": "no_events"}
    t_min = all_events[0]["time"]
    t_max = all_events[-1]["time"]
    total_days = max((t_max - t_min).total_seconds() / 86400, 1)

    # Count M5+ per cell
    cell_counts = {}
    m5_events = [e for e in all_events if e["mag"] >= 5.0]
    for e in m5_events:
        cell = (round(e["lat"]), round(e["lon"]))
        cell_counts[cell] = cell_counts.get(cell, 0) + 1

    total_m5 = len(m5_events)
    global_base_rate = total_m5 / total_days  # events per day (globally)

    # Scan all grid points across all available epochs
    # For each grid-epoch, compute Z-score and check if M5+ follows
    thresholds = [1.5, 2.0, 2.5, 3.0]
    alarm_results = {f"threshold_{t}": {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
                     for t in thresholds}

    # Collect all unique dates in GNSS-TEC data
    all_dates = set()
    for key, ts in gnss_by_loc.items():
        for ep, tec, dtec in ts:
            all_dates.add(ep.strftime("%Y-%m-%d"))
    all_dates = sorted(all_dates)

    logger.info("    Scanning %d grid points x %d dates", len(gnss_by_loc), len(all_dates))

    alarm_details = {t: [] for t in thresholds}

    for key, ts in gnss_by_loc.items():
        grid_lat, grid_lon = key
        epochs = [r[0] for r in ts]
        tec_vals = [r[1] for r in ts]

        # For each epoch, compute running Z-score (30-day baseline before 3-day pre)
        for i, (ep, tec_val, dtec_val) in enumerate(ts):
            # Baseline: 30 days before, excluding last 3 days
            bl_start = ep - timedelta(days=30)
            bl_end = ep - timedelta(days=3)
            idx_bl_s = bisect.bisect_left(epochs, bl_start)
            idx_bl_e = bisect.bisect_left(epochs, bl_end)
            baseline = tec_vals[idx_bl_s:idx_bl_e]

            if len(baseline) < 5:
                continue

            bl_mean = sum(baseline) / len(baseline)
            bl_var = sum((v - bl_mean) ** 2 for v in baseline) / len(baseline)
            bl_std = math.sqrt(bl_var)
            if bl_std < 0.01:
                continue

            z = (tec_val - bl_mean) / bl_std

            # Check: does M5+ occur within 7 days and 1 degree?
            has_eq = False
            for day_offset in range(8):
                check_date = (ep + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                for eq in eq_by_day.get(check_date, []):
                    if haversine_deg(grid_lat, grid_lon, eq["lat"], eq["lon"]) <= 1.0:
                        has_eq = True
                        break
                if has_eq:
                    break

            for t in thresholds:
                is_alarm = abs(z) > t
                key_name = f"threshold_{t}"
                if is_alarm and has_eq:
                    alarm_results[key_name]["tp"] += 1
                elif is_alarm and not has_eq:
                    alarm_results[key_name]["fp"] += 1
                elif not is_alarm and has_eq:
                    alarm_results[key_name]["fn"] += 1
                else:
                    alarm_results[key_name]["tn"] += 1

    # Compute metrics
    output = {}
    for t in thresholds:
        key_name = f"threshold_{t}"
        r = alarm_results[key_name]
        tp, fp, fn, tn = r["tp"], r["fp"], r["fn"], r["tn"]
        total = tp + fp + fn + tn

        precision = tp / max(tp + fp, 1)
        recall = tp / max(tp + fn, 1)
        alarm_rate = (tp + fp) / max(total, 1)

        # Base rate: fraction of all observations that have M5+ within 7d/1deg
        base_rate = (tp + fn) / max(total, 1)
        probability_gain = precision / max(base_rate, 1e-6) if base_rate > 0 else None

        output[key_name] = {
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "alarm_rate": round(alarm_rate, 4),
            "base_rate": round(base_rate, 6),
            "probability_gain": round(probability_gain, 2) if probability_gain is not None else None,
        }

        logger.info("    |Z|>%.1f: prec=%.4f recall=%.4f alarm_rate=%.4f prob_gain=%s",
                    t, precision, recall, alarm_rate,
                    round(probability_gain, 2) if probability_gain else "N/A")

    output["global_base_rate_per_day"] = round(global_base_rate, 4)
    output["total_days"] = round(total_days, 1)
    output["total_m5_events"] = total_m5

    return output


# ---------------------------------------------------------------------------
# Analysis 5: IONEX comparison (resolution effect)
# ---------------------------------------------------------------------------

async def analysis_ionex_comparison(targets, gnss_by_loc, ionex_by_loc, all_events):
    """Run the same Z-score analysis on low-res IONEX and compare."""
    logger.info("  --- Analysis 5: IONEX (low-res) comparison ---")

    if not ionex_by_loc:
        logger.warning("    No IONEX data available for comparison.")
        return {"error": "no_ionex_data"}

    # GNSS-TEC Z-scores (already computed in targets)
    gnss_z3 = [e["zscore_3d"] for e in targets if e.get("zscore_3d") is not None]

    # Compute IONEX Z-scores for same events
    ionex_z3 = []
    ionex_matched = 0

    for e in targets:
        key, dist = find_nearest_grid(e["lat"], e["lon"], ionex_by_loc, max_dist_deg=3.0)
        if key is None:
            continue

        ts = ionex_by_loc[key]
        # Convert to (epoch, tec, None) for compatibility
        ts_compat = [(ep, tec, None) for ep, tec in ts]
        pre3, bl3 = extract_tec_windows(ts_compat, e["time"], pre_days=3, baseline_days=30)
        z = compute_zscore(pre3, bl3)
        if z is not None:
            ionex_z3.append(z)
            ionex_matched += 1

    # Random baseline for IONEX
    rng = random.Random(42)
    ionex_rand_z3 = []
    if all_events and ionex_by_loc:
        t_min = all_events[0]["time"]
        t_max = all_events[-1]["time"]
        t_span = (t_max - t_min).total_seconds()
        ionex_keys = list(ionex_by_loc.keys())

        for _ in range(500):
            rk = rng.choice(ionex_keys)
            rt = t_min + timedelta(seconds=rng.random() * t_span)
            ts = ionex_by_loc[rk]
            ts_compat = [(ep, tec, None) for ep, tec in ts]
            pre3, bl3 = extract_tec_windows(ts_compat, rt, pre_days=3, baseline_days=30)
            z = compute_zscore(pre3, bl3)
            if z is not None:
                ionex_rand_z3.append(z)

    def stats(values, label):
        if not values:
            return {"n": 0, "label": label}
        n = len(values)
        return {
            "n": n,
            "label": label,
            "mean": round(sum(values) / n, 4),
            "abs_gt_2_pct": round(sum(1 for v in values if abs(v) > 2.0) / n * 100, 1),
            "bootstrap_ci": bootstrap_ci(values),
        }

    gnss_stats = stats(gnss_z3, "gnss_tec_0.5deg")
    ionex_stats = stats(ionex_z3, "ionex_2.5x5deg")
    ionex_rand = stats(ionex_rand_z3, "ionex_random")

    # Resolution effect: lift comparison
    gnss_lift = None
    ionex_lift = None

    # For GNSS-TEC, use Analysis 1 results (already computed above, pass through)
    # We recompute lift here for direct comparison
    gnss_rand_z3_for_cmp = []
    if all_events and gnss_by_loc:
        t_min = all_events[0]["time"]
        t_max = all_events[-1]["time"]
        t_span = (t_max - t_min).total_seconds()
        grid_keys = list(gnss_by_loc.keys())
        rng2 = random.Random(42)
        for _ in range(500):
            rk = rng2.choice(grid_keys)
            rt = t_min + timedelta(seconds=rng2.random() * t_span)
            ts = gnss_by_loc[rk]
            pre3, bl3 = extract_tec_windows(ts, rt, pre_days=3, baseline_days=30)
            z = compute_zscore(pre3, bl3)
            if z is not None:
                gnss_rand_z3_for_cmp.append(z)

    gnss_rand_stats = stats(gnss_rand_z3_for_cmp, "gnss_random")

    if gnss_stats.get("n", 0) > 0 and gnss_rand_stats.get("n", 0) > 0:
        eq_pct = gnss_stats["abs_gt_2_pct"]
        rand_pct = gnss_rand_stats["abs_gt_2_pct"]
        gnss_lift = round(eq_pct / max(rand_pct, 0.1), 2) if rand_pct > 0.1 else None

    if ionex_stats.get("n", 0) > 0 and ionex_rand.get("n", 0) > 0:
        eq_pct = ionex_stats["abs_gt_2_pct"]
        rand_pct = ionex_rand["abs_gt_2_pct"]
        ionex_lift = round(eq_pct / max(rand_pct, 0.1), 2) if rand_pct > 0.1 else None

    resolution_gain = None
    if gnss_lift is not None and ionex_lift is not None and ionex_lift > 0:
        resolution_gain = round(gnss_lift / ionex_lift, 2)

    logger.info("    GNSS-TEC (0.5deg): n=%d |Z|>2=%.1f%% lift=%s",
                gnss_stats.get("n", 0), gnss_stats.get("abs_gt_2_pct", 0), gnss_lift)
    logger.info("    IONEX   (2.5x5):   n=%d |Z|>2=%.1f%% lift=%s",
                ionex_stats.get("n", 0), ionex_stats.get("abs_gt_2_pct", 0), ionex_lift)
    logger.info("    Resolution gain (GNSS lift / IONEX lift): %s", resolution_gain)

    return {
        "gnss_tec": gnss_stats,
        "gnss_random": gnss_rand_stats,
        "gnss_lift_absZ2": gnss_lift,
        "ionex": ionex_stats,
        "ionex_random": ionex_rand,
        "ionex_lift_absZ2": ionex_lift,
        "ionex_matched": ionex_matched,
        "resolution_gain": resolution_gain,
        "interpretation": (
            "resolution_gain > 1 means higher spatial resolution improves "
            "precursor detection; < 1 means resolution has no effect or worse"
        ),
    }


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def run_gnss_tec_analysis(min_mag: float = 5.0):
    """Run complete GNSS-TEC high-resolution analysis."""
    logger.info("=== GNSS-TEC High-Resolution Analysis (min_mag=%.1f) ===", min_mag)

    async with aiosqlite.connect(DB_PATH) as db:
        all_events, targets, gnss_by_loc, ionex_by_loc = await load_data(db, min_mag)

    logger.info("  Earthquakes: %d total, %d M%.1f+ targets (%d isolated)",
                len(all_events), len(targets), min_mag,
                sum(1 for e in targets if e["isolated"]))
    logger.info("  GNSS-TEC grid points: %d", len(gnss_by_loc))
    logger.info("  GNSS-TEC total records: %d",
                sum(len(v) for v in gnss_by_loc.values()))
    logger.info("  IONEX grid points: %d", len(ionex_by_loc))

    if not gnss_by_loc:
        logger.error("No GNSS-TEC data. Run fetch_gnss_tec.py first.")
        return {"error": "no_gnss_tec_data"}

    if not targets:
        logger.error("No M%.1f+ earthquakes found.", min_mag)
        return {"error": "no_target_earthquakes"}

    # Run all analyses
    a1 = await analysis_epicentral_zscore(targets, gnss_by_loc, all_events)
    a2 = await analysis_isolated(targets)
    a3 = await analysis_day_night(targets, gnss_by_loc)
    a4 = await analysis_prospective_alarm(targets, gnss_by_loc, all_events)
    a5 = await analysis_ionex_comparison(targets, gnss_by_loc, ionex_by_loc, all_events)

    # Collect per-event details (first 200 for JSON size)
    event_details = []
    for e in targets[:200]:
        detail = e.get("_detail")
        if detail:
            event_details.append(detail)

    results = {
        "metadata": {
            "min_mag": min_mag,
            "n_all_earthquakes": len(all_events),
            "n_targets": len(targets),
            "n_isolated": sum(1 for e in targets if e["isolated"]),
            "n_gnss_grid_points": len(gnss_by_loc),
            "n_gnss_records": sum(len(v) for v in gnss_by_loc.values()),
            "n_ionex_grid_points": len(ionex_by_loc),
            "n_ionex_records": sum(len(v) for v in ionex_by_loc.values()),
            "gnss_resolution_deg": 0.5,
            "ionex_resolution_deg": "2.5x5.0",
        },
        "analysis_1_epicentral_zscore": a1,
        "analysis_2_isolated_events": a2,
        "analysis_3_day_night": a3,
        "analysis_4_prospective_alarm": a4,
        "analysis_5_ionex_comparison": a5,
        "interpretation": {
            "key_questions": [
                "Does mean Z-score differ from 0? (bootstrap CI excluding 0 = signal)",
                "Is |Z|>2 rate higher for pre-EQ than random? (lift > 1 = signal)",
                "Does signal survive isolation filter? (yes = not aftershock artifact)",
                "Is nighttime signal stronger? (yes = consistent with LAIC)",
                "Does probability_gain > 1 for any threshold? (yes = predictive value)",
                "Does GNSS-TEC outperform IONEX? (resolution_gain > 1 = resolution matters)",
            ],
            "null_hypothesis": "TEC Z-scores before M5+ are indistinguishable from random",
            "laic_predictions": [
                "Positive TEC anomalies 1-7 days before earthquake",
                "Nighttime enhancement > daytime",
                "Signal survives aftershock isolation",
                "Higher resolution → stronger signal (less spatial dilution)",
            ],
        },
        "event_details": event_details,
    }

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_gnss_tec_analysis()

    out_path = RESULTS_DIR / f"gnss_tec_analysis_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
