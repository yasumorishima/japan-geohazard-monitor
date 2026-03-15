"""Run earthquake correlation analysis and output results.

Usage:
    python scripts/run_analysis.py --min-mag 5.0 --type all
"""

import argparse
import asyncio
import json
import logging
import math
import os
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


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def calc_b(mags: list[float]) -> float | None:
    if len(mags) < 20:
        return None
    m_min = min(mags)
    m_mean = sum(mags) / len(mags)
    d = m_mean - m_min
    if d < 0.01:
        return None
    return math.log10(math.e) / d


def pearson(x: list[float], y: list[float]) -> float | None:
    if len(x) != len(y) or len(x) < 10:
        return None
    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n
    cov = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / n
    sx = (sum((xi - mx) ** 2 for xi in x) / n) ** 0.5
    sy = (sum((yi - my) ** 2 for yi in y) / n) ** 0.5
    if sx < 1e-10 or sy < 1e-10:
        return None
    return cov / (sx * sy)


def percentile(values: list[float], p: float) -> float:
    """Compute the p-th percentile (0-100) of a sorted list."""
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    d = k - f
    return s[f] + d * (s[c] - s[f])


def filter_isolated(
    target_events: list[tuple],
    all_events_sorted: list[tuple],
    days: float = 3.0,
    degrees: float = 1.5,
) -> list[tuple]:
    """Remove aftershock-contaminated events.

    An event is 'isolated' if no event with magnitude >= (its mag - 0.5)
    occurred within `days` days and `degrees` degrees before it.
    all_events_sorted: list of (datetime, lat, lon, mag) sorted by time.
    target_events: list of (datetime, lat, lon, mag).
    """
    import bisect

    # Build time index for efficient lookup
    all_times = [e[0] for e in all_events_sorted]
    isolated = []

    for t, lat, lon, mag in target_events:
        t_min = t - timedelta(days=days)
        # Find events in [t_min, t) window
        idx_start = bisect.bisect_left(all_times, t_min)
        idx_end = bisect.bisect_left(all_times, t)

        is_iso = True
        for i in range(idx_start, idx_end):
            te, late, lone, mage = all_events_sorted[i]
            if te >= t:
                break
            if (
                abs(lat - late) <= degrees
                and abs(lon - lone) <= degrees
                and mage >= mag - 0.5
            ):
                is_iso = False
                break

        if is_iso:
            isolated.append((t, lat, lon, mag))

    return isolated


def depth_bin(depth_km: float | None) -> str:
    """Classify earthquake by depth."""
    if depth_km is None:
        return "unknown"
    if depth_km < 30:
        return "shallow_lt30km"
    if depth_km < 70:
        return "intermediate_30_70km"
    return "deep_gt70km"


# ---------------------------------------------------------------------------
# B-value analysis
# ---------------------------------------------------------------------------

async def analyze_bvalue(db: aiosqlite.Connection, min_mag: float) -> dict:
    """b-value analysis with control experiment and isolation filter."""
    logger.info("=== b-value analysis (min_mag=%.1f) ===", min_mag)

    all_eq = await db.execute_fetchall(
        "SELECT occurred_at, magnitude, latitude, longitude FROM earthquakes "
        "WHERE magnitude IS NOT NULL AND magnitude >= 3.0 ORDER BY occurred_at"
    )
    target_eq = await db.execute_fetchall(
        "SELECT occurred_at, magnitude, latitude, longitude FROM earthquakes "
        "WHERE magnitude >= ? ORDER BY occurred_at",
        (min_mag,),
    )

    events = []
    for r in all_eq:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events.append((t, r[1]))
        except (ValueError, TypeError):
            continue

    if len(events) < 100:
        return {"error": "Insufficient data", "n_events": len(events)}

    # Parse target events with lat/lon for isolation filter
    target_parsed = []
    for r in target_eq:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            target_parsed.append((t, r[2], r[3], r[1]))  # (time, lat, lon, mag)
        except (ValueError, TypeError):
            continue

    all_parsed = []
    for r in all_eq:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            all_parsed.append((t, r[2], r[3], r[1]))  # (time, lat, lon, mag)
        except (ValueError, TypeError):
            continue

    isolated_targets = filter_isolated(target_parsed, all_parsed)
    logger.info(
        "  Target events: %d total, %d isolated (%.0f%%)",
        len(target_parsed), len(isolated_targets),
        100 * len(isolated_targets) / max(len(target_parsed), 1),
    )

    t_min = events[0][0] + timedelta(days=91)
    t_max = events[-1][0]

    def get_b(target_time, window_days=90):
        ws = target_time - timedelta(days=window_days)
        mags = [m for t, m in events if ws <= t < target_time]
        return calc_b(mags)

    results = {}
    for window in [7, 14, 30, 60, 90, 180]:
        # Random baseline
        random.seed(42)
        rand_b = [b for b in (get_b(
            t_min + timedelta(seconds=random.randint(0, int((t_max - t_min).total_seconds()))),
            window
        ) for _ in range(1000)) if b is not None]

        # ALL target earthquakes (includes aftershocks)
        eq_b_all = [b for b in (get_b(t, window) for t, *_ in target_parsed[:500]) if b is not None]

        # ISOLATED target earthquakes only
        eq_b_isolated = [b for b in (get_b(t, window) for t, *_ in isolated_targets[:500]) if b is not None]

        # Magnitude-binned (all targets)
        mag_bins = {}
        for t, lat, lon, mag in target_parsed[:500]:
            b = get_b(t, window)
            if b is None:
                continue
            bin_label = f"M{int(mag)}"
            mag_bins.setdefault(bin_label, []).append(b)

        def summarize_b(vals):
            if not vals:
                return {"n": 0, "mean_b": None, "b_lt_07": None}
            return {
                "n": len(vals),
                "mean_b": round(sum(vals) / len(vals), 3),
                "b_lt_07": round(sum(1 for b in vals if b < 0.7) / len(vals) * 100, 1),
            }

        results[f"window_{window}d"] = {
            "random": summarize_b(rand_b),
            "pre_earthquake_all": summarize_b(eq_b_all),
            "pre_earthquake_isolated": summarize_b(eq_b_isolated),
            "by_magnitude": {
                label: summarize_b(vals)
                for label, vals in sorted(mag_bins.items())
            },
        }
        r_all = results[f"window_{window}d"]
        logger.info(
            "  %3dd: random b=%.3f (%d%% <0.7) | all b=%.3f (%d%% <0.7) | isolated b=%.3f (%d%% <0.7)",
            window,
            r_all["random"]["mean_b"] or 0, r_all["random"]["b_lt_07"] or 0,
            r_all["pre_earthquake_all"]["mean_b"] or 0, r_all["pre_earthquake_all"]["b_lt_07"] or 0,
            r_all["pre_earthquake_isolated"]["mean_b"] or 0, r_all["pre_earthquake_isolated"]["b_lt_07"] or 0,
        )

    return results


# ---------------------------------------------------------------------------
# TEC epicenter analysis
# ---------------------------------------------------------------------------

async def analyze_tec(db: aiosqlite.Connection, min_mag: float) -> dict:
    """Epicenter TEC analysis with control experiment."""
    logger.info("=== TEC epicenter analysis (min_mag=%.1f) ===", min_mag)

    eq_rows = await db.execute_fetchall(
        "SELECT occurred_at, latitude, longitude, magnitude FROM earthquakes "
        "WHERE magnitude >= ? ORDER BY occurred_at",
        (min_mag,),
    )
    tec_range = await db.execute_fetchall("SELECT MIN(epoch), MAX(epoch) FROM tec")
    tec_min = tec_range[0][0]
    tec_max = tec_range[0][1]

    if not tec_min or not tec_max:
        return {"error": "No TEC data"}

    eq_in_range = [(r[0], r[1], r[2], r[3]) for r in eq_rows if r[0] >= tec_min and r[0] <= tec_max]

    async def get_sigma(lat, lon, time_str, radius=5.0):
        rows = await db.execute_fetchall(
            "SELECT epoch, AVG(tec_tecu) FROM tec "
            "WHERE ABS(latitude - ?) <= ? AND ABS(longitude - ?) <= ? "
            "AND epoch BETWEEN datetime(?, '-168 hours') AND datetime(?, '+24 hours') "
            "GROUP BY epoch ORDER BY epoch",
            (lat, radius, lon, radius, time_str, time_str),
        )
        if len(rows) < 6:
            return None
        values = [r[1] for r in rows]
        if len(values) <= 8:
            return None
        baseline = values[:-8]
        precursor = values[-8:]
        b_mean = sum(baseline) / len(baseline)
        b_std = (sum((v - b_mean) ** 2 for v in baseline) / len(baseline)) ** 0.5
        if b_std < 0.1:
            return None
        p_mean = sum(precursor) / len(precursor)
        return (p_mean - b_mean) / b_std

    # Earthquake TEC
    eq_sigmas = []
    for time_str, lat, lon, mag in eq_in_range[:500]:
        s = await get_sigma(lat, lon, time_str)
        if s is not None:
            eq_sigmas.append({"mag": mag, "sigma": round(s, 3)})

    # Random control
    random.seed(42)
    rand_sigmas = []
    t_min_dt = datetime.fromisoformat(tec_min.replace("Z", "+00:00")) + timedelta(days=8)
    t_max_dt = datetime.fromisoformat(tec_max.replace("Z", "+00:00"))
    for _ in range(500):
        rt = t_min_dt + timedelta(seconds=random.randint(0, int((t_max_dt - t_min_dt).total_seconds())))
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25
        s = await get_sigma(rlat, rlon, rt.isoformat())
        if s is not None:
            rand_sigmas.append(round(s, 3))

    def summarize(vals):
        if not vals:
            return {"n": 0}
        v = [x["sigma"] if isinstance(x, dict) else x for x in vals]
        return {
            "n": len(v),
            "mean_sigma": round(sum(v) / len(v), 3),
            "negative_pct": round(sum(1 for x in v if x < 0) / len(v) * 100, 1),
            "drops_pct": round(sum(1 for x in v if x < -1) / len(v) * 100, 1),
            "spikes_pct": round(sum(1 for x in v if x > 1) / len(v) * 100, 1),
        }

    result = {
        "random": summarize(rand_sigmas),
        "pre_earthquake": summarize(eq_sigmas),
    }
    logger.info(
        "  Random: n=%d, mean_sigma=%.3f | Pre-EQ: n=%d, mean_sigma=%.3f",
        result["random"]["n"], result["random"].get("mean_sigma", 0),
        result["pre_earthquake"]["n"], result["pre_earthquake"].get("mean_sigma", 0),
    )
    return result


# ---------------------------------------------------------------------------
# Multi-indicator analysis (complete redesign)
# ---------------------------------------------------------------------------

async def analyze_multi(db: aiosqlite.Connection, min_mag: float) -> dict:
    """Multi-indicator analysis with isolation filter, adaptive thresholds,
    grid search, and depth-based spatial binning.

    For each event/random point, computes:
      - b-value (90-day window, M3+ events)
      - Kp 48h average
      - TEC sigma (7-day baseline vs 24h precursor)

    Then tests multiple threshold combinations to find which gives
    the highest lift (pre-earthquake anomaly rate / random anomaly rate).
    """
    logger.info("=== Multi-indicator analysis (min_mag=%.1f) ===", min_mag)

    # ---------------------------------------------------------------
    # 1. Load data
    # ---------------------------------------------------------------
    eq_rows = await db.execute_fetchall(
        "SELECT occurred_at, latitude, longitude, magnitude, depth_km "
        "FROM earthquakes WHERE magnitude >= ? ORDER BY occurred_at",
        (min_mag,),
    )

    all_eq = await db.execute_fetchall(
        "SELECT occurred_at, magnitude, latitude, longitude FROM earthquakes "
        "WHERE magnitude IS NOT NULL AND magnitude >= 3.0 ORDER BY occurred_at"
    )

    events_all = []
    all_parsed = []
    for r in all_eq:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events_all.append((t, r[1]))
            all_parsed.append((t, r[2], r[3], r[1]))
        except (ValueError, TypeError):
            continue

    if len(events_all) < 100:
        return {"error": "Insufficient data"}

    # Parse target events
    target_events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            target_events.append({
                "time": t,
                "time_str": r[0],
                "lat": r[1],
                "lon": r[2],
                "mag": r[3],
                "depth": r[4],
            })
        except (ValueError, TypeError):
            continue

    # Isolation filter
    target_tuples = [(e["time"], e["lat"], e["lon"], e["mag"]) for e in target_events]
    isolated_set = set(
        (t.isoformat(), lat, lon)
        for t, lat, lon, mag in filter_isolated(target_tuples, all_parsed)
    )

    for e in target_events:
        e["isolated"] = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set

    n_isolated = sum(1 for e in target_events if e["isolated"])
    logger.info(
        "  Target events: %d total, %d isolated (%.0f%%)",
        len(target_events), n_isolated,
        100 * n_isolated / max(len(target_events), 1),
    )

    # ---------------------------------------------------------------
    # 2. Compute indicators for each event
    # ---------------------------------------------------------------
    def get_b(target_time, window_days=90):
        ws = target_time - timedelta(days=window_days)
        mags = [m for t, m in events_all if ws <= t < target_time]
        return calc_b(mags)

    async def get_kp_before(time_str, hours=48):
        # Normalize time_str: replace 'T' with ' ' and remove 'Z'/timezone
        normalized = time_str.replace("T", " ").replace("Z", "")
        if "+" in normalized:
            normalized = normalized.split("+")[0]
        rows = await db.execute_fetchall(
            "SELECT AVG(kp) FROM geomag_kp "
            "WHERE time_tag BETWEEN datetime(?, ? || ' hours') AND datetime(?)",
            (normalized, f"-{hours}", normalized),
        )
        return rows[0][0] if rows and rows[0][0] is not None else None

    async def get_tec_sigma(lat, lon, time_str, radius=5.0):
        normalized = time_str.replace("T", " ").replace("Z", "")
        if "+" in normalized:
            normalized = normalized.split("+")[0]
        rows = await db.execute_fetchall(
            "SELECT epoch, AVG(tec_tecu) FROM tec "
            "WHERE ABS(latitude - ?) <= ? AND ABS(longitude - ?) <= ? "
            "AND epoch BETWEEN datetime(?, '-168 hours') AND datetime(?, '+24 hours') "
            "GROUP BY epoch ORDER BY epoch",
            (lat, radius, lon, radius, normalized, normalized),
        )
        if len(rows) < 6:
            return None
        values = [r[1] for r in rows]
        if len(values) <= 8:
            return None
        baseline = values[:-8]
        precursor = values[-8:]
        b_mean = sum(baseline) / len(baseline)
        b_std = (sum((v - b_mean) ** 2 for v in baseline) / len(baseline)) ** 0.5
        if b_std < 0.1:
            return None
        p_mean = sum(precursor) / len(precursor)
        return (p_mean - b_mean) / b_std

    # Process ALL target events (no cap)
    event_profiles = []
    for i, e in enumerate(target_events):
        b = get_b(e["time"])
        kp = await get_kp_before(e["time_str"])
        tec_s = await get_tec_sigma(e["lat"], e["lon"], e["time_str"])

        event_profiles.append({
            "time": e["time_str"][:16],
            "mag": e["mag"],
            "depth": e["depth"],
            "depth_bin": depth_bin(e["depth"]),
            "isolated": e["isolated"],
            "b_value": round(b, 3) if b is not None else None,
            "kp_48h_avg": round(kp, 2) if kp is not None else None,
            "tec_sigma": round(tec_s, 2) if tec_s is not None else None,
        })

        if (i + 1) % 100 == 0:
            logger.info("  Processed %d/%d target events", i + 1, len(target_events))

    logger.info("  All %d target events processed", len(event_profiles))

    # ---------------------------------------------------------------
    # 3. Compute indicators for random baseline
    # ---------------------------------------------------------------
    random.seed(42)
    t_min = events_all[0][0] + timedelta(days=91)
    t_max = events_all[-1][0]

    random_profiles = []
    n_random = min(500, len(event_profiles) * 2)  # At least 2x events
    for i in range(n_random):
        rt = t_min + timedelta(seconds=random.randint(0, int((t_max - t_min).total_seconds())))
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25

        b = get_b(rt)
        kp = await get_kp_before(rt.isoformat())
        tec_s = await get_tec_sigma(rlat, rlon, rt.isoformat())

        random_profiles.append({
            "b_value": round(b, 3) if b is not None else None,
            "kp_48h_avg": round(kp, 2) if kp is not None else None,
            "tec_sigma": round(tec_s, 2) if tec_s is not None else None,
        })

        if (i + 1) % 100 == 0:
            logger.info("  Processed %d/%d random points", i + 1, n_random)

    logger.info("  All %d random points processed", len(random_profiles))

    # ---------------------------------------------------------------
    # 4. Indicator availability
    # ---------------------------------------------------------------
    def availability(profiles, key):
        n = len(profiles)
        avail = sum(1 for p in profiles if p[key] is not None)
        return {"available": avail, "total": n, "pct": round(100 * avail / max(n, 1), 1)}

    indicator_availability = {
        "pre_earthquake": {
            "b_value": availability(event_profiles, "b_value"),
            "kp": availability(event_profiles, "kp_48h_avg"),
            "tec": availability(event_profiles, "tec_sigma"),
        },
        "random": {
            "b_value": availability(random_profiles, "b_value"),
            "kp": availability(random_profiles, "kp_48h_avg"),
            "tec": availability(random_profiles, "tec_sigma"),
        },
    }
    for group, avails in indicator_availability.items():
        for ind, a in avails.items():
            logger.info("  %s %s: %d/%d (%.0f%%)", group, ind, a["available"], a["total"], a["pct"])

    # ---------------------------------------------------------------
    # 5. Baseline statistics (from random)
    # ---------------------------------------------------------------
    def baseline_stats(profiles, key):
        vals = [p[key] for p in profiles if p[key] is not None]
        if not vals:
            return {"n": 0}
        return {
            "n": len(vals),
            "mean": round(sum(vals) / len(vals), 3),
            "p10": round(percentile(vals, 10), 3),
            "p25": round(percentile(vals, 25), 3),
            "p50": round(percentile(vals, 50), 3),
            "p75": round(percentile(vals, 75), 3),
            "p90": round(percentile(vals, 90), 3),
        }

    baseline = {
        "b_value": baseline_stats(random_profiles, "b_value"),
        "kp": baseline_stats(random_profiles, "kp_48h_avg"),
        "tec": baseline_stats(random_profiles, "tec_sigma"),
    }
    for ind, s in baseline.items():
        if s["n"] > 0:
            logger.info("  Baseline %s: mean=%.3f p10=%.3f p50=%.3f p90=%.3f", ind, s["mean"], s["p10"], s["p50"], s["p90"])

    # ---------------------------------------------------------------
    # 6. Grid search over threshold combinations
    # ---------------------------------------------------------------
    b_thresholds = [0.4, 0.5, 0.6, 0.7, 0.8]
    kp_thresholds = [1.5, 2.0, 2.5, 3.0, 4.0]
    tec_thresholds = [-0.5, -1.0, -1.5, -2.0]

    def count_anomalies(profile, b_thresh, kp_thresh, tec_thresh):
        """Count how many indicators are anomalous for a profile."""
        count = 0
        if profile["b_value"] is not None and profile["b_value"] < b_thresh:
            count += 1
        if profile["kp_48h_avg"] is not None and profile["kp_48h_avg"] > kp_thresh:
            count += 1
        if profile["tec_sigma"] is not None and profile["tec_sigma"] < tec_thresh:
            count += 1
        return count

    def anomaly_rate(profiles, b_t, kp_t, tec_t, min_anomalies=2):
        """Fraction of profiles with >= min_anomalies simultaneous anomalies."""
        if not profiles:
            return 0.0
        n = sum(1 for p in profiles if count_anomalies(p, b_t, kp_t, tec_t) >= min_anomalies)
        return n / len(profiles)

    # Separate isolated events
    isolated_profiles = [p for p in event_profiles if p["isolated"]]

    grid_results = []
    for bt in b_thresholds:
        for kt in kp_thresholds:
            for tt in tec_thresholds:
                eq_rate_all = anomaly_rate(event_profiles, bt, kt, tt, min_anomalies=2)
                eq_rate_iso = anomaly_rate(isolated_profiles, bt, kt, tt, min_anomalies=2)
                rand_rate = anomaly_rate(random_profiles, bt, kt, tt, min_anomalies=2)
                lift_all = eq_rate_all / max(rand_rate, 0.001)
                lift_iso = eq_rate_iso / max(rand_rate, 0.001)

                # Also check single anomaly (1+)
                eq_rate_1p = anomaly_rate(event_profiles, bt, kt, tt, min_anomalies=1)
                rand_rate_1p = anomaly_rate(random_profiles, bt, kt, tt, min_anomalies=1)
                lift_1p = eq_rate_1p / max(rand_rate_1p, 0.001)

                grid_results.append({
                    "b_thresh": bt,
                    "kp_thresh": kt,
                    "tec_thresh": tt,
                    "eq_rate_2plus_all": round(eq_rate_all * 100, 1),
                    "eq_rate_2plus_isolated": round(eq_rate_iso * 100, 1),
                    "random_rate_2plus": round(rand_rate * 100, 1),
                    "lift_2plus_all": round(lift_all, 2),
                    "lift_2plus_isolated": round(lift_iso, 2),
                    "eq_rate_1plus": round(eq_rate_1p * 100, 1),
                    "random_rate_1plus": round(rand_rate_1p * 100, 1),
                    "lift_1plus": round(lift_1p, 2),
                })

    # Sort by lift (isolated 2+)
    grid_results.sort(key=lambda x: x["lift_2plus_isolated"], reverse=True)

    # Top 10 combos
    top_combos = grid_results[:10]
    for i, c in enumerate(top_combos[:3]):
        logger.info(
            "  Top %d: b<%.1f kp>%.1f tec<%.1f → eq_iso=%.1f%% rand=%.1f%% lift=%.2f",
            i + 1, c["b_thresh"], c["kp_thresh"], c["tec_thresh"],
            c["eq_rate_2plus_isolated"], c["random_rate_2plus"], c["lift_2plus_isolated"],
        )

    # ---------------------------------------------------------------
    # 7. Fixed threshold analysis (for comparison with previous runs)
    # ---------------------------------------------------------------
    def analyze_with_thresholds(profiles, bt, kt, tt):
        dist = {0: 0, 1: 0, 2: 0, 3: 0}
        for p in profiles:
            a = count_anomalies(p, bt, kt, tt)
            dist[min(a, 3)] += 1
        n = len(profiles) or 1
        return {k: {"count": v, "pct": round(v / n * 100, 1)} for k, v in dist.items()}

    fixed_thresholds = {
        "b0.7_kp4.0_tec-1.0": {"b": 0.7, "kp": 4.0, "tec": -1.0},
        "b0.7_kp2.5_tec-1.0": {"b": 0.7, "kp": 2.5, "tec": -1.0},
        "b0.6_kp3.0_tec-1.0": {"b": 0.6, "kp": 3.0, "tec": -1.0},
    }
    fixed_results = {}
    for name, t in fixed_thresholds.items():
        fixed_results[name] = {
            "pre_earthquake_all": analyze_with_thresholds(event_profiles, t["b"], t["kp"], t["tec"]),
            "pre_earthquake_isolated": analyze_with_thresholds(isolated_profiles, t["b"], t["kp"], t["tec"]),
            "random": analyze_with_thresholds(random_profiles, t["b"], t["kp"], t["tec"]),
        }

    # ---------------------------------------------------------------
    # 8. Analysis by depth bin
    # ---------------------------------------------------------------
    depth_bins_eq = {}
    for p in event_profiles:
        db_name = p["depth_bin"]
        depth_bins_eq.setdefault(db_name, []).append(p)

    by_depth = {}
    for db_name, profiles in sorted(depth_bins_eq.items()):
        iso = [p for p in profiles if p["isolated"]]
        by_depth[db_name] = {
            "n_total": len(profiles),
            "n_isolated": len(iso),
            "b_value_stats": baseline_stats(profiles, "b_value"),
            "kp_stats": baseline_stats(profiles, "kp_48h_avg"),
            "tec_stats": baseline_stats(profiles, "tec_sigma"),
        }
        logger.info(
            "  Depth %s: n=%d (isolated=%d), b_mean=%.3f, kp_mean=%.3f",
            db_name, len(profiles), len(iso),
            by_depth[db_name]["b_value_stats"].get("mean", 0),
            by_depth[db_name]["kp_stats"].get("mean", 0),
        )

    # ---------------------------------------------------------------
    # 9. Per-indicator single analysis (is each indicator useful alone?)
    # ---------------------------------------------------------------
    def single_indicator_analysis(eq_profiles, rand_profiles, key, direction="low"):
        """Compare single indicator distribution between eq and random."""
        eq_vals = [p[key] for p in eq_profiles if p[key] is not None]
        rand_vals = [p[key] for p in rand_profiles if p[key] is not None]
        if not eq_vals or not rand_vals:
            return {"error": "insufficient data"}

        eq_mean = sum(eq_vals) / len(eq_vals)
        rand_mean = sum(rand_vals) / len(rand_vals)

        thresholds = {}
        for pct in [10, 20, 30]:
            if direction == "low":
                thresh = percentile(rand_vals, pct)
                eq_below = sum(1 for v in eq_vals if v < thresh) / len(eq_vals)
                rand_below = pct / 100.0
            else:
                thresh = percentile(rand_vals, 100 - pct)
                eq_below = sum(1 for v in eq_vals if v > thresh) / len(eq_vals)
                rand_below = pct / 100.0

            lift = eq_below / max(rand_below, 0.001)
            thresholds[f"p{pct}"] = {
                "threshold": round(thresh, 3),
                "eq_rate": round(eq_below * 100, 1),
                "random_rate": round(rand_below * 100, 1),
                "lift": round(lift, 2),
            }

        return {
            "n_eq": len(eq_vals),
            "n_random": len(rand_vals),
            "eq_mean": round(eq_mean, 3),
            "random_mean": round(rand_mean, 3),
            "thresholds": thresholds,
        }

    single_indicator = {
        "b_value": single_indicator_analysis(event_profiles, random_profiles, "b_value", "low"),
        "b_value_isolated": single_indicator_analysis(isolated_profiles, random_profiles, "b_value", "low"),
        "kp": single_indicator_analysis(event_profiles, random_profiles, "kp_48h_avg", "high"),
        "tec": single_indicator_analysis(event_profiles, random_profiles, "tec_sigma", "low"),
    }

    # ---------------------------------------------------------------
    # 10. Assemble result
    # ---------------------------------------------------------------
    result = {
        "n_events_total": len(event_profiles),
        "n_events_isolated": len(isolated_profiles),
        "n_random": len(random_profiles),
        "indicator_availability": indicator_availability,
        "baseline_statistics": baseline,
        "single_indicator": single_indicator,
        "fixed_threshold_analysis": fixed_results,
        "grid_search": {
            "n_combinations": len(grid_results),
            "top_10": top_combos,
        },
        "by_depth": by_depth,
        "sample_events": event_profiles[:20],
    }

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-mag", type=float, default=5.0)
    parser.add_argument("--type", choices=["all", "bvalue", "tec", "lag", "multi"], default="all")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    async with aiosqlite.connect(DB_PATH) as db:
        # DB stats
        eq_count = (await db.execute_fetchall("SELECT COUNT(*) FROM earthquakes"))[0][0]
        tec_count = (await db.execute_fetchall("SELECT COUNT(*) FROM tec"))[0][0]
        kp_count = (await db.execute_fetchall("SELECT COUNT(*) FROM geomag_kp"))[0][0]
        logger.info("DB: %d earthquakes, %d TEC, %d Kp", eq_count, tec_count, kp_count)

        results = {
            "timestamp": timestamp,
            "min_mag": args.min_mag,
            "db_stats": {"earthquakes": eq_count, "tec": tec_count, "kp": kp_count},
        }

        if args.type in ("all", "bvalue"):
            results["bvalue"] = await analyze_bvalue(db, args.min_mag)

        if args.type in ("all", "tec"):
            results["tec"] = await analyze_tec(db, args.min_mag)

        if args.type in ("all", "multi"):
            results["multi"] = await analyze_multi(db, args.min_mag)

    out_path = RESULTS_DIR / f"analysis_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
