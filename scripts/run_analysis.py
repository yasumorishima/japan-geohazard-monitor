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
from db_connect import safe_connect

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
# Advanced analysis: TEC detrending, Kp profile, multi-radius, MI
# ---------------------------------------------------------------------------

def mutual_information(x_bins: list, y_bins: list) -> float:
    """Compute mutual information between two discretized variables (bits)."""
    from collections import Counter

    n = len(x_bins)
    if n == 0:
        return 0.0

    joint = Counter(zip(x_bins, y_bins))
    x_counts = Counter(x_bins)
    y_counts = Counter(y_bins)

    mi = 0.0
    for (x, y), count in joint.items():
        p_xy = count / n
        p_x = x_counts[x] / n
        p_y = y_counts[y] / n
        if p_xy > 0 and p_x > 0 and p_y > 0:
            mi += p_xy * math.log2(p_xy / (p_x * p_y))
    return mi


def normalize_time_str(time_str: str) -> str:
    """Normalize ISO time string for SQLite datetime()."""
    normalized = time_str.replace("T", " ").replace("Z", "")
    if "+" in normalized:
        normalized = normalized.split("+")[0]
    return normalized


def sample_events_balanced(events: list, n: int = 500, seed: int = 42) -> list:
    """Systematic sample of n events with uniform time coverage.

    Avoids [:n] bias which over-samples early events
    (e.g., 2011 Tohoku aftershock cluster dominates first 500).
    """
    if len(events) <= n:
        return list(events)
    sorted_events = sorted(events, key=lambda e: e["time"])
    step = len(sorted_events) / n
    return [sorted_events[int(i * step)] for i in range(n)]


async def analyze_advanced(db: aiosqlite.Connection, min_mag: float) -> dict:
    """Advanced analyses beyond basic threshold comparison.

    1. TEC with seasonal/diurnal detrending
    2. Kp temporal profile at multiple lead times
    3. TEC at multiple epicenter radii (1°, 2°, 5°, 10°)
    4. Mutual Information between daily indicators and earthquake occurrence
    """
    logger.info("=== Advanced analysis (min_mag=%.1f) ===", min_mag)

    # ---------------------------------------------------------------
    # Shared data loading
    # ---------------------------------------------------------------
    eq_rows = await db.execute_fetchall(
        "SELECT occurred_at, latitude, longitude, magnitude FROM earthquakes "
        "WHERE magnitude >= ? ORDER BY occurred_at",
        (min_mag,),
    )

    target_events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            target_events.append({"time": t, "time_str": r[0], "lat": r[1], "lon": r[2], "mag": r[3]})
        except (ValueError, TypeError):
            continue

    # Random baseline dates
    random.seed(123)  # Different seed from multi for independence
    all_eq = await db.execute_fetchall(
        "SELECT occurred_at, magnitude FROM earthquakes "
        "WHERE magnitude >= 3.0 ORDER BY occurred_at"
    )
    events_all = []
    for r in all_eq:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events_all.append((t, r[1]))
        except (ValueError, TypeError):
            continue

    t_min = events_all[0][0] + timedelta(days=91)
    t_max = events_all[-1][0]

    random_points = []
    for _ in range(500):
        rt = t_min + timedelta(seconds=random.randint(0, int((t_max - t_min).total_seconds())))
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25
        random_points.append({"time": rt, "time_str": rt.isoformat(), "lat": rlat, "lon": rlon})

    # ---------------------------------------------------------------
    # Pre-compute isolation filter (used by multiple sections)
    # ---------------------------------------------------------------
    all_eq_with_loc = await db.execute_fetchall(
        "SELECT occurred_at, latitude, longitude, magnitude FROM earthquakes "
        "WHERE magnitude >= 3.0 ORDER BY occurred_at"
    )
    all_parsed_adv = []
    for r in all_eq_with_loc:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            all_parsed_adv.append((t, r[1], r[2], r[3]))
        except (ValueError, TypeError):
            continue

    target_tuples_adv = [
        (e["time"], e["lat"], e["lon"], e["mag"]) for e in target_events
    ]
    isolated_set_adv = set(
        (t.isoformat(), lat, lon)
        for t, lat, lon, mag in filter_isolated(target_tuples_adv, all_parsed_adv)
    )
    isolated_events = [
        e for e in target_events
        if (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set_adv
    ]
    logger.info(
        "  Pre-computed isolation: %d / %d events are isolated (%.0f%%)",
        len(isolated_events), len(target_events),
        100 * len(isolated_events) / max(len(target_events), 1),
    )

    # Balanced sample across full time range (avoid 2011 Tohoku bias)
    sampled_events = sample_events_balanced(target_events, 500)
    sampled_isolated = [
        e for e in sampled_events
        if (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set_adv
    ]
    logger.info(
        "  Balanced sample: %d events (%d isolated), range: %s to %s",
        len(sampled_events), len(sampled_isolated),
        sampled_events[0]["time_str"][:10] if sampled_events else "?",
        sampled_events[-1]["time_str"][:10] if sampled_events else "?",
    )

    results = {}

    # ---------------------------------------------------------------
    # 1. TEC with seasonal/diurnal detrending
    # ---------------------------------------------------------------
    logger.info("  --- TEC detrended analysis ---")

    # Build monthly-hourly climatology (global Japan average)
    clim_rows = await db.execute_fetchall(
        "SELECT CAST(strftime('%m', epoch) AS INTEGER), "
        "CAST(strftime('%H', epoch) AS INTEGER), "
        "AVG(tec_tecu), COUNT(*) "
        "FROM tec GROUP BY 1, 2"
    )
    monthly_hourly_mean = {}
    for month, hour, mean_tec, n in clim_rows:
        monthly_hourly_mean[(month, hour)] = mean_tec
    logger.info("    Climatology: %d month-hour bins", len(monthly_hourly_mean))

    async def get_sigma_detrended(lat, lon, time_str, radius=5.0):
        """Compute TEC sigma after removing seasonal/diurnal climatology."""
        norm = normalize_time_str(time_str)
        rows = await db.execute_fetchall(
            "SELECT epoch, AVG(tec_tecu) FROM tec "
            "WHERE ABS(latitude - ?) <= ? AND ABS(longitude - ?) <= ? "
            "AND epoch BETWEEN datetime(?, '-168 hours') AND datetime(?, '+24 hours') "
            "GROUP BY epoch ORDER BY epoch",
            (lat, radius, lon, radius, norm, norm),
        )
        if len(rows) < 6:
            return None

        # Detrend each epoch by month+hour climatology
        detrended = []
        for epoch_str, avg_tec in rows:
            try:
                month = int(epoch_str[5:7])
                hour = int(epoch_str[11:13])
                clim = monthly_hourly_mean.get((month, hour), avg_tec)
                detrended.append(avg_tec - clim)
            except (ValueError, TypeError, IndexError):
                detrended.append(avg_tec)

        if len(detrended) <= 8:
            return None
        baseline = detrended[:-8]
        precursor = detrended[-8:]
        b_mean = sum(baseline) / len(baseline)
        b_std = (sum((v - b_mean) ** 2 for v in baseline) / len(baseline)) ** 0.5
        if b_std < 0.01:
            return None
        p_mean = sum(precursor) / len(precursor)
        return (p_mean - b_mean) / b_std

    # Earthquake TEC (detrended) — balanced sample with isolation tracking
    eq_sigmas_dt = []
    eq_sigmas_dt_isolated = []
    eq_sigmas_dt_non_isolated = []
    for e in sampled_events:
        s = await get_sigma_detrended(e["lat"], e["lon"], e["time_str"])
        is_iso = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set_adv
        if s is not None:
            s_r = round(s, 3)
            eq_sigmas_dt.append(s_r)
            if is_iso:
                eq_sigmas_dt_isolated.append(s_r)
            else:
                eq_sigmas_dt_non_isolated.append(s_r)

    # Random TEC (detrended)
    rand_sigmas_dt = []
    for r in random_points:
        s = await get_sigma_detrended(r["lat"], r["lon"], r["time_str"])
        if s is not None:
            rand_sigmas_dt.append(round(s, 3))

    def tec_summary(vals):
        if not vals:
            return {"n": 0}
        return {
            "n": len(vals),
            "mean_sigma": round(sum(vals) / len(vals), 3),
            "negative_pct": round(sum(1 for x in vals if x < 0) / len(vals) * 100, 1),
            "drops_pct": round(sum(1 for x in vals if x < -1) / len(vals) * 100, 1),
            "spikes_pct": round(sum(1 for x in vals if x > 1) / len(vals) * 100, 1),
        }

    # Bootstrap CI for key metric (mean_sigma difference: eq vs random)
    def bootstrap_ci(eq_vals, rand_vals, n_boot=1000, ci=95):
        """Bootstrap confidence interval for mean difference."""
        if len(eq_vals) < 10 or len(rand_vals) < 10:
            return {"error": "insufficient data"}
        rng = random.Random(777)
        diffs = []
        for _ in range(n_boot):
            eq_sample = [rng.choice(eq_vals) for _ in range(len(eq_vals))]
            rand_sample = [rng.choice(rand_vals) for _ in range(len(rand_vals))]
            diffs.append(sum(eq_sample) / len(eq_sample) - sum(rand_sample) / len(rand_sample))
        diffs.sort()
        lo = (100 - ci) / 2
        hi = 100 - lo
        return {
            "n_bootstrap": n_boot,
            "mean_diff": round(sum(diffs) / len(diffs), 3),
            f"ci_{ci}_lower": round(percentile(diffs, lo), 3),
            f"ci_{ci}_upper": round(percentile(diffs, hi), 3),
            "p_value_approx": round(sum(1 for d in diffs if d <= 0) / len(diffs), 4),
        }

    results["tec_detrended"] = {
        "random": tec_summary(rand_sigmas_dt),
        "pre_earthquake_all": tec_summary(eq_sigmas_dt),
        "pre_earthquake_isolated": tec_summary(eq_sigmas_dt_isolated),
        "pre_earthquake_non_isolated": tec_summary(eq_sigmas_dt_non_isolated),
        "bootstrap_ci_all_vs_random": bootstrap_ci(eq_sigmas_dt, rand_sigmas_dt),
        "bootstrap_ci_isolated_vs_random": bootstrap_ci(eq_sigmas_dt_isolated, rand_sigmas_dt),
    }
    logger.info(
        "    Detrended TEC — Random: n=%d, σ=%.3f | All: n=%d, σ=%.3f | Isolated: n=%d, σ=%.3f | Non-iso: n=%d, σ=%.3f",
        len(rand_sigmas_dt), sum(rand_sigmas_dt) / max(len(rand_sigmas_dt), 1),
        len(eq_sigmas_dt), sum(eq_sigmas_dt) / max(len(eq_sigmas_dt), 1),
        len(eq_sigmas_dt_isolated), sum(eq_sigmas_dt_isolated) / max(len(eq_sigmas_dt_isolated), 1),
        len(eq_sigmas_dt_non_isolated), sum(eq_sigmas_dt_non_isolated) / max(len(eq_sigmas_dt_non_isolated), 1),
    )

    # ---------------------------------------------------------------
    # 2. Kp temporal profile at multiple lead times
    # ---------------------------------------------------------------
    logger.info("  --- Kp temporal profile ---")

    lead_hours = [168, 120, 72, 48, 24, 12, 6, 3]  # Hours before earthquake

    async def get_kp_at(time_str, lead_h):
        """Get mean Kp in a 6h window centered at (event_time - lead_h)."""
        norm = normalize_time_str(time_str)
        rows = await db.execute_fetchall(
            "SELECT AVG(kp) FROM geomag_kp "
            "WHERE time_tag BETWEEN datetime(?, ? || ' hours') AND datetime(?, ? || ' hours')",
            (norm, f"-{lead_h + 3}", norm, f"-{lead_h - 3}"),
        )
        return rows[0][0] if rows and rows[0][0] is not None else None

    # Earthquake Kp profile (balanced sample)
    eq_profiles_kp = {h: [] for h in lead_hours}
    for e in sampled_events:
        for h in lead_hours:
            kp = await get_kp_at(e["time_str"], h)
            if kp is not None:
                eq_profiles_kp[h].append(kp)

    # Random Kp profile
    rand_profiles_kp = {h: [] for h in lead_hours}
    for r in random_points[:500]:
        for h in lead_hours:
            kp = await get_kp_at(r["time_str"], h)
            if kp is not None:
                rand_profiles_kp[h].append(kp)

    kp_profile_result = {}
    for h in lead_hours:
        eq_vals = eq_profiles_kp[h]
        rand_vals = rand_profiles_kp[h]
        eq_mean = sum(eq_vals) / len(eq_vals) if eq_vals else None
        rand_mean = sum(rand_vals) / len(rand_vals) if rand_vals else None
        kp_profile_result[f"-{h}h"] = {
            "eq_n": len(eq_vals),
            "eq_mean_kp": round(eq_mean, 3) if eq_mean else None,
            "rand_n": len(rand_vals),
            "rand_mean_kp": round(rand_mean, 3) if rand_mean else None,
            "diff": round(eq_mean - rand_mean, 3) if eq_mean and rand_mean else None,
            "eq_high_pct": round(sum(1 for v in eq_vals if v > 3) / max(len(eq_vals), 1) * 100, 1),
            "rand_high_pct": round(sum(1 for v in rand_vals if v > 3) / max(len(rand_vals), 1) * 100, 1),
        }
        if eq_mean and rand_mean:
            logger.info(
                "    Kp at %4dh before: eq=%.2f rand=%.2f diff=%+.3f",
                h, eq_mean, rand_mean, eq_mean - rand_mean,
            )

    results["kp_temporal_profile"] = kp_profile_result

    # ---------------------------------------------------------------
    # 3. TEC at multiple epicenter radii
    # ---------------------------------------------------------------
    logger.info("  --- TEC multi-radius ---")

    async def get_sigma_radius(lat, lon, time_str, radius):
        norm = normalize_time_str(time_str)
        rows = await db.execute_fetchall(
            "SELECT epoch, AVG(tec_tecu) FROM tec "
            "WHERE ABS(latitude - ?) <= ? AND ABS(longitude - ?) <= ? "
            "AND epoch BETWEEN datetime(?, '-168 hours') AND datetime(?, '+24 hours') "
            "GROUP BY epoch ORDER BY epoch",
            (lat, radius, lon, radius, norm, norm),
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

    radii = [1.0, 2.0, 5.0, 10.0]
    radius_results = {}
    sampled_200 = sample_events_balanced(target_events, 200, seed=99)
    for radius in radii:
        eq_s = []
        for e in sampled_200:
            s = await get_sigma_radius(e["lat"], e["lon"], e["time_str"], radius)
            if s is not None:
                eq_s.append(s)

        rand_s = []
        for r in random_points[:200]:
            s = await get_sigma_radius(r["lat"], r["lon"], r["time_str"], radius)
            if s is not None:
                rand_s.append(s)

        radius_results[f"{radius}deg"] = {
            "eq": tec_summary(eq_s),
            "random": tec_summary(rand_s),
        }
        logger.info(
            "    Radius %.0f°: eq n=%d mean_σ=%.3f | rand n=%d mean_σ=%.3f",
            radius,
            len(eq_s), sum(eq_s) / max(len(eq_s), 1),
            len(rand_s), sum(rand_s) / max(len(rand_s), 1),
        )

    results["tec_multi_radius"] = radius_results

    # ---------------------------------------------------------------
    # 4. Mutual Information: daily Kp/TEC vs earthquake occurrence
    # ---------------------------------------------------------------
    logger.info("  --- Mutual Information ---")

    # Build daily earthquake count
    daily_eq = await db.execute_fetchall(
        "SELECT DATE(occurred_at), COUNT(*) FROM earthquakes "
        "WHERE magnitude >= ? GROUP BY DATE(occurred_at)",
        (min_mag,),
    )
    eq_by_day = {r[0]: r[1] for r in daily_eq}

    # Build daily Kp
    daily_kp = await db.execute_fetchall(
        "SELECT DATE(time_tag), AVG(kp) FROM geomag_kp GROUP BY DATE(time_tag)"
    )
    kp_by_day = {r[0]: r[1] for r in daily_kp}

    # Build daily mean TEC
    daily_tec = await db.execute_fetchall(
        "SELECT DATE(epoch), AVG(tec_tecu) FROM tec GROUP BY DATE(epoch)"
    )
    tec_by_day = {r[0]: r[1] for r in daily_tec}

    # Align: all dates where we have at least Kp data
    all_dates = sorted(set(kp_by_day.keys()))
    if not all_dates:
        results["mutual_information"] = {"error": "No daily data"}
    else:
        # For MI, we check: does Kp/TEC today predict earthquake tomorrow?
        kp_bins = []
        tec_bins = []
        eq_bins = []
        eq_bins_same_day = []

        for i in range(len(all_dates) - 1):
            d_today = all_dates[i]
            d_tomorrow = all_dates[i + 1]

            # Check that tomorrow is actually the next day
            # (skip gaps)
            try:
                dt_today = datetime.strptime(d_today, "%Y-%m-%d")
                dt_tomorrow = datetime.strptime(d_tomorrow, "%Y-%m-%d")
                if (dt_tomorrow - dt_today).days != 1:
                    continue
            except ValueError:
                continue

            kp_val = kp_by_day.get(d_today)
            tec_val = tec_by_day.get(d_today)
            eq_tomorrow = 1 if eq_by_day.get(d_tomorrow, 0) > 0 else 0
            eq_today = 1 if eq_by_day.get(d_today, 0) > 0 else 0

            if kp_val is not None:
                # Discretize Kp: low(<1.5), med(1.5-3), high(>3)
                if kp_val < 1.5:
                    kp_bin = "low"
                elif kp_val < 3.0:
                    kp_bin = "med"
                else:
                    kp_bin = "high"
                kp_bins.append(kp_bin)
                eq_bins.append(eq_tomorrow)
                eq_bins_same_day.append(eq_today)

            if tec_val is not None:
                # Discretize TEC: low/med/high by terciles
                tec_bins.append(("tec_day", d_today, tec_val, eq_tomorrow))

        # Kp → earthquake tomorrow
        mi_kp_tomorrow = mutual_information(kp_bins, eq_bins)
        mi_kp_sameday = mutual_information(kp_bins, eq_bins_same_day)

        # Shuffled baseline for significance
        n_shuffle = 100
        shuffled_mis_tomorrow = []
        shuffled_mis_sameday = []
        for _ in range(n_shuffle):
            shuffled_eq = eq_bins.copy()
            random.shuffle(shuffled_eq)
            shuffled_mis_tomorrow.append(mutual_information(kp_bins, shuffled_eq))
            shuffled_sd = eq_bins_same_day.copy()
            random.shuffle(shuffled_sd)
            shuffled_mis_sameday.append(mutual_information(kp_bins, shuffled_sd))

        shuffle_mean_tmr = sum(shuffled_mis_tomorrow) / len(shuffled_mis_tomorrow) if shuffled_mis_tomorrow else 0
        shuffle_mean_sd = sum(shuffled_mis_sameday) / len(shuffled_mis_sameday) if shuffled_mis_sameday else 0

        # TEC MI (need to discretize by terciles)
        tec_entries = [(d, v, eq) for _, d, v, eq in tec_bins if v is not None]
        mi_tec_result = {"n": 0}
        if len(tec_entries) > 100:
            tec_vals = [v for _, v, _ in tec_entries]
            tec_p33 = percentile(tec_vals, 33)
            tec_p67 = percentile(tec_vals, 67)
            tec_discrete = []
            tec_eq = []
            for _, v, eq in tec_entries:
                if v < tec_p33:
                    tec_discrete.append("low")
                elif v < tec_p67:
                    tec_discrete.append("med")
                else:
                    tec_discrete.append("high")
                tec_eq.append(eq)

            mi_tec = mutual_information(tec_discrete, tec_eq)
            shuffled_mis_tec = []
            for _ in range(n_shuffle):
                s = tec_eq.copy()
                random.shuffle(s)
                shuffled_mis_tec.append(mutual_information(tec_discrete, s))
            shuffle_mean_tec = sum(shuffled_mis_tec) / len(shuffled_mis_tec)

            mi_tec_result = {
                "n": len(tec_entries),
                "mi_tec_eq_tomorrow": round(mi_tec, 6),
                "shuffled_mean": round(shuffle_mean_tec, 6),
                "ratio_vs_shuffled": round(mi_tec / max(shuffle_mean_tec, 1e-10), 2),
            }

        results["mutual_information"] = {
            "n_days": len(kp_bins),
            "base_rate_eq": round(sum(eq_bins) / max(len(eq_bins), 1) * 100, 1),
            "kp_tomorrow": {
                "mi": round(mi_kp_tomorrow, 6),
                "shuffled_mean": round(shuffle_mean_tmr, 6),
                "ratio_vs_shuffled": round(mi_kp_tomorrow / max(shuffle_mean_tmr, 1e-10), 2),
            },
            "kp_same_day": {
                "mi": round(mi_kp_sameday, 6),
                "shuffled_mean": round(shuffle_mean_sd, 6),
                "ratio_vs_shuffled": round(mi_kp_sameday / max(shuffle_mean_sd, 1e-10), 2),
            },
            "tec_tomorrow": mi_tec_result,
        }
        logger.info(
            "    MI(Kp→eq_tmr)=%.6f (shuffled=%.6f, ratio=%.1f) | MI(Kp→eq_same)=%.6f",
            mi_kp_tomorrow, shuffle_mean_tmr,
            mi_kp_tomorrow / max(shuffle_mean_tmr, 1e-10),
            mi_kp_sameday,
        )

    # ---------------------------------------------------------------
    # 5. Kp temporal profile — ISOLATED events only
    # ---------------------------------------------------------------
    logger.info("  --- Kp temporal profile (isolated only) ---")
    logger.info("    Isolated events: %d / %d", len(isolated_events), len(target_events))

    iso_profiles_kp = {h: [] for h in lead_hours}
    for e in isolated_events[:500]:
        for h in lead_hours:
            kp = await get_kp_at(e["time_str"], h)
            if kp is not None:
                iso_profiles_kp[h].append(kp)

    kp_profile_isolated = {}
    for h in lead_hours:
        iso_vals = iso_profiles_kp[h]
        rand_vals = rand_profiles_kp[h]
        iso_mean = sum(iso_vals) / len(iso_vals) if iso_vals else None
        rand_mean = sum(rand_vals) / len(rand_vals) if rand_vals else None
        kp_profile_isolated[f"-{h}h"] = {
            "iso_n": len(iso_vals),
            "iso_mean_kp": round(iso_mean, 3) if iso_mean else None,
            "rand_mean_kp": round(rand_mean, 3) if rand_mean else None,
            "diff": round(iso_mean - rand_mean, 3) if iso_mean and rand_mean else None,
            "iso_high_pct": round(sum(1 for v in iso_vals if v > 3) / max(len(iso_vals), 1) * 100, 1),
            "rand_high_pct": round(sum(1 for v in rand_vals if v > 3) / max(len(rand_vals), 1) * 100, 1),
        }
        if iso_mean and rand_mean:
            logger.info(
                "    [ISO] Kp at %4dh: iso=%.2f rand=%.2f diff=%+.3f high%%: iso=%.1f rand=%.1f",
                h, iso_mean, rand_mean, iso_mean - rand_mean,
                kp_profile_isolated[f"-{h}h"]["iso_high_pct"],
                kp_profile_isolated[f"-{h}h"]["rand_high_pct"],
            )

    results["kp_temporal_profile_isolated"] = kp_profile_isolated

    # ---------------------------------------------------------------
    # 6. TEC detrended + Kp combined: both anomalous simultaneously
    # ---------------------------------------------------------------
    logger.info("  --- TEC detrended + Kp combined ---")

    # For each earthquake: get both detrended TEC sigma and Kp at -12h
    combined_eq = []
    for e in sampled_events:
        tec_s = await get_sigma_detrended(e["lat"], e["lon"], e["time_str"])
        kp_12h = await get_kp_at(e["time_str"], 12)
        is_iso = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set_adv
        combined_eq.append({
            "tec_sigma": tec_s,
            "kp_12h": kp_12h,
            "mag": e["mag"],
            "isolated": is_iso,
        })

    combined_rand = []
    for r in random_points:
        tec_s = await get_sigma_detrended(r["lat"], r["lon"], r["time_str"])
        kp_12h = await get_kp_at(r["time_str"], 12)
        combined_rand.append({
            "tec_sigma": tec_s,
            "kp_12h": kp_12h,
        })

    def combined_analysis(eq_list, rand_list, label):
        """Check various threshold combinations of TEC spike + Kp high."""
        combos = [
            ("tec>0.5_kp>2", 0.5, 2.0),
            ("tec>0.5_kp>3", 0.5, 3.0),
            ("tec>1.0_kp>2", 1.0, 2.0),
            ("tec>1.0_kp>3", 1.0, 3.0),
            ("tec>1.5_kp>2", 1.5, 2.0),
            ("tec>1.5_kp>3", 1.5, 3.0),
        ]
        result = {}
        for name, tec_t, kp_t in combos:
            eq_both = sum(
                1 for p in eq_list
                if p["tec_sigma"] is not None and p["kp_12h"] is not None
                and p["tec_sigma"] > tec_t and p["kp_12h"] > kp_t
            )
            eq_either = sum(
                1 for p in eq_list
                if p["tec_sigma"] is not None and p["kp_12h"] is not None
            )
            rand_both = sum(
                1 for p in rand_list
                if p["tec_sigma"] is not None and p["kp_12h"] is not None
                and p["tec_sigma"] > tec_t and p["kp_12h"] > kp_t
            )
            rand_either = sum(
                1 for p in rand_list
                if p["tec_sigma"] is not None and p["kp_12h"] is not None
            )
            eq_rate = eq_both / max(eq_either, 1) * 100
            rand_rate = rand_both / max(rand_either, 1) * 100
            lift = eq_rate / max(rand_rate, 0.01)
            result[name] = {
                "eq_count": eq_both, "eq_total": eq_either,
                "eq_rate": round(eq_rate, 1),
                "rand_count": rand_both, "rand_total": rand_either,
                "rand_rate": round(rand_rate, 1),
                "lift": round(lift, 2),
            }
            logger.info(
                "    %s [%s]: eq=%d/%d (%.1f%%) rand=%d/%d (%.1f%%) lift=%.2f",
                label, name, eq_both, eq_either, eq_rate,
                rand_both, rand_either, rand_rate, lift,
            )
        return result

    combined_iso = [p for p in combined_eq if p["isolated"]]
    results["tec_kp_combined"] = {
        "all_events": combined_analysis(combined_eq, combined_rand, "ALL"),
        "isolated_events": combined_analysis(combined_iso, combined_rand, "ISO"),
    }

    # ---------------------------------------------------------------
    # 7. Temporal stability: 2011-2018 vs 2019-2026
    # ---------------------------------------------------------------
    logger.info("  --- Temporal stability check ---")

    split_date = datetime(2019, 1, 1, tzinfo=timezone.utc)

    # Use ALL target events (no [:500] cap) — split by period
    all_early_eq = [e for e in target_events if e["time"] < split_date]
    all_late_eq = [e for e in target_events if e["time"] >= split_date]
    # Subsample each period to max 300 for performance
    early_eq = sample_events_balanced(all_early_eq, 300, seed=51)
    late_eq = sample_events_balanced(all_late_eq, 300, seed=52)
    logger.info(
        "    Temporal split: early=%d (sampled %d), late=%d (sampled %d)",
        len(all_early_eq), len(early_eq), len(all_late_eq), len(late_eq),
    )

    # Also split random points by period
    early_rand = [r for r in random_points if r["time"] < split_date]
    late_rand = [r for r in random_points if r["time"] >= split_date]

    async def kp_profile_for_group(events, label):
        profiles = {h: [] for h in [24, 12, 6]}
        for e in events:
            for h in [24, 12, 6]:
                kp = await get_kp_at(e["time_str"], h)
                if kp is not None:
                    profiles[h].append(kp)
        result = {}
        for h in [24, 12, 6]:
            vals = profiles[h]
            mean_v = sum(vals) / len(vals) if vals else None
            result[f"-{h}h"] = {
                "n": len(vals),
                "mean_kp": round(mean_v, 3) if mean_v else None,
                "high_pct": round(sum(1 for v in vals if v > 3) / max(len(vals), 1) * 100, 1),
            }
            if mean_v:
                logger.info("    [%s] Kp at -%dh: mean=%.2f, high%%=%.1f (n=%d)",
                            label, h, mean_v, result[f"-{h}h"]["high_pct"], len(vals))
        return result

    async def tec_dt_for_group(events, rand_group, label):
        """TEC detrended for a group, with isolation breakdown and bootstrap CI."""
        sigmas_all = []
        sigmas_iso = []
        sigmas_non_iso = []
        for e in events:
            s = await get_sigma_detrended(e["lat"], e["lon"], e["time_str"])
            is_iso = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set_adv
            if s is not None:
                sigmas_all.append(s)
                if is_iso:
                    sigmas_iso.append(s)
                else:
                    sigmas_non_iso.append(s)
        rand_sigmas = []
        for r in rand_group:
            s = await get_sigma_detrended(r["lat"], r["lon"], r["time_str"])
            if s is not None:
                rand_sigmas.append(s)
        summary = {
            "all": tec_summary(sigmas_all),
            "isolated": tec_summary(sigmas_iso),
            "non_isolated": tec_summary(sigmas_non_iso),
            "random": tec_summary(rand_sigmas),
            "bootstrap_ci_iso_vs_rand": bootstrap_ci(sigmas_iso, rand_sigmas),
        }
        logger.info(
            "    [%s] TEC dt — all: n=%d σ=%.3f | iso: n=%d σ=%.3f | rand: n=%d σ=%.3f",
            label,
            summary["all"].get("n", 0), summary["all"].get("mean_sigma", 0),
            summary["isolated"].get("n", 0), summary["isolated"].get("mean_sigma", 0),
            summary["random"].get("n", 0), summary["random"].get("mean_sigma", 0),
        )
        return summary

    results["temporal_stability"] = {
        "2011_2018": {
            "n_events_total": len(all_early_eq),
            "n_events_sampled": len(early_eq),
            "kp_profile": await kp_profile_for_group(early_eq, "2011-2018"),
            "tec_detrended": await tec_dt_for_group(early_eq, early_rand, "2011-2018"),
        },
        "2019_2026": {
            "n_events_total": len(all_late_eq),
            "n_events_sampled": len(late_eq),
            "kp_profile": await kp_profile_for_group(late_eq, "2019-2026"),
            "tec_detrended": await tec_dt_for_group(late_eq, late_rand, "2019-2026"),
        },
    }

    # ---------------------------------------------------------------
    # 8. Magnitude dependence: M5, M6+, M7+
    # ---------------------------------------------------------------
    logger.info("  --- Magnitude dependence ---")

    mag_groups = [
        ("M5_5.9", 5.0, 6.0),
        ("M6_6.9", 6.0, 7.0),
        ("M7plus", 7.0, 10.0),
    ]

    mag_results = {}
    for label, m_min, m_max in mag_groups:
        # Use ALL events for magnitude analysis (no [:500] bias)
        group_all = [e for e in target_events if m_min <= e["mag"] < m_max]
        group_iso = [
            e for e in group_all
            if (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set_adv
        ]
        # Subsample if too many
        group_sampled = sample_events_balanced(group_all, 300, seed=70)
        group_iso_sampled = sample_events_balanced(group_iso, 300, seed=71)

        if len(group_sampled) < 5:
            mag_results[label] = {
                "n_total": len(group_all),
                "n_isolated": len(group_iso),
                "note": "too few events",
            }
            continue

        async def mag_kp_tec(events, sublabel):
            kp_vals = []
            tec_vals = []
            for e in events:
                kp = await get_kp_at(e["time_str"], 12)
                if kp is not None:
                    kp_vals.append(kp)
                s = await get_sigma_detrended(e["lat"], e["lon"], e["time_str"])
                if s is not None:
                    tec_vals.append(s)
            kp_mean = sum(kp_vals) / len(kp_vals) if kp_vals else None
            tec_mean = sum(tec_vals) / len(tec_vals) if tec_vals else None
            return {
                "n": len(events),
                "kp_12h": {
                    "n": len(kp_vals),
                    "mean": round(kp_mean, 3) if kp_mean else None,
                    "high_pct": round(sum(1 for v in kp_vals if v > 3) / max(len(kp_vals), 1) * 100, 1),
                },
                "tec_detrended": {
                    "n": len(tec_vals),
                    "mean_sigma": round(tec_mean, 3) if tec_mean else None,
                    "spikes_pct": round(sum(1 for v in tec_vals if v > 1) / max(len(tec_vals), 1) * 100, 1),
                },
            }

        all_result = await mag_kp_tec(group_sampled, f"{label}_all")
        iso_result = await mag_kp_tec(group_iso_sampled, f"{label}_iso") if len(group_iso_sampled) >= 5 else {"n": len(group_iso_sampled), "note": "too few isolated events"}

        mag_results[label] = {
            "n_total": len(group_all),
            "n_isolated": len(group_iso),
            "all_events": all_result,
            "isolated_events": iso_result,
        }
        logger.info(
            "    %s — all(n=%d): Kp=%.2f TEC=%.2f | iso(n=%d): Kp=%s TEC=%s",
            label, all_result["n"],
            all_result["kp_12h"].get("mean") or 0,
            all_result["tec_detrended"].get("mean_sigma") or 0,
            iso_result.get("n", 0),
            iso_result.get("kp_12h", {}).get("mean", "N/A"),
            iso_result.get("tec_detrended", {}).get("mean_sigma", "N/A"),
        )

    results["magnitude_dependence"] = mag_results

    # ---------------------------------------------------------------
    # 9. Alternative detrending: 30-day rolling average
    # ---------------------------------------------------------------
    logger.info("  --- TEC rolling average detrending (validation) ---")

    async def get_sigma_rolling(lat, lon, time_str, radius=5.0):
        """TEC sigma using 30-day rolling average instead of monthly-hourly climatology.

        Uses a wider window (30 days before the 7-day baseline window) as
        the climatology reference. This is independent of the global
        monthly-hourly climatology used in the primary analysis.
        """
        norm = normalize_time_str(time_str)
        # Get 37 days of data: 30-day rolling context + 7-day baseline
        rows = await db.execute_fetchall(
            "SELECT epoch, AVG(tec_tecu) FROM tec "
            "WHERE ABS(latitude - ?) <= ? AND ABS(longitude - ?) <= ? "
            "AND epoch BETWEEN datetime(?, '-888 hours') AND datetime(?, '+24 hours') "
            "GROUP BY epoch ORDER BY epoch",
            (lat, radius, lon, radius, norm, norm),
        )
        if len(rows) < 20:
            return None

        values = [r[1] for r in rows]
        if len(values) <= 8:
            return None

        # Split: rolling_context | baseline (7d) | precursor (24h)
        precursor = values[-8:]
        baseline = values[-16:-8] if len(values) >= 16 else values[:-8]
        rolling_context = values[:-16] if len(values) > 16 else []

        if not rolling_context or len(rolling_context) < 8:
            # Fall back: use all pre-precursor as baseline
            all_pre = values[:-8]
            if len(all_pre) < 8:
                return None
            b_mean = sum(all_pre) / len(all_pre)
            b_std = (sum((v - b_mean) ** 2 for v in all_pre) / len(all_pre)) ** 0.5
        else:
            # Use rolling context as climatology, compare baseline+precursor
            b_mean = sum(rolling_context) / len(rolling_context)
            b_std = (sum((v - b_mean) ** 2 for v in rolling_context) / len(rolling_context)) ** 0.5

        if b_std < 0.01:
            return None
        p_mean = sum(precursor) / len(precursor)
        return (p_mean - b_mean) / b_std

    # Compute rolling detrend for sampled events (with isolation)
    rolling_eq_all = []
    rolling_eq_iso = []
    rolling_eq_non_iso = []
    for e in sampled_events:
        s = await get_sigma_rolling(e["lat"], e["lon"], e["time_str"])
        is_iso = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set_adv
        if s is not None:
            s_r = round(s, 3)
            rolling_eq_all.append(s_r)
            if is_iso:
                rolling_eq_iso.append(s_r)
            else:
                rolling_eq_non_iso.append(s_r)

    rolling_rand = []
    for r in random_points:
        s = await get_sigma_rolling(r["lat"], r["lon"], r["time_str"])
        if s is not None:
            rolling_rand.append(round(s, 3))

    results["tec_rolling_detrend"] = {
        "method": "30-day rolling average as climatology (independent of monthly-hourly)",
        "random": tec_summary(rolling_rand),
        "pre_earthquake_all": tec_summary(rolling_eq_all),
        "pre_earthquake_isolated": tec_summary(rolling_eq_iso),
        "pre_earthquake_non_isolated": tec_summary(rolling_eq_non_iso),
        "bootstrap_ci_iso_vs_random": bootstrap_ci(rolling_eq_iso, rolling_rand),
    }
    logger.info(
        "    Rolling detrend — rand: n=%d σ=%.3f | all: n=%d σ=%.3f | iso: n=%d σ=%.3f",
        len(rolling_rand), sum(rolling_rand) / max(len(rolling_rand), 1),
        len(rolling_eq_all), sum(rolling_eq_all) / max(len(rolling_eq_all), 1),
        len(rolling_eq_iso), sum(rolling_eq_iso) / max(len(rolling_eq_iso), 1),
    )

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-mag", type=float, default=5.0)
    parser.add_argument("--type", choices=["all", "bvalue", "tec", "lag", "multi", "advanced"], default="all")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    async with safe_connect() as db:
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

        if args.type in ("all", "advanced"):
            results["advanced"] = await analyze_advanced(db, args.min_mag)

    out_path = RESULTS_DIR / f"analysis_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
