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


async def analyze_bvalue(db: aiosqlite.Connection, min_mag: float) -> dict:
    """b-value analysis with control experiment."""
    logger.info("=== b-value analysis (min_mag=%.1f) ===", min_mag)

    all_eq = await db.execute_fetchall(
        "SELECT occurred_at, magnitude FROM earthquakes "
        "WHERE magnitude IS NOT NULL AND magnitude >= 3.0 ORDER BY occurred_at"
    )
    target_eq = await db.execute_fetchall(
        "SELECT occurred_at, magnitude FROM earthquakes "
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

        # Before target earthquakes
        target_times = []
        for r in target_eq:
            try:
                target_times.append(datetime.fromisoformat(r[0].replace("Z", "+00:00")))
            except (ValueError, TypeError):
                continue

        eq_b = [b for b in (get_b(t, window) for t in target_times[:500]) if b is not None]

        # Magnitude-binned
        mag_bins = {}
        for r in target_eq[:500]:
            try:
                t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
                m = r[1]
            except (ValueError, TypeError):
                continue
            b = get_b(t, window)
            if b is None:
                continue
            bin_label = f"M{int(m)}"
            mag_bins.setdefault(bin_label, []).append(b)

        results[f"window_{window}d"] = {
            "random": {
                "n": len(rand_b),
                "mean_b": round(sum(rand_b) / len(rand_b), 3) if rand_b else None,
                "b_lt_07": round(sum(1 for b in rand_b if b < 0.7) / len(rand_b) * 100, 1) if rand_b else None,
            },
            "pre_earthquake": {
                "n": len(eq_b),
                "mean_b": round(sum(eq_b) / len(eq_b), 3) if eq_b else None,
                "b_lt_07": round(sum(1 for b in eq_b if b < 0.7) / len(eq_b) * 100, 1) if eq_b else None,
            },
            "by_magnitude": {
                label: {
                    "n": len(vals),
                    "mean_b": round(sum(vals) / len(vals), 3),
                    "b_lt_07": round(sum(1 for b in vals if b < 0.7) / len(vals) * 100, 1),
                }
                for label, vals in sorted(mag_bins.items())
            },
        }
        logger.info(
            "  %3dd window: random b=%.3f (%d%% <0.7) vs eq b=%.3f (%d%% <0.7)",
            window,
            results[f"window_{window}d"]["random"]["mean_b"] or 0,
            results[f"window_{window}d"]["random"]["b_lt_07"] or 0,
            results[f"window_{window}d"]["pre_earthquake"]["mean_b"] or 0,
            results[f"window_{window}d"]["pre_earthquake"]["b_lt_07"] or 0,
        )

    return results


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


async def analyze_multi(db: aiosqlite.Connection, min_mag: float) -> dict:
    """Multi-indicator simultaneous anomaly analysis.

    Checks if COMBINATIONS of anomalies (b-value + TEC + Kp) occurring
    simultaneously are more predictive than any single indicator.
    """
    logger.info("=== Multi-indicator analysis (min_mag=%.1f) ===", min_mag)

    # Get all M6+ earthquakes with timestamps
    eq_rows = await db.execute_fetchall(
        "SELECT occurred_at, latitude, longitude, magnitude FROM earthquakes "
        "WHERE magnitude >= ? ORDER BY occurred_at",
        (min_mag,),
    )

    all_eq = await db.execute_fetchall(
        "SELECT occurred_at, magnitude FROM earthquakes "
        "WHERE magnitude IS NOT NULL AND magnitude >= 3.0 ORDER BY occurred_at"
    )

    events_all = []
    for r in all_eq:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events_all.append((t, r[1]))
        except (ValueError, TypeError):
            continue

    if len(events_all) < 100:
        return {"error": "Insufficient data"}

    def get_b(target_time, window_days=90):
        ws = target_time - timedelta(days=window_days)
        mags = [m for t, m in events_all if ws <= t < target_time]
        return calc_b(mags)

    async def get_kp_before(time_str, hours=48):
        rows = await db.execute_fetchall(
            "SELECT AVG(kp) FROM geomag_kp "
            "WHERE time_tag BETWEEN datetime(?, ? || ' hours') AND datetime(?)",
            (time_str, f"-{hours}", time_str),
        )
        return rows[0][0] if rows and rows[0][0] is not None else None

    async def get_tec_sigma(lat, lon, time_str, radius=5.0):
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

    # Analyze each earthquake
    event_profiles = []
    for time_str, lat, lon, mag in eq_rows[:200]:
        try:
            t = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        b = get_b(t)
        kp = await get_kp_before(time_str)
        tec_s = await get_tec_sigma(lat, lon, time_str)

        profile = {
            "time": time_str[:16],
            "mag": mag,
            "b_value": round(b, 3) if b else None,
            "kp_48h_avg": round(kp, 2) if kp else None,
            "tec_sigma": round(tec_s, 2) if tec_s else None,
        }

        # Count simultaneous anomalies
        anomalies = 0
        if b is not None and b < 0.7:
            anomalies += 1
        if kp is not None and kp > 4.0:
            anomalies += 1
        if tec_s is not None and tec_s < -1.0:
            anomalies += 1
        profile["simultaneous_anomalies"] = anomalies

        event_profiles.append(profile)

    # Same for random dates
    random.seed(42)
    t_min = events_all[0][0] + timedelta(days=91)
    t_max = events_all[-1][0]
    tec_range = await db.execute_fetchall("SELECT MIN(epoch), MAX(epoch) FROM tec")

    random_profiles = []
    for _ in range(200):
        rt = t_min + timedelta(seconds=random.randint(0, int((t_max - t_min).total_seconds())))
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25

        b = get_b(rt)
        kp = await get_kp_before(rt.isoformat())
        tec_s = await get_tec_sigma(rlat, rlon, rt.isoformat())

        anomalies = 0
        if b is not None and b < 0.7:
            anomalies += 1
        if kp is not None and kp > 4.0:
            anomalies += 1
        if tec_s is not None and tec_s < -1.0:
            anomalies += 1

        random_profiles.append({"simultaneous_anomalies": anomalies})

    # Compare distribution of simultaneous anomalies
    def count_dist(profiles):
        dist = {0: 0, 1: 0, 2: 0, 3: 0}
        for p in profiles:
            a = p["simultaneous_anomalies"]
            dist[min(a, 3)] += 1
        n = len(profiles) or 1
        return {k: {"count": v, "pct": round(v / n * 100, 1)} for k, v in dist.items()}

    result = {
        "pre_earthquake": {
            "n": len(event_profiles),
            "distribution": count_dist(event_profiles),
            "events": event_profiles,
        },
        "random": {
            "n": len(random_profiles),
            "distribution": count_dist(random_profiles),
        },
    }

    eq_dist = count_dist(event_profiles)
    rand_dist = count_dist(random_profiles)
    for k in [0, 1, 2, 3]:
        logger.info(
            "  %d simultaneous anomalies: random=%5.1f%% vs eq=%5.1f%%",
            k, rand_dist[k]["pct"], eq_dist[k]["pct"],
        )

    return result


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
