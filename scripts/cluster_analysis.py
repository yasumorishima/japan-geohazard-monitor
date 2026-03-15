"""Multi-scale spatiotemporal clustering analysis for foreshock detection.

Uses data-driven clustering (instead of fixed aftershock windows) to
identify earthquake sequences, then tests whether large earthquakes
are preceded by anomalous clustering patterns.

Approach:
1. Nearest-neighbor distance in (space, time, magnitude) → detect
   clusters at multiple scales
2. Identify sequences that precede M5+ events
3. Compare foreshock sequence properties with random sequences

No external clustering libraries needed — uses nearest-neighbor
distance ratios (Zaliapin & Ben-Zion, 2013) which is the modern
standard for earthquake declustering.

Reference: Zaliapin & Ben-Zion (2013) JGR 118:2847-2864
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

DEG_TO_KM = 111.32

# Gutenberg-Richter b-value (assumed)
B_VALUE = 1.0
# Fractal dimension of earthquake hypocenters
D_FRAC = 1.6


def nearest_neighbor_distance(
    events: list, idx: int, b: float = B_VALUE, d_frac: float = D_FRAC,
) -> dict | None:
    """Compute rescaled nearest-neighbor distance for event at idx.

    Following Zaliapin & Ben-Zion (2013):
        η_ij = T_ij * R_ij^d * 10^(-b*Mi)
    where T = interevent time, R = spatial distance, Mi = parent magnitude.

    Decompose into:
        T_component = T_ij * 10^(-b*Mi/2)
        R_component = R_ij^d * 10^(-b*Mi/2)

    The nearest neighbor is the event j < i that minimizes η_ij.
    """
    if idx == 0:
        return None

    target = events[idx]
    best_eta = float("inf")
    best_info = None

    # Search backward (only consider events within 30 days for efficiency)
    t_target = target["t_days"]
    for j in range(idx - 1, max(idx - 5000, -1), -1):
        parent = events[j]
        dt = t_target - parent["t_days"]
        if dt <= 0:
            continue
        if dt > 30:  # Beyond 30 days, unlikely to be related
            break

        # Spatial distance
        dlat = (target["lat"] - parent["lat"]) * DEG_TO_KM
        dlon = (target["lon"] - parent["lon"]) * DEG_TO_KM * math.cos(math.radians(target["lat"]))
        r_km = math.sqrt(dlat**2 + dlon**2) + 0.1  # Avoid log(0)

        mi = parent["mag"]

        # Rescaled distance
        eta = dt * (r_km ** d_frac) * (10 ** (-b * mi))

        # Decompose
        t_comp = dt * (10 ** (-b * mi / 2))
        r_comp = (r_km ** d_frac) * (10 ** (-b * mi / 2))

        if eta < best_eta:
            best_eta = eta
            best_info = {
                "eta": eta,
                "log10_eta": math.log10(max(eta, 1e-20)),
                "t_comp": t_comp,
                "r_comp": r_comp,
                "log10_t": math.log10(max(t_comp, 1e-20)),
                "log10_r": math.log10(max(r_comp, 1e-20)),
                "parent_idx": j,
                "parent_mag": mi,
                "dt_days": dt,
                "dist_km": r_km,
            }

    return best_info


def classify_event(nn_info: dict, eta_threshold: float = -5.0) -> str:
    """Classify event as 'clustered' or 'background' based on η.

    Events with log10(η) < threshold are clustered (aftershocks/foreshocks).
    Events with log10(η) >= threshold are background (independent).
    """
    if nn_info is None:
        return "background"
    return "clustered" if nn_info["log10_eta"] < eta_threshold else "background"


async def run_cluster_analysis(min_mag_target: float = 5.0) -> dict:
    """Run spatiotemporal clustering analysis."""
    logger.info("=== Cluster Analysis (target M%.1f+) ===", min_mag_target)

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

    logger.info("  Catalog: %d M3+ events", len(events))

    # ---------------------------------------------------------------
    # 1. Compute nearest-neighbor distances for all events
    # ---------------------------------------------------------------
    logger.info("  Computing nearest-neighbor distances...")
    nn_data = []
    for i in range(len(events)):
        nn = nearest_neighbor_distance(events, i)
        nn_data.append(nn)
        if (i + 1) % 5000 == 0:
            logger.info("    Processed %d/%d", i + 1, len(events))

    # Classify events
    # Find optimal threshold from bimodal distribution of log10(η)
    log_etas = [nn["log10_eta"] for nn in nn_data if nn is not None]
    if not log_etas:
        return {"error": "No nearest-neighbor data"}

    # Use histogram to find the valley between two modes
    bins = 50
    min_eta, max_eta = min(log_etas), max(log_etas)
    bin_width = (max_eta - min_eta) / bins
    hist = [0] * bins
    for v in log_etas:
        idx = min(int((v - min_eta) / bin_width), bins - 1)
        hist[idx] += 1

    # Find minimum between two peaks (valley)
    # Smooth histogram
    smooth = [sum(hist[max(0, j - 2):min(bins, j + 3)]) / 5 for j in range(bins)]
    # Find valley in middle third
    third = bins // 3
    valley_idx = third + smooth[third:2 * third].index(min(smooth[third:2 * third]))
    eta_threshold = min_eta + (valley_idx + 0.5) * bin_width

    logger.info("  η threshold: %.2f (auto-detected from bimodal distribution)", eta_threshold)

    for i, nn in enumerate(nn_data):
        events[i]["nn"] = nn
        events[i]["type"] = classify_event(nn, eta_threshold)

    n_clustered = sum(1 for e in events if e["type"] == "clustered")
    n_background = sum(1 for e in events if e["type"] == "background")
    logger.info("  Classification: %d clustered (%.1f%%), %d background",
                n_clustered, 100 * n_clustered / len(events), n_background)

    # ---------------------------------------------------------------
    # 2. Build sequences: chains of clustered events
    # ---------------------------------------------------------------
    # A sequence is a tree rooted at a background event
    sequences = []  # list of lists of event indices
    event_to_seq = {}

    for i, e in enumerate(events):
        if e["type"] == "background" or e["nn"] is None:
            # Start new sequence
            seq_idx = len(sequences)
            sequences.append([i])
            event_to_seq[i] = seq_idx
        else:
            # Attach to parent's sequence
            parent_idx = e["nn"]["parent_idx"]
            if parent_idx in event_to_seq:
                seq_idx = event_to_seq[parent_idx]
                sequences[seq_idx].append(i)
                event_to_seq[i] = seq_idx
            else:
                seq_idx = len(sequences)
                sequences.append([i])
                event_to_seq[i] = seq_idx

    seq_sizes = [len(s) for s in sequences]
    logger.info("  Sequences: %d total, max size %d, mean %.1f",
                len(sequences), max(seq_sizes), sum(seq_sizes) / len(seq_sizes))

    # ---------------------------------------------------------------
    # 3. Foreshock analysis: sequences that contain M5+ events
    # ---------------------------------------------------------------
    target_events = [(i, e) for i, e in enumerate(events) if e["mag"] >= min_mag_target]
    logger.info("  Target events (M%.1f+): %d", min_mag_target, len(target_events))

    foreshock_stats = []
    for target_idx, target_event in target_events:
        seq_idx = event_to_seq.get(target_idx)
        if seq_idx is None:
            continue

        seq = sequences[seq_idx]
        # Events in same sequence BEFORE the target
        before = [j for j in seq if j < target_idx]

        if not before:
            foreshock_stats.append({
                "time": target_event["time"].isoformat()[:16],
                "mag": target_event["mag"],
                "n_foreshocks": 0,
                "max_foreshock_mag": None,
                "sequence_duration_days": 0,
                "type": target_event["type"],
            })
            continue

        foreshock_mags = [events[j]["mag"] for j in before]
        foreshock_times = [events[j]["t_days"] for j in before]
        duration = target_event["t_days"] - min(foreshock_times)

        foreshock_stats.append({
            "time": target_event["time"].isoformat()[:16],
            "mag": target_event["mag"],
            "n_foreshocks": len(before),
            "max_foreshock_mag": round(max(foreshock_mags), 1),
            "mean_foreshock_mag": round(sum(foreshock_mags) / len(foreshock_mags), 2),
            "sequence_duration_days": round(duration, 2),
            "type": target_event["type"],
            "b_in_sequence": len(foreshock_mags),
        })

    # ---------------------------------------------------------------
    # 4. Compare with random baseline
    # ---------------------------------------------------------------
    # For random events (not M5+), how often do they have foreshock sequences?
    random.seed(42)
    non_target = [i for i, e in enumerate(events) if e["mag"] < min_mag_target and e["mag"] >= 4.0]
    random_sample = random.sample(non_target, min(500, len(non_target)))

    random_foreshock_stats = []
    for rand_idx in random_sample:
        rand_event = events[rand_idx]
        seq_idx = event_to_seq.get(rand_idx)
        if seq_idx is None:
            random_foreshock_stats.append({"n_foreshocks": 0})
            continue
        seq = sequences[seq_idx]
        before = [j for j in seq if j < rand_idx]
        random_foreshock_stats.append({"n_foreshocks": len(before)})

    # ---------------------------------------------------------------
    # 5. Statistics
    # ---------------------------------------------------------------
    def foreshock_summary(stats_list):
        if not stats_list:
            return {"n": 0}
        n = len(stats_list)
        n_fore = [s["n_foreshocks"] for s in stats_list]
        has_foreshock = sum(1 for f in n_fore if f > 0)
        return {
            "n": n,
            "has_foreshock_pct": round(100 * has_foreshock / n, 1),
            "mean_n_foreshocks": round(sum(n_fore) / n, 2),
            "max_n_foreshocks": max(n_fore),
            "median_n_foreshocks": sorted(n_fore)[n // 2],
        }

    eq_summary = foreshock_summary(foreshock_stats)
    rand_summary = foreshock_summary(random_foreshock_stats)

    # Lift
    lift_has_fore = eq_summary["has_foreshock_pct"] / max(rand_summary["has_foreshock_pct"], 0.1)

    # η distribution comparison: M5+ events vs all events
    target_etas = [events[i]["nn"]["log10_eta"] for i, _ in target_events
                   if events[i]["nn"] is not None]
    all_etas = [nn["log10_eta"] for nn in nn_data if nn is not None]

    def eta_stats(values):
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
            "clustered_pct": round(sum(1 for v in s if v < eta_threshold) / n * 100, 1),
        }

    # Magnitude dependence
    mag_foreshock = {}
    for s in foreshock_stats:
        bin_label = f"M{int(s['mag'])}"
        mag_foreshock.setdefault(bin_label, []).append(s["n_foreshocks"])

    results = {
        "catalog_stats": {
            "n_events": len(events),
            "n_clustered": n_clustered,
            "n_background": n_background,
            "clustered_pct": round(100 * n_clustered / len(events), 1),
            "n_sequences": len(sequences),
            "eta_threshold": round(eta_threshold, 2),
        },
        "eta_distribution": {
            "all_events": eta_stats(all_etas),
            "m5_plus": eta_stats(target_etas),
        },
        "foreshock_analysis": {
            "m5_plus_events": eq_summary,
            "random_m4_events": rand_summary,
            "lift_has_foreshock": round(lift_has_fore, 2),
        },
        "by_magnitude": {
            k: {
                "n": len(v),
                "has_foreshock_pct": round(100 * sum(1 for x in v if x > 0) / len(v), 1),
                "mean_n": round(sum(v) / len(v), 2),
            }
            for k, v in sorted(mag_foreshock.items())
        },
        "sample_foreshock_sequences": [s for s in foreshock_stats if s["n_foreshocks"] > 0][:20],
    }

    logger.info("  Foreshock — M5+: %.1f%% have foreshocks (mean %.1f) | Random: %.1f%% (mean %.1f)",
                eq_summary["has_foreshock_pct"], eq_summary["mean_n_foreshocks"],
                rand_summary["has_foreshock_pct"], rand_summary["mean_n_foreshocks"])
    logger.info("  Lift (has foreshock): %.2f", lift_has_fore)

    return results


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-mag", type=float, default=5.0)
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_cluster_analysis(args.min_mag)

    out_path = RESULTS_DIR / f"cluster_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
