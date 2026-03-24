"""MODIS LST thermal anomaly analysis for earthquake precursors.

Tests whether land surface temperature shows anomalous increases
before M5+ earthquakes, as predicted by the LAIC (Lithosphere-
Atmosphere-Ionosphere Coupling) model:

    Stress → microfracturing → radon/CO2 release → air ionization
    → latent heat release → surface/atmospheric heating → TIR anomaly

This analysis uses MODIS LST data fetched via ORNL DAAC API.
Unlike TEC (Phase 1, destroyed by low spatial resolution), LST is
measured at the surface where thermal anomalies originate.

Key references:
    - Tronin (2006) Satellite thermal survey - Remote Sens Environ
    - Ouzounov & Freund (2004) Mid-infrared emission prior to earthquakes
    - Tramutoli et al. (2005) RST method for TIR anomaly detection
    - Saraf & Choudhury (2005) NOAA AVHRR thermal anomalies

Analysis design:
    1. For each M5+ earthquake, extract LST at epicenter ±7 days
    2. Compute anomaly: deviation from seasonal baseline (30-day window
       in non-earthquake years)
    3. Test: does LST anomaly exceed 2σ more often pre-earthquake
       than at random times/locations?
    4. Apply isolated event filter (aftershock bias removal)
    5. Check correlation with CFS/rate/foreshock signals (independence)
"""

import asyncio
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
DEG_TO_KM = 111.32


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def filter_isolated(target_events, all_events_sorted, days=3.0, degrees=1.5):
    """Remove aftershock-contaminated events."""
    import bisect
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
    """Compute p-th percentile (0-100)."""
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    d = k - f
    return s[f] + d * (s[c] - s[f])


def bootstrap_ci(values, n_boot=1000, ci=95):
    """Bootstrap confidence interval for mean."""
    if not values:
        return {"mean": 0, "ci_lower": 0, "ci_upper": 0}
    random.seed(42)
    means = []
    for _ in range(n_boot):
        sample = random.choices(values, k=len(values))
        means.append(sum(sample) / len(sample))
    means.sort()
    alpha = (100 - ci) / 200
    lo = int(alpha * n_boot)
    hi = int((1 - alpha) * n_boot)
    return {
        "mean": round(sum(values) / len(values), 3),
        "ci_lower": round(means[lo], 3),
        "ci_upper": round(means[min(hi, n_boot - 1)], 3),
    }


# ---------------------------------------------------------------------------
# RST-like anomaly detection (Tramutoli et al., 2005 simplified)
# ---------------------------------------------------------------------------

def compute_lst_anomaly(event_lst_values, baseline_mean, baseline_std):
    """Compute standardized LST anomaly relative to baseline.

    RETIRA index (Tramutoli 2005):
        anomaly = (observed - μ_baseline) / σ_baseline

    Positive values = warmer than expected.
    Values > 2.0 are considered "anomalous" (2σ threshold).
    """
    if baseline_std < 0.1:  # Avoid division by near-zero
        return None
    return (event_lst_values - baseline_mean) / baseline_std


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

async def run_lst_analysis(min_mag: float = 5.0):
    logger.info("=== MODIS LST Thermal Anomaly Analysis (min_mag=%.1f) ===", min_mag)

    async with safe_connect() as db:
        # Get LST data
        lst_rows = await db.execute_fetchall(
            "SELECT latitude, longitude, lst_kelvin, observed_date, product "
            "FROM modis_lst WHERE lst_kelvin > 200 AND lst_kelvin < 350 "
            "ORDER BY observed_date"
        )

        # Get all earthquakes
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
            "ORDER BY occurred_at"
        )

        # Get M5+ targets
        target_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= ? AND magnitude IS NOT NULL "
            "ORDER BY occurred_at",
            (min_mag,),
        )

    if not lst_rows:
        logger.warning("No MODIS LST data available. Run fetch_modis_lst.py first.")
        return {"error": "no_lst_data"}

    logger.info("  LST records: %d", len(lst_rows))
    logger.info("  Total earthquakes: %d", len(eq_rows))
    logger.info("  Target events (M%.1f+): %d", min_mag, len(target_rows))

    # Parse LST data: group by (lat, lon) → list of (date, kelvin)
    lst_by_location = {}
    for lat, lon, kelvin, date_str, product in lst_rows:
        key = (round(lat, 2), round(lon, 2))
        if key not in lst_by_location:
            lst_by_location[key] = []
        lst_by_location[key].append((date_str, kelvin))

    # Parse earthquakes
    events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events.append((t, r[2], r[3], r[1]))  # (time, lat, lon, mag)
        except (ValueError, TypeError):
            continue

    targets = []
    for r in target_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            targets.append({
                "time": t,
                "mag": r[1],
                "lat": r[2],
                "lon": r[3],
                "depth": r[4] if r[4] else 10.0,
            })
        except (ValueError, TypeError):
            continue

    # Isolated event filter
    target_tuples = [(e["time"], e["lat"], e["lon"], e["mag"]) for e in targets]
    isolated_set = set(
        (t.isoformat(), lat, lon)
        for t, lat, lon, mag in filter_isolated(target_tuples, events)
    )
    for e in targets:
        e["isolated"] = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_set

    # ---------------------------------------------------------------
    # Analysis 1: Pre-earthquake LST anomaly
    # ---------------------------------------------------------------
    # For each target M5+ with LST data, compute:
    #   - pre-event LST (days -7 to -1 before earthquake)
    #   - post-event LST (days +1 to +7 after earthquake)
    #   - event-day LST
    # Compare against random temporal sampling at same location
    # ---------------------------------------------------------------

    logger.info("  --- Analysis 1: LST anomaly at earthquake epicenters ---")

    matched_events = []
    unmatched = 0

    for e in targets:
        eq_date = e["time"].strftime("%Y-%m-%d")
        eq_key = (round(e["lat"], 2), round(e["lon"], 2))

        # Find LST data near this epicenter (within 0.1°)
        best_key = None
        best_dist = float("inf")
        for key in lst_by_location:
            dist = math.sqrt((key[0] - e["lat"]) ** 2 + (key[1] - e["lon"]) ** 2)
            if dist < best_dist and dist < 0.5:  # Within ~50km
                best_dist = dist
                best_key = key

        if best_key is None:
            unmatched += 1
            continue

        loc_data = lst_by_location[best_key]

        # Split into pre/post/event
        pre_lst = []
        post_lst = []
        event_lst = []
        all_lst = []

        for date_str, kelvin in loc_data:
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            dt_days = (d - e["time"].replace(tzinfo=None)).days
            all_lst.append(kelvin)
            if -7 <= dt_days <= -1:
                pre_lst.append(kelvin)
            elif dt_days == 0:
                event_lst.append(kelvin)
            elif 1 <= dt_days <= 7:
                post_lst.append(kelvin)

        if not all_lst or len(all_lst) < 3:
            unmatched += 1
            continue

        # Baseline: mean and std of all LST at this location
        baseline_mean = sum(all_lst) / len(all_lst)
        baseline_std = (sum((v - baseline_mean) ** 2 for v in all_lst) / len(all_lst)) ** 0.5

        # Pre-event anomaly
        pre_anomaly = None
        if pre_lst:
            pre_mean = sum(pre_lst) / len(pre_lst)
            pre_anomaly = compute_lst_anomaly(pre_mean, baseline_mean, baseline_std)

        # Post-event anomaly
        post_anomaly = None
        if post_lst:
            post_mean = sum(post_lst) / len(post_lst)
            post_anomaly = compute_lst_anomaly(post_mean, baseline_mean, baseline_std)

        # Event-day anomaly
        event_anomaly = None
        if event_lst:
            event_mean = sum(event_lst) / len(event_lst)
            event_anomaly = compute_lst_anomaly(event_mean, baseline_mean, baseline_std)

        matched_events.append({
            **e,
            "lst_n_total": len(all_lst),
            "lst_n_pre": len(pre_lst),
            "lst_n_post": len(post_lst),
            "lst_baseline_mean_k": round(baseline_mean, 2),
            "lst_baseline_std_k": round(baseline_std, 2),
            "lst_pre_mean_k": round(sum(pre_lst) / len(pre_lst), 2) if pre_lst else None,
            "lst_post_mean_k": round(sum(post_lst) / len(post_lst), 2) if post_lst else None,
            "lst_pre_anomaly": round(pre_anomaly, 3) if pre_anomaly is not None else None,
            "lst_post_anomaly": round(post_anomaly, 3) if post_anomaly is not None else None,
            "lst_event_anomaly": round(event_anomaly, 3) if event_anomaly is not None else None,
            "dist_deg": round(best_dist, 3),
        })

    logger.info("  Matched: %d, Unmatched: %d", len(matched_events), unmatched)

    if not matched_events:
        logger.warning("No LST-matched earthquake events. Need more LST data or wider matching radius.")
        return {
            "error": "no_matched_events",
            "n_lst_locations": len(lst_by_location),
            "n_targets": len(targets),
            "n_lst_records": len(lst_rows),
        }

    # ---------------------------------------------------------------
    # Statistics: Pre-earthquake anomaly distribution
    # ---------------------------------------------------------------

    pre_anomalies = [e["lst_pre_anomaly"] for e in matched_events if e["lst_pre_anomaly"] is not None]
    post_anomalies = [e["lst_post_anomaly"] for e in matched_events if e["lst_post_anomaly"] is not None]
    event_anomalies = [e["lst_event_anomaly"] for e in matched_events if e["lst_event_anomaly"] is not None]

    def anomaly_stats(values, label):
        if not values:
            return {"n": 0, "label": label}
        n = len(values)
        return {
            "n": n,
            "label": label,
            "mean": round(sum(values) / n, 3),
            "median": round(sorted(values)[n // 2], 3),
            "std": round((sum((v - sum(values) / n) ** 2 for v in values) / n) ** 0.5, 3),
            "gt_1sigma_pct": round(sum(1 for v in values if v > 1.0) / n * 100, 1),
            "gt_2sigma_pct": round(sum(1 for v in values if v > 2.0) / n * 100, 1),
            "gt_3sigma_pct": round(sum(1 for v in values if v > 3.0) / n * 100, 1),
            "lt_neg1sigma_pct": round(sum(1 for v in values if v < -1.0) / n * 100, 1),
            "bootstrap_mean_ci": bootstrap_ci(values),
            "p10": round(percentile(values, 10), 3),
            "p25": round(percentile(values, 25), 3),
            "p75": round(percentile(values, 75), 3),
            "p90": round(percentile(values, 90), 3),
        }

    pre_stats = anomaly_stats(pre_anomalies, "pre_earthquake_7d")
    post_stats = anomaly_stats(post_anomalies, "post_earthquake_7d")
    event_stats = anomaly_stats(event_anomalies, "event_day")

    logger.info("  Pre-EQ anomaly: n=%d mean=%.3f >2σ=%.1f%%",
                pre_stats.get("n", 0), pre_stats.get("mean", 0), pre_stats.get("gt_2sigma_pct", 0))
    logger.info("  Post-EQ anomaly: n=%d mean=%.3f >2σ=%.1f%%",
                post_stats.get("n", 0), post_stats.get("mean", 0), post_stats.get("gt_2sigma_pct", 0))

    # ---------------------------------------------------------------
    # Analysis 2: Isolated vs non-isolated events
    # ---------------------------------------------------------------

    logger.info("  --- Analysis 2: Isolated event check ---")

    iso_pre = [e["lst_pre_anomaly"] for e in matched_events
               if e["lst_pre_anomaly"] is not None and e["isolated"]]
    non_iso_pre = [e["lst_pre_anomaly"] for e in matched_events
                   if e["lst_pre_anomaly"] is not None and not e["isolated"]]

    iso_stats = anomaly_stats(iso_pre, "isolated_pre")
    non_iso_stats = anomaly_stats(non_iso_pre, "non_isolated_pre")

    logger.info("  Isolated: n=%d mean=%.3f >2σ=%.1f%%",
                iso_stats.get("n", 0), iso_stats.get("mean", 0), iso_stats.get("gt_2sigma_pct", 0))
    logger.info("  Non-iso:  n=%d mean=%.3f >2σ=%.1f%%",
                non_iso_stats.get("n", 0), non_iso_stats.get("mean", 0), non_iso_stats.get("gt_2sigma_pct", 0))

    # ---------------------------------------------------------------
    # Analysis 3: Magnitude dependence
    # ---------------------------------------------------------------

    logger.info("  --- Analysis 3: Magnitude dependence ---")

    mag_bins = {}
    for e in matched_events:
        if e["lst_pre_anomaly"] is None:
            continue
        mag_bin = f"M{int(e['mag'])}"
        if mag_bin not in mag_bins:
            mag_bins[mag_bin] = []
        mag_bins[mag_bin].append(e["lst_pre_anomaly"])

    mag_results = {}
    for mag_bin, values in sorted(mag_bins.items()):
        mag_results[mag_bin] = anomaly_stats(values, mag_bin)
        logger.info("  %s: n=%d mean=%.3f >2σ=%.1f%%",
                    mag_bin, len(values), sum(values) / len(values),
                    sum(1 for v in values if v > 2.0) / len(values) * 100)

    # ---------------------------------------------------------------
    # Analysis 4: Temporal profile (day-by-day anomaly)
    # ---------------------------------------------------------------

    logger.info("  --- Analysis 4: Day-by-day temporal profile ---")

    day_profiles = {}  # day_offset → list of anomalies
    for e in matched_events:
        eq_key = (round(e["lat"], 2), round(e["lon"], 2))
        best_key = None
        best_dist = float("inf")
        for key in lst_by_location:
            dist = math.sqrt((key[0] - e["lat"]) ** 2 + (key[1] - e["lon"]) ** 2)
            if dist < best_dist and dist < 0.5:
                best_dist = dist
                best_key = key

        if best_key is None:
            continue

        loc_data = lst_by_location[best_key]
        all_vals = [k for _, k in loc_data]
        if len(all_vals) < 3:
            continue
        bl_mean = sum(all_vals) / len(all_vals)
        bl_std = (sum((v - bl_mean) ** 2 for v in all_vals) / len(all_vals)) ** 0.5
        if bl_std < 0.1:
            continue

        for date_str, kelvin in loc_data:
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            dt_days = (d - e["time"].replace(tzinfo=None)).days
            if -7 <= dt_days <= 7:
                anomaly = (kelvin - bl_mean) / bl_std
                if dt_days not in day_profiles:
                    day_profiles[dt_days] = []
                day_profiles[dt_days].append(anomaly)

    temporal_profile = {}
    for day_offset in sorted(day_profiles.keys()):
        vals = day_profiles[day_offset]
        temporal_profile[f"day_{day_offset:+d}"] = {
            "n": len(vals),
            "mean_anomaly": round(sum(vals) / len(vals), 3) if vals else 0,
            "gt_2sigma_pct": round(sum(1 for v in vals if v > 2.0) / len(vals) * 100, 1) if vals else 0,
        }

    # ---------------------------------------------------------------
    # Analysis 5: Depth dependence
    # ---------------------------------------------------------------

    logger.info("  --- Analysis 5: Depth dependence ---")

    depth_bins = {"shallow_lt30km": [], "intermediate_30_70km": [], "deep_gt70km": []}
    for e in matched_events:
        if e["lst_pre_anomaly"] is None:
            continue
        if e["depth"] < 30:
            depth_bins["shallow_lt30km"].append(e["lst_pre_anomaly"])
        elif e["depth"] < 70:
            depth_bins["intermediate_30_70km"].append(e["lst_pre_anomaly"])
        else:
            depth_bins["deep_gt70km"].append(e["lst_pre_anomaly"])

    depth_results = {}
    for bin_name, values in depth_bins.items():
        depth_results[bin_name] = anomaly_stats(values, bin_name)
        if values:
            logger.info("  %s: n=%d mean=%.3f >2σ=%.1f%%",
                        bin_name, len(values), sum(values) / len(values),
                        sum(1 for v in values if v > 2.0) / len(values) * 100)

    # ---------------------------------------------------------------
    # Analysis 6: LST signal vs Phase 2 signals (independence check)
    # ---------------------------------------------------------------
    # This requires CFS/rate/foreshock data from validation_phase2
    # We compute it here if validation results exist

    logger.info("  --- Analysis 6: Cross-signal independence ---")

    # Load latest validation results if available
    cross_signal = {"status": "requires_concurrent_validation_data"}
    val_files = sorted(RESULTS_DIR.glob("validation_*.json"), reverse=True)
    if val_files:
        try:
            with open(val_files[0]) as f:
                val_data = json.load(f)
            cross_signal = {
                "status": "validation_data_available",
                "note": "Cross-correlation with CFS/rate/foreshock requires "
                        "matching individual events (future integration)"
            }
        except (json.JSONDecodeError, KeyError):
            pass

    # ---------------------------------------------------------------
    # Compile results
    # ---------------------------------------------------------------

    results = {
        "metadata": {
            "min_mag": min_mag,
            "n_lst_records": len(lst_rows),
            "n_lst_locations": len(lst_by_location),
            "n_targets": len(targets),
            "n_matched": len(matched_events),
            "n_unmatched": unmatched,
            "n_isolated": sum(1 for e in matched_events if e["isolated"]),
            "n_non_isolated": sum(1 for e in matched_events if not e["isolated"]),
        },
        "pre_earthquake_anomaly": pre_stats,
        "post_earthquake_anomaly": post_stats,
        "event_day_anomaly": event_stats,
        "isolation_check": {
            "isolated": iso_stats,
            "non_isolated": non_iso_stats,
            "lift_gt2sigma_iso_vs_null": round(
                iso_stats.get("gt_2sigma_pct", 0) / max(4.55, 0.1), 2  # null expectation: 2.275% one-tailed
            ) if iso_stats.get("n", 0) > 0 else None,
        },
        "magnitude_dependence": mag_results,
        "depth_dependence": depth_results,
        "temporal_profile": temporal_profile,
        "cross_signal_independence": cross_signal,
        "interpretation": {
            "laic_hypothesis": (
                "If mean pre-earthquake anomaly > 0 with CI excluding 0, "
                "and >2σ% exceeds null (4.55%), LAIC thermal precursor is supported."
            ),
            "null_expectation_gt2sigma": "4.55% (Gaussian two-tailed 2σ)",
            "key_tests": [
                "Pre-EQ anomaly significantly > 0",
                "Pre > Post (asymmetry = precursor, not co-seismic)",
                "Isolated events show signal (not aftershock artifact)",
                "Shallow > Deep (thermal anomaly should decay with depth)",
                "Higher M → stronger signal (physical expectation)",
            ],
        },
    }

    # Per-event details (for cross-referencing)
    event_details = []
    for e in matched_events[:100]:  # Cap for JSON size
        event_details.append({
            "time": e["time"].isoformat(),
            "mag": e["mag"],
            "lat": e["lat"],
            "lon": e["lon"],
            "depth": e["depth"],
            "isolated": e["isolated"],
            "lst_pre_anomaly": e["lst_pre_anomaly"],
            "lst_post_anomaly": e["lst_post_anomaly"],
            "lst_event_anomaly": e["lst_event_anomaly"],
            "lst_n_total": e["lst_n_total"],
            "dist_deg": e["dist_deg"],
        })
    results["event_details"] = event_details

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_lst_analysis()

    out_path = RESULTS_DIR / f"lst_analysis_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
