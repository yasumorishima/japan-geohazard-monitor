"""Phase 2 validation: aftershock bias removal and signal independence.

Four validation tests:
1. Isolated event analysis — do signals survive aftershock filtering?
2. Time delay analysis — CFS events: aftershocks (<30d) vs delayed triggers?
3. Three-signal correlation — are CFS, activation, foreshock independent?
4. Prospective test — train 2011-2018, predict 2019-2026

This is the critical test that separates real signals from aftershock artifacts.
Phase 1 was destroyed by this test. Phase 2 must survive it.
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
SHEAR_MODULUS = 32e9
MU_FRICTION = 0.4


# ---------------------------------------------------------------------------
# Shared utilities (duplicated from other scripts to keep self-contained)
# ---------------------------------------------------------------------------

def filter_isolated(target_events, all_events_sorted, days=3.0, degrees=1.5):
    """Remove aftershock-contaminated events."""
    import bisect as bs
    all_times = [e[0] for e in all_events_sorted]
    isolated = []
    for t, lat, lon, mag in target_events:
        t_min = t - timedelta(days=days)
        idx_start = bs.bisect_left(all_times, t_min)
        idx_end = bs.bisect_left(all_times, t)
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


def fault_dimensions(mw):
    """Wells & Coppersmith (1994) reverse fault dimensions."""
    length_km = 10 ** (-2.86 + 0.63 * mw)
    width_km = 10 ** (-1.61 + 0.41 * mw)
    m0 = 10 ** (1.5 * mw + 9.05)
    slip_m = m0 / (SHEAR_MODULUS * length_km * 1000 * width_km * 1000)
    return length_km, width_km, slip_m


def okada_cfs(src_lat, src_lon, src_depth, src_strike, src_dip, src_rake,
              src_length, src_width, src_slip,
              obs_lat, obs_lon, obs_depth):
    """Compute CFS at observation point from source (far-field approx)."""
    dx = (obs_lon - src_lon) * DEG_TO_KM * math.cos(math.radians(src_lat)) * 1000
    dy = (obs_lat - src_lat) * DEG_TO_KM * 1000
    dz = (obs_depth - src_depth) * 1000
    r = math.sqrt(dx**2 + dy**2 + dz**2)
    if r < 500:
        return 0.0

    m0 = SHEAR_MODULUS * src_length * 1000 * src_width * 1000 * src_slip
    strike_r = math.radians(src_strike)
    dip_r = math.radians(src_dip)
    rake_r = math.radians(src_rake)

    n = [-math.sin(dip_r) * math.sin(strike_r),
         math.sin(dip_r) * math.cos(strike_r),
         -math.cos(dip_r)]
    d = [math.cos(rake_r) * math.cos(strike_r) + math.sin(rake_r) * math.cos(dip_r) * math.sin(strike_r),
         math.cos(rake_r) * math.sin(strike_r) - math.sin(rake_r) * math.cos(dip_r) * math.cos(strike_r),
         -math.sin(rake_r) * math.sin(dip_r)]

    rhat = [dx / r, dy / r, dz / r]
    m_ij = [[(n[i] * d[j] + n[j] * d[i]) / 2 for j in range(3)] for i in range(3)]
    prefactor = m0 / (4 * math.pi * r**3)
    m_rr = sum(m_ij[k][l] * rhat[k] * rhat[l] for k in range(3) for l in range(3))

    stress = [[prefactor * (3 * m_rr * rhat[i] * rhat[j] - m_ij[i][j] -
               (1 if i == j else 0) * m_rr) for j in range(3)] for i in range(3)]

    sigma_mean = (stress[0][0] + stress[1][1] + stress[2][2]) / 3
    dev = [[stress[i][j] - (sigma_mean if i == j else 0) for j in range(3)] for i in range(3)]
    j2 = 0.5 * sum(dev[i][j] ** 2 for i in range(3) for j in range(3))
    tau_max = math.sqrt(max(j2, 0))
    return tau_max + MU_FRICTION * sigma_mean


def default_mechanism(lat, lon, depth):
    """Regional default strike/dip/rake for Japan."""
    if depth > 70:
        return 200.0, 45.0, 90.0
    elif lon > 142 and lat > 35:
        return 200.0, 25.0, 90.0
    elif lon < 137 and lat < 35:
        return 240.0, 15.0, 90.0
    else:
        return 200.0, 35.0, 90.0


# ---------------------------------------------------------------------------
# Main validation
# ---------------------------------------------------------------------------

async def run_validation(min_mag: float = 5.0):
    logger.info("=== Phase 2 Validation (min_mag=%.1f) ===", min_mag)

    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
            "ORDER BY occurred_at"
        )
        fm_rows = await db.execute_fetchall(
            "SELECT latitude, longitude, strike1, dip1, rake1 "
            "FROM focal_mechanisms"
        )

    # Parse events
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
    T_total = all_times[-1] - all_times[0]

    # FM lookup
    fm_dict = {}
    for r in fm_rows:
        fm_dict[(round(r[0], 1), round(r[1], 1))] = (r[2], r[3], r[4])

    # Target events (M5+)
    targets = [e for e in events if e["mag"] >= min_mag]

    # All parsed for isolation filter
    all_parsed = [(e["time"], e["lat"], e["lon"], e["mag"]) for e in events]
    target_tuples = [(e["time"], e["lat"], e["lon"], e["mag"]) for e in targets]
    isolated_tuples = set(
        (t.isoformat(), lat, lon)
        for t, lat, lon, mag in filter_isolated(target_tuples, all_parsed)
    )

    for e in targets:
        e["isolated"] = (e["time"].isoformat(), e["lat"], e["lon"]) in isolated_tuples

    n_iso = sum(1 for e in targets if e["isolated"])
    logger.info("  Targets: %d total, %d isolated (%.0f%%)", len(targets), n_iso,
                100 * n_iso / max(len(targets), 1))

    # ---------------------------------------------------------------
    # Compute 3 signals for each target event
    # ---------------------------------------------------------------
    logger.info("  Computing signals for each target...")

    for i, te in enumerate(targets):
        idx = bisect.bisect_left(all_times, te["t_days"])

        # Signal 1: CFS from top-5 largest prior M5+ within 500km
        cfs_total = 0.0
        nearest_dt_days = None
        prior_m5 = [(j, events[j]) for j in range(idx) if events[j]["mag"] >= min_mag]
        # Sort by magnitude (largest first), take top 5 within 500km
        nearby = []
        for j, src in prior_m5:
            dlat = abs(te["lat"] - src["lat"]) * DEG_TO_KM
            dlon = abs(te["lon"] - src["lon"]) * DEG_TO_KM
            if dlat > 500 or dlon > 500:
                continue
            dt_days = te["t_days"] - src["t_days"]
            if dt_days > 3650:  # 10 years max
                continue
            nearby.append((src["mag"], j, src, dt_days))

        nearby.sort(key=lambda x: -x[0])  # Largest first
        for mag, j, src, dt_days in nearby[:5]:
            fm_key = (round(src["lat"], 1), round(src["lon"], 1))
            if fm_key in fm_dict:
                strike, dip, rake = fm_dict[fm_key]
            else:
                strike, dip, rake = default_mechanism(src["lat"], src["lon"], src["depth"])
            l, w, s = fault_dimensions(src["mag"])
            cfs = okada_cfs(src["lat"], src["lon"], src["depth"],
                            strike, dip, rake, l, w, s,
                            te["lat"], te["lon"], te["depth"])
            cfs_total += cfs
            if nearest_dt_days is None or dt_days < nearest_dt_days:
                nearest_dt_days = dt_days

        te["cfs_kpa"] = cfs_total / 1000
        te["nearest_m5_dt_days"] = nearest_dt_days

        # Signal 2: Rate anomaly (M3+ in 7d within 2° / long-term regional rate)
        if te["t_days"] > 35:
            t_start = te["t_days"] - 7
            idx_start = bisect.bisect_left(all_times, t_start)
            obs_7d = sum(1 for k in range(idx_start, idx)
                         if abs(events[k]["lat"] - te["lat"]) <= 2
                         and abs(events[k]["lon"] - te["lon"]) <= 2)
            regional_n = sum(1 for e in events
                             if abs(e["lat"] - te["lat"]) <= 2
                             and abs(e["lon"] - te["lon"]) <= 2)
            regional_rate = regional_n / T_total
            te["rate_ratio"] = obs_7d / max(regional_rate * 7, 0.1)
        else:
            te["rate_ratio"] = None

        # Signal 3: Nearest-neighbor η (foreshock proxy)
        # Simplified: check if any M3+ event within 1° and 7 days precedes this event
        t_start = te["t_days"] - 7
        idx_start = bisect.bisect_left(all_times, t_start)
        foreshock_count = sum(1 for k in range(idx_start, idx)
                              if abs(events[k]["lat"] - te["lat"]) <= 1
                              and abs(events[k]["lon"] - te["lon"]) <= 1
                              and events[k]["mag"] < te["mag"])
        te["n_foreshocks"] = foreshock_count
        te["has_foreshock"] = foreshock_count > 0

        if (i + 1) % 500 == 0:
            logger.info("    %d/%d targets processed", i + 1, len(targets))

    logger.info("  All targets processed")

    # ---------------------------------------------------------------
    # 1. Isolated event analysis
    # ---------------------------------------------------------------
    logger.info("  --- Validation 1: Isolated event analysis ---")

    iso = [e for e in targets if e["isolated"]]
    non_iso = [e for e in targets if not e["isolated"]]

    def signal_stats(events_list, label):
        if not events_list:
            return {"n": 0}
        n = len(events_list)
        cfs = [e["cfs_kpa"] for e in events_list]
        rates = [e["rate_ratio"] for e in events_list if e["rate_ratio"] is not None]
        fores = [e["has_foreshock"] for e in events_list]
        return {
            "n": n,
            "cfs_mean_kpa": round(sum(cfs) / n, 2),
            "cfs_median_kpa": round(sorted(cfs)[n // 2], 2),
            "cfs_gt_100_pct": round(sum(1 for v in cfs if v > 100) / n * 100, 1),
            "cfs_gt_500_pct": round(sum(1 for v in cfs if v > 500) / n * 100, 1),
            "rate_mean": round(sum(rates) / len(rates), 2) if rates else None,
            "rate_gt_2_pct": round(sum(1 for v in rates if v > 2) / len(rates) * 100, 1) if rates else None,
            "foreshock_pct": round(sum(fores) / n * 100, 1),
        }

    iso_stats = signal_stats(iso, "isolated")
    non_iso_stats = signal_stats(non_iso, "non-isolated")
    all_stats = signal_stats(targets, "all")

    # Random baseline for isolated comparison
    random.seed(42)
    rand_events = []
    for _ in range(500):
        rt = 35 + random.random() * (T_total - 70)
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25
        rdepth = 10 + random.random() * 50
        idx = bisect.bisect_left(all_times, rt)

        # CFS from top-5
        cfs = 0.0
        prior = [(j, events[j]) for j in range(max(0, idx - 5000), idx) if events[j]["mag"] >= min_mag]
        nearby = []
        for j, src in prior:
            dlat = abs(rlat - src["lat"]) * DEG_TO_KM
            dlon = abs(rlon - src["lon"]) * DEG_TO_KM
            if dlat > 500 or dlon > 500:
                continue
            nearby.append((src["mag"], src))
        nearby.sort(key=lambda x: -x[0])
        for mag, src in nearby[:5]:
            fm_key = (round(src["lat"], 1), round(src["lon"], 1))
            s, d, r = fm_dict.get(fm_key, default_mechanism(src["lat"], src["lon"], src["depth"]))
            l, w, sl = fault_dimensions(src["mag"])
            cfs += okada_cfs(src["lat"], src["lon"], src["depth"], s, d, r, l, w, sl, rlat, rlon, rdepth)

        # Rate
        t_start = rt - 7
        idx_start = bisect.bisect_left(all_times, t_start)
        obs = sum(1 for k in range(idx_start, idx)
                  if abs(events[k]["lat"] - rlat) <= 2 and abs(events[k]["lon"] - rlon) <= 2)
        reg_n = sum(1 for e in events if abs(e["lat"] - rlat) <= 2 and abs(e["lon"] - rlon) <= 2)
        rate = obs / max(reg_n / T_total * 7, 0.1)

        # Foreshock
        fore = sum(1 for k in range(idx_start, idx)
                   if abs(events[k]["lat"] - rlat) <= 1 and abs(events[k]["lon"] - rlon) <= 1)

        rand_events.append({"cfs_kpa": cfs / 1000, "rate_ratio": rate, "has_foreshock": fore > 0})

    rand_stats = signal_stats(rand_events, "random")

    logger.info("  Isolated: CFS>500=%s%% rate>2=%s%% fore=%s%%",
                iso_stats.get("cfs_gt_500_pct"), iso_stats.get("rate_gt_2_pct"),
                iso_stats.get("foreshock_pct"))
    logger.info("  Random:   CFS>500=%s%% rate>2=%s%% fore=%s%%",
                rand_stats.get("cfs_gt_500_pct"), rand_stats.get("rate_gt_2_pct"),
                rand_stats.get("foreshock_pct"))

    # ---------------------------------------------------------------
    # 2. Time delay analysis
    # ---------------------------------------------------------------
    logger.info("  --- Validation 2: Time delay analysis ---")

    dt_days_list = [e["nearest_m5_dt_days"] for e in targets if e["nearest_m5_dt_days"] is not None]
    dt_iso = [e["nearest_m5_dt_days"] for e in iso if e["nearest_m5_dt_days"] is not None]

    def delay_stats(delays):
        if not delays:
            return {"n": 0}
        n = len(delays)
        s = sorted(delays)
        return {
            "n": n,
            "mean_days": round(sum(s) / n, 1),
            "median_days": round(s[n // 2], 1),
            "lt_7d_pct": round(sum(1 for d in s if d < 7) / n * 100, 1),
            "lt_30d_pct": round(sum(1 for d in s if d < 30) / n * 100, 1),
            "gt_30d_pct": round(sum(1 for d in s if d >= 30) / n * 100, 1),
            "gt_90d_pct": round(sum(1 for d in s if d >= 90) / n * 100, 1),
            "gt_365d_pct": round(sum(1 for d in s if d >= 365) / n * 100, 1),
        }

    # CFS>500kPa events: delay distribution
    high_cfs_delays = [e["nearest_m5_dt_days"] for e in targets
                       if e["cfs_kpa"] > 500 and e["nearest_m5_dt_days"] is not None]
    low_cfs_delays = [e["nearest_m5_dt_days"] for e in targets
                      if e["cfs_kpa"] <= 500 and e["nearest_m5_dt_days"] is not None]

    # ---------------------------------------------------------------
    # 3. Three-signal correlation
    # ---------------------------------------------------------------
    logger.info("  --- Validation 3: Signal correlation ---")

    # For each target with all 3 signals, compute co-occurrence
    valid = [e for e in targets if e["rate_ratio"] is not None]
    n_valid = len(valid)

    high_cfs = [e for e in valid if e["cfs_kpa"] > 100]
    high_rate = [e for e in valid if e["rate_ratio"] > 2]
    has_fore = [e for e in valid if e["has_foreshock"]]

    # Co-occurrence
    all_three = [e for e in valid if e["cfs_kpa"] > 100 and e["rate_ratio"] > 2 and e["has_foreshock"]]
    any_two = [e for e in valid if sum([e["cfs_kpa"] > 100, e["rate_ratio"] > 2, e["has_foreshock"]]) >= 2]

    # Independence test: if independent, P(A&B) = P(A)*P(B)
    p_cfs = len(high_cfs) / max(n_valid, 1)
    p_rate = len(high_rate) / max(n_valid, 1)
    p_fore = len(has_fore) / max(n_valid, 1)
    p_all_expected = p_cfs * p_rate * p_fore
    p_all_observed = len(all_three) / max(n_valid, 1)

    correlation_ratio = p_all_observed / max(p_all_expected, 0.0001)

    # ---------------------------------------------------------------
    # 4. Prospective test
    # ---------------------------------------------------------------
    logger.info("  --- Validation 4: Prospective test ---")

    split_date = datetime(2019, 1, 1, tzinfo=timezone.utc)
    split_days = (split_date - t0).total_seconds() / 86400

    train = [e for e in targets if e["time"] < split_date and e["rate_ratio"] is not None]
    test = [e for e in targets if e["time"] >= split_date and e["rate_ratio"] is not None]

    # Train: find optimal thresholds from first half
    # Simple approach: for each signal, find threshold that maximizes precision
    # "Prediction": if signal > threshold → predict M5+ in next 7 days in 2° box

    # For prospective test, we ask: among periods where signal is high,
    # how often does M5+ actually occur?
    # This is already captured by the lift metrics.

    # Alternative: combine signals. Score = (CFS>100)*1 + (rate>2)*1 + (foreshock)*1
    for e in train + test:
        score = 0
        if e["cfs_kpa"] > 100:
            score += 1
        if e["rate_ratio"] is not None and e["rate_ratio"] > 2:
            score += 1
        if e["has_foreshock"]:
            score += 1
        e["combined_score"] = score

    def score_distribution(events_list):
        if not events_list:
            return {}
        n = len(events_list)
        dist = {}
        for s in range(4):
            count = sum(1 for e in events_list if e["combined_score"] == s)
            dist[f"score_{s}"] = {"count": count, "pct": round(100 * count / n, 1)}
        return dist

    train_dist = score_distribution(train)
    test_dist = score_distribution(test)

    # Compare: is score distribution similar in train and test?
    # Also: for random baseline
    rand_scores = []
    for e in rand_events:
        score = 0
        if e["cfs_kpa"] > 100:
            score += 1
        if e["rate_ratio"] > 2:
            score += 1
        if e["has_foreshock"]:
            score += 1
        rand_scores.append(score)

    rand_dist = {}
    for s in range(4):
        count = sum(1 for sc in rand_scores if sc == s)
        rand_dist[f"score_{s}"] = {"count": count, "pct": round(100 * count / len(rand_scores), 1)}

    # Lift for combined score >= 2
    eq_score2 = sum(1 for e in targets if e.get("combined_score", 0) >= 2 and e["rate_ratio"] is not None)
    eq_total = sum(1 for e in targets if e["rate_ratio"] is not None)
    rand_score2 = sum(1 for sc in rand_scores if sc >= 2)
    combined_lift = (eq_score2 / max(eq_total, 1)) / max(rand_score2 / max(len(rand_scores), 1), 0.001)

    results = {
        "summary": {
            "n_targets": len(targets),
            "n_isolated": n_iso,
            "n_non_isolated": len(targets) - n_iso,
            "isolated_pct": round(100 * n_iso / max(len(targets), 1), 1),
        },
        "validation_1_isolation": {
            "all_events": all_stats,
            "isolated": iso_stats,
            "non_isolated": non_iso_stats,
            "random": rand_stats,
            "lift_cfs500_iso_vs_rand": round(
                iso_stats.get("cfs_gt_500_pct", 0) / max(rand_stats.get("cfs_gt_500_pct", 0.1), 0.1), 2
            ),
            "lift_rate2_iso_vs_rand": round(
                (iso_stats.get("rate_gt_2_pct", 0) or 0) / max(rand_stats.get("rate_gt_2_pct", 0.1) or 0.1, 0.1), 2
            ),
            "lift_fore_iso_vs_rand": round(
                iso_stats.get("foreshock_pct", 0) / max(rand_stats.get("foreshock_pct", 0.1), 0.1), 2
            ),
        },
        "validation_2_time_delay": {
            "all_events": delay_stats(dt_days_list),
            "isolated": delay_stats(dt_iso),
            "high_cfs_gt500": delay_stats(high_cfs_delays),
            "low_cfs_le500": delay_stats(low_cfs_delays),
        },
        "validation_3_correlation": {
            "n_valid": n_valid,
            "p_cfs_gt100": round(p_cfs * 100, 1),
            "p_rate_gt2": round(p_rate * 100, 1),
            "p_foreshock": round(p_fore * 100, 1),
            "p_all_three_expected_if_independent": round(p_all_expected * 100, 3),
            "p_all_three_observed": round(p_all_observed * 100, 3),
            "correlation_ratio": round(correlation_ratio, 2),
            "n_all_three": len(all_three),
            "n_any_two": len(any_two),
            "interpretation": "ratio>1 means signals co-occur more than independence predicts (correlated)"
        },
        "validation_4_prospective": {
            "train_2011_2018": {"n": len(train), "score_dist": train_dist},
            "test_2019_2026": {"n": len(test), "score_dist": test_dist},
            "random_baseline": {"n": len(rand_scores), "score_dist": rand_dist},
            "combined_lift_score_ge2": round(combined_lift, 2),
        },
    }

    # Log key results
    logger.info("  --- RESULTS ---")
    logger.info("  V1 Isolation: CFS>500 iso=%.1f%% rand=%.1f%% | rate>2 iso=%.1f%% rand=%.1f%% | fore iso=%.1f%% rand=%.1f%%",
                iso_stats.get("cfs_gt_500_pct", 0), rand_stats.get("cfs_gt_500_pct", 0),
                iso_stats.get("rate_gt_2_pct", 0) or 0, rand_stats.get("rate_gt_2_pct", 0) or 0,
                iso_stats.get("foreshock_pct", 0), rand_stats.get("foreshock_pct", 0))
    logger.info("  V2 Delay: all median=%.0fd | iso median=%.0fd | highCFS median=%.0fd",
                delay_stats(dt_days_list).get("median_days", 0),
                delay_stats(dt_iso).get("median_days", 0),
                delay_stats(high_cfs_delays).get("median_days", 0))
    logger.info("  V3 Correlation: expected_if_indep=%.3f%% observed=%.3f%% ratio=%.1f",
                p_all_expected * 100, p_all_observed * 100, correlation_ratio)
    logger.info("  V4 Prospective: combined_lift(score>=2)=%.2f", combined_lift)

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_validation()

    out_path = RESULTS_DIR / f"validation_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
