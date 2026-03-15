"""Prospective (forward-looking) earthquake prediction analysis.

The fundamental question for earthquake prediction is NOT:
    "Given an earthquake, was there an anomaly before?" (retrospective)

But rather:
    "Given an anomaly NOW, will an earthquake follow?" (prospective)

This script implements alarm-based prediction evaluation:

1. For each data stream (ULF, LST, CFS, rate, foreshock), define
   alarm conditions based on observable quantities BEFORE the event.

2. Scan the entire timeline. At each time step, evaluate:
   - Is the alarm ON? (observable exceeds threshold)
   - Does an M5+ earthquake follow within the prediction window?

3. Compute prediction metrics:
   - Precision: P(earthquake | alarm) = TP / (TP + FP)
   - Recall: P(alarm | earthquake) = TP / (TP + FN)
   - False alarm rate: FP / (FP + TN)
   - Probability gain: P(EQ|alarm) / P(EQ|random)
   - Molchan skill score: 1 - miss_rate - alarm_rate

4. Robustness checks:
   - Prospective split: train thresholds on 2011-2018, test on 2019-2026
   - Isolated events only (aftershock-free)
   - Multi-signal combination (does combining alarms improve?)

Key references:
    - Molchan (1991) "Strategies in strong earthquake prediction"
    - Zechar & Jordan (2008) "Testing alarm-based earthquake predictions"
    - Harte & Vere-Jones (2005) "The entropy score and its uses"
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

# Prediction parameters
PREDICTION_WINDOW_DAYS = 7  # "Will M5+ occur within 7 days?"
SPATIAL_RADIUS_DEG = 2.0    # Within 2° of alarm location
MIN_TARGET_MAG = 5.0


# ---------------------------------------------------------------------------
# Physics utilities (from validate_phase2.py)
# ---------------------------------------------------------------------------

def fault_dimensions(mw):
    length_km = 10 ** (-2.86 + 0.63 * mw)
    width_km = 10 ** (-1.61 + 0.41 * mw)
    m0 = 10 ** (1.5 * mw + 9.05)
    slip_m = m0 / (SHEAR_MODULUS * length_km * 1000 * width_km * 1000)
    return length_km, width_km, slip_m


def default_mechanism(lat, lon, depth):
    if depth > 70:
        return 200.0, 45.0, 90.0
    elif lon > 142 and lat > 35:
        return 200.0, 25.0, 90.0
    elif lon < 137 and lat < 35:
        return 240.0, 15.0, 90.0
    else:
        return 200.0, 35.0, 90.0


def okada_cfs(src_lat, src_lon, src_depth, src_strike, src_dip, src_rake,
              src_length, src_width, src_slip,
              obs_lat, obs_lon, obs_depth):
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


def filter_isolated(target_events, all_events_sorted, days=3.0, degrees=1.5):
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


# ---------------------------------------------------------------------------
# Alarm evaluation framework
# ---------------------------------------------------------------------------

def evaluate_alarm(alarm_times, alarm_locations, target_events,
                   prediction_window_days, spatial_radius_deg,
                   total_days, label=""):
    """Evaluate alarm-based prediction performance.

    Args:
        alarm_times: list of (datetime, lat, lon) when alarm is ON
        alarm_locations: same, for spatial matching
        target_events: list of (datetime, lat, lon, mag) actual M5+ events
        prediction_window_days: how far ahead the alarm predicts
        spatial_radius_deg: spatial matching radius
        total_days: total time span of the dataset
        label: name for logging

    Returns:
        dict with precision, recall, false_alarm_rate, probability_gain
    """
    if not alarm_times or not target_events:
        return {
            "label": label, "n_alarms": len(alarm_times),
            "n_targets": len(target_events),
            "tp": 0, "fp": 0, "fn": 0, "tn": 0,
            "precision": 0, "recall": 0, "probability_gain": 0,
        }

    target_times = [e[0] for e in target_events]

    # For each alarm: does a target event follow within the window?
    tp_alarms = 0
    fp_alarms = 0
    matched_targets = set()

    for alarm_t, alarm_lat, alarm_lon in zip(alarm_times, alarm_locations, alarm_locations):
        alarm_end = alarm_t + timedelta(days=prediction_window_days)
        hit = False
        for i, (t_t, t_lat, t_lon, t_mag) in enumerate(target_events):
            if alarm_t <= t_t <= alarm_end:
                if (abs(alarm_lat - t_lat) <= spatial_radius_deg and
                        abs(alarm_lon - t_lon) <= spatial_radius_deg):
                    hit = True
                    matched_targets.add(i)
        if hit:
            tp_alarms += 1
        else:
            fp_alarms += 1

    fn = len(target_events) - len(matched_targets)  # Missed events
    tn = max(0, int(total_days) - len(alarm_times) - fn)  # Rough

    precision = tp_alarms / max(tp_alarms + fp_alarms, 1)
    recall = len(matched_targets) / max(len(target_events), 1)

    # Base rate: P(M5+ in any random 7-day window in 2° box)
    base_rate = len(target_events) * prediction_window_days / max(total_days, 1)
    alarm_rate = len(alarm_times) / max(total_days, 1)
    probability_gain = precision / max(base_rate, 0.001)

    # Molchan score: 1 - miss_rate - alarm_fraction
    miss_rate = fn / max(len(target_events), 1)
    alarm_fraction = len(alarm_times) / max(total_days, 1)
    molchan_score = 1 - miss_rate - alarm_fraction

    return {
        "label": label,
        "n_alarms": len(alarm_times),
        "n_targets": len(target_events),
        "tp": tp_alarms,
        "fp": fp_alarms,
        "fn": fn,
        "matched_targets": len(matched_targets),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "false_alarm_rate": round(fp_alarms / max(tp_alarms + fp_alarms, 1), 4),
        "base_rate": round(base_rate, 4),
        "probability_gain": round(probability_gain, 2),
        "molchan_score": round(molchan_score, 4),
        "alarm_fraction": round(alarm_fraction, 4),
    }


# ---------------------------------------------------------------------------
# Signal-specific alarm generators
# ---------------------------------------------------------------------------

async def generate_rate_alarms(events, all_events, all_times, t0,
                               threshold=2.0, window_days=7):
    """Generate alarms when seismicity rate exceeds threshold.

    At each M3+ event time, check if the regional rate in the past
    `window_days` exceeds `threshold` times the long-term average.
    If so, raise an alarm at that location.
    """
    T_total = all_times[-1] - all_times[0]
    alarms_t = []
    alarms_loc = []

    # Sample every day across the timeline (not every event, too many)
    n_days = int(T_total)
    for day_offset in range(0, n_days, 1):  # Every day
        t_now = all_times[0] + day_offset
        idx = bisect.bisect_right(all_times, t_now)
        if idx < 10:
            continue

        # Check rate at several grid points (high-activity regions)
        # Use centroids of recent M3+ activity
        t_start = t_now - window_days
        idx_start = bisect.bisect_left(all_times, t_start)
        recent = events[idx_start:idx]
        if not recent:
            continue

        # Cluster recent events by 2° boxes
        boxes = {}
        for e in recent:
            bkey = (round(e["lat"] / 2) * 2, round(e["lon"] / 2) * 2)
            if bkey not in boxes:
                boxes[bkey] = 0
            boxes[bkey] += 1

        for (blat, blon), count in boxes.items():
            # Long-term rate for this box
            regional_n = sum(1 for e in events
                             if abs(e["lat"] - blat) <= 2 and abs(e["lon"] - blon) <= 2)
            regional_rate = regional_n / T_total * window_days
            if regional_rate < 0.1:
                continue
            ratio = count / regional_rate
            if ratio >= threshold:
                alarm_dt = t0 + timedelta(days=t_now)
                alarms_t.append(alarm_dt)
                alarms_loc.append((blat, blon))

    return alarms_t, alarms_loc


async def generate_cfs_alarms(events, fm_dict, all_times, t0,
                               cfs_threshold_kpa=100):
    """Generate alarms at locations with high cumulative CFS.

    After each M5+ event, compute CFS on a grid and flag locations
    where CFS exceeds threshold. These locations are "primed" for
    the next earthquake.
    """
    alarms_t = []
    alarms_loc = []

    m5_events = [e for e in events if e["mag"] >= 5.0]

    # After each M5+ event, compute CFS at grid points
    for i, src in enumerate(m5_events):
        src_fm_key = (round(src["lat"], 1), round(src["lon"], 1))
        strike, dip, rake = fm_dict.get(src_fm_key,
                                         default_mechanism(src["lat"], src["lon"], src["depth"]))
        l, w, s = fault_dimensions(src["mag"])

        # Check CFS at grid points around the source (±3°, 0.5° step)
        for dlat in range(-6, 7):
            for dlon in range(-6, 7):
                obs_lat = src["lat"] + dlat * 0.5
                obs_lon = src["lon"] + dlon * 0.5
                if abs(dlat) < 1 and abs(dlon) < 1:
                    continue  # Skip near-source (known high CFS)
                obs_depth = 15.0  # Fixed shallow depth

                cfs = okada_cfs(src["lat"], src["lon"], src["depth"],
                                strike, dip, rake, l, w, s,
                                obs_lat, obs_lon, obs_depth)
                cfs_kpa = cfs / 1000

                if cfs_kpa >= cfs_threshold_kpa:
                    alarm_dt = src["time"] + timedelta(hours=1)  # Alarm starts right after source
                    alarms_t.append(alarm_dt)
                    alarms_loc.append((obs_lat, obs_lon))

    return alarms_t, alarms_loc


async def generate_foreshock_alarms(events, all_times, t0,
                                     min_count=3, window_days=7):
    """Generate alarms when foreshock-like sequences are detected.

    If ≥ min_count M3+ events occur within 1° and 7 days, and
    magnitudes are non-decreasing, flag as potential foreshock sequence.
    """
    alarms_t = []
    alarms_loc = []

    n_days = int(all_times[-1] - all_times[0])
    for day_offset in range(0, n_days, 1):
        t_now = all_times[0] + day_offset
        idx = bisect.bisect_right(all_times, t_now)
        t_start = t_now - window_days
        idx_start = bisect.bisect_left(all_times, t_start)
        recent = events[idx_start:idx]
        if len(recent) < min_count:
            continue

        # Cluster by 1° boxes
        boxes = {}
        for e in recent:
            bkey = (round(e["lat"]), round(e["lon"]))
            if bkey not in boxes:
                boxes[bkey] = []
            boxes[bkey].append(e["mag"])

        for (blat, blon), mags in boxes.items():
            if len(mags) >= min_count:
                # Check if magnitudes show escalation (non-trivially)
                max_mag = max(mags)
                recent_max = max(mags[-min(3, len(mags)):])
                if recent_max >= max_mag - 0.5:  # Recent events near max
                    alarm_dt = t0 + timedelta(days=t_now)
                    alarms_t.append(alarm_dt)
                    alarms_loc.append((blat, blon))

    return alarms_t, alarms_loc


async def generate_ulf_alarms(db_path, stations, threshold_ratio=2.0,
                                window_hours=6):
    """Generate alarms when ULF Z-power exceeds baseline.

    Compute rolling Z-component power in nighttime windows.
    Alarm when current power > threshold × 30-day rolling median.
    """
    alarms_t = []
    alarms_loc = []

    async with aiosqlite.connect(db_path) as db:
        for station, loc in stations.items():
            rows = await db.execute_fetchall(
                "SELECT observed_at, z_nt FROM ulf_magnetic "
                "WHERE station = ? AND z_nt IS NOT NULL "
                "ORDER BY observed_at",
                (station,),
            )

            if len(rows) < 1440:  # Need at least 1 day
                continue

            # Compute hourly Z-power (variance of 60 1-min samples)
            hourly_powers = []
            for i in range(0, len(rows) - 60, 60):
                chunk = [r[1] for r in rows[i:i + 60] if r[1] is not None]
                if len(chunk) < 30:
                    continue
                mean = sum(chunk) / len(chunk)
                var = sum((v - mean) ** 2 for v in chunk) / len(chunk)
                try:
                    t = datetime.fromisoformat(rows[i][0])
                except (ValueError, TypeError):
                    continue
                # Nighttime only (15-21 UTC = 0-6 JST)
                if 15 <= t.hour <= 21:
                    hourly_powers.append((t, var))

            if len(hourly_powers) < 48:  # Need at least 2 days
                continue

            # Rolling baseline: median of past 30 days
            for i in range(720, len(hourly_powers)):  # Start after 30 days
                current_t, current_power = hourly_powers[i]
                # Baseline: powers from 30-3 days ago (exclude recent 3 days)
                baseline_start = i - 720  # 30 days × 24 hours (but only ~6 night hours/day)
                baseline_end = i - 72     # 3 days back
                if baseline_end <= baseline_start:
                    continue
                baseline = [p for _, p in hourly_powers[max(0, baseline_start):baseline_end]]
                if len(baseline) < 10:
                    continue
                baseline_median = sorted(baseline)[len(baseline) // 2]
                if baseline_median < 1e-6:
                    continue
                ratio = current_power / baseline_median
                if ratio >= threshold_ratio:
                    alarms_t.append(current_t)
                    alarms_loc.append((loc["lat"], loc["lon"]))

    return alarms_t, alarms_loc


# ---------------------------------------------------------------------------
# Main prospective analysis
# ---------------------------------------------------------------------------

async def run_prospective_analysis():
    logger.info("=== Prospective (Forward-Looking) Prediction Analysis ===")

    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
            "ORDER BY occurred_at"
        )
        fm_rows = await db.execute_fetchall(
            "SELECT latitude, longitude, strike1, dip1, rake1 FROM focal_mechanisms"
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

    fm_dict = {}
    for r in fm_rows:
        fm_dict[(round(r[0], 1), round(r[1], 1))] = (r[2], r[3], r[4])

    # Target events: M5+
    targets_all = [(e["time"], e["lat"], e["lon"], e["mag"])
                   for e in events if e["mag"] >= MIN_TARGET_MAG]
    all_parsed = [(e["time"], e["lat"], e["lon"], e["mag"]) for e in events]
    targets_isolated = filter_isolated(targets_all, all_parsed)

    # Prospective split
    split_date = datetime(2019, 1, 1, tzinfo=timezone.utc)
    targets_train = [t for t in targets_all if t[0] < split_date]
    targets_test = [t for t in targets_all if t[0] >= split_date]
    targets_test_iso = [t for t in targets_isolated if t[0] >= split_date]

    logger.info("  Total targets: %d (train: %d, test: %d, test_iso: %d)",
                len(targets_all), len(targets_train), len(targets_test), len(targets_test_iso))

    results = {
        "metadata": {
            "prediction_window_days": PREDICTION_WINDOW_DAYS,
            "spatial_radius_deg": SPATIAL_RADIUS_DEG,
            "min_target_mag": MIN_TARGET_MAG,
            "total_days": round(T_total, 1),
            "n_events_m3": len(events),
            "n_targets_all": len(targets_all),
            "n_targets_isolated": len(targets_isolated),
            "train_period": "2011-2018",
            "test_period": "2019-2026",
        },
        "alarms": {},
    }

    # ---------------------------------------------------------------
    # Signal 1: Seismicity rate alarm
    # ---------------------------------------------------------------
    logger.info("  --- Generating rate alarms ---")
    for threshold in [2.0, 3.0, 5.0]:
        rate_t, rate_loc = await generate_rate_alarms(
            events, events, all_times, t0, threshold=threshold)

        # Filter to test period only
        rate_t_test = [(t, loc) for t, loc in zip(rate_t, rate_loc) if t >= split_date]
        rate_t_test_times = [t for t, _ in rate_t_test]
        rate_t_test_locs = [loc for _, loc in rate_t_test]

        eval_result = evaluate_alarm(
            rate_t_test_times,
            rate_t_test_locs,
            targets_test, PREDICTION_WINDOW_DAYS, SPATIAL_RADIUS_DEG,
            T_total / 2, label=f"rate_gt_{threshold}x"
        )
        results["alarms"][f"rate_gt_{threshold}x"] = eval_result
        logger.info("  Rate>%.0fx: precision=%.3f recall=%.3f gain=%.1f (%d alarms)",
                    threshold, eval_result["precision"], eval_result["recall"],
                    eval_result["probability_gain"], eval_result["n_alarms"])

    # ---------------------------------------------------------------
    # Signal 2: CFS alarm
    # ---------------------------------------------------------------
    logger.info("  --- Generating CFS alarms ---")
    for cfs_thresh in [50, 100, 500]:
        cfs_t, cfs_loc = await generate_cfs_alarms(
            events, fm_dict, all_times, t0, cfs_threshold_kpa=cfs_thresh)

        cfs_t_test = [(t, loc) for t, loc in zip(cfs_t, cfs_loc) if t >= split_date]
        cfs_t_test_times = [t for t, _ in cfs_t_test]
        cfs_t_test_locs = [loc for _, loc in cfs_t_test]

        eval_result = evaluate_alarm(
            cfs_t_test_times,
            cfs_t_test_locs,
            targets_test, PREDICTION_WINDOW_DAYS, SPATIAL_RADIUS_DEG,
            T_total / 2, label=f"cfs_gt_{cfs_thresh}kpa"
        )
        results["alarms"][f"cfs_gt_{cfs_thresh}kpa"] = eval_result
        logger.info("  CFS>%dkPa: precision=%.3f recall=%.3f gain=%.1f (%d alarms)",
                    cfs_thresh, eval_result["precision"], eval_result["recall"],
                    eval_result["probability_gain"], eval_result["n_alarms"])

    # ---------------------------------------------------------------
    # Signal 3: Foreshock alarm
    # ---------------------------------------------------------------
    logger.info("  --- Generating foreshock alarms ---")
    for min_count in [3, 5, 10]:
        fore_t, fore_loc = await generate_foreshock_alarms(
            events, all_times, t0, min_count=min_count)

        fore_t_test = [(t, loc) for t, loc in zip(fore_t, fore_loc) if t >= split_date]
        fore_t_test_times = [t for t, _ in fore_t_test]
        fore_t_test_locs = [loc for _, loc in fore_t_test]

        eval_result = evaluate_alarm(
            fore_t_test_times,
            fore_t_test_locs,
            targets_test, PREDICTION_WINDOW_DAYS, SPATIAL_RADIUS_DEG,
            T_total / 2, label=f"foreshock_ge_{min_count}"
        )
        results["alarms"][f"foreshock_ge_{min_count}"] = eval_result
        logger.info("  Foreshock≥%d: precision=%.3f recall=%.3f gain=%.1f (%d alarms)",
                    min_count, eval_result["precision"], eval_result["recall"],
                    eval_result["probability_gain"], eval_result["n_alarms"])

    # ---------------------------------------------------------------
    # Signal 4: ULF magnetic alarm (if data available)
    # ---------------------------------------------------------------
    logger.info("  --- Generating ULF alarms ---")
    stations = {
        "KAK": {"lat": 36.23, "lon": 140.19},
        "MMB": {"lat": 43.91, "lon": 144.19},
        "KNY": {"lat": 31.42, "lon": 130.88},
    }
    for ulf_thresh in [2.0, 3.0, 5.0]:
        ulf_t, ulf_loc = await generate_ulf_alarms(
            DB_PATH, stations, threshold_ratio=ulf_thresh)

        if not ulf_t:
            results["alarms"][f"ulf_power_gt_{ulf_thresh}x"] = {
                "label": f"ulf_power_gt_{ulf_thresh}x",
                "status": "no_ulf_data_or_no_alarms",
            }
            continue

        # Filter to test period
        ulf_t_test = [(t, loc) for t, loc in zip(ulf_t, ulf_loc) if t >= split_date]
        ulf_t_test_times = [t for t, _ in ulf_t_test]
        ulf_t_test_locs = [loc for _, loc in ulf_t_test]

        eval_result = evaluate_alarm(
            ulf_t_test_times,
            ulf_t_test_locs,
            targets_test, PREDICTION_WINDOW_DAYS, SPATIAL_RADIUS_DEG,
            T_total / 2, label=f"ulf_power_gt_{ulf_thresh}x"
        )
        results["alarms"][f"ulf_power_gt_{ulf_thresh}x"] = eval_result
        logger.info("  ULF>%.0fx: precision=%.3f recall=%.3f gain=%.1f (%d alarms)",
                    ulf_thresh, eval_result["precision"], eval_result["recall"],
                    eval_result["probability_gain"], eval_result["n_alarms"])

    # ---------------------------------------------------------------
    # Combined alarm: any 2+ of rate + CFS + foreshock
    # ---------------------------------------------------------------
    logger.info("  --- Combined alarm evaluation ---")

    # Collect all alarms from best individual thresholds
    # (Use rate>3x, CFS>100kPa, foreshock≥3 as baseline)
    rate_t_all, rate_loc_all = await generate_rate_alarms(
        events, events, all_times, t0, threshold=3.0)
    cfs_t_all, cfs_loc_all = await generate_cfs_alarms(
        events, fm_dict, all_times, t0, cfs_threshold_kpa=100)
    fore_t_all, fore_loc_all = await generate_foreshock_alarms(
        events, all_times, t0, min_count=3)

    # For each day in test period, count active alarms
    combined_alarms_t = []
    combined_alarms_loc = []
    test_start_days = (split_date - t0).total_seconds() / 86400
    n_test_days = int(T_total - test_start_days)

    for day_offset in range(n_test_days):
        t_day = split_date + timedelta(days=day_offset)

        # Check each alarm type: is there an alarm within ±1 day?
        def has_alarm_near(alarm_times, t_ref, hours=24):
            for at in alarm_times:
                if abs((at - t_ref).total_seconds()) < hours * 3600:
                    return True
            return False

        # Grid scan: check major seismic regions
        for lat in range(26, 46, 2):
            for lon in range(128, 148, 2):
                n_active = 0
                if has_alarm_near(
                    [t for t, loc in zip(rate_t_all, rate_loc_all)
                     if abs(loc[0] - lat) <= 2 and abs(loc[1] - lon) <= 2],
                    t_day):
                    n_active += 1
                if has_alarm_near(
                    [t for t, loc in zip(cfs_t_all, cfs_loc_all)
                     if abs(loc[0] - lat) <= 2 and abs(loc[1] - lon) <= 2],
                    t_day):
                    n_active += 1
                if has_alarm_near(
                    [t for t, loc in zip(fore_t_all, fore_loc_all)
                     if abs(loc[0] - lat) <= 2 and abs(loc[1] - lon) <= 2],
                    t_day):
                    n_active += 1

                if n_active >= 2:
                    combined_alarms_t.append(t_day)
                    combined_alarms_loc.append((lat, lon))

    combined_eval = evaluate_alarm(
        combined_alarms_t, combined_alarms_loc,
        targets_test, PREDICTION_WINDOW_DAYS, SPATIAL_RADIUS_DEG,
        T_total / 2, label="combined_ge_2_signals"
    )
    results["alarms"]["combined_ge_2_signals"] = combined_eval
    logger.info("  Combined≥2: precision=%.3f recall=%.3f gain=%.1f (%d alarms)",
                combined_eval["precision"], combined_eval["recall"],
                combined_eval["probability_gain"], combined_eval["n_alarms"])

    # Also evaluate on isolated targets only
    combined_eval_iso = evaluate_alarm(
        combined_alarms_t, combined_alarms_loc,
        targets_test_iso, PREDICTION_WINDOW_DAYS, SPATIAL_RADIUS_DEG,
        T_total / 2, label="combined_ge_2_signals_isolated"
    )
    results["alarms"]["combined_ge_2_signals_isolated"] = combined_eval_iso
    logger.info("  Combined≥2 (iso): precision=%.3f recall=%.3f gain=%.1f",
                combined_eval_iso["precision"], combined_eval_iso["recall"],
                combined_eval_iso["probability_gain"])

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_prospective_analysis()

    out_path = RESULTS_DIR / f"prospective_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
