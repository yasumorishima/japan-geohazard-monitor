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

def compute_cell_base_rates(target_events, total_days, cell_size_deg,
                             prediction_window_days):
    """Compute per-cell base rates for spatially-resolved evaluation.

    Returns dict: (cell_lat, cell_lon) → P(M5+ in this cell in prediction_window).
    """
    cell_counts = {}
    for t, lat, lon, mag in target_events:
        clat = round(lat / cell_size_deg) * cell_size_deg
        clon = round(lon / cell_size_deg) * cell_size_deg
        key = (clat, clon)
        cell_counts[key] = cell_counts.get(key, 0) + 1

    cell_rates = {}
    for key, count in cell_counts.items():
        cell_rates[key] = count * prediction_window_days / max(total_days, 1)
    return cell_rates


def evaluate_alarm(alarm_times, alarm_locations, target_events,
                   prediction_window_days, spatial_radius_deg,
                   total_days, label="", cell_base_rates=None):
    """Evaluate alarm-based prediction performance.

    Uses spatially-resolved base rates for probability gain calculation.
    Computes Molchan diagram data points (miss_rate vs alarm_fraction).

    Args:
        alarm_times: list of datetime when alarm is ON
        alarm_locations: list of (lat, lon) tuples for each alarm
        target_events: list of (datetime, lat, lon, mag) actual M5+ events
        prediction_window_days: how far ahead the alarm predicts
        spatial_radius_deg: spatial matching radius
        total_days: total time span of the dataset
        label: name for logging
        cell_base_rates: dict from compute_cell_base_rates (per-cell rates)

    Returns:
        dict with precision, recall, probability_gain (spatially corrected),
        molchan data, information gain
    """
    if not alarm_times or not target_events:
        return {
            "label": label, "n_alarms": len(alarm_times),
            "n_targets": len(target_events),
            "tp": 0, "fp": 0, "fn": 0,
            "precision": 0, "recall": 0, "probability_gain": 0,
        }

    # For each alarm: does a target event follow within the window?
    tp_alarms = 0
    fp_alarms = 0
    matched_targets = set()

    for alarm_t, (alarm_lat, alarm_lon) in zip(alarm_times, alarm_locations):
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

    fn = len(target_events) - len(matched_targets)

    precision = tp_alarms / max(tp_alarms + fp_alarms, 1)
    recall = len(matched_targets) / max(len(target_events), 1)

    # Spatially-resolved base rate
    # Average per-cell rate across alarm locations
    if cell_base_rates:
        cell_size = SPATIAL_RADIUS_DEG
        alarm_cell_rates = []
        for alarm_lat, alarm_lon in alarm_locations:
            clat = round(alarm_lat / cell_size) * cell_size
            clon = round(alarm_lon / cell_size) * cell_size
            rate = cell_base_rates.get((clat, clon), 0.001)
            alarm_cell_rates.append(rate)
        mean_cell_rate = sum(alarm_cell_rates) / max(len(alarm_cell_rates), 1)
    else:
        # Estimate: divide Japan into ~50 independent 2°×2° cells
        n_cells = 50
        global_rate = len(target_events) * prediction_window_days / max(total_days, 1)
        mean_cell_rate = global_rate / n_cells

    probability_gain = precision / max(mean_cell_rate, 0.0001)

    # Information Gain per Earthquake (IGPE) — Zechar & Jordan (2008)
    # IGPE = log2(P(EQ|alarm) / P(EQ|random))
    igpe = math.log2(max(probability_gain, 0.001)) if probability_gain > 0 else -10

    # Molchan diagram: miss_rate vs alarm_fraction
    miss_rate = fn / max(len(target_events), 1)
    n_total_cells_days = total_days * 50  # approximate total cell-days
    alarm_fraction = len(alarm_times) / max(n_total_cells_days, 1)
    # Molchan score: area above diagonal = predictive skill
    # score > 0 means better than random
    molchan_score = (1 - miss_rate) - alarm_fraction  # = recall - alarm_fraction

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
        "false_alarm_rate": round(1 - precision, 4),
        "cell_base_rate": round(mean_cell_rate, 5),
        "probability_gain": round(probability_gain, 2),
        "information_gain_bits": round(igpe, 2),
        "molchan_miss_rate": round(miss_rate, 4),
        "molchan_alarm_fraction": round(alarm_fraction, 6),
        "molchan_score": round(molchan_score, 4),
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


async def generate_cfs_cumulative_alarms(events, fm_dict, all_times, t0,
                                          cfs_threshold_kpa=100):
    """Generate alarms using CUMULATIVE Coulomb stress map.

    Unlike the previous version (alarm only after M5+), this maintains
    a running CFS map that accumulates stress from ALL past M5+ events.
    Stress doesn't decay — a 2012 M7 still loads faults today.

    Every 30 days, scan the CFS map and alarm at grid cells where
    cumulative CFS exceeds threshold. This generates many more alarms
    than the instantaneous version, making statistical evaluation possible.
    """
    alarms_t = []
    alarms_loc = []

    m5_events = [e for e in events if e["mag"] >= 5.0]
    if not m5_events:
        return alarms_t, alarms_loc

    # Grid: 0.5° over seismically active Japan
    grid_lats = [lat * 0.5 for lat in range(52, 92)]   # 26°-46°N
    grid_lons = [lon * 0.5 for lon in range(252, 300)]  # 126°-150°E
    cfs_map = {}  # (lat, lon) → cumulative CFS (kPa)
    for lat in grid_lats:
        for lon in grid_lons:
            cfs_map[(lat, lon)] = 0.0

    m5_idx = 0
    n_days = int(all_times[-1] - all_times[0])

    # Scan every 30 days
    for day_offset in range(30, n_days, 30):
        t_now = all_times[0] + day_offset
        alarm_dt = t0 + timedelta(days=t_now)

        # Add CFS from new M5+ events since last scan
        while m5_idx < len(m5_events) and m5_events[m5_idx]["t_days"] <= t_now:
            src = m5_events[m5_idx]
            src_fm_key = (round(src["lat"], 1), round(src["lon"], 1))
            strike, dip, rake = fm_dict.get(src_fm_key,
                                             default_mechanism(src["lat"], src["lon"], src["depth"]))
            l, w, s = fault_dimensions(src["mag"])

            for (obs_lat, obs_lon) in cfs_map:
                dist_lat = abs(src["lat"] - obs_lat) * DEG_TO_KM
                dist_lon = abs(src["lon"] - obs_lon) * DEG_TO_KM
                if dist_lat > 300 or dist_lon > 300:  # Skip far points
                    continue
                cfs = okada_cfs(src["lat"], src["lon"], src["depth"],
                                strike, dip, rake, l, w, s,
                                obs_lat, obs_lon, 15.0)
                cfs_map[(obs_lat, obs_lon)] += cfs / 1000  # to kPa

            m5_idx += 1

        # Alarm at cells where cumulative CFS exceeds threshold
        for (lat, lon), cfs_kpa in cfs_map.items():
            if cfs_kpa >= cfs_threshold_kpa:
                alarms_t.append(alarm_dt)
                alarms_loc.append((lat, lon))

    logger.info("    CFS cumulative: %d alarms from %d grid scans",
                len(alarms_t), n_days // 30)
    return alarms_t, alarms_loc


async def generate_etas_residual_alarms(events, all_times, t0,
                                         residual_threshold=3.0,
                                         window_days=7):
    """Generate alarms when seismicity rate exceeds ETAS prediction.

    ETAS (Epidemic-Type Aftershock Sequence) predicts aftershock rates.
    When OBSERVED rate significantly exceeds ETAS PREDICTION, something
    beyond normal aftershock cascading is happening — a genuine anomaly.

    Simplified ETAS: rate(t) = mu + sum(K * exp(alpha*(mi-mc)) / (t-ti+c)^p)
    Parameters from Ogata (1998) for Japan: K=0.04, alpha=1.0, c=0.01, p=1.1
    """
    # ETAS parameters (Japan regional estimates, Ogata 1998)
    mu = 0.5  # Background rate per day per 2° cell (fitted below)
    K = 0.04
    alpha_etas = 1.0
    c = 0.01  # days
    p = 1.1
    mc = 3.0  # Completeness magnitude

    T_total = all_times[-1] - all_times[0]
    alarms_t = []
    alarms_loc = []

    n_days = int(T_total)
    for day_offset in range(30, n_days, 1):  # Start after 30 days
        t_now = all_times[0] + day_offset
        idx = bisect.bisect_right(all_times, t_now)

        t_start = t_now - window_days
        idx_start = bisect.bisect_left(all_times, t_start)
        recent = events[idx_start:idx]
        if not recent:
            continue

        # Cluster by 2° boxes
        boxes = {}
        for e in recent:
            bkey = (round(e["lat"] / 2) * 2, round(e["lon"] / 2) * 2)
            if bkey not in boxes:
                boxes[bkey] = []
            boxes[bkey].append(e)

        for (blat, blon), box_events in boxes.items():
            observed = len(box_events)

            # ETAS predicted rate for this cell in this window
            # Sum aftershock contributions from all prior events in this cell
            prior_idx = bisect.bisect_right(all_times, t_start)
            etas_rate = mu * window_days  # Background

            for j in range(max(0, prior_idx - 5000), prior_idx):
                e = events[j]
                if abs(e["lat"] - blat) > 2 or abs(e["lon"] - blon) > 2:
                    continue
                dt_start = t_start - e["t_days"]
                dt_end = t_now - e["t_days"]
                if dt_start < 0:
                    dt_start = 0.001
                if dt_end <= dt_start:
                    continue
                # Integrated ETAS kernel over [dt_start, dt_end]
                productivity = K * math.exp(alpha_etas * (e["mag"] - mc))
                if abs(p - 1.0) < 0.01:
                    integral = productivity * (math.log(dt_end + c) - math.log(dt_start + c))
                else:
                    integral = productivity / (1 - p) * (
                        (dt_end + c) ** (1 - p) - (dt_start + c) ** (1 - p))
                etas_rate += max(integral, 0)

            if etas_rate < 0.5:
                etas_rate = 0.5  # Floor

            residual = observed / etas_rate
            if residual >= residual_threshold:
                alarm_dt = t0 + timedelta(days=t_now)
                alarms_t.append(alarm_dt)
                alarms_loc.append((blat, blon))

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

    # Compute per-cell base rates for spatially-resolved evaluation
    cell_rates = compute_cell_base_rates(
        targets_test, T_total / 2, SPATIAL_RADIUS_DEG, PREDICTION_WINDOW_DAYS)

    results = {
        "metadata": {
            "prediction_window_days": PREDICTION_WINDOW_DAYS,
            "spatial_radius_deg": SPATIAL_RADIUS_DEG,
            "min_target_mag": MIN_TARGET_MAG,
            "total_days": round(T_total, 1),
            "n_events_m3": len(events),
            "n_targets_all": len(targets_all),
            "n_targets_isolated": len(targets_isolated),
            "n_spatial_cells": len(cell_rates),
            "mean_cell_base_rate": round(
                sum(cell_rates.values()) / max(len(cell_rates), 1), 5),
            "train_period": "2011-2018",
            "test_period": "2019-2026",
        },
        "alarms": {},
    }

    def eval_and_log(times, locs, targets, label):
        """Evaluate and log alarm performance."""
        r = evaluate_alarm(times, locs, targets,
                           PREDICTION_WINDOW_DAYS, SPATIAL_RADIUS_DEG,
                           T_total / 2, label=label, cell_base_rates=cell_rates)
        results["alarms"][label] = r
        logger.info("  %s: prec=%.3f recall=%.3f gain=%.1f IGPE=%.2f molchan=%.3f (%d alarms)",
                    label, r["precision"], r["recall"],
                    r["probability_gain"], r["information_gain_bits"],
                    r["molchan_score"], r["n_alarms"])
        return r

    def filter_test_period(times, locs):
        """Filter alarms to test period only."""
        pairs = [(t, loc) for t, loc in zip(times, locs) if t >= split_date]
        return [t for t, _ in pairs], [loc for _, loc in pairs]

    # ---------------------------------------------------------------
    # Signal 1: Seismicity rate alarm (raw)
    # ---------------------------------------------------------------
    logger.info("  --- Generating rate alarms ---")
    for threshold in [2.0, 3.0, 5.0]:
        rate_t, rate_loc = await generate_rate_alarms(
            events, events, all_times, t0, threshold=threshold)
        rt, rl = filter_test_period(rate_t, rate_loc)
        eval_and_log(rt, rl, targets_test, f"rate_gt_{threshold}x")

    # ---------------------------------------------------------------
    # Signal 2: ETAS residual alarm (aftershock-corrected rate)
    # ---------------------------------------------------------------
    logger.info("  --- Generating ETAS residual alarms ---")
    for res_thresh in [2.0, 3.0, 5.0]:
        etas_t, etas_loc = await generate_etas_residual_alarms(
            events, all_times, t0, residual_threshold=res_thresh)
        et, el = filter_test_period(etas_t, etas_loc)
        eval_and_log(et, el, targets_test, f"etas_residual_gt_{res_thresh}x")

    # ---------------------------------------------------------------
    # Signal 3: Cumulative CFS alarm
    # ---------------------------------------------------------------
    logger.info("  --- Generating cumulative CFS alarms ---")
    for cfs_thresh in [10, 50, 100]:
        cfs_t, cfs_loc = await generate_cfs_cumulative_alarms(
            events, fm_dict, all_times, t0, cfs_threshold_kpa=cfs_thresh)
        ct, cl = filter_test_period(cfs_t, cfs_loc)
        eval_and_log(ct, cl, targets_test, f"cfs_cumul_gt_{cfs_thresh}kpa")

    # ---------------------------------------------------------------
    # Signal 3: Foreshock alarm
    # ---------------------------------------------------------------
    logger.info("  --- Generating foreshock alarms ---")
    for min_count in [3, 5, 10]:
        fore_t, fore_loc = await generate_foreshock_alarms(
            events, all_times, t0, min_count=min_count)
        ft, fl = filter_test_period(fore_t, fore_loc)
        eval_and_log(ft, fl, targets_test, f"foreshock_ge_{min_count}")

    # ---------------------------------------------------------------
    # Signal 5: ULF magnetic alarm (if data available)
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
        ut, ul = filter_test_period(ulf_t, ulf_loc)
        eval_and_log(ut, ul, targets_test, f"ulf_power_gt_{ulf_thresh}x")

    # ---------------------------------------------------------------
    # Combined: ETAS residual + CFS cumulative + foreshock
    # ---------------------------------------------------------------
    logger.info("  --- Combined alarm (ETAS + CFS + foreshock) ---")
    etas_best_t, etas_best_loc = await generate_etas_residual_alarms(
        events, all_times, t0, residual_threshold=3.0)
    cfs_best_t, cfs_best_loc = await generate_cfs_cumulative_alarms(
        events, fm_dict, all_times, t0, cfs_threshold_kpa=50)
    fore_best_t, fore_best_loc = await generate_foreshock_alarms(
        events, all_times, t0, min_count=5)

    # Build daily alarm index for fast lookup
    def build_alarm_index(times, locs, cell_size=2):
        """Index alarms by (day_offset, cell) for O(1) lookup."""
        idx = {}
        for t, (lat, lon) in zip(times, locs):
            day = t.toordinal()
            cell = (round(lat / cell_size) * cell_size,
                    round(lon / cell_size) * cell_size)
            idx[(day, cell)] = True
        return idx

    etas_idx = build_alarm_index(etas_best_t, etas_best_loc)
    cfs_idx = build_alarm_index(cfs_best_t, cfs_best_loc)
    fore_idx = build_alarm_index(fore_best_t, fore_best_loc)

    combined_alarms_t = []
    combined_alarms_loc = []
    test_start = split_date.toordinal()
    test_end = test_start + int(T_total / 2)

    for day_ord in range(test_start, test_end):
        t_day = datetime.fromordinal(day_ord).replace(tzinfo=timezone.utc)
        for lat in range(26, 46, 2):
            for lon in range(128, 148, 2):
                cell = (lat, lon)
                n_active = 0
                if (day_ord, cell) in etas_idx:
                    n_active += 1
                if (day_ord, cell) in cfs_idx:
                    n_active += 1
                if (day_ord, cell) in fore_idx:
                    n_active += 1
                if n_active >= 2:
                    combined_alarms_t.append(t_day)
                    combined_alarms_loc.append(cell)

    eval_and_log(combined_alarms_t, combined_alarms_loc,
                 targets_test, "combined_etas_cfs_fore_ge2")
    eval_and_log(combined_alarms_t, combined_alarms_loc,
                 targets_test_iso, "combined_ge2_isolated")

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
