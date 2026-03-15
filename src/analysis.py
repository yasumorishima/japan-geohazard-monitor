"""Anomaly detection and time-lag cross-correlation analysis.

Detects statistical anomalies (>2σ from rolling mean) in TEC, Kp, pressure,
and GOES magnetic field. Computes lagged cross-correlation between each metric
and earthquake frequency to find precursor patterns.
"""

import logging
import math
from datetime import datetime, timedelta, timezone

import aiosqlite

from config import DB_PATH

logger = logging.getLogger(__name__)

# Rolling window for anomaly detection (hours)
_ROLLING_WINDOW_HOURS = 72  # 3-day baseline
_ANOMALY_SIGMA = 2.0        # ±2σ threshold


async def detect_anomalies(days: int = 7) -> dict:
    """Detect anomalies in TEC, Kp, pressure, and GOES magnetic field.

    An anomaly is a value outside ±2σ from the rolling 72-hour mean.
    Returns per-metric list of {time, value, mean, std, sigma_distance}.
    """
    cutoff = f"-{days}"
    results = {}

    async with aiosqlite.connect(DB_PATH) as db:
        # --- TEC anomalies (mean TEC per epoch) ---
        tec_rows = await db.execute_fetchall(
            """SELECT epoch, AVG(tec_tecu) as avg_tec
               FROM tec
               WHERE epoch > datetime('now', ? || ' days')
               GROUP BY epoch ORDER BY epoch""",
            (cutoff,),
        )
        results["tec"] = _find_anomalies(
            [(r[0], r[1]) for r in tec_rows], "TECU"
        )

        # --- Kp anomalies ---
        kp_rows = await db.execute_fetchall(
            """SELECT time_tag, kp FROM geomag_kp
               WHERE time_tag > datetime('now', ? || ' days')
               ORDER BY time_tag""",
            (cutoff,),
        )
        results["kp"] = _find_anomalies(
            [(r[0], r[1]) for r in kp_rows if r[1] is not None], "Kp"
        )

        # --- GOES magnetic total (hourly mean) ---
        goes_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', time_tag) as hour,
                      AVG(total) as avg_total
               FROM geomag_goes
               WHERE time_tag > datetime('now', ? || ' days')
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )
        results["goes"] = _find_anomalies(
            [(r[0], r[1]) for r in goes_rows if r[1] is not None], "nT"
        )

        # --- Pressure anomalies (hourly mean) ---
        pressure_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', observed_at) as hour,
                      AVG(pressure_hpa) as avg_p
               FROM amedas
               WHERE observed_at > datetime('now', ? || ' days')
                 AND pressure_hpa IS NOT NULL
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )
        results["pressure"] = _find_anomalies(
            [(r[0], r[1]) for r in pressure_rows if r[1] is not None], "hPa"
        )

    total = sum(len(v["anomalies"]) for v in results.values())
    logger.info("Anomaly scan: %d anomalies found across %d days", total, days)
    return results


def _find_anomalies(
    time_values: list[tuple[str, float]], unit: str
) -> dict:
    """Find values outside ±2σ from rolling mean.

    Uses expanding window (min 6 points) to compute running mean/std.
    """
    anomalies = []
    if len(time_values) < 6:
        return {"anomalies": anomalies, "unit": unit, "total_points": len(time_values)}

    values = [v for _, v in time_values]

    for i in range(6, len(time_values)):
        # Use the preceding window for baseline
        window_start = max(0, i - _ROLLING_WINDOW_HOURS)
        window = values[window_start:i]

        mean = sum(window) / len(window)
        variance = sum((x - mean) ** 2 for x in window) / len(window)
        std = variance ** 0.5

        if std < 1e-10:
            continue

        current = values[i]
        sigma_dist = (current - mean) / std

        if abs(sigma_dist) >= _ANOMALY_SIGMA:
            anomalies.append({
                "time": time_values[i][0],
                "value": round(current, 2),
                "mean": round(mean, 2),
                "std": round(std, 3),
                "sigma": round(sigma_dist, 2),
                "direction": "high" if sigma_dist > 0 else "low",
            })

    return {"anomalies": anomalies, "unit": unit, "total_points": len(time_values)}


async def compute_lag_correlation(
    days: int = 30, max_lag_hours: int = 48, min_mag: float = 0.0,
) -> dict:
    """Compute lagged cross-correlation between each metric and earthquake count.

    For each metric, computes Pearson correlation at lags from -max_lag to 0,
    where negative lag means the metric leads the earthquake.
    e.g., lag=-6 means "metric value 6 hours BEFORE earthquake spike".

    min_mag: only count earthquakes with magnitude >= this value.

    Returns per-metric list of {lag_hours, correlation}.
    """
    cutoff = f"-{days}"

    async with aiosqlite.connect(DB_PATH) as db:
        # Hourly earthquake counts (filtered by magnitude)
        eq_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', occurred_at) as hour,
                      COUNT(*) as count
               FROM earthquakes
               WHERE occurred_at > datetime('now', ? || ' days')
                 AND (magnitude >= ? OR magnitude IS NULL)
               GROUP BY hour ORDER BY hour""",
            (cutoff, min_mag),
        )

        kp_rows = await db.execute_fetchall(
            """SELECT time_tag, kp FROM geomag_kp
               WHERE time_tag > datetime('now', ? || ' days')
               ORDER BY time_tag""",
            (cutoff,),
        )

        goes_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', time_tag) as hour,
                      AVG(total) as avg_total
               FROM geomag_goes
               WHERE time_tag > datetime('now', ? || ' days')
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )

        tec_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', epoch) as hour,
                      AVG(tec_tecu) as avg_tec
               FROM tec
               WHERE epoch > datetime('now', ? || ' days')
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )

        pressure_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', observed_at) as hour,
                      AVG(pressure_hpa) as avg_p
               FROM amedas
               WHERE observed_at > datetime('now', ? || ' days')
                 AND pressure_hpa IS NOT NULL
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )

    # Build hourly time-indexed dicts
    eq_dict = {r[0]: r[1] for r in eq_rows}
    kp_dict = {r[0]: r[1] for r in kp_rows if r[1] is not None}
    goes_dict = {r[0]: r[1] for r in goes_rows if r[1] is not None}
    tec_dict = {r[0]: r[1] for r in tec_rows if r[1] is not None}
    pressure_dict = {r[0]: r[1] for r in pressure_rows if r[1] is not None}

    # All unique hours
    all_hours = sorted(set(eq_dict) | set(kp_dict) | set(goes_dict)
                        | set(tec_dict) | set(pressure_dict))

    if len(all_hours) < max_lag_hours + 10:
        return {"kp": [], "goes": [], "tec": [], "pressure": [],
                "max_lag_hours": max_lag_hours, "message": "Insufficient data"}

    # Build aligned arrays
    eq_arr = [eq_dict.get(h, 0) for h in all_hours]

    results = {}
    for name, metric_dict in [
        ("kp", kp_dict), ("goes", goes_dict),
        ("tec", tec_dict), ("pressure", pressure_dict),
    ]:
        metric_arr = [metric_dict.get(h) for h in all_hours]
        correlations = []

        for lag in range(-max_lag_hours, 1):
            corr = _pearson_with_lag(eq_arr, metric_arr, lag)
            if corr is not None:
                correlations.append({"lag": lag, "r": round(corr, 4)})

        # Find peak correlation
        peak = max(correlations, key=lambda x: abs(x["r"])) if correlations else None
        results[name] = {
            "correlations": correlations,
            "peak": peak,
        }

    results["max_lag_hours"] = max_lag_hours
    return results


def _pearson_with_lag(
    target: list[float], metric: list[float | None], lag: int
) -> float | None:
    """Compute Pearson correlation between target and metric shifted by lag.

    lag < 0 means metric leads target (precursor).
    """
    n = len(target)
    pairs = []

    for i in range(n):
        j = i + lag  # metric index
        if 0 <= j < n and metric[j] is not None:
            pairs.append((target[i], metric[j]))

    if len(pairs) < 20:
        return None

    x_vals = [p[0] for p in pairs]
    y_vals = [p[1] for p in pairs]

    n_pairs = len(pairs)
    mean_x = sum(x_vals) / n_pairs
    mean_y = sum(y_vals) / n_pairs

    cov = sum((x - mean_x) * (y - mean_y) for x, y in pairs) / n_pairs
    std_x = (sum((x - mean_x) ** 2 for x in x_vals) / n_pairs) ** 0.5
    std_y = (sum((y - mean_y) ** 2 for y in y_vals) / n_pairs) ** 0.5

    if std_x < 1e-10 or std_y < 1e-10:
        return None

    return cov / (std_x * std_y)


async def analyze_epicenter_tec(
    min_mag: float = 6.0, radius_deg: float = 5.0,
    hours_before: int = 168, hours_after: int = 24,
) -> dict:
    """Analyze TEC behavior near earthquake epicenters.

    For each M6+ earthquake, extracts TEC values within radius_deg of the
    epicenter for hours_before to hours_after, and computes:
    - Mean TEC in the 7 days before vs. 24h before (precursor drop/spike?)
    - TEC anomaly score (deviation from 7-day baseline)
    - Per-event timeline

    This addresses the key limitation of the global correlation:
    spatial proximity to the epicenter matters.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Get M6+ earthquakes
        eq_rows = await db.execute_fetchall(
            """SELECT occurred_at, latitude, longitude, magnitude, location_en
               FROM earthquakes
               WHERE magnitude >= ?
               ORDER BY occurred_at""",
            (min_mag,),
        )

        events = []
        for eq in eq_rows:
            eq_lat = eq["latitude"]
            eq_lon = eq["longitude"]
            eq_time = eq["occurred_at"]
            eq_mag = eq["magnitude"]

            # Get TEC near epicenter in the time window
            tec_rows = await db.execute_fetchall(
                """SELECT epoch, AVG(tec_tecu) as avg_tec, COUNT(*) as n_points
                   FROM tec
                   WHERE ABS(latitude - ?) <= ?
                     AND ABS(longitude - ?) <= ?
                     AND epoch BETWEEN datetime(?, ? || ' hours')
                                    AND datetime(?, ? || ' hours')
                   GROUP BY epoch
                   ORDER BY epoch""",
                (eq_lat, radius_deg, eq_lon, radius_deg,
                 eq_time, f"-{hours_before}",
                 eq_time, f"+{hours_after}"),
            )

            if len(tec_rows) < 6:
                continue

            # Split into baseline (7d-24h before) and precursor (24h before)
            tec_data = [(r["epoch"], r["avg_tec"]) for r in tec_rows]
            all_values = [v for _, v in tec_data]
            baseline_mean = sum(all_values[:-8]) / len(all_values[:-8]) if len(all_values) > 8 else sum(all_values) / len(all_values)
            baseline_std = (sum((v - baseline_mean) ** 2 for v in all_values[:-8]) / max(1, len(all_values) - 8)) ** 0.5 if len(all_values) > 8 else 1.0

            # Last 24h (up to 8 epochs at hourly resolution)
            precursor_values = all_values[-8:] if len(all_values) >= 8 else all_values[-3:]
            precursor_mean = sum(precursor_values) / len(precursor_values)

            # Anomaly score: how many σ is the precursor period from baseline
            anomaly_sigma = (precursor_mean - baseline_mean) / baseline_std if baseline_std > 0.1 else 0.0

            events.append({
                "time": eq_time,
                "mag": eq_mag,
                "location": eq["location_en"],
                "lat": eq_lat,
                "lon": eq_lon,
                "baseline_tec": round(baseline_mean, 2),
                "precursor_tec": round(precursor_mean, 2),
                "anomaly_sigma": round(anomaly_sigma, 2),
                "direction": "drop" if anomaly_sigma < -1.0 else "spike" if anomaly_sigma > 1.0 else "normal",
                "n_epochs": len(tec_data),
                "timeline": [
                    {"time": t, "tec": round(v, 2)} for t, v in tec_data
                ],
            })

    # Summary statistics
    if events:
        anomalous = [e for e in events if abs(e["anomaly_sigma"]) >= 1.0]
        drops = [e for e in events if e["direction"] == "drop"]
        spikes = [e for e in events if e["direction"] == "spike"]
        mean_sigma = sum(e["anomaly_sigma"] for e in events) / len(events)
    else:
        anomalous, drops, spikes = [], [], []
        mean_sigma = 0.0

    return {
        "events": events,
        "total_earthquakes": len(eq_rows),
        "with_tec_data": len(events),
        "anomalous_count": len(anomalous),
        "drops": len(drops),
        "spikes": len(spikes),
        "mean_anomaly_sigma": round(mean_sigma, 3),
        "params": {
            "min_mag": min_mag,
            "radius_deg": radius_deg,
            "hours_before": hours_before,
        },
    }


async def compute_bvalue(days: int = 365, window_days: int = 30) -> dict:
    """Compute Gutenberg-Richter b-value over sliding time windows.

    b-value decrease is theorized to precede large earthquakes.
    Normal b-value ≈ 1.0. Values < 0.7 may indicate stress buildup.

    Returns time series of {window_end, b_value, n_events, a_value}.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            """SELECT occurred_at, magnitude
               FROM earthquakes
               WHERE occurred_at > datetime('now', ? || ' days')
                 AND magnitude IS NOT NULL AND magnitude >= 2.0
               ORDER BY occurred_at""",
            (f"-{days}",),
        )

    if len(eq_rows) < 20:
        return {"timeseries": [], "message": "Insufficient data"}

    events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events.append((t, r[1]))
        except (ValueError, TypeError):
            continue

    if not events:
        return {"timeseries": [], "message": "No parseable events"}

    # Sliding window b-value
    timeseries = []
    window_td = timedelta(days=window_days)

    # Step through in 1-day increments
    current = events[0][0]
    end = events[-1][0]

    while current + window_td <= end:
        window_end = current + window_td
        window_mags = [m for t, m in events if current <= t < window_end]

        if len(window_mags) >= 10:
            # Maximum likelihood b-value: b = log10(e) / (M_mean - M_min)
            m_min = min(window_mags)
            m_mean = sum(window_mags) / len(window_mags)
            denom = m_mean - m_min
            if denom > 0.01:
                b_val = math.log10(math.e) / denom
                # a-value: log10(N) = a - b*M_min
                a_val = math.log10(len(window_mags)) + b_val * m_min
                timeseries.append({
                    "time": window_end.isoformat(),
                    "b_value": round(b_val, 3),
                    "a_value": round(a_val, 2),
                    "n_events": len(window_mags),
                    "m_min": round(m_min, 1),
                    "m_mean": round(m_mean, 2),
                })

        current += timedelta(days=1)

    # Find periods where b < 0.7 (potential stress buildup)
    low_b = [t for t in timeseries if t["b_value"] < 0.7]

    return {
        "timeseries": timeseries,
        "total_points": len(timeseries),
        "low_b_count": len(low_b),
        "mean_b": round(sum(t["b_value"] for t in timeseries) / len(timeseries), 3) if timeseries else None,
        "window_days": window_days,
    }
