"""Anomaly detection and time-lag cross-correlation analysis.

Detects statistical anomalies (>2σ from rolling mean) in TEC, Kp, pressure,
and GOES magnetic field. Computes lagged cross-correlation between each metric
and earthquake frequency to find precursor patterns.
"""

import logging
from datetime import datetime, timezone

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
