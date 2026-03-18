"""Cosmic ray anomaly analysis for earthquake precursors.

Analyzes daily neutron monitor count rates from NMDB stations for
anomalous deviations that may correlate with seismic activity.

Methods implemented:
1. **27-day solar rotation baseline**: Compute rolling 27-day mean
   (one solar rotation period) and measure deviations. Homola et al. (2023)
   found cosmic ray anomalies leading earthquakes by ~15 days.

2. **Multi-station differential**: Compare IRKT (closest to Japan) vs
   OULU (reference station) to isolate regional geomagnetic effects
   from global solar modulation.

3. **Forbush decrease detection**: Identify sudden decreases in cosmic
   ray intensity (Forbush decreases) from solar events, which may
   affect tectonic stress via geomagnetic field changes.

Output: JSON with per-date cosmic ray features for ML integration.

References:
    - Homola et al. (2023) J. Atmos. Sol.-Terr. Phys. 247:106068
"""

import asyncio
import json
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"


def rolling_mean(values: list[float | None], window: int) -> list[float | None]:
    """Compute rolling mean with None handling."""
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        valid = [v for v in values[start:i + 1] if v is not None]
        if len(valid) >= window // 2:
            result.append(sum(valid) / len(valid))
        else:
            result.append(None)
    return result


def rolling_std(values: list[float | None], window: int) -> list[float | None]:
    """Compute rolling standard deviation."""
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        valid = [v for v in values[start:i + 1] if v is not None]
        if len(valid) >= window // 2:
            mean = sum(valid) / len(valid)
            var = sum((v - mean) ** 2 for v in valid) / len(valid)
            result.append(math.sqrt(var))
        else:
            result.append(None)
    return result


def linear_slope(values: list[float | None], window: int) -> list[float | None]:
    """Compute rolling linear regression slope."""
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        segment = values[start:i + 1]
        valid_pairs = [(j, v) for j, v in enumerate(segment) if v is not None]
        if len(valid_pairs) < window // 2:
            result.append(None)
            continue
        n = len(valid_pairs)
        sx = sum(x for x, _ in valid_pairs)
        sy = sum(y for _, y in valid_pairs)
        sxx = sum(x * x for x, _ in valid_pairs)
        sxy = sum(x * y for x, y in valid_pairs)
        denom = n * sxx - sx * sx
        if abs(denom) < 1e-15:
            result.append(0.0)
        else:
            result.append((n * sxy - sx * sy) / denom)
    return result


async def run_cosmic_ray_analysis():
    """Main analysis: compute cosmic ray features per date."""
    logger.info("=== Cosmic Ray Anomaly Analysis ===")

    async with aiosqlite.connect(DB_PATH) as db:
        # Check data availability
        stats = await db.execute_fetchall(
            "SELECT station, COUNT(*), MIN(observed_at), MAX(observed_at) "
            "FROM cosmic_ray GROUP BY station"
        )
        if not stats:
            logger.warning("No cosmic ray data. Run fetch_nmdb_cosmicray.py first.")
            return {"error": "no_cosmic_ray_data"}

        for s in stats:
            logger.info("  %s: %d records (%s to %s)", s[0], s[1], s[2], s[3])

        # Load all data per station
        station_data = {}
        for station in ["IRKT", "OULU", "PSNM"]:
            rows = await db.execute_fetchall(
                "SELECT observed_at, counts_per_sec FROM cosmic_ray "
                "WHERE station = ? ORDER BY observed_at",
                (station,),
            )
            if rows:
                station_data[station] = {
                    r[0]: r[1] for r in rows
                }

    if not station_data:
        logger.warning("No station data loaded.")
        return {"error": "no_data"}

    # Use IRKT as primary station (closest to Japan)
    primary = "IRKT"
    if primary not in station_data:
        primary = list(station_data.keys())[0]
        logger.info("IRKT not available, using %s as primary", primary)

    dates = sorted(station_data[primary].keys())
    values = [station_data[primary][d] for d in dates]

    logger.info("Primary station %s: %d daily values (%s to %s)",
                primary, len(dates), dates[0], dates[-1])

    # Compute features
    # 1. 27-day solar rotation baseline anomaly
    mean_27d = rolling_mean(values, 27)
    std_27d = rolling_std(values, 27)

    # 2. 15-day trend (Homola lag)
    trend_15d = linear_slope(values, 15)

    # 3. 7-day rate of change
    trend_7d = linear_slope(values, 7)

    # 4. Multi-station differential (IRKT - OULU normalized)
    diff_irkt_oulu = []
    if "OULU" in station_data:
        oulu_data = station_data["OULU"]
        for d in dates:
            irkt_v = station_data[primary].get(d)
            oulu_v = oulu_data.get(d)
            if irkt_v is not None and oulu_v is not None and oulu_v > 0:
                diff_irkt_oulu.append(irkt_v / oulu_v)
            else:
                diff_irkt_oulu.append(None)
    else:
        diff_irkt_oulu = [None] * len(dates)

    # 5. Forbush decrease detection (>3% drop in 1-2 days)
    forbush_flag = []
    for i in range(len(values)):
        if i < 2 or values[i] is None or values[i - 2] is None:
            forbush_flag.append(0)
            continue
        change_pct = (values[i] - values[i - 2]) / values[i - 2] * 100
        forbush_flag.append(1 if change_pct < -3.0 else 0)

    # Build per-date feature dict
    features = {}
    for i, date in enumerate(dates):
        anomaly = None
        if mean_27d[i] is not None and std_27d[i] is not None and std_27d[i] > 0:
            anomaly = (values[i] - mean_27d[i]) / std_27d[i] if values[i] is not None else None

        features[date] = {
            "cosmic_ray_rate": values[i],
            "cosmic_ray_anomaly": round(anomaly, 4) if anomaly is not None else None,
            "cosmic_ray_trend_15d": round(trend_15d[i], 6) if trend_15d[i] is not None else None,
            "cosmic_ray_trend_7d": round(trend_7d[i], 6) if trend_7d[i] is not None else None,
            "cosmic_ray_diff_ratio": round(diff_irkt_oulu[i], 6) if diff_irkt_oulu[i] is not None else None,
            "cosmic_ray_forbush": forbush_flag[i],
        }

    # Correlate with earthquakes
    async with aiosqlite.connect(DB_PATH) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT DATE(occurred_at), magnitude FROM earthquakes "
            "WHERE magnitude >= 5.0 ORDER BY occurred_at"
        )

    # Compute AUC proxy: mean cosmic ray anomaly before M5+ events
    pre_eq_anomalies = []
    non_eq_anomalies = []
    eq_dates = set(r[0] for r in eq_rows)

    for date, feat in features.items():
        anomaly = feat["cosmic_ray_anomaly"]
        if anomaly is None:
            continue
        # Check if M5+ in next 1-30 days
        is_pre_eq = False
        for lag in range(1, 31):
            d = datetime.strptime(date, "%Y-%m-%d")
            future = (d + timedelta(days=lag)).strftime("%Y-%m-%d")
            if future in eq_dates:
                is_pre_eq = True
                break
        if is_pre_eq:
            pre_eq_anomalies.append(anomaly)
        else:
            non_eq_anomalies.append(anomaly)

    if pre_eq_anomalies and non_eq_anomalies:
        mean_pre = sum(pre_eq_anomalies) / len(pre_eq_anomalies)
        mean_non = sum(non_eq_anomalies) / len(non_eq_anomalies)
        logger.info("  Pre-M5+ cosmic ray anomaly (1-30d): mean=%.4f (n=%d)",
                     mean_pre, len(pre_eq_anomalies))
        logger.info("  Non-M5+ cosmic ray anomaly: mean=%.4f (n=%d)",
                     mean_non, len(non_eq_anomalies))
        logger.info("  Difference: %.4f", mean_pre - mean_non)

    # Save results
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    result = {
        "analysis": "cosmic_ray_anomaly",
        "timestamp": ts,
        "primary_station": primary,
        "n_dates": len(features),
        "date_range": {"start": dates[0], "end": dates[-1]},
        "n_forbush_events": sum(1 for f in features.values() if f["cosmic_ray_forbush"]),
        "features_sample": {d: features[d] for d in dates[-5:]},
    }

    # Save full features for ML pipeline
    output_path = RESULTS_DIR / f"cosmic_ray_features_{ts}.json"
    with open(output_path, "w") as f:
        json.dump({"features": features, "metadata": result}, f, indent=2)
    logger.info("Results saved: %s", output_path)

    # Also save latest features for stacking pipeline
    latest_path = RESULTS_DIR / "cosmic_ray_features_latest.json"
    with open(latest_path, "w") as f:
        json.dump(features, f)
    logger.info("Latest features saved: %s", latest_path)

    return result


async def main():
    result = await run_cosmic_ray_analysis()
    if isinstance(result, dict) and "error" not in result:
        logger.info("Cosmic ray analysis complete: %d dates processed", result["n_dates"])


if __name__ == "__main__":
    asyncio.run(main())
