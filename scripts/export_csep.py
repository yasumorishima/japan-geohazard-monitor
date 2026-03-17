"""Export ML predictions as CSEP-compatible forecast.

Reads the latest ml_prediction results and converts to CSEP XML + JSON format.
Designed to be called from GitHub Actions after ml_prediction.py.

Usage:
    python3 scripts/export_csep.py
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from csep_format import (
    generate_csep_forecast,
    forecast_to_xml,
    forecast_to_json,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"


def load_latest_predictions():
    """Load predictions from latest level-0 export files.

    Returns dict: {(cell_lat, cell_lon): probability} aggregated over
    the most recent time step for each cell.
    """
    predictions = {}

    # Try level-0 predictions (Phase 8 format)
    level0_files = sorted(RESULTS_DIR.glob("level0_predictions_*.json"), reverse=True)
    if level0_files:
        # Use M5+ as primary forecast
        m5_file = None
        for f in level0_files:
            if "M5plus" in f.name or "M5" in f.name:
                m5_file = f
                break
        if m5_file is None:
            m5_file = level0_files[0]

        logger.info("Loading level-0 predictions from: %s", m5_file.name)
        with open(m5_file) as f:
            data = json.load(f)

        # Get latest time step per cell
        cell_latest = {}  # (lat, lon) -> (t_days, prob)
        for rec in data.get("predictions", []):
            ck = (rec["cell_lat"], rec["cell_lon"])
            t = rec["t_days"]
            if ck not in cell_latest or t > cell_latest[ck][0]:
                cell_latest[ck] = (t, rec["prob"])

        for ck, (_, prob) in cell_latest.items():
            predictions[ck] = prob

        logger.info("  Loaded %d cell predictions", len(predictions))
        return predictions, data.get("target", "M5+")

    # Fallback: load from ml_prediction JSON
    ml_files = sorted(RESULTS_DIR.glob("ml_prediction_*.json"), reverse=True)
    if not ml_files:
        logger.error("No ML prediction files found")
        return {}, "M5+"

    logger.info("Loading from ML prediction file: %s", ml_files[0].name)
    with open(ml_files[0]) as f:
        ml_data = json.load(f)

    # Extract threshold evaluation to estimate per-cell probabilities
    # This is a fallback — level-0 files are preferred
    targets = ml_data.get("targets", {})
    if targets:
        m5_data = targets.get("M5+", {})
    else:
        m5_data = ml_data

    final = m5_data.get("final_model", {})
    perf = final.get("performance", {})
    base_rate = perf.get("base_rate_test", 0.01)

    # Without per-cell predictions, use base rate as uniform forecast
    from csep_format import GRID_LAT_MIN, GRID_LAT_MAX, GRID_LON_MIN, GRID_LON_MAX, CELL_SIZE_DEG
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        lon = GRID_LON_MIN
        while lon <= GRID_LON_MAX:
            predictions[(lat, lon)] = base_rate
            lon += CELL_SIZE_DEG
        lat += CELL_SIZE_DEG

    logger.info("  Uniform fallback: %d cells at base_rate=%.4f", len(predictions), base_rate)
    return predictions, "M5+"


def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    predictions, target = load_latest_predictions()
    if not predictions:
        logger.error("No predictions available for CSEP export")
        return

    # Determine window_days from target config
    window_days = 7
    try:
        from target_config import TARGET_CONFIGS
        cfg = TARGET_CONFIGS.get(target, {})
        window_days = cfg.get("window_days", 7)
    except ImportError:
        pass

    # Generate forecast
    forecast = generate_csep_forecast(
        predictions,
        window_days=window_days,
        model_name=f"JapanGeohazardML_{target}",
    )

    # Export
    xml_path = RESULTS_DIR / f"csep_forecast_{timestamp}.xml"
    json_path = RESULTS_DIR / f"csep_forecast_{timestamp}.json"

    forecast_to_xml(forecast, xml_path)
    forecast_to_json(forecast, json_path)

    logger.info("CSEP export complete: total_expected_rate=%.4f", forecast["total_expected_rate"])


if __name__ == "__main__":
    main()
