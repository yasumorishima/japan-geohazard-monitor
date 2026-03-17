"""CSEP-compatible forecast format generation.

Converts ML prediction probabilities to CSEP XML forecast format
(rate per cell / magnitude bin / time bin).

CSEP (Collaboratory for the Study of Earthquake Predictability) defines
a standard format for earthquake forecasts to enable fair model comparison.

Grid: 2x2 degree (consistent with feature extraction spatial scale)
Magnitude bins: [5.0,5.5), [5.5,6.0), [6.0,6.5), [6.5+]
Depth: 0-100 km (single bin)

References:
    - Schorlemmer et al. (2007) CSEP forecast format
    - Zechar et al. (2010) Testing forecasts via CSEP
"""

import json
import logging
import math
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Grid configuration (matches features.py)
GRID_LAT_MIN, GRID_LAT_MAX = 26.0, 46.0
GRID_LON_MIN, GRID_LON_MAX = 128.0, 148.0
CELL_SIZE_DEG = 2.0

# Magnitude bins
MAG_BINS = [
    (5.0, 5.5),
    (5.5, 6.0),
    (6.0, 6.5),
    (6.5, 9.0),  # 6.5+ open-ended
]

# Depth range
DEPTH_MIN, DEPTH_MAX = 0.0, 100.0

# Gutenberg-Richter b-value for magnitude distribution
DEFAULT_B_VALUE = 1.0


def csep_rate_from_probability(prob, window_days, b_value=DEFAULT_B_VALUE):
    """Convert cell-level M5+ probability to expected rates per magnitude bin.

    Uses Gutenberg-Richter distribution to split total expected rate
    across magnitude bins.

    Args:
        prob: P(M5+ event in cell within window_days)
        window_days: prediction window in days
        b_value: regional b-value

    Returns:
        dict: {(mag_min, mag_max): expected_rate}
    """
    # P(event) ≈ 1 - exp(-rate * window), so rate ≈ -ln(1-p) / window
    if prob <= 0:
        return {mag_bin: 0.0 for mag_bin in MAG_BINS}
    if prob >= 1.0:
        prob = 0.999

    total_rate = -math.log(1 - prob) / window_days

    # GR distribution: P(M >= m) = 10^(-b*(m - Mc))
    # Rate in [m1, m2) = total_rate * (10^(-b*(m1-5)) - 10^(-b*(m2-5)))
    rates = {}
    for mag_min, mag_max in MAG_BINS:
        p_above_min = 10 ** (-b_value * (mag_min - 5.0))
        if mag_max < 9.0:
            p_above_max = 10 ** (-b_value * (mag_max - 5.0))
        else:
            p_above_max = 0.0  # open-ended bin
        frac = p_above_min - p_above_max
        rates[(mag_min, mag_max)] = total_rate * max(frac, 0)

    return rates


def generate_csep_forecast(predictions, window_days=7, b_value=DEFAULT_B_VALUE,
                            forecast_start=None, model_name="JapanGeohazardML"):
    """Generate CSEP-format forecast from ML predictions.

    Args:
        predictions: dict {(cell_lat, cell_lon): probability}
        window_days: forecast window in days
        b_value: b-value for GR magnitude distribution
        forecast_start: datetime of forecast start (default: now)
        model_name: model identifier

    Returns:
        dict: CSEP forecast data structure
    """
    if forecast_start is None:
        forecast_start = datetime.now(timezone.utc)

    cells = []
    for lat in _grid_lats():
        for lon in _grid_lons():
            prob = predictions.get((lat, lon), 0.0)
            rates = csep_rate_from_probability(prob, window_days, b_value)

            for (mag_min, mag_max), rate in rates.items():
                cells.append({
                    "lat_min": lat - CELL_SIZE_DEG / 2,
                    "lat_max": lat + CELL_SIZE_DEG / 2,
                    "lon_min": lon - CELL_SIZE_DEG / 2,
                    "lon_max": lon + CELL_SIZE_DEG / 2,
                    "depth_min": DEPTH_MIN,
                    "depth_max": DEPTH_MAX,
                    "mag_min": mag_min,
                    "mag_max": mag_max,
                    "rate": rate,
                })

    forecast = {
        "model_name": model_name,
        "forecast_start": forecast_start.isoformat(),
        "forecast_duration_days": window_days,
        "grid": {
            "lat_range": [GRID_LAT_MIN, GRID_LAT_MAX],
            "lon_range": [GRID_LON_MIN, GRID_LON_MAX],
            "cell_size_deg": CELL_SIZE_DEG,
            "depth_range": [DEPTH_MIN, DEPTH_MAX],
            "mag_bins": [[m1, m2] for m1, m2 in MAG_BINS],
        },
        "n_cells": len(cells),
        "total_expected_rate": sum(c["rate"] for c in cells),
        "cells": cells,
    }

    return forecast


def forecast_to_xml(forecast, output_path):
    """Write CSEP forecast to XML file.

    Args:
        forecast: dict from generate_csep_forecast()
        output_path: Path to write XML
    """
    root = ET.Element("CSEPForecast")
    root.set("xmlns", "http://www.csep.org/forecast/1.0")

    meta = ET.SubElement(root, "ModelMetadata")
    ET.SubElement(meta, "ModelName").text = forecast["model_name"]
    ET.SubElement(meta, "ForecastStart").text = forecast["forecast_start"]
    ET.SubElement(meta, "ForecastDuration").text = str(forecast["forecast_duration_days"])
    ET.SubElement(meta, "ForecastDurationUnit").text = "days"

    grid_elem = ET.SubElement(root, "Grid")
    ET.SubElement(grid_elem, "LatRange").text = (
        f"{forecast['grid']['lat_range'][0]} {forecast['grid']['lat_range'][1]}")
    ET.SubElement(grid_elem, "LonRange").text = (
        f"{forecast['grid']['lon_range'][0]} {forecast['grid']['lon_range'][1]}")
    ET.SubElement(grid_elem, "CellSize").text = str(forecast["grid"]["cell_size_deg"])
    ET.SubElement(grid_elem, "DepthRange").text = (
        f"{forecast['grid']['depth_range'][0]} {forecast['grid']['depth_range'][1]}")

    cells_elem = ET.SubElement(root, "Cells")
    for cell in forecast["cells"]:
        c = ET.SubElement(cells_elem, "Cell")
        c.set("lat_min", f"{cell['lat_min']:.1f}")
        c.set("lat_max", f"{cell['lat_max']:.1f}")
        c.set("lon_min", f"{cell['lon_min']:.1f}")
        c.set("lon_max", f"{cell['lon_max']:.1f}")
        c.set("depth_min", f"{cell['depth_min']:.0f}")
        c.set("depth_max", f"{cell['depth_max']:.0f}")
        c.set("mag_min", f"{cell['mag_min']:.1f}")
        c.set("mag_max", f"{cell['mag_max']:.1f}")
        c.set("rate", f"{cell['rate']:.8e}")

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(str(output_path), encoding="unicode", xml_declaration=True)
    logger.info("CSEP XML written: %s (%d cells)", output_path, len(forecast["cells"]))


def forecast_to_json(forecast, output_path):
    """Write CSEP forecast to JSON file."""
    with open(output_path, "w") as f:
        json.dump(forecast, f, indent=2, ensure_ascii=False)
    logger.info("CSEP JSON written: %s", output_path)


def _grid_lats():
    """Generate grid latitude centers."""
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        yield lat
        lat += CELL_SIZE_DEG


def _grid_lons():
    """Generate grid longitude centers."""
    lon = GRID_LON_MIN
    while lon <= GRID_LON_MAX:
        yield lon
        lon += CELL_SIZE_DEG
