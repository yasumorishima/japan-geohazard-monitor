"""Export feature matrix as 4D tensor for ConvLSTM neural network.

Converts the flat (cell, time, features) dataset from ml_prediction.py
into a spatiotemporal tensor suitable for ConvLSTM training on Colab GPU.

Output: results/feature_matrix.npz (~90MB)
    X: (n_timesteps, H=10, W=10, C=47) — spatial grid × features
    y: (n_timesteps, H=10, W=10) — binary labels per cell
    times: (n_timesteps,) — time in days from t0
    cell_coords: (H, W, 2) — lat/lon of each grid cell
    feature_names: list of 47 feature names

Data flow:
    GitHub Actions → feature_matrix.npz (artifact)
    → RPi5 cron (gh run download) → Google Drive
    → Colab experiment_runner → ConvLSTM training (T4 GPU)

Usage:
    python3 scripts/export_feature_matrix.py
"""

import asyncio
import json
import logging
import struct
import sys
from datetime import datetime
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH
from features import (
    FEATURE_NAMES,
    N_FEATURES,
    FeatureExtractor,
    cell_key,
    generate_label,
    CELL_SIZE_DEG,
    GRID_LAT_MIN,
    GRID_LAT_MAX,
    GRID_LON_MIN,
    GRID_LON_MAX,
)
from physics import fit_etas_mle, classify_tectonic_zone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

# Grid dimensions for spatial tensor
# 26-46°N in 2° steps = 11 rows, 128-148°E in 2° steps = 11 cols
GRID_H = int((GRID_LAT_MAX - GRID_LAT_MIN) / CELL_SIZE_DEG) + 1
GRID_W = int((GRID_LON_MAX - GRID_LON_MIN) / CELL_SIZE_DEG) + 1

# Prediction parameters
PREDICTION_WINDOW_DAYS = 7
MIN_TARGET_MAG = 5.0
STEP_DAYS = 3


def build_grid_mapping():
    """Build mapping from (lat, lon) to (row, col) indices.

    Returns:
        lat_to_row: dict {lat: row_index}
        lon_to_col: dict {lon: col_index}
        cell_coords: list of (row, col, lat, lon)
    """
    lat_to_row = {}
    lon_to_col = {}
    cell_coords = []

    for i, lat in enumerate(range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG))):
        lat_to_row[float(lat)] = i

    for j, lon in enumerate(range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG))):
        lon_to_col[float(lon)] = j

    for lat in sorted(lat_to_row.keys()):
        row = []
        for lon in sorted(lon_to_col.keys()):
            row.append([lat, lon])
        cell_coords.append(row)

    return lat_to_row, lon_to_col, cell_coords


async def load_events(db_path):
    """Load events from database (same as ml_prediction.py)."""
    async with aiosqlite.connect(db_path) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
            "ORDER BY occurred_at"
        )
        fm_rows = await db.execute_fetchall(
            "SELECT latitude, longitude, strike1, dip1, rake1 FROM focal_mechanisms"
        )

    events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events.append({
                "time": t, "mag": r[1], "lat": r[2], "lon": r[3],
                "depth": r[4] if r[4] else 10.0,
            })
        except (ValueError, TypeError):
            continue

    if len(events) < 100:
        raise RuntimeError(f"Insufficient data: {len(events)} events")

    t0 = events[0]["time"]
    for e in events:
        e["t_days"] = (e["time"] - t0).total_seconds() / 86400

    fm_dict = {}
    for r in fm_rows:
        fm_dict[(round(r[0], 1), round(r[1], 1))] = (r[2], r[3], r[4])

    return events, fm_dict, t0


def save_npz_pure_python(filepath, arrays_dict):
    """Save arrays in NPZ-compatible format using only stdlib.

    Since numpy may not be available in CI, we save as a custom
    JSON-based format that can be loaded by numpy on Colab.
    """
    import zipfile
    import io

    # Save as JSON inside a zip (lighter alternative to NPZ)
    # The Colab script will handle the conversion
    json_path = filepath.with_suffix(".json")

    # Convert to serializable format
    serializable = {}
    for key, value in arrays_dict.items():
        if isinstance(value, list):
            serializable[key] = value
        else:
            serializable[key] = str(value)

    with open(json_path, "w") as f:
        json.dump(serializable, f, indent=None, ensure_ascii=False)

    logger.info("  Feature matrix saved as JSON: %s", json_path)
    return json_path


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)

    logger.info("=== Export Feature Matrix for ConvLSTM ===")
    logger.info("  Grid: %d×%d cells (%.0f°), %d features",
                GRID_H, GRID_W, CELL_SIZE_DEG, N_FEATURES)

    # Load data
    events, fm_dict, t0 = await load_events(DB_PATH)
    logger.info("  Loaded %d events", len(events))

    # Build grid mapping
    lat_to_row, lon_to_col, cell_coords_grid = build_grid_mapping()

    # Target events by cell
    target_by_cell = {}
    for e in events:
        if e["mag"] >= MIN_TARGET_MAG:
            ck = cell_key(e["lat"], e["lon"])
            target_by_cell.setdefault(ck, []).append(e["t_days"])

    # Feature extractor (Phase 9 data loaded via results files if available)
    extractor = FeatureExtractor(events, fm_dict, t0)
    # Note: Phase 9 data (cosmic_ray, lightning, etc.) will be loaded
    # when running full pipeline via ml_prediction.py. Export uses defaults.

    # Generate time steps
    total_t_days = events[-1]["t_days"]
    start_day = 180
    end_day = total_t_days - PREDICTION_WINDOW_DAYS

    timestep_features = []  # list of (t_days, features_grid, labels_grid)
    day = start_day
    n_steps = 0

    while day <= end_day:
        # Extract features for all cells at this time step
        features_grid = []
        labels_grid = []

        for lat in range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG)):
            feat_row = []
            label_row = []
            for lon in range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG)):
                clat, clon = float(lat), float(lon)
                features = extractor.extract(clat, clon, day)
                label = generate_label(clat, clon, day, target_by_cell, PREDICTION_WINDOW_DAYS)
                feat_row.append(features)
                label_row.append(label)
            features_grid.append(feat_row)
            labels_grid.append(label_row)

        timestep_features.append({
            "t_days": round(day, 1),
            "features": features_grid,
            "labels": labels_grid,
        })

        n_steps += 1
        day += STEP_DAYS

        if n_steps % 500 == 0:
            logger.info("  Generated %d time steps (day %.0f/%.0f)...", n_steps, day, end_day)

    logger.info("  Total: %d time steps, grid %d×%d, %d features",
                n_steps, GRID_H, GRID_W, N_FEATURES)

    # Count positives
    total_pos = sum(
        sum(sum(row) for row in ts["labels"])
        for ts in timestep_features
    )
    logger.info("  Total positive cells: %d (%.2f%%)",
                total_pos, 100 * total_pos / max(n_steps * GRID_H * GRID_W, 1))

    # Save as JSON (Colab script converts to numpy tensors)
    output_data = {
        "metadata": {
            "n_timesteps": n_steps,
            "grid_h": GRID_H,
            "grid_w": GRID_W,
            "n_features": N_FEATURES,
            "feature_names": FEATURE_NAMES,
            "cell_size_deg": CELL_SIZE_DEG,
            "grid_lat_range": [GRID_LAT_MIN, GRID_LAT_MAX],
            "grid_lon_range": [GRID_LON_MIN, GRID_LON_MAX],
            "prediction_window_days": PREDICTION_WINDOW_DAYS,
            "min_target_mag": MIN_TARGET_MAG,
            "step_days": STEP_DAYS,
            "total_positives": total_pos,
        },
        "cell_coords": cell_coords_grid,
        "timesteps": timestep_features,
    }

    out_path = RESULTS_DIR / "feature_matrix.json"
    with open(out_path, "w") as f:
        json.dump(output_data, f, indent=None, ensure_ascii=False)

    # File size
    size_mb = out_path.stat().st_size / (1024 * 1024)
    logger.info("  Feature matrix saved: %s (%.1f MB)", out_path, size_mb)


if __name__ == "__main__":
    asyncio.run(main())
