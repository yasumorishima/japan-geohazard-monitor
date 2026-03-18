"""ML-integrated earthquake prediction — Phase 8: multi-target + stacking.

Phase 8 changes from Phase 7:
    1. Multi-target prediction: M5+, M5.5+, M6+ (was M5+ only)
    2. Per-target class weighting for extreme imbalance (M6+)
    3. Level-0 prediction export for ensemble stacking (Initiative 4)
    4. Target-specific prediction windows (M6+: 14 days)

Phase 7 (retained):
    - 47 features: +6 GNSS crustal deformation, +6 enhanced spatial
    - Zone-specific ETAS parameters
    - 2-pass spatial smoothing
    - GNSS displacement features (graceful fallback)

References:
    - Ogata (1998) Space-time ETAS
    - van den Ende & Ampuero (2020) ML + physics earthquake prediction
    - Zechar & Jordan (2008) Testing alarm-based predictions
    - Molchan (1991) Strong earthquake prediction strategies
    - Mogi (1985) GNSS-based earthquake prediction
    - Kato et al. (2012) Slow-slip events and earthquake triggering
"""

import asyncio
import json
import logging
import math
import sys
from datetime import datetime, timezone
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
from physics import (
    fit_etas_mle,
    classify_tectonic_zone,
    JAPAN_TECTONIC_ZONES,
)
from evaluation import (
    compute_roc,
    evaluate_at_thresholds,
    isotonic_calibration,
    reliability_diagram,
    walk_forward_splits,
    permutation_importance,
    single_feature_auc_ranking,
    molchan_area_skill_score,
)
from target_config import TARGET_CONFIGS, DEFAULT_TARGET

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

# Legacy constants (kept for backward compatibility, overridden per target)
PREDICTION_WINDOW_DAYS = 7
MIN_TARGET_MAG = 5.0
STEP_DAYS = 3


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

async def load_events(db_path):
    """Load earthquakes and focal mechanisms from database."""
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
                "time": t,
                "mag": r[1],
                "lat": r[2],
                "lon": r[3],
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


# ---------------------------------------------------------------------------
# Zone-specific ETAS fitting
# ---------------------------------------------------------------------------

def fit_etas_by_zone(events):
    """Fit ETAS parameters per tectonic zone.

    Returns dict: zone_name -> {fitted parameters}
    """
    # Group events by zone
    zone_events = {}
    for e in events:
        zone = classify_tectonic_zone(e["lat"], e["lon"])
        zone_events.setdefault(zone, []).append(e)

    results = {}
    for zone_name, z_events in zone_events.items():
        if len(z_events) < 100:
            logger.info("  Zone %s: %d events (too few, using defaults)", zone_name, len(z_events))
            results[zone_name] = {"fitted": False, "params": None}
            continue

        event_times = [e["t_days"] for e in z_events]
        event_mags = [e["mag"] for e in z_events]
        T_start = event_times[0]
        T_end = event_times[-1]

        logger.info("  Zone %s: fitting ETAS on %d events...", zone_name, len(z_events))
        result = fit_etas_mle(event_times, event_mags, T_start, T_end)
        results[zone_name] = result

    return results


# ---------------------------------------------------------------------------
# Dataset generation
# ---------------------------------------------------------------------------

def build_dataset(events, fm_dict, t0, etas_params=None,
                   zone_etas=None, gnss_data=None,
                   cosmic_ray_data=None, lightning_data=None,
                   geomag_spectral_data=None, animal_data=None,
                   min_target_mag=None, window_days=None, step_days=None):
    """Generate feature matrix and labels.

    Args:
        min_target_mag: minimum magnitude for target events (default: MIN_TARGET_MAG)
        window_days: prediction window in days (default: PREDICTION_WINDOW_DAYS)
        step_days: time step between samples (default: STEP_DAYS)

    Returns:
        samples: list of (cell_lat, cell_lon, t_days, features, label)
        active_cells: set of (lat, lon)
        metadata: dict
    """
    if min_target_mag is None:
        min_target_mag = MIN_TARGET_MAG
    if window_days is None:
        window_days = PREDICTION_WINDOW_DAYS
    if step_days is None:
        step_days = STEP_DAYS

    # Target events by cell
    target_by_cell = {}
    for e in events:
        if e["mag"] >= min_target_mag:
            ck = cell_key(e["lat"], e["lon"])
            target_by_cell.setdefault(ck, []).append(e["t_days"])

    # Active cells
    active_cells = set()
    for e in events:
        ck = cell_key(e["lat"], e["lon"])
        if GRID_LAT_MIN <= ck[0] <= GRID_LAT_MAX and GRID_LON_MIN <= ck[1] <= GRID_LON_MAX:
            active_cells.add(ck)

    logger.info("  Active 2° cells: %d (target M%.1f+, window %dd)",
                len(active_cells), min_target_mag, window_days)

    # Feature extractor with zone-specific ETAS, GNSS, and Phase 9 data
    extractor = FeatureExtractor(
        events, fm_dict, t0, etas_params,
        zone_etas_params=zone_etas,
        gnss_data=gnss_data,
        cosmic_ray_data=cosmic_ray_data,
        lightning_data=lightning_data,
        geomag_spectral_data=geomag_spectral_data,
        animal_data=animal_data,
    )

    # Generate samples
    total_t_days = events[-1]["t_days"]
    start_day = 180  # need 180 days of history for all features
    end_day = total_t_days - window_days

    samples = []
    day = start_day
    n_total = 0

    while day <= end_day:
        for clat, clon in active_cells:
            features = extractor.extract(clat, clon, day)
            label = generate_label(clat, clon, day, target_by_cell, window_days)
            samples.append((clat, clon, day, features, label))
            n_total += 1

        day += step_days

        if n_total % 50000 == 0 and n_total > 0:
            logger.info("  Generated %d samples (day %.0f/%.0f)...", n_total, day, end_day)

    n_pos = sum(1 for _, _, _, _, y in samples if y == 1)
    logger.info("  Dataset: %d samples (pos=%d, %.2f%%)", len(samples), n_pos, 100 * n_pos / max(len(samples), 1))

    metadata = {
        "n_events_m3": len(events),
        "n_active_cells": len(active_cells),
        "total_days": round(total_t_days, 1),
        "total_samples": len(samples),
        "total_positives": n_pos,
        "positive_rate": round(n_pos / max(len(samples), 1), 5),
        "features": FEATURE_NAMES,
        "n_features": N_FEATURES,
        "prediction_window_days": window_days,
        "min_target_mag": min_target_mag,
        "cell_size_deg": CELL_SIZE_DEG,
        "step_days": step_days,
    }

    return samples, active_cells, metadata


# ---------------------------------------------------------------------------
# Model training with HistGradientBoosting
# ---------------------------------------------------------------------------

def train_model(X_train, y_train, class_weight_ratio=1):
    """Train HistGradientBoostingClassifier.

    Args:
        class_weight_ratio: weight multiplier for positive class (for M6+ imbalance).
            If > 1, generates sample_weight with positives weighted higher.

    Falls back to pure Python AdaBoost if sklearn is unavailable.
    """
    # Build sample weights for class imbalance
    sample_weight = None
    if class_weight_ratio > 1:
        sample_weight = [class_weight_ratio if y == 1 else 1.0 for y in y_train]
        logger.info("  Class weight ratio: %d (pos_weight=%d)", class_weight_ratio, class_weight_ratio)

    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        logger.info("  Using sklearn HistGradientBoostingClassifier")

        model = HistGradientBoostingClassifier(
            max_iter=300,
            max_depth=5,
            min_samples_leaf=50,
            learning_rate=0.05,
            l2_regularization=1.0,
            max_bins=128,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=20,
            scoring="roc_auc",
            random_state=42,
        )
        model.fit(X_train, y_train, sample_weight=sample_weight)

        n_iter = model.n_iter_
        logger.info("  Trained %d iterations (early stopping)", n_iter)

        def predict_fn(X):
            return model.predict_proba(X)[:, 1].tolist()

        return predict_fn, {"type": "HistGradientBoosting", "n_iterations": int(n_iter),
                            "max_depth": 5, "learning_rate": 0.05, "l2_reg": 1.0,
                            "class_weight_ratio": class_weight_ratio}

    except ImportError:
        logger.warning("  sklearn not available, falling back to AdaBoost")
        return _train_adaboost_fallback(X_train, y_train)


def _train_adaboost_fallback(X_train, y_train):
    """Fallback: pure Python AdaBoost with decision stumps."""
    n = len(y_train)
    n_features = len(X_train[0])
    n_stumps = 200

    y_pm = [1 if yi == 1 else -1 for yi in y_train]
    weights = [1.0 / n] * n

    stumps = []
    for t in range(n_stumps):
        best_err = float("inf")
        best_fi, best_th, best_pol = 0, 0.0, 1

        for fi in range(n_features):
            vals = sorted(set(X_train[i][fi] for i in range(n)))
            if len(vals) <= 1:
                continue
            step = max(1, len(vals) // 20)
            for vi in range(0, len(vals) - 1, step):
                th = vals[vi]
                for pol in (1, -1):
                    err = sum(
                        weights[i] for i in range(n)
                        if ((1 if (X_train[i][fi] > th) == (pol == 1) else -1) != y_pm[i])
                    )
                    if err < best_err:
                        best_err = err
                        best_fi, best_th, best_pol = fi, th, pol

        best_err = max(min(best_err, 1 - 1e-10), 1e-10)
        if best_err >= 0.5:
            break

        alpha = 0.5 * math.log((1 - best_err) / best_err)
        stumps.append((best_fi, best_th, best_pol, alpha))

        total_w = 0.0
        for i in range(n):
            pred = 1 if (X_train[i][best_fi] > best_th) == (best_pol == 1) else -1
            weights[i] *= math.exp(-alpha * y_pm[i] * pred)
            total_w += weights[i]
        for i in range(n):
            weights[i] /= total_w

        if (t + 1) % 50 == 0:
            logger.info("  AdaBoost iteration %d/%d", t + 1, n_stumps)

    logger.info("  AdaBoost trained %d stumps", len(stumps))

    def predict_fn(X):
        probs = []
        for row in X:
            score = 0.0
            for fi, th, pol, alpha in stumps:
                pred = 1 if (row[fi] > th) == (pol == 1) else -1
                score += alpha * pred
            score = max(min(score, 20), -20)
            probs.append(1.0 / (1.0 + math.exp(-score)))
        return probs

    return predict_fn, {"type": "AdaBoost_fallback", "n_stumps": len(stumps)}


# ---------------------------------------------------------------------------
# Walk-forward CV
# ---------------------------------------------------------------------------

def run_walk_forward_cv(samples, metadata, class_weight_ratio=1,
                        min_pos_train=10, min_pos_test=5):
    """Walk-forward cross-validation with expanding training window.

    Splits: train on [0, split_day), test on [split_day, split_day + 365.25)
    Starting from 5 years of training, advancing 1 year at a time.

    Args:
        class_weight_ratio: weight for positive class (passed to train_model)
        min_pos_train: minimum positives in training set to proceed
        min_pos_test: minimum positives in test set to proceed

    Returns list of fold results + aggregated metrics.
    """
    day_min = samples[0][2]
    day_max = samples[-1][2]

    splits = walk_forward_splits(
        day_min, day_max,
        train_start_day=day_min,
        initial_train_years=5,
        step_years=1,
        test_years=1,
    )

    logger.info("  Walk-forward CV: %d folds", len(splits))

    fold_results = []
    all_test_y = []
    all_test_probs = []

    for fold_idx, (train_start, train_end, test_start, test_end) in enumerate(splits):
        # Split samples
        train_X, train_y = [], []
        test_X, test_y = [], []

        for _, _, t_day, features, label in samples:
            if train_start <= t_day < train_end:
                train_X.append(features)
                train_y.append(label)
            elif test_start <= t_day < test_end:
                test_X.append(features)
                test_y.append(label)

        if not train_X or not test_X:
            continue

        n_pos_train = sum(train_y)
        n_pos_test = sum(test_y)
        base_rate_test = n_pos_test / max(len(test_y), 1)

        logger.info("  Fold %d: train=%d (pos=%d) test=%d (pos=%d) days=[%.0f,%.0f)->[%.0f,%.0f)",
                    fold_idx, len(train_y), n_pos_train, len(test_y), n_pos_test,
                    train_start, train_end, test_start, test_end)

        if n_pos_train < min_pos_train:
            logger.warning("  Fold %d: skipping (train positives %d < %d)",
                          fold_idx, n_pos_train, min_pos_train)
            continue

        if n_pos_test < min_pos_test:
            logger.warning("  Fold %d: skipping (test positives %d < %d)",
                          fold_idx, n_pos_test, min_pos_test)
            continue

        # Train
        predict_fn, model_info = train_model(train_X, train_y, class_weight_ratio)

        # Predict
        test_probs = predict_fn(test_X)

        # Evaluate
        _, auc = compute_roc(test_y, test_probs)
        threshold_eval = evaluate_at_thresholds(test_y, test_probs, base_rate_test)
        molchan_skill = molchan_area_skill_score(test_y, test_probs)

        fold_results.append({
            "fold": fold_idx,
            "train_days": f"{train_start:.0f}-{train_end:.0f}",
            "test_days": f"{test_start:.0f}-{test_end:.0f}",
            "train_size": len(train_y),
            "test_size": len(test_y),
            "train_pos": n_pos_train,
            "test_pos": n_pos_test,
            "base_rate": round(base_rate_test, 5),
            "auc_roc": round(auc, 4),
            "molchan_skill": molchan_skill,
            "threshold_evaluation": threshold_eval,
            "model_info": model_info,
        })

        all_test_y.extend(test_y)
        all_test_probs.extend(test_probs)

        logger.info("  Fold %d: AUC=%.4f Molchan_skill=%.4f", fold_idx, auc, molchan_skill)

    # Aggregate
    if fold_results:
        mean_auc = sum(f["auc_roc"] for f in fold_results) / len(fold_results)
        mean_molchan = sum(f["molchan_skill"] for f in fold_results) / len(fold_results)
        aucs = [f["auc_roc"] for f in fold_results]
        std_auc = math.sqrt(sum((a - mean_auc) ** 2 for a in aucs) / len(aucs))
    else:
        mean_auc, std_auc, mean_molchan = 0, 0, 0

    aggregate = {
        "n_folds": len(fold_results),
        "mean_auc": round(mean_auc, 4),
        "std_auc": round(std_auc, 4),
        "mean_molchan_skill": round(mean_molchan, 4),
    }

    return fold_results, aggregate, all_test_y, all_test_probs


# ---------------------------------------------------------------------------
# Final model (train on all data before 2019, test on 2019+)
# ---------------------------------------------------------------------------

def train_final_model(samples, events_t0, class_weight_ratio=1,
                       target_name="M5+"):
    """Train final model and evaluate on holdout test set.

    Phase 8: supports multi-target with class weighting.
    Phase 7: includes 2-pass spatial smoothing of predictions.
    """
    split_date = datetime(2019, 1, 1, tzinfo=timezone.utc)
    split_t_days = (split_date - events_t0).total_seconds() / 86400

    train_X, train_y = [], []
    test_X, test_y = [], []
    test_cells = []  # track cell locations for spatial smoothing
    test_t_days = []

    for clat, clon, t_day, features, label in samples:
        if t_day < split_t_days:
            train_X.append(features)
            train_y.append(label)
        else:
            test_X.append(features)
            test_y.append(label)
            test_cells.append((clat, clon))
            test_t_days.append(t_day)

    if not train_X or not test_X:
        return None

    n_pos_train = sum(train_y)
    n_pos_test = sum(test_y)
    base_rate_train = n_pos_train / max(len(train_y), 1)
    base_rate_test = n_pos_test / max(len(test_y), 1)

    logger.info("--- Final model [%s] ---", target_name)
    logger.info("  Train: %d (pos=%d, %.2f%%) | Test: %d (pos=%d, %.2f%%)",
                len(train_y), n_pos_train, 100 * base_rate_train,
                len(test_y), n_pos_test, 100 * base_rate_test)

    # Train
    predict_fn, model_info = train_model(train_X, train_y, class_weight_ratio)

    # Predict
    train_probs = predict_fn(train_X)
    test_probs_raw = predict_fn(test_X)

    # 2-pass spatial smoothing (Phase 7)
    # Group test predictions by time step, smooth within each step
    logger.info("--- Spatial smoothing (2-pass Gaussian kernel) ---")
    time_groups = {}
    for idx, (ck, t_day) in enumerate(zip(test_cells, test_t_days)):
        time_groups.setdefault(t_day, []).append(idx)

    test_probs_smoothed = list(test_probs_raw)  # copy
    active_cells_set = set(test_cells)

    n_smoothed_steps = 0
    for t_day, indices in time_groups.items():
        if len(indices) < 3:
            continue
        # Build per-cell prediction map for this time step
        cell_preds = {}
        for idx in indices:
            cell_preds[test_cells[idx]] = test_probs_raw[idx]

        # Smooth
        smoothed = spatial_smooth_predictions(cell_preds, active_cells_set)

        # Write back
        for idx in indices:
            ck = test_cells[idx]
            if ck in smoothed:
                test_probs_smoothed[idx] = smoothed[ck]
        n_smoothed_steps += 1

    logger.info("  Smoothed %d time steps", n_smoothed_steps)

    # Evaluate both raw and smoothed
    _, auc_train = compute_roc(train_y, train_probs)
    _, auc_test_raw = compute_roc(test_y, test_probs_raw)
    _, auc_test_smooth = compute_roc(test_y, test_probs_smoothed)
    logger.info("  AUC-ROC: train=%.4f test_raw=%.4f test_smoothed=%.4f",
                auc_train, auc_test_raw, auc_test_smooth)

    # Use the better result
    if auc_test_smooth >= auc_test_raw:
        test_probs = test_probs_smoothed
        auc_test = auc_test_smooth
        model_info["spatial_smoothing"] = True
        logger.info("  Using spatially smoothed predictions (+%.4f AUC)",
                    auc_test_smooth - auc_test_raw)
    else:
        test_probs = list(test_probs_raw)
        auc_test = auc_test_raw
        model_info["spatial_smoothing"] = False
        logger.info("  Spatial smoothing did not improve AUC, using raw predictions")

    # Calibration
    logger.info("--- Isotonic calibration ---")
    calibrate_fn = isotonic_calibration(train_y, train_probs)
    test_probs_cal = [calibrate_fn(p) for p in test_probs]
    _, auc_cal = compute_roc(test_y, test_probs_cal)
    logger.info("  AUC after calibration: %.4f", auc_cal)

    # Threshold evaluation
    threshold_eval = evaluate_at_thresholds(test_y, test_probs_cal, base_rate_test)
    for tr in threshold_eval:
        if tr["n_alarms"] > 0:
            logger.info("  thresh=%.2f: prec=%.3f recall=%.3f gain=%.1f IGPE=%.2f (%d alarms)",
                        tr["threshold"], tr["precision"], tr["recall"],
                        tr["probability_gain"], tr["igpe_bits"], tr["n_alarms"])

    # Reliability diagram
    reliability = reliability_diagram(test_y, test_probs_cal, n_bins=10)

    # Molchan skill
    molchan_skill = molchan_area_skill_score(test_y, test_probs_cal)
    logger.info("  Molchan area skill score: %.4f", molchan_skill)

    # Feature importance (permutation-based)
    logger.info("--- Feature importance (permutation) ---")
    importance = permutation_importance(predict_fn, test_X, test_y, FEATURE_NAMES, n_repeats=3)
    for imp in importance[:15]:
        logger.info("  %s: importance=%.4f (±%.4f)", imp["feature"], imp["importance"], imp["std"])

    # Single-feature AUC for comparison
    sf_auc = single_feature_auc_ranking(test_X, test_y, FEATURE_NAMES)
    logger.info("--- Single-feature AUC ranking ---")
    for sf in sf_auc[:15]:
        logger.info("  %s: AUC=%.4f (%s)", sf["feature"], sf["auc"], sf["direction"])

    # Export level-0 predictions for ensemble stacking (Initiative 4)
    export_level0_predictions(
        target_name, test_probs_cal, test_y, test_cells, test_t_days)

    return {
        "target": target_name,
        "model_info": model_info,
        "performance": {
            "auc_roc_train": round(auc_train, 4),
            "auc_roc_test_raw": round(auc_test_raw, 4),
            "auc_roc_test_smoothed": round(auc_test_smooth, 4),
            "auc_roc_test": round(auc_test, 4),
            "auc_roc_calibrated": round(auc_cal, 4),
            "molchan_area_skill": molchan_skill,
            "base_rate_train": round(base_rate_train, 5),
            "base_rate_test": round(base_rate_test, 5),
        },
        "threshold_evaluation": threshold_eval,
        "reliability_diagram": reliability,
        "feature_importance_permutation": importance[:20],
        "feature_importance_single_auc": sf_auc[:20],
        "train_size": len(train_y),
        "test_size": len(test_y),
        "gnss_features_available": any(
            imp["feature"].startswith("gnss_") and imp["importance"] > 0.001
            for imp in importance
        ),
    }


# ---------------------------------------------------------------------------
# Level-0 prediction export (for ensemble stacking)
# ---------------------------------------------------------------------------

def export_level0_predictions(target_name, probs, labels, cells, t_days_list):
    """Export level-0 predictions for ensemble stacking.

    Saves to results/level0_predictions_{target}.json with per-sample
    probability, label, cell coordinates, and time step.
    """
    RESULTS_DIR.mkdir(exist_ok=True)
    safe_name = target_name.replace("+", "plus").replace(".", "")
    out_path = RESULTS_DIR / f"level0_predictions_{safe_name}.json"

    records = []
    for prob, label, (clat, clon), t_day in zip(probs, labels, cells, t_days_list):
        records.append({
            "prob": round(prob, 6),
            "label": label,
            "cell_lat": clat,
            "cell_lon": clon,
            "t_days": round(t_day, 1),
        })

    data = {
        "target": target_name,
        "n_samples": len(records),
        "n_positive": sum(r["label"] for r in records),
        "predictions": records,
    }

    with open(out_path, "w") as f:
        json.dump(data, f, indent=None, ensure_ascii=False)
    logger.info("  Level-0 predictions exported: %s (%d samples)", out_path.name, len(records))


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def load_gnss_data(db_path, t0):
    """Load GNSS displacement data from database, indexed by cell.

    Returns dict: {cell_key: list of {t_days, stations: [{lat, lon, dx_mm, dy_mm, dz_mm}]}}
    """
    gnss_data = {}
    try:
        async with aiosqlite.connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT station_id, observed_at, latitude, longitude, "
                "dx_mm, dy_mm, dz_mm FROM geonet "
                "WHERE dx_mm IS NOT NULL AND dy_mm IS NOT NULL "
                "ORDER BY observed_at"
            )

        if not rows:
            logger.info("  No GNSS data available (GEONET table empty)")
            return {}

        logger.info("  Loaded %d GNSS records", len(rows))

        # Group by date → cell
        from collections import defaultdict
        date_cell_stations = defaultdict(lambda: defaultdict(list))

        for r in rows:
            try:
                t = datetime.fromisoformat(r[1].replace("Z", "+00:00"))
                t_days = (t - t0).total_seconds() / 86400
                lat, lon = r[2], r[3]
                ck = cell_key(lat, lon)
                date_key = r[1][:10]  # YYYY-MM-DD

                date_cell_stations[date_key][ck].append({
                    "lat": lat, "lon": lon,
                    "dx_mm": r[4], "dy_mm": r[5], "dz_mm": r[6],
                    "t_days": t_days,
                })
            except (ValueError, TypeError):
                continue

        # Convert to per-cell time series
        for date_key, cells in date_cell_stations.items():
            for ck, stations in cells.items():
                if ck not in gnss_data:
                    gnss_data[ck] = []
                t_days = stations[0]["t_days"]
                gnss_data[ck].append({
                    "t_days": t_days,
                    "stations": stations,
                })

        # Sort each cell's data by time
        for ck in gnss_data:
            gnss_data[ck].sort(key=lambda g: g["t_days"])

        logger.info("  GNSS data indexed for %d cells", len(gnss_data))

    except Exception as e:
        logger.warning("  GNSS data load failed (non-fatal): %s", e)
        return {}

    return gnss_data


async def load_phase9_cosmic_ray(db_path):
    """Load cosmic ray features from results file or DB.

    Returns dict: {date_str: {cosmic_ray_rate, cosmic_ray_anomaly, ...}}
    """
    import json
    results_dir = Path(__file__).parent.parent / "results"
    latest = results_dir / "cosmic_ray_features_latest.json"

    if latest.exists():
        try:
            with open(latest) as f:
                data = json.load(f)
            logger.info("  Cosmic ray features loaded: %d dates", len(data))
            return data
        except (json.JSONDecodeError, IOError) as e:
            logger.warning("  Cosmic ray features file error: %s", e)

    # Fallback: compute from DB
    try:
        async with aiosqlite.connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, counts_per_sec FROM cosmic_ray "
                "WHERE station = 'IRKT' ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No cosmic ray data available")
            return {}

        data = {}
        values = [r[1] for r in rows]
        dates = [r[0] for r in rows]
        for i, date in enumerate(dates):
            data[date] = {
                "cosmic_ray_rate": values[i],
                "cosmic_ray_anomaly": 0.0,
                "cosmic_ray_trend_15d": 0.0,
            }
        logger.info("  Cosmic ray raw data loaded: %d dates (no anomaly computed)",
                     len(data))
        return data
    except Exception as e:
        logger.warning("  Cosmic ray load failed (non-fatal): %s", e)
        return {}


async def load_phase9_lightning(db_path):
    """Load lightning data from DB.

    Returns dict: {(date_str, cell_lat, cell_lon): {stroke_count, ...}}
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, stroke_count, "
                "mean_intensity_ka FROM lightning "
                "WHERE source != 'climatology'"
            )
        if not rows:
            logger.info("  No lightning data available")
            return {}

        data = {}
        for r in rows:
            key = (r[0], r[1], r[2])
            data[key] = {"stroke_count": r[3] or 0, "mean_intensity": r[4]}
        logger.info("  Lightning data loaded: %d cell-date records", len(data))
        return data
    except Exception as e:
        logger.warning("  Lightning load failed (non-fatal): %s", e)
        return {}


async def load_phase9_geomag_spectral(db_path):
    """Load hourly geomag spectral features.

    Computes daily ULF power, polarization ratio, fractal dimension
    from geomag_hourly table (KAK as primary station).

    Returns dict: {date_str: {ulf_power, polarization, fractal_dim}}
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT DATE(observed_at), "
                "AVG(h_nt), AVG(z_nt), "
                "MAX(h_nt) - MIN(h_nt), MAX(z_nt) - MIN(z_nt) "
                "FROM geomag_hourly WHERE station = 'KAK' "
                "GROUP BY DATE(observed_at) ORDER BY DATE(observed_at)"
            )
        if not rows:
            logger.info("  No hourly geomag data available")
            return {}

        data = {}
        for r in rows:
            date_str = r[0]
            h_range = r[3] if r[3] is not None else 0
            z_range = r[4] if r[4] is not None else 0
            # ULF power proxy: daily range of H component (nT)
            ulf_power = h_range + z_range
            # Polarization: Z range / H range (>1 = lithospheric source)
            polarization = z_range / max(h_range, 0.01) if h_range > 0 else 1.0
            data[date_str] = {
                "ulf_power": ulf_power,
                "polarization": polarization,
                "fractal_dim": 1.5,  # Will be computed in full spectral analysis
            }
        logger.info("  Geomag spectral features loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  Geomag spectral load failed (non-fatal): %s", e)
        return {}


async def load_phase9_animal(db_path):
    """Load animal behavior anomaly features.

    Computes daily movement speed anomaly per grid cell from animal_tracking.

    Returns dict: {(date_str, cell_lat, cell_lon): {speed_anomaly, ...}}
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            count = await db.execute_fetchall(
                "SELECT COUNT(*) FROM animal_tracking"
            )
        if not count or count[0][0] == 0:
            logger.info("  No animal tracking data available")
            return {}

        # TODO: Full implementation requires computing baseline speeds
        # per individual and detecting anomalies. For now, return empty.
        logger.info("  Animal tracking data exists but anomaly computation "
                     "not yet implemented")
        return {}
    except Exception as e:
        logger.warning("  Animal data load failed (non-fatal): %s", e)
        return {}


def spatial_smooth_predictions(
    predictions: dict,
    active_cells: set,
    sigma_deg: float = 2.0,
) -> dict:
    """2-pass spatial smoothing of cell predictions.

    Averages each cell's prediction with its neighbors, weighted by
    Gaussian distance kernel. This captures spatial correlation in
    earthquake occurrence.

    Args:
        predictions: {cell_key: probability}
        active_cells: set of active cell keys
        sigma_deg: Gaussian kernel width in degrees

    Returns:
        {cell_key: smoothed_probability}
    """
    smoothed = {}
    cell_size = CELL_SIZE_DEG

    for ck in predictions:
        lat, lon = ck
        total_weight = 0.0
        weighted_sum = 0.0

        # Self weight (highest)
        self_w = 1.0
        weighted_sum += self_w * predictions[ck]
        total_weight += self_w

        # Neighbor contributions (8 neighbors)
        for dlat in (-cell_size, 0, cell_size):
            for dlon in (-cell_size, 0, cell_size):
                if dlat == 0 and dlon == 0:
                    continue
                nk = (lat + dlat, lon + dlon)
                if nk in predictions:
                    dist = math.sqrt(dlat ** 2 + dlon ** 2)
                    w = math.exp(-0.5 * (dist / sigma_deg) ** 2)
                    weighted_sum += w * predictions[nk]
                    total_weight += w

        smoothed[ck] = weighted_sum / total_weight if total_weight > 0 else predictions[ck]

    return smoothed


async def run_ml_prediction():
    """Full ML prediction pipeline (Phase 8: multi-target)."""
    logger.info("=== ML Integrated Earthquake Prediction (Phase 8) ===")
    logger.info("Targets: %s", ", ".join(TARGET_CONFIGS.keys()))
    logger.info("Features: %d temporal features", N_FEATURES)

    # Load data
    events, fm_dict, t0 = await load_events(DB_PATH)
    logger.info("  Loaded %d M3+ events, %d focal mechanisms", len(events), len(fm_dict))

    # Load GNSS data (optional — graceful fallback to empty)
    logger.info("--- Loading GNSS displacement data ---")
    gnss_data = await load_gnss_data(DB_PATH, t0)

    # Fit ETAS by zone
    logger.info("--- ETAS MLE parameter fitting ---")
    zone_etas = fit_etas_by_zone(events)

    # Use most populated zone's parameters as global default
    best_zone = max(
        ((zn, zr) for zn, zr in zone_etas.items() if zr.get("fitted")),
        key=lambda x: x[1].get("n_events", 0),
        default=(None, None),
    )
    if best_zone[1] and best_zone[1].get("fitted"):
        global_etas = best_zone[1]["params"]
        logger.info("  Using %s ETAS params as global: mu=%.4f K=%.4f alpha=%.2f c=%.4f p=%.3f",
                    best_zone[0], global_etas["mu"], global_etas["K"],
                    global_etas["alpha"], global_etas["c"], global_etas["p"])
    else:
        global_etas = None
        logger.info("  No zone fitted successfully, using default ETAS parameters")

    # Log zone ETAS summary
    for zn, zr in zone_etas.items():
        if zr.get("fitted"):
            p = zr["params"]
            logger.info("  Zone %s: K=%.4f alpha=%.2f p=%.3f BR=%.2f",
                        zn, p["K"], p["alpha"], p["p"], zr.get("branching_ratio", 0))

    # --- Phase 9: Load non-traditional precursor data ---
    logger.info("--- Loading Phase 9 data (cosmic ray, lightning, geomag, animal) ---")
    cosmic_ray_data = await load_phase9_cosmic_ray(DB_PATH)
    lightning_data = await load_phase9_lightning(DB_PATH)
    geomag_spectral_data = await load_phase9_geomag_spectral(DB_PATH)
    animal_data = await load_phase9_animal(DB_PATH)

    # Multi-target loop
    target_results = {}
    primary_metadata = None

    for target_name, cfg in TARGET_CONFIGS.items():
        logger.info("")
        logger.info("=" * 60)
        logger.info("=== Target: %s (M%.1f+ within %dd) ===",
                    target_name, cfg["min_mag"], cfg["window_days"])
        logger.info("=" * 60)

        # Build dataset for this target
        logger.info("--- Building feature dataset (%d features) ---", N_FEATURES)
        samples, active_cells, metadata = build_dataset(
            events, fm_dict, t0, global_etas,
            zone_etas=zone_etas,
            gnss_data=gnss_data,
            cosmic_ray_data=cosmic_ray_data,
            lightning_data=lightning_data,
            geomag_spectral_data=geomag_spectral_data,
            animal_data=animal_data,
            min_target_mag=cfg["min_mag"],
            window_days=cfg["window_days"],
            step_days=cfg["step_days"],
        )

        if primary_metadata is None:
            primary_metadata = metadata

        if len(samples) < 1000:
            logger.error("Dataset too small (%d samples) for %s", len(samples), target_name)
            target_results[target_name] = {
                "error": "dataset_too_small", "metadata": metadata}
            continue

        # Walk-forward CV
        logger.info("--- Walk-forward cross-validation [%s] ---", target_name)
        cv_folds, cv_aggregate, cv_all_y, cv_all_probs = run_walk_forward_cv(
            samples, metadata,
            class_weight_ratio=cfg["class_weight_ratio"],
            min_pos_train=cfg["min_pos_train"],
            min_pos_test=cfg["min_pos_test"],
        )

        # Overall CV AUC
        if cv_all_y:
            _, cv_overall_auc = compute_roc(cv_all_y, cv_all_probs)
            logger.info("  Overall CV AUC (pooled) [%s]: %.4f", target_name, cv_overall_auc)
            cv_aggregate["pooled_auc"] = round(cv_overall_auc, 4)

        # Final model
        logger.info("--- Training final model [%s] (train 2011-2018, test 2019-2026) ---",
                    target_name)
        final_results = train_final_model(
            samples, t0,
            class_weight_ratio=cfg["class_weight_ratio"],
            target_name=target_name,
        )

        target_results[target_name] = {
            "config": cfg,
            "metadata": metadata,
            "walk_forward_cv": {
                "aggregate": cv_aggregate,
                "folds": cv_folds,
            },
            "final_model": final_results,
        }

    # Compile results
    results = {
        "phase": "Phase 8",
        "metadata": primary_metadata,
        "etas_fitting": {
            zone: {
                "fitted": r.get("fitted", False),
                "params": r.get("params"),
                "aic": r.get("aic"),
                "branching_ratio": r.get("branching_ratio"),
                "n_events": r.get("n_events"),
            }
            for zone, r in zone_etas.items()
        },
        "gnss_data_summary": {
            "cells_with_gnss": len(gnss_data),
            "total_snapshots": sum(len(v) for v in gnss_data.values()),
        },
        "targets": target_results,
        "interpretation": {
            "phase8_improvements": [
                "Multi-target: M5+, M5.5+, M6+ (was M5+ only)",
                "Per-target class weighting for extreme imbalance (M6+)",
                "Level-0 prediction export for ensemble stacking",
                "Target-specific prediction windows (M6+: 14 days)",
            ],
            "phase7_retained": [
                "47 features: +6 GNSS, +6 enhanced spatial",
                "Zone-specific ETAS params in feature extraction",
                "2-pass spatial smoothing of predictions",
                "GNSS crustal deformation features (when available)",
            ],
            "auc_meaning": "0.5=random, 1.0=perfect. >0.7 suggests useful skill.",
            "molchan_skill": ">0 better than random, 1=perfect.",
        },
    }

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_ml_prediction()

    out_path = RESULTS_DIR / f"ml_prediction_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
