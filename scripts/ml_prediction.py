"""ML-integrated earthquake prediction — Phase 6 overhaul.

Major changes from Phase 5:
    1. 35 temporal features (was 11 static)
    2. sklearn HistGradientBoostingClassifier (was pure Python AdaBoost)
    3. Walk-forward CV with expanding window (was single train/test split)
    4. ETAS MLE parameter fitting per tectonic zone (was fixed literature values)
    5. Rate-and-state CFS (was non-decaying cumulative)
    6. Isotonic calibration for reliable probabilities
    7. Permutation importance (was single-feature AUC only)

Target: M5.0+ within 7 days in 2°×2° cells.

References:
    - Ogata (1998) Space-time ETAS
    - van den Ende & Ampuero (2020) ML + physics earthquake prediction
    - Zechar & Jordan (2008) Testing alarm-based predictions
    - Molchan (1991) Strong earthquake prediction strategies
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

# Prediction parameters
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

def build_dataset(events, fm_dict, t0, etas_params=None):
    """Generate feature matrix and labels.

    Returns:
        samples: list of (cell_lat, cell_lon, t_days, features, label)
        active_cells: set of (lat, lon)
        metadata: dict
    """
    # Target events (M5+) by cell
    target_by_cell = {}
    for e in events:
        if e["mag"] >= MIN_TARGET_MAG:
            ck = cell_key(e["lat"], e["lon"])
            target_by_cell.setdefault(ck, []).append(e["t_days"])

    # Active cells
    active_cells = set()
    for e in events:
        ck = cell_key(e["lat"], e["lon"])
        if GRID_LAT_MIN <= ck[0] <= GRID_LAT_MAX and GRID_LON_MIN <= ck[1] <= GRID_LON_MAX:
            active_cells.add(ck)

    logger.info("  Active 2° cells: %d", len(active_cells))

    # Feature extractor
    extractor = FeatureExtractor(events, fm_dict, t0, etas_params)

    # Generate samples
    total_t_days = events[-1]["t_days"]
    start_day = 180  # need 180 days of history for all features
    end_day = total_t_days - PREDICTION_WINDOW_DAYS

    samples = []
    day = start_day
    n_total = 0

    while day <= end_day:
        for clat, clon in active_cells:
            features = extractor.extract(clat, clon, day)
            label = generate_label(clat, clon, day, target_by_cell, PREDICTION_WINDOW_DAYS)
            samples.append((clat, clon, day, features, label))
            n_total += 1

        day += STEP_DAYS

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
        "prediction_window_days": PREDICTION_WINDOW_DAYS,
        "cell_size_deg": CELL_SIZE_DEG,
        "step_days": STEP_DAYS,
    }

    return samples, active_cells, metadata


# ---------------------------------------------------------------------------
# Model training with HistGradientBoosting
# ---------------------------------------------------------------------------

def train_model(X_train, y_train):
    """Train HistGradientBoostingClassifier.

    Falls back to pure Python AdaBoost if sklearn is unavailable.
    """
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
        model.fit(X_train, y_train)

        n_iter = model.n_iter_
        logger.info("  Trained %d iterations (early stopping)", n_iter)

        def predict_fn(X):
            return model.predict_proba(X)[:, 1].tolist()

        return predict_fn, {"type": "HistGradientBoosting", "n_iterations": int(n_iter),
                            "max_depth": 5, "learning_rate": 0.05, "l2_reg": 1.0}

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

def run_walk_forward_cv(samples, metadata):
    """Walk-forward cross-validation with expanding training window.

    Splits: train on [0, split_day), test on [split_day, split_day + 365.25)
    Starting from 5 years of training, advancing 1 year at a time.

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

        if n_pos_train < 10 or n_pos_test < 5:
            logger.warning("  Fold %d: skipping (insufficient positives)", fold_idx)
            continue

        # Train
        predict_fn, model_info = train_model(train_X, train_y)

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

def train_final_model(samples, events_t0):
    """Train final model and evaluate on holdout test set."""
    split_date = datetime(2019, 1, 1, tzinfo=timezone.utc)
    split_t_days = (split_date - events_t0).total_seconds() / 86400

    train_X, train_y = [], []
    test_X, test_y = [], []

    for _, _, t_day, features, label in samples:
        if t_day < split_t_days:
            train_X.append(features)
            train_y.append(label)
        else:
            test_X.append(features)
            test_y.append(label)

    if not train_X or not test_X:
        return None

    n_pos_train = sum(train_y)
    n_pos_test = sum(test_y)
    base_rate_train = n_pos_train / max(len(train_y), 1)
    base_rate_test = n_pos_test / max(len(test_y), 1)

    logger.info("--- Final model ---")
    logger.info("  Train: %d (pos=%d, %.2f%%) | Test: %d (pos=%d, %.2f%%)",
                len(train_y), n_pos_train, 100 * base_rate_train,
                len(test_y), n_pos_test, 100 * base_rate_test)

    # Train
    predict_fn, model_info = train_model(train_X, train_y)

    # Predict
    train_probs = predict_fn(train_X)
    test_probs = predict_fn(test_X)

    # ROC
    _, auc_train = compute_roc(train_y, train_probs)
    _, auc_test = compute_roc(test_y, test_probs)
    logger.info("  AUC-ROC: train=%.4f test=%.4f", auc_train, auc_test)

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
    for imp in importance[:10]:
        logger.info("  %s: importance=%.4f (±%.4f)", imp["feature"], imp["importance"], imp["std"])

    # Single-feature AUC for comparison
    sf_auc = single_feature_auc_ranking(test_X, test_y, FEATURE_NAMES)
    logger.info("--- Single-feature AUC ranking ---")
    for sf in sf_auc[:10]:
        logger.info("  %s: AUC=%.4f (%s)", sf["feature"], sf["auc"], sf["direction"])

    return {
        "model_info": model_info,
        "performance": {
            "auc_roc_train": round(auc_train, 4),
            "auc_roc_test": round(auc_test, 4),
            "auc_roc_calibrated": round(auc_cal, 4),
            "molchan_area_skill": molchan_skill,
            "base_rate_train": round(base_rate_train, 5),
            "base_rate_test": round(base_rate_test, 5),
        },
        "threshold_evaluation": threshold_eval,
        "reliability_diagram": reliability,
        "feature_importance_permutation": importance[:15],
        "feature_importance_single_auc": sf_auc[:15],
        "train_size": len(train_y),
        "test_size": len(test_y),
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_ml_prediction():
    """Full ML prediction pipeline."""
    logger.info("=== ML Integrated Earthquake Prediction (Phase 6) ===")
    logger.info("Target: M%.1f+ within %d days in %.0f° cells",
                MIN_TARGET_MAG, PREDICTION_WINDOW_DAYS, CELL_SIZE_DEG)
    logger.info("Features: %d temporal features", N_FEATURES)

    # Load data
    events, fm_dict, t0 = await load_events(DB_PATH)
    logger.info("  Loaded %d M3+ events, %d focal mechanisms", len(events), len(fm_dict))

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

    # Build dataset
    logger.info("--- Building feature dataset ---")
    samples, active_cells, metadata = build_dataset(events, fm_dict, t0, global_etas)

    if len(samples) < 1000:
        logger.error("Dataset too small (%d samples)", len(samples))
        return {"error": "dataset_too_small", "metadata": metadata}

    # Walk-forward CV
    logger.info("--- Walk-forward cross-validation ---")
    cv_folds, cv_aggregate, cv_all_y, cv_all_probs = run_walk_forward_cv(samples, metadata)

    # Overall CV AUC
    if cv_all_y:
        _, cv_overall_auc = compute_roc(cv_all_y, cv_all_probs)
        logger.info("  Overall CV AUC (pooled): %.4f", cv_overall_auc)
        cv_aggregate["pooled_auc"] = round(cv_overall_auc, 4)

    # Final model
    logger.info("--- Training final model (train 2011-2018, test 2019-2026) ---")
    final_results = train_final_model(samples, t0)

    # Compile results
    results = {
        "phase": "Phase 6",
        "metadata": metadata,
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
        "walk_forward_cv": {
            "aggregate": cv_aggregate,
            "folds": cv_folds,
        },
        "final_model": final_results,
        "interpretation": {
            "phase6_improvements": [
                "35 temporal features (was 11 static)",
                "HistGradientBoosting (was AdaBoost stumps)",
                "Walk-forward CV (was single split)",
                "ETAS MLE per zone (was fixed params)",
                "Isotonic calibration (was raw sigmoid)",
                "Permutation importance (was single-feature AUC)",
                "Rate-and-state CFS (was non-decaying)",
            ],
            "auc_meaning": "0.5=random, 1.0=perfect. >0.7 suggests useful skill.",
            "molchan_skill": ">0 better than random, 1=perfect.",
            "calibration": "Reliability diagram shows predicted vs observed frequency.",
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
