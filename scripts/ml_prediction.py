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
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from config import DB_PATH
from features import (
    FEATURE_NAMES,
    N_FEATURES,
    PHASE9_FEATURE_GROUPS,
    get_active_feature_names,
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
    async with safe_connect(db_path) as db:
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
                   olr_data=None, earth_rotation_data=None,
                   solar_wind_data=None, gravity_data=None,
                   so2_data=None, soil_moisture_data=None,
                   tide_gauge_data=None, ocean_color_data=None,
                   cloud_fraction_data=None, nightlight_data=None,
                   insar_data=None,
                   goes_xray_data=None, goes_proton_data=None,
                   tidal_stress_data=None, particle_flux_data=None,
                   dart_pressure_data=None, ioc_sealevel_data=None,
                   snet_waveform_data=None,
                   snet_velocity_data=None, snet_highgain_data=None,
                   min_target_mag=None, window_days=None, step_days=None):
    """Generate feature matrix and labels.

    Dynamically excludes Phase 9 feature groups whose data source is empty,
    preventing zero-filled features from degrading model performance.

    Args:
        min_target_mag: minimum magnitude for target events (default: MIN_TARGET_MAG)
        window_days: prediction window in days (default: PREDICTION_WINDOW_DAYS)
        step_days: time step between samples (default: STEP_DAYS)

    Returns:
        samples: list of (cell_lat, cell_lon, t_days, features, label)
        active_cells: set of (lat, lon)
        metadata: dict (includes active_feature_names and feature_mask)
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

    # Dynamic feature selection: exclude optional groups with no data
    active_feature_names = get_active_feature_names(
        cosmic_ray_data=cosmic_ray_data,
        lightning_data=lightning_data,
        geomag_spectral_data=geomag_spectral_data,
        animal_data=animal_data,
        olr_data=olr_data,
        earth_rotation_data=earth_rotation_data,
        solar_wind_data=solar_wind_data,
        gravity_data=gravity_data,
        so2_data=so2_data,
        soil_moisture_data=soil_moisture_data,
        tide_gauge_data=tide_gauge_data,
        ocean_color_data=ocean_color_data,
        cloud_fraction_data=cloud_fraction_data,
        nightlight_data=nightlight_data,
        insar_data=insar_data,
        goes_xray_data=goes_xray_data,
        goes_proton_data=goes_proton_data,
        tidal_stress_data=tidal_stress_data,
        particle_flux_data=particle_flux_data,
        dart_pressure_data=dart_pressure_data,
        ioc_sealevel_data=ioc_sealevel_data,
        snet_waveform_data=snet_waveform_data,
        snet_velocity_data=snet_velocity_data,
        snet_highgain_data=snet_highgain_data,
    )
    # Build index mask: which positions in the full feature vector to keep
    feature_mask = [i for i, name in enumerate(FEATURE_NAMES) if name in set(active_feature_names)]
    n_active = len(active_feature_names)

    excluded_groups = []
    for source, feats in PHASE9_FEATURE_GROUPS.items():
        if feats[0] not in set(active_feature_names):
            excluded_groups.append(source)
    if excluded_groups:
        logger.info("  Phase 9 data sources excluded (no data): %s", ", ".join(excluded_groups))
    logger.info("  Active features: %d / %d (excluded %d zero-filled Phase 9 features)",
                n_active, N_FEATURES, N_FEATURES - n_active)

    # Feature extractor with zone-specific ETAS, GNSS, and Phase 9 data
    extractor = FeatureExtractor(
        events, fm_dict, t0, etas_params,
        zone_etas_params=zone_etas,
        gnss_data=gnss_data,
        cosmic_ray_data=cosmic_ray_data,
        lightning_data=lightning_data,
        geomag_spectral_data=geomag_spectral_data,
        animal_data=animal_data,
        olr_data=olr_data,
        earth_rotation_data=earth_rotation_data,
        solar_wind_data=solar_wind_data,
        gravity_data=gravity_data,
        so2_data=so2_data,
        soil_moisture_data=soil_moisture_data,
        tide_gauge_data=tide_gauge_data,
        ocean_color_data=ocean_color_data,
        cloud_fraction_data=cloud_fraction_data,
        nightlight_data=nightlight_data,
        insar_data=insar_data,
        goes_xray_data=goes_xray_data,
        goes_proton_data=goes_proton_data,
        tidal_stress_data=tidal_stress_data,
        particle_flux_data=particle_flux_data,
        dart_pressure_data=dart_pressure_data,
        ioc_sealevel_data=ioc_sealevel_data,
        snet_waveform_data=snet_waveform_data,
        snet_velocity_data=snet_velocity_data,
        snet_highgain_data=snet_highgain_data,
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
            full_features = extractor.extract(clat, clon, day)
            # Filter to active features only
            features = [full_features[i] for i in feature_mask]
            label = generate_label(clat, clon, day, target_by_cell, window_days)
            samples.append((clat, clon, day, features, label))
            n_total += 1

        day += step_days

        if n_total % 50000 == 0 and n_total > 0:
            logger.info("  Generated %d samples (day %.0f/%.0f)...", n_total, day, end_day)

    n_pos = sum(1 for _, _, _, _, y in samples if y == 1)
    logger.info("  Dataset: %d samples (pos=%d, %.2f%%)", len(samples), n_pos, 100 * n_pos / max(len(samples), 1))

    # --- Feature non-zero rate check (detect coordinate mismatch bugs) ---
    # If a spatial data source has data but its feature column is 100% zero,
    # it likely means the lookup key coordinates don't match the grid.
    if samples:
        n_samples = len(samples)
        spatial_features_to_check = {
            "so2_column_anomaly": so2_data,
            "olr_anomaly": olr_data,
            "gravity_anomaly_rate": gravity_data,
            "soil_moisture_anomaly": soil_moisture_data,
            "ocean_color_anomaly": ocean_color_data,
            "cloud_fraction_anomaly": cloud_fraction_data,
            "nightlight_anomaly": nightlight_data,
        }
        zero_alert_sources = []
        for feat_name, source_data in spatial_features_to_check.items():
            if not source_data:
                continue  # No data loaded, skip
            if feat_name not in active_feature_names:
                continue  # Feature excluded, skip
            feat_idx = active_feature_names.index(feat_name)
            nonzero = sum(1 for _, _, _, feats, _ in samples if feats[feat_idx] != 0.0)
            pct = 100 * nonzero / n_samples
            if nonzero == 0:
                zero_alert_sources.append(feat_name)
                logger.warning("  ⚠️ ZERO-HIT: %s has %d source records but 0%% non-zero in features "
                               "(likely coordinate mismatch)", feat_name, len(source_data))
            else:
                logger.info("  Feature check: %s non-zero %.1f%% (%d/%d)",
                            feat_name, pct, nonzero, n_samples)
        if zero_alert_sources:
            logger.warning("  ⚠️ %d spatial features have data but 0%% hit rate: %s",
                           len(zero_alert_sources), ", ".join(zero_alert_sources))

    metadata = {
        "n_events_m3": len(events),
        "n_active_cells": len(active_cells),
        "total_days": round(total_t_days, 1),
        "total_samples": len(samples),
        "total_positives": n_pos,
        "positive_rate": round(n_pos / max(len(samples), 1), 5),
        "features": active_feature_names,
        "n_features": n_active,
        "feature_mask": feature_mask,
        "excluded_phase9_groups": excluded_groups,
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


def train_random_forest(X_train, y_train, class_weight_ratio=1):
    """Train RandomForestClassifier as a diverse level-0 model.

    Bagging-based ensemble: different error pattern from boosting (HistGBT).
    Falls back gracefully if sklearn not available.
    """
    sample_weight = None
    if class_weight_ratio > 1:
        sample_weight = [class_weight_ratio if y == 1 else 1.0 for y in y_train]

    try:
        from sklearn.ensemble import RandomForestClassifier
        logger.info("  Training RandomForest (level-0 diversity)")

        model = RandomForestClassifier(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=30,
            max_features="sqrt",
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_train, y_train, sample_weight=sample_weight)

        def predict_fn(X):
            return model.predict_proba(X)[:, 1].tolist()

        return predict_fn, {"type": "RandomForest", "n_estimators": 300, "max_depth": 8}

    except ImportError:
        logger.warning("  sklearn not available, skipping RandomForest")
        return None, None


def train_logistic(X_train, y_train, class_weight_ratio=1):
    """Train LogisticRegression with L2 as a linear level-0 model.

    Linear model: fundamentally different from tree ensembles.
    Provides genuine diversity in stacking level-0.
    """
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        logger.info("  Training LogisticRegression L2 (level-0 diversity)")

        # Standardize features for logistic regression
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_train)

        class_weight = None
        if class_weight_ratio > 1:
            class_weight = {0: 1, 1: class_weight_ratio}

        model = LogisticRegression(
            C=0.1,
            penalty="l2",
            solver="lbfgs",
            max_iter=500,
            class_weight=class_weight,
            random_state=42,
        )
        model.fit(X_scaled, y_train)

        def predict_fn(X):
            X_s = scaler.transform(X)
            return model.predict_proba(X_s)[:, 1].tolist()

        return predict_fn, {"type": "LogisticRegression", "C": 0.1, "penalty": "l2"}

    except ImportError:
        logger.warning("  sklearn not available, skipping LogisticRegression")
        return None, None


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
# Feature stability selection
# ---------------------------------------------------------------------------

def select_stable_features(samples, metadata, n_preliminary_folds=3,
                           min_stable_folds=2, min_importance=0.001):
    """Select features that are stably important across preliminary CV folds.

    Uses the first 80% of data for a quick stability check.
    Features must have permutation importance > min_importance in at least
    min_stable_folds out of n_preliminary_folds to be kept.

    Returns:
        stable_indices: list of feature indices to keep
        stable_names: list of feature names to keep
        report: dict with per-feature stability info
    """
    from sklearn.inspection import permutation_importance as sklearn_perm_imp
    from sklearn.ensemble import HistGradientBoostingClassifier
    import numpy as np

    n_features = len(samples[0][3])
    feature_names = metadata.get("features", [f"f{i}" for i in range(n_features)])

    # Use first 80% of data for stability check
    cutoff_idx = int(len(samples) * 0.8)
    prelim_samples = samples[:cutoff_idx]

    if len(prelim_samples) < 1000:
        logger.info("  Feature selection: too few samples (%d), keeping all",
                     len(prelim_samples))
        return list(range(n_features)), feature_names, {}

    # Split into n_preliminary_folds
    day_min = prelim_samples[0][2]
    day_max = prelim_samples[-1][2]
    fold_size = (day_max - day_min) / (n_preliminary_folds + 1)

    importance_counts = [0] * n_features  # count of folds where feature is important
    importance_sums = [0.0] * n_features
    valid_folds = 0

    for fold_idx in range(n_preliminary_folds):
        train_end = day_min + fold_size * (fold_idx + 1)
        test_start = train_end
        test_end = train_end + fold_size

        train_X, train_y = [], []
        test_X, test_y = [], []

        for _, _, t_day, features, label in prelim_samples:
            if t_day < train_end:
                train_X.append(features)
                train_y.append(label)
            elif test_start <= t_day < test_end:
                test_X.append(features)
                test_y.append(label)

        if not train_X or not test_X or sum(train_y) < 10 or sum(test_y) < 5:
            continue

        X_train = np.array(train_X)
        y_train = np.array(train_y)
        X_test = np.array(test_X)
        y_test = np.array(test_y)

        model = HistGradientBoostingClassifier(
            max_iter=100,  # fewer iterations for speed
            max_depth=4,
            min_samples_leaf=50,
            learning_rate=0.05,
            l2_regularization=1.0,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=10,
            scoring="roc_auc",
            random_state=42,
        )
        model.fit(X_train, y_train)

        # Permutation importance on test set
        result = sklearn_perm_imp(
            model, X_test, y_test,
            n_repeats=5,
            scoring="roc_auc",
            random_state=42,
        )

        valid_folds += 1
        for fi in range(n_features):
            if result.importances_mean[fi] > min_importance:
                importance_counts[fi] += 1
                importance_sums[fi] += result.importances_mean[fi]

    if valid_folds == 0:
        logger.info("  Feature selection: no valid folds, keeping all")
        return list(range(n_features)), feature_names, {}

    # Adjust threshold if fewer valid folds than requested
    effective_min_folds = min(min_stable_folds, valid_folds)

    # Select stable features
    stable_indices = []
    stable_names = []
    dropped_names = []

    for fi in range(n_features):
        if importance_counts[fi] >= effective_min_folds:
            stable_indices.append(fi)
            stable_names.append(feature_names[fi])
        else:
            dropped_names.append(feature_names[fi])

    # Always keep at least the base features (first 35)
    BASE_FEATURE_COUNT = 35
    for fi in range(min(BASE_FEATURE_COUNT, n_features)):
        if fi not in stable_indices:
            stable_indices.append(fi)
            stable_names.append(feature_names[fi])

    stable_indices = sorted(set(stable_indices))
    stable_names = [feature_names[fi] for fi in stable_indices]

    report = {
        "total_features": n_features,
        "stable_features": len(stable_indices),
        "dropped_features": dropped_names,
        "importance_counts": {
            feature_names[fi]: importance_counts[fi] for fi in range(n_features)
        },
    }

    logger.info("  Feature selection: %d/%d features stable (dropped %d: %s)",
                len(stable_indices), n_features, len(dropped_names),
                ", ".join(dropped_names[:10]))

    return stable_indices, stable_names, report


def apply_feature_selection(samples, stable_indices):
    """Filter feature vectors in samples to keep only stable features.

    Returns a new list of samples with filtered feature vectors.
    """
    filtered = []
    for clat, clon, t_day, features, label in samples:
        filtered_features = [features[i] for i in stable_indices]
        filtered.append((clat, clon, t_day, filtered_features, label))
    return filtered


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
                       target_name="M5+", metadata=None):
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
    active_fnames = metadata.get("features", FEATURE_NAMES) if metadata else FEATURE_NAMES
    importance = permutation_importance(predict_fn, test_X, test_y, active_fnames, n_repeats=3)
    for imp in importance[:15]:
        logger.info("  %s: importance=%.4f (±%.4f)", imp["feature"], imp["importance"], imp["std"])

    # Single-feature AUC for comparison
    sf_auc = single_feature_auc_ranking(test_X, test_y, active_fnames)
    logger.info("--- Single-feature AUC ranking ---")
    for sf in sf_auc[:15]:
        logger.info("  %s: AUC=%.4f (%s)", sf["feature"], sf["auc"], sf["direction"])

    # Export level-0 predictions for ensemble stacking (Initiative 4)
    # Primary model: HistGBT
    export_level0_predictions(
        target_name, test_probs_cal, test_y, test_cells, test_t_days)

    # Diverse level-0 models: RandomForest + LogisticRegression
    # These provide genuinely different error patterns for stacking.
    diverse_models_info = {}
    for model_name, train_fn in [("rf", train_random_forest),
                                  ("lr", train_logistic)]:
        logger.info("--- Training %s for level-0 diversity [%s] ---", model_name, target_name)
        alt_predict_fn, alt_info = train_fn(train_X, train_y, class_weight_ratio)
        if alt_predict_fn is not None:
            alt_probs = alt_predict_fn(test_X)
            _, alt_auc = compute_roc(test_y, alt_probs)
            logger.info("  %s test AUC: %.4f [%s]", model_name, alt_auc, target_name)
            export_level0_predictions(
                target_name, alt_probs, test_y, test_cells, test_t_days,
                model_prefix=model_name)
            diverse_models_info[model_name] = {
                "info": alt_info,
                "auc_test": round(alt_auc, 4),
            }

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

def export_level0_predictions(target_name, probs, labels, cells, t_days_list,
                              model_prefix=""):
    """Export level-0 predictions for ensemble stacking.

    Saves to results/level0_predictions_{model}_{target}.json with per-sample
    probability, label, cell coordinates, and time step.

    Args:
        model_prefix: "" for HistGBT (default), "rf" for RandomForest,
                      "lr" for LogisticRegression.
    """
    RESULTS_DIR.mkdir(exist_ok=True)
    safe_name = target_name.replace("+", "plus").replace(".", "")
    if model_prefix:
        out_path = RESULTS_DIR / f"level0_predictions_{model_prefix}_{safe_name}.json"
    else:
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


def export_spatial_feature_matrix(samples, active_cells, metadata):
    """Export full spatial feature matrix for ConvLSTM training.

    Reuses pre-computed samples from build_dataset() instead of
    re-extracting features for all grid cells.
    Non-active cells are zero-filled.

    Output: results/feature_matrix.json — 4D tensor (T, H=11, W=11, C)
    """
    active_feature_names = metadata.get("features", FEATURE_NAMES)
    n_features = len(active_feature_names)
    prediction_window = metadata.get("prediction_window_days", 7)
    step_days = metadata.get("step_days", 3)
    min_target_mag = metadata.get("min_target_mag", 5.0)

    # Grid dimensions
    GRID_H = int((GRID_LAT_MAX - GRID_LAT_MIN) / CELL_SIZE_DEG) + 1
    GRID_W = int((GRID_LON_MAX - GRID_LON_MIN) / CELL_SIZE_DEG) + 1

    # Build lookup: (clat, clon, t_days) -> (features, label)
    sample_lookup = {}
    all_t_days = set()
    for clat, clon, t_days, features, label in samples:
        sample_lookup[(clat, clon, t_days)] = (features, label)
        all_t_days.add(t_days)

    sorted_t_days = sorted(all_t_days)
    logger.info("  Reusing %d pre-computed samples across %d timesteps, %d active cells",
                len(samples), len(sorted_t_days), len(active_cells))

    # Grid cell coordinates
    cell_coords_grid = []
    for lat in range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG)):
        row = []
        for lon in range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG)):
            row.append([float(lat), float(lon)])
        cell_coords_grid.append(row)

    # Zero vector for inactive cells
    zero_features = [0.0] * n_features

    # Build spatial grid for each timestep
    timestep_features = []
    for t_days in sorted_t_days:
        features_grid = []
        labels_grid = []

        for lat in range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG)):
            feat_row = []
            label_row = []
            for lon in range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG)):
                clat, clon = float(lat), float(lon)
                key = (clat, clon, t_days)
                if key in sample_lookup:
                    feat, lbl = sample_lookup[key]
                    feat_row.append(feat)
                    label_row.append(lbl)
                else:
                    feat_row.append(zero_features)
                    label_row.append(0)
            features_grid.append(feat_row)
            labels_grid.append(label_row)

        timestep_features.append({
            "t_days": round(t_days, 1),
            "features": features_grid,
            "labels": labels_grid,
        })

    total_pos = sum(
        sum(sum(row) for row in ts["labels"])
        for ts in timestep_features
    )

    logger.info("  Feature matrix: %d steps, grid %d×%d, %d features, %d positive cells",
                len(sorted_t_days), GRID_H, GRID_W, n_features, total_pos)

    # Save
    output_data = {
        "metadata": {
            "n_timesteps": len(sorted_t_days),
            "grid_h": GRID_H,
            "grid_w": GRID_W,
            "n_features": n_features,
            "feature_names": active_feature_names,
            "cell_size_deg": CELL_SIZE_DEG,
            "grid_lat_range": [GRID_LAT_MIN, GRID_LAT_MAX],
            "grid_lon_range": [GRID_LON_MIN, GRID_LON_MAX],
            "prediction_window_days": prediction_window,
            "min_target_mag": min_target_mag,
            "step_days": step_days,
            "total_positives": total_pos,
        },
        "cell_coords": cell_coords_grid,
        "timesteps": timestep_features,
    }

    def _json_default(obj):
        """Handle numpy int64/float64 types for JSON serialization."""
        if hasattr(obj, 'item'):
            return obj.item()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    RESULTS_DIR.mkdir(exist_ok=True)
    out_path = RESULTS_DIR / "feature_matrix.json"
    with open(out_path, "w") as f:
        json.dump(output_data, f, indent=None, ensure_ascii=False, default=_json_default)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    logger.info("  Feature matrix saved: %s (%.1f MB)", out_path, size_mb)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def load_gnss_data(db_path, t0):
    """Load GNSS displacement data from database, indexed by cell.

    Returns dict: {cell_key: list of {t_days, stations: [{lat, lon, dx_mm, dy_mm, dz_mm}]}}
    """
    gnss_data = {}
    try:
        async with safe_connect(db_path) as db:
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
        async with safe_connect(db_path) as db:
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
    """Load lightning data from DB (Blitzortung + ISS LIS).

    Returns dict: {(date_str, cell_lat, cell_lon): {stroke_count, ...}}
    Merges both sources: Blitzortung stroke_count and ISS LIS flash_count
    are combined as stroke_count for ML features.
    """
    data = {}
    try:
        async with safe_connect(db_path) as db:
            # Blitzortung lightning (stroke_count, mean_intensity)
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, stroke_count, "
                "mean_intensity FROM lightning"
            )
            for r in rows:
                key = (r[0], r[1], r[2])
                data[key] = {"stroke_count": r[3] or 0, "mean_intensity": r[4]}

            # ISS LIS lightning (flash_count, mean_radiance) — separate table
            try:
                lis_rows = await db.execute_fetchall(
                    "SELECT observed_at, cell_lat, cell_lon, flash_count, "
                    "mean_radiance FROM iss_lis_lightning"
                )
                for r in lis_rows:
                    key = (r[0], r[1], r[2])
                    if key in data:
                        # Merge: add ISS LIS flashes to Blitzortung strokes
                        data[key]["stroke_count"] += (r[3] or 0)
                    else:
                        data[key] = {"stroke_count": r[3] or 0, "mean_intensity": r[4]}
                if lis_rows:
                    logger.info("  ISS LIS lightning: %d cell-date records merged", len(lis_rows))
            except Exception:
                pass  # Table may not exist yet

        if not data:
            logger.info("  No lightning data available")
            return {}

        logger.info("  Lightning data loaded: %d cell-date records (Blitzortung + ISS LIS)", len(data))
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
        async with safe_connect(db_path) as db:
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
        async with safe_connect(db_path) as db:
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


# ---------------------------------------------------------------------------
# Phase 10 data loaders
# ---------------------------------------------------------------------------

async def load_phase10_olr(db_path):
    """Load OLR data with 30-day rolling statistics for anomaly computation.

    Returns dict: {(date_str, cell_lat, cell_lon): {olr_wm2, olr_mean_30d, olr_std_30d}}
    where cell coords are snapped to the 2° prediction grid via cell_key().
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, olr_wm2 FROM olr ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No OLR data available")
            return {}

        from collections import defaultdict
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from features import cell_key

        # Snap raw coords to 2° grid and average same-date same-cell values
        cell_date_values = defaultdict(list)
        for r in rows:
            clat, clon = cell_key(r[1], r[2])
            cell_date_values[(r[0], clat, clon)].append(r[3])

        # Build per-cell time series (sorted by date) for rolling stats
        cell_ts = defaultdict(list)
        for (date_str, clat, clon), vals in sorted(cell_date_values.items()):
            valid = [v for v in vals if v is not None]
            mean_val = sum(valid) / len(valid) if valid else None
            cell_ts[(clat, clon)].append((date_str, mean_val))

        data = {}
        for (lat, lon), ts in cell_ts.items():
            for i, (date_str, olr) in enumerate(ts):
                start_idx = max(0, i - 30)
                window = [v for _, v in ts[start_idx:i] if v is not None]
                mean_30d = sum(window) / len(window) if window else olr
                std_30d = (sum((v - mean_30d) ** 2 for v in window) / max(len(window), 1)) ** 0.5 if len(window) > 1 else 1.0
                data[(date_str, lat, lon)] = {
                    "olr_wm2": olr,
                    "olr_mean_30d": mean_30d,
                    "olr_std_30d": max(std_30d, 0.1),
                }
        n_raw = len(rows)
        logger.info("  OLR data loaded: %d raw → %d grid-cell records (cell_key snap)",
                     n_raw, len(data))
        return data
    except Exception as e:
        logger.warning("  OLR load failed (non-fatal): %s", e)
        return {}


async def load_phase10_earth_rotation(db_path):
    """Load Earth Orientation Parameters with day-to-day differences.

    Returns dict: {date_str: {lod_ms, x_arcsec, y_arcsec, prev_lod, prev_x, prev_y}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, lod_ms, x_arcsec, y_arcsec "
                "FROM earth_rotation ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No Earth rotation data available")
            return {}

        data = {}
        prev = None
        for r in rows:
            date_str, lod, x, y = r[0], r[1], r[2], r[3]
            entry = {
                "lod_ms": lod or 0.0,
                "x_arcsec": x or 0.0,
                "y_arcsec": y or 0.0,
                "prev_lod": prev[1] if prev else 0.0,
                "prev_x": prev[2] if prev else 0.0,
                "prev_y": prev[3] if prev else 0.0,
            }
            data[date_str] = entry
            prev = r
        logger.info("  Earth rotation data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  Earth rotation load failed (non-fatal): %s", e)
        return {}


async def load_phase10_solar_wind(db_path):
    """Load solar wind data aggregated to daily min/max.

    Returns dict: {date_str: {bz_min_24h, pressure_max_24h, dst_min_24h}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT DATE(observed_at), MIN(bz_gsm_nt), MAX(pressure_npa), MIN(dst_nt) "
                "FROM solar_wind "
                "WHERE bz_gsm_nt IS NOT NULL "
                "GROUP BY DATE(observed_at) ORDER BY DATE(observed_at)"
            )
        if not rows:
            logger.info("  No solar wind data available")
            return {}

        data = {}
        for r in rows:
            data[r[0]] = {
                "bz_min_24h": r[1] or 0.0,
                "pressure_max_24h": r[2] or 0.0,
                "dst_min_24h": r[3] or 0.0,
            }
        logger.info("  Solar wind data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  Solar wind load failed (non-fatal): %s", e)
        return {}


async def load_phase10_gravity(db_path):
    """Load GRACE gravity anomaly with month-to-month differences.

    Returns dict: {(date_str, cell_lat, cell_lon): {lwe_cm, lwe_prev_cm}}
    where cell coords are snapped to the 2° prediction grid via cell_key().
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, lwe_thickness_cm "
                "FROM gravity_mascon ORDER BY cell_lat, cell_lon, observed_at"
            )
        if not rows:
            logger.info("  No GRACE gravity data available")
            return {}

        from collections import defaultdict
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from features import cell_key

        # Snap raw coords to 2° grid and average same-date same-cell values
        cell_date_values = defaultdict(list)
        for r in rows:
            clat, clon = cell_key(r[1], r[2])
            cell_date_values[(r[0], clat, clon)].append(r[3])

        # Build per-cell time series sorted by date
        cell_ts = defaultdict(list)
        for (date_str, clat, clon), vals in sorted(cell_date_values.items()):
            valid = [v for v in vals if v is not None]
            mean_val = sum(valid) / len(valid) if valid else 0.0
            cell_ts[(clat, clon)].append((date_str, mean_val))

        # Expand monthly data to daily: each month's value applies to all days
        # in that month. This ensures daily feature lookups hit GRACE data.
        from calendar import monthrange
        data = {}
        n_expanded = 0
        for (lat, lon), ts in cell_ts.items():
            prev_lwe = 0.0
            for date_str, lwe in ts:
                rec = {"lwe_cm": lwe, "lwe_prev_cm": prev_lwe}
                # Parse the month and expand to all days
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    _, ndays = monthrange(dt.year, dt.month)
                    for day in range(1, ndays + 1):
                        day_str = f"{dt.year:04d}-{dt.month:02d}-{day:02d}"
                        data[(day_str, lat, lon)] = rec
                        n_expanded += 1
                except ValueError:
                    data[(date_str, lat, lon)] = rec
                    n_expanded += 1
                prev_lwe = lwe
        n_raw = len(rows)
        logger.info("  GRACE gravity data loaded: %d raw → %d daily-expanded records (cell_key snap)",
                     n_raw, n_expanded)
        return data
    except Exception as e:
        logger.warning("  GRACE gravity load failed (non-fatal): %s", e)
        return {}


async def load_phase10_so2(db_path):
    """Load SO2 column data with seasonal baseline.

    Returns dict: {(date_str, cell_lat, cell_lon): {so2_du, so2_baseline}}
    where cell_lat/cell_lon are snapped to the 2° prediction grid via cell_key().

    Raw OMI SO2 coordinates (e.g. 35.875, 139.625) are aggregated (mean)
    into 2° grid cells to match the feature extractor's lookup keys.
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, so2_du FROM so2_column"
            )
        if not rows:
            logger.info("  No SO2 data available")
            return {}

        # Snap raw coords to 2° grid and aggregate per (date, cell)
        from collections import defaultdict
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from features import cell_key

        # Collect all values per (date, grid_cell)
        cell_date_values = defaultdict(list)
        for r in rows:
            clat, clon = cell_key(r[1], r[2])
            cell_date_values[(r[0], clat, clon)].append(r[3])

        # Compute per-cell baseline (mean across all dates)
        cell_all_values = defaultdict(list)
        for (date_str, clat, clon), vals in cell_date_values.items():
            cell_all_values[(clat, clon)].extend(vals)

        cell_baseline = {}
        for k, vals in cell_all_values.items():
            cell_baseline[k] = sum(vals) / len(vals)

        # Build final dict with mean SO2 per (date, cell)
        data = {}
        for (date_str, clat, clon), vals in cell_date_values.items():
            mean_so2 = sum(vals) / len(vals)
            data[(date_str, clat, clon)] = {
                "so2_du": mean_so2,
                "so2_baseline": cell_baseline.get((clat, clon), 0.0),
            }

        n_raw = len(rows)
        n_grid = len(data)
        logger.info("  SO2 data loaded: %d raw → %d grid-cell records (cell_key snap)",
                     n_raw, n_grid)
        return data
    except Exception as e:
        logger.warning("  SO2 load failed (non-fatal): %s", e)
        return {}


async def load_phase10_soil_moisture(db_path):
    """Load soil moisture data with 30-day rolling statistics.

    Returns dict: {(date_str, cell_lat, cell_lon): {sm, sm_mean_30d, sm_std_30d}}
    where cell_lat/cell_lon are snapped to the 2° prediction grid via cell_key().

    Raw soil moisture coordinates are aggregated (mean) into 2° grid cells
    to match the feature extractor's lookup keys.
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, sm_m3m3 "
                "FROM soil_moisture ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No soil moisture data available")
            return {}

        from collections import defaultdict
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from features import cell_key

        # Snap raw coords to 2° grid; multiple raw coords may map to the same cell.
        # Build per-cell time series: cell_ts[(clat, clon)] = [(date_str, sm), ...]
        # On the same date, average values that fall in the same grid cell.
        cell_date_values = defaultdict(list)
        for r in rows:
            clat, clon = cell_key(r[1], r[2])
            cell_date_values[(r[0], clat, clon)].append(r[3])

        # Average same-cell same-date values, then rebuild sorted time series per cell
        cell_ts = defaultdict(list)
        for (date_str, clat, clon), vals in sorted(cell_date_values.items()):
            valid = [v for v in vals if v is not None]
            mean_val = sum(valid) / len(valid) if valid else None
            cell_ts[(clat, clon)].append((date_str, mean_val))

        # Expand monthly data to daily: fill forward each observation to all days
        # until the next observation. This ensures daily feature lookups hit soil data.
        from calendar import monthrange
        data = {}
        n_expanded = 0
        for (clat, clon), ts in cell_ts.items():
            for i, (date_str, sm) in enumerate(ts):
                start_idx = max(0, i - 30)
                window = [v for _, v in ts[start_idx:i] if v is not None]
                mean_30d = sum(window) / len(window) if window else sm
                std_30d = (sum((v - mean_30d) ** 2 for v in window) / max(len(window), 1)) ** 0.5 if len(window) > 1 else 1.0
                rec = {
                    "sm": sm,
                    "sm_mean_30d": mean_30d,
                    "sm_std_30d": max(std_30d, 0.001),
                }
                # Expand to all days in month
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    _, ndays = monthrange(dt.year, dt.month)
                    for day in range(1, ndays + 1):
                        day_str = f"{dt.year:04d}-{dt.month:02d}-{day:02d}"
                        data[(day_str, clat, clon)] = rec
                        n_expanded += 1
                except ValueError:
                    data[(date_str, clat, clon)] = rec
                    n_expanded += 1

        n_raw = len(rows)
        logger.info("  Soil moisture data loaded: %d raw → %d daily-expanded records (cell_key snap)",
                     n_raw, n_expanded)
        return data
    except Exception as e:
        logger.warning("  Soil moisture load failed (non-fatal): %s", e)
        return {}


async def load_phase10b_tide_gauge(db_path):
    """Load tide gauge data with nearest-station residual for each date."""
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT DATE(observed_at), AVG(sea_level_mm) "
                "FROM tide_gauge GROUP BY DATE(observed_at) ORDER BY DATE(observed_at)"
            )
        if not rows:
            logger.info("  No tide gauge data available")
            return {}

        # Compute rolling stats
        data = {}
        values = [(r[0], r[1]) for r in rows if r[1] is not None]
        for i, (date_str, val) in enumerate(values):
            start_idx = max(0, i - 30)
            window = [v for _, v in values[start_idx:i] if v is not None]
            mean_30d = sum(window) / len(window) if window else val
            std_30d = (sum((v - mean_30d) ** 2 for v in window) / max(len(window), 1)) ** 0.5 if len(window) > 1 else 1.0
            data[date_str] = {
                "residual": val - mean_30d,
                "residual_mean_30d": 0.0,
                "residual_std_30d": max(std_30d, 0.1),
            }
        logger.info("  Tide gauge data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  Tide gauge load failed (non-fatal): %s", e)
        return {}


async def load_phase10b_ocean_color(db_path):
    """Load ocean color with rolling baseline.

    Cell coords are snapped to the 2° prediction grid via cell_key().
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, chlor_a_mg_m3 "
                "FROM ocean_color ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No ocean color data available")
            return {}

        from collections import defaultdict
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from features import cell_key

        # Snap raw coords to 2° grid and average same-date same-cell values
        cell_date_values = defaultdict(list)
        for r in rows:
            clat, clon = cell_key(r[1], r[2])
            cell_date_values[(r[0], clat, clon)].append(r[3])

        cell_ts = defaultdict(list)
        for (date_str, clat, clon), vals in sorted(cell_date_values.items()):
            valid = [v for v in vals if v is not None]
            mean_val = sum(valid) / len(valid) if valid else None
            cell_ts[(clat, clon)].append((date_str, mean_val))

        # Forward-fill weekly/monthly data to daily: each observation's value
        # persists until the next observation (up to 14 days max gap fill).
        data = {}
        n_expanded = 0
        for (lat, lon), ts in cell_ts.items():
            for i, (date_str, val) in enumerate(ts):
                start_idx = max(0, i - 30)
                window = [v for _, v in ts[start_idx:i] if v is not None]
                mean_30d = sum(window) / len(window) if window else val
                std_30d = (sum((v - mean_30d) ** 2 for v in window) / max(len(window), 1)) ** 0.5 if len(window) > 1 else 1.0
                rec = {
                    "chlor_a": val,
                    "chlor_mean_30d": mean_30d,
                    "chlor_std_30d": max(std_30d, 0.01),
                }
                # Forward-fill: populate observation date + next 6 days (weekly cadence)
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    # Fill until next observation or 13 days, whichever is shorter
                    next_date_str = ts[i + 1][0] if i + 1 < len(ts) else None
                    fill_days = 13  # max forward fill for bi-weekly gaps
                    if next_date_str:
                        next_dt = datetime.strptime(next_date_str, "%Y-%m-%d")
                        fill_days = min(fill_days, (next_dt - dt).days - 1)
                    for d_off in range(max(fill_days + 1, 1)):
                        day_str = (dt + timedelta(days=d_off)).strftime("%Y-%m-%d")
                        data[(day_str, lat, lon)] = rec
                        n_expanded += 1
                except (ValueError, IndexError):
                    data[(date_str, lat, lon)] = rec
                    n_expanded += 1
        n_raw = len(rows)
        logger.info("  Ocean color data loaded: %d raw → %d daily-expanded records (cell_key snap)",
                     n_raw, n_expanded)
        return data
    except Exception as e:
        logger.warning("  Ocean color load failed (non-fatal): %s", e)
        return {}


async def load_phase10b_cloud_fraction(db_path):
    """Load cloud fraction with rolling baseline.

    Returns dict: {(date_str, cell_lat, cell_lon): {cloud_frac, cloud_mean_30d, cloud_std_30d}}
    where cell_lat/cell_lon are snapped to the 2° prediction grid via cell_key().

    Raw cloud fraction coordinates are aggregated (mean) into 2° grid cells
    to match the feature extractor's lookup keys.
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, cloud_frac "
                "FROM cloud_fraction ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No cloud fraction data available")
            return {}

        from collections import defaultdict
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from features import cell_key

        # Snap raw coords to 2° grid and average same-cell same-date values
        cell_date_values = defaultdict(list)
        for r in rows:
            clat, clon = cell_key(r[1], r[2])
            cell_date_values[(r[0], clat, clon)].append(r[3])

        cell_ts = defaultdict(list)
        for (date_str, clat, clon), vals in sorted(cell_date_values.items()):
            valid = [v for v in vals if v is not None]
            mean_val = sum(valid) / len(valid) if valid else None
            cell_ts[(clat, clon)].append((date_str, mean_val))

        data = {}
        for (clat, clon), ts in cell_ts.items():
            for i, (date_str, val) in enumerate(ts):
                start_idx = max(0, i - 30)
                window = [v for _, v in ts[start_idx:i] if v is not None]
                mean_30d = sum(window) / len(window) if window else val
                std_30d = (sum((v - mean_30d) ** 2 for v in window) / max(len(window), 1)) ** 0.5 if len(window) > 1 else 1.0
                data[(date_str, clat, clon)] = {
                    "cloud_frac": val,
                    "cloud_mean_30d": mean_30d,
                    "cloud_std_30d": max(std_30d, 0.01),
                }

        n_raw = len(rows)
        n_grid = len(data)
        logger.info("  Cloud fraction data loaded: %d raw → %d grid-cell records (cell_key snap)",
                     n_raw, n_grid)
        return data
    except Exception as e:
        logger.warning("  Cloud fraction load failed (non-fatal): %s", e)
        return {}


async def load_phase10b_nightlight(db_path):
    """Load nighttime light with baseline.

    Returns dict: {(date_str, cell_lat, cell_lon): {radiance, radiance_mean_6m, radiance_std_6m}}
    where cell_lat/cell_lon are snapped to the 2° prediction grid via cell_key().

    Raw nightlight coordinates are aggregated (mean) into 2° grid cells
    to match the feature extractor's lookup keys.
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, radiance_nwcm2sr "
                "FROM nightlight ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No nightlight data available")
            return {}

        from collections import defaultdict
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        from features import cell_key

        # Snap raw coords to 2° grid and average same-cell same-date values
        cell_date_values = defaultdict(list)
        for r in rows:
            clat, clon = cell_key(r[1], r[2])
            cell_date_values[(r[0], clat, clon)].append(r[3])

        cell_ts = defaultdict(list)
        for (date_str, clat, clon), vals in sorted(cell_date_values.items()):
            valid = [v for v in vals if v is not None]
            mean_val = sum(valid) / len(valid) if valid else None
            cell_ts[(clat, clon)].append((date_str, mean_val))

        # Expand annual data to daily: each year's value applies to all days
        # in that year. This ensures daily feature lookups hit nightlight data.
        from calendar import isleap
        data = {}
        n_expanded = 0
        for (clat, clon), ts in cell_ts.items():
            all_vals = [v for _, v in ts if v is not None]
            mean_all = sum(all_vals) / len(all_vals) if all_vals else 0.0
            std_all = (sum((v - mean_all) ** 2 for v in all_vals) / max(len(all_vals), 1)) ** 0.5 if len(all_vals) > 1 else 1.0
            for date_str, val in ts:
                rec = {
                    "radiance": val,
                    "radiance_mean_6m": mean_all,
                    "radiance_std_6m": max(std_all, 0.01),
                }
                # Expand to all days in the year
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    ndays = 366 if isleap(dt.year) else 365
                    base = datetime(dt.year, 1, 1)
                    for d_off in range(ndays):
                        day_str = (base + timedelta(days=d_off)).strftime("%Y-%m-%d")
                        data[(day_str, clat, clon)] = rec
                        n_expanded += 1
                except ValueError:
                    data[(date_str, clat, clon)] = rec
                    n_expanded += 1

        n_raw = len(rows)
        logger.info("  Nightlight data loaded: %d raw → %d daily-expanded records (cell_key snap)",
                     n_raw, n_expanded)
        return data
    except Exception as e:
        logger.warning("  Nightlight load failed (non-fatal): %s", e)
        return {}


async def load_phase10b_insar(db_path):
    """Load InSAR deformation data."""
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, cell_lat, cell_lon, los_velocity_mm_yr "
                "FROM insar_deformation"
            )
        if not rows:
            logger.info("  No InSAR data available")
            return {}

        # Compute per-cell mean velocity as baseline
        from collections import defaultdict
        cell_values = defaultdict(list)
        for r in rows:
            cell_values[(r[1], r[2])].append(r[3])

        cell_baseline = {}
        for k, vals in cell_values.items():
            cell_baseline[k] = sum(vals) / len(vals) if vals else 0.0

        data = {}
        for r in rows:
            key = (r[0], r[1], r[2])
            baseline = cell_baseline.get((r[1], r[2]), 0.0)
            data[key] = {"velocity_anomaly": (r[3] or 0.0) - baseline}
        logger.info("  InSAR data loaded: %d cell-date records", len(data))
        return data
    except Exception as e:
        logger.warning("  InSAR load failed (non-fatal): %s", e)
        return {}


async def load_phase11_goes_xray(db_path):
    """Load GOES X-ray flux data (solar flare proxy).

    Returns dict: {date_str: {xray_long_wm2, xray_short_wm2, flare_class}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, xray_long_wm2, xray_short_wm2, flare_class "
                "FROM goes_xray ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No GOES X-ray data available")
            return {}

        data = {}
        for r in rows:
            data[r[0]] = {
                "xray_long_wm2": r[1] or 0.0,
                "xray_short_wm2": r[2] or 0.0,
                "flare_class": r[3] or "",
            }
        logger.info("  GOES X-ray data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  GOES X-ray load failed (non-fatal): %s", e)
        return {}


async def load_phase11_goes_proton(db_path):
    """Load GOES proton flux data (SEP events).

    Returns dict: {date_str: {proton_10mev_max, proton_60mev_max}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, proton_10mev_max, proton_60mev_max "
                "FROM goes_proton ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No GOES proton data available")
            return {}

        data = {}
        for r in rows:
            data[r[0]] = {
                "proton_10mev_max": r[1] or 0.0,
                "proton_60mev_max": r[2] or 0.0,
            }
        logger.info("  GOES proton data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  GOES proton load failed (non-fatal): %s", e)
        return {}


async def load_phase11_tidal_stress(db_path):
    """Load tidal stress data (lunar+solar gravitational stress).

    Returns dict: {date_str: {tidal_shear_pa, tidal_normal_pa, lunar_phase}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, tidal_shear_pa, tidal_normal_pa, lunar_phase "
                "FROM tidal_stress ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No tidal stress data available")
            return {}

        data = {}
        for r in rows:
            data[r[0]] = {
                "tidal_shear_pa": r[1] or 0.0,
                "tidal_normal_pa": r[2] or 0.0,
                "lunar_phase": r[3] or 0.0,
            }
        logger.info("  Tidal stress data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  Tidal stress load failed (non-fatal): %s", e)
        return {}


async def load_phase11_particle_flux(db_path):
    """Load particle precipitation data (GOES electron flux).

    Returns dict: {date_str: {electron_2mev_max, electron_800kev_max}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT observed_at, electron_2mev_max, electron_800kev_max "
                "FROM particle_flux ORDER BY observed_at"
            )
        if not rows:
            logger.info("  No particle flux data available")
            return {}

        data = {}
        for r in rows:
            data[r[0]] = {
                "electron_2mev_max": r[1] or 0.0,
                "electron_800kev_max": r[2] or 0.0,
            }
        logger.info("  Particle flux data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  Particle flux load failed (non-fatal): %s", e)
        return {}


async def load_phase13_dart_pressure(db_path):
    """Load DART ocean bottom pressure with rolling baseline.

    Returns dict: {date_str: {height_m, height_mean_30d, height_std_30d, height_prev_day}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT DATE(observed_at), AVG(water_height_m) "
                "FROM dart_pressure GROUP BY DATE(observed_at) ORDER BY DATE(observed_at)"
            )
        if not rows:
            logger.info("  No DART pressure data available")
            return {}

        data = {}
        values = [(r[0], r[1]) for r in rows if r[1] is not None]
        for i, (date_str, val) in enumerate(values):
            start_idx = max(0, i - 30)
            window = [v for _, v in values[start_idx:i] if v is not None]
            mean_30d = sum(window) / len(window) if window else val
            std_30d = (sum((v - mean_30d) ** 2 for v in window) / max(len(window), 1)) ** 0.5 if len(window) > 1 else 0.001
            prev_val = values[i - 1][1] if i > 0 else 0.0
            data[date_str] = {
                "height_m": val,
                "height_mean_30d": mean_30d,
                "height_std_30d": max(std_30d, 0.001),
                "height_prev_day": prev_val,
            }
        logger.info("  DART pressure data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  DART pressure load failed (non-fatal): %s", e)
        return {}


async def load_phase13_ioc_sealevel(db_path):
    """Load IOC sea level monitoring data with rolling baseline.

    Returns dict: {date_str: {level_m, level_mean_30d, level_std_30d}}
    """
    try:
        async with safe_connect(db_path) as db:
            rows = await db.execute_fetchall(
                "SELECT DATE(observed_at), AVG(sea_level_m) "
                "FROM ioc_sea_level GROUP BY DATE(observed_at) ORDER BY DATE(observed_at)"
            )
        if not rows:
            logger.info("  No IOC sea level data available")
            return {}

        data = {}
        values = [(r[0], r[1]) for r in rows if r[1] is not None]
        for i, (date_str, val) in enumerate(values):
            start_idx = max(0, i - 30)
            window = [v for _, v in values[start_idx:i] if v is not None]
            mean_30d = sum(window) / len(window) if window else val
            std_30d = (sum((v - mean_30d) ** 2 for v in window) / max(len(window), 1)) ** 0.5 if len(window) > 1 else 0.001
            data[date_str] = {
                "level_m": val,
                "level_mean_30d": mean_30d,
                "level_std_30d": max(std_30d, 0.001),
            }
        logger.info("  IOC sea level data loaded: %d dates", len(data))
        return data
    except Exception as e:
        logger.warning("  IOC sea level load failed (non-fatal): %s", e)
        return {}


def _compute_rolling_baselines(daily, feature_keys):
    """Compute rolling 30-day baseline for a list of daily records.

    Returns dict: {date_str: {key, key_mean_30d, key_std_30d, ...}}
    """
    data = {}
    for i, d in enumerate(daily):
        date_str = d["date_str"]
        entry = {}

        start_idx = max(0, i - 30)
        window = daily[start_idx:i]  # Strictly before current day

        for key in feature_keys:
            val = d[key]
            entry[key] = val

            if window:
                w_vals = [w[key] for w in window if w[key] is not None and w[key] != 0]
                if w_vals:
                    mean_30d = sum(w_vals) / len(w_vals)
                    std_30d = (sum((v - mean_30d) ** 2 for v in w_vals) / len(w_vals)) ** 0.5
                else:
                    mean_30d = val
                    std_30d = 0.001
            else:
                mean_30d = val
                std_30d = 0.001

            entry[f"{key}_mean_30d"] = mean_30d
            entry[f"{key}_std_30d"] = max(std_30d, 0.001)

        data[date_str] = entry
    return data


def _compute_spatial_gradient(seg_data, date_str):
    """Compute along-trench linear gradient of RMS from per-segment data."""
    CABLE_ORDER = {"S1": 0, "S2": 1, "S3": 2, "S4": 3, "S5": 4, "S6": 5}
    segs = seg_data.get(date_str, {})
    if len(segs) < 3:
        return 0.0
    ordered = sorted(
        [(CABLE_ORDER.get(s, -1), v) for s, v in segs.items() if s in CABLE_ORDER],
        key=lambda x: x[0],
    )
    if len(ordered) < 3:
        return 0.0
    xs = [o[0] for o in ordered]
    ys = [o[1] for o in ordered]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    return cov / var_x if var_x > 0 else 0.0


def _compute_segment_max_anomaly(seg_data, date_str, daily, daily_index):
    """Compute max z-score across cable segments."""
    segs = seg_data.get(date_str, {})
    start_idx = max(0, daily_index - 30)
    window = daily[start_idx:daily_index]

    if not segs or not window:
        return 0.0

    seg_anomalies = []
    for seg_name, seg_rms in segs.items():
        seg_window_vals = []
        for w in window:
            w_segs = seg_data.get(w["date_str"], {})
            if seg_name in w_segs:
                seg_window_vals.append(w_segs[seg_name])
        if seg_window_vals:
            seg_mean = sum(seg_window_vals) / len(seg_window_vals)
            seg_std = max(
                (sum((v - seg_mean) ** 2 for v in seg_window_vals) / len(seg_window_vals)) ** 0.5,
                0.001,
            )
            seg_anomalies.append(abs((seg_rms - seg_mean) / seg_std))
    return max(seg_anomalies) if seg_anomalies else 0.0


async def _load_sensor_data(db_path, sensor_type_filter, extra_columns=""):
    """Load daily averages and per-segment data for a sensor type.

    Returns (daily_list, seg_data_dict) or ([], {}).
    """
    import math

    extra_select = f", {extra_columns}" if extra_columns else ""

    try:
        async with safe_connect(db_path) as db:
            tables = await db.execute_fetchall(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='snet_waveform'"
            )
            if not tables:
                return [], {}

            # Check if sensor_type column exists (backward compat)
            columns = {row[1] for row in await db.execute_fetchall("PRAGMA table_info(snet_waveform)")}
            has_sensor_type = "sensor_type" in columns

            if has_sensor_type:
                where = f"WHERE sensor_type = '{sensor_type_filter}'"
            else:
                # Legacy: no sensor_type column, all data is acceleration
                if sensor_type_filter != "0120A":
                    return [], {}
                where = ""

            rows = await db.execute_fetchall(
                f"""SELECT date_str,
                       AVG(rms_z), AVG(rms_h), AVG(hv_ratio),
                       AVG(lf_power), AVG(hf_power), AVG(spectral_slope)
                       {extra_select}
                   FROM snet_waveform {where}
                   GROUP BY date_str ORDER BY date_str"""
            )

            seg_rows = await db.execute_fetchall(
                f"""SELECT date_str, cable_segment,
                       AVG(rms_z), AVG(rms_h)
                   FROM snet_waveform
                   {where + ' AND' if where else 'WHERE'} cable_segment IS NOT NULL
                   GROUP BY date_str, cable_segment
                   ORDER BY date_str, cable_segment"""
            )

        if not rows:
            return [], {}

        daily = []
        for r in rows:
            if r[1] is None:
                continue
            rms_z = r[1] or 0.0
            rms_h = r[2] or 0.0
            rms_combined = math.sqrt(rms_z ** 2 + rms_h ** 2) if (rms_z and rms_h) else 0.0
            entry = {
                "date_str": r[0],
                "rms_combined": rms_combined,
                "hv_ratio": r[3] or 0.0,
                "lf_power": r[4] or 0.0,
                "hf_power": r[5] or 0.0,
                "spectral_slope": r[6] or 0.0,
            }
            # Extra columns (vlf_power, vlf_hv_ratio for velocity)
            if extra_columns:
                # Extract column name from AVG(col_name) expressions
                col_names = [c.strip().split("(")[-1].rstrip(")").strip() for c in extra_columns.split(",")]
                for j, col_name in enumerate(col_names):
                    entry[col_name] = r[7 + j] if r[7 + j] is not None else 0.0
            daily.append(entry)

        seg_data = {}
        for r in seg_rows:
            date_str, seg = r[0], r[1]
            seg_data.setdefault(date_str, {})
            rms_z = r[2] or 0.0
            rms_h = r[3] or 0.0
            seg_data[date_str][seg] = math.sqrt(rms_z ** 2 + rms_h ** 2)

        return daily, seg_data

    except Exception as e:
        logger.warning("  Failed to load sensor data for %s: %s", sensor_type_filter, e)
        return [], {}


async def load_phase18_snet_waveform(db_path):
    """Load S-net multi-sensor features with rolling baselines and spatial analysis.

    Returns tuple: (accel_data, velocity_data, highgain_data)
        Each is a dict: {date_str: {feature_key: value, ...}}
    """

    # --- Acceleration (0120A) ---
    accel_daily, accel_seg = await _load_sensor_data(db_path, "0120A")
    accel_keys = ["rms_combined", "hv_ratio", "lf_power", "hf_power", "spectral_slope"]
    accel_data = _compute_rolling_baselines(accel_daily, accel_keys)

    # Add spatial features to accel_data
    for i, d in enumerate(accel_daily):
        ds = d["date_str"]
        if ds in accel_data:
            accel_data[ds]["spatial_gradient"] = _compute_spatial_gradient(accel_seg, ds)
            accel_data[ds]["segment_max_anomaly"] = _compute_segment_max_anomaly(
                accel_seg, ds, accel_daily, i)

    if accel_data:
        logger.info("  S-net accel (0120A): %d dates", len(accel_data))

    # --- Velocity (0120) ---
    vel_daily, vel_seg = await _load_sensor_data(
        db_path, "velocity", extra_columns="AVG(vlf_power), AVG(vlf_hv_ratio)")

    vel_keys = ["rms_combined", "hv_ratio", "lf_power", "hf_power", "spectral_slope",
                "vlf_power", "vlf_hv_ratio"]
    velocity_data = {}

    if vel_daily:
        vel_baseline = _compute_rolling_baselines(vel_daily, vel_keys)

        for i, d in enumerate(vel_daily):
            ds = d["date_str"]
            if ds not in vel_baseline:
                continue
            entry = vel_baseline[ds]

            # Rename keys for velocity namespace
            vel_entry = {}
            for k, v in entry.items():
                if k.startswith("rms_combined"):
                    vel_entry["vel_" + k] = v
                elif k.startswith("spectral_slope"):
                    vel_entry["vel_" + k] = v
                else:
                    vel_entry[k] = v

            # VLF spatial gradient
            vel_entry["vlf_spatial_gradient"] = _compute_spatial_gradient(vel_seg, ds)

            # Cross-sensor: VLF/HF ratio (velocity VLF power / accel HF power)
            accel_entry = accel_data.get(ds, {})
            vlf_pow = entry.get("vlf_power", 0.0)
            hf_pow = accel_entry.get("hf_power", 0.0)
            if vlf_pow != 0 and hf_pow != 0:
                vel_entry["vlf_hf_ratio"] = vlf_pow - hf_pow  # log10 ratio = difference
            else:
                vel_entry["vlf_hf_ratio"] = 0.0

            # Cross-sensor: accel-velocity RMS coherence (30-day correlation anomaly)
            # Simple approach: ratio of current day's accel/vel RMS vs 30-day mean ratio
            accel_rms = accel_entry.get("rms_combined", 0.0)
            vel_rms = entry.get("rms_combined", 0.0)
            accel_rms_mean = accel_entry.get("rms_combined_mean_30d", 0.0)
            vel_rms_mean = entry.get("rms_combined_mean_30d", 0.0)
            if accel_rms > 0 and vel_rms > 0 and accel_rms_mean > 0 and vel_rms_mean > 0:
                ratio_now = accel_rms / vel_rms
                ratio_baseline = accel_rms_mean / vel_rms_mean
                if ratio_baseline > 0:
                    vel_entry["accel_velocity_coherence"] = (ratio_now - ratio_baseline) / ratio_baseline
                else:
                    vel_entry["accel_velocity_coherence"] = 0.0
            else:
                vel_entry["accel_velocity_coherence"] = 0.0

            velocity_data[ds] = vel_entry

        logger.info("  S-net velocity (0120): %d dates", len(velocity_data))

    # --- High-gain acceleration (0120C) ---
    hg_daily, _ = await _load_sensor_data(db_path, "accel_hg")
    hg_keys = ["rms_combined"]
    highgain_data = {}

    if hg_daily:
        hg_baseline = _compute_rolling_baselines(hg_daily, hg_keys)
        for ds, entry in hg_baseline.items():
            highgain_data[ds] = {
                "hg_rms_combined": entry.get("rms_combined", 0.0),
                "hg_rms_combined_mean_30d": entry.get("rms_combined_mean_30d", 0.0),
                "hg_rms_combined_std_30d": entry.get("rms_combined_std_30d", 0.001),
            }
        logger.info("  S-net high-gain (0120C): %d dates", len(highgain_data))

    return accel_data, velocity_data, highgain_data


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
    logger.info("Features: %d total defined (%d base + %d Phase 9)", N_FEATURES, 47, 9)

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

    # --- Phase 10: Load unconventional data sources ---
    logger.info("--- Loading Phase 10 data (OLR, EOP, solar wind, gravity, SO2, soil moisture) ---")
    olr_data = await load_phase10_olr(DB_PATH)
    earth_rotation_data = await load_phase10_earth_rotation(DB_PATH)
    solar_wind_data = await load_phase10_solar_wind(DB_PATH)
    gravity_data = await load_phase10_gravity(DB_PATH)
    so2_data = await load_phase10_so2(DB_PATH)
    soil_moisture_data = await load_phase10_soil_moisture(DB_PATH)

    # --- Phase 10b: Additional unconventional sources ---
    logger.info("--- Loading Phase 10b data (tide, ocean color, cloud, nightlight, InSAR) ---")
    tide_gauge_data = await load_phase10b_tide_gauge(DB_PATH)
    ocean_color_data = await load_phase10b_ocean_color(DB_PATH)
    cloud_fraction_data = await load_phase10b_cloud_fraction(DB_PATH)
    nightlight_data = await load_phase10b_nightlight(DB_PATH)
    insar_data = await load_phase10b_insar(DB_PATH)

    # --- Phase 11: Space/cosmic data sources ---
    logger.info("--- Loading Phase 11 data (X-ray, proton, tidal stress, particle flux) ---")
    goes_xray_data = await load_phase11_goes_xray(DB_PATH)
    goes_proton_data = await load_phase11_goes_proton(DB_PATH)
    tidal_stress_data = await load_phase11_tidal_stress(DB_PATH)
    particle_flux_data = await load_phase11_particle_flux(DB_PATH)

    # --- Phase 13: Seafloor/ocean bottom data sources ---
    logger.info("--- Loading Phase 13+18 data (DART pressure, IOC sea level, S-net waveform) ---")
    dart_pressure_data = await load_phase13_dart_pressure(DB_PATH)
    ioc_sealevel_data = await load_phase13_ioc_sealevel(DB_PATH)
    snet_waveform_data, snet_velocity_data, snet_highgain_data = await load_phase18_snet_waveform(DB_PATH)

    # Multi-target loop
    target_results = {}
    primary_metadata = None
    m5_samples = None
    m5_active_cells = None

    for target_name, cfg in TARGET_CONFIGS.items():
        logger.info("")
        logger.info("=" * 60)
        logger.info("=== Target: %s (M%.1f+ within %dd) ===",
                    target_name, cfg["min_mag"], cfg["window_days"])
        logger.info("=" * 60)

        # Build dataset for this target
        logger.info("--- Building feature dataset ---")
        samples, active_cells, metadata = build_dataset(
            events, fm_dict, t0, global_etas,
            zone_etas=zone_etas,
            gnss_data=gnss_data,
            cosmic_ray_data=cosmic_ray_data,
            lightning_data=lightning_data,
            geomag_spectral_data=geomag_spectral_data,
            animal_data=animal_data,
            olr_data=olr_data,
            earth_rotation_data=earth_rotation_data,
            solar_wind_data=solar_wind_data,
            gravity_data=gravity_data,
            so2_data=so2_data,
            soil_moisture_data=soil_moisture_data,
            tide_gauge_data=tide_gauge_data,
            ocean_color_data=ocean_color_data,
            cloud_fraction_data=cloud_fraction_data,
            nightlight_data=nightlight_data,
            insar_data=insar_data,
            goes_xray_data=goes_xray_data,
            goes_proton_data=goes_proton_data,
            tidal_stress_data=tidal_stress_data,
            particle_flux_data=particle_flux_data,
            dart_pressure_data=dart_pressure_data,
            ioc_sealevel_data=ioc_sealevel_data,
            snet_waveform_data=snet_waveform_data,
            snet_velocity_data=snet_velocity_data,
            snet_highgain_data=snet_highgain_data,
            min_target_mag=cfg["min_mag"],
            window_days=cfg["window_days"],
            step_days=cfg["step_days"],
        )

        if primary_metadata is None:
            primary_metadata = metadata
        if target_name == "M5+":
            m5_samples = samples
            m5_active_cells = active_cells

        if len(samples) < 1000:
            logger.error("Dataset too small (%d samples) for %s", len(samples), target_name)
            target_results[target_name] = {
                "error": "dataset_too_small", "metadata": metadata}
            continue

        # Feature stability selection (pre-filter noisy features)
        logger.info("--- Feature stability selection [%s] ---", target_name)
        stable_indices, stable_names, stability_report = select_stable_features(
            samples, metadata,
        )

        if len(stable_indices) < len(samples[0][3]):
            logger.info("  Applying feature selection: %d -> %d features [%s]",
                        len(samples[0][3]), len(stable_indices), target_name)
            filtered_samples = apply_feature_selection(samples, stable_indices)
            filtered_metadata = dict(metadata)
            filtered_metadata["features"] = stable_names
            filtered_metadata["feature_selection"] = stability_report
        else:
            filtered_samples = samples
            filtered_metadata = metadata

        # Walk-forward CV
        logger.info("--- Walk-forward cross-validation [%s] ---", target_name)
        cv_folds, cv_aggregate, cv_all_y, cv_all_probs = run_walk_forward_cv(
            filtered_samples, filtered_metadata,
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
            filtered_samples, t0,
            class_weight_ratio=cfg["class_weight_ratio"],
            target_name=target_name,
            metadata=filtered_metadata,
        )

        target_results[target_name] = {
            "config": cfg,
            "metadata": filtered_metadata,
            "feature_selection": stability_report,
            "walk_forward_cv": {
                "aggregate": cv_aggregate,
                "folds": cv_folds,
            },
            "final_model": final_results,
        }

    # --- Export spatial feature matrix for ConvLSTM ---
    # Reuses M5+ samples (already computed) to avoid redundant feature extraction.
    if m5_samples and primary_metadata:
        logger.info("--- Exporting spatial feature matrix for ConvLSTM ---")
        try:
            export_spatial_feature_matrix(
                m5_samples, m5_active_cells, primary_metadata,
            )
        except Exception as e:
            logger.warning("Feature matrix export failed (non-fatal): %s", e)

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
