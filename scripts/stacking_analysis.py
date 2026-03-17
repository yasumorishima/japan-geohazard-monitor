"""Ensemble stacking analysis: combine ML + physics predictions.

Reads level-0 ML predictions (from ml_prediction.py) and physics alarm
features (from prospective_analysis.py), merges them by (cell, time),
and trains a level-1 meta-learner.

Usage:
    python3 scripts/stacking_analysis.py
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from stacking import (
    LEVEL0_FEATURE_NAMES,
    StackingEnsemble,
    walk_forward_stacking,
)
from evaluation import compute_roc, molchan_area_skill_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"


def load_level0_predictions():
    """Load ML level-0 predictions for all targets.

    Returns:
        dict: {target_name: {(cell_lat, cell_lon, t_days): probability}}
    """
    predictions = {}

    for target_suffix in ["M5plus", "M55plus", "M6plus"]:
        target_file = RESULTS_DIR / f"level0_predictions_{target_suffix}.json"
        if not target_file.exists():
            logger.warning("  Level-0 file not found: %s", target_file.name)
            continue

        with open(target_file) as f:
            data = json.load(f)

        target_name = data.get("target", target_suffix)
        preds = {}
        for rec in data.get("predictions", []):
            key = (rec["cell_lat"], rec["cell_lon"], rec["t_days"])
            preds[key] = {
                "prob": rec["prob"],
                "label": rec["label"],
            }

        predictions[target_name] = preds
        logger.info("  Loaded %s: %d predictions", target_name, len(preds))

    return predictions


def load_physics_alarms():
    """Load physics alarm features from prospective_analysis.py export.

    Returns:
        dict: {(cell_lat, cell_lon, t_days): {etas_rate, cfs_kpa, ...}}
    """
    alarms_file = RESULTS_DIR / "physics_alarms.json"
    if not alarms_file.exists():
        logger.warning("  Physics alarms file not found: %s", alarms_file.name)
        return {}

    with open(alarms_file) as f:
        data = json.load(f)

    alarms = {}
    for rec in data.get("records", []):
        key = (rec["cell_lat"], rec["cell_lon"], rec["t_days"])
        alarms[key] = {
            "etas_rate": rec.get("etas_rate", 0),
            "cfs_kpa": rec.get("cfs_kpa", 0),
            "cfs_rate_state": rec.get("cfs_rate_state", 1.0),
            "foreshock_alarm": rec.get("foreshock_alarm", 0),
            "n_alarms": rec.get("n_alarms", 0),
        }

    logger.info("  Loaded physics alarms: %d records", len(alarms))
    return alarms


def merge_level0_features(ml_predictions, physics_alarms):
    """Merge ML predictions and physics alarms by exact (cell, time) key.

    Phase 8.1 fix: physics alarms are now generated at the same (cell, t_days)
    keys as ML level-0, so exact key matching works. No fuzzy matching needed.

    Returns:
        level0_data: list of feature vectors (8 features each)
        labels: list of binary labels
        t_days_list: list of time values
        keys: list of (cell_lat, cell_lon, t_days) tuples
    """
    # Get the primary target (M5+) as reference for keys and labels
    m5_preds = ml_predictions.get("M5+", {})
    m55_preds = ml_predictions.get("M5.5+", {})
    m6_preds = ml_predictions.get("M6+", {})

    if not m5_preds:
        logger.error("  No M5+ predictions available for stacking")
        return [], [], [], []

    level0_data = []
    labels = []
    t_days_list = []
    keys = []
    n_matched = 0
    n_missed = 0

    for key, m5_info in m5_preds.items():
        cell_lat, cell_lon, t_days = key

        # ML probabilities
        ml_m5 = m5_info["prob"]
        ml_m55 = m55_preds.get(key, {}).get("prob", 0.0)
        ml_m6 = m6_preds.get(key, {}).get("prob", 0.0)

        # Physics alarms — exact key match (keys are now aligned)
        physics = physics_alarms.get(key)

        if physics is not None:
            n_matched += 1
        else:
            n_missed += 1
            physics = {
                "etas_rate": 0.0,
                "cfs_kpa": 0.0,
                "cfs_rate_state": 1.0,
                "foreshock_alarm": 0,
                "n_alarms": 0,
            }

        feature_vec = [
            ml_m5,
            ml_m55,
            ml_m6,
            physics["etas_rate"],
            physics["cfs_kpa"],
            physics["cfs_rate_state"],
            physics["foreshock_alarm"],
            physics["n_alarms"],
        ]

        level0_data.append(feature_vec)
        labels.append(m5_info["label"])
        t_days_list.append(t_days)
        keys.append(key)

    match_rate = n_matched / max(n_matched + n_missed, 1) * 100
    logger.info("  Physics alarm match rate: %d/%d (%.1f%%)",
                n_matched, n_matched + n_missed, match_rate)
    logger.info("  Merged stacking dataset: %d samples, %d positive (%.2f%%)",
                len(labels), sum(labels),
                100 * sum(labels) / max(len(labels), 1))

    return level0_data, labels, t_days_list, keys


def compare_with_single_models(level0_data, labels, t_days_list):
    """Compare stacked ensemble vs best single model AUC."""
    if not level0_data or not labels:
        return {}

    results = {}

    # Individual feature AUCs
    for i, name in enumerate(LEVEL0_FEATURE_NAMES[:len(level0_data[0])]):
        vals = [row[i] for row in level0_data]
        if len(set(vals)) < 2:
            results[name] = {"auc": 0.5}
            continue
        _, auc = compute_roc(labels, vals)
        results[name] = {"auc": round(auc, 4)}

    # Simple average of ML probs
    avg_probs = [sum(row[:3]) / 3 for row in level0_data]
    _, avg_auc = compute_roc(labels, avg_probs)
    results["ml_average"] = {"auc": round(avg_auc, 4)}

    return results


def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("=== Ensemble Stacking Analysis ===")

    # Load level-0 predictions
    logger.info("--- Loading level-0 predictions ---")
    ml_predictions = load_level0_predictions()

    if not ml_predictions:
        logger.error("No ML predictions available. Run ml_prediction.py first.")
        return

    # Load physics alarms
    logger.info("--- Loading physics alarm features ---")
    physics_alarms = load_physics_alarms()

    # Merge features
    logger.info("--- Merging level-0 features ---")
    level0_data, labels, t_days_list, keys = merge_level0_features(
        ml_predictions, physics_alarms)

    if len(level0_data) < 100:
        logger.error("Insufficient merged data (%d samples)", len(level0_data))
        return

    # Compare individual features
    logger.info("--- Individual feature AUCs ---")
    single_results = compare_with_single_models(level0_data, labels, t_days_list)
    for name, info in sorted(single_results.items(), key=lambda x: -x[1].get("auc", 0)):
        logger.info("  %s: AUC=%.4f", name, info.get("auc", 0))

    # Walk-forward stacking with both meta-learner types
    stacking_results = {}

    for meta_type in ["logistic", "isotonic"]:
        logger.info("--- Walk-forward stacking (%s) ---", meta_type)
        folds, aggregate = walk_forward_stacking(
            level0_data, labels, t_days_list,
            initial_train_years=5, step_years=1, test_years=1,
            meta_type=meta_type,
        )
        stacking_results[meta_type] = {
            "aggregate": aggregate,
            "folds": folds,
        }
        logger.info("  %s: mean_AUC=%.4f (±%.4f) pooled_AUC=%.4f Molchan=%.4f",
                    meta_type, aggregate["mean_auc"], aggregate["std_auc"],
                    aggregate["pooled_auc"], aggregate["mean_molchan_skill"])

    # Final comparison
    best_single_auc = max(
        (info.get("auc", 0) for info in single_results.values()), default=0)
    best_stacked_auc = max(
        (sr["aggregate"]["pooled_auc"] for sr in stacking_results.values()), default=0)

    logger.info("--- Final Comparison ---")
    logger.info("  Best single model AUC: %.4f", best_single_auc)
    logger.info("  Best stacked AUC:      %.4f", best_stacked_auc)
    logger.info("  Improvement:           %+.4f", best_stacked_auc - best_single_auc)

    # Save results
    results = {
        "timestamp": timestamp,
        "n_samples": len(level0_data),
        "n_positive": sum(labels),
        "level0_features": LEVEL0_FEATURE_NAMES[:len(level0_data[0])] if level0_data else [],
        "single_model_aucs": single_results,
        "stacking": stacking_results,
        "comparison": {
            "best_single_auc": best_single_auc,
            "best_stacked_auc": best_stacked_auc,
            "improvement": round(best_stacked_auc - best_single_auc, 4),
        },
    }

    out_path = RESULTS_DIR / f"stacking_analysis_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved: %s", out_path)


if __name__ == "__main__":
    main()
