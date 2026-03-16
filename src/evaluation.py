"""Shared evaluation module for earthquake prediction.

Consolidates metrics from ml_prediction.py and prospective_analysis.py:
    - ROC-AUC computation
    - Precision/Recall/Gain/IGPE/Molchan at multiple thresholds
    - Walk-forward CV framework
    - Isotonic calibration
    - Reliability diagram data

References:
    - Zechar & Jordan (2008) "Testing alarm-based earthquake predictions"
    - Molchan (1991) "Strategies in strong earthquake prediction"
    - Platt (1999) "Probabilistic outputs for SVMs" (calibration)
"""

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ROC-AUC
# ---------------------------------------------------------------------------

def compute_roc(y_true: list, y_prob: list, n_thresholds: int = 200):
    """Compute ROC curve and AUC via trapezoidal rule.

    Args:
        y_true: binary labels (0/1)
        y_prob: predicted probabilities
        n_thresholds: max number of threshold points

    Returns:
        (roc_points, auc) where roc_points is list of (fpr, tpr, threshold)
    """
    n_pos = sum(y_true)
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return [], 0.5

    # Sort by decreasing probability
    combined = sorted(zip(y_prob, y_true), key=lambda x: -x[0])

    thresholds = sorted(set(p for p, _ in combined), reverse=True)
    if len(thresholds) > n_thresholds:
        step = max(1, len(thresholds) // n_thresholds)
        thresholds = thresholds[::step]
        if thresholds[-1] != 0.0:
            thresholds.append(0.0)

    roc_points = [(0.0, 0.0, 1.1)]

    for thresh in thresholds:
        tp = sum(1 for p, y in combined if p >= thresh and y == 1)
        fp = sum(1 for p, y in combined if p >= thresh and y == 0)
        tpr = tp / n_pos
        fpr = fp / n_neg
        roc_points.append((fpr, tpr, thresh))

    roc_points.append((1.0, 1.0, 0.0))
    roc_points = sorted(set(roc_points), key=lambda x: (x[0], x[1]))

    # AUC via trapezoidal rule
    auc = 0.0
    for i in range(1, len(roc_points)):
        dx = roc_points[i][0] - roc_points[i - 1][0]
        avg_y = (roc_points[i][1] + roc_points[i - 1][1]) / 2
        auc += dx * avg_y

    return roc_points, auc


# ---------------------------------------------------------------------------
# Threshold-based alarm evaluation
# ---------------------------------------------------------------------------

def evaluate_at_thresholds(
    y_true: list,
    y_prob: list,
    base_rate: float,
    thresholds: Optional[list] = None,
) -> list:
    """Evaluate at multiple probability thresholds.

    Returns list of dicts with:
        threshold, n_alarms, tp, fp, fn, precision, recall,
        false_alarm_rate, probability_gain, igpe_bits,
        molchan_miss_rate, alarm_fraction, molchan_score
    """
    if thresholds is None:
        thresholds = [0.05, 0.10, 0.15, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80]

    n_total = len(y_true)
    n_pos = sum(y_true)
    results = []

    for thresh in thresholds:
        tp = sum(1 for p, y in zip(y_prob, y_true) if p >= thresh and y == 1)
        fp = sum(1 for p, y in zip(y_prob, y_true) if p >= thresh and y == 0)
        fn = n_pos - tp
        tn = n_total - n_pos - fp
        n_alarms = tp + fp

        precision = tp / max(n_alarms, 1)
        recall = tp / max(n_pos, 1)
        false_alarm_rate = fp / max(fp + tn, 1)

        prob_gain = precision / max(base_rate, 1e-6)
        igpe = math.log2(max(prob_gain, 1e-6)) if prob_gain > 0 else -10

        alarm_fraction = n_alarms / max(n_total, 1)
        miss_rate = fn / max(n_pos, 1)
        molchan_score = recall - alarm_fraction

        results.append({
            "threshold": round(thresh, 3),
            "n_alarms": n_alarms,
            "tp": tp, "fp": fp, "fn": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "false_alarm_rate": round(false_alarm_rate, 4),
            "probability_gain": round(prob_gain, 2),
            "igpe_bits": round(igpe, 2),
            "molchan_miss_rate": round(miss_rate, 4),
            "alarm_fraction": round(alarm_fraction, 6),
            "molchan_score": round(molchan_score, 4),
        })

    return results


# ---------------------------------------------------------------------------
# Probability Gain and IGPE
# ---------------------------------------------------------------------------

def probability_gain(precision: float, base_rate: float) -> float:
    """Probability gain: P(event|alarm) / P(event)."""
    return precision / max(base_rate, 1e-10)


def igpe(precision: float, base_rate: float) -> float:
    """Information Gain Per Earthquake in bits."""
    gain = probability_gain(precision, base_rate)
    if gain <= 0:
        return -10.0
    return math.log2(gain)


# ---------------------------------------------------------------------------
# Molchan diagram
# ---------------------------------------------------------------------------

def molchan_area_skill_score(y_true: list, y_prob: list, n_points: int = 100) -> float:
    """Molchan diagram area skill score.

    Molchan plots miss_rate vs alarm_fraction. Random forecast = diagonal.
    Area below Molchan curve: 0.5 = random, 0 = perfect.
    Skill score: 1 - 2*area (1 = perfect, 0 = random, <0 = worse).
    """
    n_pos = sum(y_true)
    n_total = len(y_true)
    if n_pos == 0 or n_total == 0:
        return 0.0

    combined = sorted(zip(y_prob, y_true), key=lambda x: -x[0])

    # Sweep thresholds
    thresholds = sorted(set(p for p, _ in combined), reverse=True)
    if len(thresholds) > n_points:
        step = max(1, len(thresholds) // n_points)
        thresholds = thresholds[::step]

    points = [(0.0, 1.0)]  # (alarm_fraction=0, miss_rate=1)
    for thresh in thresholds:
        tp = sum(1 for p, y in combined if p >= thresh and y == 1)
        n_alarm = sum(1 for p, _ in combined if p >= thresh)
        miss_rate = 1 - tp / n_pos
        alarm_frac = n_alarm / n_total
        points.append((alarm_frac, miss_rate))
    points.append((1.0, 0.0))

    points = sorted(set(points), key=lambda x: x[0])

    # Area under Molchan curve (trapezoidal)
    area = 0.0
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        avg_y = (points[i][1] + points[i - 1][1]) / 2
        area += dx * avg_y

    # Skill score: 1 - 2*area
    return round(1 - 2 * area, 4)


# ---------------------------------------------------------------------------
# Isotonic calibration
# ---------------------------------------------------------------------------

def isotonic_calibration(y_true: list, y_prob: list):
    """Pool Adjacent Violators (PAV) isotonic regression.

    Pure Python implementation — no sklearn needed.

    Args:
        y_true: binary labels (0/1)
        y_prob: predicted probabilities

    Returns:
        Callable that maps raw probability → calibrated probability.
    """
    # Sort by predicted probability
    combined = sorted(zip(y_prob, y_true))
    n = len(combined)

    # Bin into ~50 bins for stability
    n_bins = min(50, n // 10)
    if n_bins < 5:
        # Not enough data for calibration
        return lambda p: p

    bin_size = n // n_bins
    bin_probs = []
    bin_means = []

    for i in range(n_bins):
        start = i * bin_size
        end = start + bin_size if i < n_bins - 1 else n
        bin_items = combined[start:end]
        avg_pred = sum(p for p, _ in bin_items) / len(bin_items)
        avg_true = sum(y for _, y in bin_items) / len(bin_items)
        bin_probs.append(avg_pred)
        bin_means.append(avg_true)

    # PAV algorithm: enforce monotonicity
    calibrated = list(bin_means)
    i = 0
    while i < len(calibrated) - 1:
        if calibrated[i] > calibrated[i + 1]:
            # Pool violators
            j = i + 1
            while j < len(calibrated) and calibrated[j] <= calibrated[i]:
                j += 1
            # Average the pool
            pool_avg = sum(calibrated[i:j]) / (j - i)
            for k in range(i, j):
                calibrated[k] = pool_avg
            i = j
        else:
            i += 1

    # Build lookup: interpolate between bins
    def calibrate(p):
        if p <= bin_probs[0]:
            return calibrated[0]
        if p >= bin_probs[-1]:
            return calibrated[-1]
        # Binary search
        lo, hi = 0, len(bin_probs) - 1
        while lo < hi - 1:
            mid = (lo + hi) // 2
            if bin_probs[mid] <= p:
                lo = mid
            else:
                hi = mid
        # Linear interpolation
        frac = (p - bin_probs[lo]) / max(bin_probs[hi] - bin_probs[lo], 1e-10)
        return calibrated[lo] + frac * (calibrated[hi] - calibrated[lo])

    return calibrate


# ---------------------------------------------------------------------------
# Reliability diagram data
# ---------------------------------------------------------------------------

def reliability_diagram(
    y_true: list,
    y_prob: list,
    n_bins: int = 10,
) -> list:
    """Compute reliability diagram (calibration curve) data.

    Returns list of dicts:
        bin_center, mean_predicted, mean_observed, count
    """
    bins = [[] for _ in range(n_bins)]
    for p, y in zip(y_prob, y_true):
        idx = min(int(p * n_bins), n_bins - 1)
        bins[idx].append((p, y))

    result = []
    for i, b in enumerate(bins):
        if not b:
            continue
        result.append({
            "bin_center": round((i + 0.5) / n_bins, 3),
            "mean_predicted": round(sum(p for p, _ in b) / len(b), 4),
            "mean_observed": round(sum(y for _, y in b) / len(b), 4),
            "count": len(b),
        })
    return result


# ---------------------------------------------------------------------------
# Walk-Forward Cross-Validation
# ---------------------------------------------------------------------------

def walk_forward_splits(
    event_day_min: float,
    event_day_max: float,
    train_start_day: float = 0.0,
    initial_train_years: int = 5,
    step_years: int = 1,
    test_years: int = 1,
) -> list:
    """Generate walk-forward CV splits (expanding window).

    Each split: (train_start, train_end, test_start, test_end) in days.

    Args:
        event_day_min, event_day_max: data extent in days
        train_start_day: first available training day
        initial_train_years: minimum training window
        step_years: how much to advance between splits
        test_years: test window size

    Returns:
        List of (train_start, train_end, test_start, test_end) tuples.
    """
    splits = []
    initial_train_days = initial_train_years * 365.25
    step_days = step_years * 365.25
    test_days = test_years * 365.25

    # First test start
    test_start = train_start_day + initial_train_days

    while test_start + test_days <= event_day_max:
        train_end = test_start
        test_end = test_start + test_days

        splits.append((
            train_start_day,
            train_end,
            test_start,
            min(test_end, event_day_max),
        ))

        test_start += step_days

    return splits


# ---------------------------------------------------------------------------
# Feature importance (permutation-based)
# ---------------------------------------------------------------------------

def permutation_importance(
    model_predict_fn,
    X: list,
    y: list,
    feature_names: list,
    n_repeats: int = 5,
    metric: str = "auc",
) -> list:
    """Compute permutation importance for each feature.

    Shuffles each feature column and measures performance drop.

    Args:
        model_predict_fn: callable(X) -> probabilities
        X: feature matrix (list of lists)
        y: binary labels
        feature_names: feature names
        n_repeats: number of shuffle repeats
        metric: "auc" or "log_loss"

    Returns:
        List of dicts sorted by importance (descending):
            feature, baseline_score, mean_score, importance, std
    """
    import random

    # Baseline score
    y_prob = model_predict_fn(X)
    if metric == "auc":
        _, baseline = compute_roc(y, y_prob)
    else:
        baseline = -_log_loss(y, y_prob)

    n_samples = len(X)
    n_features = len(X[0])
    results = []

    for fi in range(n_features):
        scores = []
        for _ in range(n_repeats):
            # Shuffle feature fi
            X_perm = [row[:] for row in X]  # deep copy rows
            perm_vals = [X_perm[i][fi] for i in range(n_samples)]
            random.shuffle(perm_vals)
            for i in range(n_samples):
                X_perm[i][fi] = perm_vals[i]

            y_prob_perm = model_predict_fn(X_perm)
            if metric == "auc":
                _, score = compute_roc(y, y_prob_perm)
            else:
                score = -_log_loss(y, y_prob_perm)
            scores.append(score)

        mean_score = sum(scores) / len(scores)
        importance = baseline - mean_score
        std = math.sqrt(sum((s - mean_score) ** 2 for s in scores) / len(scores))

        results.append({
            "feature": feature_names[fi],
            "baseline_score": round(baseline, 4),
            "mean_permuted_score": round(mean_score, 4),
            "importance": round(importance, 4),
            "std": round(std, 4),
        })

    results.sort(key=lambda x: -x["importance"])
    return results


def _log_loss(y_true, y_prob, eps=1e-15):
    """Binary cross-entropy log loss."""
    total = 0.0
    for yi, pi in zip(y_true, y_prob):
        pi = max(min(pi, 1 - eps), eps)
        total += yi * math.log(pi) + (1 - yi) * math.log(1 - pi)
    return -total / max(len(y_true), 1)


# ---------------------------------------------------------------------------
# Single-feature AUC ranking
# ---------------------------------------------------------------------------

def single_feature_auc_ranking(X: list, y: list, feature_names: list) -> list:
    """Rank features by their individual AUC.

    For each feature, test both raw and inverted direction.

    Returns list of dicts sorted by AUC descending:
        feature, auc, direction
    """
    n_features = len(X[0])
    results = []

    for fi in range(n_features):
        vals = [X[i][fi] for i in range(len(X))]
        _, auc_pos = compute_roc(y, vals)
        inv_vals = [-v for v in vals]
        _, auc_neg = compute_roc(y, inv_vals)

        best_auc = max(auc_pos, auc_neg)
        direction = "higher_risk" if auc_pos >= auc_neg else "lower_risk"

        results.append({
            "feature": feature_names[fi],
            "auc": round(best_auc, 4),
            "direction": direction,
        })

    results.sort(key=lambda x: -x["auc"])
    return results
