"""Ensemble stacking: combine physics-based and ML predictions.

Level-0 inputs (8 features):
    1. HistGBT M5+ probability
    2. HistGBT M5.5+ probability
    3. HistGBT M6+ probability
    4. ETAS expected rate (continuous)
    5. CFS cumulative kPa (continuous)
    6. CFS rate-state modified rate ratio (continuous)
    7. Foreshock alarm (binary)
    8. Composite alarm count (0-4)

Level-1 meta-learner: Logistic regression or Isotonic regression.
Simple by design — positive class has 50-200 samples, overfitting is
the primary risk.

Walk-Forward stacking: inner CV generates out-of-fold level-0
predictions to prevent temporal leakage.

References:
    - Wolpert (1992) "Stacked generalization"
    - van den Ende & Ampuero (2020) "Combining physics and ML"
"""

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# Base level-0 features (HistGBT + physics)
LEVEL0_FEATURE_NAMES_BASE = [
    "ml_m5_prob",
    "ml_m55_prob",
    "ml_m6_prob",
    "etas_rate",
    "cfs_kpa",
    "cfs_rate_state",
    "foreshock_alarm",
    "n_alarms",
]

# Extended level-0 features (diverse models: RandomForest + LogisticRegression)
LEVEL0_FEATURE_NAMES_DIVERSE = [
    "rf_m5_prob",
    "rf_m55_prob",
    "rf_m6_prob",
    "lr_m5_prob",
    "lr_m55_prob",
    "lr_m6_prob",
]

# Full feature list: base + diverse models (used when diverse predictions available)
LEVEL0_FEATURE_NAMES = LEVEL0_FEATURE_NAMES_BASE + LEVEL0_FEATURE_NAMES_DIVERSE


class StackingEnsemble:
    """Two-level stacking ensemble for earthquake prediction.

    Level-0: pre-trained models (HistGBT per target + physics alarms)
    Level-1: simple meta-learner (logistic regression or isotonic)
    """

    def __init__(self, meta_type="logistic"):
        """
        Args:
            meta_type: "logistic" or "isotonic"
        """
        self.meta_type = meta_type
        self.weights = None
        self.bias = 0.0
        self.isotonic_bins = None
        self._means = None
        self._stds = None

    def fit(self, X_meta, y_meta):
        """Fit level-1 meta-learner.

        Args:
            X_meta: list of lists, shape (n_samples, n_level0_features)
            y_meta: list of binary labels
        """
        n = len(y_meta)
        n_features = len(X_meta[0])

        if self.meta_type == "isotonic":
            self._fit_isotonic(X_meta, y_meta)
        else:
            self._fit_logistic(X_meta, y_meta, n_features)

        logger.info("  Stacking meta-learner (%s) fitted on %d samples", self.meta_type, n)

    def predict(self, X_meta):
        """Predict probabilities from level-0 features.

        Args:
            X_meta: list of lists, shape (n_samples, n_level0_features)

        Returns:
            list of probabilities
        """
        if self.meta_type == "isotonic":
            return self._predict_isotonic(X_meta)
        else:
            return self._predict_logistic(X_meta)

    def _fit_logistic(self, X, y, n_features):
        """Fit logistic regression via gradient descent with L2 regularization.

        Standardizes features (zero mean, unit variance) before fitting to handle
        different scales between ML probabilities (0-1) and physics features
        (ETAS rate ~0-50, CFS ~0-1000 kPa).
        """
        # Standardize features
        self._means = [0.0] * n_features
        self._stds = [1.0] * n_features
        n = len(y)
        for j in range(n_features):
            vals = [X[i][j] for i in range(n)]
            self._means[j] = sum(vals) / n
            var = sum((v - self._means[j]) ** 2 for v in vals) / max(n - 1, 1)
            self._stds[j] = math.sqrt(var) if var > 0 else 1.0

        # Standardize
        X_std = []
        for i in range(n):
            X_std.append([(X[i][j] - self._means[j]) / self._stds[j]
                          for j in range(n_features)])

        self.weights = [0.0] * n_features
        self.bias = 0.0
        lr = 0.01
        l2_reg = 1.0

        for epoch in range(200):
            grad_w = [0.0] * n_features
            grad_b = 0.0

            for i in range(n):
                z = self.bias + sum(w * x for w, x in zip(self.weights, X_std[i]))
                z = max(min(z, 20), -20)
                p = 1.0 / (1.0 + math.exp(-z))
                err = p - y[i]
                for j in range(n_features):
                    grad_w[j] += err * X_std[i][j] / n
                grad_b += err / n

            for j in range(n_features):
                grad_w[j] += l2_reg * self.weights[j] / n
                self.weights[j] -= lr * grad_w[j]
            self.bias -= lr * grad_b

    def _predict_logistic(self, X):
        probs = []
        n_features = len(self.weights)
        for row in X:
            x_std = [(row[j] - self._means[j]) / self._stds[j]
                     for j in range(n_features)]
            z = self.bias + sum(w * x for w, x in zip(self.weights, x_std))
            z = max(min(z, 20), -20)
            probs.append(1.0 / (1.0 + math.exp(-z)))
        return probs

    def _fit_isotonic(self, X, y):
        """Fit isotonic regression on the mean of level-0 features.

        Uses mean(level-0 probs) as a single score, then isotonic mapping.
        """
        scores = [sum(row) / len(row) for row in X]
        combined = sorted(zip(scores, y))

        # Bin into groups
        n = len(combined)
        n_bins = min(30, n // 5)
        if n_bins < 3:
            n_bins = 3
        bin_size = n // n_bins

        bin_scores = []
        bin_means = []
        for i in range(n_bins):
            start = i * bin_size
            end = start + bin_size if i < n_bins - 1 else n
            items = combined[start:end]
            bin_scores.append(sum(s for s, _ in items) / len(items))
            bin_means.append(sum(y_val for _, y_val in items) / len(items))

        # PAV monotonicity enforcement
        cal = list(bin_means)
        i = 0
        while i < len(cal) - 1:
            if cal[i] > cal[i + 1]:
                j = i + 1
                while j < len(cal) and cal[j] <= cal[i]:
                    j += 1
                pool_avg = sum(cal[i:j]) / (j - i)
                for k in range(i, j):
                    cal[k] = pool_avg
                i = j
            else:
                i += 1

        self.isotonic_bins = list(zip(bin_scores, cal))

    def _predict_isotonic(self, X):
        scores = [sum(row) / len(row) for row in X]
        probs = []
        for s in scores:
            # Interpolate
            bins = self.isotonic_bins
            if s <= bins[0][0]:
                probs.append(bins[0][1])
            elif s >= bins[-1][0]:
                probs.append(bins[-1][1])
            else:
                for i in range(len(bins) - 1):
                    if bins[i][0] <= s <= bins[i + 1][0]:
                        frac = (s - bins[i][0]) / max(bins[i + 1][0] - bins[i][0], 1e-10)
                        probs.append(bins[i][1] + frac * (bins[i + 1][1] - bins[i][1]))
                        break
                else:
                    probs.append(bins[-1][1])
        return probs

    def get_weights(self):
        """Return meta-learner weights for interpretability."""
        if self.meta_type == "logistic" and self.weights:
            return dict(zip(LEVEL0_FEATURE_NAMES[:len(self.weights)], self.weights))
        return {}


def walk_forward_stacking(level0_data, labels, t_days_list,
                           initial_train_years=5, step_years=1, test_years=1,
                           meta_type="logistic"):
    """Walk-forward stacking with temporal leak prevention.

    For each fold:
        1. Split level-0 predictions into train/test by time
        2. Fit level-1 meta-learner on train portion
        3. Predict on test portion
        4. Evaluate

    Args:
        level0_data: list of lists (n_samples, n_level0_features)
        labels: list of binary labels
        t_days_list: list of time values (for temporal splitting)
        initial_train_years: initial training window
        step_years: advance per fold
        test_years: test window size
        meta_type: "logistic" or "isotonic"

    Returns:
        fold_results, aggregate
    """
    from evaluation import compute_roc, molchan_area_skill_score

    day_min = min(t_days_list)
    day_max = max(t_days_list)

    initial_train_days = initial_train_years * 365.25
    step_days = step_years * 365.25
    test_days_len = test_years * 365.25

    splits = []
    test_start = day_min + initial_train_days
    while test_start + test_days_len <= day_max:
        splits.append((day_min, test_start, test_start, test_start + test_days_len))
        test_start += step_days

    logger.info("  Stacking walk-forward: %d folds", len(splits))

    fold_results = []
    all_test_y = []
    all_test_probs = []

    for fold_idx, (train_start, train_end, t_start, t_end) in enumerate(splits):
        train_X, train_y = [], []
        test_X, test_y = [], []

        for x, y, t in zip(level0_data, labels, t_days_list):
            if train_start <= t < train_end:
                train_X.append(x)
                train_y.append(y)
            elif t_start <= t < t_end:
                test_X.append(x)
                test_y.append(y)

        n_pos_train = sum(train_y)
        n_pos_test = sum(test_y)

        if n_pos_train < 5 or len(test_y) < 10:
            logger.warning("  Stacking fold %d: skipping (pos_train=%d, test=%d)",
                          fold_idx, n_pos_train, len(test_y))
            continue

        # Fit meta-learner
        ensemble = StackingEnsemble(meta_type=meta_type)
        ensemble.fit(train_X, train_y)

        # Predict
        test_probs = ensemble.predict(test_X)

        # Evaluate
        if n_pos_test > 0:
            _, auc = compute_roc(test_y, test_probs)
            molchan = molchan_area_skill_score(test_y, test_probs)
        else:
            auc, molchan = 0.5, 0.0

        fold_results.append({
            "fold": fold_idx,
            "train_size": len(train_y),
            "test_size": len(test_y),
            "train_pos": n_pos_train,
            "test_pos": n_pos_test,
            "auc_roc": round(auc, 4),
            "molchan_skill": round(molchan, 4),
            "meta_weights": ensemble.get_weights(),
        })

        all_test_y.extend(test_y)
        all_test_probs.extend(test_probs)

        logger.info("  Stacking fold %d: AUC=%.4f Molchan=%.4f (pos=%d/%d)",
                    fold_idx, auc, molchan, n_pos_test, len(test_y))

    # Aggregate
    if fold_results:
        mean_auc = sum(f["auc_roc"] for f in fold_results) / len(fold_results)
        mean_molchan = sum(f["molchan_skill"] for f in fold_results) / len(fold_results)
        aucs = [f["auc_roc"] for f in fold_results]
        std_auc = math.sqrt(sum((a - mean_auc) ** 2 for a in aucs) / len(aucs)) if aucs else 0
    else:
        mean_auc, std_auc, mean_molchan = 0, 0, 0

    # Overall pooled AUC
    if all_test_y:
        from evaluation import compute_roc as _roc
        _, pooled_auc = _roc(all_test_y, all_test_probs)
    else:
        pooled_auc = 0.5

    aggregate = {
        "n_folds": len(fold_results),
        "mean_auc": round(mean_auc, 4),
        "std_auc": round(std_auc, 4),
        "pooled_auc": round(pooled_auc, 4),
        "mean_molchan_skill": round(mean_molchan, 4),
        "meta_type": meta_type,
    }

    return fold_results, aggregate
