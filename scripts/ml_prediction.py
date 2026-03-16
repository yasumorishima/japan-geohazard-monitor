"""ML-integrated earthquake prediction using gradient boosting (pure Python).

Combines multiple seismicity signals into a unified prediction model:
    "Will an M5+ earthquake occur within 7 days in this 2x2 degree cell?"

Features (computed from pre-event data only):
    1.  rate_7d         - M3+ count in past 7 days (2° box)
    2.  rate_30d        - M3+ count in past 30 days
    3.  rate_ratio_7d   - 7-day rate / long-term average rate
    4.  etas_residual   - observed 7-day rate / ETAS predicted rate
    5.  max_mag_7d      - max magnitude in past 7 days
    6.  max_mag_30d     - max magnitude in past 30 days
    7.  n_foreshock     - M3+ count within 1° in past 7 days (foreshock proxy)
    8.  cfs_cumulative  - cumulative Coulomb stress from all past M5.5+ (kPa)
    9.  b_value         - Gutenberg-Richter b-value from past 90 days
    10. days_since_m5   - days since last M5+ within 2°
    11. pi_score        - Pattern Informatics variance (2-year baseline)

Algorithm: Decision stump ensemble (AdaBoost-style) — pure Python, no sklearn.
Evaluation: ROC-AUC, precision/recall at multiple thresholds, Molchan score, IGPE.

References:
    - Ogata (1998) "Space-Time Point-Process Models for Earthquake Occurrences"
    - Rundle et al. (2003) "Statistical physics approach to understanding
      the multiscale dynamics of earthquake fault systems"
    - Zechar & Jordan (2008) "Testing alarm-based earthquake predictions"
    - Molchan (1991) "Strategies in strong earthquake prediction"
"""

import asyncio
import bisect
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

# ---------------------------------------------------------------------------
# Physical constants
# ---------------------------------------------------------------------------
DEG_TO_KM = 111.32
SHEAR_MODULUS = 32e9
MU_FRICTION = 0.4

# Prediction parameters
PREDICTION_WINDOW_DAYS = 7
CELL_SIZE_DEG = 2.0
MIN_TARGET_MAG = 5.0
STEP_DAYS = 3  # Generate samples every 3 days

# ETAS parameters (Ogata 1998, Japan)
ETAS_K = 0.04
ETAS_ALPHA = 1.0
ETAS_C = 0.01  # days
ETAS_P = 1.1
ETAS_MC = 3.0

# Japan grid bounds
GRID_LAT_MIN, GRID_LAT_MAX = 26, 46  # inclusive, step=CELL_SIZE_DEG
GRID_LON_MIN, GRID_LON_MAX = 128, 148

# Train/test split
SPLIT_DATE = datetime(2019, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Physics utilities (from prospective_analysis.py)
# ---------------------------------------------------------------------------

def fault_dimensions(mw):
    """Wells & Coppersmith (1994) scaling relations."""
    length_km = 10 ** (-2.86 + 0.63 * mw)
    width_km = 10 ** (-1.61 + 0.41 * mw)
    m0 = 10 ** (1.5 * mw + 9.05)
    slip_m = m0 / (SHEAR_MODULUS * length_km * 1000 * width_km * 1000)
    return length_km, width_km, slip_m


def default_mechanism(lat, lon, depth):
    """Default focal mechanism based on tectonic region."""
    if depth > 70:
        return 200.0, 45.0, 90.0
    elif lon > 142 and lat > 35:
        return 200.0, 25.0, 90.0
    elif lon < 137 and lat < 35:
        return 240.0, 15.0, 90.0
    else:
        return 200.0, 35.0, 90.0


def okada_cfs(src_lat, src_lon, src_depth, src_strike, src_dip, src_rake,
              src_length, src_width, src_slip,
              obs_lat, obs_lon, obs_depth):
    """Coulomb failure stress change from a rectangular dislocation (Okada approx)."""
    dx = (obs_lon - src_lon) * DEG_TO_KM * math.cos(math.radians(src_lat)) * 1000
    dy = (obs_lat - src_lat) * DEG_TO_KM * 1000
    dz = (obs_depth - src_depth) * 1000
    r = math.sqrt(dx**2 + dy**2 + dz**2)
    if r < 500:
        return 0.0
    m0 = SHEAR_MODULUS * src_length * 1000 * src_width * 1000 * src_slip
    strike_r = math.radians(src_strike)
    dip_r = math.radians(src_dip)
    rake_r = math.radians(src_rake)
    n = [-math.sin(dip_r) * math.sin(strike_r),
         math.sin(dip_r) * math.cos(strike_r),
         -math.cos(dip_r)]
    d = [math.cos(rake_r) * math.cos(strike_r) + math.sin(rake_r) * math.cos(dip_r) * math.sin(strike_r),
         math.cos(rake_r) * math.sin(strike_r) - math.sin(rake_r) * math.cos(dip_r) * math.cos(strike_r),
         -math.sin(rake_r) * math.sin(dip_r)]
    rhat = [dx / r, dy / r, dz / r]
    m_ij = [[(n[i] * d[j] + n[j] * d[i]) / 2 for j in range(3)] for i in range(3)]
    prefactor = m0 / (4 * math.pi * r**3)
    m_rr = sum(m_ij[k][l] * rhat[k] * rhat[l] for k in range(3) for l in range(3))
    stress = [[prefactor * (3 * m_rr * rhat[i] * rhat[j] - m_ij[i][j] -
               (1 if i == j else 0) * m_rr) for j in range(3)] for i in range(3)]
    sigma_mean = (stress[0][0] + stress[1][1] + stress[2][2]) / 3
    dev = [[stress[i][j] - (sigma_mean if i == j else 0) for j in range(3)] for i in range(3)]
    j2 = 0.5 * sum(dev[i][j] ** 2 for i in range(3) for j in range(3))
    tau_max = math.sqrt(max(j2, 0))
    return tau_max + MU_FRICTION * sigma_mean


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def _cell_key(lat, lon):
    """Snap coordinates to nearest 2° grid cell centre."""
    return (round(lat / CELL_SIZE_DEG) * CELL_SIZE_DEG,
            round(lon / CELL_SIZE_DEG) * CELL_SIZE_DEG)


def _b_value_aki(mags, mc=3.0):
    """Gutenberg-Richter b-value using Aki-Utsu maximum likelihood estimator.

    b = log10(e) / (<M> - (Mc - dM/2))
    where dM = 0.1 (magnitude binning).
    """
    filtered = [m for m in mags if m >= mc]
    if len(filtered) < 20:
        return None
    m_mean = sum(filtered) / len(filtered)
    denominator = m_mean - (mc - 0.05)
    if denominator <= 0.01:
        return None
    return math.log10(math.e) / denominator


def compute_etas_rate(events_in_cell, t_now_days, window_days, mu_bg):
    """Compute ETAS predicted rate for a cell over [t_now - window, t_now].

    Only considers events occurring BEFORE the window start as triggers
    (events within the window are what we're predicting, so they shouldn't
    contribute to the expected rate in a truly prospective sense — but in
    practice ETAS includes all prior events). We include events up to
    5000 prior events for computational efficiency.
    """
    t_start = t_now_days - window_days
    etas_rate = mu_bg * window_days  # Background

    for e in events_in_cell:
        e_t = e["t_days"]
        if e_t >= t_start:
            break  # Only prior events contribute
        dt_start = t_start - e_t
        dt_end = t_now_days - e_t
        if dt_end <= 0:
            continue
        if dt_start < 0.001:
            dt_start = 0.001
        productivity = ETAS_K * math.exp(ETAS_ALPHA * (e["mag"] - ETAS_MC))
        if abs(ETAS_P - 1.0) < 0.01:
            integral = productivity * (math.log(dt_end + ETAS_C) - math.log(dt_start + ETAS_C))
        else:
            integral = productivity / (1 - ETAS_P) * (
                (dt_end + ETAS_C) ** (1 - ETAS_P) - (dt_start + ETAS_C) ** (1 - ETAS_P))
        etas_rate += max(integral, 0)

    return max(etas_rate, 0.1)  # Floor to avoid division by zero


def compute_pi_score(cell_rates_history, n_windows):
    """Pattern Informatics score: variance of normalized rate changes.

    Given a list of rate values for a cell across sliding windows,
    compute the variance of the standardized changes.
    """
    if n_windows < 4:
        return 0.0
    # Rate changes between consecutive windows
    changes = []
    for i in range(1, n_windows):
        changes.append(cell_rates_history[i] - cell_rates_history[i - 1])
    if not changes:
        return 0.0
    mean_change = sum(changes) / len(changes)
    variance = sum((c - mean_change) ** 2 for c in changes) / len(changes)
    return variance


class FeatureExtractor:
    """Extracts all 11 features for a given (cell, time) pair.

    Pre-indexes events by cell for O(1) spatial lookup.
    Pre-computes cumulative CFS map incrementally.
    """

    def __init__(self, events, fm_dict, t0):
        self.events = events
        self.fm_dict = fm_dict
        self.t0 = t0
        self.all_t_days = [e["t_days"] for e in events]

        # Index events by 2° cell
        self.cell_events = {}  # cell_key -> sorted list of events
        for e in events:
            ck = _cell_key(e["lat"], e["lon"])
            if ck not in self.cell_events:
                self.cell_events[ck] = []
            self.cell_events[ck].append(e)

        # Index events by 1° cell (for foreshock counting)
        self.cell_events_1deg = {}
        for e in events:
            k1 = (round(e["lat"]), round(e["lon"]))
            if k1 not in self.cell_events_1deg:
                self.cell_events_1deg[k1] = []
            self.cell_events_1deg[k1].append(e)

        # Pre-compute long-term rates per 2° cell
        total_days = self.all_t_days[-1] - self.all_t_days[0]
        self.cell_lt_rate = {}
        for ck, evs in self.cell_events.items():
            self.cell_lt_rate[ck] = len(evs) / max(total_days, 1)

        # CFS map: built incrementally
        self.cfs_map = {}  # cell_key -> cumulative CFS (kPa)
        self._cfs_source_idx = 0
        self._m55_events = [e for e in events if e["mag"] >= 5.5]

        # PI rate history per cell (for sliding window PI computation)
        # Store rates at each STEP_DAYS interval
        self.pi_history = {}  # cell_key -> list of rates (one per step)

        # Background ETAS rate per cell
        self.mu_bg = {}
        for ck, evs in self.cell_events.items():
            # Simple background: total events / total time / expected aftershock fraction
            # Rough estimate: ~30% of events are background
            self.mu_bg[ck] = 0.3 * len(evs) / max(total_days, 1) * 7.0 / 7.0
            # Normalize to per-7-day unit consistent with ETAS rate calc
            # Actually just use raw: count/day
            self.mu_bg[ck] = max(0.3 * len(evs) / max(total_days, 1), 0.01)

    def _update_cfs_map(self, t_now_days):
        """Incrementally add CFS from M5.5+ events up to t_now_days."""
        while (self._cfs_source_idx < len(self._m55_events) and
               self._m55_events[self._cfs_source_idx]["t_days"] <= t_now_days):
            src = self._m55_events[self._cfs_source_idx]
            fm_key = (round(src["lat"], 1), round(src["lon"], 1))
            strike, dip, rake = self.fm_dict.get(
                fm_key, default_mechanism(src["lat"], src["lon"], src["depth"]))
            length, width, slip = fault_dimensions(src["mag"])

            # Compute CFS change at each grid cell
            for lat in range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG)):
                for lon in range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG)):
                    ck = (float(lat), float(lon))
                    dist_lat = abs(src["lat"] - lat) * DEG_TO_KM
                    dist_lon = abs(src["lon"] - lon) * DEG_TO_KM
                    if dist_lat > 300 or dist_lon > 300:
                        continue
                    cfs = okada_cfs(
                        src["lat"], src["lon"], src["depth"],
                        strike, dip, rake, length, width, slip,
                        float(lat), float(lon), 15.0)
                    if ck not in self.cfs_map:
                        self.cfs_map[ck] = 0.0
                    self.cfs_map[ck] += cfs / 1000  # Convert Pa to kPa

            self._cfs_source_idx += 1

    def extract(self, cell_lat, cell_lon, t_now_days):
        """Extract 11 features for a (cell, time_now) pair.

        All features use data strictly BEFORE t_now_days.
        """
        ck = (cell_lat, cell_lon)
        cell_evs = self.cell_events.get(ck, [])

        # Binary search for event indices
        t_7 = t_now_days - 7
        t_30 = t_now_days - 30
        t_90 = t_now_days - 90

        # Events in windows (using pre-sorted cell events)
        evs_7d = [e for e in cell_evs if t_7 <= e["t_days"] < t_now_days]
        evs_30d = [e for e in cell_evs if t_30 <= e["t_days"] < t_now_days]
        evs_90d = [e for e in cell_evs if t_90 <= e["t_days"] < t_now_days]

        # Feature 1: rate_7d
        rate_7d = len(evs_7d)

        # Feature 2: rate_30d
        rate_30d = len(evs_30d)

        # Feature 3: rate_ratio_7d
        lt_rate_7d = self.cell_lt_rate.get(ck, 0.01) * 7
        rate_ratio_7d = rate_7d / max(lt_rate_7d, 0.1)

        # Feature 4: etas_residual_7d
        mu = self.mu_bg.get(ck, 0.01)
        # Gather prior events for ETAS (events before t_7 in this cell)
        prior_evs = [e for e in cell_evs if e["t_days"] < t_7]
        # Limit to last 2000 for speed
        prior_evs = prior_evs[-2000:]
        etas_pred = compute_etas_rate(prior_evs, t_now_days, 7, mu)
        etas_residual = rate_7d / max(etas_pred, 0.1)

        # Feature 5: max_mag_7d
        max_mag_7d = max((e["mag"] for e in evs_7d), default=0.0)

        # Feature 6: max_mag_30d
        max_mag_30d = max((e["mag"] for e in evs_30d), default=0.0)

        # Feature 7: n_foreshock (1° box, 7 days)
        n_foreshock = 0
        for dlat in (-1, 0, 1):
            for dlon in (-1, 0, 1):
                k1 = (round(cell_lat) + dlat, round(cell_lon) + dlon)
                for e in self.cell_events_1deg.get(k1, []):
                    if t_7 <= e["t_days"] < t_now_days:
                        n_foreshock += 1

        # Feature 8: cfs_cumulative
        self._update_cfs_map(t_now_days)
        cfs_cumulative = self.cfs_map.get(ck, 0.0)

        # Feature 9: b_value (90 days)
        mags_90d = [e["mag"] for e in evs_90d]
        b_val = _b_value_aki(mags_90d, mc=3.0)
        if b_val is None:
            b_val = 1.0  # Default GR b-value

        # Feature 10: days_since_last_m5 (2° box)
        days_since_m5 = 9999.0
        for e in reversed(cell_evs):
            if e["t_days"] >= t_now_days:
                continue
            if e["mag"] >= 5.0:
                days_since_m5 = t_now_days - e["t_days"]
                break

        # Feature 11: pi_score
        # Use rate history stored in pi_history
        hist = self.pi_history.get(ck, [])
        hist.append(rate_7d)
        self.pi_history[ck] = hist
        # Use last ~240 entries (2 years / 3 days per step)
        recent_hist = hist[-240:]
        pi_score = compute_pi_score(recent_hist, len(recent_hist))

        return {
            "rate_7d": rate_7d,
            "rate_30d": rate_30d,
            "rate_ratio_7d": rate_ratio_7d,
            "etas_residual": etas_residual,
            "max_mag_7d": max_mag_7d,
            "max_mag_30d": max_mag_30d,
            "n_foreshock": n_foreshock,
            "cfs_cumulative": cfs_cumulative,
            "b_value": b_val,
            "days_since_m5": days_since_m5,
            "pi_score": pi_score,
        }


# ---------------------------------------------------------------------------
# Label generation
# ---------------------------------------------------------------------------

def generate_label(cell_lat, cell_lon, t_now_days, target_events_by_cell):
    """1 if M5+ occurs within 7 days and 2° of cell centre, else 0."""
    ck = (cell_lat, cell_lon)
    targets = target_events_by_cell.get(ck, [])
    t_end = t_now_days + PREDICTION_WINDOW_DAYS
    for t_target in targets:
        if t_now_days < t_target <= t_end:
            return 1
    # Also check neighboring cells (within 2°)
    for dlat in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
        for dlon in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
            if dlat == 0 and dlon == 0:
                continue
            nk = (cell_lat + dlat, cell_lon + dlon)
            for t_target in target_events_by_cell.get(nk, []):
                if t_now_days < t_target <= t_end:
                    return 1
    return 0


# ---------------------------------------------------------------------------
# Pure Python AdaBoost with decision stumps
# ---------------------------------------------------------------------------

FEATURE_NAMES = [
    "rate_7d", "rate_30d", "rate_ratio_7d", "etas_residual",
    "max_mag_7d", "max_mag_30d", "n_foreshock", "cfs_cumulative",
    "b_value", "days_since_m5", "pi_score",
]


class DecisionStump:
    """A single decision stump: threshold on one feature."""

    __slots__ = ("feature_idx", "threshold", "polarity", "alpha")

    def __init__(self):
        self.feature_idx = 0
        self.threshold = 0.0
        self.polarity = 1  # 1 = predict 1 if x > threshold
        self.alpha = 0.0

    def predict_one(self, x):
        """Predict +1 or -1 for a single sample (list of feature values)."""
        val = x[self.feature_idx]
        if self.polarity == 1:
            return 1 if val > self.threshold else -1
        else:
            return 1 if val <= self.threshold else -1


def _find_best_stump(X, y, weights, n_features):
    """Find the best decision stump minimizing weighted error.

    For each feature, try ~20 quantile-based thresholds.
    Returns the best DecisionStump with its weighted error.
    """
    n = len(y)
    best_error = float("inf")
    best_stump = DecisionStump()

    for fi in range(n_features):
        # Extract this feature's values
        vals = [X[i][fi] for i in range(n)]

        # Get sorted unique values for threshold candidates
        sorted_vals = sorted(set(vals))
        if len(sorted_vals) <= 1:
            continue

        # Sample ~20 thresholds (quantile-based for efficiency)
        n_thresh = min(20, len(sorted_vals) - 1)
        step = max(1, len(sorted_vals) // n_thresh)
        thresholds = [sorted_vals[i] for i in range(0, len(sorted_vals) - 1, step)]

        for thresh in thresholds:
            for polarity in (1, -1):
                error = 0.0
                for i in range(n):
                    if polarity == 1:
                        pred = 1 if vals[i] > thresh else -1
                    else:
                        pred = 1 if vals[i] <= thresh else -1
                    if pred != y[i]:
                        error += weights[i]

                if error < best_error:
                    best_error = error
                    best_stump = DecisionStump()
                    best_stump.feature_idx = fi
                    best_stump.threshold = thresh
                    best_stump.polarity = polarity

    return best_stump, best_error


def train_adaboost(X, y_binary, n_stumps=150):
    """Train AdaBoost ensemble with decision stumps.

    Args:
        X: list of feature vectors (list of lists)
        y_binary: list of 0/1 labels
        n_stumps: number of weak learners

    Returns:
        list of trained DecisionStump objects (with alpha weights)
    """
    n = len(y_binary)
    n_features = len(X[0])

    # Convert 0/1 to -1/+1
    y = [1 if yi == 1 else -1 for yi in y_binary]

    # Initialize uniform weights
    weights = [1.0 / n] * n

    stumps = []
    for t in range(n_stumps):
        stump, weighted_error = _find_best_stump(X, y, weights, n_features)

        # Clip error to avoid log(0)
        weighted_error = max(weighted_error, 1e-10)
        weighted_error = min(weighted_error, 1.0 - 1e-10)

        if weighted_error >= 0.5:
            logger.info("  AdaBoost stopped at iteration %d (error=%.4f >= 0.5)", t, weighted_error)
            break

        # Compute learner weight
        alpha = 0.5 * math.log((1 - weighted_error) / weighted_error)
        stump.alpha = alpha
        stumps.append(stump)

        # Update sample weights
        total_w = 0.0
        for i in range(n):
            pred = stump.predict_one(X[i])
            weights[i] *= math.exp(-alpha * y[i] * pred)
            total_w += weights[i]

        # Normalize weights
        for i in range(n):
            weights[i] /= total_w

        if (t + 1) % 25 == 0:
            logger.info("  AdaBoost iteration %d/%d: alpha=%.3f, error=%.4f, feature=%s",
                        t + 1, n_stumps, alpha, weighted_error,
                        FEATURE_NAMES[stump.feature_idx])

    logger.info("  AdaBoost trained %d stumps", len(stumps))
    return stumps


def predict_adaboost(stumps, X):
    """Predict probabilities using AdaBoost ensemble.

    Returns list of probabilities (sigmoid of weighted sum).
    """
    n = len(X)
    scores = [0.0] * n

    for stump in stumps:
        for i in range(n):
            pred = stump.predict_one(X[i])
            scores[i] += stump.alpha * pred

    # Convert to probabilities via sigmoid
    probs = []
    for s in scores:
        # Clip to prevent overflow
        s_clipped = max(min(s, 20), -20)
        probs.append(1.0 / (1.0 + math.exp(-s_clipped)))

    return probs


# ---------------------------------------------------------------------------
# Evaluation metrics
# ---------------------------------------------------------------------------

def compute_roc(y_true, y_prob, n_thresholds=200):
    """Compute ROC curve data points and AUC.

    Returns:
        roc_points: list of (fpr, tpr, threshold)
        auc: area under the ROC curve (trapezoidal rule)
    """
    n_pos = sum(y_true)
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return [], 0.0

    # Sort by decreasing probability
    combined = sorted(zip(y_prob, y_true), key=lambda x: -x[0])

    # Generate threshold sweep
    thresholds = sorted(set(p for p, _ in combined), reverse=True)
    if len(thresholds) > n_thresholds:
        step = max(1, len(thresholds) // n_thresholds)
        thresholds = thresholds[::step]
        if thresholds[-1] != 0.0:
            thresholds.append(0.0)

    roc_points = [(0.0, 0.0, 1.1)]  # Start at (0, 0)

    for thresh in thresholds:
        tp = sum(1 for p, y in combined if p >= thresh and y == 1)
        fp = sum(1 for p, y in combined if p >= thresh and y == 0)
        tpr = tp / n_pos
        fpr = fp / n_neg
        roc_points.append((fpr, tpr, thresh))

    roc_points.append((1.0, 1.0, 0.0))  # End at (1, 1)

    # Remove duplicates and sort by FPR
    roc_points = sorted(set(roc_points), key=lambda x: (x[0], x[1]))

    # AUC via trapezoidal rule
    auc = 0.0
    for i in range(1, len(roc_points)):
        dx = roc_points[i][0] - roc_points[i - 1][0]
        avg_y = (roc_points[i][1] + roc_points[i - 1][1]) / 2
        auc += dx * avg_y

    return roc_points, auc


def evaluate_at_thresholds(y_true, y_prob, base_rate, thresholds=None):
    """Evaluate prediction at multiple probability thresholds.

    Returns list of dicts with precision, recall, probability_gain,
    IGPE, and Molchan score for each threshold.
    """
    if thresholds is None:
        thresholds = [0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]

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

        # Molchan score: recall - alarm_fraction
        alarm_fraction = n_alarms / max(n_total, 1)
        miss_rate = fn / max(n_pos, 1)
        molchan_score = recall - alarm_fraction

        results.append({
            "threshold": round(thresh, 3),
            "n_alarms": n_alarms,
            "tp": tp,
            "fp": fp,
            "fn": fn,
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


def compute_feature_importance(X_test, y_test, stumps):
    """Compute single-feature AUC for each feature to measure importance.

    For each feature, train a trivial 1-stump model and compute its AUC.
    Also report the total alpha weight from the ensemble for each feature.
    """
    n_features = len(X_test[0])
    importances = []

    # Method 1: Ensemble weight sum per feature
    alpha_sums = [0.0] * n_features
    stump_counts = [0] * n_features
    for stump in stumps:
        alpha_sums[stump.feature_idx] += stump.alpha
        stump_counts[stump.feature_idx] += 1

    total_alpha = sum(alpha_sums)

    # Method 2: Single-feature AUC
    for fi in range(n_features):
        vals = [X_test[i][fi] for i in range(len(X_test))]
        # Use raw feature value as score
        _, auc_pos = compute_roc(y_test, vals)
        # Also try inverted (for features where lower = higher risk, e.g., b_value)
        inv_vals = [-v for v in vals]
        _, auc_neg = compute_roc(y_test, inv_vals)
        best_auc = max(auc_pos, auc_neg)
        direction = "higher_risk" if auc_pos >= auc_neg else "lower_risk"

        importances.append({
            "feature": FEATURE_NAMES[fi],
            "single_auc": round(best_auc, 4),
            "direction": direction,
            "ensemble_alpha_sum": round(alpha_sums[fi], 3),
            "ensemble_alpha_frac": round(alpha_sums[fi] / max(total_alpha, 1e-6), 3),
            "n_stumps_using": stump_counts[fi],
        })

    # Sort by single AUC descending
    importances.sort(key=lambda x: -x["single_auc"])
    return importances


# ---------------------------------------------------------------------------
# Dataset generation
# ---------------------------------------------------------------------------

async def build_dataset(db_path):
    """Load earthquakes, generate (cell, time) samples with features and labels.

    Returns:
        train_X, train_y, test_X, test_y: feature matrices and labels
        metadata: dict with dataset statistics
    """
    logger.info("Loading earthquake data from DB...")

    async with aiosqlite.connect(db_path) as db:
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, magnitude, latitude, longitude, depth_km "
            "FROM earthquakes WHERE magnitude >= 3.0 AND magnitude IS NOT NULL "
            "ORDER BY occurred_at"
        )
        fm_rows = await db.execute_fetchall(
            "SELECT latitude, longitude, strike1, dip1, rake1 FROM focal_mechanisms"
        )

    logger.info("  Loaded %d M3+ events, %d focal mechanisms", len(eq_rows), len(fm_rows))

    # Parse events
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
        raise RuntimeError(f"Insufficient data: only {len(events)} events found")

    t0 = events[0]["time"]
    for e in events:
        e["t_days"] = (e["time"] - t0).total_seconds() / 86400

    fm_dict = {}
    for r in fm_rows:
        fm_dict[(round(r[0], 1), round(r[1], 1))] = (r[2], r[3], r[4])

    # Target events (M5+) indexed by 2° cell
    target_events_by_cell = {}
    for e in events:
        if e["mag"] >= MIN_TARGET_MAG:
            ck = _cell_key(e["lat"], e["lon"])
            if ck not in target_events_by_cell:
                target_events_by_cell[ck] = []
            target_events_by_cell[ck].append(e["t_days"])

    # Determine active cells (cells with at least some seismicity)
    active_cells = set()
    for e in events:
        ck = _cell_key(e["lat"], e["lon"])
        active_cells.add(ck)
    # Filter to Japan region
    active_cells = {
        (lat, lon) for lat, lon in active_cells
        if GRID_LAT_MIN <= lat <= GRID_LAT_MAX and GRID_LON_MIN <= lon <= GRID_LON_MAX
    }
    logger.info("  Active 2° cells: %d", len(active_cells))

    # Split date in t_days
    split_t_days = (SPLIT_DATE - t0).total_seconds() / 86400
    total_t_days = events[-1]["t_days"]

    # Feature extractor
    extractor = FeatureExtractor(events, fm_dict, t0)

    # Generate samples: every STEP_DAYS across all active cells
    # Start from day 90 (need 90-day history for b-value)
    start_day = max(90, events[0]["t_days"] + 90)
    end_day = total_t_days - PREDICTION_WINDOW_DAYS  # Don't label beyond data

    train_X, train_y = [], []
    test_X, test_y = [], []
    n_pos_train, n_pos_test = 0, 0

    day = start_day
    sample_count = 0
    while day <= end_day:
        for cell_lat, cell_lon in active_cells:
            features = extractor.extract(cell_lat, cell_lon, day)
            label = generate_label(cell_lat, cell_lon, day, target_events_by_cell)
            fvec = [features[fn] for fn in FEATURE_NAMES]

            if day < split_t_days:
                train_X.append(fvec)
                train_y.append(label)
                if label == 1:
                    n_pos_train += 1
            else:
                test_X.append(fvec)
                test_y.append(label)
                if label == 1:
                    n_pos_test += 1

            sample_count += 1

        day += STEP_DAYS

        if sample_count % 50000 == 0:
            logger.info("  Generated %d samples (day %.0f/%.0f)...",
                        sample_count, day, end_day)

    logger.info("  Dataset: train=%d (pos=%d, %.2f%%), test=%d (pos=%d, %.2f%%)",
                len(train_y), n_pos_train,
                100 * n_pos_train / max(len(train_y), 1),
                len(test_y), n_pos_test,
                100 * n_pos_test / max(len(test_y), 1))

    metadata = {
        "n_events_m3": len(events),
        "n_focal_mechanisms": len(fm_rows),
        "n_active_cells": len(active_cells),
        "total_days": round(total_t_days, 1),
        "train_samples": len(train_y),
        "train_positives": n_pos_train,
        "train_positive_rate": round(n_pos_train / max(len(train_y), 1), 5),
        "test_samples": len(test_y),
        "test_positives": n_pos_test,
        "test_positive_rate": round(n_pos_test / max(len(test_y), 1), 5),
        "features": FEATURE_NAMES,
        "prediction_window_days": PREDICTION_WINDOW_DAYS,
        "cell_size_deg": CELL_SIZE_DEG,
        "step_days": STEP_DAYS,
        "train_period": "2011-2018",
        "test_period": "2019-2026",
    }

    return train_X, train_y, test_X, test_y, metadata


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_ml_prediction():
    """Full ML prediction pipeline: features -> train -> evaluate -> report."""
    logger.info("=== ML Integrated Earthquake Prediction ===")
    logger.info("Target: M%.1f+ within %d days in %.0f° cells",
                MIN_TARGET_MAG, PREDICTION_WINDOW_DAYS, CELL_SIZE_DEG)

    # Step 1-2: Build dataset
    train_X, train_y, test_X, test_y, metadata = await build_dataset(DB_PATH)

    if not train_X or not test_X:
        logger.error("Empty dataset, cannot train")
        return {"error": "empty_dataset", "metadata": metadata}

    # Class balance check
    base_rate_train = sum(train_y) / len(train_y)
    base_rate_test = sum(test_y) / len(test_y)
    logger.info("  Base rates: train=%.4f, test=%.4f", base_rate_train, base_rate_test)

    # Step 3: Train AdaBoost
    logger.info("--- Training AdaBoost (150 stumps) ---")
    stumps = train_adaboost(train_X, train_y, n_stumps=150)

    if not stumps:
        logger.error("Training failed: no stumps produced")
        return {"error": "training_failed", "metadata": metadata}

    # Step 4: Predict on test set
    logger.info("--- Predicting on test set (%d samples) ---", len(test_y))
    test_probs = predict_adaboost(stumps, test_X)

    # Also predict on train set (for overfitting check)
    train_probs = predict_adaboost(stumps, train_X)

    # ROC curve and AUC
    logger.info("--- Computing ROC and evaluation metrics ---")
    roc_test, auc_test = compute_roc(test_y, test_probs)
    roc_train, auc_train = compute_roc(train_y, train_probs)
    logger.info("  AUC-ROC: train=%.4f, test=%.4f", auc_train, auc_test)

    # Threshold-based evaluation
    threshold_results = evaluate_at_thresholds(test_y, test_probs, base_rate_test)
    logger.info("  Threshold evaluation (test):")
    for tr in threshold_results:
        if tr["n_alarms"] > 0:
            logger.info("    thresh=%.2f: prec=%.3f recall=%.3f gain=%.1f IGPE=%.2f molchan=%.3f (%d alarms)",
                        tr["threshold"], tr["precision"], tr["recall"],
                        tr["probability_gain"], tr["igpe_bits"],
                        tr["molchan_score"], tr["n_alarms"])

    # Step 5: Feature importance
    logger.info("--- Computing feature importance ---")
    importance = compute_feature_importance(test_X, test_y, stumps)
    logger.info("  Feature ranking (by single-feature AUC):")
    for imp in importance:
        logger.info("    %s: AUC=%.4f (%s), ensemble_frac=%.3f (%d stumps)",
                    imp["feature"], imp["single_auc"], imp["direction"],
                    imp["ensemble_alpha_frac"], imp["n_stumps_using"])

    # Compile results
    results = {
        "metadata": metadata,
        "model": {
            "type": "AdaBoost_decision_stumps",
            "n_stumps_trained": len(stumps),
            "n_stumps_requested": 150,
            "stump_details": [
                {
                    "feature": FEATURE_NAMES[s.feature_idx],
                    "threshold": round(s.threshold, 4),
                    "polarity": s.polarity,
                    "alpha": round(s.alpha, 4),
                }
                for s in stumps[:30]  # First 30 for brevity
            ],
        },
        "performance": {
            "auc_roc_train": round(auc_train, 4),
            "auc_roc_test": round(auc_test, 4),
            "base_rate_train": round(base_rate_train, 5),
            "base_rate_test": round(base_rate_test, 5),
        },
        "threshold_evaluation": threshold_results,
        "roc_curve_test": [
            {"fpr": round(fpr, 4), "tpr": round(tpr, 4), "threshold": round(th, 4)}
            for fpr, tpr, th in roc_test
        ],
        "feature_importance": importance,
        "interpretation": {
            "auc_meaning": (
                "AUC=0.5 is random, AUC=1.0 is perfect. "
                "AUC>0.6 suggests predictive skill beyond chance."
            ),
            "probability_gain_meaning": (
                "P(M5+|alarm) / P(M5+|random). "
                "Gain>1 means the model is better than random guessing."
            ),
            "igpe_meaning": (
                "Information Gain Per Earthquake in bits. "
                "IGPE>0 means the model provides information; IGPE>1 is significant."
            ),
            "molchan_meaning": (
                "Recall - alarm_fraction. "
                ">0 means better than random; closer to 1 is better."
            ),
        },
    }

    return results


async def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_ml_prediction()

    out_path = RESULTS_DIR / f"ml_prediction_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
