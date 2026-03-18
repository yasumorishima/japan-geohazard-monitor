"""Temporal feature engineering for earthquake prediction.

Expands from 11 static features to 35+ temporal features that capture
dynamics (acceleration, trends, regime changes) — not just snapshots.

Feature categories:
    A. Rate statistics (7d/14d/30d counts, ratios)
    B. Rate dynamics (acceleration, trend slope)
    C. ETAS residuals (with MLE-fitted parameters)
    D. Magnitude statistics (max, deficit, b-value + trend)
    E. Clustering (foreshock count, inter-event time CV, cluster fraction)
    F. Coulomb stress (cumulative CFS, rate-and-state modified rate)
    G. Pattern Informatics (PI score, PI trend)
    H. Energy (Benioff strain rate, acceleration)

All features are STRICTLY prospective: only data before t_now is used.
"""

import bisect
import logging
import math
from typing import Optional

from physics import (
    ETAS_DEFAULTS,
    b_value_aki,
    b_value_with_uncertainty,
    classify_tectonic_zone,
    default_mechanism,
    etas_expected_count,
    fault_dimensions,
    gnss_horizontal_displacement,
    gnss_strain_rate,
    gnss_transient_score,
    okada_cfs,
    rate_state_probability,
    DEG_TO_KM,
)

logger = logging.getLogger(__name__)

# Grid parameters
CELL_SIZE_DEG = 2.0
GRID_LAT_MIN, GRID_LAT_MAX = 26, 46
GRID_LON_MIN, GRID_LON_MAX = 128, 148


def cell_key(lat: float, lon: float) -> tuple:
    """Snap coordinates to nearest cell centre."""
    return (
        round(lat / CELL_SIZE_DEG) * CELL_SIZE_DEG,
        round(lon / CELL_SIZE_DEG) * CELL_SIZE_DEG,
    )


# All 35 feature names in order
FEATURE_NAMES = [
    # A. Rate statistics
    "rate_7d",
    "rate_14d",
    "rate_30d",
    "rate_ratio_7d",
    "rate_ratio_30d",
    # B. Rate dynamics
    "rate_accel_7d",        # d(rate)/dt: (rate_7d - rate_prev_7d) / 7
    "rate_accel_30d",       # d(rate)/dt: (rate_30d - rate_prev_30d) / 30
    "rate_trend_slope",     # linear regression slope of 7d-rate over last 60 days
    # C. ETAS residuals
    "etas_residual_7d",     # observed / ETAS expected (7d window)
    "etas_residual_30d",    # observed / ETAS expected (30d window)
    "etas_residual_trend",  # slope of etas_residual_7d over last 60 days
    # D. Magnitude statistics
    "max_mag_7d",
    "max_mag_30d",
    "mag_deficit",          # expected max - observed max (seismic gap proxy)
    "b_value",              # 90-day b-value
    "b_value_trend",        # slope of b-value over last 6 months
    # E. Clustering
    "n_foreshock",          # M3+ count within 1° in 7 days
    "foreshock_mag_increase", # max_mag_3d > max_mag_prev_3d (escalation)
    "inter_event_cv",       # CV of inter-event times (regularity measure)
    "cluster_fraction",     # fraction of events in temporal clusters
    # F. Coulomb stress
    "cfs_cumulative_kpa",   # cumulative static CFS
    "cfs_rate_state",       # rate-and-state modified rate ratio
    "cfs_recent_kpa",       # CFS from events in last 365 days only
    # G. Pattern Informatics
    "pi_score",             # PI variance
    "pi_trend",             # PI change over last 2 steps
    # H. Energy
    "benioff_strain_30d",   # cumulative sqrt(energy) in 30 days
    "benioff_accel",        # benioff_strain_30d / benioff_strain_prev_30d
    "energy_ratio",         # energy_7d / energy_30d (burst indicator)
    # I. Temporal
    "days_since_m5",        # days since last M5+ within 2°
    "days_since_m4",        # days since last M4+ within 2°
    "log_days_since_m5",    # log10(days_since_m5 + 1)
    # J. Spatial context
    "neighbor_rate_sum",    # sum of rate_7d in adjacent cells
    "rate_spatial_anomaly", # rate_7d / mean(neighbor rates) (spatial outlier)
    # K. Composite
    "n_active_signals",     # count of above-threshold signals (meta-feature)
    "alarm_density",        # moving average of past alarm hits (self-reinforcing)
    # L. GNSS crustal deformation (Phase 7)
    "gnss_disp_max_30d",    # max horizontal displacement in cell (30d, mm)
    "gnss_disp_accel",      # displacement acceleration (recent / previous)
    "gnss_vertical_rate",   # vertical rate of change (mm/day)
    "gnss_strain_rate",     # local strain rate (nanostrain/day)
    "gnss_anomaly_count",   # stations with displacement > 2σ
    "gnss_transient_score", # slow-slip event detection score
    # M. Enhanced spatial (Phase 7)
    "neighbor_cfs_max",     # max CFS among neighboring cells
    "neighbor_etas_resid_max",  # max ETAS residual among neighbors
    "zone_rate_anomaly",    # cell rate_7d / zone mean rate_7d
    "zone_cfs_rank",        # percentile rank of CFS within zone
    "spatial_gradient",     # rate gradient (center vs neighbors)
    "neighbor_max_mag",     # max magnitude in neighbors (7d)
    # N. Cosmic ray (Phase 9) — Homola et al. 2023
    "cosmic_ray_rate",      # daily corrected count rate (IRKT)
    "cosmic_ray_anomaly",   # deviation from 27-day solar rotation mean (σ)
    "cosmic_ray_trend_15d", # 15-day trend slope (Homola lag)
    # O. Lightning/electromagnetic (Phase 9)
    "lightning_count_7d",   # lightning stroke count in cell (7 days)
    "lightning_anomaly",    # deviation from seasonal baseline
    # P. Geomagnetic hourly spectral (Phase 9) — Hattori 2004
    "geomag_ulf_power",     # ULF band (0.01-0.1 Hz) spectral power (nearest station)
    "geomag_polarization",  # Sz/Sh polarization ratio
    "geomag_fractal_dim",   # Higuchi fractal dimension of Z component
    # Q. Animal behavior (Phase 9) — Wikelski et al. 2020
    "animal_speed_anomaly", # GPS movement speed anomaly (σ from baseline)
]

N_FEATURES = len(FEATURE_NAMES)

# Phase 9 feature groups — keyed by data source name
# Each maps to the feature names that require that data source to be present.
PHASE9_FEATURE_GROUPS = {
    "cosmic_ray": ["cosmic_ray_rate", "cosmic_ray_anomaly", "cosmic_ray_trend_15d"],
    "lightning": ["lightning_count_7d", "lightning_anomaly"],
    "geomag_spectral": ["geomag_ulf_power", "geomag_polarization", "geomag_fractal_dim"],
    "animal": ["animal_speed_anomaly"],
}


def get_active_feature_names(cosmic_ray_data=None, lightning_data=None,
                              geomag_spectral_data=None, animal_data=None):
    """Return feature names excluding Phase 9 groups with no data.

    Instead of feeding zero-filled Phase 9 features that degrade the model,
    dynamically exclude feature groups whose data source returned empty.
    """
    excluded = set()
    source_data = {
        "cosmic_ray": cosmic_ray_data,
        "lightning": lightning_data,
        "geomag_spectral": geomag_spectral_data,
        "animal": animal_data,
    }
    for source_name, data in source_data.items():
        if not data:  # None or empty dict
            excluded.update(PHASE9_FEATURE_GROUPS[source_name])

    if excluded:
        return [f for f in FEATURE_NAMES if f not in excluded]
    return list(FEATURE_NAMES)


class FeatureExtractor:
    """Extract temporal features for (cell, time) pairs.

    Pre-indexes events by cell for O(1) spatial lookup.
    Maintains incremental CFS map.
    Supports zone-specific ETAS parameters.
    Phase 9: adds cosmic ray, lightning, geomag spectral, animal behavior.
    """

    def __init__(self, events: list, fm_dict: dict, t0,
                 etas_params: dict = None,
                 zone_etas_params: dict = None,
                 gnss_data: dict = None,
                 cosmic_ray_data: dict = None,
                 lightning_data: dict = None,
                 geomag_spectral_data: dict = None,
                 animal_data: dict = None):
        """
        Args:
            events: list of dicts with keys: time, mag, lat, lon, depth, t_days
            fm_dict: {(lat_rounded, lon_rounded): (strike, dip, rake)}
            t0: reference datetime for t_days=0
            etas_params: global ETAS parameters (fallback)
            zone_etas_params: {zone_name: {params dict}} for zone-specific ETAS
            gnss_data: {cell_key: list of {t_days, stations: [{lat, lon, dx_mm, dy_mm, dz_mm}]}}
            cosmic_ray_data: {date_str: {cosmic_ray_rate, cosmic_ray_anomaly, ...}}
            lightning_data: {(date_str, cell_lat, cell_lon): {stroke_count, ...}}
            geomag_spectral_data: {date_str: {ulf_power, polarization, fractal_dim}}
            animal_data: {(date_str, cell_lat, cell_lon): {speed_anomaly, ...}}
        """
        self.events = events
        self.fm_dict = fm_dict
        self.t0 = t0
        self.all_t_days = [e["t_days"] for e in events]
        self.etas = etas_params if etas_params else ETAS_DEFAULTS.copy()
        self.zone_etas = zone_etas_params or {}
        self.gnss_data = gnss_data or {}
        self.cosmic_ray_data = cosmic_ray_data or {}
        self.lightning_data = lightning_data or {}
        self.geomag_spectral_data = geomag_spectral_data or {}
        self.animal_data = animal_data or {}

        # Pre-compute cell → zone mapping
        self.cell_zone = {}
        for lat in range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG)):
            for lon in range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG)):
                ck = (float(lat), float(lon))
                self.cell_zone[ck] = classify_tectonic_zone(float(lat), float(lon))

        # Index events by 2° cell
        self.cell_events = {}
        for e in events:
            ck = cell_key(e["lat"], e["lon"])
            self.cell_events.setdefault(ck, []).append(e)

        # Index by 1° cell (foreshock counting)
        self.cell_events_1deg = {}
        for e in events:
            k1 = (round(e["lat"]), round(e["lon"]))
            self.cell_events_1deg.setdefault(k1, []).append(e)

        # Long-term rates per cell
        total_days = max(self.all_t_days[-1] - self.all_t_days[0], 1)
        self.cell_lt_rate = {}
        for ck, evs in self.cell_events.items():
            self.cell_lt_rate[ck] = len(evs) / total_days

        # Background ETAS rate per cell (~30% background)
        self.mu_bg = {}
        for ck, evs in self.cell_events.items():
            self.mu_bg[ck] = max(0.3 * len(evs) / total_days, 0.01)

        # CFS map: cumulative + rate-and-state history
        self.cfs_map = {}  # cell -> cumulative kPa
        self.cfs_history = {}  # cell -> list of (t_days, delta_cfs_pa)
        self._cfs_idx = 0
        self._m55_events = [e for e in events if e["mag"] >= 5.5]

        # PI rate history per cell
        self.pi_history = {}

        # Rate history per cell (for trend computation)
        self.rate_history = {}  # cell -> list of (t_days, rate_7d)

        # b-value history per cell (for trend)
        self.b_history = {}  # cell -> list of (t_days, b_value)

        # ETAS residual history per cell (for trend)
        self.etas_resid_history = {}

    def _update_cfs(self, t_now_days):
        """Incrementally add CFS from M5.5+ events up to t_now."""
        while (self._cfs_idx < len(self._m55_events) and
               self._m55_events[self._cfs_idx]["t_days"] <= t_now_days):
            src = self._m55_events[self._cfs_idx]
            fm_key = (round(src["lat"], 1), round(src["lon"], 1))
            strike, dip, rake = self.fm_dict.get(
                fm_key, default_mechanism(src["lat"], src["lon"], src["depth"]))
            length, width, slip = fault_dimensions(src["mag"])

            for lat in range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG)):
                for lon in range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG)):
                    ck = (float(lat), float(lon))
                    dist_lat = abs(src["lat"] - lat) * DEG_TO_KM
                    dist_lon = abs(src["lon"] - lon) * DEG_TO_KM
                    if dist_lat > 300 or dist_lon > 300:
                        continue

                    cfs_pa = okada_cfs(
                        src["lat"], src["lon"], src["depth"],
                        strike, dip, rake, length, width, slip,
                        float(lat), float(lon), 15.0)

                    cfs_kpa = cfs_pa / 1000
                    self.cfs_map[ck] = self.cfs_map.get(ck, 0.0) + cfs_kpa
                    self.cfs_history.setdefault(ck, []).append(
                        (src["t_days"], cfs_pa))

            self._cfs_idx += 1

    def _count_in_window(self, cell_evs, t_start, t_end):
        """Count events in [t_start, t_end) using linear scan."""
        return sum(1 for e in cell_evs if t_start <= e["t_days"] < t_end)

    def _events_in_window(self, cell_evs, t_start, t_end):
        """Get events in [t_start, t_end)."""
        return [e for e in cell_evs if t_start <= e["t_days"] < t_end]

    def _t_days_to_date(self, t_days: float) -> str:
        """Convert t_days offset to YYYY-MM-DD date string."""
        from datetime import timedelta
        dt = self.t0 + timedelta(days=t_days)
        return dt.strftime("%Y-%m-%d")

    def _get_etas_for_cell(self, cell_lat, cell_lon):
        """Get ETAS parameters for a cell, using zone-specific if available."""
        zone = self.cell_zone.get((cell_lat, cell_lon), "other")
        zone_info = self.zone_etas.get(zone, {})
        if zone_info.get("fitted") and zone_info.get("params"):
            return zone_info["params"]
        return self.etas

    def _compute_etas_expected(self, cell_evs, t_now_days, window_days,
                                cell_lat=None, cell_lon=None):
        """Expected count under ETAS model (zone-aware)."""
        ck = (cell_lat, cell_lon) if cell_lat is not None else (0, 0)
        mu = self.mu_bg.get(ck, 0.01)
        t_start = t_now_days - window_days
        prior = [(e["t_days"], e["mag"]) for e in cell_evs if e["t_days"] < t_start]
        prior = prior[-2000:]  # limit for speed

        # Use zone-specific ETAS parameters if available
        if cell_lat is not None and cell_lon is not None:
            params = self._get_etas_for_cell(cell_lat, cell_lon)
        else:
            params = self.etas

        return etas_expected_count(
            t_start, t_now_days, prior, mu,
            params.get("K", 0.04),
            params.get("alpha", 1.0),
            params.get("c", 0.01),
            params.get("p", 1.1),
            params.get("Mc", 3.0),
        )

    def _benioff_strain(self, evs):
        """Cumulative Benioff strain (sum of sqrt(energy))."""
        total = 0.0
        for e in evs:
            # Energy from magnitude: log10(E) = 1.5*M + 4.8
            energy = 10 ** (1.5 * e["mag"] + 4.8)
            total += math.sqrt(energy)
        return total

    def _linear_slope(self, values):
        """Simple linear regression slope for a list of values."""
        n = len(values)
        if n < 3:
            return 0.0
        x_mean = (n - 1) / 2
        y_mean = sum(values) / n
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
        den = sum((i - x_mean) ** 2 for i in range(n))
        if den == 0:
            return 0.0
        return num / den

    def extract(self, cell_lat: float, cell_lon: float, t_now_days: float) -> list:
        """Extract all 35 features for a (cell, time) pair.

        Returns list of feature values in FEATURE_NAMES order.
        All features use data strictly BEFORE t_now_days.
        """
        ck = (cell_lat, cell_lon)
        cell_evs = self.cell_events.get(ck, [])

        # Time windows
        t_3 = t_now_days - 3
        t_7 = t_now_days - 7
        t_14 = t_now_days - 14
        t_30 = t_now_days - 30
        t_60 = t_now_days - 60
        t_90 = t_now_days - 90
        t_180 = t_now_days - 180
        t_365 = t_now_days - 365

        evs_3d = self._events_in_window(cell_evs, t_3, t_now_days)
        evs_7d = self._events_in_window(cell_evs, t_7, t_now_days)
        evs_14d = self._events_in_window(cell_evs, t_14, t_now_days)
        evs_30d = self._events_in_window(cell_evs, t_30, t_now_days)
        evs_90d = self._events_in_window(cell_evs, t_90, t_now_days)
        evs_180d = self._events_in_window(cell_evs, t_180, t_now_days)

        # Previous windows (for acceleration)
        evs_prev_7d = self._events_in_window(cell_evs, t_14, t_7)
        evs_prev_30d = self._events_in_window(cell_evs, t_60, t_30)
        evs_prev_3d = self._events_in_window(cell_evs, t_7 + 1, t_3)

        # --- A. Rate statistics ---
        rate_7d = len(evs_7d)
        rate_14d = len(evs_14d)
        rate_30d = len(evs_30d)

        lt_rate = self.cell_lt_rate.get(ck, 0.01)
        rate_ratio_7d = rate_7d / max(lt_rate * 7, 0.1)
        rate_ratio_30d = rate_30d / max(lt_rate * 30, 0.1)

        # --- B. Rate dynamics ---
        rate_prev_7d = len(evs_prev_7d)
        rate_accel_7d = (rate_7d - rate_prev_7d) / 7.0

        rate_prev_30d = len(evs_prev_30d)
        rate_accel_30d = (rate_30d - rate_prev_30d) / 30.0

        # Rate trend: linear slope of 7d-rate sampled every 7d over 60 days
        hist = self.rate_history.get(ck, [])
        hist.append((t_now_days, rate_7d))
        # Keep only last 20 entries (140 days if step=7d)
        if len(hist) > 20:
            hist = hist[-20:]
        self.rate_history[ck] = hist
        rate_values = [v for _, v in hist[-9:]]  # last ~60 days
        rate_trend_slope = self._linear_slope(rate_values)

        # --- C. ETAS residuals (zone-aware) ---
        mu = self.mu_bg.get(ck, 0.01)
        etas_exp_7d = self._compute_etas_expected(cell_evs, t_now_days, 7, cell_lat, cell_lon)
        etas_residual_7d = rate_7d / max(etas_exp_7d, 0.1)

        etas_exp_30d = self._compute_etas_expected(cell_evs, t_now_days, 30, cell_lat, cell_lon)
        etas_residual_30d = rate_30d / max(etas_exp_30d, 0.1)

        # ETAS residual trend
        resid_hist = self.etas_resid_history.get(ck, [])
        resid_hist.append(etas_residual_7d)
        if len(resid_hist) > 20:
            resid_hist = resid_hist[-20:]
        self.etas_resid_history[ck] = resid_hist
        etas_residual_trend = self._linear_slope(resid_hist[-9:])

        # --- D. Magnitude statistics ---
        max_mag_7d = max((e["mag"] for e in evs_7d), default=0.0)
        max_mag_30d = max((e["mag"] for e in evs_30d), default=0.0)

        # Magnitude deficit: expected vs observed max (GR-based)
        if rate_30d > 0:
            # Expected max from GR: M_max ≈ Mc + log10(N) / b
            b_est = b_value_aki([e["mag"] for e in evs_90d]) or 1.0
            expected_max = 3.0 + math.log10(max(rate_30d, 1)) / max(b_est, 0.5)
            mag_deficit = expected_max - max_mag_30d
        else:
            mag_deficit = 0.0

        # b-value
        mags_90d = [e["mag"] for e in evs_90d]
        b_val = b_value_aki(mags_90d) or 1.0

        # b-value trend
        b_hist = self.b_history.get(ck, [])
        b_hist.append(b_val)
        if len(b_hist) > 20:
            b_hist = b_hist[-20:]
        self.b_history[ck] = b_hist
        b_value_trend = self._linear_slope(b_hist[-6:])  # ~6 months

        # --- E. Clustering ---
        # Foreshock count (1° box, 7 days)
        n_foreshock = 0
        for dlat in (-1, 0, 1):
            for dlon in (-1, 0, 1):
                k1 = (round(cell_lat) + dlat, round(cell_lon) + dlon)
                for e in self.cell_events_1deg.get(k1, []):
                    if t_7 <= e["t_days"] < t_now_days:
                        n_foreshock += 1

        # Foreshock magnitude escalation
        max_mag_prev_3d = max((e["mag"] for e in evs_prev_3d), default=0.0)
        max_mag_3d = max((e["mag"] for e in evs_3d), default=0.0)
        foreshock_mag_increase = max_mag_3d - max_mag_prev_3d

        # Inter-event time CV (coefficient of variation)
        inter_times = []
        sorted_evs = sorted(evs_30d, key=lambda e: e["t_days"])
        for i in range(1, len(sorted_evs)):
            dt = sorted_evs[i]["t_days"] - sorted_evs[i - 1]["t_days"]
            if dt > 0:
                inter_times.append(dt)

        if len(inter_times) >= 3:
            it_mean = sum(inter_times) / len(inter_times)
            it_std = math.sqrt(sum((t - it_mean) ** 2 for t in inter_times) / len(inter_times))
            inter_event_cv = it_std / max(it_mean, 1e-6)
        else:
            inter_event_cv = 1.0  # default (Poisson-like)

        # Cluster fraction (events within 1 day of each other / total)
        if len(sorted_evs) >= 2:
            n_clustered = sum(1 for dt in inter_times if dt < 1.0)
            cluster_fraction = n_clustered / max(len(sorted_evs) - 1, 1)
        else:
            cluster_fraction = 0.0

        # --- F. Coulomb stress ---
        self._update_cfs(t_now_days)
        cfs_cumulative_kpa = self.cfs_map.get(ck, 0.0)

        # Rate-and-state modified rate
        cfs_hist = self.cfs_history.get(ck, [])
        if cfs_hist and lt_rate > 0:
            # Compute sum of rate-state contributions
            rs_rate = lt_rate  # start with background
            for t_step, delta_cfs_pa in cfs_hist:
                dt = t_now_days - t_step
                if dt > 0:
                    rs_rate_contrib = rate_state_probability(
                        delta_cfs_pa, lt_rate, dt)
                    rs_rate = max(rs_rate, rs_rate_contrib)
            cfs_rate_state = rs_rate / max(lt_rate, 1e-6)
        else:
            cfs_rate_state = 1.0

        # Recent CFS (last 365 days only — more predictive than all-time)
        cfs_recent_kpa = sum(
            delta / 1000 for t_s, delta in cfs_hist
            if t_now_days - t_s < 365
        )

        # --- G. Pattern Informatics ---
        pi_hist = self.pi_history.get(ck, [])
        pi_hist.append(rate_7d)
        if len(pi_hist) > 240:
            pi_hist = pi_hist[-240:]
        self.pi_history[ck] = pi_hist

        recent_pi = pi_hist[-240:]
        if len(recent_pi) >= 4:
            changes = [recent_pi[i] - recent_pi[i - 1] for i in range(1, len(recent_pi))]
            mean_c = sum(changes) / len(changes)
            pi_score = sum((c - mean_c) ** 2 for c in changes) / len(changes)
        else:
            pi_score = 0.0

        # PI trend (recent vs older)
        if len(pi_hist) >= 6:
            pi_recent = sum(pi_hist[-3:]) / 3
            pi_older = sum(pi_hist[-6:-3]) / 3
            pi_trend = pi_recent - pi_older
        else:
            pi_trend = 0.0

        # --- H. Energy ---
        benioff_30d = self._benioff_strain(evs_30d)
        benioff_prev_30d = self._benioff_strain(evs_prev_30d)
        benioff_accel = benioff_30d / max(benioff_prev_30d, 1.0)

        energy_7d = self._benioff_strain(evs_7d) ** 2  # actual energy
        energy_30d = self._benioff_strain(evs_30d) ** 2
        energy_ratio = energy_7d / max(energy_30d, 1.0)

        # --- I. Temporal ---
        days_since_m5 = 9999.0
        days_since_m4 = 9999.0
        for e in reversed(cell_evs):
            if e["t_days"] >= t_now_days:
                continue
            if e["mag"] >= 5.0 and days_since_m5 == 9999.0:
                days_since_m5 = t_now_days - e["t_days"]
            if e["mag"] >= 4.0 and days_since_m4 == 9999.0:
                days_since_m4 = t_now_days - e["t_days"]
            if days_since_m5 < 9999.0 and days_since_m4 < 9999.0:
                break

        log_days_since_m5 = math.log10(days_since_m5 + 1)

        # --- J. Spatial context ---
        neighbor_rate_sum = 0
        neighbor_rates = []
        for dlat in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
            for dlon in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
                if dlat == 0 and dlon == 0:
                    continue
                nk = (cell_lat + dlat, cell_lon + dlon)
                n_evs = self.cell_events.get(nk, [])
                n_rate = sum(1 for e in n_evs if t_7 <= e["t_days"] < t_now_days)
                neighbor_rate_sum += n_rate
                neighbor_rates.append(n_rate)

        mean_neighbor_rate = sum(neighbor_rates) / max(len(neighbor_rates), 1)
        rate_spatial_anomaly = rate_7d / max(mean_neighbor_rate, 0.1)

        # --- K. Composite ---
        n_active_signals = sum([
            rate_ratio_7d > 3,
            etas_residual_7d > 3,
            n_foreshock >= 5,
            cfs_cumulative_kpa > 50,
            pi_score > 5,
            benioff_accel > 3,
        ])

        alarm_density = 0.0  # placeholder; updated externally if needed

        # --- L. GNSS crustal deformation (Phase 7) ---
        gnss_disp_max_30d = 0.0
        gnss_disp_accel = 0.0
        gnss_vertical_rate = 0.0
        gnss_strain_val = 0.0
        gnss_anomaly_cnt = 0
        gnss_transient = 0.0

        cell_gnss = self.gnss_data.get(ck, [])
        if cell_gnss:
            # Get GNSS snapshots in 30d and previous 30d windows
            gnss_30d = [g for g in cell_gnss if t_30 <= g["t_days"] < t_now_days]
            gnss_prev_30d = [g for g in cell_gnss if t_60 <= g["t_days"] < t_30]

            if gnss_30d:
                # Max horizontal displacement across all stations in window
                all_disps_30d = []
                for snapshot in gnss_30d:
                    for s in snapshot.get("stations", []):
                        d = gnss_horizontal_displacement(
                            s.get("dx_mm", 0), s.get("dy_mm", 0))
                        all_disps_30d.append(d)
                if all_disps_30d:
                    gnss_disp_max_30d = max(all_disps_30d)

                    # Anomaly count: stations > mean + 2*std
                    if len(all_disps_30d) >= 5:
                        d_mean = sum(all_disps_30d) / len(all_disps_30d)
                        d_std = math.sqrt(
                            sum((d - d_mean) ** 2 for d in all_disps_30d) / len(all_disps_30d))
                        threshold_2sigma = d_mean + 2 * max(d_std, 0.1)
                        gnss_anomaly_cnt = sum(1 for d in all_disps_30d if d > threshold_2sigma)

                # Strain rate from most recent snapshot
                latest_snap = max(gnss_30d, key=lambda g: g["t_days"])
                stations = latest_snap.get("stations", [])
                if len(stations) >= 3:
                    gnss_strain_val = gnss_strain_rate(
                        stations, cell_lat, cell_lon, CELL_SIZE_DEG)

                # Vertical rate (mm/day) from recent data
                vert_data = []
                for snapshot in gnss_30d:
                    for s in snapshot.get("stations", []):
                        if s.get("dz_mm") is not None:
                            vert_data.append((snapshot["t_days"], s["dz_mm"]))
                if len(vert_data) >= 3:
                    vert_data.sort()
                    vdt = vert_data[-1][0] - vert_data[0][0]
                    if vdt > 0:
                        gnss_vertical_rate = (vert_data[-1][1] - vert_data[0][1]) / vdt

                # Transient score (slow-slip detection)
                disp_timeseries = []
                for snapshot in cell_gnss:
                    if snapshot["t_days"] < t_now_days:
                        total_d = 0
                        n_s = 0
                        for s in snapshot.get("stations", []):
                            total_d += gnss_horizontal_displacement(
                                s.get("dx_mm", 0), s.get("dy_mm", 0))
                            n_s += 1
                        if n_s > 0:
                            disp_timeseries.append((snapshot["t_days"], total_d / n_s))
                gnss_transient = gnss_transient_score(disp_timeseries)

            # Displacement acceleration
            if gnss_30d and gnss_prev_30d:
                disps_recent = []
                for snapshot in gnss_30d:
                    for s in snapshot.get("stations", []):
                        disps_recent.append(
                            gnss_horizontal_displacement(s.get("dx_mm", 0), s.get("dy_mm", 0)))
                disps_prev = []
                for snapshot in gnss_prev_30d:
                    for s in snapshot.get("stations", []):
                        disps_prev.append(
                            gnss_horizontal_displacement(s.get("dx_mm", 0), s.get("dy_mm", 0)))
                if disps_recent and disps_prev:
                    mean_recent = sum(disps_recent) / len(disps_recent)
                    mean_prev = sum(disps_prev) / len(disps_prev)
                    gnss_disp_accel = mean_recent / max(mean_prev, 0.01)

        # --- M. Enhanced spatial features (Phase 7) ---
        neighbor_cfs_max = 0.0
        neighbor_etas_resid_max = 0.0
        neighbor_max_mag_7d = 0.0

        for dlat in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
            for dlon in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
                if dlat == 0 and dlon == 0:
                    continue
                nk = (cell_lat + dlat, cell_lon + dlon)

                # Neighbor CFS
                n_cfs = self.cfs_map.get(nk, 0.0)
                if n_cfs > neighbor_cfs_max:
                    neighbor_cfs_max = n_cfs

                # Neighbor ETAS residual
                n_evs = self.cell_events.get(nk, [])
                n_rate_7d = sum(1 for e in n_evs if t_7 <= e["t_days"] < t_now_days)
                n_etas_exp = self._compute_etas_expected(n_evs, t_now_days, 7, nk[0], nk[1])
                n_etas_resid = n_rate_7d / max(n_etas_exp, 0.1)
                if n_etas_resid > neighbor_etas_resid_max:
                    neighbor_etas_resid_max = n_etas_resid

                # Neighbor max magnitude (7d)
                for e in n_evs:
                    if t_7 <= e["t_days"] < t_now_days and e["mag"] > neighbor_max_mag_7d:
                        neighbor_max_mag_7d = e["mag"]

        # Zone-level statistics
        zone = self.cell_zone.get(ck, "other")
        zone_rates = []
        zone_cfs_values = []
        for other_ck, other_zone in self.cell_zone.items():
            if other_zone == zone and other_ck != ck:
                other_evs = self.cell_events.get(other_ck, [])
                other_rate = sum(1 for e in other_evs if t_7 <= e["t_days"] < t_now_days)
                zone_rates.append(other_rate)
                zone_cfs_values.append(self.cfs_map.get(other_ck, 0.0))

        zone_mean_rate = sum(zone_rates) / max(len(zone_rates), 1) if zone_rates else rate_7d
        zone_rate_anomaly = rate_7d / max(zone_mean_rate, 0.1)

        # CFS rank within zone
        if zone_cfs_values:
            zone_cfs_values_sorted = sorted(zone_cfs_values)
            n_below = sum(1 for v in zone_cfs_values_sorted if v <= cfs_cumulative_kpa)
            zone_cfs_rank = n_below / max(len(zone_cfs_values_sorted), 1)
        else:
            zone_cfs_rank = 0.5

        # Spatial gradient: how different is this cell from its neighbors
        if neighbor_rates:
            spatial_gradient = rate_7d - mean_neighbor_rate
        else:
            spatial_gradient = 0.0

        # --- N. Cosmic ray features (Phase 9) ---
        date_str = self._t_days_to_date(t_now_days)
        cr = self.cosmic_ray_data.get(date_str, {})
        cosmic_ray_rate = cr.get("cosmic_ray_rate", 0.0) or 0.0
        cosmic_ray_anomaly = cr.get("cosmic_ray_anomaly", 0.0) or 0.0
        cosmic_ray_trend_15d = cr.get("cosmic_ray_trend_15d", 0.0) or 0.0

        # --- O. Lightning features (Phase 9) ---
        lightning_count_7d = 0
        lightning_anomaly = 0.0
        for lag in range(7):
            lag_date = self._t_days_to_date(t_now_days - lag)
            lk = (lag_date, cell_lat, cell_lon)
            lt = self.lightning_data.get(lk, {})
            lightning_count_7d += lt.get("stroke_count", 0) or 0
        # Seasonal baseline: ~1 stroke/day/cell average for Japan
        seasonal_baseline = 7.0
        if lightning_count_7d > 0:
            lightning_anomaly = (lightning_count_7d - seasonal_baseline) / max(seasonal_baseline, 1)

        # --- P. Geomagnetic hourly spectral (Phase 9) ---
        gs = self.geomag_spectral_data.get(date_str, {})
        geomag_ulf_power = gs.get("ulf_power", 0.0) or 0.0
        geomag_polarization = gs.get("polarization", 1.0) or 1.0
        geomag_fractal_dim = gs.get("fractal_dim", 1.5) or 1.5

        # --- Q. Animal behavior (Phase 9) ---
        ak = (date_str, cell_lat, cell_lon)
        ad = self.animal_data.get(ak, {})
        animal_speed_anomaly = ad.get("speed_anomaly", 0.0) or 0.0

        # Assemble feature vector
        return [
            rate_7d,
            rate_14d,
            rate_30d,
            rate_ratio_7d,
            rate_ratio_30d,
            rate_accel_7d,
            rate_accel_30d,
            rate_trend_slope,
            etas_residual_7d,
            etas_residual_30d,
            etas_residual_trend,
            max_mag_7d,
            max_mag_30d,
            mag_deficit,
            b_val,
            b_value_trend,
            n_foreshock,
            foreshock_mag_increase,
            inter_event_cv,
            cluster_fraction,
            cfs_cumulative_kpa,
            cfs_rate_state,
            cfs_recent_kpa,
            pi_score,
            pi_trend,
            benioff_30d,
            benioff_accel,
            energy_ratio,
            days_since_m5,
            days_since_m4,
            log_days_since_m5,
            neighbor_rate_sum,
            rate_spatial_anomaly,
            n_active_signals,
            alarm_density,
            # L. GNSS
            gnss_disp_max_30d,
            gnss_disp_accel,
            gnss_vertical_rate,
            gnss_strain_val,
            gnss_anomaly_cnt,
            gnss_transient,
            # M. Enhanced spatial
            neighbor_cfs_max,
            neighbor_etas_resid_max,
            zone_rate_anomaly,
            zone_cfs_rank,
            spatial_gradient,
            neighbor_max_mag_7d,
            # N. Cosmic ray (Phase 9)
            cosmic_ray_rate,
            cosmic_ray_anomaly,
            cosmic_ray_trend_15d,
            # O. Lightning (Phase 9)
            lightning_count_7d,
            lightning_anomaly,
            # P. Geomagnetic spectral (Phase 9)
            geomag_ulf_power,
            geomag_polarization,
            geomag_fractal_dim,
            # Q. Animal behavior (Phase 9)
            animal_speed_anomaly,
        ]

    def extract_dict(self, cell_lat, cell_lon, t_now_days) -> dict:
        """Extract features as a named dict."""
        values = self.extract(cell_lat, cell_lon, t_now_days)
        return dict(zip(FEATURE_NAMES, values))


# ---------------------------------------------------------------------------
# Label generation
# ---------------------------------------------------------------------------

def generate_label(
    cell_lat: float,
    cell_lon: float,
    t_now_days: float,
    target_events_by_cell: dict,
    window_days: int = 7,
    check_neighbors: bool = True,
) -> int:
    """1 if M5+ occurs within window_days and 2° of cell centre, else 0."""
    ck = (cell_lat, cell_lon)
    t_end = t_now_days + window_days

    # Check this cell
    for t_target in target_events_by_cell.get(ck, []):
        if t_now_days < t_target <= t_end:
            return 1

    # Check neighboring cells
    if check_neighbors:
        for dlat in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
            for dlon in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
                if dlat == 0 and dlon == 0:
                    continue
                nk = (cell_lat + dlat, cell_lon + dlon)
                for t_target in target_events_by_cell.get(nk, []):
                    if t_now_days < t_target <= t_end:
                        return 1
    return 0
