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
from collections import deque
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
    # R. Outgoing Longwave Radiation (Phase 10) — Ouzounov et al. 2007
    "olr_anomaly",          # OLR deviation from 30-day cell mean (σ)
    # S. Earth rotation (Phase 10) — novel, untested in earthquake ML
    "lod_rate",             # LOD day-to-day change rate (ms/day)
    "polar_motion_speed",   # polar motion velocity (arcsec/day)
    # T. Solar wind (Phase 10) — Sobolev & Zakrzhevskaya 2020
    "sw_bz_min_24h",        # minimum IMF Bz in 24h (nT, negative = geoeffective)
    "sw_pressure_max_24h",  # max dynamic pressure in 24h (nPa)
    "dst_min_24h",          # minimum Dst in 24h (nT, negative = storm)
    # U. GRACE gravity (Phase 10) — Matsuo & Heki 2011
    "gravity_anomaly_rate", # LWE change rate per cell (cm/month)
    # V. Atmospheric SO2 (Phase 10) — Carn et al. 2016
    "so2_column_anomaly",   # SO2 column deviation from baseline (DU)
    # W. Soil moisture (Phase 10) — Nissen et al. 2014
    "soil_moisture_anomaly", # SM deviation from 30-day baseline (σ)
    # X. Tide gauge (Phase 10b) — Ito et al. 2013
    "tide_residual_anomaly", # sea level residual deviation from 30-day mean (σ)
    # Y. Ocean color (Phase 10b) — Escalera-Reyes et al. 2019
    "ocean_color_anomaly",  # chlorophyll-a deviation from 30-day baseline (σ)
    # Z. Cloud fraction (Phase 10b) — Guangmeng & Jie 2013
    "cloud_fraction_anomaly", # cloud cover deviation from 30-day baseline (σ)
    # AA. Nighttime light/airglow (Phase 10b) — Ouzounov et al. 2022
    "nightlight_anomaly",   # radiance deviation from 6-month baseline (σ)
    # AB. InSAR deformation (Phase 10b) — Bürgmann et al. 2000
    "insar_deformation_rate", # LOS velocity anomaly per cell (mm/yr deviation)
    # AC. Solar X-ray flux (Phase 11) — Sobolev 2020
    "xray_flux_max_24h",    # peak GOES 1-8Å flux in 24h (W/m², log10 scale)
    # AD. Solar proton flux (Phase 11) — SEP events
    "proton_flux_max_24h",  # peak >=10 MeV proton flux in 24h (log10 pfu)
    # AE. Tidal stress (Phase 11) — Cochran et al. 2004
    "tidal_shear_stress",   # combined lunar+solar tidal shear at Japan (Pa)
    "tidal_stress_rate",    # tidal shear rate of change (Pa/day)
    # AF. Particle precipitation (Phase 11) — LAIC coupling
    "particle_precip_rate", # GOES >=2 MeV electron flux (log10 pfu)
    # AG. DART ocean bottom pressure (Phase 13) — Baba et al. 2020
    "dart_pressure_anomaly",  # nearest DART station pressure deviation from 30-day mean (σ)
    "dart_pressure_rate",     # pressure rate of change (m/day)
    # AH. IOC sea level (Phase 13) — additional coastal stations
    "ioc_sealevel_anomaly",   # IOC sea level deviation from 30-day mean (σ)
    # AI. S-net seafloor pressure (Phase 13) — Aoi et al. 2020
    "snet_pressure_anomaly",  # S-net water pressure deviation from baseline (σ)
]

N_FEATURES = len(FEATURE_NAMES)

# Optional feature groups — keyed by data source name.
# Each maps to the feature names that require that data source to be present.
# Dynamic feature selection excludes groups whose data source returned empty.
OPTIONAL_FEATURE_GROUPS = {
    # Phase 9
    "cosmic_ray": ["cosmic_ray_rate", "cosmic_ray_anomaly", "cosmic_ray_trend_15d"],
    "lightning": ["lightning_count_7d", "lightning_anomaly"],
    "geomag_spectral": ["geomag_ulf_power", "geomag_polarization", "geomag_fractal_dim"],
    # Phase 10
    "olr": ["olr_anomaly"],
    "earth_rotation": ["lod_rate", "polar_motion_speed"],
    "solar_wind": ["sw_bz_min_24h", "sw_pressure_max_24h", "dst_min_24h"],
    "gravity": ["gravity_anomaly_rate"],
    "so2": ["so2_column_anomaly"],
    "soil_moisture": ["soil_moisture_anomaly"],
    # Phase 10b
    "tide_gauge": ["tide_residual_anomaly"],
    "ocean_color": ["ocean_color_anomaly"],
    "cloud_fraction": ["cloud_fraction_anomaly"],
    "nightlight": ["nightlight_anomaly"],
    "insar": ["insar_deformation_rate"],
    # Phase 11 — Space/cosmic
    "goes_xray": ["xray_flux_max_24h"],
    "goes_proton": ["proton_flux_max_24h"],
    "tidal_stress": ["tidal_shear_stress", "tidal_stress_rate"],
    "particle_flux": ["particle_precip_rate"],
    # Phase 13 — Seafloor/ocean bottom
    "dart_pressure": ["dart_pressure_anomaly", "dart_pressure_rate"],
    "ioc_sealevel": ["ioc_sealevel_anomaly"],
    "snet_pressure": ["snet_pressure_anomaly"],
}

# Backward compatibility alias
PHASE9_FEATURE_GROUPS = OPTIONAL_FEATURE_GROUPS


def get_active_feature_names(**source_data_kwargs):
    """Return feature names excluding optional groups with no data.

    Instead of feeding zero-filled features that degrade the model,
    dynamically exclude feature groups whose data source returned empty.

    Pass each data source as a keyword argument matching its group name
    (e.g., cosmic_ray_data=..., olr_data=..., solar_wind_data=...).
    The '_data' suffix is stripped to match group names.
    """
    excluded = set()
    for kwarg_name, data in source_data_kwargs.items():
        # Strip '_data' suffix to get group name
        group_name = kwarg_name.removesuffix("_data")
        if group_name in OPTIONAL_FEATURE_GROUPS and not data:
            excluded.update(OPTIONAL_FEATURE_GROUPS[group_name])

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
                 animal_data: dict = None,
                 olr_data: dict = None,
                 earth_rotation_data: dict = None,
                 solar_wind_data: dict = None,
                 gravity_data: dict = None,
                 so2_data: dict = None,
                 soil_moisture_data: dict = None,
                 tide_gauge_data: dict = None,
                 ocean_color_data: dict = None,
                 cloud_fraction_data: dict = None,
                 nightlight_data: dict = None,
                 insar_data: dict = None,
                 goes_xray_data: dict = None,
                 goes_proton_data: dict = None,
                 tidal_stress_data: dict = None,
                 particle_flux_data: dict = None,
                 dart_pressure_data: dict = None,
                 ioc_sealevel_data: dict = None,
                 snet_pressure_data: dict = None):
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
            olr_data: {(date_str, cell_lat, cell_lon): {olr_wm2, olr_mean_30d, olr_std_30d}}
            earth_rotation_data: {date_str: {lod_ms, x_arcsec, y_arcsec, prev_lod, prev_x, prev_y}}
            solar_wind_data: {date_str: {bz_min_24h, pressure_max_24h, dst_min_24h}}
            gravity_data: {(date_str, cell_lat, cell_lon): {lwe_cm, lwe_prev_cm}}
            so2_data: {(date_str, cell_lat, cell_lon): {so2_du, so2_baseline}}
            soil_moisture_data: {(date_str, cell_lat, cell_lon): {sm, sm_mean_30d, sm_std_30d}}
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
        self.olr_data = olr_data or {}
        self.earth_rotation_data = earth_rotation_data or {}
        self.solar_wind_data = solar_wind_data or {}
        self.gravity_data = gravity_data or {}
        self.so2_data = so2_data or {}
        self.soil_moisture_data = soil_moisture_data or {}
        self.tide_gauge_data = tide_gauge_data or {}
        self.ocean_color_data = ocean_color_data or {}
        self.cloud_fraction_data = cloud_fraction_data or {}
        self.nightlight_data = nightlight_data or {}
        self.insar_data = insar_data or {}
        self.goes_xray_data = goes_xray_data or {}
        self.goes_proton_data = goes_proton_data or {}
        self.tidal_stress_data = tidal_stress_data or {}
        self.particle_flux_data = particle_flux_data or {}
        self.dart_pressure_data = dart_pressure_data or {}
        self.ioc_sealevel_data = ioc_sealevel_data or {}
        self.snet_pressure_data = snet_pressure_data or {}

        # Pre-compute cell → zone mapping + zone → cells index
        self.cell_zone = {}
        self.zone_cells = {}  # zone_name -> list of cell_keys
        for lat in range(GRID_LAT_MIN, GRID_LAT_MAX + 1, int(CELL_SIZE_DEG)):
            for lon in range(GRID_LON_MIN, GRID_LON_MAX + 1, int(CELL_SIZE_DEG)):
                ck = (float(lat), float(lon))
                zone = classify_tectonic_zone(float(lat), float(lon))
                self.cell_zone[ck] = zone
                self.zone_cells.setdefault(zone, []).append(ck)

        # Index events by 2° cell (sorted by t_days for bisect)
        self.cell_events = {}
        for e in events:
            ck = cell_key(e["lat"], e["lon"])
            self.cell_events.setdefault(ck, []).append(e)
        # Sort and build bisect key arrays
        self.cell_events_tdays = {}  # cell -> sorted list of t_days
        for ck, evs in self.cell_events.items():
            evs.sort(key=lambda e: e["t_days"])
            self.cell_events_tdays[ck] = [e["t_days"] for e in evs]

        # Index by 1° cell (foreshock counting, sorted)
        self.cell_events_1deg = {}
        self.cell_events_1deg_tdays = {}
        for e in events:
            k1 = (round(e["lat"]), round(e["lon"]))
            self.cell_events_1deg.setdefault(k1, []).append(e)
        for k1, evs in self.cell_events_1deg.items():
            evs.sort(key=lambda e: e["t_days"])
            self.cell_events_1deg_tdays[k1] = [e["t_days"] for e in evs]

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

        # PI rate history per cell (deque for auto-truncation)
        self.pi_history = {}  # cell -> deque(maxlen=240)

        # Rate history per cell (for trend computation)
        self.rate_history = {}  # cell -> deque(maxlen=20)

        # b-value history per cell (for trend)
        self.b_history = {}  # cell -> deque(maxlen=20)

        # ETAS residual history per cell (for trend)
        self.etas_resid_history = {}  # cell -> deque(maxlen=20)

        # Per-day caches (reset when t_now_days changes)
        self._cached_day = None
        self._cached_date_str = None
        self._cached_zone_stats = {}  # zone -> (mean_rate, cfs_values_sorted)

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

    def _count_in_window_bisect(self, tdays_list, t_start, t_end):
        """Count events in [t_start, t_end) using bisect on sorted t_days."""
        lo = bisect.bisect_left(tdays_list, t_start)
        hi = bisect.bisect_left(tdays_list, t_end)
        return hi - lo

    def _events_in_window_bisect(self, cell_evs, tdays_list, t_start, t_end):
        """Get events in [t_start, t_end) using bisect on sorted t_days."""
        lo = bisect.bisect_left(tdays_list, t_start)
        hi = bisect.bisect_left(tdays_list, t_end)
        return cell_evs[lo:hi]

    def _t_days_to_date(self, t_days: float) -> str:
        """Convert t_days offset to YYYY-MM-DD date string."""
        from datetime import timedelta
        dt = self.t0 + timedelta(days=t_days)
        return dt.strftime("%Y-%m-%d")

    def _get_date_str(self, t_now_days: float) -> str:
        """Cached date string conversion (same for all cells at same day)."""
        if self._cached_day != t_now_days:
            self._cached_day = t_now_days
            self._cached_date_str = self._t_days_to_date(t_now_days)
            self._cached_zone_stats = {}  # invalidate zone cache
        return self._cached_date_str

    def _get_zone_stats(self, zone, t_7, t_now_days):
        """Cached zone-level rate stats (computed once per day per zone)."""
        if zone in self._cached_zone_stats:
            return self._cached_zone_stats[zone]

        zone_rates = []
        zone_cfs_values = []
        for other_ck in self.zone_cells.get(zone, []):
            other_tdays = self.cell_events_tdays.get(other_ck, [])
            other_rate = self._count_in_window_bisect(other_tdays, t_7, t_now_days)
            zone_rates.append((other_ck, other_rate))
            zone_cfs_values.append(self.cfs_map.get(other_ck, 0.0))

        result = {
            "rates": zone_rates,  # list of (ck, rate)
            "mean_rate": (sum(r for _, r in zone_rates) / max(len(zone_rates), 1)
                          if zone_rates else 0.0),
            "cfs_sorted": sorted(zone_cfs_values),
        }
        self._cached_zone_stats[zone] = result
        return result

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
        # Use bisect to find events before t_start efficiently
        tdays_list = self.cell_events_tdays.get(ck, [])
        hi = bisect.bisect_left(tdays_list, t_start)
        # Take last 2000 events before t_start
        lo = max(0, hi - 2000)
        prior = [(cell_evs[i]["t_days"], cell_evs[i]["mag"]) for i in range(lo, hi)]

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
        cell_tdays = self.cell_events_tdays.get(ck, [])

        # Time windows
        t_3 = t_now_days - 3
        t_7 = t_now_days - 7
        t_14 = t_now_days - 14
        t_30 = t_now_days - 30
        t_60 = t_now_days - 60
        t_90 = t_now_days - 90
        t_180 = t_now_days - 180
        t_365 = t_now_days - 365

        evs_3d = self._events_in_window_bisect(cell_evs, cell_tdays, t_3, t_now_days)
        evs_7d = self._events_in_window_bisect(cell_evs, cell_tdays, t_7, t_now_days)
        evs_14d = self._events_in_window_bisect(cell_evs, cell_tdays, t_14, t_now_days)
        evs_30d = self._events_in_window_bisect(cell_evs, cell_tdays, t_30, t_now_days)
        evs_90d = self._events_in_window_bisect(cell_evs, cell_tdays, t_90, t_now_days)
        evs_180d = self._events_in_window_bisect(cell_evs, cell_tdays, t_180, t_now_days)

        # Previous windows (for acceleration)
        evs_prev_7d = self._events_in_window_bisect(cell_evs, cell_tdays, t_14, t_7)
        evs_prev_30d = self._events_in_window_bisect(cell_evs, cell_tdays, t_60, t_30)
        evs_prev_3d = self._events_in_window_bisect(cell_evs, cell_tdays, t_7 + 1, t_3)

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
        if ck not in self.rate_history:
            self.rate_history[ck] = deque(maxlen=20)
        self.rate_history[ck].append((t_now_days, rate_7d))
        hist = self.rate_history[ck]
        rate_values = [v for _, v in list(hist)[-9:]]  # last ~60 days
        rate_trend_slope = self._linear_slope(rate_values)

        # --- C. ETAS residuals (zone-aware) ---
        mu = self.mu_bg.get(ck, 0.01)
        etas_exp_7d = self._compute_etas_expected(cell_evs, t_now_days, 7, cell_lat, cell_lon)
        etas_residual_7d = rate_7d / max(etas_exp_7d, 0.1)

        etas_exp_30d = self._compute_etas_expected(cell_evs, t_now_days, 30, cell_lat, cell_lon)
        etas_residual_30d = rate_30d / max(etas_exp_30d, 0.1)

        # ETAS residual trend
        if ck not in self.etas_resid_history:
            self.etas_resid_history[ck] = deque(maxlen=20)
        self.etas_resid_history[ck].append(etas_residual_7d)
        etas_residual_trend = self._linear_slope(list(self.etas_resid_history[ck])[-9:])

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
        if ck not in self.b_history:
            self.b_history[ck] = deque(maxlen=20)
        self.b_history[ck].append(b_val)
        b_value_trend = self._linear_slope(list(self.b_history[ck])[-6:])

        # --- E. Clustering ---
        # Foreshock count (1° box, 7 days) — bisect
        n_foreshock = 0
        for dlat in (-1, 0, 1):
            for dlon in (-1, 0, 1):
                k1 = (round(cell_lat) + dlat, round(cell_lon) + dlon)
                k1_tdays = self.cell_events_1deg_tdays.get(k1, [])
                n_foreshock += self._count_in_window_bisect(k1_tdays, t_7, t_now_days)

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
        if ck not in self.pi_history:
            self.pi_history[ck] = deque(maxlen=240)
        self.pi_history[ck].append(rate_7d)
        pi_hist = self.pi_history[ck]

        recent_pi = list(pi_hist)
        if len(recent_pi) >= 4:
            changes = [recent_pi[i] - recent_pi[i - 1] for i in range(1, len(recent_pi))]
            mean_c = sum(changes) / len(changes)
            pi_score = sum((c - mean_c) ** 2 for c in changes) / len(changes)
        else:
            pi_score = 0.0

        # PI trend (recent vs older) — use list for slicing (deque doesn't support slices)
        if len(recent_pi) >= 6:
            pi_recent = sum(recent_pi[-3:]) / 3
            pi_older = sum(recent_pi[-6:-3]) / 3
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

        # --- J. Spatial context --- (bisect + cache neighbor rates for M reuse)
        neighbor_rate_sum = 0
        neighbor_rates = []
        _neighbor_data = {}  # nk -> {rate, evs, tdays} for reuse in section M
        for dlat in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
            for dlon in (-CELL_SIZE_DEG, 0, CELL_SIZE_DEG):
                if dlat == 0 and dlon == 0:
                    continue
                nk = (cell_lat + dlat, cell_lon + dlon)
                n_tdays = self.cell_events_tdays.get(nk, [])
                n_rate = self._count_in_window_bisect(n_tdays, t_7, t_now_days)
                neighbor_rate_sum += n_rate
                neighbor_rates.append(n_rate)
                _neighbor_data[nk] = {"rate": n_rate, "tdays": n_tdays}

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
                # Limit to last 180 days for efficiency
                disp_timeseries = []
                t_trans_start = t_now_days - 180
                for snapshot in cell_gnss:
                    if t_trans_start <= snapshot["t_days"] < t_now_days:
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
        # Reuse neighbor data from section J (no re-scanning)
        neighbor_cfs_max = 0.0
        neighbor_etas_resid_max = 0.0
        neighbor_max_mag_7d = 0.0

        for nk, nd in _neighbor_data.items():
            # Neighbor CFS
            n_cfs = self.cfs_map.get(nk, 0.0)
            if n_cfs > neighbor_cfs_max:
                neighbor_cfs_max = n_cfs

            # Neighbor ETAS residual (reuse cached rate)
            n_evs = self.cell_events.get(nk, [])
            n_etas_exp = self._compute_etas_expected(n_evs, t_now_days, 7, nk[0], nk[1])
            n_etas_resid = nd["rate"] / max(n_etas_exp, 0.1)
            if n_etas_resid > neighbor_etas_resid_max:
                neighbor_etas_resid_max = n_etas_resid

            # Neighbor max magnitude (7d) — bisect
            n_tdays = nd["tdays"]
            lo = bisect.bisect_left(n_tdays, t_7)
            hi = bisect.bisect_left(n_tdays, t_now_days)
            for idx in range(lo, hi):
                mag = n_evs[idx]["mag"]
                if mag > neighbor_max_mag_7d:
                    neighbor_max_mag_7d = mag

        # Zone-level statistics (cached per day)
        zone = self.cell_zone.get(ck, "other")
        zstats = self._get_zone_stats(zone, t_7, t_now_days)
        zone_mean_rate = zstats["mean_rate"] if zstats["mean_rate"] > 0 else rate_7d
        zone_rate_anomaly = rate_7d / max(zone_mean_rate, 0.1)

        # CFS rank within zone (using pre-sorted list)
        cfs_sorted = zstats["cfs_sorted"]
        if cfs_sorted:
            zone_cfs_rank = bisect.bisect_right(cfs_sorted, cfs_cumulative_kpa) / len(cfs_sorted)
        else:
            zone_cfs_rank = 0.5

        # Spatial gradient: how different is this cell from its neighbors
        if neighbor_rates:
            spatial_gradient = rate_7d - mean_neighbor_rate
        else:
            spatial_gradient = 0.0

        # --- N. Cosmic ray features (Phase 9) ---
        date_str = self._get_date_str(t_now_days)
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

        # --- R. OLR (Phase 10) ---
        olr_key = (date_str, cell_lat, cell_lon)
        olr_d = self.olr_data.get(olr_key, {})
        olr_val = olr_d.get("olr_wm2", 0.0) or 0.0
        olr_mean = olr_d.get("olr_mean_30d", 0.0) or 0.0
        olr_std = olr_d.get("olr_std_30d", 1.0) or 1.0
        olr_anomaly = (olr_val - olr_mean) / max(olr_std, 0.1) if olr_val > 0 else 0.0

        # --- S. Earth rotation (Phase 10) ---
        er = self.earth_rotation_data.get(date_str, {})
        lod_now = er.get("lod_ms", 0.0)
        lod_prev = er.get("prev_lod", 0.0)
        lod_rate = (lod_now - lod_prev) if (lod_now and lod_prev) else 0.0
        x_now = er.get("x_arcsec", 0.0) or 0.0
        y_now = er.get("y_arcsec", 0.0) or 0.0
        x_prev = er.get("prev_x", 0.0) or 0.0
        y_prev = er.get("prev_y", 0.0) or 0.0
        polar_motion_speed = ((x_now - x_prev) ** 2 + (y_now - y_prev) ** 2) ** 0.5

        # --- T. Solar wind (Phase 10) ---
        sw = self.solar_wind_data.get(date_str, {})
        sw_bz_min_24h = sw.get("bz_min_24h", 0.0) or 0.0
        sw_pressure_max_24h = sw.get("pressure_max_24h", 0.0) or 0.0
        dst_min_24h = sw.get("dst_min_24h", 0.0) or 0.0

        # --- U. GRACE gravity (Phase 10) ---
        gk = (date_str, cell_lat, cell_lon)
        gd = self.gravity_data.get(gk, {})
        lwe_now = gd.get("lwe_cm", 0.0) or 0.0
        lwe_prev = gd.get("lwe_prev_cm", 0.0) or 0.0
        gravity_anomaly_rate = lwe_now - lwe_prev  # cm/month

        # --- V. SO2 (Phase 10) ---
        so2k = (date_str, cell_lat, cell_lon)
        so2d = self.so2_data.get(so2k, {})
        so2_val = so2d.get("so2_du", 0.0) or 0.0
        so2_baseline = so2d.get("so2_baseline", 0.0) or 0.0
        so2_column_anomaly = so2_val - so2_baseline if so2_val > 0 else 0.0

        # --- W. Soil moisture (Phase 10) ---
        smk = (date_str, cell_lat, cell_lon)
        smd = self.soil_moisture_data.get(smk, {})
        sm_val = smd.get("sm", 0.0) or 0.0
        sm_mean = smd.get("sm_mean_30d", 0.0) or 0.0
        sm_std = smd.get("sm_std_30d", 1.0) or 1.0
        soil_moisture_anomaly = (sm_val - sm_mean) / max(sm_std, 0.001) if sm_val > 0 else 0.0

        # --- X. Tide gauge (Phase 10b) ---
        tg = self.tide_gauge_data.get(date_str, {})
        tg_val = tg.get("residual", 0.0) or 0.0
        tg_mean = tg.get("residual_mean_30d", 0.0) or 0.0
        tg_std = tg.get("residual_std_30d", 1.0) or 1.0
        tide_residual_anomaly = (tg_val - tg_mean) / max(tg_std, 0.1) if tg_val != 0 else 0.0

        # --- Y. Ocean color (Phase 10b) ---
        ock = (date_str, cell_lat, cell_lon)
        ocd = self.ocean_color_data.get(ock, {})
        oc_val = ocd.get("chlor_a", 0.0) or 0.0
        oc_mean = ocd.get("chlor_mean_30d", 0.0) or 0.0
        oc_std = ocd.get("chlor_std_30d", 1.0) or 1.0
        ocean_color_anomaly = (oc_val - oc_mean) / max(oc_std, 0.01) if oc_val > 0 else 0.0

        # --- Z. Cloud fraction (Phase 10b) ---
        cfk = (date_str, cell_lat, cell_lon)
        cfd = self.cloud_fraction_data.get(cfk, {})
        cf_val = cfd.get("cloud_frac", 0.0) or 0.0
        cf_mean = cfd.get("cloud_mean_30d", 0.0) or 0.0
        cf_std = cfd.get("cloud_std_30d", 1.0) or 1.0
        cloud_fraction_anomaly = (cf_val - cf_mean) / max(cf_std, 0.01) if cf_val > 0 else 0.0

        # --- AA. Nighttime light (Phase 10b) ---
        nlk = (date_str, cell_lat, cell_lon)
        nld = self.nightlight_data.get(nlk, {})
        nl_val = nld.get("radiance", 0.0) or 0.0
        nl_mean = nld.get("radiance_mean_6m", 0.0) or 0.0
        nl_std = nld.get("radiance_std_6m", 1.0) or 1.0
        nightlight_anomaly = (nl_val - nl_mean) / max(nl_std, 0.01) if nl_val > 0 else 0.0

        # --- AB. InSAR deformation (Phase 10b) ---
        isk = (date_str, cell_lat, cell_lon)
        isd = self.insar_data.get(isk, {})
        insar_deformation_rate = isd.get("velocity_anomaly", 0.0) or 0.0

        # --- AC. Solar X-ray flux (Phase 11) ---
        xr = self.goes_xray_data.get(date_str, {})
        xray_raw = xr.get("xray_long_wm2", 0.0) or 0.0
        xray_flux_max_24h = math.log10(max(xray_raw, 1e-9)) if xray_raw > 0 else -9.0

        # --- AD. Solar proton flux (Phase 11) ---
        pr = self.goes_proton_data.get(date_str, {})
        proton_raw = pr.get("proton_10mev_max", 0.0) or 0.0
        proton_flux_max_24h = math.log10(max(proton_raw, 0.01)) if proton_raw > 0 else -2.0

        # --- AE. Tidal stress (Phase 11) ---
        td = self.tidal_stress_data.get(date_str, {})
        tidal_shear_stress = td.get("tidal_shear_pa", 0.0) or 0.0
        prev_date = self._t_days_to_date(t_now_days - 1)
        td_prev = self.tidal_stress_data.get(prev_date, {})
        prev_shear = td_prev.get("tidal_shear_pa", 0.0) or 0.0
        tidal_stress_rate = tidal_shear_stress - prev_shear

        # --- AF. Particle precipitation (Phase 11) ---
        pf = self.particle_flux_data.get(date_str, {})
        elec_raw = pf.get("electron_2mev_max", 0.0) or 0.0
        particle_precip_rate = math.log10(max(elec_raw, 0.1)) if elec_raw > 0 else -1.0

        # --- AG. DART ocean bottom pressure (Phase 13) ---
        # DART data keyed by date_str (nearest station aggregated)
        dp = self.dart_pressure_data.get(date_str, {})
        dp_val = dp.get("height_m", 0.0) or 0.0
        dp_mean = dp.get("height_mean_30d", 0.0) or 0.0
        dp_std = dp.get("height_std_30d", 1.0) or 1.0
        dart_pressure_anomaly = (dp_val - dp_mean) / max(dp_std, 0.001) if dp_val > 0 else 0.0
        dp_prev = dp.get("height_prev_day", 0.0) or 0.0
        dart_pressure_rate = dp_val - dp_prev if (dp_val > 0 and dp_prev > 0) else 0.0

        # --- AH. IOC sea level (Phase 13) ---
        ioc = self.ioc_sealevel_data.get(date_str, {})
        ioc_val = ioc.get("level_m", 0.0) or 0.0
        ioc_mean = ioc.get("level_mean_30d", 0.0) or 0.0
        ioc_std = ioc.get("level_std_30d", 1.0) or 1.0
        ioc_sealevel_anomaly = (ioc_val - ioc_mean) / max(ioc_std, 0.001) if ioc_val != 0 else 0.0

        # --- AI. S-net seafloor pressure (Phase 13) ---
        sn = self.snet_pressure_data.get(date_str, {})
        sn_val = sn.get("pressure_hpa", 0.0) or 0.0
        sn_mean = sn.get("pressure_mean_30d", 0.0) or 0.0
        sn_std = sn.get("pressure_std_30d", 1.0) or 1.0
        snet_pressure_anomaly = (sn_val - sn_mean) / max(sn_std, 0.001) if sn_val > 0 else 0.0

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
            # R. OLR (Phase 10)
            olr_anomaly,
            # S. Earth rotation (Phase 10)
            lod_rate,
            polar_motion_speed,
            # T. Solar wind (Phase 10)
            sw_bz_min_24h,
            sw_pressure_max_24h,
            dst_min_24h,
            # U. GRACE gravity (Phase 10)
            gravity_anomaly_rate,
            # V. SO2 (Phase 10)
            so2_column_anomaly,
            # W. Soil moisture (Phase 10)
            soil_moisture_anomaly,
            # X. Tide gauge (Phase 10b)
            tide_residual_anomaly,
            # Y. Ocean color (Phase 10b)
            ocean_color_anomaly,
            # Z. Cloud fraction (Phase 10b)
            cloud_fraction_anomaly,
            # AA. Nighttime light (Phase 10b)
            nightlight_anomaly,
            # AB. InSAR (Phase 10b)
            insar_deformation_rate,
            # AC. Solar X-ray (Phase 11)
            xray_flux_max_24h,
            # AD. Solar proton (Phase 11)
            proton_flux_max_24h,
            # AE. Tidal stress (Phase 11)
            tidal_shear_stress,
            tidal_stress_rate,
            # AF. Particle precipitation (Phase 11)
            particle_precip_rate,
            # AG. DART ocean bottom pressure (Phase 13)
            dart_pressure_anomaly,
            dart_pressure_rate,
            # AH. IOC sea level (Phase 13)
            ioc_sealevel_anomaly,
            # AI. S-net seafloor pressure (Phase 13)
            snet_pressure_anomaly,
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
