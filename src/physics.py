"""Shared physics module for earthquake analysis.

Consolidates all physical models previously duplicated across
coulomb_analysis.py, ml_prediction.py, prospective_analysis.py, etas_analysis.py.

Models:
    1. Okada (1992) far-field CFS from rectangular dislocation
    2. Wells & Coppersmith (1994) fault scaling relations
    3. Regional default focal mechanisms for Japan
    4. ETAS (Ogata 1998) temporal aftershock model
    5. ETAS MLE parameter estimation (scipy L-BFGS-B)
    6. Rate-and-State CFS (Dieterich 1994) time-dependent probability

References:
    - Okada (1992) BSSA 82:1018-1040
    - Wells & Coppersmith (1994) BSSA 84:974-1002
    - Ogata (1998) Ann. Inst. Stat. Math. 50:379-402
    - Dieterich (1994) JGR 99:2601-2618
    - Toda & Stein (2003) JGR 108(B5):2228
"""

import bisect
import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Physical constants
# ---------------------------------------------------------------------------

DEG_TO_KM = 111.32
DEG_TO_M = DEG_TO_KM * 1000
SHEAR_MODULUS = 32e9  # Pa (32 GPa)
POISSON_RATIO = 0.25
LAME_LAMBDA = SHEAR_MODULUS * 2 * POISSON_RATIO / (1 - 2 * POISSON_RATIO)
MU_FRICTION = 0.4  # effective friction coefficient


# ---------------------------------------------------------------------------
# Wells & Coppersmith (1994) fault scaling
# ---------------------------------------------------------------------------

# Coefficients: (a, b) for log10(X) = a + b * M
_SCALING = {
    "reverse": {"length": (-2.86, 0.63), "width": (-1.61, 0.41)},
    "strike_slip": {"length": (-3.55, 0.74), "width": (-0.76, 0.27)},
    "normal": {"length": (-2.01, 0.50), "width": (-1.14, 0.35)},
    "all": {"length": (-2.44, 0.59), "width": (-1.01, 0.32)},
}


def fault_dimensions(mw: float, fault_type: str = "reverse"):
    """Wells & Coppersmith (1994) fault dimensions and slip.

    Returns:
        (length_km, width_km, slip_m)
    """
    s = _SCALING.get(fault_type, _SCALING["all"])
    length_km = 10 ** (s["length"][0] + s["length"][1] * mw)
    width_km = 10 ** (s["width"][0] + s["width"][1] * mw)
    m0 = 10 ** (1.5 * mw + 9.05)  # N-m
    slip_m = m0 / (SHEAR_MODULUS * length_km * 1000 * width_km * 1000)
    return length_km, width_km, slip_m


def classify_fault_type(rake: float) -> str:
    """Classify fault type from rake angle."""
    rake = rake % 360
    if rake > 180:
        rake -= 360
    if -30 <= rake <= 30 or 150 <= rake <= 210 or -210 <= rake <= -150:
        return "strike_slip"
    elif 30 < rake < 150:
        return "reverse"
    else:
        return "normal"


# ---------------------------------------------------------------------------
# Regional default focal mechanisms for Japan
# ---------------------------------------------------------------------------

def default_mechanism(lat: float, lon: float, depth_km: float):
    """Default (strike, dip, rake) based on Japan tectonic setting.

    Regions:
        - Deep intraslab (>70km): down-dip compression
        - Tohoku offshore (lon>142, lat>35): Pacific plate interface
        - Nankai (lon<137, lat<35): Philippine Sea plate
        - Northern Honshu inland (lat>40): E-W compression
        - Kyushu extensional (130<lon<132, lat<34): normal faulting
        - Default: E-W compression reverse
    """
    if depth_km > 70:
        return (200.0, 45.0, 90.0)
    elif lon > 142 and lat > 35:
        return (200.0, 25.0, 90.0)
    elif lon < 137 and lat < 35:
        return (240.0, 15.0, 90.0)
    elif lat > 40:
        return (200.0, 40.0, 90.0)
    elif 130 < lon < 132 and lat < 34:
        return (30.0, 60.0, -90.0)
    else:
        return (200.0, 35.0, 90.0)


# ---------------------------------------------------------------------------
# Okada (1992) far-field CFS
# ---------------------------------------------------------------------------

def okada_cfs(
    src_lat: float, src_lon: float, src_depth: float,
    src_strike: float, src_dip: float, src_rake: float,
    src_length: float, src_width: float, src_slip: float,
    obs_lat: float, obs_lon: float, obs_depth: float,
) -> float:
    """Coulomb failure stress change from rectangular dislocation.

    Far-field point-source approximation (Aki & Richards, 2002).
    Valid when distance >> fault dimensions.

    Args:
        src_*: source fault parameters (lat/lon in deg, depth/length/width in km, slip in m)
        obs_*: observation point (lat/lon in deg, depth in km)

    Returns:
        delta_CFS in Pa on optimally oriented receiver fault.
    """
    dx = (obs_lon - src_lon) * DEG_TO_KM * math.cos(math.radians(src_lat)) * 1000
    dy = (obs_lat - src_lat) * DEG_TO_KM * 1000
    dz = (obs_depth - src_depth) * 1000
    r = math.sqrt(dx**2 + dy**2 + dz**2)
    if r < 500:  # 500m minimum to avoid singularity
        return 0.0

    m0 = SHEAR_MODULUS * src_length * 1000 * src_width * 1000 * src_slip

    strike_r = math.radians(src_strike)
    dip_r = math.radians(src_dip)
    rake_r = math.radians(src_rake)

    # Fault normal vector
    n = [
        -math.sin(dip_r) * math.sin(strike_r),
        math.sin(dip_r) * math.cos(strike_r),
        -math.cos(dip_r),
    ]
    # Slip direction vector
    d = [
        math.cos(rake_r) * math.cos(strike_r) + math.sin(rake_r) * math.cos(dip_r) * math.sin(strike_r),
        math.cos(rake_r) * math.sin(strike_r) - math.sin(rake_r) * math.cos(dip_r) * math.cos(strike_r),
        -math.sin(rake_r) * math.sin(dip_r),
    ]

    rhat = [dx / r, dy / r, dz / r]

    # Normalized moment tensor
    m_ij = [[(n[i] * d[j] + n[j] * d[i]) / 2 for j in range(3)] for i in range(3)]

    prefactor = m0 / (4 * math.pi * r**3)
    m_rr = sum(m_ij[k][l] * rhat[k] * rhat[l] for k in range(3) for l in range(3))

    # Full stress tensor
    stress = [
        [prefactor * (3 * m_rr * rhat[i] * rhat[j] - m_ij[i][j] - (1 if i == j else 0) * m_rr)
         for j in range(3)]
        for i in range(3)
    ]

    # CFS on optimally oriented plane (King et al., 1994)
    sigma_mean = (stress[0][0] + stress[1][1] + stress[2][2]) / 3
    dev = [[stress[i][j] - (sigma_mean if i == j else 0) for j in range(3)] for i in range(3)]
    j2 = 0.5 * sum(dev[i][j] ** 2 for i in range(3) for j in range(3))
    tau_max = math.sqrt(max(j2, 0))

    return tau_max + MU_FRICTION * sigma_mean


def okada_cfs_full(
    src_lat, src_lon, src_depth,
    src_strike, src_dip, src_rake,
    src_length, src_width, src_slip,
    obs_lat, obs_lon, obs_depth,
):
    """Same as okada_cfs but returns (tau, sigma_n, delta_cfs) tuple."""
    dx = (obs_lon - src_lon) * DEG_TO_KM * math.cos(math.radians(src_lat)) * 1000
    dy = (obs_lat - src_lat) * DEG_TO_KM * 1000
    dz = (obs_depth - src_depth) * 1000
    r = math.sqrt(dx**2 + dy**2 + dz**2)
    if r < 500:
        return (0.0, 0.0, 0.0)

    m0 = SHEAR_MODULUS * src_length * 1000 * src_width * 1000 * src_slip
    strike_r = math.radians(src_strike)
    dip_r = math.radians(src_dip)
    rake_r = math.radians(src_rake)

    n = [
        -math.sin(dip_r) * math.sin(strike_r),
        math.sin(dip_r) * math.cos(strike_r),
        -math.cos(dip_r),
    ]
    d = [
        math.cos(rake_r) * math.cos(strike_r) + math.sin(rake_r) * math.cos(dip_r) * math.sin(strike_r),
        math.cos(rake_r) * math.sin(strike_r) - math.sin(rake_r) * math.cos(dip_r) * math.cos(strike_r),
        -math.sin(rake_r) * math.sin(dip_r),
    ]
    rhat = [dx / r, dy / r, dz / r]
    m_ij = [[(n[i] * d[j] + n[j] * d[i]) / 2 for j in range(3)] for i in range(3)]
    prefactor = m0 / (4 * math.pi * r**3)
    m_rr = sum(m_ij[k][l] * rhat[k] * rhat[l] for k in range(3) for l in range(3))
    stress = [
        [prefactor * (3 * m_rr * rhat[i] * rhat[j] - m_ij[i][j] - (1 if i == j else 0) * m_rr)
         for j in range(3)]
        for i in range(3)
    ]
    sigma_mean = (stress[0][0] + stress[1][1] + stress[2][2]) / 3
    dev = [[stress[i][j] - (sigma_mean if i == j else 0) for j in range(3)] for i in range(3)]
    j2 = 0.5 * sum(dev[i][j] ** 2 for i in range(3) for j in range(3))
    tau_max = math.sqrt(max(j2, 0))
    delta_cfs = tau_max + MU_FRICTION * sigma_mean
    return (tau_max, sigma_mean, delta_cfs)


# ---------------------------------------------------------------------------
# Rate-and-State CFS (Dieterich 1994)
# ---------------------------------------------------------------------------

def rate_state_probability(
    delta_cfs_pa: float,
    base_rate: float,
    dt_days: float,
    a_sigma: float = 10000.0,  # 10 kPa = 0.01 MPa (typical for Japan crust)
    ta_days: float = 1825.0,   # aftershock duration ~5 years
) -> float:
    """Rate-and-state seismicity rate change from CFS perturbation.

    Dieterich (1994) model: seismicity rate jumps after a stress step
    and decays back to background over time ta.

    R(t) = R0 / [exp(-ΔCFS/Aσ) + (1 - exp(-ΔCFS/Aσ)) * exp(-t/ta)]

    where:
        R0 = background seismicity rate
        ΔCFS = Coulomb stress change (Pa)
        Aσ = constitutive parameter × normal stress (Pa)
        ta = aftershock duration (days)
        t = time since stress step (days)

    Args:
        delta_cfs_pa: Coulomb stress change in Pa
        base_rate: background seismicity rate (events/day)
        dt_days: time since the stress step (days)
        a_sigma: frictional parameter Aσ in Pa (range: 5000-50000)
        ta_days: characteristic aftershock duration in days

    Returns:
        Modified seismicity rate (events/day) at time dt_days after the step.
    """
    if abs(delta_cfs_pa) < 1.0 or dt_days <= 0:
        return base_rate

    # Clamp exponent to avoid overflow
    exp_arg = -delta_cfs_pa / a_sigma
    exp_arg = max(min(exp_arg, 50), -50)

    exp_cfs = math.exp(exp_arg)
    decay = math.exp(-dt_days / ta_days)

    denominator = exp_cfs + (1 - exp_cfs) * decay
    if denominator <= 0 or denominator > 1e10:
        return base_rate

    return base_rate / denominator


def cumulative_rate_state_cfs(
    cfs_history: list,
    t_now_days: float,
    base_rate: float,
    a_sigma: float = 10000.0,
    ta_days: float = 1825.0,
) -> float:
    """Compute effective seismicity rate from multiple CFS perturbations.

    Each entry in cfs_history is (t_step_days, delta_cfs_pa).
    The rate-and-state formulation handles superposition through
    the state variable γ (Toda & Stein 2003):

        γ(t) = Σ_i γ_i(t)
        where γ_i(t) = exp(-ΔCFS_i/Aσ) * exp(-(t-t_i)/ta)

    R(t) = R0 / γ(t)

    Args:
        cfs_history: list of (time_days, delta_cfs_pa) tuples, sorted by time
        t_now_days: current time in days
        base_rate: background rate (events/day)
        a_sigma: Aσ parameter in Pa
        ta_days: aftershock duration in days

    Returns:
        Modified rate at t_now_days (events/day)
    """
    if not cfs_history:
        return base_rate

    gamma = 0.0
    for t_step, delta_cfs in cfs_history:
        dt = t_now_days - t_step
        if dt <= 0:
            continue

        exp_arg = -delta_cfs / a_sigma
        exp_arg = max(min(exp_arg, 50), -50)

        gamma += math.exp(exp_arg) * math.exp(-dt / ta_days)

    # Add the background state contribution
    # At steady state (no perturbations), gamma = 1/R0, so R = R0
    # With perturbations, gamma is modified
    if gamma <= 0:
        return base_rate

    # Normalize: at t → ∞, gamma should → 1 (steady state)
    # In practice, sum contribution from N sources + steady-state term
    n_sources = len([1 for t_s, _ in cfs_history if t_now_days - t_s > 0])
    steady_term = n_sources * math.exp(-max(t_now_days - cfs_history[0][0], 0) / ta_days) if n_sources else 1.0
    total_gamma = gamma + max(1.0 - steady_term, 0)

    if total_gamma <= 0:
        return base_rate

    return base_rate / max(total_gamma, 1e-10)


# ---------------------------------------------------------------------------
# ETAS temporal model
# ---------------------------------------------------------------------------

# Default Japan M3+ parameters (Ogata 1998)
ETAS_DEFAULTS = {
    "K": 0.04,
    "alpha": 1.0,
    "c": 0.01,   # days
    "p": 1.1,
    "Mc": 3.0,
}


def etas_intensity(
    t_days: float,
    events: list,
    mu: float,
    K: float = 0.04,
    alpha: float = 1.0,
    c: float = 0.01,
    p: float = 1.1,
    Mc: float = 3.0,
) -> float:
    """ETAS conditional intensity at time t.

    λ(t) = μ + Σ_{t_i < t} K * exp(α(m_i - Mc)) / (t - t_i + c)^p

    Args:
        t_days: evaluation time
        events: list of (t_days, magnitude) tuples, pre-sorted by time
        mu: background rate (events/day)
        K, alpha, c, p, Mc: ETAS parameters

    Returns:
        Conditional intensity (events/day)
    """
    rate = mu
    for ti, mi in events:
        dt = t_days - ti
        if dt <= 0:
            break
        rate += K * math.exp(alpha * (mi - Mc)) / (dt + c) ** p
    return rate


def etas_expected_count(
    t_start: float,
    t_end: float,
    events: list,
    mu: float,
    K: float = 0.04,
    alpha: float = 1.0,
    c: float = 0.01,
    p: float = 1.1,
    Mc: float = 3.0,
    n_quadrature: int = 7,
) -> float:
    """Expected event count in [t_start, t_end] under ETAS model.

    Uses trapezoidal quadrature for numerical integration.

    Args:
        t_start, t_end: time interval (days)
        events: list of (t_days, magnitude) tuples prior to t_start
        mu, K, alpha, c, p, Mc: ETAS parameters
        n_quadrature: number of quadrature points

    Returns:
        Expected count of M>=Mc events in the window.
    """
    dt = (t_end - t_start) / n_quadrature
    total = 0.0
    for k in range(n_quadrature):
        t_sample = t_start + (k + 0.5) * dt
        # Only use events strictly before t_sample
        prior = [(ti, mi) for ti, mi in events if ti < t_sample]
        rate = etas_intensity(t_sample, prior, mu, K, alpha, c, p, Mc)
        total += rate * dt
    return total


# ---------------------------------------------------------------------------
# ETAS MLE parameter estimation
# ---------------------------------------------------------------------------

def etas_log_likelihood(
    params_vec: list,
    event_times: list,
    event_mags: list,
    T_start: float,
    T_end: float,
    Mc: float = 3.0,
) -> float:
    """Negative log-likelihood for temporal ETAS model.

    For scipy.optimize.minimize (minimization target).

    Args:
        params_vec: [log_mu, log_K, alpha, log_c, p] (log-transformed for positivity)
        event_times: list of event times in days
        event_mags: list of event magnitudes
        T_start, T_end: observation window
        Mc: completeness magnitude

    Returns:
        Negative log-likelihood (lower is better).
    """
    # Unpack and transform parameters
    log_mu, log_K, alpha, log_c, p = params_vec

    mu = math.exp(log_mu)
    K = math.exp(log_K)
    c = math.exp(log_c)

    # Validate constraints
    if p <= 1.0:
        return 1e20  # ETAS requires p > 1 for convergence
    if alpha < 0 or alpha > 3:
        return 1e20

    n = len(event_times)
    if n < 10:
        return 1e20

    # Term 1: Sum of log(lambda(t_i)) for each event
    log_lambda_sum = 0.0
    for i in range(n):
        ti = event_times[i]
        if ti < T_start or ti > T_end:
            continue

        lam = mu
        # Sum contributions from all prior events (limit to 500 for speed)
        start_j = max(0, i - 500)
        for j in range(start_j, i):
            dt = ti - event_times[j]
            if dt <= 0:
                continue
            lam += K * math.exp(alpha * (event_mags[j] - Mc)) / (dt + c) ** p

        if lam <= 0:
            return 1e20
        log_lambda_sum += math.log(lam)

    # Term 2: Integral of lambda(t) over [T_start, T_end]
    # = mu * (T_end - T_start) + sum of aftershock integrals
    integral = mu * (T_end - T_start)

    for i in range(n):
        ti = event_times[i]
        mi = event_mags[i]
        if ti >= T_end:
            break

        # Integral of K*exp(alpha*(m-Mc)) / (t-ti+c)^p from max(ti, T_start) to T_end
        t_lo = max(ti, T_start)
        t_hi = T_end
        dt_lo = t_lo - ti + c
        dt_hi = t_hi - ti + c

        if dt_lo <= 0:
            dt_lo = c

        productivity = K * math.exp(alpha * (mi - Mc))

        if abs(p - 1.0) < 0.01:
            # p ≈ 1: integral is log
            aftershock_integral = productivity * (math.log(dt_hi) - math.log(dt_lo))
        else:
            aftershock_integral = productivity / (1 - p) * (
                dt_hi ** (1 - p) - dt_lo ** (1 - p)
            )

        integral += max(aftershock_integral, 0)

    # Negative log-likelihood
    nll = integral - log_lambda_sum
    return nll


def fit_etas_mle(
    event_times: list,
    event_mags: list,
    T_start: float,
    T_end: float,
    Mc: float = 3.0,
    max_events: int = 5000,
) -> dict:
    """Fit ETAS parameters via Maximum Likelihood Estimation.

    Uses scipy.optimize.minimize with L-BFGS-B.
    Parameters are log-transformed for positivity constraints.

    Args:
        event_times: list of event times (days since epoch)
        event_mags: list of magnitudes (>= Mc)
        T_start, T_end: observation window
        Mc: completeness magnitude
        max_events: limit events for computational efficiency

    Returns:
        dict with fitted parameters and diagnostics
    """
    try:
        from scipy.optimize import minimize
    except ImportError:
        logger.warning("scipy not available, using default ETAS parameters")
        return {
            "fitted": False,
            "params": ETAS_DEFAULTS.copy(),
            "reason": "scipy_not_available",
        }

    # Subsample if too many events (use last max_events)
    if len(event_times) > max_events:
        offset = len(event_times) - max_events
        event_times = event_times[offset:]
        event_mags = event_mags[offset:]
        T_start = max(T_start, event_times[0])

    n = len(event_times)
    duration = T_end - T_start
    if n < 50 or duration < 30:
        logger.warning("Insufficient data for ETAS MLE (%d events, %.0f days)", n, duration)
        return {
            "fitted": False,
            "params": ETAS_DEFAULTS.copy(),
            "reason": "insufficient_data",
        }

    # Initial guess (log-transformed)
    mu_init = 0.3 * n / duration  # ~30% background
    x0 = [
        math.log(mu_init),    # log_mu
        math.log(0.04),       # log_K
        1.0,                  # alpha
        math.log(0.01),       # log_c
        1.1,                  # p
    ]

    bounds = [
        (math.log(0.001), math.log(100)),   # log_mu
        (math.log(0.001), math.log(1.0)),   # log_K
        (0.1, 2.5),                         # alpha
        (math.log(0.0001), math.log(1.0)),  # log_c
        (1.001, 2.0),                       # p (must be > 1)
    ]

    logger.info("  ETAS MLE: fitting %d events over %.0f days...", n, duration)

    result = minimize(
        etas_log_likelihood,
        x0,
        args=(event_times, event_mags, T_start, T_end, Mc),
        method="L-BFGS-B",
        bounds=bounds,
        options={"maxiter": 200, "ftol": 1e-8},
    )

    if not result.success:
        logger.warning("  ETAS MLE did not converge: %s", result.message)
        return {
            "fitted": False,
            "params": ETAS_DEFAULTS.copy(),
            "reason": f"no_convergence: {result.message}",
            "nll": float(result.fun),
        }

    # Extract fitted parameters
    log_mu, log_K, alpha, log_c, p = result.x
    fitted = {
        "mu": math.exp(log_mu),
        "K": math.exp(log_K),
        "alpha": alpha,
        "c": math.exp(log_c),
        "p": p,
        "Mc": Mc,
    }

    # Compute AIC
    n_params = 5
    aic = 2 * result.fun + 2 * n_params

    # Branching ratio: n_bar = K * integral(exp(alpha*(m-Mc)) * f_GR(m)) / (p-1) * c^(1-p)
    # Simplified for GR with b=1: n_bar ≈ K / (p - 1) * c^(1-p) * 1/(alpha - b*ln10)
    # This is approximate; use empirical ratio instead
    n_bar_empirical = 1.0 - fitted["mu"] * duration / max(n, 1)

    logger.info("  ETAS MLE converged: mu=%.4f K=%.4f alpha=%.2f c=%.4f p=%.3f",
                fitted["mu"], fitted["K"], fitted["alpha"], fitted["c"], fitted["p"])
    logger.info("  NLL=%.1f AIC=%.1f branching_ratio=%.2f", result.fun, aic, n_bar_empirical)

    return {
        "fitted": True,
        "params": fitted,
        "nll": float(result.fun),
        "aic": float(aic),
        "branching_ratio": round(n_bar_empirical, 3),
        "n_events": n,
        "duration_days": round(duration, 1),
        "n_iterations": result.nit,
    }


# ---------------------------------------------------------------------------
# b-value estimation
# ---------------------------------------------------------------------------

def b_value_aki(mags: list, mc: float = 3.0, min_count: int = 20) -> Optional[float]:
    """Gutenberg-Richter b-value via Aki-Utsu maximum likelihood.

    b = log10(e) / (<M> - (Mc - dM/2))
    where dM = 0.1 (magnitude binning).

    Returns None if insufficient data.
    """
    filtered = [m for m in mags if m >= mc]
    if len(filtered) < min_count:
        return None
    m_mean = sum(filtered) / len(filtered)
    denominator = m_mean - (mc - 0.05)
    if denominator <= 0.01:
        return None
    return math.log10(math.e) / denominator


def b_value_with_uncertainty(mags: list, mc: float = 3.0, min_count: int = 20):
    """b-value with Shi-Bolt (1982) uncertainty estimate.

    Returns (b, sigma_b) or (None, None).
    """
    filtered = [m for m in mags if m >= mc]
    n = len(filtered)
    if n < min_count:
        return None, None

    m_mean = sum(filtered) / n
    denominator = m_mean - (mc - 0.05)
    if denominator <= 0.01:
        return None, None

    b = math.log10(math.e) / denominator

    # Shi-Bolt variance
    var_m = sum((m - m_mean) ** 2 for m in filtered) / (n * (n - 1))
    sigma_b = 2.30 * b**2 * math.sqrt(var_m) if var_m > 0 else None

    return b, sigma_b


# ---------------------------------------------------------------------------
# Tectonic zone classification for Japan
# ---------------------------------------------------------------------------

# Spatial zones for zone-specific ETAS fitting
JAPAN_TECTONIC_ZONES = {
    "tohoku_offshore": {"lat_min": 35, "lat_max": 42, "lon_min": 140, "lon_max": 148},
    "kanto_tokai": {"lat_min": 33, "lat_max": 36, "lon_min": 137, "lon_max": 142},
    "nankai": {"lat_min": 30, "lat_max": 35, "lon_min": 130, "lon_max": 137},
    "kyushu": {"lat_min": 28, "lat_max": 34, "lon_min": 128, "lon_max": 133},
    "hokkaido": {"lat_min": 41, "lat_max": 46, "lon_min": 140, "lon_max": 148},
    "izu_bonin": {"lat_min": 26, "lat_max": 35, "lon_min": 138, "lon_max": 145},
}


def classify_tectonic_zone(lat: float, lon: float) -> str:
    """Classify location into a tectonic zone.

    Returns the most specific matching zone, or 'other' if no match.
    Priority order handles overlapping zones.
    """
    for zone_name, bbox in JAPAN_TECTONIC_ZONES.items():
        if (bbox["lat_min"] <= lat <= bbox["lat_max"] and
                bbox["lon_min"] <= lon <= bbox["lon_max"]):
            return zone_name
    return "other"
