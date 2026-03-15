"""Coulomb stress transfer analysis for earthquake interaction.

Computes Coulomb Failure Stress (CFS) changes imparted by each M5+
earthquake on subsequent earthquake locations, using the Okada (1992)
dislocation model. Tests whether subsequent M5+ events preferentially
occur in stress-enhanced zones.

Physics:
    delta_CFS = delta_tau + mu' * delta_sigma_n
    where delta_tau = shear stress change (positive in slip direction)
          delta_sigma_n = normal stress change (unclamping positive)
          mu' = effective friction coefficient (0.4)

References:
    - Okada (1992) BSSA 82:1018-1040
    - Toda & Stein (2011) EPS 63:171-185
    - Wells & Coppersmith (1994) BSSA 84:974-1002
"""

import argparse
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent.parent / "results"

# Elastic parameters (standard for crustal studies)
SHEAR_MODULUS = 32e9  # Pa (32 GPa)
POISSON_RATIO = 0.25
LAME_LAMBDA = SHEAR_MODULUS * 2 * POISSON_RATIO / (1 - 2 * POISSON_RATIO)
MU_FRICTION = 0.4  # effective friction coefficient

# Earth parameters
DEG_TO_KM = 111.32  # approximate km per degree latitude
DEG_TO_M = DEG_TO_KM * 1000


# ---------------------------------------------------------------------------
# Wells & Coppersmith (1994) scaling relations
# ---------------------------------------------------------------------------

def fault_dimensions(mw: float, fault_type: str = "reverse") -> dict:
    """Estimate fault length, width, and slip from magnitude.

    Returns dict with length_km, width_km, slip_m.
    Uses Wells & Coppersmith (1994) empirical relations.
    """
    # Coefficients: (a, b) for log10(X) = a + b * M
    # Using "all fault types" for generality, with reverse as default
    scaling = {
        "reverse": {
            "length": (-2.86, 0.63),
            "width": (-1.61, 0.41),
        },
        "strike_slip": {
            "length": (-3.55, 0.74),
            "width": (-0.76, 0.27),
        },
        "normal": {
            "length": (-2.01, 0.50),
            "width": (-1.14, 0.35),
        },
        "all": {
            "length": (-2.44, 0.59),
            "width": (-1.01, 0.32),
        },
    }

    s = scaling.get(fault_type, scaling["all"])
    length_km = 10 ** (s["length"][0] + s["length"][1] * mw)
    width_km = 10 ** (s["width"][0] + s["width"][1] * mw)

    # Slip from seismic moment: M0 = mu * L * W * D
    m0 = 10 ** (1.5 * mw + 9.05)  # N-m
    slip_m = m0 / (SHEAR_MODULUS * length_km * 1000 * width_km * 1000)

    return {
        "length_km": round(length_km, 2),
        "width_km": round(width_km, 2),
        "slip_m": round(slip_m, 3),
    }


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
# Okada (1992) — simplified analytical solution
# ---------------------------------------------------------------------------

def okada_stress(
    source_lat: float, source_lon: float, source_depth_km: float,
    strike_deg: float, dip_deg: float, rake_deg: float,
    length_km: float, width_km: float, slip_m: float,
    obs_lat: float, obs_lon: float, obs_depth_km: float,
) -> tuple[float, float, float]:
    """Compute stress tensor at observation point from a rectangular dislocation.

    Simplified Okada solution for a finite rectangular fault in an elastic
    half-space. Returns (sigma_xx, sigma_yy, sigma_xy) in Pa.

    This is a far-field approximation valid when distance >> fault dimensions.
    For near-field, a full DC3D implementation would be needed.

    Returns: (delta_tau, delta_sigma_n, delta_cfs) in Pa on an optimally
    oriented receiver fault.
    """
    # Convert to local Cartesian coordinates (x=East, y=North, z=Up)
    dx_km = (obs_lon - source_lon) * DEG_TO_KM * math.cos(math.radians(source_lat))
    dy_km = (obs_lat - source_lat) * DEG_TO_KM
    dz_km = obs_depth_km - source_depth_km

    # Distance from source centroid
    r_km = math.sqrt(dx_km**2 + dy_km**2 + dz_km**2)
    if r_km < 0.5:  # Avoid singularity near source
        return (0.0, 0.0, 0.0)

    r_m = r_km * 1000

    # Seismic moment
    area_m2 = length_km * 1000 * width_km * 1000
    m0 = SHEAR_MODULUS * area_m2 * slip_m

    # Far-field stress from point double-couple (Aki & Richards, 2002)
    # Stress decays as 1/r^3 for a point source
    strike_r = math.radians(strike_deg)
    dip_r = math.radians(dip_deg)
    rake_r = math.radians(rake_deg)

    # Fault normal vector (pointing into footwall)
    n = [
        -math.sin(dip_r) * math.sin(strike_r),
        math.sin(dip_r) * math.cos(strike_r),
        -math.cos(dip_r),
    ]

    # Slip direction vector in geographic coordinates
    d = [
        math.cos(rake_r) * math.cos(strike_r) + math.sin(rake_r) * math.cos(dip_r) * math.sin(strike_r),
        math.cos(rake_r) * math.sin(strike_r) - math.sin(rake_r) * math.cos(dip_r) * math.cos(strike_r),
        -math.sin(rake_r) * math.sin(dip_r),
    ]

    # Direction to observation point
    if r_m < 1:
        return (0.0, 0.0, 0.0)
    dx_m, dy_m, dz_m = dx_km * 1000, dy_km * 1000, dz_km * 1000
    rhat = [dx_m / r_m, dy_m / r_m, dz_m / r_m]

    # Normalized moment tensor m_ij = (n_i * d_j + n_j * d_i) / 2
    # M0 is factored out into the prefactor (avoid double-counting)
    m = [[0.0] * 3 for _ in range(3)]
    for i in range(3):
        for j in range(3):
            m[i][j] = (n[i] * d[j] + n[j] * d[i]) / 2

    # Far-field stress from point double-couple:
    # sigma_ij = (M0 / (4*pi*r^3)) * [3*(m_kl*rhat_k*rhat_l)*rhat_i*rhat_j - m_ij]
    prefactor = m0 / (4 * math.pi * r_m**3)

    m_rr = sum(m[k][l] * rhat[k] * rhat[l] for k in range(3) for l in range(3))

    stress = [[0.0] * 3 for _ in range(3)]
    for i in range(3):
        for j in range(3):
            stress[i][j] = prefactor * (
                3 * m_rr * rhat[i] * rhat[j]
                - m[i][j]
                - (1 if i == j else 0) * m_rr
            )

    # For CFS on optimally oriented fault (King et al., 1994):
    # Use the maximum shear stress and mean normal stress
    # sigma_mean = (stress_xx + stress_yy + stress_zz) / 3
    sigma_mean = (stress[0][0] + stress[1][1] + stress[2][2]) / 3

    # Deviatoric stress
    dev = [[stress[i][j] - (sigma_mean if i == j else 0) for j in range(3)] for i in range(3)]

    # Maximum shear stress (second invariant of deviatoric stress)
    j2 = 0.5 * sum(dev[i][j] ** 2 for i in range(3) for j in range(3))
    tau_max = math.sqrt(max(j2, 0))

    # Coulomb stress on optimally oriented plane
    delta_cfs = tau_max + MU_FRICTION * sigma_mean

    return (tau_max, sigma_mean, delta_cfs)


# ---------------------------------------------------------------------------
# Regional default mechanisms for Japan
# ---------------------------------------------------------------------------

def default_mechanism(lat: float, lon: float, depth_km: float) -> tuple[float, float, float]:
    """Return default (strike, dip, rake) for a region without CMT solution.

    Based on Japan's tectonic setting:
    - Pacific plate subduction: Tohoku offshore, reverse
    - Philippine Sea plate: Nankai/Tokai, reverse
    - Shallow crustal: varies by region
    - Deep intraslab: down-dip compression
    """
    if depth_km > 70:
        # Deep intraslab: down-dip compression
        return (200.0, 45.0, 90.0)
    elif lon > 142 and lat > 35:
        # Tohoku offshore plate interface
        return (200.0, 25.0, 90.0)
    elif lon < 137 and lat < 35:
        # Nankai/SW Japan
        return (240.0, 15.0, 90.0)
    elif lat > 40:
        # Northern Honshu inland
        return (200.0, 40.0, 90.0)
    elif 130 < lon < 132 and lat < 34:
        # Kyushu extensional
        return (30.0, 60.0, -90.0)
    else:
        # Default: E-W compression (reverse)
        return (200.0, 35.0, 90.0)


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

async def run_coulomb_analysis(min_mag: float = 5.0) -> dict:
    """Run Coulomb stress transfer analysis."""
    logger.info("=== Coulomb Stress Transfer Analysis (min_mag=%.1f) ===", min_mag)

    async with aiosqlite.connect(DB_PATH) as db:
        # Load focal mechanisms
        fm_rows = await db.execute_fetchall(
            "SELECT event_id, occurred_at, latitude, longitude, depth_km, "
            "magnitude, strike1, dip1, rake1, strike2, dip2, rake2, moment_nm "
            "FROM focal_mechanisms WHERE magnitude >= ? ORDER BY occurred_at",
            (min_mag,),
        )
        logger.info("Focal mechanisms available: %d (M%.1f+)", len(fm_rows), min_mag)

        # Load all M5+ earthquakes
        eq_rows = await db.execute_fetchall(
            "SELECT occurred_at, latitude, longitude, depth_km, magnitude "
            "FROM earthquakes WHERE magnitude >= ? ORDER BY occurred_at",
            (min_mag,),
        )
        logger.info("Target earthquakes: %d", len(eq_rows))

    # Build FM lookup by approximate time+location matching
    fm_lookup = {}
    for fm in fm_rows:
        key = (round(fm[2], 1), round(fm[3], 1), fm[0])  # (lat, lon, event_id)
        fm_lookup[key] = {
            "strike": fm[6], "dip": fm[7], "rake": fm[8],
            "strike2": fm[9], "dip2": fm[10], "rake2": fm[11],
            "moment_nm": fm[12],
        }

    # Parse earthquakes
    events = []
    for r in eq_rows:
        try:
            t = datetime.fromisoformat(r[0].replace("Z", "+00:00"))
            events.append({
                "time": t,
                "time_str": r[0],
                "lat": r[1],
                "lon": r[2],
                "depth_km": r[3] if r[3] is not None else 10.0,
                "mag": r[4],
            })
        except (ValueError, TypeError):
            continue

    if len(events) < 10:
        return {"error": "Insufficient events", "n": len(events)}

    # Match CMT to earthquakes (spatial proximity)
    def find_fm(lat, lon, depth):
        """Find best matching focal mechanism."""
        # Direct lookup
        key = (round(lat, 1), round(lon, 1))
        for k, v in fm_lookup.items():
            if abs(k[0] - lat) < 0.3 and abs(k[1] - lon) < 0.3:
                return v
        return None

    # Assign mechanism to each event
    n_with_cmt = 0
    n_default = 0
    for e in events:
        fm = find_fm(e["lat"], e["lon"], e["depth_km"])
        if fm:
            e["strike"] = fm["strike"]
            e["dip"] = fm["dip"]
            e["rake"] = fm["rake"]
            e["has_cmt"] = True
            n_with_cmt += 1
        else:
            s, d, r = default_mechanism(e["lat"], e["lon"], e["depth_km"])
            e["strike"] = s
            e["dip"] = d
            e["rake"] = r
            e["has_cmt"] = False
            n_default += 1

        ftype = classify_fault_type(e["rake"])
        dims = fault_dimensions(e["mag"], ftype)
        e["length_km"] = dims["length_km"]
        e["width_km"] = dims["width_km"]
        e["slip_m"] = dims["slip_m"]

    logger.info("  CMT matched: %d, Default mechanism: %d", n_with_cmt, n_default)

    # ---------------------------------------------------------------
    # Core analysis: For each subsequent earthquake, compute cumulative
    # CFS from all prior earthquakes
    # ---------------------------------------------------------------
    logger.info("Computing Coulomb stress at each event location...")

    cfs_at_events = []  # CFS at each event from all prior events
    positive_cfs_count = 0
    total_computed = 0

    # Limit to manageable subset for initial analysis
    # Use all events but skip pairs > 500 km apart (stress negligible)
    MAX_DISTANCE_KM = 500.0

    for i, target in enumerate(events):
        cumulative_cfs = 0.0
        n_sources = 0

        for j in range(i):
            source = events[j]

            # Quick distance check
            dlat = abs(target["lat"] - source["lat"])
            dlon = abs(target["lon"] - source["lon"])
            if dlat * DEG_TO_KM > MAX_DISTANCE_KM or dlon * DEG_TO_KM > MAX_DISTANCE_KM:
                continue

            # Time window: only consider sources within 10 years
            dt = (target["time"] - source["time"]).total_seconds()
            if dt > 10 * 365.25 * 86400:
                continue

            # Compute CFS
            tau, sigma_n, cfs = okada_stress(
                source["lat"], source["lon"], source["depth_km"],
                source["strike"], source["dip"], source["rake"],
                source["length_km"], source["width_km"], source["slip_m"],
                target["lat"], target["lon"], target["depth_km"],
            )

            cumulative_cfs += cfs
            n_sources += 1

        if n_sources > 0:
            cfs_at_events.append({
                "time": target["time_str"][:16],
                "mag": target["mag"],
                "lat": target["lat"],
                "lon": target["lon"],
                "depth_km": target["depth_km"],
                "cumulative_cfs_pa": round(cumulative_cfs, 2),
                "cumulative_cfs_kpa": round(cumulative_cfs / 1000, 3),
                "n_sources": n_sources,
                "has_cmt": target["has_cmt"],
            })
            if cumulative_cfs > 0:
                positive_cfs_count += 1
            total_computed += 1

        if (i + 1) % 200 == 0:
            logger.info("  Processed %d/%d events", i + 1, len(events))

    logger.info("  Done: %d events with CFS computed", total_computed)

    # ---------------------------------------------------------------
    # Statistical analysis
    # ---------------------------------------------------------------

    if total_computed < 10:
        return {"error": "Too few events with CFS", "n": total_computed}

    cfs_values = [e["cumulative_cfs_kpa"] for e in cfs_at_events]
    positive_pct = 100 * positive_cfs_count / total_computed

    # Compare with random locations
    import random
    random.seed(42)
    random_cfs = []
    n_random_positive = 0
    n_random = min(500, total_computed)

    for _ in range(n_random):
        # Random location within Japan
        rlat = 25 + random.random() * 20
        rlon = 125 + random.random() * 25
        rdepth = 10 + random.random() * 50
        # Pick random time within data range
        t_idx = random.randint(len(events) // 4, len(events) - 1)
        target_time = events[t_idx]["time"]

        cum_cfs = 0.0
        n_src = 0
        for j in range(t_idx):
            source = events[j]
            dlat = abs(rlat - source["lat"])
            dlon = abs(rlon - source["lon"])
            if dlat * DEG_TO_KM > MAX_DISTANCE_KM or dlon * DEG_TO_KM > MAX_DISTANCE_KM:
                continue
            dt = (target_time - source["time"]).total_seconds()
            if dt > 10 * 365.25 * 86400:
                continue

            _, _, cfs = okada_stress(
                source["lat"], source["lon"], source["depth_km"],
                source["strike"], source["dip"], source["rake"],
                source["length_km"], source["width_km"], source["slip_m"],
                rlat, rlon, rdepth,
            )
            cum_cfs += cfs
            n_src += 1

        if n_src > 0:
            random_cfs.append(round(cum_cfs / 1000, 3))
            if cum_cfs > 0:
                n_random_positive += 1

    random_positive_pct = 100 * n_random_positive / max(len(random_cfs), 1) if random_cfs else 0

    # CFS distribution statistics
    def stats(values):
        if not values:
            return {"n": 0}
        s = sorted(values)
        n = len(s)
        return {
            "n": n,
            "mean_kpa": round(sum(s) / n, 3),
            "median_kpa": round(s[n // 2], 3),
            "p10": round(s[int(n * 0.1)], 3),
            "p25": round(s[int(n * 0.25)], 3),
            "p75": round(s[int(n * 0.75)], 3),
            "p90": round(s[int(n * 0.9)], 3),
            "positive_pct": round(sum(1 for v in s if v > 0) / n * 100, 1),
            "gt_10kpa_pct": round(sum(1 for v in s if v > 10) / n * 100, 1),
            "gt_100kpa_pct": round(sum(1 for v in s if v > 100) / n * 100, 1),
        }

    # Magnitude-dependent analysis
    mag_bins = {}
    for e in cfs_at_events:
        bin_label = f"M{int(e['mag'])}"
        mag_bins.setdefault(bin_label, []).append(e["cumulative_cfs_kpa"])

    # Time-dependent analysis (does CFS increase before larger events?)
    cfs_by_cmt = {
        "with_cmt": [e["cumulative_cfs_kpa"] for e in cfs_at_events if e["has_cmt"]],
        "without_cmt": [e["cumulative_cfs_kpa"] for e in cfs_at_events if not e["has_cmt"]],
    }

    # Lift calculation
    lift = positive_pct / max(random_positive_pct, 0.1)

    results = {
        "summary": {
            "n_events": total_computed,
            "n_random": len(random_cfs),
            "n_focal_mechanisms": n_with_cmt,
            "n_default_mechanisms": n_default,
            "max_distance_km": MAX_DISTANCE_KM,
            "friction_coefficient": MU_FRICTION,
        },
        "earthquake_locations": stats(cfs_values),
        "random_locations": stats(random_cfs),
        "lift_positive_cfs": round(lift, 2),
        "by_magnitude": {k: stats(v) for k, v in sorted(mag_bins.items())},
        "by_cmt_availability": {k: stats(v) for k, v in cfs_by_cmt.items()},
        "sample_events": cfs_at_events[:30],
    }

    logger.info("  Earthquake locations: CFS>0 = %.1f%%, mean = %.3f kPa",
                positive_pct, sum(cfs_values) / len(cfs_values))
    logger.info("  Random locations:     CFS>0 = %.1f%%, mean = %.3f kPa",
                random_positive_pct, sum(random_cfs) / len(random_cfs) if random_cfs else 0)
    logger.info("  Lift (positive CFS): %.2f", lift)

    return results


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-mag", type=float, default=5.0)
    args = parser.parse_args()

    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results = await run_coulomb_analysis(args.min_mag)

    out_path = RESULTS_DIR / f"coulomb_{timestamp}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("Results saved to %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
