"""Compute lunar and solar tidal stress for earthquake precursor analysis.

Pure astronomical calculation — no external data fetched. All celestial
mechanics computed from analytical formulas (Jean Meeus, "Astronomical
Algorithms") using only the standard library.

Physical mechanism:
    The Moon and Sun exert tidal forces on Earth's crust, creating periodic
    stress variations of ~1-4 kPa. While small compared to tectonic stress
    (MPa), tidal stress can trigger earthquakes on faults already near
    failure threshold — the "last straw" effect. Cochran et al. (2004)
    found statistical correlation between tidal phase and shallow thrust
    earthquakes.

    Novel aspect: instead of simple lunar phase proxy, we compute actual
    tidal shear stress tensor including lunar distance variation (+/-6%)
    and solar contribution.

Computation approach (Meeus simplified):
    1. Julian Day Number from calendar date
    2. Moon position: ecliptic longitude, latitude, distance via
       Brown's lunar theory (low-precision, ~1 deg accuracy)
    3. Sun position: ecliptic longitude from mean anomaly
    4. Tidal shear stress at observation point:
       tau = -(3/2) * g * h2 * (M/M_earth) * (R_earth/d)^3 * sin(2*theta)
       where theta is zenith angle of body, h2 is Love number
    5. Tidal normal stress:
       sigma = g * h2 * (M/M_earth) * (R_earth/d)^3 * (3*cos^2(theta) - 1)

Observation point: center of Japan (35 deg N, 137 deg E)

References:
    - Meeus, J. (1998) Astronomical Algorithms, 2nd ed.
    - Cochran et al. (2004) Science 306:1164-1166
    - Tanaka et al. (2002) Geophys. Res. Lett. 29(11):1529
"""

import asyncio
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

START_YEAR = 2011

# --- Physical constants ---
G = 6.674e-11          # gravitational constant (N m^2 / kg^2)
M_MOON = 7.342e22      # lunar mass (kg)
M_SUN = 1.989e30       # solar mass (kg)
M_EARTH = 5.972e24     # Earth mass (kg)
R_EARTH = 6.371e6      # mean Earth radius (m)
g_SURFACE = 9.80665    # surface gravity (m/s^2)

# Love numbers (degree-2)
H2 = 0.609             # radial displacement Love number
L2 = 0.0847            # tangential displacement Love number

# Observation point: center of Japan
OBS_LAT = 35.0         # degrees N
OBS_LON = 137.0        # degrees E

DEG2RAD = math.pi / 180.0
RAD2DEG = 180.0 / math.pi


# =========================================================================
# Julian Day and centuries from J2000.0 (Meeus Ch.7)
# =========================================================================

def julian_day(year: int, month: int, day: int, hour: float = 0.0) -> float:
    """Compute Julian Day Number for a given date/time (UT).

    Meeus, Astronomical Algorithms, Ch. 7, Eq. 7.1.
    Valid for Gregorian calendar dates.
    """
    if month <= 2:
        year -= 1
        month += 12
    A = int(year / 100)
    B = 2 - A + int(A / 4)
    return (int(365.25 * (year + 4716))
            + int(30.6001 * (month + 1))
            + day + hour / 24.0 + B - 1524.5)


def julian_centuries(jd: float) -> float:
    """Julian centuries from J2000.0 epoch."""
    return (jd - 2451545.0) / 36525.0


# =========================================================================
# Solar position (Meeus Ch.25, low-precision)
# =========================================================================

def sun_position(T: float) -> tuple[float, float]:
    """Compute Sun's geocentric ecliptic longitude and distance.

    Args:
        T: Julian centuries from J2000.0

    Returns:
        (ecliptic_longitude_deg, distance_m)

    Accuracy: ~0.01 deg in longitude, ~0.01 AU in distance.
    Meeus Ch.25, using geometric mean longitude and mean anomaly.
    """
    # Geometric mean longitude (deg)
    L0 = (280.46646 + T * (36000.76983 + T * 0.0003032)) % 360.0
    # Mean anomaly (deg)
    M = (357.52911 + T * (35999.05029 - T * 0.0001537)) % 360.0
    M_rad = M * DEG2RAD
    # Equation of center
    C = ((1.914602 - T * (0.004817 + T * 0.000014)) * math.sin(M_rad)
         + (0.019993 - T * 0.000101) * math.sin(2 * M_rad)
         + 0.000289 * math.sin(3 * M_rad))
    # Sun's true longitude
    sun_lon = (L0 + C) % 360.0

    # Eccentricity of Earth's orbit
    e = 0.016708634 - T * (0.000042037 + T * 0.0000001267)
    # True anomaly
    v = M + C
    v_rad = v * DEG2RAD
    # Distance in AU
    R_au = 1.000001018 * (1.0 - e * e) / (1.0 + e * math.cos(v_rad))
    # Convert AU to meters
    R_m = R_au * 1.496e11

    return sun_lon, R_m


# =========================================================================
# Lunar position (Meeus Ch.47, simplified Brown's theory)
# =========================================================================

def moon_position(T: float) -> tuple[float, float, float]:
    """Compute Moon's geocentric ecliptic longitude, latitude, and distance.

    Simplified version of Meeus Ch.47 using the most significant periodic
    terms from Brown's lunar theory. Accuracy ~1 deg in longitude,
    ~0.5 deg in latitude, ~1000 km in distance. Sufficient for tidal
    stress computation.

    Args:
        T: Julian centuries from J2000.0

    Returns:
        (ecliptic_longitude_deg, ecliptic_latitude_deg, distance_m)
    """
    # Fundamental arguments (degrees)
    # Mean longitude of Moon
    Lp = (218.3164477 + T * (481267.88123421
          - T * (0.0015786 - T * (1.0 / 538841.0
          - T / 65194000.0)))) % 360.0
    # Mean elongation of Moon
    D = (297.8501921 + T * (445267.1114034
         - T * (0.0018819 - T * (1.0 / 545868.0
         + T / 113065000.0)))) % 360.0
    # Sun's mean anomaly
    M = (357.5291092 + T * (35999.0502909
         - T * (0.0001536 + T / 24490000.0))) % 360.0
    # Moon's mean anomaly
    Mp = (134.9633964 + T * (477198.8675055
          + T * (0.0087414 + T * (1.0 / 69699.0
          - T / 14712000.0)))) % 360.0
    # Moon's argument of latitude
    F = (93.2720950 + T * (483202.0175233
         - T * (0.0036539 - T * (1.0 / 3526000.0
         + T / 863310000.0)))) % 360.0

    D_r = D * DEG2RAD
    M_r = M * DEG2RAD
    Mp_r = Mp * DEG2RAD
    F_r = F * DEG2RAD

    # Eccentricity correction
    E = 1.0 - 0.002516 * T - 0.0000074 * T * T

    # --- Longitude terms (most significant from Meeus Table 47.A) ---
    # Each entry: (D_mult, M_mult, Mp_mult, F_mult, sin_coeff)
    lon_terms = [
        (0, 0, 1, 0, 6288774),
        (2, 0, -1, 0, 1274027),
        (2, 0, 0, 0, 658314),
        (0, 0, 2, 0, 213618),
        (0, 1, 0, 0, -185116),
        (0, 0, 0, 2, -114332),
        (2, 0, -2, 0, 58793),
        (2, -1, -1, 0, 57066),
        (2, 0, 1, 0, 53322),
        (2, -1, 0, 0, 45758),
        (0, 1, -1, 0, -40923),
        (1, 0, 0, 0, -34720),
        (0, 1, 1, 0, -30383),
        (2, 0, 0, -2, 15327),
        (0, 0, 1, 2, -12528),
        (0, 0, 1, -2, 10980),
        (4, 0, -1, 0, 10675),
        (0, 0, 3, 0, 10034),
        (4, 0, -2, 0, 8548),
        (2, 1, -1, 0, -7888),
        (2, 1, 0, 0, -6766),
        (1, 0, -1, 0, -5163),
        (1, 1, 0, 0, 4987),
        (2, -1, 1, 0, 4036),
        (2, 0, 2, 0, 3994),
        (4, 0, 0, 0, 3861),
        (2, 0, -3, 0, 3665),
        (0, 1, -2, 0, -2689),
        (2, 0, -1, 2, -2602),
        (2, -1, -2, 0, 2390),
        (1, 0, 1, 0, -2348),
        (2, -2, 0, 0, 2236),
        (0, 1, 2, 0, -2120),
        (0, 2, 0, 0, -2069),
        (2, -2, -1, 0, 2048),
        (2, 0, 1, -2, -1773),
        (2, 0, 0, 2, -1595),
        (4, -1, -1, 0, 1215),
        (0, 0, 2, 2, -1110),
        (3, 0, -1, 0, -892),
        (2, 1, 1, 0, -810),
        (4, -1, -2, 0, 759),
        (0, 2, -1, 0, -713),
        (2, 2, -1, 0, -700),
        (2, 1, -2, 0, 691),
        (2, -1, 0, -2, 596),
        (4, 0, 1, 0, 549),
        (0, 0, 4, 0, 537),
        (4, -1, 0, 0, 520),
        (1, 0, -2, 0, -487),
    ]

    sigma_l = 0.0
    for d, m, mp, f, coeff in lon_terms:
        arg = d * D_r + m * M_r + mp * Mp_r + f * F_r
        e_factor = E ** abs(m)
        sigma_l += coeff * e_factor * math.sin(arg)

    # Add additional term
    sigma_l += 3958.0 * math.sin((119.75 + 131.849 * T) * DEG2RAD)
    sigma_l += 1962.0 * math.sin((Lp - F) * DEG2RAD)
    sigma_l += 318.0 * math.sin((53.09 + 479264.290 * T) * DEG2RAD)

    moon_lon = Lp + sigma_l / 1.0e6

    # --- Latitude terms (most significant from Meeus Table 47.B) ---
    lat_terms = [
        (0, 0, 0, 1, 5128122),
        (0, 0, 1, 1, 280602),
        (0, 0, 1, -1, 277693),
        (2, 0, 0, -1, 173237),
        (2, 0, -1, 1, 55413),
        (2, 0, -1, -1, 46271),
        (2, 0, 0, 1, 32573),
        (0, 0, 2, 1, 17198),
        (2, 0, 1, -1, 9266),
        (0, 0, 2, -1, 8822),
        (2, -1, 0, -1, 8216),
        (2, 0, -2, -1, 4324),
        (2, 0, 1, 1, 4200),
        (2, 1, 0, -1, -3359),
        (2, -1, -1, 1, 2463),
        (2, -1, 0, 1, 2211),
        (2, -1, -1, -1, 2065),
        (0, 1, -1, -1, -1870),
        (4, 0, -1, -1, 1828),
        (0, 1, 0, 1, -1794),
        (0, 0, 0, 3, -1749),
        (0, 1, -1, 1, -1565),
        (1, 0, 0, 1, -1491),
        (0, 1, 1, 1, -1475),
        (0, 1, 1, -1, -1410),
        (0, 1, 0, -1, -1344),
        (1, 0, 0, -1, -1335),
        (0, 0, 3, 1, 1107),
        (4, 0, 0, -1, 1021),
        (4, 0, -1, 1, 833),
    ]

    sigma_b = 0.0
    for d, m, mp, f, coeff in lat_terms:
        arg = d * D_r + m * M_r + mp * Mp_r + f * F_r
        e_factor = E ** abs(m)
        sigma_b += coeff * e_factor * math.sin(arg)

    sigma_b += -2235.0 * math.sin(Lp * DEG2RAD)
    sigma_b += 382.0 * math.sin((313.45 + 481266.484 * T) * DEG2RAD)
    sigma_b += 175.0 * math.sin(((119.75 + 131.849 * T) - F) * DEG2RAD)
    sigma_b += 175.0 * math.sin(((119.75 + 131.849 * T) + F) * DEG2RAD)
    sigma_b += 127.0 * math.sin((Lp - Mp) * DEG2RAD)
    sigma_b += -115.0 * math.sin((Lp + Mp) * DEG2RAD)

    moon_lat = sigma_b / 1.0e6

    # --- Distance terms (most significant from Meeus Table 47.A cosine) ---
    dist_terms = [
        (0, 0, 1, 0, -20905355),
        (2, 0, -1, 0, -3699111),
        (2, 0, 0, 0, -2955968),
        (0, 0, 2, 0, -569925),
        (0, 1, 0, 0, 48888),
        (0, 0, 0, 2, -3149),
        (2, 0, -2, 0, 246158),
        (2, -1, -1, 0, -152138),
        (2, 0, 1, 0, -170733),
        (2, -1, 0, 0, -204586),
        (0, 1, -1, 0, -129620),
        (1, 0, 0, 0, 108743),
        (0, 1, 1, 0, 104755),
        (2, 0, 0, -2, 10321),
        (0, 0, 1, -2, 0),
        (0, 0, 1, 2, -34782),
        (4, 0, -1, 0, -23210),
        (0, 0, 3, 0, -21636),
        (4, 0, -2, 0, 24208),
        (2, 1, -1, 0, -22003),  # corr: need E factor for M=1
        (2, 1, 0, 0, 16322),
        (1, 0, -1, 0, -12831),
        (1, 1, 0, 0, -10445),
        (2, -1, 1, 0, -11650),
        (2, 0, 2, 0, 14403),
        (4, 0, 0, 0, -7003),
        (2, 0, -3, 0, -10056),
        (0, 1, -2, 0, 6322),
        (2, 0, -1, 2, -9884),
        (2, -1, -2, 0, 5751),
    ]

    sigma_r = 0.0
    for d, m, mp, f, coeff in dist_terms:
        arg = d * D_r + m * M_r + mp * Mp_r + f * F_r
        e_factor = E ** abs(m)
        sigma_r += coeff * e_factor * math.cos(arg)

    # Distance in km, then convert to meters
    moon_dist_km = 385000.56 + sigma_r / 1000.0
    moon_dist_m = moon_dist_km * 1000.0

    return moon_lon % 360.0, moon_lat, moon_dist_m


# =========================================================================
# Coordinate transforms
# =========================================================================

def ecliptic_to_equatorial(lon_deg: float, lat_deg: float,
                           T: float) -> tuple[float, float]:
    """Convert ecliptic coordinates to equatorial (RA, Dec).

    Args:
        lon_deg: ecliptic longitude (degrees)
        lat_deg: ecliptic latitude (degrees)
        T: Julian centuries from J2000.0

    Returns:
        (right_ascension_deg, declination_deg)
    """
    # Mean obliquity of ecliptic (Meeus Eq.22.2)
    eps = (23.439291 - 0.0130042 * T
           - 1.64e-7 * T * T + 5.04e-7 * T * T * T)
    eps_r = eps * DEG2RAD
    lon_r = lon_deg * DEG2RAD
    lat_r = lat_deg * DEG2RAD

    sin_lon = math.sin(lon_r)
    cos_lon = math.cos(lon_r)
    sin_lat = math.sin(lat_r)
    cos_lat = math.cos(lat_r)
    sin_eps = math.sin(eps_r)
    cos_eps = math.cos(eps_r)

    # Right ascension
    ra = math.atan2(sin_lon * cos_eps - math.tan(lat_r) * sin_eps, cos_lon)
    # Declination
    dec = math.asin(sin_lat * cos_eps + cos_lat * sin_eps * sin_lon)

    return ra * RAD2DEG % 360.0, dec * RAD2DEG


def zenith_angle(ra_deg: float, dec_deg: float,
                 obs_lat: float, obs_lon: float,
                 gmst_deg: float) -> float:
    """Compute zenith angle of a celestial body at an observer's location.

    Args:
        ra_deg: right ascension (degrees)
        dec_deg: declination (degrees)
        obs_lat: observer latitude (degrees N)
        obs_lon: observer longitude (degrees E)
        gmst_deg: Greenwich Mean Sidereal Time (degrees)

    Returns:
        zenith angle in radians
    """
    # Local hour angle
    ha = (gmst_deg + obs_lon - ra_deg) * DEG2RAD
    lat_r = obs_lat * DEG2RAD
    dec_r = dec_deg * DEG2RAD

    # cos(zenith angle) = sin(lat)*sin(dec) + cos(lat)*cos(dec)*cos(ha)
    cos_z = (math.sin(lat_r) * math.sin(dec_r)
             + math.cos(lat_r) * math.cos(dec_r) * math.cos(ha))
    cos_z = max(-1.0, min(1.0, cos_z))
    return math.acos(cos_z)


def gmst_degrees(jd: float) -> float:
    """Greenwich Mean Sidereal Time in degrees (Meeus Eq.12.4)."""
    T = (jd - 2451545.0) / 36525.0
    gmst = (280.46061837
            + 360.98564736629 * (jd - 2451545.0)
            + 0.000387933 * T * T
            - T * T * T / 38710000.0)
    return gmst % 360.0


# =========================================================================
# Tidal stress computation
# =========================================================================

def compute_tidal_stress(year: int, month: int, day: int) -> dict:
    """Compute combined lunar and solar tidal stress at observation point.

    All computations at 00:00 UTC on the given date.

    Returns dict with:
        - tidal_shear_pa: combined shear stress (Pa)
        - tidal_normal_pa: combined normal stress (Pa)
        - lunar_distance_km: Earth-Moon distance (km)
        - lunar_phase: 0=new, 0.5=full moon (normalized 0-1)
    """
    jd = julian_day(year, month, day, 0.0)
    T = julian_centuries(jd)
    gmst = gmst_degrees(jd)

    # --- Moon ---
    moon_lon, moon_lat, moon_dist_m = moon_position(T)
    moon_ra, moon_dec = ecliptic_to_equatorial(moon_lon, moon_lat, T)
    theta_moon = zenith_angle(moon_ra, moon_dec, OBS_LAT, OBS_LON, gmst)

    # --- Sun ---
    sun_lon, sun_dist_m = sun_position(T)
    sun_ra, sun_dec = ecliptic_to_equatorial(sun_lon, 0.0, T)
    theta_sun = zenith_angle(sun_ra, sun_dec, OBS_LAT, OBS_LON, gmst)

    # --- Tidal shear stress ---
    # tau = -(3/2) * g * h2 * (M/M_earth) * (R_earth/d)^3 * sin(2*theta)
    def shear_stress(mass: float, dist: float, theta: float) -> float:
        ratio = (R_EARTH / dist) ** 3
        return -1.5 * g_SURFACE * H2 * (mass / M_EARTH) * ratio * math.sin(2.0 * theta)

    tau_moon = shear_stress(M_MOON, moon_dist_m, theta_moon)
    tau_sun = shear_stress(M_SUN, sun_dist_m, theta_sun)
    tau_total = tau_moon + tau_sun

    # --- Tidal normal stress ---
    # sigma = g * h2 * (M/M_earth) * (R_earth/d)^3 * (3*cos^2(theta) - 1)
    def normal_stress(mass: float, dist: float, theta: float) -> float:
        ratio = (R_EARTH / dist) ** 3
        return g_SURFACE * H2 * (mass / M_EARTH) * ratio * (3.0 * math.cos(theta) ** 2 - 1.0)

    sigma_moon = normal_stress(M_MOON, moon_dist_m, theta_moon)
    sigma_sun = normal_stress(M_SUN, sun_dist_m, theta_sun)
    sigma_total = sigma_moon + sigma_sun

    # --- Lunar phase ---
    # Elongation = moon_lon - sun_lon; phase = elongation / 360
    elongation = (moon_lon - sun_lon) % 360.0
    lunar_phase = elongation / 360.0  # 0=new, 0.5=full

    return {
        "tidal_shear_pa": tau_total,
        "tidal_normal_pa": sigma_total,
        "lunar_distance_km": moon_dist_m / 1000.0,
        "lunar_phase": lunar_phase,
    }


# =========================================================================
# Database operations
# =========================================================================

async def init_tidal_stress_table():
    """Create tidal stress table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tidal_stress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                tidal_shear_pa REAL,
                tidal_normal_pa REAL,
                lunar_distance_km REAL,
                lunar_phase REAL,
                UNIQUE(observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_tidal_time
            ON tidal_stress(observed_at)
        """)
        await db.commit()


async def main():
    await init_db()
    await init_tidal_stress_table()

    current_date = datetime.now(timezone.utc).date()

    # Check existing data
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT observed_at FROM tidal_stress"
        )
    existing_dates = {row[0] for row in existing}
    logger.info("Tidal stress: %d existing records", len(existing_dates))

    # Build list of dates to compute
    start_date = datetime(START_YEAR, 1, 1).date()
    all_dates = []
    d = start_date
    while d <= current_date:
        date_str = d.strftime("%Y-%m-%dT00:00:00")
        if date_str not in existing_dates:
            all_dates.append(d)
        d += timedelta(days=1)

    if not all_dates:
        logger.info("Tidal stress: all dates already computed, nothing to do")
        return

    logger.info("Tidal stress: computing %d missing dates (%s to %s)",
                len(all_dates),
                all_dates[0].isoformat(),
                all_dates[-1].isoformat())

    # Compute in batches for efficient DB writes
    BATCH_SIZE = 500
    total_computed = 0

    for batch_start in range(0, len(all_dates), BATCH_SIZE):
        batch = all_dates[batch_start:batch_start + BATCH_SIZE]
        rows = []

        for d in batch:
            result = compute_tidal_stress(d.year, d.month, d.day)
            observed_at = d.strftime("%Y-%m-%dT00:00:00")
            rows.append((
                observed_at,
                result["tidal_shear_pa"],
                result["tidal_normal_pa"],
                result["lunar_distance_km"],
                result["lunar_phase"],
            ))

        async with aiosqlite.connect(DB_PATH) as db:
            await db.executemany(
                """INSERT OR IGNORE INTO tidal_stress
                   (observed_at, tidal_shear_pa, tidal_normal_pa,
                    lunar_distance_km, lunar_phase)
                   VALUES (?, ?, ?, ?, ?)""",
                rows,
            )
            await db.commit()

        total_computed += len(rows)
        if total_computed % 1000 == 0 or batch_start + BATCH_SIZE >= len(all_dates):
            logger.info("Tidal stress: computed %d / %d dates",
                        total_computed, len(all_dates))

    logger.info("Tidal stress computation complete: %d records added", total_computed)


if __name__ == "__main__":
    asyncio.run(main())
