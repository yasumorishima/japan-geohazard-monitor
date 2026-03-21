"""Fetch NOAA GOES integral electron flux for earthquake precursor analysis.

Energetic electrons (>=2 MeV) from Earth's radiation belts precipitate into
the atmosphere during geomagnetic disturbances, creating ionization at 50-90 km
altitude. This modifies the global atmospheric electric circuit (fair-weather
field ~100 V/m). Changes in atmospheric conductivity alter the coupling between
ionosphere and lithosphere through the Lithosphere-Atmosphere-Ionosphere
Coupling (LAIC) chain. Pre-seismic radon emission modifies local conductivity,
which interacts with the global circuit modulated by particle precipitation.

Novel aspect:
    While Kp/Dst capture geomagnetic storm intensity, direct electron flux
    captures the actual particle energy input to the atmosphere above Japan —
    more physically relevant for LAIC coupling.

Target features:
    - electron_2mev_max: daily max >=2 MeV integral electron flux (pfu)
    - electron_800kev_max: daily max >=800 keV integral electron flux (pfu)

Data sources:
    - Recent 1 month: NOAA SWPC JSON API
        https://services.swpc.noaa.gov/json/goes/primary/integral-electrons-1-month.json
    - 2017-present: NCEI GOES-R SEISS L2 avg5m netCDF (daily files)
        https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l2/data/mpsh-l2-avg5m_science/
    - 2011-2016: NOAA NCEI GOES SEM archive (CSV, legacy)
        https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/avg/

References:
    - Pulinets & Ouzounov (2011) Adv. Space Res. 47:413-435 (LAIC model)
    - Fidani (2018) Nat. Hazards Earth Syst. Sci. 18:2127-2144
    - Sgrigna et al. (2005) Phys. Earth Planet. Inter. 148:149-159
"""

import asyncio
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import os
import re
import tempfile

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NOAA SWPC JSON endpoints (recent data)
SWPC_ELECTRONS_7DAY = (
    "https://services.swpc.noaa.gov/json/goes/primary/integral-electrons-7-day.json"
)
SWPC_ELECTRONS_1MONTH = (
    "https://services.swpc.noaa.gov/json/goes/primary/integral-electrons-1-month.json"
)
SWPC_ELECTRONS_3DAY = (
    "https://services.swpc.noaa.gov/json/goes/primary/integral-electrons-3-day.json"
)

# NOAA NCEI GOES historical daily averages (up to 2020)
# GOES-16 for 2017-2020, GOES-13/15 for 2011-2016
NCEI_AVG_BASE_URL = (
    "https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/avg/"
)

# NCEI GOES-R SEISS L2 avg5m (2017-present, netCDF, >=2 MeV integral electron)
# GOES-16 science has lag (~6 months); GOES-18 fills the gap
SEISS_L2_SOURCES = [
    # (satellite, base_url, priority) — try science first, then operational
    ("g16_sci", "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/"
                "goes/goes16/l2/data/mpsh-l2-avg5m_science/"),
    ("g18_sci", "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/"
                "goes/goes18/l2/data/mpsh-l2-avg5m_science/"),
    ("g18_ops", "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/"
                "goes/goes18/l2/data/mpsh-l2-avg5m/"),
]
# SEISS data starts from 2017
SEISS_START_YEAR = 2017

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)
TIMEOUT_SHORT = aiohttp.ClientTimeout(total=60, connect=15)
START_YEAR = 2011
# NCEI avg (CSV) archive only goes up to 2020 — no data for 2021+
NCEI_MAX_YEAR = 2016  # Use SEISS L2 for 2017+, CSV only for 2011-2016


async def init_particle_flux_table():
    """Create particle flux table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS particle_flux (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                electron_2mev_max REAL,
                electron_800kev_max REAL,
                received_at TEXT NOT NULL,
                UNIQUE(observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_particle_flux_time
            ON particle_flux(observed_at)
        """)
        await db.commit()


def parse_swpc_electrons(data: list[dict]) -> list[dict]:
    """Parse SWPC integral electron JSON into daily max aggregates.

    Input records have:
        - time_tag: "2026-03-18T00:00:00Z"
        - energy: ">=2 MeV" or ">=10 MeV" etc.
        - flux: float (pfu, particles/cm²/s/sr)

    We aggregate to daily max for >=2 MeV.
    """
    daily = defaultdict(lambda: {"electron_2mev_max": None, "electron_800kev_max": None})

    for record in data:
        time_tag = record.get("time_tag", "")
        energy = record.get("energy", "")
        flux = record.get("flux")

        if not time_tag or flux is None:
            continue

        # Negative flux values indicate missing/bad data
        try:
            flux = float(flux)
        except (TypeError, ValueError):
            continue
        if flux < 0:
            continue

        # Extract date (YYYY-MM-DD)
        date_str = time_tag[:10]

        if ">=2 MeV" in energy or ">= 2 MeV" in energy:
            current = daily[date_str]["electron_2mev_max"]
            if current is None or flux > current:
                daily[date_str]["electron_2mev_max"] = flux

        # Some endpoints provide >=800 keV or similar — capture if available
        if ">=800" in energy or ">= 800" in energy:
            current = daily[date_str]["electron_800kev_max"]
            if current is None or flux > current:
                daily[date_str]["electron_800kev_max"] = flux

    rows = []
    for date_str in sorted(daily):
        vals = daily[date_str]
        if vals["electron_2mev_max"] is not None:
            rows.append({
                "observed_at": f"{date_str}T00:00:00",
                "electron_2mev_max": vals["electron_2mev_max"],
                "electron_800kev_max": vals["electron_800kev_max"],
            })

    return rows


async def fetch_json(session: aiohttp.ClientSession, url: str) -> list[dict] | None:
    """Fetch JSON with retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 404:
                    logger.info("Particle flux: 404 for %s", url)
                    return None
                else:
                    logger.warning(
                        "Particle flux: HTTP %d from %s (attempt %d)",
                        resp.status, url, attempt,
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("Particle flux: %s for %s", type(e).__name__, url)
            await asyncio.sleep(2 ** attempt)

    return None


async def fetch_swpc_recent(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch recent data from all SWPC endpoints and merge."""
    all_rows = []

    # Try 1-month first (most data), fall back to 7-day, then 3-day
    for label, url in [
        ("1-month", SWPC_ELECTRONS_1MONTH),
        ("7-day", SWPC_ELECTRONS_7DAY),
        ("3-day", SWPC_ELECTRONS_3DAY),
    ]:
        data = await fetch_json(session, url)
        if data:
            rows = parse_swpc_electrons(data)
            logger.info("SWPC %s: %d daily records parsed", label, len(rows))
            all_rows.extend(rows)
            # 1-month is superset; if successful, skip shorter endpoints
            if label == "1-month" and rows:
                break

    # Deduplicate: keep the entry with highest flux per date
    merged = {}
    for row in all_rows:
        key = row["observed_at"]
        if key not in merged:
            merged[key] = row
        else:
            existing = merged[key]
            if (row["electron_2mev_max"] or 0) > (existing["electron_2mev_max"] or 0):
                merged[key]["electron_2mev_max"] = row["electron_2mev_max"]
            if (row["electron_800kev_max"] or 0) > (existing["electron_800kev_max"] or 0):
                merged[key]["electron_800kev_max"] = row["electron_800kev_max"]

    return sorted(merged.values(), key=lambda r: r["observed_at"])


async def fetch_seiss_yearly(
    session: aiohttp.ClientSession, year: int
) -> list[dict]:
    """Fetch an entire year of SEISS L2 data, one directory listing per month.

    Tries GOES-16 science first, then GOES-18 science, then GOES-18
    operational as fallback (covers processing lag in GOES-16).
    """
    now = datetime.now(timezone.utc)
    all_rows = []
    seen_dates: set[str] = set()
    sem = asyncio.Semaphore(5)  # Max 5 concurrent downloads per month

    for month in range(1, 13):
        if year == now.year and month > now.month:
            break

        # Try each SEISS source until we find files for this month
        dir_url = None
        files = []
        for _label, base_url in SEISS_L2_SOURCES:
            candidate_url = f"{base_url}{year}/{month:02d}/"
            try:
                async with session.get(
                    candidate_url, timeout=TIMEOUT_SHORT
                ) as resp:
                    if resp.status != 200:
                        continue
                    text = await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                continue

            # Match any GOES satellite (g16, g18, etc.)
            pattern = re.compile(
                r'href="(sci_mpsh-l2-avg5m_g\d+_d(\d{8})_v[\d\-]+\.nc)"'
            )
            found = [
                (fname, ds) for fname, ds in pattern.findall(text)
                if ds not in seen_dates
            ]
            if found:
                dir_url = candidate_url
                files = found
                break  # Use first source that has data

        if not dir_url or not files:
            continue

        async def _download_day(
            dl_url: str, fname: str, date_str: str
        ) -> dict | None:
            async with sem:
                nc_url = dl_url + fname
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        async with session.get(
                            nc_url, timeout=TIMEOUT
                        ) as resp:
                            if resp.status != 200:
                                if attempt == MAX_RETRIES:
                                    return None
                                await asyncio.sleep(2 ** attempt)
                                continue
                            nc_bytes = await resp.read()

                        try:
                            import netCDF4
                            import numpy as np
                        except ImportError:
                            return None

                        tmp_path = os.path.join(
                            tempfile.gettempdir(), f"seiss_{date_str}.nc"
                        )
                        with open(tmp_path, "wb") as f:
                            f.write(nc_bytes)
                        try:
                            ds = netCDF4.Dataset(tmp_path)
                            flux = ds.variables["AvgIntElectronFlux"][:]
                            if hasattr(flux, "mask"):
                                flux = np.where(flux.mask, np.nan, flux.data)
                            daily_max = float(np.nanmax(flux))
                            ds.close()
                        finally:
                            os.unlink(tmp_path)

                        if np.isnan(daily_max) or daily_max < 0:
                            return None

                        d = date_str
                        return {
                            "observed_at": f"{d[:4]}-{d[4:6]}-{d[6:8]}T00:00:00",
                            "electron_2mev_max": daily_max,
                            "electron_800kev_max": None,
                        }
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        if attempt == MAX_RETRIES:
                            return None
                        await asyncio.sleep(2 ** attempt)
                return None

        if files:
            results = await asyncio.gather(
                *[_download_day(dir_url, fname, ds) for fname, ds in files],
                return_exceptions=True,
            )
            for res in results:
                if isinstance(res, dict):
                    all_rows.append(res)
                    # Track dates to avoid duplicates from fallback sources
                    obs = res["observed_at"]  # "YYYY-MM-DDT00:00:00"
                    seen_dates.add(obs[0:4] + obs[5:7] + obs[8:10])

        await asyncio.sleep(0.5)  # Polite delay between months

    return sorted(all_rows, key=lambda r: r["observed_at"])


async def fetch_ncei_yearly(
    session: aiohttp.ClientSession, year: int
) -> list[dict]:
    """Attempt to fetch historical electron flux from NCEI for a given year.

    NCEI provides GOES-16 SEISS data (2017+) as CSV files per month.
    For 2011-2016, we use GOES-13/15 data from a different path.

    Returns daily-max aggregated rows.
    """
    # NCEI avg daily electron flux (GOES-16, 2017+)
    # Format: https://www.ncei.noaa.gov/data/goes-space-environment-monitor/
    #         access/avg/{year}/{month:02d}/goes16/csv/
    # This is best-effort: if data isn't available, return empty.

    all_records = []

    for month in range(1, 13):
        # Skip future months
        now = datetime.now(timezone.utc)
        if year == now.year and month > now.month:
            break

        # Try GOES-16 (2017+) and GOES-13/15 (2011-2016)
        if year >= 2017:
            satellites = ["goes16"]
        else:
            satellites = ["goes15", "goes13"]

        for sat in satellites:
            url = (
                f"https://www.ncei.noaa.gov/data/goes-space-environment-monitor/"
                f"access/avg/{year}/{month:02d}/{sat}/csv/"
            )
            # Fetch the directory listing to find electron data files
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    async with session.get(url, timeout=TIMEOUT) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            # Parse directory listing for electron CSV files
                            electron_files = []
                            for line in text.split('"'):
                                if "e2ew" in line.lower() or "electron" in line.lower():
                                    if line.endswith(".csv"):
                                        electron_files.append(line)

                            for fname in electron_files[:1]:
                                csv_url = url + fname
                                records = await _fetch_electron_csv(
                                    session, csv_url, year, month
                                )
                                all_records.extend(records)
                            break
                        elif resp.status == 404:
                            break
                        else:
                            logger.debug(
                                "NCEI %s %d/%02d: HTTP %d (attempt %d)",
                                sat, year, month, resp.status, attempt,
                            )
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt == MAX_RETRIES:
                        break
                    await asyncio.sleep(2 ** attempt)

            if all_records:
                break  # Got data from one satellite, skip alternatives
        await asyncio.sleep(0.5)  # Be polite to NCEI

    # Aggregate to daily max
    daily = defaultdict(lambda: {"electron_2mev_max": None})
    for rec in all_records:
        date_str = rec["date"]
        flux = rec.get("flux_2mev")
        if flux is not None and flux >= 0:
            current = daily[date_str]["electron_2mev_max"]
            if current is None or flux > current:
                daily[date_str]["electron_2mev_max"] = flux

    rows = []
    for date_str in sorted(daily):
        vals = daily[date_str]
        if vals["electron_2mev_max"] is not None:
            rows.append({
                "observed_at": f"{date_str}T00:00:00",
                "electron_2mev_max": vals["electron_2mev_max"],
                "electron_800kev_max": None,
            })

    return rows


async def _fetch_electron_csv(
    session: aiohttp.ClientSession, url: str, year: int, month: int
) -> list[dict]:
    """Fetch and parse a single NCEI electron CSV file."""
    records = []
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    lines = text.strip().split("\n")
                    if len(lines) < 2:
                        return []

                    header = lines[0].lower()
                    cols = [c.strip() for c in header.split(",")]

                    # Find relevant columns
                    time_col = None
                    flux_col = None
                    for i, col in enumerate(cols):
                        if "time" in col or "date" in col:
                            time_col = i
                        if "e>2" in col or "e_2" in col or "2mev" in col.replace(" ", ""):
                            flux_col = i

                    if time_col is None or flux_col is None:
                        return []

                    for line in lines[1:]:
                        parts = [p.strip() for p in line.split(",")]
                        if len(parts) <= max(time_col, flux_col):
                            continue
                        try:
                            date_str = parts[time_col][:10]
                            flux = float(parts[flux_col])
                            records.append({"date": date_str, "flux_2mev": flux})
                        except (ValueError, IndexError):
                            continue
                    return records
                elif resp.status == 404:
                    return []
                else:
                    logger.debug("NCEI CSV: HTTP %d for %s", resp.status, url)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == MAX_RETRIES:
                return []
            await asyncio.sleep(2 ** attempt)

    return []


async def main():
    await init_db()
    await init_particle_flux_table()

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    current_year = now.year

    # Check existing data
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), COUNT(*) FROM particle_flux"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    n_existing = existing[0][1] if existing else 0

    if last_date:
        # Always start from START_YEAR to ensure all historical years are checked.
        # The per-year skip logic (>300 days) handles deduplication.
        start_year = START_YEAR
        logger.info(
            "Particle flux existing: %d records (latest: %s), checking from %d",
            n_existing, last_date, start_year,
        )
    else:
        start_year = START_YEAR
        logger.info(
            "Particle flux: no existing data, starting from %d", start_year
        )

    total_records = 0

    async with aiohttp.ClientSession() as session:
        # --- Phase 1: Fetch recent data from SWPC JSON API ---
        logger.info("Fetching recent electron flux from SWPC...")
        recent_rows = await fetch_swpc_recent(session)

        if recent_rows:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR REPLACE INTO particle_flux
                       (observed_at, electron_2mev_max, electron_800kev_max,
                        received_at)
                       VALUES (?, ?, ?, ?)""",
                    [
                        (
                            r["observed_at"],
                            r["electron_2mev_max"],
                            r["electron_800kev_max"],
                            now_iso,
                        )
                        for r in recent_rows
                    ],
                )
                await db.commit()
            total_records += len(recent_rows)
            logger.info("SWPC recent: stored %d daily records", len(recent_rows))

        # --- Phase 2: Fetch historical data ---
        # Refresh coverage after SWPC insert
        async with aiosqlite.connect(DB_PATH) as db:
            yearly_counts = await db.execute_fetchall(
                """SELECT substr(observed_at, 1, 4) as year, COUNT(*)
                   FROM particle_flux
                   GROUP BY year"""
            )
        year_coverage = {row[0]: row[1] for row in yearly_counts}

        async def _store_rows(rows: list[dict]) -> int:
            if not rows:
                return 0
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR IGNORE INTO particle_flux
                       (observed_at, electron_2mev_max, electron_800kev_max,
                        received_at)
                       VALUES (?, ?, ?, ?)""",
                    [
                        (
                            r["observed_at"],
                            r["electron_2mev_max"],
                            r["electron_800kev_max"],
                            now_iso,
                        )
                        for r in rows
                    ],
                )
                await db.commit()
            return len(rows)

        # --- Phase 2a: NCEI CSV for legacy years (2011-2016) ---
        ncei_csv_years = []
        for year in range(start_year, min(NCEI_MAX_YEAR, current_year) + 1):
            year_str = str(year)
            existing_days = year_coverage.get(year_str, 0)
            if year < current_year and existing_days > 300:
                logger.info(
                    "NCEI CSV %d: already have %d daily records, skipping",
                    year, existing_days,
                )
                continue
            ncei_csv_years.append(year)

        ncei_semaphore = asyncio.Semaphore(3)

        async def _fetch_ncei_csv_year(yr: int) -> int:
            async with ncei_semaphore:
                logger.info("Fetching NCEI CSV for %d...", yr)
                hist_rows = await fetch_ncei_yearly(session, yr)
            n = await _store_rows(hist_rows)
            if n:
                logger.info("NCEI CSV %d: stored %d daily records", yr, n)
            else:
                logger.info("NCEI CSV %d: no data available", yr)
            return n

        if ncei_csv_years:
            logger.info(
                "Fetching %d years from NCEI CSV (2011-2016): %s",
                len(ncei_csv_years), ncei_csv_years,
            )
            results = await asyncio.gather(
                *[_fetch_ncei_csv_year(yr) for yr in ncei_csv_years],
                return_exceptions=True,
            )
            for yr, res in zip(ncei_csv_years, results):
                if isinstance(res, Exception):
                    logger.warning("NCEI CSV %d: failed with %s", yr, res)
                elif isinstance(res, int):
                    total_records += res

        # --- Phase 2b: SEISS L2 netCDF for 2017-present ---
        seiss_years = []
        for year in range(max(start_year, SEISS_START_YEAR), current_year + 1):
            year_str = str(year)
            existing_days = year_coverage.get(year_str, 0)
            if year < current_year and existing_days > 300:
                logger.info(
                    "SEISS %d: already have %d daily records, skipping",
                    year, existing_days,
                )
                continue
            seiss_years.append(year)

        seiss_semaphore = asyncio.Semaphore(2)  # netCDF files are larger

        async def _fetch_seiss_year(yr: int) -> int:
            async with seiss_semaphore:
                logger.info("Fetching SEISS L2 for %d...", yr)
                rows = await fetch_seiss_yearly(session, yr)
            n = await _store_rows(rows)
            if n:
                logger.info("SEISS %d: stored %d daily records", yr, n)
            else:
                logger.info("SEISS %d: no data available", yr)
            return n

        if seiss_years:
            logger.info(
                "Fetching %d years from SEISS L2 (2017-present): %s",
                len(seiss_years), seiss_years,
            )
            results = await asyncio.gather(
                *[_fetch_seiss_year(yr) for yr in seiss_years],
                return_exceptions=True,
            )
            for yr, res in zip(seiss_years, results):
                if isinstance(res, Exception):
                    logger.warning("SEISS %d: failed with %s", yr, res)
                elif isinstance(res, int):
                    total_records += res

    # Final summary
    async with aiosqlite.connect(DB_PATH) as db:
        final = await db.execute_fetchall(
            "SELECT MIN(observed_at), MAX(observed_at), COUNT(*) FROM particle_flux"
        )
    if final and final[0][2]:
        logger.info(
            "Particle flux fetch complete: %d new records, "
            "DB total: %d records (%s to %s)",
            total_records,
            final[0][2],
            final[0][0],
            final[0][1],
        )
    else:
        logger.info("Particle flux fetch complete: %d records processed", total_records)


if __name__ == "__main__":
    asyncio.run(main())
