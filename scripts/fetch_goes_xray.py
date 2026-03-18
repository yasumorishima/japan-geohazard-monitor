"""Fetch GOES X-ray flux data (solar flare proxy) for earthquake precursor analysis.

Solar flares produce intense X-ray emission measured by GOES satellites in two
energy bands: 1-8 Angstrom (long) and 0.5-4 Angstrom (short). The long-wavelength
band is used for the standard A/B/C/M/X flare classification system.

Physical mechanism:
    Solar flares -> X-ray/EUV -> ionosphere heating -> conductivity change ->
    telluric current perturbation -> Coulomb stress at faults. Strong X-class
    flares documented to correlate with increased M5+ seismicity within 10 days
    (Sobolev 2020). The 1-8A band captures thermal emission from flare plasma.

Target features:
    - xray_long_wm2: peak 1-8 Angstrom X-ray flux per day (W/m2)
    - xray_short_wm2: peak 0.5-4 Angstrom X-ray flux per day (W/m2)
    - flare_class: derived classification (A/B/C/M/X + numeric)

Data sources (in priority order):
    1. NOAA SWPC GOES JSON API (recent 7-30 days, 1-min resolution)
       https://services.swpc.noaa.gov/json/goes/primary/xrays-7-day.json
    2. LASP LISIRD daily flare data (2011-present, daily resolution)
       https://lasp.colorado.edu/lisird/latis/dap/goes_xrs_flare_daily.json
    3. NOAA SWPC event reports (historical flare events)
       https://services.swpc.noaa.gov/json/goes/primary/xray-flares-latest.json

Strategy:
    - Fetch LISIRD daily data first for full 2011-present coverage
    - Then overlay SWPC JSON for the most recent 30 days at higher fidelity
    - Aggregate SWPC 1-min data to daily max flux
    - Deduplicate via UNIQUE(observed_at) constraint

References:
    - Sobolev & Zakrzhevskaya (2020) Pure Appl. Geophys. 177:629-640
    - Hathaway (2015) Living Rev. Sol. Phys. 12:4
"""

import asyncio
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NOAA SWPC GOES X-ray JSON (recent data, 1-min resolution)
SWPC_XRAY_URL = "https://services.swpc.noaa.gov/json/goes/primary/xrays-{n}-day.json"

# LASP LISIRD daily flare data (2011-present)
LISIRD_URL = (
    "https://lasp.colorado.edu/lisird/latis/dap/"
    "goes_xrs_flare_daily.json?time>={start}&time<{end}"
)

# NOAA SWPC flare event list (recent events with classification)
SWPC_FLARE_EVENTS_URL = (
    "https://services.swpc.noaa.gov/json/goes/primary/xray-flares-latest.json"
)

MAX_RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=300, connect=30)
START_YEAR = 2011


def classify_flare(flux_wm2: float) -> str:
    """Derive GOES flare classification from 1-8A flux in W/m2.

    Classification thresholds:
        A: < 1e-7
        B: 1e-7 to < 1e-6
        C: 1e-6 to < 1e-5
        M: 1e-5 to < 1e-4
        X: >= 1e-4

    Returns e.g. 'C3.2', 'M1.0', 'X5.4'
    """
    if flux_wm2 is None or flux_wm2 <= 0:
        return None

    classes = [
        (1e-4, "X"),
        (1e-5, "M"),
        (1e-6, "C"),
        (1e-7, "B"),
        (0.0, "A"),
    ]
    for threshold, letter in classes:
        if flux_wm2 >= threshold:
            if threshold > 0:
                magnitude = flux_wm2 / threshold
            else:
                magnitude = flux_wm2 / 1e-8
            return f"{letter}{magnitude:.1f}"
    return "A0.0"


async def init_goes_xray_table():
    """Create GOES X-ray flux table."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS goes_xray (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                xray_long_wm2 REAL,
                xray_short_wm2 REAL,
                flare_class TEXT,
                UNIQUE(observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_goes_xray_time
            ON goes_xray(observed_at)
        """)
        await db.commit()


async def fetch_swpc_json(session: aiohttp.ClientSession, n_days: int = 7) -> list[dict]:
    """Fetch recent X-ray data from SWPC JSON API (1-min resolution).

    Returns list of dicts with keys: observed_at, xray_long, xray_short.
    The SWPC JSON returns objects like:
        {
            "time_tag": "2024-01-15 12:00:00.000",
            "satellite": 16,
            "current_class": "B2.3",
            "current_ratio": 0.034,
            "energy": "0.1-0.8nm",   # = 1-8 Angstrom (long)
            "flux": 2.3e-07
        }
    Two entries per timestamp: one for 0.1-0.8nm (long) and one for 0.05-0.4nm (short).
    """
    url = SWPC_XRAY_URL.format(n=n_days)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    break
                elif resp.status == 404:
                    logger.info("SWPC xrays-%d-day: not available (404)", n_days)
                    return []
                else:
                    logger.warning("SWPC xrays-%d-day: HTTP %d (attempt %d)",
                                   n_days, resp.status, attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("SWPC xrays-%d-day: %s", n_days, type(e).__name__)
                return []
            await asyncio.sleep(2 ** attempt)
    else:
        return []

    # Group by timestamp, merge long/short bands
    time_map = {}  # time_tag -> {long: flux, short: flux}
    for entry in data:
        tag = entry.get("time_tag", "")
        energy = entry.get("energy", "")
        flux = entry.get("flux")

        if not tag or flux is None:
            continue

        if tag not in time_map:
            time_map[tag] = {"long": None, "short": None}

        # 0.1-0.8nm = 1-8 Angstrom = long wavelength band
        if "0.1-0.8" in energy:
            time_map[tag]["long"] = flux
        # 0.05-0.4nm = 0.5-4 Angstrom = short wavelength band
        elif "0.05-0.4" in energy:
            time_map[tag]["short"] = flux

    rows = []
    for tag, fluxes in time_map.items():
        # Parse time_tag: "2024-01-15 12:00:00.000"
        try:
            dt = datetime.strptime(tag[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        rows.append({
            "observed_at": dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "xray_long": fluxes["long"],
            "xray_short": fluxes["short"],
        })

    return rows


def aggregate_daily(rows: list[dict]) -> list[dict]:
    """Aggregate sub-daily X-ray records to daily max flux.

    For each day, keep the maximum flux value (peak flare intensity).
    """
    daily = {}  # date_str -> {long_max, short_max}

    for r in rows:
        date_str = r["observed_at"][:10]  # YYYY-MM-DD

        if date_str not in daily:
            daily[date_str] = {"long": None, "short": None}

        xlong = r.get("xray_long")
        xshort = r.get("xray_short")

        if xlong is not None:
            if daily[date_str]["long"] is None or xlong > daily[date_str]["long"]:
                daily[date_str]["long"] = xlong

        if xshort is not None:
            if daily[date_str]["short"] is None or xshort > daily[date_str]["short"]:
                daily[date_str]["short"] = xshort

    result = []
    for date_str in sorted(daily.keys()):
        d = daily[date_str]
        result.append({
            "observed_at": f"{date_str}T00:00:00",
            "xray_long": d["long"],
            "xray_short": d["short"],
            "flare_class": classify_flare(d["long"]),
        })

    return result


async def fetch_lisird(session: aiohttp.ClientSession, start_year: int) -> list[dict]:
    """Fetch historical daily X-ray flare data from LASP LISIRD.

    LISIRD provides daily-averaged GOES XRS data from ~1986-present.
    We fetch year by year to avoid timeouts on large requests.
    """
    current_year = datetime.now(timezone.utc).year
    all_rows = []

    for year in range(start_year, current_year + 1):
        start = f"{year}-01-01"
        end = f"{year + 1}-01-01"
        url = LISIRD_URL.format(start=start, end=end)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        try:
                            data = await resp.json(content_type=None) if False else None
                            # We already have text, parse it
                            import json
                            data = json.loads(text)
                        except Exception:
                            logger.warning("LISIRD %d: JSON parse error", year)
                            data = None
                            break
                        break
                    elif resp.status == 404:
                        logger.info("LISIRD %d: not available (404)", year)
                        data = None
                        break
                    else:
                        logger.warning("LISIRD %d: HTTP %d (attempt %d)",
                                       year, resp.status, attempt)
                        data = None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                data = None
                if attempt == MAX_RETRIES:
                    logger.warning("LISIRD %d: %s", year, type(e).__name__)
                await asyncio.sleep(2 ** attempt)
        else:
            continue

        if data is None:
            continue

        # LISIRD JSON format varies; try to extract time series
        # Expected structure: {"goes_xrs_flare_daily": {"samples": {"time": [...], "value": [...]}}}
        # or: {"parameters": [...], "data": [...]}
        rows = _parse_lisird_response(data, year)
        if rows:
            all_rows.extend(rows)
            logger.info("LISIRD %d: %d daily records", year, len(rows))
        else:
            logger.info("LISIRD %d: no parseable data", year)

        await asyncio.sleep(1.0)

    return all_rows


def _parse_lisird_response(data: dict, year: int) -> list[dict]:
    """Parse LISIRD JSON response into standardized rows.

    LISIRD LaTiS JSON format:
        {
            "goes_xrs_flare_daily": {
                "metadata": {...},
                "parameters": [
                    {"name": "time", ...},
                    {"name": "value", ...}
                ],
                "data": [[timestamp_ms, flux_value], ...]
            }
        }
    """
    rows = []

    # Try top-level dataset key
    dataset = None
    for key in data:
        if isinstance(data[key], dict) and "data" in data[key]:
            dataset = data[key]
            break

    if dataset is None:
        # Try flat structure
        if "data" in data:
            dataset = data
        else:
            return rows

    records = dataset.get("data", [])
    if not records:
        return rows

    for record in records:
        if not isinstance(record, (list, tuple)) or len(record) < 2:
            continue

        try:
            time_val = record[0]
            flux_val = record[1]

            # Time might be milliseconds since epoch or ISO string
            if isinstance(time_val, (int, float)):
                # Milliseconds since epoch
                dt = datetime.fromtimestamp(time_val / 1000.0, tz=timezone.utc)
            elif isinstance(time_val, str):
                dt = datetime.fromisoformat(time_val.replace("Z", "+00:00"))
            else:
                continue

            if dt.year != year:
                continue

            xray_long = float(flux_val) if flux_val is not None else None

            # Filter out fill values (negative or absurdly large)
            if xray_long is not None and (xray_long < 0 or xray_long > 1e-2):
                xray_long = None

            rows.append({
                "observed_at": dt.strftime("%Y-%m-%dT00:00:00"),
                "xray_long": xray_long,
                "xray_short": None,  # LISIRD daily data typically only has long band
                "flare_class": classify_flare(xray_long),
            })
        except (ValueError, TypeError, OverflowError):
            continue

    return rows


async def fetch_swpc_flare_events(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch recent flare event list from SWPC for classification cross-check.

    Returns list of flare events with peak times and classifications.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(SWPC_FLARE_EVENTS_URL, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    break
                elif resp.status == 404:
                    logger.info("SWPC flare events: not available (404)")
                    return []
                else:
                    logger.warning("SWPC flare events: HTTP %d (attempt %d)",
                                   resp.status, attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                logger.warning("SWPC flare events: %s", type(e).__name__)
                return []
            await asyncio.sleep(2 ** attempt)
    else:
        return []

    rows = []
    for event in data:
        try:
            peak_time = event.get("max_time", "")
            class_type = event.get("max_class", "")
            if not peak_time:
                continue
            dt = datetime.strptime(peak_time[:19], "%Y-%m-%d %H:%M:%S")
            rows.append({
                "observed_at": dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "class": class_type,
            })
        except (ValueError, KeyError):
            continue

    return rows


async def main():
    await init_db()
    await init_goes_xray_table()

    # Check existing data
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await db.execute_fetchall(
            "SELECT MAX(observed_at), MIN(observed_at), COUNT(*) FROM goes_xray"
        )
    last_date = existing[0][0] if existing and existing[0][0] else None
    first_date = existing[0][1] if existing and existing[0][1] else None
    n_existing = existing[0][2] if existing else 0

    if last_date:
        logger.info("GOES X-ray existing: %d records (%s to %s)",
                     n_existing, first_date, last_date)
    else:
        logger.info("GOES X-ray: no existing data, starting from %d", START_YEAR)

    total_inserted = 0

    async with aiohttp.ClientSession() as session:
        # --- Phase 1: LISIRD historical daily data (2011-present) ---
        lisird_start = START_YEAR
        if first_date:
            # Only fetch years we don't have yet, or re-fetch current year
            lisird_start = int(first_date[:4])

        logger.info("Phase 1: Fetching LISIRD daily data from %d...", lisird_start)
        lisird_rows = await fetch_lisird(session, lisird_start)

        if lisird_rows:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR REPLACE INTO goes_xray
                       (observed_at, xray_long_wm2, xray_short_wm2, flare_class)
                       VALUES (?, ?, ?, ?)""",
                    [(r["observed_at"], r["xray_long"], r["xray_short"],
                      r["flare_class"]) for r in lisird_rows],
                )
                await db.commit()
            total_inserted += len(lisird_rows)
            logger.info("LISIRD: stored %d daily records", len(lisird_rows))
        else:
            logger.info("LISIRD: no data retrieved (may be unavailable)")

        # --- Phase 2: SWPC JSON for recent 30 days (higher fidelity) ---
        logger.info("Phase 2: Fetching SWPC recent X-ray data (30 days)...")
        swpc_rows = await fetch_swpc_json(session, n_days=7)

        if not swpc_rows:
            logger.info("SWPC 7-day: no data, trying 3-day...")
            swpc_rows = await fetch_swpc_json(session, n_days=3)

        if swpc_rows:
            # Aggregate 1-min data to daily max
            daily_rows = aggregate_daily(swpc_rows)

            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    """INSERT OR REPLACE INTO goes_xray
                       (observed_at, xray_long_wm2, xray_short_wm2, flare_class)
                       VALUES (?, ?, ?, ?)""",
                    [(r["observed_at"], r["xray_long"], r["xray_short"],
                      r["flare_class"]) for r in daily_rows],
                )
                await db.commit()
            total_inserted += len(daily_rows)
            logger.info("SWPC: stored %d daily records (aggregated from %d 1-min records)",
                         len(daily_rows), len(swpc_rows))
        else:
            logger.info("SWPC: no recent data retrieved")

    # Summary
    async with aiosqlite.connect(DB_PATH) as db:
        final = await db.execute_fetchall(
            "SELECT MIN(observed_at), MAX(observed_at), COUNT(*) FROM goes_xray"
        )
    f_min, f_max, f_count = final[0]
    logger.info("GOES X-ray fetch complete: %d records total (%s to %s), "
                "%d new/updated this run", f_count, f_min, f_max, total_inserted)


if __name__ == "__main__":
    asyncio.run(main())
