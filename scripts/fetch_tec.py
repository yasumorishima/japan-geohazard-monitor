"""Fetch TEC (IONEX) data from CODE (Bern).

Modes:
  event  — Fetch around M6.5+ earthquakes ±7 days (existing)
  random — Fetch random dates ±7 days for control baseline
"""

import argparse
import asyncio
import gzip
import logging
import random as random_mod
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import aiosqlite
from db_connect import safe_connect

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from db import init_db
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JAPAN_BBOX = "minlatitude=20&maxlatitude=50&minlongitude=120&maxlongitude=155"
TEC_LAT_MIN, TEC_LAT_MAX = 25.0, 45.0
TEC_LON_MIN, TEC_LON_MAX = 125.0, 150.0
WINDOW_DAYS = 7
MAX_CONCURRENT = 3


async def get_major_earthquakes(session: aiohttp.ClientSession) -> list[date]:
    """Get dates of M6.5+ earthquakes in Japan."""
    url = (
        "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
        f"&starttime=2011-01-01&endtime={datetime.now().strftime('%Y-%m-%d')}"
        f"&minmagnitude=6.5&{JAPAN_BBOX}&orderby=time"
    )
    async with session.get(url) as resp:
        data = await resp.json()

    dates = set()
    for f in data["features"]:
        t = datetime.fromtimestamp(f["properties"]["time"] / 1000, tz=timezone.utc)
        for offset in range(-WINDOW_DAYS, WINDOW_DAYS + 1):
            dates.add(t.date() + timedelta(days=offset))

    return sorted(dates)


def parse_ionex_japan(content: str) -> list[tuple]:
    """Parse IONEX and extract Japan region TEC grid."""
    lines = content.split("\n")
    exponent = -1
    for line in lines:
        if "EXPONENT" in line:
            try:
                exponent = int(line[:6].strip())
            except ValueError:
                pass
        if "END OF HEADER" in line:
            break
    scale = 10 ** exponent

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    i = 0
    while i < len(lines):
        if "START OF TEC MAP" not in lines[i]:
            i += 1
            continue
        i += 1
        parts = lines[i].split()
        try:
            epoch = datetime(
                int(parts[0]), int(parts[1]), int(parts[2]),
                int(parts[3]), int(parts[4]), int(parts[5]),
                tzinfo=timezone.utc,
            ).isoformat()
        except (ValueError, IndexError):
            i += 1
            continue
        i += 1
        while i < len(lines) and "END OF TEC MAP" not in lines[i]:
            if "LAT/LON1/LON2/DLON/H" in lines[i]:
                h = lines[i]
                try:
                    lat = float(h[:8])
                    lon1 = float(h[8:14])
                    lon2 = float(h[14:20])
                    dlon = float(h[20:26])
                except ValueError:
                    i += 1
                    continue
                if lat < TEC_LAT_MIN or lat > TEC_LAT_MAX:
                    i += 1
                    continue
                n_lons = int((lon2 - lon1) / dlon) + 1
                vals = []
                i += 1
                while len(vals) < n_lons and i < len(lines):
                    vals.extend(int(v) for v in lines[i].split())
                    i += 1
                for j, val in enumerate(vals):
                    lon = lon1 + j * dlon
                    if TEC_LON_MIN <= lon <= TEC_LON_MAX and val != 9999:
                        rows.append((lat, lon, val * scale, epoch, "final", now))
            else:
                i += 1
        i += 1
    return rows


async def download_ionex(session: aiohttp.ClientSession, d: date) -> str | None:
    """Download and decompress IONEX file for a given date. Returns content or None."""
    doy = d.timetuple().tm_yday
    yr = d.year
    yy = yr % 100

    urls = [
        f"http://ftp.aiub.unibe.ch/CODE/{yr}/COD0OPSFIN_{yr}{doy:03d}0000_01D_01H_GIM.INX.gz",
        f"http://ftp.aiub.unibe.ch/CODE/{yr}/CODG{doy:03d}0.{yy:02d}I.Z",
    ]

    for url in urls:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    continue
                compressed = await resp.read()
            if url.endswith(".gz"):
                return gzip.decompress(compressed).decode("ascii", errors="ignore")
            else:
                proc = subprocess.run(
                    ["uncompress", "-c"], input=compressed, capture_output=True
                )
                if proc.returncode == 0:
                    return proc.stdout.decode("ascii", errors="ignore")
        except Exception:
            continue
    return None


async def download_and_parse(
    session: aiohttp.ClientSession, d: date, sem: asyncio.Semaphore
) -> tuple[date, list[tuple]]:
    """Download, parse one date. Rate-limited by semaphore."""
    async with sem:
        content = await download_ionex(session, d)
        await asyncio.sleep(0.3)  # Rate limiting
        if not content:
            return d, []
        return d, parse_ionex_japan(content)


async def get_existing_tec_dates(db: aiosqlite.Connection) -> set[str]:
    """Get set of dates (YYYY-MM-DD) that already have TEC data in DB."""
    rows = await db.execute_fetchall("SELECT DISTINCT DATE(epoch) FROM tec")
    return {r[0] for r in rows if r[0]}


def expand_with_window(anchor_dates: list[date], window: int = WINDOW_DAYS) -> list[date]:
    """Expand anchor dates by ±window days and deduplicate."""
    all_dates = set()
    for d in anchor_dates:
        for offset in range(-window, window + 1):
            all_dates.add(d + timedelta(days=offset))
    return sorted(all_dates)


async def main():
    parser = argparse.ArgumentParser(description="Fetch TEC IONEX data")
    parser.add_argument(
        "--mode", choices=["event", "random", "continuous"], default="continuous",
        help="continuous: all dates 2011-now | event: M6.5+ earthquakes ±7d | random: baseline",
    )
    parser.add_argument(
        "--n-dates", type=int, default=300,
        help="Number of random anchor dates (random mode only)",
    )
    args = parser.parse_args()

    await init_db()

    async with aiohttp.ClientSession() as session:
        # 1. Determine candidate dates
        if args.mode == "continuous":
            start = date(2011, 1, 1)
            end = date.today() - timedelta(days=1)
            total = (end - start).days + 1
            all_dates = [start + timedelta(days=i) for i in range(total)]
            logger.info("Continuous mode: %d candidate dates (2011-01-01..%s)", total, end)
        elif args.mode == "event":
            anchor_dates = await get_major_earthquakes(session)
            all_dates = expand_with_window(anchor_dates, WINDOW_DAYS)
            logger.info("Event mode: %d dates from M6.5+ earthquakes ±%dd", len(all_dates), WINDOW_DAYS)
        else:
            random_mod.seed(42)
            start = date(2011, 1, 1)
            end = date.today() - timedelta(days=8)
            total_days = (end - start).days
            anchors = set()
            attempts = 0
            while len(anchors) < args.n_dates and attempts < args.n_dates * 3:
                anchors.add(start + timedelta(days=random_mod.randint(0, total_days)))
                attempts += 1
            all_dates = expand_with_window(sorted(anchors), WINDOW_DAYS)
            logger.info("Random mode: %d dates", len(all_dates))

        # 2. Check which dates already exist in DB
        async with safe_connect() as db:
            existing = await get_existing_tec_dates(db)

        # Recent-first: sort descending so active-analysis window fills first
        missing = [d for d in all_dates if d.isoformat() not in existing]
        new_dates = sorted(missing, reverse=True)

        import os as _os
        max_dates = int(_os.environ.get("TEC_MAX_DATES", "100"))
        new_dates = new_dates[:max_dates]

        logger.info(
            "Total: %d | Existing: %d | Missing: %d | This run: %d",
            len(all_dates), len(existing), len(missing), len(new_dates),
        )

        if not new_dates:
            logger.info("No new dates to fetch. Done.")
            return

        # 4. Download concurrently (semaphore limits parallelism)
        sem = asyncio.Semaphore(MAX_CONCURRENT)
        tasks = [download_and_parse(session, d, sem) for d in new_dates]

        # Process in batches to show progress and commit periodically
        batch_size = 50
        total = 0
        async with safe_connect() as db:
            for batch_start in range(0, len(tasks), batch_size):
                batch = tasks[batch_start:batch_start + batch_size]
                results = await asyncio.gather(*batch, return_exceptions=True)

                batch_count = 0
                for r in results:
                    if isinstance(r, Exception):
                        logger.warning("Download error: %s", r)
                        continue
                    d, rows = r
                    if rows:
                        await db.executemany(
                            """INSERT OR IGNORE INTO tec
                               (latitude, longitude, tec_tecu, epoch, product_type, received_at)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            rows,
                        )
                        batch_count += len(rows)

                await db.commit()
                total += batch_count
                logger.info(
                    "Progress: %d/%d dates done, %d new TEC records this batch",
                    min(batch_start + batch_size, len(new_dates)),
                    len(new_dates),
                    batch_count,
                )

        logger.info("Total new TEC records inserted: %d", total)


if __name__ == "__main__":
    asyncio.run(main())
