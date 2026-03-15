"""Fetch TEC (IONEX) data around M6.5+ earthquakes from CODE (Bern)."""

import asyncio
import gzip
import logging
import subprocess
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

JAPAN_BBOX = "minlatitude=20&maxlatitude=50&minlongitude=120&maxlongitude=155"
TEC_LAT_MIN, TEC_LAT_MAX = 25.0, 45.0
TEC_LON_MIN, TEC_LON_MAX = 125.0, 150.0
WINDOW_DAYS = 7


async def get_major_earthquakes(session: aiohttp.ClientSession) -> list[str]:
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


async def main():
    await init_db()

    async with aiohttp.ClientSession() as session:
        dates = await get_major_earthquakes(session)
        logger.info("TEC dates to fetch: %d (around %d+ events)", len(dates), len(dates) // 15)

        total = 0
        async with aiosqlite.connect(DB_PATH) as db:
            for d in dates:
                doy = d.timetuple().tm_yday
                yr = d.year
                yy = yr % 100

                # Try new naming, then old
                urls = [
                    f"http://ftp.aiub.unibe.ch/CODE/{yr}/COD0OPSFIN_{yr}{doy:03d}0000_01D_01H_GIM.INX.gz",
                    f"http://ftp.aiub.unibe.ch/CODE/{yr}/CODG{doy:03d}0.{yy:02d}I.Z",
                ]

                content = None
                for url in urls:
                    try:
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                continue
                            compressed = await resp.read()
                        if url.endswith(".gz"):
                            content = gzip.decompress(compressed).decode("ascii", errors="ignore")
                        else:
                            proc = subprocess.run(
                                ["uncompress", "-c"], input=compressed, capture_output=True
                            )
                            if proc.returncode == 0:
                                content = proc.stdout.decode("ascii", errors="ignore")
                        if content:
                            break
                    except Exception:
                        continue

                if not content:
                    continue

                rows = parse_ionex_japan(content)
                if rows:
                    await db.executemany(
                        """INSERT OR IGNORE INTO tec
                           (latitude, longitude, tec_tecu, epoch, product_type, received_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        rows,
                    )
                    await db.commit()
                    total += len(rows)
                    logger.info("TEC %s: %d points", d.isoformat(), len(rows))

        logger.info("Total TEC records: %d", total)


if __name__ == "__main__":
    asyncio.run(main())
