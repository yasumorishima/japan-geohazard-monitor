"""Ionosphere TEC collector — CODE predicted Global Ionosphere Maps.

Downloads IONEX format files from CODE (Bern) FTP server containing
global Total Electron Content (TEC) maps. Extracts Japan region
(lat 25-45°N, lon 125-150°E) from the 2.5° × 5° grid.

TEC is measured in TECU (1 TECU = 10^16 electrons/m²). Anomalous
TEC drops have been observed before major earthquakes in research
literature.
"""

import gzip
import io
import logging
from datetime import date, datetime, timezone

import aiosqlite

from collectors.base import BaseCollector
from config import DB_PATH, TEC_FTP_BASE, TEC_INTERVAL

logger = logging.getLogger(__name__)

# Japan region filter
_LAT_MIN, _LAT_MAX = 25.0, 45.0
_LON_MIN, _LON_MAX = 125.0, 150.0


def _parse_ionex(content: str) -> list[dict]:
    """Parse IONEX format and extract TEC grid values for Japan region.

    IONEX grid format:
    - Each map starts with "START OF TEC MAP"
    - Epoch line follows
    - Then latitude bands, each with header "LAT/LON1/LON2/DLON/H"
      followed by TEC values (integers in 0.1 TECU)
    - Values: 73 values per lat band (lon -180 to 180, step 5°)
    """
    records = []
    lines = content.split("\n")

    # Parse grid parameters from header
    exponent = -1  # default: values in 0.1 TECU
    for line in lines:
        if "EXPONENT" in line:
            try:
                exponent = int(line[:6].strip())
            except ValueError:
                pass
        if "END OF HEADER" in line:
            break

    scale = 10 ** exponent  # Convert to TECU

    i = 0
    while i < len(lines):
        line = lines[i]

        # Find start of TEC map
        if "START OF TEC MAP" not in line:
            i += 1
            continue

        # Parse epoch
        i += 1
        epoch_line = lines[i]
        try:
            parts = epoch_line.split()
            epoch = datetime(
                int(parts[0]), int(parts[1]), int(parts[2]),
                int(parts[3]), int(parts[4]), int(parts[5]),
                tzinfo=timezone.utc,
            ).isoformat()
        except (ValueError, IndexError):
            i += 1
            continue

        i += 1

        # Parse latitude bands until END OF TEC MAP
        while i < len(lines) and "END OF TEC MAP" not in lines[i]:
            if "LAT/LON1/LON2/DLON/H" in lines[i]:
                # Parse lat band header: "   87.5-180.0 180.0   5.0 450.0"
                header = lines[i]
                try:
                    lat = float(header[:8].strip())
                    lon1 = float(header[8:14].strip())
                    lon2 = float(header[14:20].strip())
                    dlon = float(header[20:26].strip())
                except ValueError:
                    i += 1
                    continue

                # Skip lat bands outside Japan
                if lat < _LAT_MIN or lat > _LAT_MAX:
                    i += 1
                    continue

                # Read TEC values (spread across multiple lines, 16 values per line)
                n_lons = int((lon2 - lon1) / dlon) + 1
                values = []
                i += 1
                while len(values) < n_lons and i < len(lines):
                    vals = lines[i].split()
                    values.extend(int(v) for v in vals)
                    i += 1

                # Extract Japan longitude range
                for j, val in enumerate(values):
                    lon = lon1 + j * dlon
                    if _LON_MIN <= lon <= _LON_MAX and val != 9999:
                        records.append({
                            "lat": lat,
                            "lon": lon,
                            "tec": val * scale,
                            "epoch": epoch,
                        })
            else:
                i += 1

        i += 1

    return records


class TECCollector(BaseCollector):
    source_name = "tec"
    interval_sec = TEC_INTERVAL

    def __init__(self):
        self._last_doy: int | None = None

    def _build_url(self) -> str:
        """Build FTP URL for today's predicted IONEX file."""
        today = date.today()
        doy = today.timetuple().tm_yday
        year = today.year
        return (
            f"{TEC_FTP_BASE}/COD0OPSPRD_{year}{doy:03d}0000"
            f"_01D_01H_GIM.INX.gz"
        )

    async def fetch(self, session) -> list[dict]:
        """Download and parse IONEX file, extract Japan TEC grid."""
        today_doy = date.today().timetuple().tm_yday

        # Only re-download when the day changes (file updates daily)
        if self._last_doy == today_doy:
            return []

        url = self._build_url()
        logger.info("[tec] Fetching %s", url)

        async with session.get(url) as resp:
            if resp.status != 200:
                # Try final product if predicted not available
                today = date.today()
                final_url = (
                    f"{TEC_FTP_BASE}/{today.year}/COD0OPSFIN_{today.year}"
                    f"{today_doy:03d}0000_01D_01H_GIM.INX.gz"
                )
                async with session.get(final_url) as resp2:
                    resp2.raise_for_status()
                    compressed = await resp2.read()
                    product_type = "final"
            else:
                compressed = await resp.read()
                product_type = "predicted"

        # Decompress gzip
        content = gzip.decompress(compressed).decode("ascii", errors="ignore")

        records = _parse_ionex(content)
        for r in records:
            r["product_type"] = product_type

        self._last_doy = today_doy
        logger.info("[tec] Parsed %d Japan TEC grid points", len(records))
        return records

    def to_rows(self, records: list[dict]) -> list[tuple]:
        now = datetime.now(timezone.utc).isoformat()
        return [
            (r["lat"], r["lon"], r["tec"], r["epoch"], r["product_type"], now)
            for r in records
        ]

    async def insert_rows(self, db: aiosqlite.Connection, rows: list[tuple]) -> int:
        await db.executemany(
            """INSERT OR IGNORE INTO tec
               (latitude, longitude, tec_tecu, epoch, product_type, received_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows,
        )
        return db.total_changes
