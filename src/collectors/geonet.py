"""GEONET crustal deformation collector — GSI F5 daily GNSS solutions.

Downloads daily coordinate files from GSI's SFTP server (terras.gsi.go.jp)
for ~1,300 GNSS stations across Japan. Computes displacement (mm) relative
to the first observation in each file.

F5 solutions are final products with ~1-2 week delay.
"""

import gzip
import io
import logging
import os
from datetime import datetime, timezone

import asyncssh
import aiosqlite

from collectors.base import BaseCollector
from config import (
    DB_PATH,
    GEONET_INTERVAL,
    GSI_SFTP_HOST,
    GSI_SFTP_PATH,
    GSI_SFTP_USER,
)

logger = logging.getLogger(__name__)

# Sample ~200 stations spread across Japan for practical daily collection
# Full 1,304 stations would be too heavy for RPi5
_SAMPLE_STATIONS = None  # Will be populated on first run


def _parse_pos_file(content: str) -> list[dict]:
    """Parse a GEONET F5 .pos file.

    Returns list of {station_id, station_name, date, lat, lon, height, x, y, z}.
    """
    records = []
    station_id = ""
    station_name = ""
    in_data = False

    for line in content.split("\n"):
        line = line.rstrip()
        if line.startswith(" ID"):
            station_id = line.split()[-1].strip()
        elif line.startswith(" J_NAME"):
            station_name = line.split(None, 1)[-1].strip()
        elif line.startswith("+DATA"):
            in_data = True
            continue
        elif line.startswith("-DATA"):
            break
        elif in_data and line and not line.startswith("*"):
            parts = line.split()
            if len(parts) < 9:
                continue
            try:
                records.append({
                    "station_id": station_id,
                    "station_name": station_name,
                    "year": int(parts[0]),
                    "month": int(parts[1]),
                    "day": int(parts[2]),
                    "time": parts[3],
                    "x": float(parts[4]),
                    "y": float(parts[5]),
                    "z": float(parts[6]),
                    "lat": float(parts[7]),
                    "lon": float(parts[8]),
                    "height": float(parts[9]),
                })
            except (ValueError, IndexError):
                continue

    return records


class GEONETCollector(BaseCollector):
    source_name = "geonet"
    interval_sec = GEONET_INTERVAL

    def __init__(self):
        self._last_fetch_date: str | None = None

    async def _list_stations(self, sftp, year: int) -> list[str]:
        """List available station files for the given year."""
        path = f"{GSI_SFTP_PATH}/{year}"
        entries = await sftp.listdir(path)
        return [e for e in entries if e.endswith(".pos.gz")]

    async def fetch(self, session) -> list[dict]:
        """Download and parse GEONET F5 position files via SFTP."""
        password = os.environ.get("GSI_SFTP_PASSWORD")
        if not password:
            logger.warning("[geonet] GSI_SFTP_PASSWORD not set, skipping")
            return []

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._last_fetch_date == today:
            return []  # Already fetched today

        year = datetime.now(timezone.utc).year

        try:
            async with asyncssh.connect(
                GSI_SFTP_HOST,
                username=GSI_SFTP_USER,
                password=password,
                known_hosts=None,
            ) as conn:
                async with conn.start_sftp_client() as sftp:
                    station_files = await self._list_stations(sftp, year)
                    logger.info("[geonet] Found %d station files for %d",
                                len(station_files), year)

                    # Sample every 6th station (~200 out of 1,304)
                    sampled = sorted(station_files)[::6]
                    logger.info("[geonet] Downloading %d sampled stations", len(sampled))

                    all_records = []
                    for filename in sampled:
                        remote_path = f"{GSI_SFTP_PATH}/{year}/{filename}"
                        try:
                            data = await sftp.read(remote_path)
                            content = gzip.decompress(data).decode(
                                "utf-8", errors="ignore"
                            )
                            records = _parse_pos_file(content)
                            if records:
                                # Only keep the last 7 days of data
                                records = records[-7:]
                                # Compute displacement relative to first record
                                ref = records[0]
                                for r in records:
                                    r["dx_mm"] = (r["x"] - ref["x"]) * 1000
                                    r["dy_mm"] = (r["y"] - ref["y"]) * 1000
                                    r["dz_mm"] = (r["z"] - ref["z"]) * 1000
                                all_records.extend(records)
                        except Exception as e:
                            logger.debug("[geonet] Skip %s: %s", filename, e)

                    self._last_fetch_date = today
                    logger.info("[geonet] Parsed %d records from %d stations",
                                len(all_records), len(sampled))
                    return all_records

        except Exception as e:
            logger.warning("[geonet] SFTP error: %s", e)
            return []

    def to_rows(self, records: list[dict]) -> list[tuple]:
        now = datetime.now(timezone.utc).isoformat()
        return [
            (
                r["station_id"], r["station_name"],
                f"{r['year']:04d}-{r['month']:02d}-{r['day']:02d}T{r['time']}",
                r["lat"], r["lon"], r["height"],
                r.get("dx_mm"), r.get("dy_mm"), r.get("dz_mm"), now,
            )
            for r in records
        ]

    async def insert_rows(self, db: aiosqlite.Connection, rows: list[tuple]) -> int:
        await db.executemany(
            """INSERT OR IGNORE INTO geonet
               (station_id, station_name, observed_at,
                latitude, longitude, height_m,
                dx_mm, dy_mm, dz_mm, received_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        return db.total_changes
