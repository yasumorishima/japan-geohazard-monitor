"""Geomagnetic data collector — NOAA SWPC GOES magnetometer + Kp index.

GOES satellite magnetometer provides minute-resolution magnetic field data.
Kp index is the standard 3-hourly planetary geomagnetic activity measure.
Both are key for earthquake-geomagnetic correlation analysis.
"""

import logging
from datetime import datetime, timezone

import aiosqlite

from collectors.base import BaseCollector
from config import DB_PATH, GEOMAG_INTERVAL, GOES_MAG_URL, KP_INDEX_URL

logger = logging.getLogger(__name__)


class GeomagCollector(BaseCollector):
    source_name = "geomag"
    interval_sec = GEOMAG_INTERVAL

    async def fetch(self, session) -> list[dict]:
        """Fetch GOES magnetometer (1-day) and Kp index data."""
        records = []

        # 1. GOES magnetometer — 1-minute resolution, 1-day window
        async with session.get(GOES_MAG_URL) as resp:
            resp.raise_for_status()
            goes_data = await resp.json()

        for entry in goes_data:
            records.append({
                "type": "goes",
                "time_tag": entry["time_tag"],
                "satellite": entry.get("satellite"),
                "he": entry.get("He"),
                "hp": entry.get("Hp"),
                "hn": entry.get("Hn"),
                "total": entry.get("total"),
            })

        # 2. Kp index — 3-hourly
        async with session.get(KP_INDEX_URL) as resp:
            resp.raise_for_status()
            kp_data = await resp.json()

        # First row is header: ["time_tag", "Kp", "a_running", "station_count"]
        for row in kp_data[1:]:
            try:
                records.append({
                    "type": "kp",
                    "time_tag": row[0],
                    "kp": float(row[1]) if row[1] else None,
                    "a_running": float(row[2]) if row[2] else None,
                    "station_count": int(row[3]) if row[3] else None,
                })
            except (ValueError, IndexError):
                continue

        return records

    def to_rows(self, records: list[dict]) -> list[tuple]:
        """Split into GOES rows and Kp rows."""
        # Return as-is; insert_rows handles the split
        return records

    async def insert_rows(self, db: aiosqlite.Connection, rows: list) -> int:
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0

        goes_rows = [
            (r["time_tag"], r["satellite"], r["he"], r["hp"], r["hn"], r["total"], now)
            for r in rows if r["type"] == "goes"
        ]
        if goes_rows:
            await db.executemany(
                """INSERT OR IGNORE INTO geomag_goes
                   (time_tag, satellite, he, hp, hn, total, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                goes_rows,
            )
            inserted += db.total_changes

        kp_rows = [
            (r["time_tag"], r["kp"], r["a_running"], r["station_count"], now)
            for r in rows if r["type"] == "kp"
        ]
        if kp_rows:
            await db.executemany(
                """INSERT OR IGNORE INTO geomag_kp
                   (time_tag, kp, a_running, station_count, received_at)
                   VALUES (?, ?, ?, ?, ?)""",
                kp_rows,
            )
            inserted += db.total_changes

        return inserted
