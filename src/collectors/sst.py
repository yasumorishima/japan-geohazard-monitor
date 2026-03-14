"""Sea surface temperature collector — NOAA ERDDAP MUR SST.

Fetches daily sea surface temperature on a 0.5° grid around Japan
(25-45°N, 125-150°E) from the JPL MUR SST v4.1 dataset.
~2,000 grid points per fetch, updated every 6 hours.
"""

import logging
from datetime import datetime, timezone

import aiosqlite

from collectors.base import BaseCollector
from config import DB_PATH, SST_INTERVAL, SST_URL

logger = logging.getLogger(__name__)


class SSTCollector(BaseCollector):
    source_name = "sst"
    interval_sec = SST_INTERVAL

    async def fetch(self, session) -> list[dict]:
        """Fetch SST grid from NOAA ERDDAP."""
        async with session.get(SST_URL) as resp:
            resp.raise_for_status()
            data = await resp.json()

        table = data.get("table", {})
        col_names = table.get("columnNames", [])
        rows = table.get("rows", [])

        # Columns: ["time", "latitude", "longitude", "analysed_sst"]
        time_idx = col_names.index("time") if "time" in col_names else 0
        lat_idx = col_names.index("latitude") if "latitude" in col_names else 1
        lon_idx = col_names.index("longitude") if "longitude" in col_names else 2
        sst_idx = col_names.index("analysed_sst") if "analysed_sst" in col_names else 3

        records = []
        for row in rows:
            sst_val = row[sst_idx]
            if sst_val is None:
                continue  # Skip land/missing data
            records.append({
                "observed_at": row[time_idx],
                "lat": row[lat_idx],
                "lon": row[lon_idx],
                "sst": sst_val,
            })

        return records

    def to_rows(self, records: list[dict]) -> list[tuple]:
        now = datetime.now(timezone.utc).isoformat()
        return [
            (r["lat"], r["lon"], r["sst"], r["observed_at"], now)
            for r in records
        ]

    async def insert_rows(self, db: aiosqlite.Connection, rows: list[tuple]) -> int:
        await db.executemany(
            """INSERT OR IGNORE INTO sst
               (latitude, longitude, temperature_c, observed_at, received_at)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
        return db.total_changes
