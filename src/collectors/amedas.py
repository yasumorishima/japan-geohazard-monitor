"""AMeDAS collector — ~1,300 JMA weather stations across Japan.

Fetches temperature, pressure, humidity, wind, and precipitation
every 10 minutes from the JMA Bosai API.
"""

import logging
from datetime import datetime, timezone

import aiosqlite

from collectors.base import BaseCollector
from config import (
    AMEDAS_DATA_URL,
    AMEDAS_INTERVAL,
    AMEDAS_LATEST_TIME_URL,
    AMEDAS_TABLE_URL,
    DB_PATH,
)

logger = logging.getLogger(__name__)


class AMeDASCollector(BaseCollector):
    source_name = "amedas"
    interval_sec = AMEDAS_INTERVAL

    def __init__(self):
        self._station_table: dict | None = None

    async def _load_station_table(self, session) -> dict:
        """Fetch and cache the station metadata table."""
        if self._station_table is not None:
            return self._station_table
        async with session.get(AMEDAS_TABLE_URL) as resp:
            resp.raise_for_status()
            self._station_table = await resp.json()
        logger.info("[amedas] Loaded %d stations", len(self._station_table))
        return self._station_table

    @staticmethod
    def _parse_latlon(deg_min: list) -> float:
        """Convert [degrees, minutes] to decimal degrees."""
        return deg_min[0] + deg_min[1] / 60.0

    async def fetch(self, session) -> list[dict]:
        """Fetch latest AMeDAS snapshot for all stations."""
        stations = await self._load_station_table(session)

        # Get latest available timestamp
        async with session.get(AMEDAS_LATEST_TIME_URL) as resp:
            resp.raise_for_status()
            latest_time = (await resp.text()).strip()

        # Convert "2026-03-15T07:30:00+09:00" → "20260315073000"
        dt = datetime.fromisoformat(latest_time)
        ts_key = dt.strftime("%Y%m%d%H%M%S")

        url = AMEDAS_DATA_URL.format(timestamp=ts_key)
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        records = []
        for station_id, obs in data.items():
            meta = stations.get(station_id)
            if meta is None:
                continue

            lat = self._parse_latlon(meta["lat"])
            lon = self._parse_latlon(meta["lon"])

            def val(key):
                """Extract value from [value, quality_flag] pair."""
                v = obs.get(key)
                if v is None:
                    return None
                return v[0] if isinstance(v, list) else v

            records.append({
                "station_id": station_id,
                "station_name": meta.get("kjName", ""),
                "lat": lat,
                "lon": lon,
                "observed_at": dt.astimezone(timezone.utc).isoformat(),
                "pressure": val("pressure"),
                "temp": val("temp"),
                "humidity": val("humidity"),
                "wind_speed": val("wind"),
                "wind_dir": val("windDirection"),
                "precip_1h": val("precipitation1h"),
            })

        return records

    def to_rows(self, records: list[dict]) -> list[tuple]:
        now = datetime.now(timezone.utc).isoformat()
        return [
            (
                r["station_id"], r["station_name"], r["lat"], r["lon"],
                r["observed_at"], r["pressure"], r["temp"], r["humidity"],
                r["wind_speed"], r["wind_dir"], r["precip_1h"], now,
            )
            for r in records
        ]

    async def insert_rows(self, db: aiosqlite.Connection, rows: list[tuple]) -> int:
        await db.executemany(
            """INSERT OR IGNORE INTO amedas
               (station_id, station_name, latitude, longitude, observed_at,
                pressure_hpa, temperature_c, humidity_pct,
                wind_speed_ms, wind_direction, precipitation_1h, received_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        return db.total_changes
