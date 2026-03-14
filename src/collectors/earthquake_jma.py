"""JMA Bosai earthquake list collector."""

import re

import aiosqlite

from collectors.base import BaseCollector
from config import JMA_INTERVAL

ENDPOINT = "https://www.jma.go.jp/bosai/quake/data/list.json"


def parse_cod(cod: str) -> tuple[float | None, float | None, float | None]:
    """Parse JMA coordinate string like '+35.9+137.6-10000/'.

    Returns (latitude, longitude, depth_km).
    """
    if not cod:
        return None, None, None

    # Pattern: +lat+lon+depth/ or +lat+lon-depth/
    match = re.match(r'([+-][\d.]+)([+-][\d.]+)([+-][\d.]+)/', cod)
    if not match:
        return None, None, None

    lat = float(match.group(1))
    lon = float(match.group(2))
    depth_raw = float(match.group(3))
    # depth is in meters (negative = below sea level), convert to km
    depth_km = abs(depth_raw) / 1000.0

    return lat, lon, depth_km


class JMACollector(BaseCollector):
    source_name = "jma"
    interval_sec = JMA_INTERVAL

    async def fetch(self, session) -> list[dict]:
        async with session.get(ENDPOINT) as resp:
            return await resp.json()

    def to_rows(self, records: list[dict]) -> list[tuple]:
        from datetime import datetime, timezone

        rows = []
        now = datetime.now(timezone.utc).isoformat()

        for r in records:
            event_id = r.get("eid", "")
            if not event_id:
                continue

            lat, lon, depth_km = parse_cod(r.get("cod", ""))
            if lat is None or lon is None:
                continue

            # at field is ISO 8601 with timezone
            occurred = r.get("at", now)

            # maxi is a string like "3" or "5-"
            maxi_str = r.get("maxi", "")
            max_intensity = None
            if maxi_str:
                # Convert JMA intensity string to P2P-compatible scale
                intensity_map = {
                    "1": 10, "2": 20, "3": 30, "4": 40,
                    "5-": 45, "5+": 50, "6-": 55, "6+": 60, "7": 70,
                }
                max_intensity = intensity_map.get(maxi_str)

            mag_str = r.get("mag", "")
            magnitude = None
            if mag_str:
                try:
                    magnitude = float(mag_str)
                except (ValueError, TypeError):
                    pass

            rows.append((
                self.source_name,
                event_id,
                occurred,
                lat,
                lon,
                depth_km,
                magnitude,
                "Mj",
                max_intensity,
                r.get("anm", ""),
                r.get("en_anm", ""),
                now,
            ))
        return rows

    async def insert_rows(self, db: aiosqlite.Connection, rows: list[tuple]) -> int:
        inserted = 0
        for row in rows:
            try:
                await db.execute(
                    """INSERT OR IGNORE INTO earthquakes
                    (source, event_id, occurred_at, latitude, longitude, depth_km,
                     magnitude, magnitude_type, max_intensity, location_ja, location_en, received_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    row,
                )
                if db.total_changes:
                    inserted += 1
            except Exception:
                pass
        return inserted
