"""USGS Earthquake GeoJSON feed collector."""

import aiosqlite

from collectors.base import BaseCollector
from config import JAPAN_BBOX, USGS_INTERVAL

ENDPOINT = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
    "?format=geojson"
    f"&minlatitude={JAPAN_BBOX['min_lat']}"
    f"&maxlatitude={JAPAN_BBOX['max_lat']}"
    f"&minlongitude={JAPAN_BBOX['min_lon']}"
    f"&maxlongitude={JAPAN_BBOX['max_lon']}"
    "&orderby=time&limit=50"
)


class USGSCollector(BaseCollector):
    source_name = "usgs"
    interval_sec = USGS_INTERVAL

    async def fetch(self, session) -> list[dict]:
        async with session.get(ENDPOINT) as resp:
            data = await resp.json()
        return data.get("features", [])

    def to_rows(self, records: list[dict]) -> list[tuple]:
        from datetime import datetime, timezone

        rows = []
        now = datetime.now(timezone.utc).isoformat()
        for f in records:
            props = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [])
            if len(coords) < 3:
                continue

            lon, lat, depth = coords[0], coords[1], coords[2]
            event_id = props.get("code", "")
            if not event_id:
                continue

            # time is milliseconds since epoch
            ts_ms = props.get("time")
            if ts_ms:
                occurred = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            else:
                occurred = now

            rows.append((
                self.source_name,
                event_id,
                occurred,
                lat,
                lon,
                depth,
                props.get("mag"),
                props.get("magType"),
                None,  # USGS doesn't provide JMA intensity
                None,  # location_ja
                props.get("place", ""),
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
