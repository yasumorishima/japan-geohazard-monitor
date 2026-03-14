"""P2P Earthquake JSON API collector."""

import aiosqlite

from collectors.base import BaseCollector
from config import P2P_INTERVAL

ENDPOINT = "https://api.p2pquake.net/v2/history?codes=551&limit=50"


class P2PQuakeCollector(BaseCollector):
    source_name = "p2p"
    interval_sec = P2P_INTERVAL

    async def fetch(self, session) -> list[dict]:
        async with session.get(ENDPOINT) as resp:
            return await resp.json()

    def to_rows(self, records: list[dict]) -> list[tuple]:
        from datetime import datetime, timezone

        rows = []
        now = datetime.now(timezone.utc).isoformat()
        for r in records:
            eq = r.get("earthquake", {})
            hypo = eq.get("hypocenter", {})

            lat = hypo.get("latitude")
            lon = hypo.get("longitude")
            if not lat or not lon:
                continue

            # Event ID: use the timestamp as unique key
            event_id = r.get("id", r.get("time", ""))
            if not event_id:
                continue

            # Time format: "YYYY/MM/DD HH:MM:SS"
            time_str = r.get("time", "")
            if time_str:
                try:
                    dt = datetime.strptime(time_str, "%Y/%m/%d %H:%M:%S")
                    # P2P times are JST
                    occurred = dt.isoformat() + "+09:00"
                except ValueError:
                    occurred = now
            else:
                occurred = now

            rows.append((
                self.source_name,
                str(event_id),
                occurred,
                lat,
                lon,
                hypo.get("depth"),
                hypo.get("magnitude"),
                "Mj",  # JMA magnitude
                eq.get("maxScale"),
                hypo.get("name", ""),
                None,  # location_en
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
