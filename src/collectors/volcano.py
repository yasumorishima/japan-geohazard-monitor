"""Volcano collector — JMA Bosai volcano list + warning data.

Fetches the list of 120 active volcanoes in Japan and their current
warning/alert levels. Data is upserted (one row per volcano).

Alert level codes (JMA):
  11 = Level 1 (Active volcano, potential for increased activity)
  12 = Level 2 (Restriction on proximity to the crater)
  13 = Level 3 (Restriction on proximity to the volcano)
  14 = Level 4 (Evacuation of the elderly, etc.)
  15 = Level 5 (Evacuation)
  21 = Volcanic forecast (no level system)
  22 = Near-crater danger
  23 = Entry prohibited
"""

import logging
from datetime import datetime, timezone

import aiosqlite

from collectors.base import BaseCollector
from config import DB_PATH, VOLCANO_INTERVAL, VOLCANO_LIST_URL, VOLCANO_WARNING_URL

logger = logging.getLogger(__name__)

# Map JMA alert codes to numeric levels (1-5 for leveled, 2-3 for non-leveled)
_ALERT_LEVEL = {
    "11": 1, "12": 2, "13": 3, "14": 4, "15": 5,
    "21": 1, "22": 2, "23": 3,
}


class VolcanoCollector(BaseCollector):
    source_name = "volcano"
    interval_sec = VOLCANO_INTERVAL

    async def fetch(self, session) -> list[dict]:
        """Fetch volcano list and current warnings."""
        # 1. Volcano master list (120 volcanoes with coordinates)
        async with session.get(VOLCANO_LIST_URL) as resp:
            resp.raise_for_status()
            volcano_list = await resp.json()

        # Build lookup: code → volcano info
        volcanoes = {}
        for v in volcano_list:
            if "latlon" not in v:
                continue  # Skip meta entries (900, 901, 902)
            code = v["code"]
            volcanoes[code] = {
                "code": code,
                "name_ja": v.get("name_jp", ""),
                "name_en": v.get("name_en", ""),
                "lat": float(v["latlon"][0]),
                "lon": float(v["latlon"][1]),
                "alert_level": 1,
                "alert_code": "11",
                "alert_name_ja": "活火山であることに留意",
                "report_datetime": None,
            }

        # 2. Current warnings (only volcanoes with elevated alert)
        async with session.get(VOLCANO_WARNING_URL) as resp:
            resp.raise_for_status()
            warnings = await resp.json()

        for w in warnings:
            report_dt = w.get("reportDatetime")
            for vi in w.get("volcanoInfos", []):
                if vi.get("type") != "噴火警報・予報（対象火山）":
                    continue
                for item in vi.get("items", []):
                    alert_code = item.get("code", "11")
                    for area in item.get("areas", []):
                        vcode = area.get("code")
                        if vcode and vcode in volcanoes:
                            volcanoes[vcode]["alert_level"] = _ALERT_LEVEL.get(alert_code, 1)
                            volcanoes[vcode]["alert_code"] = alert_code
                            volcanoes[vcode]["alert_name_ja"] = item.get("name", "")
                            volcanoes[vcode]["report_datetime"] = report_dt

        return list(volcanoes.values())

    def to_rows(self, records: list[dict]) -> list[tuple]:
        now = datetime.now(timezone.utc).isoformat()
        return [
            (
                r["code"], r["name_ja"], r["name_en"], r["lat"], r["lon"],
                r["alert_level"], r["alert_code"], r["alert_name_ja"],
                r["report_datetime"], now,
            )
            for r in records
        ]

    async def insert_rows(self, db: aiosqlite.Connection, rows: list[tuple]) -> int:
        # UPSERT: update alert level if volcano already exists
        await db.executemany(
            """INSERT INTO volcanoes
               (volcano_code, volcano_name_ja, volcano_name_en,
                latitude, longitude, alert_level, alert_code,
                alert_name_ja, report_datetime, received_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(volcano_code) DO UPDATE SET
                   alert_level = excluded.alert_level,
                   alert_code = excluded.alert_code,
                   alert_name_ja = excluded.alert_name_ja,
                   report_datetime = excluded.report_datetime,
                   received_at = excluded.received_at""",
            rows,
        )
        return len(rows)
