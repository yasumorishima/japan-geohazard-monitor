"""FastAPI endpoints for the Japan Geohazard Monitor."""

import aiosqlite
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config import DB_PATH

app = FastAPI(title="Japan Geohazard Monitor")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def intensity_label(scale: int | None) -> str:
    """Convert P2P-style intensity code to display string."""
    if scale is None:
        return ""
    labels = {
        10: "1", 20: "2", 30: "3", 40: "4",
        45: "5-", 50: "5+", 55: "6-", 60: "6+", 70: "7",
    }
    return labels.get(scale, str(scale))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("map.html", {"request": request})


@app.get("/api/earthquakes")
async def earthquakes(hours: int = 24):
    """Return earthquakes from the last N hours."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall(
            """
            SELECT source, event_id, occurred_at, latitude, longitude,
                   depth_km, magnitude, magnitude_type, max_intensity,
                   location_ja, location_en
            FROM earthquakes
            WHERE occurred_at > datetime('now', ? || ' hours')
            ORDER BY occurred_at DESC
            """,
            (f"-{hours}",),
        )
    events = []
    for r in rows:
        events.append({
            "source": r["source"],
            "event_id": r["event_id"],
            "time": r["occurred_at"],
            "lat": r["latitude"],
            "lon": r["longitude"],
            "depth": r["depth_km"],
            "mag": r["magnitude"],
            "mag_type": r["magnitude_type"],
            "intensity": intensity_label(r["max_intensity"]),
            "intensity_raw": r["max_intensity"],
            "location_ja": r["location_ja"],
            "location_en": r["location_en"],
        })
    return {"earthquakes": events, "count": len(events)}


@app.get("/api/stats")
async def stats():
    """Return basic statistics."""
    async with aiosqlite.connect(DB_PATH) as db:
        total = (await db.execute_fetchall(
            "SELECT COUNT(*) FROM earthquakes"
        ))[0][0]
        last_24h = (await db.execute_fetchall(
            "SELECT COUNT(*) FROM earthquakes WHERE occurred_at > datetime('now', '-24 hours')"
        ))[0][0]
        last_7d = (await db.execute_fetchall(
            "SELECT COUNT(*) FROM earthquakes WHERE occurred_at > datetime('now', '-7 days')"
        ))[0][0]

        # Latest collector status per source
        collectors = await db.execute_fetchall("""
            SELECT source, status, records_inserted, collected_at
            FROM collector_status
            WHERE id IN (SELECT MAX(id) FROM collector_status GROUP BY source)
        """)

    return {
        "total_records": total,
        "earthquakes_24h": last_24h,
        "earthquakes_7d": last_7d,
        "collectors": [
            {
                "source": c[0],
                "status": c[1],
                "records": c[2],
                "last_run": c[3],
            }
            for c in collectors
        ],
    }
