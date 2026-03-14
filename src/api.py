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


@app.get("/api/amedas")
async def amedas_data(metric: str = "pressure"):
    """Return latest AMeDAS snapshot as GeoJSON-like list.

    metric: pressure | temperature | humidity | wind | precipitation
    """
    col_map = {
        "pressure": "pressure_hpa",
        "temperature": "temperature_c",
        "humidity": "humidity_pct",
        "wind": "wind_speed_ms",
        "precipitation": "precipitation_1h",
    }
    col = col_map.get(metric, "pressure_hpa")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Get the latest observed_at timestamp
        latest = await db.execute_fetchall(
            "SELECT MAX(observed_at) FROM amedas"
        )
        latest_time = latest[0][0] if latest and latest[0][0] else None

        if not latest_time:
            return {"stations": [], "count": 0, "observed_at": None, "metric": metric}

        rows = await db.execute_fetchall(
            f"""SELECT station_id, station_name, latitude, longitude,
                       observed_at, {col} as value
                FROM amedas
                WHERE observed_at = ? AND {col} IS NOT NULL""",
            (latest_time,),
        )

    stations = [
        {
            "id": r["station_id"],
            "name": r["station_name"],
            "lat": r["latitude"],
            "lon": r["longitude"],
            "value": r["value"],
        }
        for r in rows
    ]
    return {
        "stations": stations,
        "count": len(stations),
        "observed_at": latest_time,
        "metric": metric,
    }


@app.get("/api/geomag/goes")
async def geomag_goes(hours: int = 24):
    """Return GOES magnetometer data (downsampled to 10-min intervals)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Downsample: take every 10th row (10-min cadence from 1-min data)
        rows = await db.execute_fetchall(
            """SELECT time_tag, he, hp, hn, total
               FROM geomag_goes
               WHERE time_tag > datetime('now', ? || ' hours')
               AND id % 10 = 0
               ORDER BY time_tag""",
            (f"-{hours}",),
        )
    return {
        "data": [
            {
                "time": r["time_tag"],
                "He": r["he"],
                "Hp": r["hp"],
                "Hn": r["hn"],
                "total": r["total"],
            }
            for r in rows
        ],
        "count": len(rows),
    }


@app.get("/api/geomag/kp")
async def geomag_kp(days: int = 7):
    """Return Kp index time series."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall(
            """SELECT time_tag, kp, a_running, station_count
               FROM geomag_kp
               WHERE time_tag > datetime('now', ? || ' days')
               ORDER BY time_tag""",
            (f"-{days}",),
        )
    return {
        "data": [
            {
                "time": r["time_tag"],
                "kp": r["kp"],
                "a_running": r["a_running"],
                "station_count": r["station_count"],
            }
            for r in rows
        ],
        "count": len(rows),
    }


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

        amedas_count = (await db.execute_fetchall(
            "SELECT COUNT(DISTINCT station_id) FROM amedas"
        ))[0][0]
        amedas_latest = (await db.execute_fetchall(
            "SELECT MAX(observed_at) FROM amedas"
        ))[0][0]

        kp_latest = (await db.execute_fetchall(
            "SELECT kp FROM geomag_kp ORDER BY time_tag DESC LIMIT 1"
        ))
        kp_val = kp_latest[0][0] if kp_latest else None

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
        "amedas_stations": amedas_count,
        "amedas_latest": amedas_latest,
        "kp_latest": kp_val,
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
