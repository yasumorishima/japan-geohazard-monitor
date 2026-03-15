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


@app.get("/api/volcanoes")
async def volcano_data():
    """Return all volcanoes with current alert levels."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall(
            """SELECT volcano_code, volcano_name_ja, volcano_name_en,
                      latitude, longitude, alert_level, alert_code,
                      alert_name_ja, report_datetime
               FROM volcanoes
               ORDER BY alert_level DESC, volcano_code"""
        )
    volcanoes = [
        {
            "code": r["volcano_code"],
            "name_ja": r["volcano_name_ja"],
            "name_en": r["volcano_name_en"],
            "lat": r["latitude"],
            "lon": r["longitude"],
            "alert_level": r["alert_level"],
            "alert_code": r["alert_code"],
            "alert_name_ja": r["alert_name_ja"],
            "report_datetime": r["report_datetime"],
        }
        for r in rows
    ]
    elevated = sum(1 for v in volcanoes if v["alert_level"] >= 2)
    return {"volcanoes": volcanoes, "count": len(volcanoes), "elevated": elevated}


@app.get("/api/sst")
async def sst_data():
    """Return latest SST grid."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        latest = await db.execute_fetchall("SELECT MAX(observed_at) FROM sst")
        latest_time = latest[0][0] if latest and latest[0][0] else None

        if not latest_time:
            return {"grid": [], "count": 0, "observed_at": None}

        rows = await db.execute_fetchall(
            """SELECT latitude, longitude, temperature_c
               FROM sst WHERE observed_at = ?""",
            (latest_time,),
        )
    grid = [
        {"lat": r["latitude"], "lon": r["longitude"], "sst": r["temperature_c"]}
        for r in rows
    ]
    return {"grid": grid, "count": len(grid), "observed_at": latest_time}


@app.get("/api/tec")
async def tec_data(hours: int = 24):
    """Return TEC grid for the most recent epoch within the given window."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        latest = await db.execute_fetchall(
            """SELECT MAX(epoch) FROM tec
               WHERE epoch > datetime('now', ? || ' hours')""",
            (f"-{hours}",),
        )
        latest_epoch = latest[0][0] if latest and latest[0][0] else None

        if not latest_epoch:
            return {"grid": [], "count": 0, "epoch": None}

        rows = await db.execute_fetchall(
            """SELECT latitude, longitude, tec_tecu, product_type
               FROM tec WHERE epoch = ?""",
            (latest_epoch,),
        )
    grid = [
        {"lat": r["latitude"], "lon": r["longitude"],
         "tec": r["tec_tecu"], "product": r["product_type"]}
        for r in rows
    ]
    return {"grid": grid, "count": len(grid), "epoch": latest_epoch}


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


@app.get("/api/correlation")
async def correlation_data(days: int = 7):
    """Return time-aligned data for the correlation panel.

    Returns hourly earthquake counts, 3-hourly Kp, hourly TEC mean,
    and hourly mean pressure — all within the given day window.
    """
    cutoff = f"-{days}"
    async with aiosqlite.connect(DB_PATH) as db:
        # Hourly earthquake count
        eq_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', occurred_at) as hour,
                      COUNT(*) as count
               FROM earthquakes
               WHERE occurred_at > datetime('now', ? || ' days')
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )

        # Kp index (already 3-hourly)
        kp_rows = await db.execute_fetchall(
            """SELECT time_tag, kp FROM geomag_kp
               WHERE time_tag > datetime('now', ? || ' days')
               ORDER BY time_tag""",
            (cutoff,),
        )

        # GOES magnetic field total (hourly average)
        goes_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', time_tag) as hour,
                      AVG(total) as avg_total
               FROM geomag_goes
               WHERE time_tag > datetime('now', ? || ' days')
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )

        # TEC mean over Japan region (per epoch)
        tec_rows = await db.execute_fetchall(
            """SELECT epoch, AVG(tec_tecu) as avg_tec
               FROM tec
               WHERE epoch > datetime('now', ? || ' days')
               GROUP BY epoch ORDER BY epoch""",
            (cutoff,),
        )

        # Mean pressure (hourly, from AMeDAS stations that have pressure)
        pressure_rows = await db.execute_fetchall(
            """SELECT strftime('%Y-%m-%dT%H:00:00Z', observed_at) as hour,
                      AVG(pressure_hpa) as avg_pressure
               FROM amedas
               WHERE observed_at > datetime('now', ? || ' days')
                 AND pressure_hpa IS NOT NULL
               GROUP BY hour ORDER BY hour""",
            (cutoff,),
        )

    return {
        "earthquakes": [{"time": r[0], "count": r[1]} for r in eq_rows],
        "kp": [{"time": r[0], "kp": r[1]} for r in kp_rows],
        "goes": [{"time": r[0], "total": r[1]} for r in goes_rows],
        "tec": [{"time": r[0], "tec": r[1]} for r in tec_rows],
        "pressure": [{"time": r[0], "hpa": r[1]} for r in pressure_rows],
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

        volcano_elevated = (await db.execute_fetchall(
            "SELECT COUNT(*) FROM volcanoes WHERE alert_level >= 2"
        ))[0][0]
        volcano_total = (await db.execute_fetchall(
            "SELECT COUNT(*) FROM volcanoes"
        ))[0][0]

        sst_latest = (await db.execute_fetchall(
            "SELECT MAX(observed_at) FROM sst"
        ))[0][0]

        tec_latest = (await db.execute_fetchall(
            "SELECT MAX(epoch) FROM tec"
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
        "amedas_stations": amedas_count,
        "amedas_latest": amedas_latest,
        "kp_latest": kp_val,
        "volcano_total": volcano_total,
        "volcano_elevated": volcano_elevated,
        "sst_latest": sst_latest,
        "tec_latest": tec_latest,
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
