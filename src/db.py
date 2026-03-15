"""Database initialization and helpers."""

import logging

import aiosqlite

from config import DB_PATH

logger = logging.getLogger(__name__)


async def init_db():
    """Create all tables if they don't exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS earthquakes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                event_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL,
                magnitude REAL,
                magnitude_type TEXT,
                max_intensity INTEGER,
                location_ja TEXT,
                location_en TEXT,
                received_at TEXT NOT NULL,
                UNIQUE(source, event_id)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_eq_occurred
            ON earthquakes(occurred_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_eq_location
            ON earthquakes(latitude, longitude)
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS collector_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                records_inserted INTEGER DEFAULT 0,
                error_message TEXT,
                collected_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_collector_time
            ON collector_status(collected_at)
        """)

        # Phase 2: AMeDAS atmospheric data
        await db.execute("""
            CREATE TABLE IF NOT EXISTS amedas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                station_name TEXT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                observed_at TEXT NOT NULL,
                pressure_hpa REAL,
                temperature_c REAL,
                humidity_pct REAL,
                wind_speed_ms REAL,
                wind_direction INTEGER,
                precipitation_1h REAL,
                received_at TEXT NOT NULL,
                UNIQUE(station_id, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_amedas_time
            ON amedas(observed_at)
        """)

        # Phase 2: Geomagnetic data (NOAA SWPC GOES + Kp)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS geomag_goes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time_tag TEXT NOT NULL,
                satellite INTEGER,
                he REAL,
                hp REAL,
                hn REAL,
                total REAL,
                received_at TEXT NOT NULL,
                UNIQUE(time_tag, satellite)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_geomag_goes_time
            ON geomag_goes(time_tag)
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS geomag_kp (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time_tag TEXT NOT NULL UNIQUE,
                kp REAL,
                a_running REAL,
                station_count INTEGER,
                received_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_geomag_kp_time
            ON geomag_kp(time_tag)
        """)

        # Phase 3: Volcano warnings
        await db.execute("""
            CREATE TABLE IF NOT EXISTS volcanoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                volcano_code TEXT NOT NULL,
                volcano_name_ja TEXT,
                volcano_name_en TEXT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                alert_level INTEGER DEFAULT 1,
                alert_code TEXT,
                alert_name_ja TEXT,
                report_datetime TEXT,
                received_at TEXT NOT NULL,
                UNIQUE(volcano_code)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_volcano_code
            ON volcanoes(volcano_code)
        """)

        # Phase 3: Sea surface temperature
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sst (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                temperature_c REAL NOT NULL,
                observed_at TEXT NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(latitude, longitude, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sst_time
            ON sst(observed_at)
        """)

        # Phase 4: Ionosphere TEC (Total Electron Content)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tec (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                tec_tecu REAL NOT NULL,
                epoch TEXT NOT NULL,
                product_type TEXT NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(latitude, longitude, epoch)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_tec_epoch
            ON tec(epoch)
        """)

        # Phase 4: GEONET crustal deformation (F5 daily solutions)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS geonet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                station_name TEXT,
                observed_at TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                height_m REAL NOT NULL,
                dx_mm REAL,
                dy_mm REAL,
                dz_mm REAL,
                received_at TEXT NOT NULL,
                UNIQUE(station_id, observed_at)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_geonet_time
            ON geonet(observed_at)
        """)

        # Phase 5: Focal mechanisms (CMT solutions)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS focal_mechanisms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                event_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL NOT NULL,
                magnitude REAL NOT NULL,
                strike1 REAL NOT NULL,
                dip1 REAL NOT NULL,
                rake1 REAL NOT NULL,
                strike2 REAL,
                dip2 REAL,
                rake2 REAL,
                moment_nm REAL,
                received_at TEXT NOT NULL,
                UNIQUE(source, event_id)
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_fm_occurred
            ON focal_mechanisms(occurred_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_fm_location
            ON focal_mechanisms(latitude, longitude)
        """)

        await db.commit()
    logger.info("Database initialized: %s", DB_PATH)


async def purge_old_data(days: int = 90):
    """Delete records older than the specified number of days."""
    async with aiosqlite.connect(DB_PATH) as db:
        cutoff = f"-{days}"
        result = await db.execute(
            "DELETE FROM earthquakes WHERE occurred_at < datetime('now', ? || ' days')",
            (cutoff,),
        )
        eq_deleted = result.rowcount

        result = await db.execute(
            "DELETE FROM amedas WHERE observed_at < datetime('now', ? || ' days')",
            (cutoff,),
        )
        amedas_deleted = result.rowcount

        result = await db.execute(
            "DELETE FROM geomag_goes WHERE time_tag < datetime('now', ? || ' days')",
            (cutoff,),
        )
        goes_deleted = result.rowcount

        result = await db.execute(
            "DELETE FROM geomag_kp WHERE time_tag < datetime('now', ? || ' days')",
            (cutoff,),
        )
        kp_deleted = result.rowcount

        result = await db.execute(
            "DELETE FROM sst WHERE observed_at < datetime('now', ? || ' days')",
            (cutoff,),
        )
        sst_deleted = result.rowcount

        result = await db.execute(
            "DELETE FROM tec WHERE epoch < datetime('now', ? || ' days')",
            (cutoff,),
        )
        tec_deleted = result.rowcount

        await db.execute(
            "DELETE FROM collector_status WHERE collected_at < datetime('now', '-7 days')"
        )
        await db.commit()

    total = eq_deleted + amedas_deleted + goes_deleted + kp_deleted + sst_deleted + tec_deleted
    if total:
        logger.info(
            "Purged: eq=%d amedas=%d goes=%d kp=%d sst=%d tec=%d",
            eq_deleted, amedas_deleted, goes_deleted, kp_deleted, sst_deleted, tec_deleted,
        )
