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

        await db.commit()
    logger.info("Database initialized: %s", DB_PATH)


async def purge_old_data(days: int = 90):
    """Delete records older than the specified number of days."""
    async with aiosqlite.connect(DB_PATH) as db:
        result = await db.execute(
            "DELETE FROM earthquakes WHERE occurred_at < datetime('now', ? || ' days')",
            (f"-{days}",),
        )
        deleted = result.rowcount
        await db.execute(
            "DELETE FROM collector_status WHERE collected_at < datetime('now', '-7 days')"
        )
        await db.commit()
    if deleted:
        logger.info("Purged %d old earthquake records", deleted)
