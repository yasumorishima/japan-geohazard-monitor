"""Base collector with retry, batch insert, and health tracking."""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import aiosqlite

from config import DB_PATH

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base for all data collectors."""

    source_name: str = "unknown"
    interval_sec: int = 300

    @abstractmethod
    async def fetch(self, session) -> list[dict]:
        """Fetch data from the source. Return list of parsed records."""

    @abstractmethod
    def to_rows(self, records: list[dict]) -> list[tuple]:
        """Convert parsed records to DB insert tuples."""

    @abstractmethod
    async def insert_rows(self, db: aiosqlite.Connection, rows: list[tuple]) -> int:
        """Insert rows into the database. Return number inserted."""

    async def run(self, session):
        """Main loop: fetch → insert → sleep → repeat."""
        logger.info("[%s] Collector started (interval=%ds)", self.source_name, self.interval_sec)

        while True:
            now = datetime.now(timezone.utc).isoformat()
            try:
                records = await self.fetch(session)
                rows = self.to_rows(records)

                inserted = 0
                if rows:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("PRAGMA synchronous=FULL")
                        await db.execute("PRAGMA busy_timeout=10000")
                        inserted = await self.insert_rows(db, rows)
                        await db.execute(
                            "INSERT INTO collector_status (source, status, records_inserted, collected_at) "
                            "VALUES (?, 'success', ?, ?)",
                            (self.source_name, inserted, now),
                        )
                        await db.commit()

                if inserted:
                    logger.info("[%s] Inserted %d records", self.source_name, inserted)

            except Exception as e:
                logger.warning("[%s] Error: %s", self.source_name, e)
                try:
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "INSERT INTO collector_status (source, status, error_message, collected_at) "
                            "VALUES (?, 'error', ?, ?)",
                            (self.source_name, str(e)[:500], now),
                        )
                        await db.commit()
                except Exception:
                    pass

            await asyncio.sleep(self.interval_sec)
