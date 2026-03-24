"""Safe database connection with corruption-prevention PRAGMAs.

Every fetch script MUST use this instead of raw aiosqlite.connect().
PRAGMAs set on every connection:
  - journal_mode=WAL: concurrent readers + single writer
  - synchronous=FULL: fsync after every commit (prevents data loss on crash)
  - busy_timeout=10000: wait 10s for locks instead of failing immediately
"""

import os
from contextlib import asynccontextmanager

import aiosqlite

DB_PATH = os.environ.get("GEOHAZARD_DB_PATH", "/app/data/geohazard.db")


@asynccontextmanager
async def safe_connect(db_path=None):
    """Open an aiosqlite connection with all safety PRAGMAs set."""
    path = db_path or DB_PATH
    async with aiosqlite.connect(path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=FULL")
        await db.execute("PRAGMA busy_timeout=10000")
        yield db
