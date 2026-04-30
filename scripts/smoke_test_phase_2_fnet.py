"""Smoke test for Phase 2 F-net backfill acceleration.

Tests:
- Constants raised (MAX_BACKFILL_DAYS_PER_RUN, FAILED_DATES_RETRY_AFTER_DAYS).
- get_failed_dates respects last_failed_at cutoff (older than retry window
  rolls back into the fetch pool).

No network, no NIED credentials. Uses in-memory aiosqlite.
"""
from __future__ import annotations
import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))


class TestPhase2Constants(unittest.TestCase):
    def test_max_backfill_days_per_run_raised(self):
        import fetch_fnet_waveform as f
        self.assertEqual(f.MAX_BACKFILL_DAYS_PER_RUN, 30)

    def test_failed_dates_retry_after_days_defined(self):
        import fetch_fnet_waveform as f
        self.assertGreater(f.FAILED_DATES_RETRY_AFTER_DAYS, 0)

    def test_max_retries_before_skip_unchanged(self):
        import fetch_fnet_waveform as f
        self.assertEqual(f.MAX_RETRIES_BEFORE_SKIP, 3)


class TestFailedDatesRetryAfterDays(unittest.TestCase):
    def test_old_failed_date_rolls_back_into_fetch_pool(self):
        import fetch_fnet_waveform as f
        import aiosqlite

        async def run():
            async with aiosqlite.connect(":memory:") as db:
                await db.execute(f.FAILED_DATES_DDL)
                now = datetime.now(timezone.utc)
                old_iso = (now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS + 5)).isoformat()
                recent_iso = (now - timedelta(days=5)).isoformat()
                await db.execute(
                    "INSERT INTO fnet_failed_dates (date_str, last_failed_at, retry_count, reason) "
                    "VALUES (?, ?, ?, ?)",
                    ("2010-01-01", old_iso, 5, "no_records"),
                )
                await db.execute(
                    "INSERT INTO fnet_failed_dates (date_str, last_failed_at, retry_count, reason) "
                    "VALUES (?, ?, ?, ?)",
                    ("2024-01-01", recent_iso, 5, "no_records"),
                )
                await db.commit()

                skip = await f.get_failed_dates(db)
                self.assertNotIn("2010-01-01", skip)
                self.assertIn("2024-01-01", skip)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
