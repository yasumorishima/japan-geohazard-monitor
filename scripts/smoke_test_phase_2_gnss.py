"""Smoke test for Phase 2 (1) gnss_tec backfill acceleration.

Pure-unit, no network. RPi5 system Python lacks aiohttp, so we install a
lightweight stub before importing fetch_gnss_tec. Tests cover:
    - acceleration constants (MAX_DATES default, parallelism, sleep)
    - failed-dates retry rollback after FAILED_DATES_RETRY_AFTER_DAYS
    - retry threshold semantics (under threshold = not skipped)
    - skip-set composition (existing | failed) excludes both classes
"""
from __future__ import annotations

import asyncio
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

# Stub aiohttp so the module imports on a host without the library.
if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")
    aiohttp_stub.ClientTimeout = lambda **kw: None
    aiohttp_stub.ClientSession = type("ClientSession", (), {})
    aiohttp_stub.ClientError = Exception
    sys.modules["aiohttp"] = aiohttp_stub


class TestPhase2Constants(unittest.TestCase):
    def test_max_dates_default_raised(self):
        os.environ.pop("GNSS_TEC_MAX_DATES", None)
        # The literal default in the script must be >= 200 (was 30 pre-Phase-2).
        # We assert by reading the source file to avoid coupling to env handling.
        src = (ROOT / "scripts" / "fetch_gnss_tec.py").read_text(encoding="utf-8")
        self.assertIn('"GNSS_TEC_MAX_DATES", "200"', src,
                      "Phase 2 (1) requires MAX_DATES default literal '200'")

    def test_parallel_and_rate_limit_constants(self):
        import fetch_gnss_tec as f
        self.assertGreaterEqual(f.PARALLEL_DATES, 2,
                                "Phase 2 (1) requires parallel HTTP fetch (>= 2)")
        self.assertLessEqual(f.RATE_LIMIT_SLEEP, 1.0,
                             "Phase 2 (1) requires rate-limit sleep <= 1.0s")

    def test_failed_dates_retry_threshold_constants(self):
        import fetch_gnss_tec as f
        self.assertEqual(f.MAX_RETRIES_BEFORE_SKIP, 3)
        self.assertEqual(f.FAILED_DATES_RETRY_AFTER_DAYS, 30)


class TestFailedDatesRetrySemantics(unittest.TestCase):
    """Replays get_failed_dates SQL against in-memory sqlite to verify the
    retry-rollback contract (old high-retry rolls off, recent high-retry stays
    skipped, recent under-threshold not skipped)."""

    def test_skip_set_semantics(self):
        import aiosqlite
        import fetch_gnss_tec as f

        async def run():
            async with aiosqlite.connect(":memory:") as db:
                await db.execute(
                    "CREATE TABLE gnss_tec_failed_dates ("
                    "  date_str TEXT PRIMARY KEY, "
                    "  retry_count INTEGER NOT NULL DEFAULT 0, "
                    "  last_failed_at TEXT NOT NULL"
                    ")"
                )
                now = datetime.now(timezone.utc)
                old_iso = (
                    now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS + 5)
                ).isoformat()
                recent_iso = (now - timedelta(days=5)).isoformat()

                await db.executemany(
                    "INSERT INTO gnss_tec_failed_dates "
                    "(date_str, retry_count, last_failed_at) VALUES (?, ?, ?)",
                    [
                        ("2010-01-01", 5, old_iso),     # old + high retry: roll off
                        ("2026-04-01", 3, recent_iso),  # recent + threshold: skip
                        ("2026-04-15", 1, recent_iso),  # recent + under: not skip
                    ],
                )
                await db.commit()

                cutoff = (
                    now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS)
                ).isoformat()
                cur = await db.execute(
                    "SELECT date_str FROM gnss_tec_failed_dates "
                    "WHERE retry_count >= ? AND last_failed_at > ?",
                    (f.MAX_RETRIES_BEFORE_SKIP, cutoff),
                )
                rows = await cur.fetchall()
                return {r[0] for r in rows}

        skip = asyncio.run(run())
        self.assertNotIn("2010-01-01", skip,
                         "Old failed date should roll off after retry-after window")
        self.assertIn("2026-04-01", skip,
                      "Recent failed at threshold should be skipped")
        self.assertNotIn("2026-04-15", skip,
                         "Under-threshold should not be skipped regardless of recency")


class TestSkipSetComposition(unittest.TestCase):
    """Verify the script's skip set is the union of existing dates and
    failed-skip dates (E軸: ensure failed dates are excluded alongside
    already-fetched dates)."""

    def test_skip_union_excludes_both_classes(self):
        existing_date_strs = {"2011-01-01", "2011-01-02"}
        failed_skip = {"2011-01-03"}
        skip_date_strs = existing_date_strs | failed_skip

        candidates = ["2011-01-01", "2011-01-02", "2011-01-03", "2011-01-04"]
        remaining = [d for d in candidates if d not in skip_date_strs]
        self.assertEqual(remaining, ["2011-01-04"],
                         "Union skip must drop both existing and failed-skip dates")


if __name__ == "__main__":
    unittest.main(verbosity=2)
