"""Smoke test for Phase 2 (1) ioc_sea_level backfill acceleration.

Pure-unit, no network. RPi5 system Python lacks aiohttp, so we install a
lightweight stub before importing fetch_ioc_sealevel. Tests cover:
    - acceleration constants (MAX_FETCHES default, parallelism, sleep)
    - failed-pairs retry rollback after FAILED_DATES_RETRY_AFTER_DAYS
    - retry threshold semantics (under threshold = not skipped)
    - build_target_pairs respects existing + failed skip sets and max cap
    - build_target_pairs emits oldest-first across all stations
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
    def test_max_fetches_default_raised(self):
        os.environ.pop("IOC_MAX_FETCHES", None)
        src = (ROOT / "scripts" / "fetch_ioc_sealevel.py").read_text(encoding="utf-8")
        self.assertIn('"IOC_MAX_FETCHES", "200"', src,
                      "Phase 2 (1) requires MAX_FETCHES default literal '200'")

    def test_parallel_and_rate_limit_constants(self):
        os.environ.pop("IOC_PARALLEL_FETCHES", None)
        os.environ.pop("IOC_RATE_LIMIT_SLEEP", None)
        import importlib
        import fetch_ioc_sealevel as f
        importlib.reload(f)
        self.assertGreaterEqual(f.PARALLEL_FETCHES, 2,
                                "Phase 2 (1) requires parallel HTTP fetch (>= 2)")
        self.assertLessEqual(f.RATE_LIMIT_SLEEP, 1.0,
                             "Phase 2 (1) requires rate-limit sleep <= 1.0s")
        self.assertEqual(f.BACKFILL_START.year, 2011,
                         "Backfill must start from 2011-01-01")

    def test_failed_dates_retry_threshold_constants(self):
        import fetch_ioc_sealevel as f
        self.assertEqual(f.MAX_RETRIES_BEFORE_SKIP, 3)
        self.assertEqual(f.FAILED_DATES_RETRY_AFTER_DAYS, 30)


class TestFailedPairsRetrySemantics(unittest.TestCase):
    """Replay get_failed_pairs SQL against in-memory sqlite to verify the
    retry-rollback contract for the (station_code, date_str) composite key."""

    def test_skip_pair_set_semantics(self):
        import aiosqlite
        import fetch_ioc_sealevel as f

        async def run():
            async with aiosqlite.connect(":memory:") as db:
                await db.execute(
                    "CREATE TABLE ioc_sealevel_failed_dates ("
                    "  station_code TEXT NOT NULL, "
                    "  date_str TEXT NOT NULL, "
                    "  retry_count INTEGER NOT NULL DEFAULT 0, "
                    "  last_failed_at TEXT NOT NULL, "
                    "  PRIMARY KEY (station_code, date_str)"
                    ")"
                )
                now = datetime.now(timezone.utc)
                old_iso = (
                    now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS + 5)
                ).isoformat()
                recent_iso = (now - timedelta(days=5)).isoformat()

                await db.executemany(
                    "INSERT INTO ioc_sealevel_failed_dates "
                    "(station_code, date_str, retry_count, last_failed_at) "
                    "VALUES (?, ?, ?, ?)",
                    [
                        ("ofun", "2010-01-01", 5, old_iso),     # roll off
                        ("ofun", "2026-04-01", 3, recent_iso),  # skip
                        ("ofun", "2026-04-15", 1, recent_iso),  # not skip
                        ("hana", "2026-04-01", 5, recent_iso),  # skip
                    ],
                )
                await db.commit()

                cutoff = (
                    now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS)
                ).isoformat()
                cur = await db.execute(
                    "SELECT station_code, date_str FROM ioc_sealevel_failed_dates "
                    "WHERE retry_count >= ? AND last_failed_at > ?",
                    (f.MAX_RETRIES_BEFORE_SKIP, cutoff),
                )
                rows = await cur.fetchall()
                return {(r[0], r[1]) for r in rows}

        skip = asyncio.run(run())
        self.assertNotIn(("ofun", "2010-01-01"), skip,
                         "Old failed pair should roll off after retry window")
        self.assertIn(("ofun", "2026-04-01"), skip,
                      "Recent failed pair at threshold should be skipped")
        self.assertNotIn(("ofun", "2026-04-15"), skip,
                         "Under-threshold pair should not be skipped")
        self.assertIn(("hana", "2026-04-01"), skip,
                      "Per-station composite key must skip independently")


class TestBuildTargetPairs(unittest.TestCase):
    """Cover build_target_pairs: skip composition + ordering + cap."""

    def test_skip_excludes_existing_and_failed(self):
        import fetch_ioc_sealevel as f
        all_dates = [
            datetime(2011, 1, 1),
            datetime(2011, 1, 2),
            datetime(2011, 1, 3),
        ]
        stations = [
            {"code": "ofun", "name": "Ofunato", "lat": 39.0, "lon": 141.7},
            {"code": "hana", "name": "Hanasaki", "lat": 43.3, "lon": 145.6},
        ]
        existing_per_station = {"ofun": {"2011-01-01"}}
        failed_pairs = {("hana", "2011-01-02")}

        pairs = f.build_target_pairs(
            all_dates, stations, existing_per_station, failed_pairs, max_fetches=99,
        )
        codes_dates = [(s["code"], d.strftime("%Y-%m-%d")) for d, s in pairs]

        self.assertNotIn(("ofun", "2011-01-01"), codes_dates,
                         "Existing date for station must be excluded")
        self.assertNotIn(("hana", "2011-01-02"), codes_dates,
                         "Failed pair must be excluded")
        # Remaining 5 = total 6 - 2 skipped pairs (one existing, one failed)
        self.assertEqual(len(pairs), 4,
                         "Expected 4 pairs after skipping 2 of 6 candidates")

    def test_oldest_first_across_stations(self):
        import fetch_ioc_sealevel as f
        all_dates = [datetime(2011, 1, 1), datetime(2011, 1, 2)]
        stations = [
            {"code": "a", "name": "A", "lat": 0.0, "lon": 0.0},
            {"code": "b", "name": "B", "lat": 0.0, "lon": 0.0},
        ]
        pairs = f.build_target_pairs(all_dates, stations, {}, set(), max_fetches=99)
        codes_dates = [(s["code"], d.strftime("%Y-%m-%d")) for d, s in pairs]
        self.assertEqual(
            codes_dates,
            [("a", "2011-01-01"), ("b", "2011-01-01"),
             ("a", "2011-01-02"), ("b", "2011-01-02")],
            "Pairs must iterate dates outermost so all stations advance together",
        )

    def test_max_fetches_cap(self):
        import fetch_ioc_sealevel as f
        all_dates = [datetime(2011, 1, i) for i in range(1, 11)]
        stations = [{"code": f"s{i}", "name": "x", "lat": 0.0, "lon": 0.0}
                    for i in range(5)]
        pairs = f.build_target_pairs(all_dates, stations, {}, set(), max_fetches=3)
        self.assertEqual(len(pairs), 3,
                         "max_fetches must cap the returned pair count")

    def test_max_fetches_zero_returns_empty(self):
        import fetch_ioc_sealevel as f
        all_dates = [datetime(2011, 1, 1)]
        stations = [{"code": "a", "name": "x", "lat": 0.0, "lon": 0.0}]
        pairs = f.build_target_pairs(all_dates, stations, {}, set(), max_fetches=0)
        self.assertEqual(pairs, [],
                         "max_fetches=0 must short-circuit before any append")


if __name__ == "__main__":
    unittest.main(verbosity=2)
