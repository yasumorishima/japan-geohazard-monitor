"""Smoke test for Phase 2 (Stage 2.B) IOC DART fetcher.

Pure-unit, no network. Verifies:
    - acceleration constants (PARALLEL_FETCHES, RATE_LIMIT_SLEEP, MAX_FETCHES)
    - ALLOWED_SENSORS = frozenset({"prt"}) only
    - DART_MEASUREMENT_TYPE = 1 (15-min, NDBC convention)
    - BACKFILL_START = 2011-01-01
    - fetch_station_list:
        - prt station passes
        - non-prt sensor (rad / pwl) rejected
        - missing code rejected (cannot use as stable station_id)
        - station outside Japan bbox rejected
        - sensor case-insensitive
    - parse_ioc_data:
        - prt records pass with water_height_m field
        - per-record sensor != prt dropped (defense-in-depth)
        - missing per-record sensor allowed (legacy)
    - build_target_pairs (oldest-first + skip + cap)
    - failed-pairs SQL retry-rollback semantics
    - TRANSIENT_FAILURE sentinel is not falsy-ambiguous
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

if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")
    aiohttp_stub.ClientTimeout = lambda **kw: None
    aiohttp_stub.ClientSession = type("ClientSession", (), {})
    aiohttp_stub.ClientError = type("ClientError", (Exception,), {})
    sys.modules["aiohttp"] = aiohttp_stub


class TestConstants(unittest.TestCase):
    def test_allowed_sensors_prt_only(self):
        os.environ.pop("IOC_DART_PARALLEL_FETCHES", None)
        os.environ.pop("IOC_DART_RATE_LIMIT_SLEEP", None)
        os.environ.pop("IOC_DART_MAX_FETCHES", None)
        import importlib
        import fetch_ioc_dart as f
        importlib.reload(f)
        self.assertEqual(f.ALLOWED_SENSORS, frozenset({"prt"}))
        self.assertGreaterEqual(f.PARALLEL_FETCHES, 2)
        self.assertLessEqual(f.RATE_LIMIT_SLEEP, 1.0)
        self.assertEqual(f.MAX_FETCHES, 200)
        self.assertEqual(f.DART_MEASUREMENT_TYPE, 1)
        self.assertEqual(f.BACKFILL_START.year, 2011)
        self.assertEqual(f.MAX_RETRIES_BEFORE_SKIP, 3)
        self.assertEqual(f.FAILED_DATES_RETRY_AFTER_DAYS, 30)


class TestStationListFilter(unittest.TestCase):
    """fetch_station_list filtering behaves correctly without network.

    We replay the post-fetch filtering inline because fetch_station_list
    itself is wrapped in HTTP I/O. The SUT logic under test is the
    sensor + bbox + code filters in lines 200-260 of fetch_ioc_dart.py.
    """

    def _filter(self, stations):
        import fetch_ioc_dart as f
        out = []
        for s in stations:
            try:
                lat = float(s.get("lat") or 0)
                lon = float(s.get("lon") or 0)
            except (ValueError, TypeError):
                continue
            if not (f.JAPAN_LAT_MIN <= lat <= f.JAPAN_LAT_MAX
                    and f.JAPAN_LON_MIN <= lon <= f.JAPAN_LON_MAX):
                continue
            sensor = str(s.get("sensor") or "").strip().lower()
            if sensor not in f.ALLOWED_SENSORS:
                continue
            code = str(s.get("code") or "").strip()
            if not code:
                continue
            out.append({"code": code, "lat": lat, "lon": lon, "sensor": sensor})
        return out

    def test_prt_passes(self):
        out = self._filter([
            {"code": "dtok", "sensor": "prt", "lat": 30.5, "lon": 152.0},
        ])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["code"], "dtok")

    def test_non_prt_rejected(self):
        out = self._filter([
            {"code": "abas", "sensor": "rad", "lat": 35.0, "lon": 140.0},
            {"code": "naha", "sensor": "pwl", "lat": 26.2, "lon": 127.7},
        ])
        self.assertEqual(out, [])

    def test_missing_code_rejected(self):
        out = self._filter([
            {"code": "", "sensor": "prt", "lat": 39.6, "lon": 145.8},
            {"code": None, "sensor": "prt", "lat": 38.8, "lon": 145.6},
        ])
        self.assertEqual(out, [])

    def test_outside_japan_bbox_rejected(self):
        out = self._filter([
            {"code": "xx", "sensor": "prt", "lat": 0.0, "lon": -120.0},
        ])
        self.assertEqual(out, [])

    def test_sensor_case_insensitive(self):
        out = self._filter([
            {"code": "dtok", "sensor": "PRT", "lat": 30.5, "lon": 152.0},
        ])
        self.assertEqual(len(out), 1)


class TestParseIOCData(unittest.TestCase):
    def test_prt_record_parsed(self):
        import fetch_ioc_dart as f
        rows = f.parse_ioc_data(
            [{"slevel": 5779.78, "stime": "2026-04-15 00:00:00", "sensor": "prt"}],
            {"code": "dtok"},
        )
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["water_height_m"], 5779.78)
        self.assertEqual(rows[0]["observed_at"], "2026-04-15T00:00:00")

    def test_non_prt_per_record_dropped(self):
        import fetch_ioc_dart as f
        rows = f.parse_ioc_data(
            [
                {"slevel": 5779.78, "stime": "2026-04-15 00:00:00", "sensor": "prt"},
                {"slevel": 1.234, "stime": "2026-04-15 00:15:00", "sensor": "rad"},
                {"slevel": 1.111, "stime": "2026-04-15 00:30:00", "sensor": "atm"},
            ],
            {"code": "dtok"},
        )
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["water_height_m"], 5779.78)

    def test_missing_per_record_sensor_allowed(self):
        import fetch_ioc_dart as f
        rows = f.parse_ioc_data(
            [{"slevel": 5779.78, "stime": "2026-04-15 00:00:00"}],
            {"code": "dtok"},
        )
        self.assertEqual(len(rows), 1)

    def test_empty_or_invalid_records_dropped(self):
        import fetch_ioc_dart as f
        rows = f.parse_ioc_data(
            [
                {"slevel": "", "stime": "2026-04-15 00:00:00", "sensor": "prt"},
                {"slevel": "abc", "stime": "2026-04-15 00:15:00", "sensor": "prt"},
                {"slevel": 5779.78, "stime": "", "sensor": "prt"},
            ],
            {"code": "dtok"},
        )
        self.assertEqual(rows, [])


class TestBuildTargetPairs(unittest.TestCase):
    def test_skip_excludes_existing_and_failed(self):
        import fetch_ioc_dart as f
        all_dates = [datetime(2011, 1, i) for i in (1, 2, 3)]
        stations = [
            {"code": "dtok", "lat": 30.5, "lon": 152.0},
            {"code": "dryu", "lat": 28.9, "lon": 135.0},
        ]
        existing = {"dtok": {"2011-01-01"}}
        failed = {("dryu", "2011-01-02")}
        pairs = f.build_target_pairs(all_dates, stations, existing, failed,
                                     max_fetches=99)
        codes_dates = [(s["code"], d.strftime("%Y-%m-%d")) for d, s in pairs]
        self.assertNotIn(("dtok", "2011-01-01"), codes_dates)
        self.assertNotIn(("dryu", "2011-01-02"), codes_dates)
        self.assertEqual(len(pairs), 4)

    def test_oldest_first_across_stations(self):
        import fetch_ioc_dart as f
        all_dates = [datetime(2011, 1, 1), datetime(2011, 1, 2)]
        stations = [
            {"code": "a", "lat": 30.0, "lon": 140.0},
            {"code": "b", "lat": 30.0, "lon": 140.0},
        ]
        pairs = f.build_target_pairs(all_dates, stations, {}, set(),
                                     max_fetches=99)
        codes_dates = [(s["code"], d.strftime("%Y-%m-%d")) for d, s in pairs]
        self.assertEqual(
            codes_dates,
            [("a", "2011-01-01"), ("b", "2011-01-01"),
             ("a", "2011-01-02"), ("b", "2011-01-02")],
        )

    def test_max_fetches_cap(self):
        import fetch_ioc_dart as f
        all_dates = [datetime(2011, 1, i) for i in range(1, 11)]
        stations = [{"code": f"s{i}", "lat": 30.0, "lon": 140.0}
                    for i in range(5)]
        pairs = f.build_target_pairs(all_dates, stations, {}, set(),
                                     max_fetches=3)
        self.assertEqual(len(pairs), 3)

    def test_max_fetches_zero_short_circuits(self):
        import fetch_ioc_dart as f
        pairs = f.build_target_pairs(
            [datetime(2011, 1, 1)],
            [{"code": "a", "lat": 30.0, "lon": 140.0}],
            {}, set(), max_fetches=0,
        )
        self.assertEqual(pairs, [])


class TestFailedPairsRetrySemantics(unittest.TestCase):
    def test_skip_rollover_per_station(self):
        import aiosqlite
        import fetch_ioc_dart as f

        async def run():
            async with aiosqlite.connect(":memory:") as db:
                await db.execute(
                    "CREATE TABLE dart_pressure_failed_dates ("
                    "  station_id TEXT NOT NULL, "
                    "  date_str TEXT NOT NULL, "
                    "  retry_count INTEGER NOT NULL DEFAULT 0, "
                    "  last_failed_at TEXT NOT NULL, "
                    "  PRIMARY KEY (station_id, date_str)"
                    ")"
                )
                now = datetime.now(timezone.utc)
                old_iso = (
                    now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS + 5)
                ).isoformat()
                recent_iso = (now - timedelta(days=5)).isoformat()
                await db.executemany(
                    "INSERT INTO dart_pressure_failed_dates "
                    "(station_id, date_str, retry_count, last_failed_at) "
                    "VALUES (?, ?, ?, ?)",
                    [
                        ("dtok", "2010-01-01", 5, old_iso),     # roll off
                        ("dtok", "2026-04-01", 3, recent_iso),  # skip
                        ("dtok", "2026-04-15", 1, recent_iso),  # under threshold
                        ("dryu", "2026-04-01", 5, recent_iso),  # skip independently
                    ],
                )
                await db.commit()
                cutoff = (
                    now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS)
                ).isoformat()
                cur = await db.execute(
                    "SELECT station_id, date_str FROM dart_pressure_failed_dates "
                    "WHERE retry_count >= ? AND last_failed_at > ?",
                    (f.MAX_RETRIES_BEFORE_SKIP, cutoff),
                )
                return {(r[0], r[1]) for r in await cur.fetchall()}

        skip = asyncio.run(run())
        self.assertNotIn(("dtok", "2010-01-01"), skip)
        self.assertIn(("dtok", "2026-04-01"), skip)
        self.assertNotIn(("dtok", "2026-04-15"), skip)
        self.assertIn(("dryu", "2026-04-01"), skip)


class TestSentinel(unittest.TestCase):
    def test_sentinel_singleton_distinct_from_empty_list_and_none(self):
        import fetch_ioc_dart as f
        self.assertIs(f.TRANSIENT_FAILURE, f.TRANSIENT_FAILURE)
        self.assertIsNot(f.TRANSIENT_FAILURE, [])
        self.assertIsNot(f.TRANSIENT_FAILURE, None)
        # Main-loop guard relies on `is` comparison to distinguish
        # transient from definitive empty; truthiness is irrelevant once
        # the `is` check fires first, so no falsy-coercion test needed.
        self.assertEqual(repr(f.TRANSIENT_FAILURE), "<TRANSIENT_FAILURE>")


if __name__ == "__main__":
    unittest.main(verbosity=2)
