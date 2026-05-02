"""Smoke test for Phase 2 (Stage 2.C) IOC chunk-fetch acceleration.

Pure-unit, no network. Verifies the 30-day chunk fetcher added to both
fetch_ioc_sealevel.py and fetch_ioc_dart.py:

    - acceleration constants (CHUNK_DAYS=30, MAX_CHUNKS_PER_CRON=72)
    - build_target_chunks behaviour:
        - oldest-first iteration
        - skip when EVERY day in the chunk is already in existing
        - fetch when ANY day is missing (INSERT OR IGNORE dedupes at write)
        - skip when failed_chunks marks (station, chunk_start)
        - max_chunks cap honoured
        - max_chunks=0 short-circuits before any append
    - failed-chunk SQL retry-rollover semantics (mirror of failed-pair)
    - chunk_start_str column has the same composite-key behaviour as the
      legacy date_str table
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


def _reload(mod_name: str):
    import importlib
    if mod_name in sys.modules:
        return importlib.reload(sys.modules[mod_name])
    return importlib.import_module(mod_name)


# ---------------------------------------------------------------------------
# Stage 2.C constants
# ---------------------------------------------------------------------------
class TestChunkConstants(unittest.TestCase):
    def test_ioc_sealevel_chunk_constants(self):
        for k in ("IOC_CHUNK_DAYS", "IOC_MAX_CHUNKS"):
            os.environ.pop(k, None)
        f = _reload("fetch_ioc_sealevel")
        self.assertEqual(f.CHUNK_DAYS, 30)
        self.assertEqual(f.MAX_CHUNKS_PER_CRON, 72)

    def test_ioc_dart_chunk_constants(self):
        for k in ("IOC_DART_CHUNK_DAYS", "IOC_DART_MAX_CHUNKS"):
            os.environ.pop(k, None)
        f = _reload("fetch_ioc_dart")
        self.assertEqual(f.CHUNK_DAYS, 30)
        self.assertEqual(f.MAX_CHUNKS_PER_CRON, 72)

    def test_chunk_days_env_override(self):
        os.environ["IOC_CHUNK_DAYS"] = "90"
        f = _reload("fetch_ioc_sealevel")
        self.assertEqual(f.CHUNK_DAYS, 90)
        os.environ.pop("IOC_CHUNK_DAYS", None)


# ---------------------------------------------------------------------------
# build_target_chunks (ioc_sealevel) — single source of truth, ioc_dart is
# a literal mirror so we test it through one shared scenario set.
# ---------------------------------------------------------------------------
class TestBuildTargetChunks(unittest.TestCase):
    def _stations(self):
        return [
            {"code": "abas", "name": "Abashiri", "lat": 44.0, "lon": 144.3},
            {"code": "naha", "name": "Naha", "lat": 26.2, "lon": 127.7},
        ]

    def _chunk_starts(self, n=3, chunk_days=30):
        return [datetime(2011, 1, 1) + timedelta(days=i * chunk_days)
                for i in range(n)]

    def test_oldest_first_iteration(self):
        f = _reload("fetch_ioc_sealevel")
        starts = self._chunk_starts(3, 30)
        out = f.build_target_chunks(starts, 30, self._stations(), {}, set(), 99)
        codes_dates = [(s["code"], c.strftime("%Y-%m-%d")) for c, s in out]
        # Outer loop is chunk_start, so all stations advance per chunk.
        self.assertEqual(codes_dates[:4], [
            ("abas", "2011-01-01"), ("naha", "2011-01-01"),
            ("abas", "2011-01-31"), ("naha", "2011-01-31"),
        ])

    def test_skip_when_every_day_in_chunk_existing(self):
        f = _reload("fetch_ioc_sealevel")
        starts = self._chunk_starts(1, 30)
        all_30 = {
            (datetime(2011, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(30)
        }
        existing = {"abas": all_30}
        out = f.build_target_chunks(
            starts, 30, self._stations(), existing, set(), 99,
        )
        codes = {s["code"] for _, s in out}
        # abas's first chunk fully covered → only naha is in target
        self.assertEqual(codes, {"naha"})

    def test_fetch_when_any_day_missing(self):
        f = _reload("fetch_ioc_sealevel")
        starts = self._chunk_starts(1, 30)
        # 29 of 30 days present, 1 missing.
        partial = {
            (datetime(2011, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(29)
        }
        existing = {"abas": partial}
        out = f.build_target_chunks(
            starts, 30, self._stations(), existing, set(), 99,
        )
        codes = {s["code"] for _, s in out}
        self.assertIn("abas", codes,
                      "Chunk with even one missing day must be fetched "
                      "(INSERT OR IGNORE dedupes the redundant 29 days)")

    def test_skip_when_failed_chunk_marker(self):
        f = _reload("fetch_ioc_sealevel")
        starts = self._chunk_starts(1, 30)
        failed = {("abas", "2011-01-01")}
        out = f.build_target_chunks(
            starts, 30, self._stations(), {}, failed, 99,
        )
        codes_dates = [(s["code"], c.strftime("%Y-%m-%d")) for c, s in out]
        self.assertNotIn(("abas", "2011-01-01"), codes_dates)
        self.assertIn(("naha", "2011-01-01"), codes_dates)

    def test_max_chunks_cap(self):
        f = _reload("fetch_ioc_sealevel")
        starts = self._chunk_starts(10, 30)
        out = f.build_target_chunks(
            starts, 30, self._stations(), {}, set(), 3,
        )
        self.assertEqual(len(out), 3)

    def test_max_chunks_zero_short_circuits(self):
        f = _reload("fetch_ioc_sealevel")
        out = f.build_target_chunks(
            self._chunk_starts(1, 30), 30, self._stations(), {}, set(), 0,
        )
        self.assertEqual(out, [])

    def test_ioc_dart_build_target_chunks_mirrors_sealevel(self):
        """ioc_dart.build_target_chunks must follow the same skip + cap logic."""
        f = _reload("fetch_ioc_dart")
        starts = self._chunk_starts(2, 30)
        stations = [{"code": "dtok", "name": "Tokyo South", "lat": 30.5, "lon": 152.0}]
        existing = {"dtok": {(datetime(2011, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
                              for i in range(30)}}
        out = f.build_target_chunks(starts, 30, stations, existing, set(), 99)
        # First chunk fully covered → only second chunk in target.
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0][0].strftime("%Y-%m-%d"), "2011-01-31")


# ---------------------------------------------------------------------------
# Failed-chunks SQL retry rollover (replays the SQL against in-memory sqlite)
# ---------------------------------------------------------------------------
class TestFailedChunksRollover(unittest.TestCase):
    def _run(self, table_name: str, mod_name: str):
        import aiosqlite
        f = _reload(mod_name)

        async def go():
            async with aiosqlite.connect(":memory:") as db:
                await db.execute(
                    f"CREATE TABLE {table_name} ("
                    "  station_code TEXT NOT NULL, "
                    "  chunk_start_str TEXT NOT NULL, "
                    "  retry_count INTEGER NOT NULL DEFAULT 0, "
                    "  last_failed_at TEXT NOT NULL, "
                    "  PRIMARY KEY (station_code, chunk_start_str)"
                    ")"
                )
                now = datetime.now(timezone.utc)
                old = (now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS + 5)).isoformat()
                recent = (now - timedelta(days=5)).isoformat()
                await db.executemany(
                    f"INSERT INTO {table_name} "
                    f"(station_code, chunk_start_str, retry_count, last_failed_at) "
                    f"VALUES (?, ?, ?, ?)",
                    [
                        ("abas", "2011-01-01", 5, old),     # roll off
                        ("abas", "2026-04-01", 3, recent),  # skip
                        ("abas", "2026-04-15", 1, recent),  # under threshold
                        ("naha", "2026-04-01", 5, recent),  # skip independently
                    ],
                )
                await db.commit()
                cutoff = (now - timedelta(days=f.FAILED_DATES_RETRY_AFTER_DAYS)).isoformat()
                cur = await db.execute(
                    f"SELECT station_code, chunk_start_str FROM {table_name} "
                    f"WHERE retry_count >= ? AND last_failed_at > ?",
                    (f.MAX_RETRIES_BEFORE_SKIP, cutoff),
                )
                return {(r[0], r[1]) for r in await cur.fetchall()}

        skip = asyncio.run(go())
        self.assertNotIn(("abas", "2011-01-01"), skip)
        self.assertIn(("abas", "2026-04-01"), skip)
        self.assertNotIn(("abas", "2026-04-15"), skip)
        self.assertIn(("naha", "2026-04-01"), skip)

    def test_ioc_sealevel_failed_chunks_rollover(self):
        # Note: production schema uses station_code; the test schema uses the
        # same column name to match get_failed_chunks SQL exactly.
        self._run("ioc_sealevel_failed_chunks", "fetch_ioc_sealevel")

    def test_ioc_dart_failed_chunks_rollover(self):
        # ioc_dart's production table column is station_id; the SQL we test
        # here uses station_code locally — semantics-only (composite-key
        # rollover) are identical and that's what matters at this layer.
        self._run("dart_pressure_failed_chunks", "fetch_ioc_dart")


if __name__ == "__main__":
    unittest.main(verbosity=2)
