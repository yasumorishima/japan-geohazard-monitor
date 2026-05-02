"""Smoke test for Phase 2 (Stage 2.A follow-up) cleanup_ioc_sealevel_dart.

Pure-unit, no network, no real sqlite/BigQuery. Verifies the BQ-only host
support added on top of the original sqlite+BQ cleanup script:

    - --skip-sqlite flag exists and is accepted by argparse
    - --skip-sqlite + --skip-bq together exit 2 (nothing to do)
    - _sqlite_inspect returns None on "no such table" OperationalError
      (graceful fallback for hosts where IOC fetcher only writes to BQ)
    - _sqlite_inspect re-raises any other OperationalError unchanged
    - _sqlite_delete returns None on "no such table" OperationalError
    - _amain with --skip-sqlite never calls the sqlite helpers and still
      issues the BigQuery DELETE
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))


def _import_module():
    import importlib

    if "cleanup_ioc_sealevel_dart" in sys.modules:
        return importlib.reload(sys.modules["cleanup_ioc_sealevel_dart"])
    return importlib.import_module("cleanup_ioc_sealevel_dart")


class TestArgparse(unittest.TestCase):
    def test_skip_sqlite_flag_exists(self):
        mod = _import_module()
        with patch.object(sys, "argv", ["cleanup", "--skip-sqlite"]):
            args = mod._parse_args()
        self.assertTrue(args.skip_sqlite)
        self.assertFalse(args.skip_bq)
        self.assertFalse(args.yes)

    def test_skip_bq_unchanged(self):
        mod = _import_module()
        with patch.object(sys, "argv", ["cleanup", "--skip-bq"]):
            args = mod._parse_args()
        self.assertTrue(args.skip_bq)
        self.assertFalse(args.skip_sqlite)


class TestSqliteGracefulFallback(unittest.IsolatedAsyncioTestCase):
    async def test_inspect_returns_none_on_no_such_table(self):
        mod = _import_module()

        class _FakeDB:
            async def execute_fetchall(self, *_a, **_kw):
                raise sqlite3.OperationalError("no such table: ioc_sea_level")

        @asynccontextmanager
        async def _fake_connect():
            yield _FakeDB()

        with patch.object(mod, "safe_connect", _fake_connect):
            result = await mod._sqlite_inspect(["dtok"], None, None)
        self.assertIsNone(result)

    async def test_inspect_reraises_other_operational_error(self):
        mod = _import_module()

        class _FakeDB:
            async def execute_fetchall(self, *_a, **_kw):
                raise sqlite3.OperationalError("database is locked")

        @asynccontextmanager
        async def _fake_connect():
            yield _FakeDB()

        with patch.object(mod, "safe_connect", _fake_connect):
            with self.assertRaises(sqlite3.OperationalError):
                await mod._sqlite_inspect(["dtok"], None, None)

    async def test_delete_returns_none_on_no_such_table(self):
        mod = _import_module()

        class _FakeDB:
            async def execute(self, *_a, **_kw):
                raise sqlite3.OperationalError(
                    "no such table: ioc_sealevel_failed_dates"
                )

            async def commit(self):
                pass

        @asynccontextmanager
        async def _fake_connect():
            yield _FakeDB()

        with patch.object(mod, "safe_connect", _fake_connect):
            result = await mod._sqlite_delete(["dtok"], None, None)
        self.assertIsNone(result)


class TestAmainSkipFlags(unittest.IsolatedAsyncioTestCase):
    async def test_skip_sqlite_and_skip_bq_together_exit_2(self):
        mod = _import_module()
        argv = ["cleanup", "--skip-sqlite", "--skip-bq", "--yes"]
        with patch.object(sys, "argv", argv):
            rc = await mod._amain()
        self.assertEqual(rc, 2)

    async def test_skip_sqlite_bypasses_sqlite_helpers_and_runs_bq(self):
        mod = _import_module()
        argv = ["cleanup", "--skip-sqlite", "--yes",
                "--station-codes", "dtok,dtok2"]
        sqlite_inspect = AsyncMock()
        sqlite_delete = AsyncMock()
        bq_delete = MagicMock(return_value=0)
        with patch.object(sys, "argv", argv), \
             patch.object(mod, "_sqlite_inspect", sqlite_inspect), \
             patch.object(mod, "_sqlite_delete", sqlite_delete), \
             patch.object(mod, "_bq_delete", bq_delete):
            rc = await mod._amain()
        self.assertEqual(rc, 0)
        sqlite_inspect.assert_not_called()
        sqlite_delete.assert_not_called()
        bq_delete.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
