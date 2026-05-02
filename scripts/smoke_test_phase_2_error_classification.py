"""Smoke test for Phase 2 (1) Stage 1 transient vs definitive error classification.

Pure-unit, no network. Mocks aiohttp ClientSession to simulate scenarios:
    - HTTP 200 + valid payload     → records list (definitive success)
    - HTTP 200 + empty body        → []                (definitive no-data)
    - HTTP 200 + HTML error page   → TRANSIENT_FAILURE (retry next cron)
    - HTTP 200 + JSON decode error → TRANSIENT_FAILURE (retry next cron)
    - HTTP 200 + non-list payload  → TRANSIENT_FAILURE (retry next cron)
    - HTTP 404                     → []                (definitive 404)
    - HTTP 5xx (3 retries)         → TRANSIENT_FAILURE
    - HTTP 429 (3 retries)         → TRANSIENT_FAILURE
    - asyncio.TimeoutError x3      → TRANSIENT_FAILURE
    - ConnectionError x3           → TRANSIENT_FAILURE
    - 5xx -> 200 (recovery)        → records list

Verifies the 3-way return contract that lets the main loop:
    - skip mark_failed_* on transient (so retry_count is preserved)
    - mark on definitive empty so the 30-day blacklist works as designed
"""
from __future__ import annotations

import asyncio
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

# Stub aiohttp so module imports on a host without the library. We mock the
# real network entrypoints (session.get) per-test below; the stubbed
# ClientError is used as the base for ConnectionError simulation.
if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")
    aiohttp_stub.ClientTimeout = lambda **kw: None
    aiohttp_stub.ClientSession = type("ClientSession", (), {})
    aiohttp_stub.ClientError = type("ClientError", (Exception,), {})
    sys.modules["aiohttp"] = aiohttp_stub


def _make_resp(status: int, text_body: str | None = None,
               bytes_body: bytes | None = None):
    """Build an async-context-manager-shaped response mock for session.get(...)."""
    resp = AsyncMock()
    resp.status = status
    if text_body is not None:
        resp.text = AsyncMock(return_value=text_body)
    if bytes_body is not None:
        resp.read = AsyncMock(return_value=bytes_body)
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


def _session_with_responses(side_effects):
    """Build a fake aiohttp.ClientSession whose .get() returns the queued cms.

    Each side_effects element is either:
        - a context-manager mock (callable that returns it on .get())
        - an Exception instance to raise
    """
    session = AsyncMock()
    queue = list(side_effects)

    def get(*_a, **_kw):
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    session.get = get
    return session


class TestIOCErrorClassification(unittest.TestCase):
    """fetch_ioc_sealevel.fetch_station_data return contract."""

    def setUp(self):
        import fetch_ioc_sealevel as f
        self.f = f
        self.station = {"code": "ofun", "name": "Ofunato",
                        "lat": 39.0, "lon": 141.7}

    def _run_fetch(self, side_effects):
        async def go():
            with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
                session = _session_with_responses(side_effects)
                return await self.f.fetch_station_data(
                    session, self.station,
                    "2011-01-01 00:00:00", "2011-01-01 23:59:59",
                )
        return asyncio.run(go())

    def test_200_valid_json_returns_records(self):
        body = '[{"stime": "2011-01-01 00:00:00", "slevel": "1.234"}]'
        result = self._run_fetch([_make_resp(200, text_body=body)])
        self.assertIsInstance(result, list, "200 OK + JSON → list")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["sea_level_m"], 1.234)

    def test_200_empty_body_returns_empty_list(self):
        result = self._run_fetch([_make_resp(200, text_body="")])
        self.assertEqual(result, [],
                         "200 OK + empty body must be definitive (mark failed)")
        self.assertIsNot(result, self.f.TRANSIENT_FAILURE)

    def test_200_html_body_returns_transient(self):
        html = "<html><body>Service Temporarily Unavailable</body></html>"
        result = self._run_fetch([_make_resp(200, text_body=html)] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE,
                      "200 OK + HTML must be transient (server overload pattern)")

    def test_200_invalid_json_returns_transient(self):
        result = self._run_fetch([_make_resp(200, text_body="not json {")] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE,
                      "200 OK + JSON decode error must be transient")

    def test_200_non_list_payload_returns_transient(self):
        result = self._run_fetch([_make_resp(200, text_body='{"err": "x"}')] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE,
                      "200 OK + non-list payload must be transient")

    def test_404_returns_empty_list(self):
        result = self._run_fetch([_make_resp(404)])
        self.assertEqual(result, [], "404 must be definitive empty")
        self.assertIsNot(result, self.f.TRANSIENT_FAILURE)

    def test_503_three_times_returns_transient(self):
        result = self._run_fetch([_make_resp(503)] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE,
                      "Persistent 5xx must be transient (don't burn retry_count)")

    def test_429_three_times_returns_transient(self):
        result = self._run_fetch([_make_resp(429)] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE,
                      "Persistent 429 must be transient")

    def test_timeout_three_times_returns_transient(self):
        result = self._run_fetch([asyncio.TimeoutError()] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE)

    def test_connection_error_three_times_returns_transient(self):
        import aiohttp
        result = self._run_fetch([aiohttp.ClientError("conn refused")] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE)

    def test_503_then_200_recovers(self):
        body = '[{"stime": "2011-01-01 00:00:00", "slevel": "0.5"}]'
        result = self._run_fetch([
            _make_resp(503),
            _make_resp(200, text_body=body),
        ])
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1, "5xx then 200 must recover within retries")


class TestGNSSErrorClassification(unittest.TestCase):
    """fetch_gnss_tec.try_fetch return contract."""

    def setUp(self):
        import fetch_gnss_tec as f
        self.f = f

    def _run_fetch(self, side_effects):
        async def go():
            with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
                session = _session_with_responses(side_effects)
                return await self.f.try_fetch(session, "https://example/x.nc")
        return asyncio.run(go())

    def test_200_returns_bytes(self):
        result = self._run_fetch([_make_resp(200, bytes_body=b"CDF\x01" + b"x" * 200)])
        self.assertIsInstance(result, bytes)
        self.assertGreater(len(result), 100)

    def test_404_returns_none(self):
        result = self._run_fetch([_make_resp(404)])
        self.assertIsNone(result, "404 = definitive no-data (None)")
        self.assertIsNot(result, self.f.TRANSIENT_FAILURE)

    def test_503_three_times_returns_transient(self):
        result = self._run_fetch([_make_resp(503)] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE)

    def test_timeout_three_times_returns_transient(self):
        result = self._run_fetch([asyncio.TimeoutError()] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE)

    def test_connection_error_three_times_returns_transient(self):
        import aiohttp
        result = self._run_fetch([aiohttp.ClientError("conn refused")] * 3)
        self.assertIs(result, self.f.TRANSIENT_FAILURE)

    def test_503_then_200_recovers(self):
        result = self._run_fetch([
            _make_resp(503),
            _make_resp(200, bytes_body=b"CDF\x01" + b"x" * 200),
        ])
        self.assertIsInstance(result, bytes)


class TestGNSSFetchDateAggregation(unittest.TestCase):
    """fetch_gnss_tec.fetch_date hour-grain status aggregation contract."""

    def setUp(self):
        import fetch_gnss_tec as f
        self.f = f

    def _patched_run(self, try_fetch_returns,
                     parse_returns=None):
        """Run fetch_date with try_fetch and parse_netcdf_simple monkey-patched."""
        from datetime import datetime as _dt
        try_fetch_calls = iter(try_fetch_returns)
        parse_calls = iter(parse_returns or [])

        async def fake_try_fetch(_session, _url):
            return next(try_fetch_calls)

        def fake_parse(_data, _epoch):
            try:
                return next(parse_calls)
            except StopIteration:
                return []

        async def go():
            with patch.object(self.f, "try_fetch", new=fake_try_fetch), \
                 patch.object(self.f, "parse_netcdf_simple", new=fake_parse):
                # FETCH_HOURS = [3, 12] → 2 hours × 2 URLs = up to 4 try_fetch calls
                return await self.f.fetch_date(
                    AsyncMock(),  # session, unused since try_fetch is faked
                    _dt(2011, 1, 1),
                )
        return asyncio.run(go())

    def test_all_hours_404_returns_none(self):
        # 4 try_fetch calls (2 hours × {VTEC, dTEC}), all 404.
        result = self._patched_run([None, None, None, None])
        self.assertIsNone(result,
                          "All-404 must be definitive (mark failed)")

    def test_all_hours_transient_returns_transient(self):
        T = self.f.TRANSIENT_FAILURE
        result = self._patched_run([T, T, T, T])
        self.assertIs(result, T,
                      "All-transient must surface as TRANSIENT_FAILURE")

    def test_partial_success_returns_records(self):
        big = b"CDF\x01" + b"x" * 200
        # Hour 3: VTEC returns bytes (records). Hour 12: VTEC 404, dTEC 404.
        result = self._patched_run(
            try_fetch_returns=[big, None, None],
            parse_returns=[[(35.0, 140.0, 12.5, None, "2011-01-01 03:00:00")]],
        )
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1,
                         "Partial success returns merged records list")

    def test_transient_then_404_no_records_returns_transient(self):
        T = self.f.TRANSIENT_FAILURE
        # Hour 3: VTEC TRANSIENT, dTEC TRANSIENT. Hour 12: VTEC 404, dTEC 404.
        result = self._patched_run([T, T, None, None])
        self.assertIs(result, T,
                      "Mix of TRANSIENT + 404 with no records must be TRANSIENT")

    def test_one_hour_data_one_hour_transient_returns_records(self):
        big = b"CDF\x01" + b"x" * 200
        # Hour 3: VTEC returns bytes (records). Hour 12: TRANSIENT, TRANSIENT.
        result = self._patched_run(
            try_fetch_returns=[big, self.f.TRANSIENT_FAILURE,
                               self.f.TRANSIENT_FAILURE],
            parse_returns=[[(35.0, 140.0, 12.5, None, "2011-01-01 03:00:00")]],
        )
        self.assertIsInstance(result, list,
                              "Any hour with records → list (not transient)")
        self.assertEqual(len(result), 1)


class TestSentinelDistinctness(unittest.TestCase):
    """TRANSIENT_FAILURE must be distinguishable from None / [] / 0 / False."""

    def test_ioc_sentinel_is_not_falsy_or_list_or_none(self):
        import fetch_ioc_sealevel as f
        self.assertIsNot(f.TRANSIENT_FAILURE, None)
        self.assertIsNot(f.TRANSIENT_FAILURE, [])
        self.assertNotEqual(f.TRANSIENT_FAILURE, [])
        self.assertNotEqual(f.TRANSIENT_FAILURE, None)

    def test_gnss_sentinel_is_not_falsy_or_list_or_none(self):
        import fetch_gnss_tec as f
        self.assertIsNot(f.TRANSIENT_FAILURE, None)
        self.assertIsNot(f.TRANSIENT_FAILURE, [])
        self.assertNotEqual(f.TRANSIENT_FAILURE, [])
        self.assertNotEqual(f.TRANSIENT_FAILURE, None)

    def test_sentinels_are_independent_per_module(self):
        """Each module owns its own sentinel — `is` must not cross-match."""
        import fetch_ioc_sealevel as f_ioc
        import fetch_gnss_tec as f_gnss
        # Independent sentinels by design — keeps each fetcher's main loop
        # from accidentally accepting a foreign module's sentinel.
        self.assertIsNot(f_ioc.TRANSIENT_FAILURE, f_gnss.TRANSIENT_FAILURE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
