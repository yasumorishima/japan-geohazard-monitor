"""Smoke tests for BQ regression guard in load_raw_to_bq.py.

Tests _query_bq_count() and _should_skip_regression() in isolation using
mock bigquery client. RPi5 does not have google-cloud-bigquery installed,
so we stub the google.* modules in sys.modules before importing.

Verifies:
- 5/5 incident scenarios block upload (regression detection works)
- Normal growth and table creation pass through
- Threshold boundary (50%) behavior
- BQ_FORCE_OVERWRITE=true bypass (case-insensitive)
- Transient BQ errors fail-open (allow upload)
"""
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


# Stub google.* modules before importing load_raw_to_bq.
# When google-cloud-bigquery is actually installed (e.g. CI env with the
# library), inherit from the real NotFound so _query_bq_count's
#  branch is exercised correctly.
# On RPi5 (no google library), fall back to bare Exception subclass.
try:
    from google.api_core.exceptions import NotFound as _RealNotFound  # type: ignore[import]
    class _StubNotFound(_RealNotFound):
        pass
except ImportError:
    class _StubNotFound(Exception):  # type: ignore[no-redef]
        pass


_google = types.ModuleType("google")
_api_core = types.ModuleType("google.api_core")
_exceptions = types.ModuleType("google.api_core.exceptions")
_exceptions.NotFound = _StubNotFound
_cloud = types.ModuleType("google.cloud")
_bigquery = types.ModuleType("google.cloud.bigquery")

sys.modules.setdefault("google", _google)
sys.modules.setdefault("google.api_core", _api_core)
sys.modules.setdefault("google.api_core.exceptions", _exceptions)
sys.modules.setdefault("google.cloud", _cloud)
sys.modules.setdefault("google.cloud.bigquery", _bigquery)

# Now safe to import the module under test
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import load_raw_to_bq  # noqa: E402


def _mock_client_returning(rows):
    """Create a mock client whose query().result() yields rows."""
    client = MagicMock()
    result_mock = MagicMock()
    result_mock.__iter__.return_value = iter(rows)
    client.query.return_value.result.return_value = result_mock
    return client


def _mock_client_raising(exc):
    client = MagicMock()
    client.query.side_effect = exc
    return client


class TestQueryBqCount(unittest.TestCase):
    def test_returns_count_from_first_row(self):
        client = _mock_client_returning([(12345,)])
        self.assertEqual(load_raw_to_bq._query_bq_count(client, "p.d.t"), 12345)

    def test_not_found_returns_zero(self):
        client = _mock_client_raising(_StubNotFound("missing"))
        self.assertEqual(load_raw_to_bq._query_bq_count(client, "p.d.missing"), 0)

    def test_generic_exception_returns_zero_fail_open(self):
        client = _mock_client_raising(RuntimeError("transient boom"))
        self.assertEqual(load_raw_to_bq._query_bq_count(client, "p.d.t"), 0)

    def test_empty_result_returns_zero(self):
        client = _mock_client_returning([])
        self.assertEqual(load_raw_to_bq._query_bq_count(client, "p.d.t"), 0)


class TestShouldSkipRegression(unittest.TestCase):
    def setUp(self):
        os.environ.pop("BQ_FORCE_OVERWRITE", None)

    def tearDown(self):
        os.environ.pop("BQ_FORCE_OVERWRITE", None)

    def test_5_5_ulf_magnetic_32pct_blocks(self):
        # SQLite 7,776,000 vs BQ 24,179,040 = 32% < 50% threshold
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=24179040):
            self.assertTrue(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.ulf_magnetic", 7776000, "ulf_magnetic"))

    def test_5_5_iss_lis_14pct_blocks(self):
        # SQLite 1,046 vs BQ 7,311 = 14%
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=7311):
            self.assertTrue(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.iss_lis_lightning", 1046, "iss_lis_lightning"))

    def test_growth_ratio_110pct_passes(self):
        # snet healthy growth: 973,228 vs 886,626 = 110%
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=886626):
            self.assertFalse(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.snet_waveform", 973228, "snet_waveform"))

    def test_exactly_50pct_passes(self):
        # ratio == 0.5, NOT < 0.5 => allow
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=2000):
            self.assertFalse(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.t", 1000, "t"))

    def test_just_below_50pct_blocks(self):
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=2000):
            self.assertTrue(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.t", 999, "t"))

    def test_first_time_table_creation_passes(self):
        # bq_count == 0 (NotFound or empty) => allow
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=0):
            self.assertFalse(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.brand_new", 100, "brand_new"))

    def test_force_overwrite_true_bypasses(self):
        os.environ["BQ_FORCE_OVERWRITE"] = "true"
        # Even at 1% ratio, env var bypasses guard
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=10000):
            self.assertFalse(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.t", 100, "t"))

    def test_force_overwrite_TRUE_uppercase_bypasses(self):
        os.environ["BQ_FORCE_OVERWRITE"] = "TRUE"
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=10000):
            self.assertFalse(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.t", 100, "t"))

    def test_force_overwrite_false_string_does_not_bypass(self):
        os.environ["BQ_FORCE_OVERWRITE"] = "false"
        with patch.object(load_raw_to_bq, "_query_bq_count", return_value=10000):
            self.assertTrue(load_raw_to_bq._should_skip_regression(
                MagicMock(), "p.d.t", 100, "t"))


if __name__ == "__main__":
    unittest.main()
