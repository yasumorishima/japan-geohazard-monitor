"""Smoke test for Phase D3 graceful partial-save logic.

Pure-unit, no network, no NIED credentials. Mocks HinetPy Client interface
and time.monotonic so deadline behavior can be exercised deterministically.
"""
from __future__ import annotations
import sys
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))


class FakeMonotonic:
    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


class FakeClient:
    def __init__(self):
        self.calls: list[tuple] = []

    def get_continuous_waveform(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return None


class TestPhaseD3Deadline(unittest.TestCase):
    def setUp(self):
        self._sleep_patch = patch("time.sleep", lambda *a, **kw: None)
        self._sleep_patch.start()

    def tearDown(self):
        self._sleep_patch.stop()

    def test_fnet_constants(self):
        import fetch_fnet_waveform as f
        self.assertIsInstance(f.STEP_BUDGET_SEC, int)
        self.assertIsInstance(f.DEADLINE_MARGIN_SEC, int)
        self.assertGreater(f.STEP_BUDGET_SEC, f.DEADLINE_MARGIN_SEC)

    def test_snet_constants(self):
        import fetch_snet_waveform as s
        self.assertIsInstance(s.STEP_BUDGET_SEC, int)
        self.assertIsInstance(s.DEADLINE_MARGIN_SEC, int)
        self.assertGreater(s.STEP_BUDGET_SEC, s.DEADLINE_MARGIN_SEC)

    def test_fnet_fetch_day_breaks_when_deadline_past(self):
        import fetch_fnet_waveform as f
        fake = FakeMonotonic(1000.0)
        with patch.object(f.time, "monotonic", fake):
            client = FakeClient()
            target = datetime(2026, 4, 28)
            results = f._fetch_day(client, {}, target, n_segments=4, deadline=999.0)
        self.assertEqual(results, [])
        self.assertEqual(client.calls, [])

    def test_fnet_fetch_day_no_deadline_attempts_all_segments(self):
        import fetch_fnet_waveform as f
        client = FakeClient()
        target = datetime(2026, 4, 28)
        results = f._fetch_day(client, {}, target, n_segments=4)
        self.assertEqual(results, [])
        self.assertEqual(len(client.calls), 4)

    def test_fnet_fetch_day_breaks_midloop_when_deadline_reached(self):
        import fetch_fnet_waveform as f
        fake = FakeMonotonic(1000.0)
        def time_advancing_call():
            v = fake.now
            fake.now += 6.0
            return v
        with patch.object(f.time, "monotonic", time_advancing_call):
            client = FakeClient()
            target = datetime(2026, 4, 28)
            results = f._fetch_day(
                client, {}, target, n_segments=4,
                deadline=1010.0,
            )
        self.assertLess(len(client.calls), 4)

    def test_snet_fetch_day_breaks_when_deadline_past(self):
        import fetch_snet_waveform as s
        fake = FakeMonotonic(1000.0)
        with patch.object(s.time, "monotonic", fake):
            client = FakeClient()
            target = datetime(2026, 4, 28)
            results = s._fetch_day(
                client, {}, target, n_segments=4,
                network_code="0120A",
                sensor_config={"sensor_type": "accel", "vlf_analysis": False},
                deadline=999.0,
            )
        self.assertEqual(results, [])
        self.assertEqual(client.calls, [])

    def test_snet_fetch_day_no_deadline_attempts_all_segments(self):
        import fetch_snet_waveform as s
        client = FakeClient()
        target = datetime(2026, 4, 28)
        results = s._fetch_day(
            client, {}, target, n_segments=4,
            network_code="0120A",
            sensor_config={"sensor_type": "accel", "vlf_analysis": False},
        )
        self.assertEqual(results, [])
        self.assertEqual(len(client.calls), 4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
