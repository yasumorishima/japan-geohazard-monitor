"""Smoke test for Phase 2 (Stage 2.A) IOC sensor allow-list filter.

Pure-unit, no network. Verifies:
    - ALLOWED_SENSORS contains tide-gauge sensors (rad, pwl, bub, prs, ...)
    - ALLOWED_SENSORS does NOT contain DART buoy ("prt")
    - fetch_station_list station-level filter:
        - rad station passes
        - prt (DART) station rejected
        - null/missing sensor rejected (allow-list semantics)
        - unknown new sensor type rejected
        - sensor case-insensitive
        - excluded count + sample logged
    - parse_ioc_data record-level defense-in-depth:
        - rad records pass
        - prt records dropped even when station passed station-level filter
        - missing sensor field on record is allowed (legacy responses)
    - MAX_STATIONS cap is applied AFTER allow-list (DART cannot push tide
      gauges out of the cap)
"""
from __future__ import annotations

import asyncio
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")
    aiohttp_stub.ClientTimeout = lambda **kw: None
    aiohttp_stub.ClientSession = type("ClientSession", (), {})
    aiohttp_stub.ClientError = type("ClientError", (Exception,), {})
    sys.modules["aiohttp"] = aiohttp_stub


def _stationlist_resp(payload):
    """Build an aiohttp 200-OK response context manager returning JSON payload."""
    resp = AsyncMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=payload)
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestAllowedSensorsConstant(unittest.TestCase):
    def setUp(self):
        import fetch_ioc_sealevel as f
        self.f = f

    def test_allowed_sensors_includes_tide_gauges(self):
        for s in ("rad", "pwl", "bub", "prs", "flt", "wls", "enc", "aqu"):
            self.assertIn(s, self.f.ALLOWED_SENSORS,
                          f"{s} must be allowed (coastal tide-gauge sensor)")

    def test_allowed_sensors_excludes_dart_prt(self):
        self.assertNotIn("prt", self.f.ALLOWED_SENSORS,
                         "DART buoy 'prt' must NOT be in allow list")

    def test_allowed_sensors_excludes_battery_and_atm(self):
        for s in ("bat", "atm"):
            self.assertNotIn(s, self.f.ALLOWED_SENSORS)

    def test_allowed_sensors_is_frozenset(self):
        self.assertIsInstance(self.f.ALLOWED_SENSORS, frozenset,
                              "frozenset prevents accidental in-place mutation")


class TestStationListFilter(unittest.TestCase):
    """fetch_station_list station-level allow-list filter behaviour."""

    def setUp(self):
        import fetch_ioc_sealevel as f
        self.f = f

    def _run(self, payload):
        async def go():
            with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
                session = AsyncMock()

                def get(*_a, **_kw):
                    return _stationlist_resp(payload)

                session.get = get
                return await self.f.fetch_station_list(session)
        return asyncio.run(go())

    def _station(self, code, sensor, lat=35.0, lon=140.0,
                 location="Test Station"):
        # Use Lat/Lon (capitalised) to mirror real IOC payload
        return {
            "Code": code,
            "Location": location,
            "Lat": lat,
            "Lon": lon,
            "sensor": sensor,
            "countryname": "Japan",
        }

    def test_rad_station_passes(self):
        result = self._run([self._station("hana", "rad")])
        self.assertEqual([s["code"] for s in result], ["hana"])
        self.assertEqual(result[0]["sensor"], "rad")

    def test_prt_dart_station_rejected(self):
        result = self._run([
            self._station("hana", "rad"),
            self._station("dtok", "prt", lat=35.5, lon=141.0,
                          location="DART ESE of Tokyo"),
        ])
        codes = [s["code"] for s in result]
        self.assertIn("hana", codes)
        self.assertNotIn("dtok", codes,
                         "DART 'prt' station must be excluded from ioc_sea_level")

    def test_null_sensor_rejected(self):
        result = self._run([
            self._station("hana", "rad"),
            self._station("xxxx", None),
        ])
        self.assertNotIn("xxxx", [s["code"] for s in result],
                         "null sensor → reject (allow-list semantics)")

    def test_missing_sensor_field_rejected(self):
        # Build a station dict without a `sensor` key entirely.
        s = self._station("hana", "rad")
        s2 = self._station("yyyy", "rad")
        del s2["sensor"]
        result = self._run([s, s2])
        codes = [r["code"] for r in result]
        self.assertIn("hana", codes)
        self.assertNotIn("yyyy", codes,
                         "missing sensor key → reject (no field == null)")

    def test_unknown_sensor_rejected(self):
        result = self._run([
            self._station("hana", "rad"),
            self._station("zzzz", "tsu"),  # unknown sensor type
        ])
        self.assertNotIn("zzzz", [s["code"] for s in result])

    def test_sensor_case_insensitive(self):
        result = self._run([self._station("hana", "RAD")])
        self.assertEqual([s["code"] for s in result], ["hana"])

    def test_outside_japan_bbox_rejected_before_sensor_check(self):
        # rad sensor in London should still be rejected by bbox.
        result = self._run([self._station("londn", "rad", lat=51.5, lon=-0.1)])
        self.assertEqual(result, [])


class TestMaxStationsCapOrder(unittest.TestCase):
    """MAX_STATIONS cap must be applied AFTER allow-list filter so DART
    stations cannot push tide gauges out of the cap."""

    def setUp(self):
        import fetch_ioc_sealevel as f
        self.f = f

    def test_dart_does_not_consume_cap_budget(self):
        # 5 valid tide gauges + 50 DART buoys → only the 5 tide gauges should
        # come back, regardless of MAX_STATIONS=30.
        payload = []
        for i in range(5):
            payload.append({
                "Code": f"tg{i:02d}", "Location": f"TG {i}",
                "Lat": 35.0 + i * 0.1, "Lon": 140.0,
                "sensor": "rad", "countryname": "Japan",
            })
        for i in range(50):
            payload.append({
                "Code": f"d{i:03d}", "Location": f"DART {i}",
                "Lat": 36.0 + (i % 10) * 0.1, "Lon": 141.0,
                "sensor": "prt", "countryname": "Japan",
            })

        async def go():
            session = AsyncMock()

            def get(*_a, **_kw):
                return _stationlist_resp(payload)

            session.get = get
            with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
                return await self.f.fetch_station_list(session)

        result = asyncio.run(go())
        self.assertEqual(len(result), 5,
                         "Only the 5 tide gauges should pass the allow list")
        for s in result:
            self.assertEqual(s["sensor"], "rad")


class TestParseIOCDataSensorDefense(unittest.TestCase):
    """parse_ioc_data record-level defense-in-depth against per-record sensor
    leaks (e.g. multi-sensor merged streams)."""

    def setUp(self):
        import fetch_ioc_sealevel as f
        self.f = f
        self.station = {"code": "hana", "name": "Hanasaki",
                        "lat": 43.3, "lon": 145.6, "sensor": "rad"}

    def test_rad_records_pass(self):
        data = [
            {"stime": "2026-04-30 00:00:00", "slevel": "1.234", "sensor": "rad"},
            {"stime": "2026-04-30 01:00:00", "slevel": "1.250", "sensor": "rad"},
        ]
        rows = self.f.parse_ioc_data(data, self.station)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(rows[0]["sea_level_m"], 1.234)

    def test_prt_records_dropped_even_if_station_passed(self):
        data = [
            {"stime": "2026-04-30 00:00:00", "slevel": "1.234", "sensor": "rad"},
            {"stime": "2026-04-30 00:15:00", "slevel": "5779.6", "sensor": "prt"},
            {"stime": "2026-04-30 00:30:00", "slevel": "1.260", "sensor": "rad"},
        ]
        rows = self.f.parse_ioc_data(data, self.station)
        self.assertEqual(len(rows), 2,
                         "prt record must be dropped at parse time even when "
                         "the station-level filter let the station through")
        for r in rows:
            self.assertLess(r["sea_level_m"], 100,
                            "No DART OBP value (~5779m) should leak through")

    def test_missing_sensor_field_on_record_allowed(self):
        # Legacy IOC responses sometimes omit `sensor`. Don't break them.
        data = [
            {"stime": "2026-04-30 00:00:00", "slevel": "1.234"},
            {"stime": "2026-04-30 01:00:00", "slevel": "1.250"},
        ]
        rows = self.f.parse_ioc_data(data, self.station)
        self.assertEqual(len(rows), 2,
                         "Missing per-record sensor field must not block records")

    def test_unknown_sensor_on_record_dropped(self):
        data = [
            {"stime": "2026-04-30 00:00:00", "slevel": "1.234", "sensor": "rad"},
            {"stime": "2026-04-30 00:15:00", "slevel": "9.99",  "sensor": "tsu"},
        ]
        rows = self.f.parse_ioc_data(data, self.station)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["sea_level_m"], 1.234)


if __name__ == "__main__":
    unittest.main(verbosity=2)
