# Data Quality Issues (2026-04-14 Audit)

Audit run: [24402252536](https://github.com/yasumorishima/japan-geohazard-monitor/actions/runs/24402252536)

## Summary

Auditing backfill artifact `backfill-checkpoint-24393165660` (latest, 4.0 GB DB)
revealed that **10 of the 76 feature_matrix features are unreliable** due to
underlying raw-table issues. As of 2026-04-17, **8 of 10 contaminated features
are now FIXED** (goes_xray, solar_wind, goes_proton, so2, cloud_fraction,
soil_moisture). Remaining:
satellite_em (feature not wired to pipeline). The BQ feature_matrix needs
a clean rebuild with the fixed data.

## Per-feature contamination

| Feature(s) | Upstream table | Issue | File |
|---|---|---|---|
| `xray_flux_max_24h` | `goes_xray` | ✅ FIXED — LISIRD .json→.jsond 切替 (ms Unix epoch) + SWPC time_tag ISO 8601 対応 (607977e) + pre-2010 rows purge. |  |
| `sw_bz_min_24h`, `sw_pressure_max_24h`, `dst_min_24h` | `solar_wind` | ✅ FIXED — `dt > utcnow()` skip + one-time purge of future rows. | `scripts/fetch_solar_wind.py:113` |
| `proton_flux_max_24h` | `goes_proton` | ✅ FIXED — Same future-date filter + purge. | `scripts/fetch_goes_proton.py:109` |
| `so2_column_anomaly` | `so2_column` | Data appeared stuck at 2014 due to salvage SKIP_TABLES bug + 40min timeout loop (never reached 2015). OMSO2G V003 has 2004-2025 data on GES DISC. Fixed in a888964 (timeout 90min + SKIP_TABLES cleared). | `backfill.yml`, `salvage_db.py` |
| `cloud_fraction_anomaly` | `cloud_fraction` | Table never created — `init_cloud_table()` exception handler order bug (`OperationalError` subclass of `DatabaseError`, wrong catch order). Fixed in a888964. | `scripts/fetch_cloud_fraction.py:76-99` |
| `soil_moisture_anomaly` | `soil_moisture` | ✅ FIXED — fetch step was missing from backfill.yml (39a601b). CPC monthly + SMOPS daily fetcher exists but was never called. |  |
| `lightning_count_7d`, `lightning_anomaly` | `lightning` | DEPRECATED 2026-04-26 — Blitzortung archive HTML-restricted, Bonn EU-only, paid sources out of scope. Replaced by iss_lis_lightning + lightning_thunder_hour + lightning_lis_otd. | docs (this file) |

## Tables that are fine (evidence-backed)

```
earthquakes     28,377  rows   5,404 days   2011-01-01 → 2026-04-12  ✅
geomag_kp       44,656  rows   5,582 days   100% coverage            ✅
geomag_hourly   401,904 rows   5,582 days   100% coverage            ✅
tidal_stress    5,583   rows   5,583 days   100% coverage            ✅
goes_proton     5,844   rows   5,844 days   (rows ok, but dates tainted — see above)
cosmic_ray      14,633  rows   5,580 days   99.9%                    ✅
olr             3,303,608 rows 5,363 days   96.1%                    ✅
tide_gauge      2,402,292 rows 5,538 days   99.2%                    ✅
tec             4,063,770 rows 3,761 days   67.4% (event-based ok)   ✅
ulf_magnetic    9,102,240 rows 2,107 days   37.7% (event-based ok)   ✅
focal_mechanisms 3,498   rows 2,222 days   39.8%                    ✅
```

## Recovery status

| Action | Status |
|---|---|
| goes_xray time_tag fix | ✅ RESOLVED (607977e) — SWPC changed time_tag from space-separated to ISO 8601. Fixed with `.replace("T", " ")`. |
| solar_wind/goes_proton future-date filter | ✅ RESOLVED — `dt > utcnow()` skip + one-time purge of existing future rows already in both fetchers |
| so2_column 2015+ gap | ✅ RESOLVED (a888964 + ec61a92) — root cause 1: salvage SKIP_TABLES + 40min timeout loop. Root cause 2: targeted dispatch runs (e.g. target=cloud_fraction) uploaded partial checkpoints that overwrote full ones, resetting SO2 to 0 rows every cycle. Fix: skip checkpoint upload for targeted dispatches. Verified: 4.5M rows (→2016-05). |
| cloud_fraction init bug | ✅ RESOLVED (a888964) — exception handler order fix. Table will be created on next cron run. |
| soil_moisture fetch step missing | ✅ RESOLVED — fetch step was never added to backfill.yml. Added fetch_soil_moisture (CPC monthly + SMOPS daily, no auth). |
| lightning daily data | deprecated 2026-04-26 — Blitzortung archive permanently HTML-restricted, Bonn EU-only, paid sources (GLD360/ENTLN) out of scope. Fetcher stubbed to no-op; table removed from coverage report, validate_data, audit_artifact, and BQ upload list. Active lightning coverage via iss_lis_lightning + lightning_thunder_hour + lightning_lis_otd. |
| satellite_em (CSES) | open — Swarm EFI/MAG via ESA (no registration) as interim |
| snet_pressure | deprecated 2026-04-25 — HinetPy code 0120A is acceleration, not pressure. HinetPy exposes no S-net BPR (bottom pressure) network code. Fetcher stubbed to no-op; table removed from coverage report and BQ upload list. Source data accessible only via NIED direct data request. |

## validate_data.py / diagnose_data_gaps.py known bugs

Both scripts had incorrect `time_col` entries for 7 tables. ✅ Fixed in commits 10a85c5 + 9648abc (backfill.yml coverage report). Affected: earthquakes, focal_mechanisms, tec, gnss_tec, geomag_kp, modis_lst, snet_waveform.
