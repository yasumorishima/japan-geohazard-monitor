# Data Quality Issues (2026-04-14 Audit)

Audit run: [24402252536](https://github.com/yasumorishima/japan-geohazard-monitor/actions/runs/24402252536)

## Summary

Auditing backfill artifact `backfill-checkpoint-24393165660` (latest, 4.0 GB DB)
revealed that **10 of the 76 feature_matrix features are unreliable** due to
underlying raw-table issues. The BQ feature_matrix loaded on 2026-03-22
(`phase="15h"`, reported CV AUC 0.7417) was trained on this mix of clean and
contaminated data — the AUC number should NOT be treated as final model
performance until the upstream tables are corrected and the feature matrix is
rebuilt.

## Per-feature contamination

| Feature(s) | Upstream table | Issue | File |
|---|---|---|---|
| `xray_flux_max_24h` | `goes_xray` | Timestamps in 1980-1995 (31-year shift). LISIRD returns seconds since J2000 (2000-01-01 12:00 UT); fetcher parses as Unix seconds. | `scripts/fetch_goes_xray.py:351,370` |
| `sw_bz_min_24h`, `sw_pressure_max_24h`, `dst_min_24h` | `solar_wind` | NASA OMNI2 yearly files include placeholder data through 2026-12-31 (future). | `scripts/fetch_solar_wind.py:49` |
| `proton_flux_max_24h` | `goes_proton` | Same OMNI2 yearly-file padding. | `scripts/fetch_goes_proton.py:46` |
| `so2_column_anomaly` | `so2_column` | Data appeared stuck at 2014 due to salvage SKIP_TABLES bug + 40min timeout loop (never reached 2015). OMSO2G V003 has 2004-2025 data on GES DISC. Fixed in a888964 (timeout 90min + SKIP_TABLES cleared). | `backfill.yml`, `salvage_db.py` |
| `cloud_fraction_anomaly` | `cloud_fraction` | Table never created — `init_cloud_table()` exception handler order bug (`OperationalError` subclass of `DatabaseError`, wrong catch order). Fixed in a888964. | `scripts/fetch_cloud_fraction.py:76-99` |
| `soil_moisture_anomaly` | `soil_moisture` | Table never created (SMOPS ERDDAP IP blacklisted, SMOPS_END_YEAR=2022 hardcoded). | `scripts/fetch_smap_moisture.py:66` |
| `lightning_count_7d`, `lightning_anomaly` | `lightning` | 0 rows — Blitzortung archive returns HTML (access restricted) for every month. | run log 2026-04-14 L2454-2460 |

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
| so2_column 2015+ gap | ✅ RESOLVED (a888964) — root cause was salvage SKIP_TABLES + 40min timeout loop. OMSO2G V003 has data 2004-2025. Will accumulate in next cron runs. |
| cloud_fraction init bug | ✅ RESOLVED (a888964) — exception handler order fix. Table will be created on next cron run. |
| soil_moisture fetch step missing | ✅ RESOLVED — fetch step was never added to backfill.yml. Added fetch_soil_moisture (CPC monthly + SMOPS daily, no auth). |
| lightning daily data | ⬜ WONTFIX — no free daily source exists (GLD360/ENTLN=paid, Blitzortung=restricted, Bonn=EU only). Monthly coverage via WWLLN+LIS/OTD (Phase 20) is sufficient. Features auto-excluded. |
| satellite_em (CSES) | open — Swarm EFI/MAG via ESA (no registration) as interim |
| snet_pressure | open — HinetPy rejects every date; S-net station catalog needs verification |

## validate_data.py / diagnose_data_gaps.py known bugs

Both scripts had incorrect `time_col` entries for 7 tables. ✅ Fixed in commits 10a85c5 + 9648abc (backfill.yml coverage report). Affected: earthquakes, focal_mechanisms, tec, gnss_tec, geomag_kp, modis_lst, snet_waveform.
