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
| `so2_column_anomaly` | `so2_column` | Data stops 2014-12-27 (OMI row anomaly end-of-life, 11 years of ingestion silently absent). | source-driven, not code |
| `cloud_fraction_anomaly` | `cloud_fraction` | Table CORRUPT — `PRAGMA integrity_check` fails, all writes since run #3 blocked. | page-level SQLite corruption |
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
| goes_xray epoch fix | pending PR — switch to `.jsond` endpoint (ms Unix) + date sanity filter + drop existing 1980-1995 rows |
| solar_wind/goes_proton future-date filter | pending PR — reject rows where `observed_at > utcnow()` |
| so2_column 2015+ gap | investigation — verify OMI actually continued past 2014; if so, find why fetcher stops |
| cloud_fraction drop+refetch | pending PR — DROP TABLE on next backfill startup, re-fetch from MODIS |
| soil_moisture alternative source | open — SMAP L3 via NASA Earthdata (CMR) instead of ERDDAP |
| lightning alternative source | open — WWLLN (requires subscription) or Bonn sferics archive |
| satellite_em (CSES) | open — Swarm EFI/MAG via ESA (no registration) as interim |
| snet_pressure | open — HinetPy rejects every date; S-net station catalog needs verification |

## validate_data.py / diagnose_data_gaps.py known bugs

Both scripts have incorrect `time_col` entries for 7 tables. `earthquakes` coverage% has been computed against a non-existent column `time` (actual: `occurred_at`) for weeks. Affected: earthquakes, focal_mechanisms, tec, gnss_tec, geomag_kp, modis_lst, snet_waveform. Separate PR planned.
