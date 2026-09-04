# Japan Geohazard Monitor

> ⚠️ **Disclaimer: this is a research log, not a forecast.**
>
> This repository is a personal research and engineering log. Nothing in it is an earthquake prediction, forecast, warning, advisory, or operational product of any kind, and it issues no alerts. The figures recorded here are retrospective research metrics computed on public catalogues; they are exploratory, may contain errors, and have not been validated for operational use. Do not rely on anything in this repository for safety, evacuation, business, or any other disaster-response decision. For official earthquake and volcano information in Japan, use the Japan Meteorological Agency (https://www.jma.go.jp/en/) and your local government. Everything here is provided as-is, without warranty of any kind, and the author accepts no liability for any loss or damage arising from its use. Data remains the property of the providers listed under Data Attribution.
>
> ⚠️ **免責事項: 本リポジトリは研究記録であり、予報ではありません**
>
> 本リポジトリは個人の研究および開発の記録です。地震の予知・予測・予報・警報その他の運用情報ではなく、いかなる警報も発信しません。記載の数値は公開カタログを用いた事後的な研究指標であり、探索段階のもので、誤りを含む可能性があり、実運用に向けた検証は行っていません。安全確保・避難・事業判断その他の防災上の判断に、本リポジトリの内容を用いないでください。日本の地震・火山の公式情報は気象庁（https://www.jma.go.jp/）および各自治体の発表をご確認ください。本リポジトリの内容は現状のまま無保証で提供され、利用により生じたいかなる損害についても作者は責任を負いません。データの権利は Data Attribution 節に記載の各提供元に帰属します。

![Live Map](docs/screenshot.png)

Real-time monitoring dashboard for Japan's geophysical activity — earthquakes, volcanoes, atmospheric conditions, geomagnetism, ocean temperature, ionosphere, and crustal deformation — all overlaid on a single dark-themed interactive map with a correlation analysis panel.

9 async collectors run continuously on a Raspberry Pi 5, pulling data from 10 public APIs and storing it in SQLite. A FastAPI server renders a Leaflet.js dashboard with togglable layers and a time-synchronized correlation panel for cross-domain anomaly detection. Mobile responsive.

## Live

Raspberry Pi 5 + Docker（Tailscaleネットワーク内）

> **Data:** Published as a public Hugging Face Dataset → [yasumorishima/japan-geohazard](https://huggingface.co/datasets/yasumorishima/japan-geohazard).

## Architecture

```
9 async collectors (independent intervals per source)
    → BaseCollector (retry, batch insert, health tracking)
    → SQLite (WAL mode, auto-purge @ 90 days)
    → FastAPI REST API (per-layer + correlation endpoints)
    → Leaflet.js dark-themed map (togglable layers, mobile responsive)
    → Chart.js correlation panel (5 time-aligned charts)
```

**Stack**: Python 3.12 / asyncio + aiohttp + asyncssh / aiosqlite / FastAPI + Uvicorn / scikit-learn + scipy / Leaflet.js + Chart.js / Docker

## Data Sources (10 APIs, 9 collectors)

| Collector | Source | Data | Interval | Records |
|---|---|---|---|---|
| `usgs` | USGS GeoJSON | Earthquakes (global → Japan filter) | 5 min | — |
| `p2p` | P2P地震情報 API | Earthquakes (JMA intensity) | 2 min | — |
| `jma` | 気象庁 Bosai | Earthquakes (COD format) | 3 min | — |
| `amedas` | 気象庁 AMeDAS | Temp / Pressure / Wind / Precip (1,286 stations) | 10 min | ~1,286/fetch |
| `geomag` | NOAA SWPC | GOES magnetometer + Kp index | 15 min | ~1,400/fetch |
| `volcano` | 気象庁 Bosai | 117 active volcanoes + alert levels (1-5) | 15 min | 117/fetch |
| `sst` | NOAA ERDDAP | Sea surface temperature (MUR 0.5° grid) | 6 hours | ~1,725/fetch |
| `tec` | CODE (Bern) IONEX | Ionosphere Total Electron Content (2.5° × 5° grid) | 2 hours | ~1,350/fetch |
| `geonet` | GSI SFTP (terras) | Crustal deformation F5 daily (218 sampled stations) | 24 hours | ~1,500/fetch |

## Map Layers

| Layer | Toggle | Visualization | Color Scheme |
|---|---|---|---|
| Earthquakes | ✅ default on | CircleMarker (mag ∝ radius) | Depth: red (shallow) → blue (deep) |
| Volcanoes | toggle | Triangle markers (SVG) | Alert level: gray=1, yellow=2, orange=3, red=4, purple=5 |
| Sea Surface Temp | toggle | Rectangle grid overlay (0.5°) | Blue (cold) → green → yellow → red (warm) |
| Ionosphere TEC | toggle | Rectangle grid overlay (2.5° × 5°) | Green (low) → yellow → red → purple (high TECU) |
| GEONET | toggle | CircleMarker (displacement ∝ radius) | Green < 5mm, yellow < 15mm, orange < 30mm, red ≥ 30mm |
| AMeDAS | toggle | CircleMarker per station | Metric-dependent colormap (4 selectable metrics) |
| Kp Index | always | Header badge | Green < 4, Orange 4-6, Red > 6 |

## Correlation Panel

Right-side collapsible panel (bottom sheet on mobile) with 5 time-synchronized Chart.js charts for cross-domain anomaly detection:

| Chart | Data | Resolution |
|---|---|---|
| Earthquake count | Hourly bar chart | 1 hour |
| Kp index | Line chart | 3 hours |
| GOES magnetic field | Hourly mean total field (nT) | 1 hour |
| Ionosphere TEC | Mean TEC over Japan (TECU) | Per IONEX epoch |
| Atmospheric pressure | Mean AMeDAS pressure (hPa) | 1 hour |

Supports 3/7/14/30-day windows. Auto-refreshes every 5 minutes when open.

**Use case**: Visual detection of precursor patterns — e.g., ionosphere TEC anomaly → geomagnetic disturbance → pressure change → earthquake sequence.

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Interactive map dashboard |
| `GET /api/earthquakes?hours=N` | Earthquake list (default 24h) |
| `GET /api/volcanoes` | All volcanoes with current alert levels |
| `GET /api/sst` | Latest SST grid |
| `GET /api/tec?hours=N` | Latest ionosphere TEC grid (default 24h) |
| `GET /api/geonet` | Latest GEONET displacement per station |
| `GET /api/amedas?metric=temperature` | Latest AMeDAS snapshot (pressure/temperature/wind/precipitation) |
| `GET /api/geomag/goes?hours=24` | GOES magnetometer time series |
| `GET /api/geomag/kp?days=7` | Kp index time series |
| `GET /api/correlation?days=7` | Time-aligned multi-domain data for correlation panel |
| `GET /api/stats` | Collector health, counts, latest Kp, volcano alerts |

## Database

SQLite with WAL mode. 10 tables:

- `earthquakes` — dedup by (source, event_id)
- `amedas` — dedup by (station_id, observed_at)
- `geomag_goes` — dedup by (time_tag, satellite)
- `geomag_kp` — dedup by time_tag
- `volcanoes` — upsert by volcano_code (one row per volcano)
- `sst` — dedup by (lat, lon, observed_at)
- `tec` — dedup by (lat, lon, epoch)
- `geonet` — dedup by (station_id, observed_at)
- `focal_mechanisms` — GCMT strike/dip/rake, dedup by (source, event_id)
- `gnss_tec` — high-res 0.25° TEC from Nagoya Univ., dedup by (lat, lon, epoch, source)
- `modis_lst` — MODIS Land Surface Temperature (Kelvin), dedup by (lat, lon, observed_date)
- `ulf_magnetic` — 1-minute geomagnetic H/D/Z/F (nT) from KAK/MMB/KNY, dedup by (station, observed_at)

Auto-purge: records older than 90 days deleted on each collector cycle (real-time tables only; analysis tables retained).

## Deployment

Runs on Raspberry Pi 5 via Docker. GEONET SFTP credentials stored in `.env`:

```bash
# .env (on RPi5, not committed)
GSI_SFTP_PASSWORD=xxxxx

# Deploy
ssh yasu@<RPi5-tailscale-ip> "cd ~/japan-geohazard-monitor && sudo git pull && sudo docker-compose up -d --build"
```

### Self-hosted runner: uploading RPi5-local artifacts to HF (2026-07-25)

The backfill/merge pipeline stays fully cloud-native (PR #166) — this does **not** revive the retired SSD-era topology. What was added is a narrow path for the opposite direction: publishing a derived artifact that only exists on the RPi5.

- A runner named `rpi5-geohazard` is registered again on this repo (labels `self-hosted, Linux, ARM64, rpi5`), installed as a **systemd service** (`svc.sh install`) so it survives reboots.
- `.github/workflows/hf-upload-local.yml` (`workflow_dispatch`, required `memo`) runs `runs-on: [self-hosted, rpi5]` and calls `scripts/hf_upload_local.py`. Because the job runs on the box, it can read a local path directly while **`HF_TOKEN` stays in GitHub Secrets and is never written to the RPi5's disk**.
- `scripts/hf_upload_local.py` refuses raw/archive extensions (`.mseed .sac .cnt .win32 .tar.gz .tgz .tar .zip`) — NIED Hi-net / S-net waveform redistribution is prohibited, so only derived artifacts may pass. After upload it re-reads the remote file list and **fails unless the remote size matches the local size**, then writes a marker under `.hf_published/` (repo, path, size, sha256, timestamp) so the RPi5 janitor can reclaim the local copy later without needing network access or a token. `--dry-run` supported.
- First use: `derived/feature_matrix.json` (156,724,706 bytes — 11×11 grid × 1,816 timesteps × 85 catalogue/stress features) published to [`yasumorishima/japan-geohazard`](https://huggingface.co/datasets/yasumorishima/japan-geohazard). Verified both in-job and independently via the public HF API. The matrix contains only aggregate statistics (`rate_7d`, `b_value`, `etas_residual_*`, `cfs_cumulative_kpa`, `benioff_strain_30d`, …), so no individual waveform can be reconstructed from it.
- The runner is public-repo hosted, so these jobs consume **no GitHub Actions minutes**.

## Phased Development

- **Phase 1** ✅ Earthquakes (3 sources: USGS, P2P, JMA)
- **Phase 2** ✅ Atmospheric (AMeDAS 1,286 stations) + Geomagnetic (NOAA SWPC GOES + Kp)
- **Phase 3** ✅ Volcanoes (JMA 117 active) + Ocean (NOAA ERDDAP MUR SST)
- **Phase 4** ✅ Ionosphere TEC (CODE Bern predicted IONEX) + GEONET crustal deformation (GSI SFTP, 218 stations)
- **Correlation** ✅ Time-synchronized 5-chart panel (earthquake/Kp/GOES/TEC/pressure)
- **Analysis Phase 1** ✅ b-value, TEC, Kp, multi-indicator grid search → all negative (aftershock/sampling artifacts)
- **Analysis Phase 2** ✅ Coulomb stress (lift 37.5 isolated), rate anomaly (lift 1.86), clustering (lift 4.12) — all survived aftershock isolation + prospective test (combined lift 20.66)
- **Analysis Phase 3a** ✅ LURR (❌), Natural Time (❌), Nowcasting (⚠️ lift 1.31) — catalog-based methods exhausted
- **Analysis Phase 3b** ✅ MODIS LST (❌), ULF magnetic (⚠️ data limited to 80 days), GNSS-TEC 0.5° (5.3M records)
- **Analysis Phase 4** ✅ **Prospective (forward-looking) prediction**: ETAS residual (gain 4.0x), foreshock (5.1x), cumulative CFS (2.4x), combined alarm (**7.8x, 62.5% precision**). Pattern Informatics (Molchan AUC 0.349)
- **Analysis Phase 5** ✅ ML integration: AdaBoost ensemble (11 features, pure Python) — AUC 0.73
- **Analysis Phase 6** ✅ ML overhaul: HistGradientBoosting (35 temporal features), walk-forward CV (0.740 ± 0.016), ETAS MLE per zone, rate-and-state CFS, isotonic calibration — **AUC 0.746**
- **Analysis Phase 7** ✅ Spatial correlation + GNSS + zone ETAS: 47 features (+6 GNSS crustal deformation, +6 enhanced spatial), zone-specific ETAS in feature extraction, 2-pass Gaussian spatial smoothing — **AUC 0.749 (CV 0.741)**
- **Analysis Phase 8** ✅ Structural overhaul: multi-target (M5+/M5.5+/M6+), CSEP benchmark (4 reference models + N/L/T-test), ensemble stacking (8-input physics×ML meta-learner), ConvLSTM spatiotemporal neural network (Colab/Kaggle GPU). **Result (2026-06): ConvLSTM reaches walk-forward AUC 0.8013 on a Kaggle T4 — the first model to hit 0.80, though within fold-noise of the ~0.799 flat-tabular ceiling (see the 2026-06-07 spatiotemporal neural benchmark below)**
- **Analysis Phase 9.0** ✅ Non-traditional precursor data sources: cosmic ray neutron monitors (NMDB ✅), animal behavior GPS (Movebank ❌ no Japan data), lightning (Blitzortung ❌ archive restricted), hourly geomagnetic (INTERMAGNET ❌ API param bugs), satellite EM (CSES ❌ auth required) — CV AUC **0.728** (regression from 0.741 due to zero-filled features acting as noise)
- **Analysis Phase 9.1** ✅ 4-bug fix + metadata NameError fix: INTERMAGNET API params → **36,000 records, 1,500 days** geomag data successfully fetched. Dynamic feature selection → 53/56 active features. **CV AUC 0.7316, Test AUC 0.7452**. Blitzortung/Sferics Bonn: server down (ECONNREFUSED), lightning data unavailable
- **Analysis Phase 10/10b** ✅ 11 unconventional data sources: OLR, Earth rotation, solar wind, GRACE gravity, SO2, soil moisture, tide gauge, ocean color, cloud fraction, nightlight, InSAR — 56 → 70 features. **CV AUC 0.7249** (regression: 12/70 features active, Solar Wind only new source, Earthdata auth broken, OLR/IERS/tide URLs dead)
- **Analysis Phase 11** ✅ 4 space/cosmic data sources: GOES X-ray flux (solar flares), GOES proton flux (SEP events), tidal stress (lunar+solar, pure calculation), particle precipitation (Van Allen belt). 70 → 75 features
- **Analysis Phase 12** ✅ Data acquisition infrastructure overhaul + ML feature stability selection + FeatureExtractor performance optimization. OLR→PSL THREDDS, IERS→OBSPM, tide→UHSLC Fast Delivery, Earthdata→OAuth2 redirect handler. ML: 3-fold stability pre-filter removes noisy features before CV. **Data acquisition all confirmed working** (OLR/IERS/tide/GOES/GRACE/SO2 ✅). Phase 12b: bisect-based window queries, zone stats caching, deque histories — extract() 20h→12min. deque slice bug fixed in Phase 13
- **Analysis Phase 13** ✅ Seafloor/ocean bottom data sources: NOAA DART bottom pressure (5 stations near Japan, 3 returned data, no auth), IOC sea level monitoring (❌ API crash on None station codes), NIED S-net seafloor pressure (❌ NIED credentials pending). 75 → 79 features (64 active after stability selection). DATA_LICENSES.md added (all 19 source policies documented). **CV AUC 0.7416 (best ever), Test AUC 0.7481**
- **Analysis Phase 14** ✅ Four-axis improvement: (1) IOC fetch crash fix (None-safe parsing + dict/list response support), (2) INTERMAGNET backfill 4x acceleration (500→2000 days/station/run), (3) Diverse stacking level-0 models (RandomForest + LogisticRegression alongside HistGBT → 14-feature meta-learner), (4) ConvLSTM full-feature export (feature_matrix.json now includes all Phase 9+ data, not zero-filled). **CV AUC 0.7415, Test AUC 0.7485. Stacking logistic=0.7484 (≒base), isotonic=0.7213 (degraded). 65 active features**
- **Analysis Phase 14b** ✅ Data acquisition overhaul: **57→71+ active features**. 11 broken sources fixed + 2 new (ISS LIS lightning, VNP46A4 nightlight) + animal removed (79→78). 8 sources switched to auth-free alternatives. All endpoints verified with curl before commit. OLR→NCEI CDR, GRACE→GFZ GravIS, Ocean Color→CoastWatch DINEOF, Soil Moisture→CPC ERDDAP, Tide Gauge→UHSLC ERDDAP (19 stations), GOES X-ray→LISIRD 1-min, InSAR→LiCSAR 34 frames, Lightning→ISS LIS (GHRC DAAC), Nightlight→VNP46A4 (LAADS), Earthdata auth→BasicAuth
- **Backfill** ✅ 2011-2026 M3+ earthquakes (29K), TEC (4M), Kp (44K), GCMT focal mechanisms
- **Analysis Phase 15** ✅ Full test with all Phase 14b source fixes + data preservation checkpoint system. **70/78 active features (+5 from Phase 14). Test AUC 0.7499 (best ever), CV AUC 0.7411.** Data validation: 21 OK / 8 EMPTY / 1 MISSING. Earthdata auth (4 sources) failed due to URS API deprecating Basic Auth — fixed in Phase 15b. Feature matrix exported (1790×11×11×78). Job timed out at 6h (CSEP completed, final artifact upload missed). DB checkpoint preserved
- **Analysis Phase 15b** ✅ Earthdata auth rewrite (Bearer token priority + Basic Auth fallback), ISS LIS table separation (`iss_lis_lightning`), workflow reliability (timeout 420min, ML results checkpoint artifact, auth pre-validation step). **Test AUC 0.7499 (same as 15), 72/78 active features. Feature matrix export failed (int64 serialization) → fixed in 15c**
- **Analysis Phase 15c** ⚠️ Partial success (Run 23366201702, cancelled at ML step after 6h):
  - cloud_fraction ✅ 120,727 rows (2011-01 → 2011-10, coverage 4.9%)
  - ISS LIS ✅ 537 rows (2017-03 → 2017-07, coverage 5.5%)
  - tide_gauge ❌ UHSLC ERDDAP ConnectionTimeout (CI→Hawaii latency)
  - nightlight ❌ LAADS EULA redirect → HTML downloaded instead of HDF5
  - SO2 ❌ GES DISC Bearer 401, BasicAuth fallback failed (session cookie contamination)
  - Data validation: 23 OK / 6 EMPTY / 1 MISSING (improved from 8 EMPTY)
  - Feature matrix export fixed (int64 serialization + samples reuse 14h→sec)
  - DB checkpoint (230MB) preserved
- **Analysis Phase 15d** ✅ EMPTY source fixes (Run 23373703010): tide_gauge ✅ 2.4M rows (UHSLC CSV fallback), cloud_fraction ✅ 132K, ocean_color ✅ 17K. Electron flux ❌ hung 2h (NCEI data ended 2020), SO2 ❌ 0 rows (Earthdata credentials invalid), VIIRS ❌ 0 rows (h5py scalar bug). Cancelled at electron flux step
- **Analysis Phase 15f** 🔄 Electron flux complete rewrite + VIIRS fix + DB checkpoint restore (Run 23382779214, 2026-03-21):
  - **DB checkpoint restore at workflow start**: previous run's DB downloaded before fetch → all skip-logic effective (incremental fetch)
  - **Electron flux**: NCEI GOES-R SEISS L2 netCDF added (GOES-16 science + GOES-18 science/ops auto-fallback). Tested: 2024=366d, 2025=342d/12mo, 2026=79d/3mo — **zero gap from 2017 to present**. NCEI CSV retained for 2011-2016. Year-parallel fetch (semaphore 2), month-internal day-parallel (semaphore 5)
  - **VIIRS nightlight**: h5py attribute numpy scalar conversion fix (`np.asarray().flat[0]`)
  - **Electron flux timeout**: 10→30min
  - **CI deps**: netCDF4 + numpy added
  - SO2 still blocked (Earthdata username/password Secret needs manual update)
- **Analysis Phase 15g** ✅ Electron flux major expansion: NCEI netCDF 286,878 new daily records (2017-2026). **Test AUC 0.7540 (best ever), CV AUC 0.7415, 75 active features**
- **Analysis Phase 15h/15i** ✅ SO2 continuous fetch (408K rows) + coordinate snap fix. Non-zero rates improved (SO2 2.0%) but AUC unchanged (Test 0.7485). Root cause: spatial feature non-zero rates still low (cloud 8.2%, SO2 2.0%, soil 1.1%)
- **Analysis Phase 16** ⚠️ Continuous spatial data fetch (SO2/cloud/ocean_color). Timed out at 6h — fetch completed (SO2 2.3M, cloud 547K, ocean 89K) but ML not reached. DB checkpoint (610MB) preserved
- **Analysis Phase 18** ✅ S-net seafloor waveform features: NIED Hi-net approved, 0120A acceleration (150 stations, 100Hz). 7 features (RMS/H-V ratio/band power/spectral slope anomalies + spatial gradient + segment max). **75 → 84 features**. Test confirmed: 150/150 stations, 447/450 SAC files parsed
- **Analysis Phase 19** 🔄 S-net multi-sensor expansion: 0120 (broadband velocity) + 0120C (high-gain acceleration) added alongside 0120A. **VLF spectral analysis with 200s FFT windows** (0.005 Hz resolution) for tremor/SSE detection in 0.01-0.1 Hz band. 8 new features: VLF power/H-V anomalies, velocity RMS, VLF/HF ratio, accel-velocity coherence, VLF spatial gradient, high-gain SNR, velocity spectral slope. Multi-code quota management (190 request cap). DB schema: sensor_type column + VLF columns with migration. **84 → 92 features (185 total incl. dynamic selection)**. Workflow fix: S-net moved to early pipeline position (was unreachable due to 6h timeout), incremental DB save per item (prevents data loss on timeout), SMAP disabled (ERDDAP IP blacklist). Smoke test validated: 149 stations × 4 segments, 596 records committed
- **Analysis Phase 20** ✅ Lightning climatology feature integration: NASA LIS/OTD (1995-2014, 20,808 cell-months) + WWLLN Thunder Hour (2013-2025, 31,824 cell-months). 6 new features: flash_rate/thunder_hours with per-cell×calendar-month z-score and ratio baselines. Cross-validation on 2013-2014 overlap: Pearson r=0.39 (winter r=0.53-0.66, summer r≈0). Two independent optional groups — model learns relative weighting. **92 → 98 features**. OLR fetcher migrated from deprecated NCEI v01r02 to S3 archive v02r00 (+134K rows, →2026-04-14). GOES X-ray SWPC time_tag fix (+8 days, →2026-04-17). Salvage SKIP_TABLES cleared for so2_column/cloud_fraction checkpoint accumulation
- **Analysis Phase 21** ✅ Forward-feature-freshness audit: verified every optional feature group has live, low-latency data at inference time. Removed **5 always-zero features** from the active forward set -- LIS/OTD flash-rate (3 features, satellite ended 2014) and daily lightning (2 features, Blitzortung 0-row no-op plus ISS LIS ended 2023-11) -- which were silently 0 for 2024+ predictions (a temporal confound, nonzero only in historical training rows), both dropped via dynamic `get_active_feature_names()` exclusion with raw tables retained as backtest assets (PR #190, #193). Fixed two alive-but-stale fetchers. **SO2** repointed from the frozen OMSO2G.003 archive to live **OMSO2G.004** (identical 0.25-deg grid and variable, verified via OPeNDAP, so2_column had stalled at 2025-06-08, PR #191). **IOC sea level** chunk walk made **dual-ended** (reserves recent chunks newest-first so ioc_sealevel_anomaly stays fresh, was stalled at 2023-10 under pure oldest-first backfill, PR #192). Confirmed gravity (GRACE-FO ~3mo latency), WWLLN thunder-hour (annual-file cadence, at source frontier), and nightlight (annual composite) are source-cadence-limited by design, and InSAR is safely auto-excluded (disabled fetcher, empty table). All four PRs Opus-reviewed and CodeRabbit-clean.
- **CI/CD** ✅ GitHub Actions weekly analysis workflow (fetch → analyze → artifact, 400min timeout). **Step ordering optimized**: S-net (highest priority) runs immediately after core earthquake data; slow Earthdata fetchers follow. SMAP permanently disabled (ERDDAP IP blacklist). S-net uses `SNET_MAX_REQUESTS` env var for smoke testing (test-snet.yml runs production script with 5-request cap). **Data preservation**: DB checkpoint after fetch phase + ML results checkpoint (feature_matrix + predictions) + final DB upload. S-net waveform fetch uses incremental sqlite3 commits per item (survives timeout kills). Earthdata auth pre-validation skips 4 sources on credential failure. Data validation report (30 tables checked — collector_status excluded as legacy) saved to artifacts. **DB corruption prevention**: All 100 DB connections use `safe_connect()` with `PRAGMA synchronous=FULL` + `busy_timeout=10000` (centralized in `scripts/db_connect.py`). 28-item preflight test suite (`scripts/test_db_checkpoint.py`) runs before every fetch. 4-step verified WAL flush before artifact upload (checkpoint + integrity + WAL size + page count) — **upload blocked when verification fails** (`flush_ok` output guard). Restore step properly deletes corrupted checkpoints (`set +e` fix for `bash -e` shell). Dedicated test workflow (`test-db-integrity.yml`) validates corruption detection and cleanup
- **Data Completeness Initiative** 🔄 (started 2026-04-11) — Target 100% coverage across all 30 validated tables from 2011-01-01 to 2026-04-17. Full Step history in the [Data Completeness Initiative](#data-completeness-initiative) section below.
- **Mobile** ✅ Responsive design (bottom sheet panel, touch-optimized controls)

## Data Completeness Initiative

The step-by-step record of this initiative -- about a hundred numbered steps with their dates and outcomes -- lives in [docs/DATA-COMPLETENESS-LOG.md](docs/DATA-COMPLETENESS-LOG.md).  It was kept inline here until 2026-08-23 and was moved unchanged.

## Analysis Results (2011-2026, 28K M3+ earthquakes, 6.4M TEC, 45K Kp, 5.3M GNSS-TEC, 24M ULF, 98 features with dynamic selection)

### Summary

Phase 1 indicators (b-value, Kp, low-res TEC) were all negative after bias correction. Phase 2 found 3 physics-based signals that survived aftershock isolation and prospective testing. **Phase 4 forward-looking evaluation achieved 62.5% precision (7.8x gain) by combining ETAS residual + cumulative CFS + foreshock alarms.**

The research narrative -- every pre-registered round, its floors, its verdict and the corrections that followed -- lives in [RESEARCH-LOG.md](RESEARCH-LOG.md), newest last, with an index at the top.  It was kept inline here until 2026-08-23 and was moved unchanged.

**Where the research stands (2026-09-04).** Since 2026-09-02 the question being worked on is
the *isolated mainshock* -- a shock that is not an aftershock of anything larger -- because a
measurement that month showed the pooled figure is carried by aftershock sequences. On the
scored rows the arm beats its own climatology by +0.018 per window on aftershock-driven
positives (39% of the positives) but by only +0.007 on isolated mainshocks (61%), and by +0.010
on the stricter 11% that had no M>=4 within 50 km in the prior year -- that last figure being
the one that does not pass its own interval. The most recent work runs in a causally-selected
global arena (2,280 one-degree cells that were already active in the first fifteen years, scored
from year twenty, 9,993,240 rows over 36 windows), judged against permutation floors under
contracts frozen before each run. Detection completeness in the global catalogue is not uniform
in space or time, and the arena's seed years are its least complete.

Two of the floored verdicts there. Aiming a second factor at *large* mainshocks rather than at
mainshocks is worth +0.0027 per window at magnitude 6.0 against a floor whose five worlds average
+0.0002 (round 201). Replacing that fitted factor with a per-cell rate counted directly from past
events -- no model at all, no feature columns read -- is worth +0.0086 against a floor at -0.0001
(round 202), and on the same windows exceeds the fitted version by +0.0059: a floor-free
comparison of two arms whose selection rules both saw sizing, not a statement about fitting in
general. Both rounds were reproduced by a checker frozen before the run.

Read those with their limits, which the contracts state and the log repeats. Neither round is an
independent estimate of its effect size: both effects were measured in fitting-free sizing before
the contracts were written, so what the rounds add is the floor and the frozen record, not a
second measurement. The base is a climatology, not the shipped forecast, and the floors move only
the counted flag -- the base is identical in every world and is never tested. Five permutation
worlds give a rank of one in six, not a p-value. The twenty floor worlds of round 202 are round
201's own, bit for bit, so the two floor passes are not independent evidence about each other. In
round 202 the causal selection sat at the top of both frozen grids at this threshold, so the
optimum may lie outside them; no grid was widened after seeing results. No new observable is
introduced -- the same catalogue is re-read. A within-cell shuffle of the same labels produces a
*stronger* arm than the truth, so the effect is not separated from a per-cell map and
event-by-event discrimination is not demonstrated. The cost on all positives was outside these
two rounds; where it was scored in this arena (round 198) the mainshock readout cost -0.019 per
window. And magnitudes are not compared across catalogues, so none of this is a statement about
Japan.



Two methodological artifacts were responsible for all false positives found during the investigation:

1. **Aftershock contamination**: Without isolating independent events, clustering inflates apparent signals (b-value: 90% → 15%, Kp -12h: 62% → 11%)
2. **Sampling bias**: Using chronologically-first events over-samples the 2011 Tohoku aftershock cluster (TEC σ: 0.942 → 0.263)

### Phase 1: Single indicators — all negative

**b-value (Gutenberg-Richter) — ❌ Aftershock artifact**

| Window | Random b<0.7 | All M5+ b<0.7 | Isolated M5+ b<0.7 |
|---|---|---|---|
| 7-day | 16.9% | 90.0% | **15.2%** (= random) |
| 30-day | 42.6% | 91.6% | **39.5%** (= random) |
| 90-day | 72.2% | 84.6% | **55.1%** (noise range) |

**Epicenter TEC (raw) — ❌ Systematic bias**

Random TEC drops *more* than pre-earthquake TEC (σ=-0.781 vs -0.222). Bias from seasonal/diurnal/solar cycle patterns.

**Multi-indicator grid search (100 combos) — ❌ No signal**

Best lift 1.82 at n=17. Fixed thresholds: earthquake 22.1% vs random 21.4% — identical.

### Phase 2: Candidate signals found → validated → all rejected

Two promising signals were identified during exploratory analysis. Both were then rigorously validated with aftershock isolation + balanced time sampling + alternative methods + bootstrap CI. **Both collapsed.**

**Kp -12h geomagnetic spike — ❌ Aftershock chain artifact (confirmed)**

| Lead time | All events Kp>3 | **Isolated events Kp>3** | Random Kp>3 |
|---|---|---|---|
| -24h | 55.9% | **12.2%** | 14.2% |
| -12h | 61.5% | **10.8%** | 14.0% |
| -6h | 55.1% | **10.0%** | 15.2% |

The apparent 62% Kp>3 rate was entirely from aftershock chains: the first M5+ in a cluster occurs during a Kp storm, then subsequent events in the same cluster all inherit the high Kp. Isolated events show Kp *below* random at every lead time.

**TEC detrended (seasonal correction) — ❌ Sampling bias + aftershock artifact (confirmed)**

| Condition | Before bias fix | **After bias fix** | Bootstrap p |
|---|---|---|---|
| Random | σ=+0.247, spikes 15.6% | σ=+0.247, spikes 15.6% | — |
| All M5+ | **σ=+0.942, spikes 56.5%** | σ=+0.279, spikes 19.5% | p=0.265 |
| **Isolated M5+ only** | not tested | **σ=+0.263, spikes 15.0%** | **p=0.389** |

The σ=0.942 "discovery" had two compounding artifacts:
- `target_events[:500]` selected chronologically-first events, biased toward 2011 Tohoku aftershock cluster
- Non-isolated events carried residual clustering effects

After balanced time sampling + isolation filter: mean_diff=0.016, 95% CI=[-0.106, +0.137], **indistinguishable from random**.

**Validation: temporal stability — ❌ No signal in either period**

| Period | Isolated TECσ | Random TECσ | Bootstrap p |
|---|---|---|---|
| 2011-2018 (n=1937) | 0.218 | 0.312 | p=0.833 |
| 2019-2026 (n=1178) | 0.095 | 0.183 | p=0.841 |

Isolated events show *lower* TEC than random in both periods.

**Validation: alternative detrending (30-day rolling) — ❌ Zero spikes**

| Condition | Rolling σ | Spikes (σ>+1) |
|---|---|---|
| Random | -0.666 | 0.0% |
| Isolated M5+ | -0.622 | 0.0% |

Independent detrending method confirms no signal.

**Validation: magnitude dependence (with isolation) — ❌ No monotonic increase**

| Magnitude | Isolated TECσ | Spikes |
|---|---|---|
| M5-5.9 (n=1373 iso) | 0.127 | 11.7% |
| M6-6.9 (n=160 iso) | 0.083 | 10.1% |
| M7+ (n=20 iso) | 0.370 | 20.0% |

M6 is *weaker* than M5. No physically consistent magnitude scaling.

### Key lessons

1. **Aftershock isolation is essential** — without it, every indicator shows inflated signals due to temporal clustering
2. **Sampling method matters** — chronological truncation (`[:N]`) can introduce severe bias when event rates are non-stationary (e.g., post-Tohoku)
3. **Low-resolution global indices cannot detect local precursors** — IONEX TEC (2.5°×5° grid) and Kp (global 3-hour average) spatially average away any local earthquake-related signal
4. **Always validate with multiple independent methods** — the TEC signal survived aftershock filtering OR sampling correction alone, but collapsed under both simultaneously

### Phase 2: Physics-based and statistical approaches — 3 signals found

Phase 1's fundamental limitation was **spatial resolution** — global indices dilute local signals below detection. Phase 2 attacks from 4 independent directions. Three produced signals:

**Coulomb stress transfer — CFS threshold-dependent lift (spatial control applied)**

Using Okada (1992) dislocation model with 3,060 GCMT focal mechanisms. Compared earthquake locations vs 2-5° shifted locations (controls for spatial clustering):

| CFS threshold | Earthquake % | Shifted 2-5° % | Lift |
|---|---|---|---|
| > 10 kPa | 63.7% | 68.6% | 0.93 (no signal) |
| > 100 kPa | 45.4% | 22.9% | **1.98** |
| > 500 kPa | 23.4% | 5.3% | **4.43** |
| > 1000 kPa | 14.7% | 2.4% | **6.03** |

Low CFS thresholds show no signal (spatial clustering effect). **High CFS (>500 kPa) shows 4-6x lift even after spatial control** — earthquakes preferentially occur at *exact* stress-enhanced locations, not just the same general region.

**Seismicity rate anomaly — 6.7x activation lift (model-free)**

Regional M3+ rate in 7 days before each M5+ event vs long-term regional average:

| Condition | Activation (>2x rate) | Quiescence (<0.5x rate) |
|---|---|---|
| Before M5+ | **47.0%** | 23.0% |
| Random | 7.0% | 75.4% |
| **Lift** | **6.71** | 0.31 |

47% of M5+ events are preceded by at least 2x normal seismicity rate in their region.

**Spatiotemporal clustering — lift 2.83, p=0.0 (validated)**

Zaliapin & Ben-Zion (2013) nearest-neighbor distance clustering:

| | Has foreshock sequence | Mean foreshock count |
|---|---|---|
| M5+ events | **14.7%** | 9.19 |
| Random M4 | 5.2% | 2.17 |
| **Lift** | **2.83** | — |

Bootstrap 95% CI: [2.01, 4.49], p=0.0. Temporally stable: 2011-2018 = 16.1%, 2019-2026 = 12.3%. Magnitude-dependent: M5 = 14.1% → M6 = 21.2%.

**High-resolution GNSS-TEC — data unavailable**

Nagoya University ISEE archive URLs returned 404 for all attempted date patterns. URL investigation needed.

### Phase 2.5: Aftershock bias validation — all 3 signals survived

Critical question: are the 3 signals independent, or just aftershock cascading? **All survived the same isolation filter that destroyed Phase 1.**

**Isolation test — signals persist for independent (non-aftershock) M5+ events**

| Signal | All M5+ | **Isolated M5+** | Random | **Isolated lift** |
|---|---|---|---|---|
| CFS > 500 kPa | 18.3% | **7.5%** | 0.2% | **37.5** |
| Activation > 2x | 47.0% | **14.9%** | 8.0% | **1.86** |
| Has foreshock | 68.3% | **42.8%** | 10.4% | **4.12** |

Phase 1's TEC detrended signal (σ=0.942) collapsed to σ=0.263 (p=0.389) under isolation. Phase 2's signals maintained significant lifts (37.5x, 1.86x, 4.12x).

**Time delay — isolated events show long-term Coulomb triggering (median 333 days)**

| Condition | Median delay | < 30 days | > 90 days | > 365 days |
|---|---|---|---|---|
| All M5+ | 161 days | 32.8% | 57.8% | 34.2% |
| **Isolated M5+** | **333 days** | **10.0%** | **77.5%** | **47.2%** |
| CFS > 500 kPa | 6 days | 62.4% | 32.2% | 19.9% |

Isolated events occur a median of 333 days after their nearest prior M5+ — not aftershocks but **delayed stress-triggered events**. 77.5% occur more than 90 days later.

**Signal correlation — partially independent (ratio 2.12)**

| Metric | Value |
|---|---|
| P(all 3) if independent | 12.2% |
| P(all 3) observed | 25.9% |
| Correlation ratio | **2.12** |

Ratio of 2.12 means signals are **moderately correlated but not redundant**. They contain partially independent information — combining them is meaningful.

**Prospective test — combined score lift 20.66 in unseen data**

Combined score: count of (CFS>100, rate>2x, has foreshock) per event.

| Score | Train 2011-2018 | **Test 2019-2026** | Random |
|---|---|---|---|
| 0 (no signals) | 22.9% | 31.7% | **84.0%** |
| 1 | 15.6% | 27.2% | 13.4% |
| 2 | 24.0% | **33.9%** | 2.4% |
| 3 (all signals) | 37.5% | 7.2% | 0.2% |

Test period: 41.1% of M5+ events have score ≥ 2, vs 2.6% of random locations → **lift 20.66**. The model generalizes to unseen time periods.

### Phase 3a: Catalog-based methods — mostly negative

Three additional methods using existing earthquake catalog only (no new data). None added significant prediction power beyond Phase 2.

**LURR (Load-Unload Response Ratio) — ❌ No signal**

| Window | EQ LURR>1.5 | Random | Lift |
|---|---|---|---|
| 30 days | 26.3% | 55.0% | 0.48 |
| 90 days | 31.6% | 36.9% | 0.86 |
| 180 days | 28.3% | 30.2% | 0.94 |

Tidal stress asymmetry shows no earthquake-specific pattern. Random locations have equal or higher LURR values.

**Natural Time Analysis — ❌ No signal**

κ1 variance near critical value (0.070) is equally common before M5+ events and at random times (lift 0.84-1.19 across all window sizes).

**Earthquake Nowcasting — ⚠️ Weak signal (lift 1.31)**

EPS > 70 before M5+ events: 26.8% vs 20.4% random (lift 1.31). Weak magnitude dependence (M7+: 35.7%). Insufficient for standalone prediction but may complement Phase 2 signals.

### Phase 3b: Independent physical observations (in progress)

The critical next step: **non-seismological data** that is physically independent from Phase 2's earthquake-catalog-based signals.

| Parameter | Physical mechanism | Data source | Status |
|---|---|---|---|
| **MODIS thermal IR** | Stress → gas release → surface heating (LAIC model) | ORNL DAAC TESViS API (no auth, 1km) | **359 records fetched**, analysis script ready |
| **ULF magnetic field** | Stress → piezoelectric/electrokinetic emission | INTERMAGNET BGS GIN + WDC Kyoto | Fetcher rewritten, testing |
| **S-net ocean bottom pressure** | Slow-slip → seafloor displacement | NIED Hi-net portal (150 stations) | Registration needed |
| GEONET GPS-TEC (per-station) | Point TEC above epicenters | GSI GEONET RINEX | Nagoya Univ. 404, alternative needed |
| Radon / He isotopes | Fault degassing | AIST monitoring | Limited access |

**MODIS LST analysis** (Tronin 2006, Ouzounov & Freund 2004): For each M5.5+ earthquake on land, MODIS Land Surface Temperature is extracted at the epicenter ±14 days. Anomaly detection uses standardized deviation from local baseline (RST/RETIRA method, Tramutoli 2005). Tests pre-event anomaly, isolation filter, magnitude/depth dependence, and temporal profile.

**ULF magnetic analysis** (Hayakawa et al. 2007, Hattori 2004): Analyzes 1-minute geomagnetic data from KAK/MMB/KNY for three precursor signatures: (1) ULF Z-component spectral power increase, (2) Sz/Sh polarization ratio > 1 (lithospheric origin), (3) fractal dimension decrease. Nighttime-only (0-6 LT) to avoid anthropogenic noise.

### Phase 3b: Independent physical observations — MODIS ❌, ULF ⚠️

**MODIS Land Surface Temperature — ❌ No thermal precursor signal**

Pre-earthquake 7-day anomaly: mean=0.061σ, 95% CI=[-0.109, 0.224], >2σ = 0.0%. The LAIC thermal precursor hypothesis (Tronin 2006) is not supported in this dataset.

**ULF Magnetic — ⚠️ Strong retrospective signal, forward evaluation pending**

| Station | Events | Power ratio (pre/post) | Sz/Sh polarization | Fractal dim |
|---|---|---|---|---|
| KAK | 439 | **mean 7.9x**, >2x = 53% | pre=0.98 > post=0.34 | pre=1.27 < post=1.33 |

All three ULF precursor signatures are present (power increase, lithospheric polarization, fractal regularization). **However, data covers only 2011-01-05 to 2011-05-05 (80 days including Tohoku M9)** — aftershock contamination is almost certain. Full-period data needed for prospective evaluation.

### Phase 4: Prospective (forward-looking) prediction — **gain up to 7.8x**

The fundamental shift: from "given earthquake, was there anomaly?" to **"given anomaly now, will earthquake follow?"** Evaluated on 2019-2026 (unseen data), with spatially-resolved base rates per 2°×2° cell.

| Signal | Alarms | Precision | Recall | **Prob. Gain** | IGPE (bits) |
|---|---|---|---|---|---|
| **Combined (ETAS+CFS+fore) ≥2** | **16** | **62.5%** | 2.5% | **7.8x** | **2.96** |
| ETAS residual > 5x | 38 | 52.6% | 6.8% | 4.0x | 1.99 |
| Foreshock ≥ 10 | 74 | 39.2% | 8.8% | **5.1x** | 2.36 |
| Foreshock ≥ 5 | 257 | 34.2% | 16.3% | 4.1x | 2.04 |
| ETAS residual > 3x | 71 | 47.9% | 8.2% | 3.8x | 1.92 |
| Rate > 5x | 464 | 19.0% | 14.4% | 4.2x | 2.09 |
| Cumulative CFS > 100 kPa | 440 | 19.8% | 3.7% | 2.4x | 1.25 |

**Key finding**: When ETAS residual, cumulative CFS, and foreshock alarms fire simultaneously, 62.5% of the time an M5+ earthquake follows within 7 days — **7.8 times better than random**. The ETAS residual (rate exceeding aftershock model prediction) is the strongest individual signal at 52.6% precision.

**Pattern Informatics (Rundle 2003)**: Prospective Molchan AUC = 0.349 (< 0.5 = better than random). PI hotspots preferentially attract future M5+ events. Top hotspots: Iburi (42.75°N), Izu-Bonin (32.75°N, 29.75°N).

### 2026-06: Skill decomposition — what the AUC actually measures

A systematic audit (2026-06-08) of where the monthly-forecast skill comes from, using an independently harvested complete ISC/JMA M3+ catalogue (108,808 events) and region-split walk-forward evaluation:

- **Offshore AUC 0.83 > onshore 0.72** (M5+/34d, cells split by GNSS-station presence as a crustal/subduction proxy). The pooled 0.79-0.87 is dominated by "the next M5+ in already-active subduction zones" — aftershock clustering, not rare-mainshock precursors.
- **19 current-activity features (rate / ETAS residual / days-since / neighbour rate) match the full 85-feature set.** The other 66 — b-value, CFS, Pattern Informatics, Benioff strain, and *every* exogenous channel (cosmic ray, geomagnetic ULF, OLR, SO2, soil moisture, cloud, X-ray, ionospheric TEC, S-net VLF, lightning, GNSS strain) — contribute net zero or negative. Onshore, the 44 exogenous features alone score **AUC 0.517 ≈ coin flip**.
- **A single clustering feature (pi_score, AUC 0.770) nearly matches the full 85-feature GBT (0.79)**; features + ML add only ~+0.02 on top of it.
- **A static spatial climatology (per-cell 2011-2020 M5+ frequency, time-invariant) scores AUC 0.862** on the 2020-2026 test labels — 0.009 below the 0.871 GBT ceiling. Pooled AUC is saturated by the "Tohoku-oki is always active" map.
- The temporal skill that remains is **sparse but real**: given an M5+ in a cell, the next-7-day M5+ probability is **2.2x** baseline (Omori clustering). It is concentrated in post-mainshock windows (~2% of cell-time), which is why it barely moves a pooled all-window AUC.

**Complete-catalogue precursor precision (buildup watch)**: rate-ratio buildup precursors are real but specific to shallow crustal earthquakes — Noto 2024 M7.5 ranks **1st of 114 cells with 32x lift using only data up to 30 days before**, and Kumamoto 2016 shows a 483x fused anomaly; subduction M7s show no catalogue precursor (activity saturation at 400-934 events/yr). Operating point: recall ~18%, lift ~2.6-3x, false-alarm rate ~7%.

**GNSS strain channel (NGL tenv3, 1,217 Japanese stations) — publication-grade null** (2026-06-08): strain-anomaly fusion correctly ranks Noto 2024 M7.5 #1 one month ahead and diagnoses the documented slow-slip precursor, but in systematic prospective evaluation it never beats seismicity-only scoring across months, and adding real GEONET-derived features to the ML changes AUC by −0.0007. Physically real, case-diagnostic, zero aggregate forecast skill.

### 2026-06: CSEP information gain — the right metric, and where ETAS wins

Pooled AUC saturates on spatial climatology and under-measures temporal skill. Re-evaluating on the CSEP standard axis — Poisson forecast log-likelihood gain over the time-invariant climatology baseline — fit on 2012-2020, scored prospectively on 2020-2026 (M3.5+ driver catalogue, 54,075 events; M5+ targets):

| Model | Information gain (nats/event) | Probability gain |
|---|---|---|
| Gridded ETAS (2°/daily, proper likelihood fit) | +0.46 | 1.58x |
| **Continuous grid-free ETAS (point-process MLE)** | **+3.63** | median 2.52x |
| **+ regional kernels (crustal vs subduction)** | **+3.69** | median 3.11x |
| Post-mainshock regime (<7d, <100 km of M≥6) | +9.10 | ~9000x (n=92) |

- The continuous model recovers textbook physics (Omori p=1.15, no degenerate parameters) and 100% of test events score above climatology. Continuous (per-event) and gridded (cell-window) evaluation frames differ, so the rows are not strictly comparable — the robust statement is that both beat climatology prospectively and gridding destroys most of the resolvable skill.
- **The regional split is real physics**: subduction kernels fit larger aftershock zones, stronger magnitude scaling and faster Omori decay than crustal ones (+0.055 nats, train and test agree; offshore-target IG 4.01 > onshore 3.48).
- Anisotropic (trench-elongated) kernels and spatially-varying b-value magnitude forecasting were both tested and rejected (overfit −0.009 / aggregate null +0.0005, with one weak real signal: causal b drops to 0.86 before M≥6.5 vs 0.94 before M5-6).
- Short-horizon research validation: weekly 7-day M5+ maps at 0.5° resolution score **1.83x climatology overall and 6.9x in post-mainshock windows** (0.25°: 1.94x / 15.9x, post-mainshock alerting only). A USGS near-real-time driver variant loses little (1.67x / 6.8x), so the approach survives on a live feed.
- **Operationally integrated (2026-06-17)**: the regional crustal/subduction split is now a live feature in the monthly gridded outlook (`operational_forecast.py`, which commits to `forecasts/`). Two onshore/offshore-driven regional-ETAS rate features — re-fit on the operational USGS M≥4 driver catalogue — were added to the existing 6-kernel + background feature set. A 10-fold walk-forward backtest (baseline reproduces the deployed OOS AUC 0.863) shows a small but consistent gain on the metric that matters: **CSEP information gain +94 nats over the backtest (+0.013/hit), OOS AUC 0.863→0.865**. AUC barely moves because it saturates on spatial climatology; the regional split's contribution is in dynamic/temporal skill, which the information-gain axis captures. Modest in absolute terms but real and physically grounded — the uniform heuristic kernels cannot express region-dependent triggering. Graceful fallback to the original kernel set if the fitted-parameter file is absent.

**Conclusion of the arc**: earthquake forecast skill exists and decomposes cleanly — a static hazard map is an AUC-0.86 product; properly-fit ETAS aftershock forecasting adds a genuine 1.6-3x (post-mainshock ~10x) probability gain on top; exogenous precursor channels add nothing measurable in aggregate. The catalogue/ETAS occurrence axis is at its ceiling.

### 2026-06: Next frontier — nucleation from raw waveforms (both episodes complete - five pre-registered nulls, a sixth b-value test [retrospective asperity only], a seventh below-catalogue tremor test [0/3], and an eighth dv/v velocity-change test [0/2], a ninth self-supervised waveform-embedding test [Kumamoto, null], and a tenth multi-case extension [Iquique, null], and an eleventh geodetic GNSS test [Kumamoto 5-min kinematic, null - raw and common-mode-filtered], and a twelfth seafloor-OBP verification [2018 Boso SSE, real OBP + open ocean model, null - reproducible detection floor ~Mw6.5], and a thirteenth multi-case floor-vs-network curve [Cascadia/Guerrero/Hikurangi + Boso onshore & OBP, noise-limited floor Mw~5.3-6.4 but realization set by near-field coverage - Hikurangi's documented Mw7.0 is invisible onshore], and a fourteenth borehole-strainmeter modality [2012 Cascadia ETS, detected 8 sigma via an inverse-variance strain-tensor matched filter, floor ~0.25 Mw below GNSS at matched geometry - overturns the earlier strainmeter dead-end], then the curve hardened by a fifth plate boundary [Nicoya 2007+2009 Costa Rica, onshore-over-slip detected, spatial pattern r=0.87 perm p<0.003], a full spatial-pattern + permutation audit of every detection [Boso/Bungo/Manawatu/Nicoya/Guerrero confirmed, Cascadia GNSS downgraded to spatially-unconfirmed, Hikurangi null confirmed at the pattern level], a sensitivity-at-slip mechanism [offshore slip ~30x below the onshore-detectable level explains the null], and a distributed-slip inversion [Boso data-driven Mw6.5 = catalog, no assumed area], a fifteenth InSAR modality [2018 Boso + Guerrero SSE, open Sentinel-1 troposphere/separation-limited null], a sixteenth offshore near-field test [S-net seafloor, the three largest S-net-era M7+ events (Miyako/Aomori/Fukushima) all NULL, extended below catalogue completeness by matched-filter with an aftershock positive control], and a seventeenth onshore Hi-net dense-array test [2016 Kumamoto M6.5, the deepest open-data injection floor (dM-5.0, ~ML1-2) and a power-calibrated null on foreshock-rate acceleration], and an eighteenth onshore high-density case [2019 Ridgecrest M7.1 (SCSN), the same powered null shown robust to both a stationary and an Omori-decaying baseline (last-3 h p=0.23-0.27), self-injection floor dM-4.5], and a nineteenth b-value FTLS test [2019 Ridgecrest M6.4 to M7.1, the Gulia and Wiemer (2019, Nature) foreshock-traffic-light showcase: no prospective foreshock b-drop with independent magnitudes, incompleteness-robust b-positive stays ~0.9 to the final approach, corroborating the Dascher-Cousineau 2020 parameter-sensitivity critique], and a twentieth b-value FTLS test [2016 Amatrice-Visso-Norcia Mw6.5, the third Gulia and Wiemer (2019, Nature) foreshock-traffic-light showcase and the only one where the B1 relative-drop is computable (real pre-Amatrice background): the Aki foreshock RED light (b 0.72, -27.5% vs background) is an incompleteness artifact (the robust b-positive is a normal 1.14, invariant to completeness), the operational [Visso, Norcia] window is flat-to-rising (1.09 to 1.21), the apparent rate acceleration is the intervening Visso Mw5.9 aftershock sequence (two-term Amatrice+Visso Omori superposition matches the final day to ratio 1.00, and the run-up rate decays monotonically into Norcia, Spearman rho -1.000), and the spatial b-field is unresolved at both subsample sizes -- a powered FTLS null on an 894k-event Mc 0.20 catalogue, completing the b-value channel at three-for-three], and a twenty-first b-value FTLS test [2009 L'Aquila Mw6.3, the canonical NATURAL foreshock case (basis of the 2012 seismology trial), not a Gulia and Wiemer showcase: a sparse aftershock-dominated catalogue gave an unresolved low foreshock b-positive (0.90), but the foreshock-specific template-matched Cabrera et al. (2022) catalogue (10x denser, Mc 0.80) shows the foreshock b-positive is normal in every phase (1.0-1.1, no drop, 1.01 with the FS1 aftershock burst removed), the prospective b is flat (non-overlapping moving rho 0.015 p 0.937), the rate is a chain of triggered Omori bursts with an overall decay rather than smooth acceleration, and the foreshock b-field has no low-b nucleation asperity (resolved at K=80 with the nucleation patch at the field median) -- denser data converting a sparse false positive to a clean null, completing the b-value channel at four-for-four and extending it to the most-cited natural foreshock case], and a twenty-second b-value/rate FTLS test [2014 Iquique Mw8.1, the canonical accelerated-nucleation subduction megathrust: on the open Sippl et al. (2018) IPOC catalogue (source-box pre-mainshock Mc 2.60) the foreshock b-positive (0.60) is not anomalously low (the Aki 0.44 drop is incompleteness, robust to FS-exclusion; the marginal foreshock-below-aftershock gap at clean Mc is the small Schurr 2014 multi-year decline, not an FTLS RED), the prospective b is flat-to-rising, the acute-window seismicity rate decays with no acceleration excess over an ETAS-lite Omori superposition (final-days ratio 1.10, p 0.28), and the foreshock b-field has no resolved low-b asperity -- so the short-term FTLS b/rate SEISMICITY channel does not capture this nucleation, whose documented precursors (slow slip, migration, the Kato 2016 ~270-day up-dip acceleration, the multi-year b-decline) are aseismic/long-timescale/up-dip and at or below open resolution; fifth case on the channel and the first subduction megathrust])

The remaining unsolved problem is **nucleation of independent mainshocks**, and that information cannot be in the M3.5+ catalogue — it discards >99% of the waveform record, while the candidate precursors (Mc<2 micro-foreshocks, tremor, slow slip) live below catalogue completeness. New approach: build Mc≈1-2 micro-earthquake catalogues directly from continuous raw waveforms with deep-learning phase pickers (SeisBench PhaseNet + PyOcto association, Kaggle GPU), and test whether documented foreshock acceleration/migration is detectable prospectively.

- **Pipeline validated on Iquique 2014** (public GEOFON/IPOC data): 17 foreshock days → 4,806 auto-located micro-events; the catalogue reproduces the documented foreshock migration toward the mainshock epicentre (median distance 182 km → 28 km over 9 days, consistent with Kato & Nakagawa 2014).
- **Tohoku 2011 (Hi-net; ground truth = Kato et al. 2012)**: a dedicated research fetch workflow (`fetch-hinet-research.yml`, window-scoped SAC export) feeds the 2011-03-09 M7.3 foreshock → 03-11 M9 window; PhaseNet on real Hi-net SAC spike-detects the M7.3 itself in a smoke window.
- **Getting clean data was an account-contention problem, not a quota one**: the back-half degradation came from the production Hi-net backfill workflow sharing the single NIED account (one concurrent data request, account-global station selection) with the research fetch — a probe re-fetch returned 0/1 segments while a backfill run was live. Re-asserting the station selection per request (shipped earlier) is necessary but not sufficient; temporarily disabling the backfill workflow for the duration of the re-fetch let all 30 re-fetched segments succeed (30/30), filling the void to ~45 min before the mainshock.
- **Prospective result, scored against a pre-registered test (2026-06-10)**: the void-filled full-window micro-catalogue (5,385 SAC over 03-09 00:00 → 03-11 14:00 JST — to ~45 min before the M9 — 49 stations, 10.7K PhaseNet picks, 87 PyOcto-located events) was scored against criteria fixed *before* the clean re-fetch arrived, to avoid post-hoc statistic tuning: C1 rate-excess (M9-proximal seismicity vs extrapolated M7.3 Omori aftershock decay), C2 migration (per-6h-bin median/p10 distance slopes over the final 30 h), C3 proximity (within-50 km fraction in the final 12 h).
- **Verdict (association catalogue - revised by the 2026-06-11 iterations below): C1 PASS, C2 FAIL, C3 FAIL (1 / 3)** — a genuine near-hypocentre rate excess, but not a clean prospective migration-to-mainshock. C1: M9-proximal (≤75 km) events in the final 30 h run **4.75× the rate predicted by extrapolating the M7.3 aftershock decay** (17 observed vs 3.6 predicted) — real elevated foreshock activity around the eventual hypocentre, from raw waveforms with no catalogue input (the Omori fit rests on only 5 proximal events, so the multiplier is approximate, but the excess is qualitatively clear). C2/C3 fail because the final two 6-h bins (03-11 06:00 / 12:00 JST) are sparse (n = 3 / 5) and locate onshore: the robust distance slope over the final 30 h comes out positive, and only 1 of 12 events in the final 12 h falls within 50 km.
- **Why C2/C3 fail, and the next lever**: the bins up to 03-11 00:00 JST *do* approach (median 207 → 78 km, p10 → 33 km), but that is a post-hoc sub-window the pre-registered test deliberately does not credit. The M9 nucleation patch sits ~180 km offshore, at the edge of the onshore Hi-net array's resolving power, and a homogeneous 0-D velocity model breaks down beyond ~200 km (Pn) — so the immediate pre-mainshock events either go undetected or mislocate onshore. Demonstrating clean migration-to-the-mainshock prospectively would need an offshore/1-D velocity model or template/cross-correlation relocation (as Kato et al. 2012 used), not naive association — that is the next lever, not a physical null.
- **Methods iterations (2026-06-11) - the verdict does not improve, and C1 weakens under uniform detection sensitivity.** (1) A layered 1-D NE-Japan velocity model (Moho 33 km + mantle Pn 8.0 km/s, PyOcto travel-time tables) improves mid-window offshore locations (p10 distance 26-35 km; the 03-11 00:00 bin moves to median longitude 142.35E, just short of the hypocentre at 142.86E) but the final 12-18 h stay sparse and mislocated onshore - verdict unchanged (C1 PASS 4.75x, C2/C3 FAIL). (2) A Kato-2012-style matched filter (the 85 located events as 4-s templates, 2-8 Hz bandpass, network-stacked normalized cross-correlation, 9xMAD threshold; 85/85 self-detections, +234 new detections, 319-event catalogue) equalizes detection sensitivity across the whole window - and the pre-registered verdict becomes **C1 FAIL (1.43x), C2 FAIL, C3 FAIL (0 / 3)**. With 38 rather than 5 fit-window proximal events anchoring the Omori extrapolation, the final-30 h excess drops from 4.75x to 1.43x, below the registered 2x threshold. Honest reading: the apparent near-hypocentre rate excess was largely a detection-sensitivity artifact - the M7.3 aftershock zone itself lies within 75 km of the M9 hypocentre, and once sensitivity is uniform the final-30 h proximal rate is consistent with ordinary M7.3 aftershock decay. None of the pre-registered prospective criteria survive the highest-sensitivity catalogue: onshore-only Hi-net data does not prospectively resolve the documented nucleation signals. Resolving them would need offshore instrumentation (OBS; S-net exists only post-2011) or a different case.
- **Case 2 methods (2026-06-12): Kumamoto 2016 — the same prospective test on a fully onshore case.** The Tohoku null's stated boundary condition was array resolution (nucleation patch ~180 km offshore). The 2016 Kumamoto sequence (Mj 6.5 foreshock 2016-04-14 21:26 JST on the northern Hinagu fault → Mj 7.3 mainshock 28 h later at 01:25 JST, hypocentres ~4.5 km apart at 11-12 km depth; ground truth = Kato et al. 2016 GRL: northeastward foreshock migration + slow slip toward the mainshock initiation point) places the entire nucleation zone inside the dense onshore Hi-net array. The research fetch workflow is now station-box-parameterized (lat/lon inputs); the full 50 h window (04-14 00:00 → 04-16 02:00 JST, 20 Kyushu stations, 2,850 SAC, 50/50 segments) was fetched with the production backfill workflow paused (account-contention lesson), and a pre-registered verdict script — C1 Omori rate excess (≤10 km of the mainshock epicentre, fit window placed after the second Mj 6.4 foreshock so its transient inflates the prediction conservatively), C2 Theil-Sen migration (per-2h-bin median and p25 distance), C3 final-6 h proximity fraction (≤5 km, scaled to the 4.5 km foreshock-mainshock separation) — was fixed before any waveform was picked, with the final verdict to be taken on the matched-filter-equalized catalogue per the Tohoku sensitivity-artifact lesson. PhaseNet → PyOcto (1-D) → matched-filter ran as a single 5.4 h Kaggle GPU kernel.
- **Case 2 verdict (2026-06-12, pre-registered, matched-filter catalogue): C1 FAIL (1.11x), C2 FAIL (+0.04 km/h), C3 FAIL (0.498) — 0 / 3, second honest null.** The pipeline itself performed ideally: 19 stations, 50/50 covered hours, 48,699 PhaseNet picks, 1,863 PyOcto-located events, 400/400 matched-filter self-detections, +745 new detections → a 2,608-event detection-uniform micro-catalogue of the 28 h foreshock sequence. Against the pre-registered criteria: C1 — final-12.4 h mainshock-proximal (≤10 km) count is 789 observed vs 711 Omori-extrapolated (1.11x; 1.24x on the plain association catalogue), i.e. the rate near the future initiation point is ordinary combined M6.5+M6.4 aftershock decay; C2 — Theil-Sen slopes of per-2h-bin median/p25 distance are slightly *positive* (+0.042 / +0.016 km/h): the cloud marginally expands rather than approaches; C3 — 220/442 = 0.498 of final-6 h events within 5 km, one event short of the 0.5 threshold (0.71 at the report-only 7.5 km radius). Honest reading: the foreshock cloud straddles the future initiation point from the first hours (median distance 3.5-5 km throughout, vs ~4.5 km foreshock-mainshock separation), so the registered bulk-migration/proximity statistics have little dynamic range here — and what motion there is points away, not toward. The pre-M6.5 background (21 h) shows nothing at the future site (near-fraction 0.0 in every pre-foreshock bin). Kato et al. 2016's documented northeastward migration is a relocated, along-strike *front* inside the zone (cross-correlation relocation, ~km precision) — it is not recoverable as bulk approach at automated single-event location precision. Combined conclusion across both cases: one offshore (Tohoku, resolution-limited) and one fully onshore (Kumamoto, resolution removed as an excuse) — none of the pre-registered prospective aggregate criteria (rate excess over Omori, bulk migration, proximity concentration) survive detection-uniform catalogues. Bulk micro-seismicity aggregates do not prospectively mark the coming mainshock; whatever discriminative signal exists lives in finer-grained structure (relocated fronts, repeaters, slow-slip proxies), which is the bar any future attempt must clear.
- **Case 2 follow-up (2026-06-12): cross-correlation relative relocation does not rescue the front — third pre-registered NULL.** The bulk-aggregate null left one stated escape: Kato et al. 2016 recovered the foreshock migration only after cross-correlation relocation, as an along-strike front inside the zone. Test: a fully automated double-difference relative relocation of the 1,863-event association catalogue (per-pair waveform cc differential times: 165,993 measurements at cc≥0.70 from 17,821 event pairs, 2-8 Hz, parabolic subsample refinement with a runtime sign self-test; damped sparse least squares for per-event horizontal + origin-time corrections at fixed 12 km depth; two passes with 4xMAD outlier rejection, final residual MAD 0.027 s; 1,840/1,863 events relocated with ≥6 links, median adjustment 0.72 km). A NEW verdict script — registered before the relocation was computed — asked R1: does the initiation-ward front quartile p75(s) advance (Theil-Sen > 0 on per-2h bins, s = coordinate along the M6.5→M7.3 epicentre axis), and R2: does the final-6 h fraction within 2.5 km of the epicentre concentrate ≥1.5x over the mid-window fraction. **Result: R1 FAIL (-0.0007 km/h, flat for 28 h; bulk median drifts slightly away at -0.04 km/h), R2 FAIL (1.03x) → NULL.** Registered caveats: the relocation is horizontal-only at fixed depth (depth-dominated distances damp the horizontal partials, so moves may be under-corrected), and Kato et al.'s migration includes a downdip component this test cannot see. Bottom line for the arc: three pre-registered prospective tests (Tohoku bulk, Kumamoto bulk, Kumamoto cc-relocated front) all return null — neither bulk aggregates nor first-order relocated geometry of automated micro-catalogues prospectively mark the coming mainshock; what remains is depth-resolved relocation, repeater/template-family analysis, and slow-slip proxies.
- **Case 2 follow-up (2026-06-12): depth-resolved (3D) DD relocation — fourth pre-registered NULL; the depth-resolution escape is closed.** The third null's registered caveat was that the fixed-depth (12 km) horizontal solve could not see a downdip migration component (Kato et al.'s migration includes one). Test: the same automated DD pipeline extended to four unknowns per event (dE, dN, dZ, dT) with initial depths from the pyocto association, depth partials in the design matrix and 3D neighbour pairing (162,730 cc measurements from 16,239 pairs; two passes with 4xMAD outlier rejection, final residual MAD 0.020 s; 1,842/1,863 events relocated with >=6 links; median 3D adjustment 1.01 km, median |dz| 0.46 km; relocated depths p10-p90 = 5.3-12.1 km, consistent with the Kumamoto seismogenic layer). A NEW verdict script registered before the relocation was computed asked R1: Theil-Sen p75(s3) > 0 along the 3D M6.5 (11 km) -> M7.3 (12 km) hypocentre axis (length 4.65 km), and R2: final-6 h fraction within 4.0 km (3D) of the mainshock hypocentre >=1.5x the mid-window fraction. **Result: R1 PASS but marginal (+0.0057 km/h = ~0.16 km over 28 h, ~3% of the axis length; the bulk median moves the other way at -0.032 km/h), R2 FAIL (1.18x) -> NULL by the registered conjunction.** Depth diagnostics (report-only): median |z - 12 km| drifts slightly up (+0.018 km/h) — the cloud does not deepen toward the mainshock hypocentre. Bottom line for the arc: four pre-registered prospective tests (Tohoku bulk, Kumamoto bulk, Kumamoto cc-relocated front, Kumamoto depth-resolved 3D front) all return null — automated micro-catalogue geometry, horizontal or depth-resolved, does not prospectively mark the coming mainshock; what remains is repeater/template-family analysis and slow-slip proxies.
- **Case 2 fifth test (2026-06-13): repeater/template-family analysis as a slow-slip proxy - fifth pre-registered NULL; the provisional PASS was another sensitivity artifact.** If an aseismic slow-slip transient drove the foreshocks (the Kato et al. 2016 GRL interpretation), repeating-earthquake families (re-ruptures of the same patch loaded by the surrounding creep) should over-produce against their own Omori decay (Q1), shorten their recurrence intervals as the creep accelerates (Q2) and migrate toward the mainshock (Q3) even where the bulk catalogue does not. Families = connected components of event pairs with max normalized cross-correlation >= 0.95 on >= 2 common stations (10 s vertical-component windows, 2-8 Hz, P-anchored, 24,972 candidate pairs measured) over the 1,863-event association catalogue: 20 multi-member families, 41 repeater events; a verdict script with the three criteria above was registered before any family was computed. On the raw association catalogue the verdict was Q1 FAIL (1.76x), Q2 SKIP (only one family with >= 3 members), Q3 PASS (Theil-Sen median -0.156 km/h, p25 -0.097 km/h) - the first registered criterion in the whole arc to pass. Per the procedure established a priori on the Tohoku episode (final verdicts are taken on a detection-sensitivity-equalized catalogue; provisional association-catalogue verdicts are artifact-prone), the 41 family members were then used as matched-filter templates - parameters verbatim from the case-2 matched filter, zero new tuning; 41/41 self-detections, +156 new family members, 197 repeater events; the 4 s dedup against existing association events can only shrink families, so the equalization is conservative toward PASS. The registered verdict on the equalized catalogue: **Q1 FAIL (1.48x), Q2 FAIL (median recurrence-interval slope +0.012 log10(s)/h with only 40 percent of families negative - intervals lengthen, i.e. ordinary afterslip decay, no creep acceleration), Q3 FAIL (per-2h-bin median slope 0.000 km/h over 14 well-populated bins - the families sit ~7 km from the mainshock epicentre, flat, for the whole 28 h) -> 0/3.** Report-only: only 1 of 20 families first activates in the final 12.4 h; final-6 h near-fraction (5 km) = 0.07. The fifth null closes the slow-slip-proxy bar: on onshore Hi-net data with automated micro-catalogues, none of bulk statistics, relocated geometry (horizontal or depth-resolved 3D) or repeater/template families prospectively mark the coming mainshock.
- **Case 2 sixth test (2026-06-13): the magnitude (b-value) channel - Gulia & Wiemer's own case - retrospective low-b asperity, no prospective temporal precursor.** Gulia & Wiemer (Nature 2019) argued, on this exact M6.5->M7.3 sequence with the JMA catalogue, that the b-value of the M6.5 aftershocks dropped enough to flag the M6.5 as a foreshock in real time (a "red light"). Tested on the independent, denser micro-catalogue using network relative magnitudes (per event: station-corrected median of log10(peak 2-8 Hz amplitude) + 1.73*log10(hypocentral distance); 23,286 amplitude measurements across 19 stations, 1,863 events, slope-1 so b is on an ML-comparable scale). The completeness cutoff was made robust against the PASS-direction artifact of late-window sensitivity loss: Mc* = max(per-4 h-bin maximum-curvature Mc) + 0.1, so no window uses a laxer threshold than the worst bin. A verdict script with three criteria - B1 G&W-style drop vs background, B2 b decreasing as the mainshock approaches, B3 the near-hypocentre patch being lower-b than its ring - was registered before any magnitude was measured. Judge v1 returned INSUFFICIENT DATA: short-term aftershock incompleteness in the first 4 h after the foreshocks (per-bin Mc 0.83 then -0.07 to -0.55) drove Mc* to 0.93 and left only 254 events. A v2 judge applying the textbook a-priori STAI correction (exclude the first 4 h; statistics byte-identical, registered before any b-value was seen) gave: **B1 SKIP** (zero associated events at the site before the M6.5 - a 50 h window has no regional/decadal background, so G&W's specific background-drop test is unmeasurable here), **B2 SKIP** (only two 4 h bins clear the n>=50 gate above Mc*, and they trend b 0.70 -> 0.81, i.e. the *opposite* of the predicted pre-mainshock drop), **B3 PASS** (b_near = 0.76 +/- over dist <= 5 km vs b_ring = 0.86 over 5-15 km, ratio 0.886 <= 0.90; robust to a +0.2 cutoff shift at ratio 0.888 - though B3 is the one criterion carrying a known S-window distance-coupling caveat and the margin is thin). The reading: the future rupture-initiation zone is identifiable as a low-b patch *after the fact*, but the genuinely prospective, temporal precursor that would constitute a real-time red light is absent - in an independent dense micro-catalogue the b-value channel adds a retrospective spatial asperity, not a forecastable signal, even on the case where it was claimed to work in real time. B3 was then put to a registered PROSPECTIVE test: build a b-map of the pre-mainshock zone and ask whether the patch that will host the M7.3 nucleation ranks in the lowest-b quartile among ALL active patches, without using the known epicentre. A coarse 0.04 deg grid was INSUFFICIENT (the compact foreshock zone gave only 5 complete cells, below the registered 6-cell minimum), so it was redone with a continuous nearest-neighbour b-field (standard b-mapping, 50 nearest events per node, no cell-count gate, registered before computing). The mainshock location then ranks at field percentile 0.62 - ABOVE the median b, with 62% of active patches lower-b - a clear **FAIL** (robust across 30/50/80 nearest neighbours: percentile 0.52 / 0.62 / 0.28, all above the 0.25 threshold). So B3's retrospective low-b contrast is an artifact of the specific <=5 km vs 5-15 km partition and does NOT survive a blind prospective ranking: the b-value channel yields no prospective signal even in the spatial dimension where it looked positive. A web check also confirmed no viable third onshore case - the 2000 Western Tottori M7.3 had essentially no foreshocks, and rich spatially-extended onshore foreshock sequences in the Hi-net era are effectively unique to Kumamoto - so this settles the b-value channel on the one catalogue dense enough to test it.
- **Seventh test (2026-06-13): the below-catalogue waveform channel - masked-residual tremor / emergent energy.** Every prior test reduced the waveforms to a PhaseNet/PyOcto impulsive micro-catalogue; this one leaves it. The hypothesis (Kato et al. 2016: slow slip drove the foreshocks) is that aseismic tremor - the direct waveform signature of slow slip, absent from earthquake catalogues - should mark the nucleation. The 19 in-box stations' 2-8 Hz envelopes were masked around every cataloged pick (+-3/+25 s, 74% kept) and the non-impulsive coherent residual was scored in 5-min windows (cross-station envelope cross-correlation), giving 343 tremor windows. A verdict registered before scoring asked: T1 emergent rate RISES toward the mainshock (and against the Omori-decaying impulsive background, so a rise cannot be leaked coda), T2 the energy migrates toward the epicentre, T3 it concentrates within 10 km in the final 6 h. Result: **T1 FAIL** (tremor rate peaks mid-sequence after the M6.4 and decays toward the mainshock, like the impulsive activity), **T3 FAIL** (final-6 h near-10 km fraction 0.29, near-5 km 0.03), **T2 PASS** (amplitude-weighted centroid migrates inward at -0.37 km/h, opposite the catalog's +0.03 km/h expansion) - but a registered control exposed T2 as an artifact: INCOHERENT windows (coh < 0.4) migrate identically (-0.355 vs -0.365 km/h), so the inward drift is a generic amplitude-weighted array-aperture effect, not a tremor signal. Net 0/3: the first move outside the impulsive catalogue also yields no prospective marker - even the below-catalogue emergent-energy channel does not mark the coming Kumamoto mainshock.
- **Eighth test (2026-06-13): a qualitatively different observable - seismic-velocity change (dv/v).** All seven prior tests measured earthquake-occurrence statistics (rate, location, magnitude, family, emergent energy); this one measures the elastic property of the medium itself. Stress build-up, micro-cracking and fluid migration before failure are expected to change crustal velocity (dilatancy/damage models; Brenguier et al. 2008), independently of the seismicity. Per in-box station the continuous 2-8 Hz record was 1-bit normalized (to suppress the abundant transient foreshocks), autocorrelated in 30-min windows, and dv/v estimated by the stretching method against an all-window reference; 19 stations gave 82 qualifying windows over [M6.5, M7.3). A verdict registered before computing asked: V1 a significant dv/v trend toward the mainshock (Theil-Sen change beyond the inter-station noise), V2 a significant velocity step in the final 6 h. Result: **V1 FAIL** (Theil-Sen 0.000 %/h, the series is flat), **V2 FAIL** (final-6 h mean -0.025% vs baseline +0.000%, far below the 0.13% inter-station noise) - 0/2. The medium's velocity shows no resolvable change toward the Kumamoto nucleation within a 28 h single-station autocorrelation measurement (caveat: dv/v monitoring usually needs days, and single-station ACF senses only near-station scattering). The first qualitatively different observable - the elastic medium rather than the seismicity - is also null. This refines rather than overturns the arc conclusion: aggregate occurrence/geometry/family statistics do not prospectively mark the mainshock, and the one spatial b-value contrast that survives is retrospective.
- **Ninth test (2026-06-14): a learned (self-supervised) waveform representation, not hand-crafted statistics.** All eight prior tests reduced the waveforms to hand-engineered statistics (rate, location, magnitude, family, emergent energy, dv/v). This one asks whether a *learned* representation of the raw foreshock waveforms - the information the micro-catalogue discards - marks the nucleation. A SimCLR contrastive encoder (augmentations: per-channel amplitude scale, time jitter, station dropout, making the representation amplitude/coverage-invariant) was trained EARLY-ONLY (first 30% of the 28 h window) on event-aligned 6 s windows from 8 core Hi-net stations (1,797 Kumamoto micro-events), then each event embedded. Confound control (the detection-rate rise the whole arc has fought): event-aligned (one embedding per event = rate-free), M- and depth-matched binning, and a triple control - C1 time-shuffle, C2 surrogate-prospective (the within-early intrinsic drift), C3 spatial null (near vs far from the rupture). A verdict registered before any embedding was computed asked whether the embedding centroid DRIFTS toward the mainshock surviving all three controls. Result: a late drift rise does exist (P2 1.80x) and survives the time-shuffle (C1) - the strongest apparent signal of the whole arc - but **C2 FAIL** (the late slope 0.0034/h is slower than the within-early intrinsic encoder drift 0.0106/h, a generic temporal/encoder-fit effect), **C3 FAIL** (the drift does not localise to the rupture; far events drift as much as near), **P1 FAIL** (non-monotone). **VERDICT NULL** - exactly the failure mode the multi-control design was built to expose: C1+P2 alone would have been a false detection. An independent audit reproduced the verdict (no bug) and confirmed the bias-free C3 spatial null is what carries it (C2 has a known FAIL-direction bias from early-only training). Even a modern learned representation of raw onshore waveforms does not prospectively mark the Kumamoto nucleation.
- **Tenth test (2026-06-14): multi-case extension - the same SSL probe on Iquique 2014.** Generalising beyond a single case: the 2014 Iquique Mw8.1 is a near-positive control - a foreshock migration toward the rupture was *reported* (Kato & Nakagawa 2014, with cGPS + relative relocation), yet the automatic micro-catalogue here shows none (day vs median-distance corr +0.104). The identical event-aligned SimCLR + triple-control harness was applied to 4,463 Iquique micro-events (6 core CX/IPOC stations, 16-day [Mw6.7 foreshock, Mw8.1) window). **VERDICT NULL, and cleaner than Kumamoto**: P1/P2/C1/C2 all FAIL - there is essentially no temporal drift toward the mainshock at all (slope ~0, the final drift even decreases); only C3 passes (a faint near>far spatial localisation, 0.00057 vs 0.00017 per hour) but with no temporal trend it is not a detection. Across two tectonic settings - Kumamoto onshore crustal and Iquique offshore megathrust (with a literature-reported migration) - the event-aligned self-supervised waveform embedding does not prospectively mark the independent mainshock, and at Iquique does not recover the migration that geodesy + relative relocation revealed. The reported nucleation signals live in observables this representation does not access.
- **Eleventh test (2026-06-14): the geodetic channel itself - high-rate GNSS.** The ten waveform tests all concluded the precursor must live in an observable the seismometer waveform does not access - geodesy above all, since Kato et al. (2016) inferred slow slip drove the Kumamoto foreshocks. This test takes that channel directly with NGL 5-minute kinematic GNSS (IGS20/kenv, open, no credentials): 105 stations including the two GEONET sites on the rupture (G071 5.3 km, J465 9.7 km), quiet-day horizontal precision ~5-7 mm. A verdict registered before the window residual was seen asked whether an aseismic net horizontal transient over the 28 h [Mj6.5, Mj7.3] window (secular-detrended, coseismic steps removed) exceeds the per-station baseline 95th percentile across the near field (G1), decays with distance (G2), is absent in far-field controls (G3 null), and accelerates toward the mainshock (G4). VERDICT NULL (raw): G1 FAIL (44% of near-field exceed) and G3null FAIL - the apparent near-field transient is a network common-mode floor (~13 mm appears even 600-900 km away, far_Aw 13.2 ~ near_Aw 14.6 mm), not localized. A pre-registered common-mode-filtered variant (subtract the per-epoch 80-1000 km regional-ring median) is NULL and cleaner (G1 33%, controls exceed at 71%). The two closest stations do carry a marginal ~15-20 mm window transient above their own baseline, but it is not a spatially-coherent, control-surviving localized field. At GEONET 5-min kinematic precision the Kumamoto pre-mainshock aseismic transient is not prospectively resolvable above the common-mode/noise floor - consistent with the inferred slow slip being at or below geodetic resolution (it was inferred from seismicity, not measured as a clear geodetic transient). The geodetic channel the waveform arc pointed to also does not prospectively mark the nucleation. A third, best-chance method confirms it: a forward elastic-dislocation inversion (validated Okada85), grid-searched over plausible nucleation-patch geometries and controlled against the same search on baseline windows, explains only 12% of the window net-displacement field - below the 17% median (and 30% 95th-percentile) it extracts from quiet baseline windows, so even an optimally-placed slip model finds the pre-mainshock field less slip-like than baseline noise. The geodetic channel is null across model-free and physical-model methods alike. **A positive control bounds what this means.** Applying the identical harness to the 2014 Iquique Mw8.1 sequence - where Ruiz et al. (2014, Science) reported a geodetically observed slow-slip transient in cGPS over the ~16-day foreshock window - the method is also null on all three variants (the forward megathrust inversion explains only 6% of the window field, below the 23% baseline median; the near-field transient does not even increase toward the offshore rupture). The positive control therefore FAILS: NGL 5-min kinematic prospective net-displacement and inversion do not recover a documented geodetic slow slip. The Kumamoto geodetic null is consequently sensitivity-bounded, not a demonstrated absence - few-mm precursory transients sit below the floor set by 5-7 mm kinematic scatter plus regional common-mode, and the published Iquique precursor required bespoke processing (station selection, transient-time-function modeling, longer-baseline cGPS) beyond this reproducible pipeline. The honest, generalizable result is about method reach: an open, cheap prospective pipeline on 5-min kinematic GNSS does not resolve nucleation-scale slow slip in either case. A synthetic-injection characterization makes this quantitative: injecting known megathrust slip into the real Iquique residuals, the pipeline's detection floor is a uniform thrust slip of ~Mw 6.5 (baseline median) to ~6.9 (actual window) - right at Ruiz et al. (2014)'s ~Mw 6.5 precursory estimate, so the documented slow slip sits at or below what the reproducible pipeline resolves. The geodetic null is thus a measured upper bound, not an unexplained absence: open 5-min kinematic prospective detection has a ~Mw 6.5 floor at these station geometries, and nucleation-scale precursors live below it. The symmetric characterization at Kumamoto - dense onshore stations 5-60 km from a strike-slip rupture - gives a tighter floor of ~Mw 5.6 (baseline) to ~5.9 (window), so the better network geometry buys about one magnitude unit of sensitivity; the Kumamoto precursory slow slip, inferred from seismicity (Kato et al. 2016) and never resolved geodetically, lies below this bound. Across both cases the geodetic null is a quantified detectability limit - Mw ~5.6-6.9 depending on station geometry - with the documented/inferred precursors sitting beneath it. Forward-modelling the floor for hypothetical networks (geometry exact via Okada85, calibrated on the two measured floors) turns the null constructive: for the offshore megathrust, seafloor GNSS-A above the rupture raises network sensitivity 3.6x and, with a daily-cGPS-level noise reduction, drops the floor to ~Mw 5.8 - below the ~Mw 6 inferred precursor, so the documented Iquique slow slip WOULD be prospectively detectable with seafloor geodesy; onshore, the dense Kumamoto network already reaches ~Mw 5.3-5.6 and only needs a lower-noise product. The negative result thus specifies the observing system that would make prospective geodetic nucleation detection feasible (seafloor instrumentation offshore, low-noise/dense processing onshore), rather than the open 5-min onshore pipeline tested here.
- **Twelfth test (2026-06-15): seafloor verification of the obsdesign prediction — real ocean-bottom-pressure data, the 2018 Boso slow-slip event.** The eleventh test's constructive endpoint was that *seafloor* instrumentation above an offshore source would lower the detection floor to ~Mw5.8. Tested directly on the 2018 Boso SSE (Mw 6.4-6.6, slip on the Philippine Sea plate interface at ~10-20 km; ground truth = Sato et al. 2024 GRL), using the only openly-available OBP records (Sato 2024, Mendeley Data `10.17632/fb9sn2zcx8.2`: 4 gauges BOSO1/BOSO2/KAP2/KAP3, hourly pressure+temperature, 2016-08 to 2018-09, covering the June-July 2018 event; DART = dead end, NIED S-net/DONET pressure channels absent). Three reproducible-pipeline approaches, each with a synthetic-injection / baseline-window control: **(1) OBP-only detiding + multichannel SSA** — a spatially-coherent SSE uplift is confounded with the coherent non-tidal ocean variability (Kuroshio-Extension mesoscale, ~16 mm/day detided residual) and removed together; the injection control proves it, an injected uniform uplift ramp of *any* size up to 80 mm is erased by MSSA (post-MSSA SSE-window percentile flat at 22-43% regardless of amplitude, vs 88->100% on the raw detided series). **(2) OBP + an open global ocean model (HYCOM GOFS 3.1, 1/12°)** — a model bottom-pressure proxy (mass component ρg(SSH - steric), EOS-80, from NCSS point time series at the 4 gauges) has the right amplitude (proxy std 1.4-2.3 vs observed 1.5-1.7 hPa) but correlates only r ~ 0.1-0.3 with the observed OBP (even 21-day smoothed), so regressing it out removes essentially nothing (NET percentile 87.6 vs 88.4 raw): a global 1/12° model does not reproduce the coastal Kuroshio-Extension bottom pressure. **(3) A vertical Okada matched filter** — projecting the 4-gauge field onto the physics-based vertical-displacement pattern of a Boso-interface thrust (validated Okada85), controlled against baseline windows and a clean control-epoch injection, gives a detection floor of ~Mw6.5 (slip ~0.2 m for a >94th-percentile step at a control epoch; central-geometry observed-SSE percentile 89, best-of-9-geometries 96.5 = marginal after multiple comparison). **Conclusion: even a known, well-documented seafloor SSE is not recoverable with an open/reproducible pipeline (open OBP data + open global ocean model); the detection floor is ~Mw6.5, and the 2018 Boso event (Mw6.4-6.6) sits right at it — a persistent but non-significant hint across all methods.** Sato et al.'s published detection required a regional 2-km data-assimilative ocean model (MOVE/MRI.COM-JPN, access-restricted; DIAS auth server unreachable + JMA-consent terms). This refines the obsdesign prediction: reaching ~Mw5.8-6 offshore needs BOTH dense seafloor instrumentation AND a high-resolution regional assimilative ocean model for noise removal — the binding constraint is an infrastructure/access barrier, not station geometry alone. It is the third instance (after the Iquique GNSS positive control) of a published detection depending on bespoke/restricted ingredients beyond an open pipeline.
- **Thirteenth test (2026-06-15): multi-case detection-floor vs network curve.** The twelfth test's floor was a single seafloor case; this generalizes the floor across four subduction settings and two modalities with one reproducible harness (NGL 5-minute kinematic GNSS, `IGS20/kenv`, open; plus the Boso OBP). Method hardened after adversarial review: per-epoch network common-mode removed, the SSE window masked from the detrend fit, the step window matched to each event's duration, and detection taken as a two-sided 95th-percentile of |step| cross-checked against a 1.96-sigma analytic floor (the matched-filter output is slip-equivalent, so its baseline std *is* the noise floor) and a clean synthetic-injection ROC at a quiet control epoch; Mw via M0=mu*A*slip on the documented fault area. Cases (N stations, documented Mw, analytic floor Mw, realized detection): Boso 2018 onshore GEONET (19, Mw6.5, floor 5.3, pct100/SNR58 - detected); Cascadia 2012 ETS PBO (34, Mw6.7, floor 6.1, pct97/SNR2.5 - detected); Guerrero 2009-10 (6, Mw7.5, floor 6.4, pct88/SNR3.5 - marginal, only 1 of 6 open-holding stations near-field); Hikurangi 2014 Gisborne GeoNet (28, Mw7.0, floor 6.0, pct10/SNR0.2 - NOT detected); Boso 2018 seafloor OBP (4, Mw6.5, floor ~6.3 vertical, pct89 - marginal). **The noise-limited analytic floor is Mw ~5.3-6.4 (lowest for dense low-noise onshore networks, highest for sparse geometry), but realized detection of the documented event decouples from it and is governed by near-field coverage of the slip patch.** Hikurangi is decisive: its Mw7.0 SSE is invisible (pct10) despite the second-lowest noise floor, because the slip is shallow and offshore and no onshore station overlies it - a low noise floor is necessary but not sufficient. **Conclusion: detection requires BOTH a low noise floor AND near-field coverage; offshore shallow sources defeat onshore GNSS regardless of its noise floor, which is the quantitative case for seafloor geodesy - but seafloor OBP carries its own ocean-noise floor (~Mw6.3).** This unifies the onshore-GNSS nucleation arc and the seafloor-OBP arc into one network-design curve and corrects the earlier 'noise environment, not offshore-ness' framing: offshore-ness matters precisely through near-field coverage. See `research/nucleation/RESULT_floor_curve.md`. **Fifth plate boundary added (Nicoya, Costa Rica 2007 SSE, Cocos-Caribbean):** the compact onshore peninsula network sitting directly over the slip cleanly detects the Mw~6.6-7 SSE (pct100, SNR 10.2, spatial-pattern r=0.87 with a shallow updip patch, permutation p<0.003, peak at the documented May 2007) - the detected onshore-over-slip counterpart to the Hikurangi offshore null, with a floor of Mw~5.4-5.7 (among the lowest, because stations overlie a shallow updip patch). A first attempt with a wrong deep-patch geometry produced a CMC-off false positive (step in July, spatial r=0.09) that the temporal+spatial verification caught and corrected - kenv and tenv3 both detect once geometry is right, so the cause was geometry, not data product. **Spatial-pattern audit of the whole curve** (per-station displacement vectors vs predicted Okada pattern, permutation null) then validated every detection: Boso (r=0.81), Bungo (0.91), Manawatu (|0.64|), Nicoya (0.87) and Guerrero (0.69) are all spatially confirmed (permutation-significant), and the Hikurangi null is confirmed at the pattern level (r=-0.12, p=0.76 - genuinely invisible onshore, not merely low-SNR). One honest correction: the 2012 Cascadia onshore-GNSS case (pct97/SNR2.5) fails the spatial test (best grid-search |r|=0.61, p=0.27) and is downgraded to spatially-unconfirmed - though the same event is detected at 8 sigma by the strainmeters here, so a real event that onshore GNSS cannot resolve but a lower-floor modality can.
- **Fourteenth test (2026-06-16): borehole-strainmeter modality - overturns the earlier strainmeter dead-end.** A third independent modality joins the floor-vs-network curve, using PBO/NOTA borehole strainmeters (BSM, public NCEDC level-2b) on the 2012 northern Cascadia ETS (PNSN 2012-07-30 to 10-12, a ~2.5-month rupture migrating from Puget Sound to Vancouver Island, geodetic Mw~6.8). An earlier probe had declared BSM unusable because the processed `dtc` field is over-smoothed into a featureless line. That was a processing artifact. The fix is the raw calibrated `s` field, daily-median (which removes the tides), with our own SSE-window-masked detrend so the slow transient survives. Method: an inverse-variance-weighted Okada strain-tensor matched filter over the areal and two shear channels of 9 stations. Shear is about 7x cleaner than areal, and common-mode removal is deliberately *not* applied because the spatially coherent ETS strain would be removed as common mode. The migrating rupture defeats a stationary window-mean statistic (only pct 87), so a peak statistic is used (specified by the event class, with the null carrying the look-elsewhere penalty), giving **detection at pct 100 / SNR 8.2**. Single-station shear confirms (B004 5.8 sigma, B928 5.2 sigma), and the result is robust across shear-only and quadratic-detrend variants (SNR 8.2-9.0, floor Mw 5.75-5.78). **At identical source geometry the BSM network detects about 2.4x smaller slip than the GNSS network (floor Mw ~5.78 vs ~6.0), about 0.25 Mw lower** (an earlier loose "0.5-0.8 Mw" figure was geometry-confounded and is retracted). This extends the curve to three modalities (GNSS, seafloor OBP, strainmeter) and reinforces the central thesis - the migrating rupture breaks the stationary matched filter even though its noise floor sits well below the event, so realized detection stays template- and geometry-limited. See `research/nucleation/RESULT_floor_curve.md`. **Hardened across three independent ETS years** (2010, 2012, 2013) with a window-agnostic whole-year scan - all detected at pct 100 with the blind peak landing inside each year's independently-documented PNSN ETS season (Aug 29 / Sep 12 / Oct 5), floor robust at Mw ~5.8-6.2, turning the single positive control into a three-event series.

- **Fifteenth test (2026-06-17): InSAR modality — open Sentinel-1 does not detect the 2018 Boso SSE; a troposphere-limited floor.** A fourth independent modality joins the floor-vs-network curve: Sentinel-1 InSAR (COMET LiCSAR unwrapped interferograms + LiCSBAS SBAS time series + open GACOS troposphere; descending frame `046D_05469_071311`, 263 interferograms / 74 epochs over 2017–2019, multilooked 10×10), applied to the *same* 2018 Boso SSE the curve detects with onshore GNSS (SNR58) and marginally with seafloor OBP. Ground truth = Honda et al. (2024, GJI), who detected a −1.34 cm descending-LOS step but only with numerical-weather-model (NWM) assistance and three tracks. Detector: the same validated-Okada85 matched filter as the GNSS/OBP cases at the identical fault geometry (140.8°E/35.1°N, 15 km depth, strike 190 / dip 15, 40×40 km, rake 90), projecting the Okada displacement to line-of-sight via the `cum.h5` E/N/U unit vectors, with a planar (deg-1) deramp applied *consistently to both data and template* to control orbital/tropospheric ramp aliasing, plus a block-bootstrap permutation null, a baseline-window distribution, and a multi-window synthetic-injection floor. **Result: NOT detected.** With GACOS + consistent deramp the matched filter gives r = −0.03 (p = 0.53). The only apparent detection — the uncorrected, GACOS-off field at r = 0.58 (p < 0.001) — collapses to r = 0.06 (p = 0.11), and its slip amplitude from 5.05 m to 0.17 m, under the same planar deramp: it is an orbital/tropospheric plane aliasing onto the broad Okada gradient, not signal (the Okada template retains 48% of its RMS through the deramp, so the test is fair — the surviving non-planar component simply is not present in the data). The multi-window injection floor on the defensible GACOS+deramp configuration is **Mw ~6.8 (median, range 5.7–7.0)** — above the catalogue Mw6.5, so the event sits below the open-pipeline floor. **Mechanism: the LiCSBAS time-series residual is ~15 mm, which buries the ~13 mm peak SSE LOS signal (SNR ~1)** — exactly why Honda et al. needed NWM correction (residual 1.3 mm) and three tracks. **The contrast with GNSS is the transferable result: the same onshore footprint is cleanly detected by GNSS (SNR58, floor Mw~6.1), so InSAR carries a ~0.7 Mw higher floor here despite millions of pixels — a per-measurement-noise (tropospheric) penalty, not a coverage one. Spatial density does not lower the detection floor when each pixel's troposphere noise exceeds the signal.** Scope: a single descending track with open GACOS only; the ~0.7 Mw penalty is an upper bound on the open/free, single-path case (Honda et al.'s NWM + 3-track processing closes most of it), and the ~52% of the SSE template lying in the ramp-degenerate planar subspace is non-identifiable from an orbital ramp without an independent troposphere constraint, rather than proven absent. This is the fourth constructive null (after the Iquique-GNSS and Boso-OBP positive controls): a documented signal an open/reproducible pipeline cannot recover, with the binding limit — here the tropospheric residual — quantified. See `research/nucleation/insar_boso.png`. **Cross-check on a large SSE (Guerrero, Mexico 2017–2018, Mw~7.5, which Maubant et al. (2020) detected with Sentinel-1 + Independent Component Analysis source separation):** the same open matched-filter pipeline on descending frame `041D_07134_222019` (275 interferograms, but only 31 epochs survive SBAS over the less-coherent Mexican forearc) also fails to cleanly recover it — the spatial correlation with the Okada interface pattern is weak even before deramp (r = 0.10 GACOS / 0.13 noGACOS; the Okada template retains 94% of its RMS through the deramp, so the test is fair), and the high noGACOS-RAW percentile is amplitude (orbital/atmospheric) rather than a pattern match. Here the binding limit differs from Boso: not the noise floor but source separation — a broad SSE is degenerate with orbital ramps, atmosphere, and the 2017 Mw8.2 Tehuantepec / Mw7.1 Puebla earthquakes' postseismic, which is exactly why Maubant et al. needed ICA. **Together the two cases bound the open, simple matched-filter InSAR modality: it does not cleanly detect SSEs — small/compact events sit below the ~Mw6.8 tropospheric floor (Boso), large/broad events are not separable from ramps without source separation (Guerrero) — while the same Okada matched filter detects both regimes cleanly with GNSS (per-station mm noise). Spatial density is not the binding variable; per-measurement noise and signal-separability are. Both published InSAR detections required bespoke processing (NWM, ICA).**

- **Sixteenth test (2026-06-19): the offshore near-field — S-net seafloor seismometers directly above the source.** The eleven-null arc's own diagnosis was that onshore arrays cannot see offshore nucleation (near-field coverage). This test changes the observing system to S-net (NIED's ~150-node seafloor array on the Japan Trench), applying the SeisBench PhaseNet + pyocto micro-catalogue pipeline (acceleration code 0120A, seafloor-station depths, P-dominant association since accelerometer S-picks are sparse) to the three largest S-net-era offshore earthquakes: M7.4 Miyako-oki (2026-04-20, final 24 h), M7.6 Aomori-oki (2025-12-08), M7.1 Fukushima-oki (2021-02-13). **All three are NULL** — no accelerating or migrating foreshock cascade at the hypocentre, confirmed in both the associated micro-catalogue and the association-independent raw near-source pick rate (the station 10-13 km directly above each source is flat right up to the mainshock). In every case the pipeline detects the contemporaneous distant regional swarm and near-source events absent from the global USGS catalogue, bounding completeness below the regional USGS Mc (~M3); nucleation, if it occurs, is below ~M3 and below the resolution of seafloor near-field coverage directly above the source. The offshore near-field NULL generalises across the trench, extending the arc from onshore to the offshore near field. Detail: `research/nucleation/RESULT_snet_m74.md`.

- **Seventeenth test (2026-06-22): onshore high-density Hi-net dense-array matched-filter — the deepest open-data injection floor, and a power-calibrated null on foreshock-rate acceleration before Kumamoto M6.5.** The S-net arc was first pushed below catalogue completeness by template matching (obspy network CC-sum + synthetic-injection floor) on the same three offshore events, taking each NULL ~1.0–1.5 magnitude below Mc and verified by an aftershock positive control (post-M7.4 6 h: 2,327 PhaseNet picks vs 264 in the 24 h pre-mainshock, ~35×, so the picker has clear sensitivity to a real cascade and the dispersed below-Mc null is a true absence; `RESULT_snet_m74.md`). This test moves the same methodology onshore to the densest open array — Hi-net (~10× S-net station density) — on the 2016 Kumamoto M6.5 foreshock (24.4 h run-up + 5.5 h post; continuous velocity, code 0101; stations within 60 km; 16 PhaseNet-template events; dataset `yasunorim/kumamoto-hinet`, 1,518 SAC). Two results. **(1)** The **self-injection detection floor reaches dM = −5.0** below the M6.5 template (100% recovery to dM−4.5, 62% at dM−5.0) — ~ML 1–2 equivalent under uniform amplitude scaling, the deepest injection floor anywhere in the arc (S-net offshore reached dM−1.5); it is a self-template upper bound, so the floor for real, geometry-mismatched micro-events is shallower. **(2)** Unlike the quiescent S-net cases, the M6.5 sits inside an **active foreshock sequence** — 175 catalogue foreshocks in the 24.4 h window — so the test asks "does the rate accelerate (end-load) toward the mainshock?" The powered primary test — last-3 h / last-6 h event count (merged catalogue + matched-filter detections, 183 events) versus a uniform-reshuffle null (binomial + permutation) — puts the run-up **at or below the stationary expectation**: last-3 h obs 22 vs exp 22.5 (p = 0.58), last-6 h obs 35 vs exp 45 (p = 0.97). The test is self-calibrated by injecting synthetic accelerating cascades into the real background: it detects an **inverse-Omori-type cascade (final-12 h concentration, p≈1) of ≥20 events with power ≥0.98** — so this is a **power-calibrated null, not a null of no power** (the calibration is for that single acceleration shape, not arbitrary nucleation signals). A binned Spearman trend (rho = −0.147) was demoted to secondary after the same injection check showed it underpowered (even n=80 injected events reach only p=0.07), and the weak cumulative-N(t) convexity is early-window, not end-loading. The foreshocks are a stationary swarm, not an accelerating nucleation cascade. This and the S-net cases are complementary regimes — "no cascade from a quiescent background" (offshore) and "no rate acceleration within an active swarm" (onshore) — that jointly support the absence of a detectable nucleation-acceleration signal at open-data resolution. Single-event result; generalisation needs more cases or denser/borehole data. Detail: `research/nucleation/RESULT_hinet_kumamoto.md`. Opus-reviewed (methodology consult + sign-off).
- **Eighteenth test (2026-06-23): onshore high-density case #2 — 2019 Ridgecrest M7.1 (SCSN), the same powered null, now robust to an Omori-decaying baseline.** The Kumamoto M6.5 powered null was a single event; this ports the identical matched-filter + injection-floor + powered last-Xh methodology to the best-instrumented foreshock sequence in the literature, on the dense Southern California Seismic Network (SCEDC open data, 17 broadband HHZ stations within 68 km, 12 h pre-M7.1 + 1 h post). Huang et al. (2020 EPSL) describe the foreshocks as a cascade to failure but conclude time-to-failure slip acceleration is largely ruled out, and report the M7.1 nucleated in a concentration that intensified ~3 h before — exactly the end-loading question. The 12 h run-up is far denser than Kumamoto (10,025 PhaseNet picks, 1032 merged events vs 183) because it sits on the decaying M6.4 aftershock background. **Result: no statistically significant end-loading against a stationary null (last-3 h obs 267 vs exp 258, p=0.27; last-6 h p=0.24) NOR against an Omori-conditioned null fitted on the early window and extrapolated (predict-late last-3 h Poisson p=0.23, last-6 h p=0.15) — the powered null is robust to baseline shape.** A slight non-significant late uptick (88 vs 84/h, cumulative-N convexity +0.348, Spearman p=0.17) is the open-data shadow of the reported ~3 h intensification, detectable only at higher injected cascade size (n>=30-50) than Kumamoto (n>=20) because the ~6x denser M6.4 aftershock background raises the floor. Self-injection floor dM-4.5 (self-template upper bound). Pre-mainshock templates were capped at 60 to bound memory (the uncapped 669-template run OOM-killed at ~24 GB); a cap-bias check confirms the 83 new matched-filter detections are not end-concentrated (15 of 83 in the last 3 h), so the catalog-dominated rate conclusion is cap-robust. Two onshore high-density cases (Kumamoto M6.5, Ridgecrest M7.1) now both return power-calibrated nulls on nucleation-style acceleration. Detail: `research/nucleation/RESULT_ridgecrest_m71.md`. Opus-reviewed (conditional sign-off; the A6 Omori-null decision rule, last-Xh p>0.05, is met).
- **Nineteenth test (2026-06-23): the magnitude (b-value) channel on Ridgecrest — the foreshock-traffic-light showcase of Gulia and Wiemer (2019, Nature) is not reproduced with independent magnitudes.** Gulia and Wiemer featured the 2019 Ridgecrest M6.4 to M7.1 sequence as a real-time RED light (after the M6.4 their FTLS turned red; the M7.1 followed about 34 h later), but Dascher-Cousineau et al. (2020) and Gulia and Wiemer (2021) showed the Ridgecrest FTLS is highly sensitive to expert-judgment parameters. This reproduces the temporal FTLS with an INDEPENDENT network relative-magnitude catalogue (station-corrected log10 peak 2-8 Hz amplitude from 17 SCSN HHZ stations over the full 35 h M6.4-to-M7.1 sequence; 1950 PhaseNet events; compact source zone so the per-station median absorbs the common distance term), mirroring the sixth (Kumamoto) b-value test which found that channel retrospective-only. **Result: no prospective foreshock b-value drop before the M7.1.** The relative-drop test (B1) is not computable (the open window opens only 1 h before M6.4, no background), as in Kumamoto. The standard maximum-curvature Aki-Utsu value (b = 0.79) cannot anchor a RED claim: it stalls at h_before about 14 h (final approach too sparse above Mc* = 1.80) and is biased low by transient post-mainshock incompleteness. The incompleteness-robust b-positive estimator (Van der Elst 2021; positive successive magnitude differences, no Mc) reaches the final approach and stays near 0.9 with no decline (whole sequence 0.91; final 12 h / 6 h / 3 h = 0.96 / 0.97 / 0.93); a 60-window moving b-positive trends slightly UPWARD toward M7.1 (Theil-Sen +0.0043/h) but the overlapping windows make that anti-conservative so no significant trend is claimed, and the endpoint contrast runs the other way (first-150 0.94 vs last-150 0.89), both within b-positive uncertainty, so the robust statement is that b stays near 0.9 with no systematic decline. The apparent FTLS red light is thus not reproduced at independent-magnitude resolution, corroborating Dascher-Cousineau et al. (2020). The spatial low-b-asperity channel (B3) is also null: PyOcto located 389 events and the nearest-neighbour b-positive field is statistically flat (observed dispersion indistinguishable from a location-shuffled null, p = 0.35), so no low-b asperity is spatially resolvable at this catalogue size (the M7.1-patch percentile 0.41 to 0.58 is unresolved-field noise), mirroring Kumamoto B3. The b-value channel is thus B1/B2/B3 complete and uniformly null. Second case on the b-value channel (n = 2 with Kumamoto); both null on a prospective temporal b-precursor. Detail: `research/nucleation/RESULT_ridgecrest_bvalue.md`. Opus-reviewed.
- **High-resolution verification (2026-06-23): the Ridgecrest nulls are not resolution-limited.** Re-testing tests 18-19 against the densest open catalogue — Shelly (2020, USGS doi:10.5066/P9JN6H0N): 34,091 template-matched, relocated, calibrated events, Mc 1.10 vs the open pipeline ~1.8 — reproduces every temporal null at 30k-event resolution: the M6.4-phase b is NOT depressed relative to M7.1-aftershocks (b+ 0.85 vs 0.87; Aki 0.72 vs 0.68, against the FTLS foreshock-b-drop premise), no prospective b-drop (Theil-Sen +0.0005/h, Spearman p=0.64), and the seismicity rate DECLINES into the M7.1 (last-3h obs 107 vs uniform exp 143, p=1.0; obs below the now-resolved fitted-Omori baseline too, Poisson p=0.999) — no end-loading. The spatial b-channel (B3) is null-consistent: the M7.1 patch sits at the low-b extreme (percentile 0.04) but the field is spatially unresolved (location-permutation p=0.98) and the patch b+ bootstrap 95% CI (0.45-0.85) includes the field median, so it is an a-posteriori sampling-noise draw, not a resolved asperity. Higher resolution converts no temporal null to a detection, so the Ridgecrest nucleation nulls are robust to resolution rather than coarse-pipeline artifacts — the denser-data escape hatch is closed for the temporal signatures. Detail: `research/nucleation/RESULT_ridgecrest_highres.md`. Opus-reviewed.
- **Twentieth test (2026-06-24): the b-value channel on the third Gulia and Wiemer showcase — 2016 Amatrice-Visso-NORCIA Mw6.5, the only showcase where the relative-drop test is computable, is null.** Gulia and Wiemer (2019, Nature) featured the central-Italy sequence (Amatrice Mw6.0 to Norcia Mw6.5) as a headline FTLS RED light alongside Ridgecrest. On the Tan et al. (2021) ML high-resolution catalogue (Zenodo 10.5281/zenodo.4736089; 894,435 events, Mc 0.20) the full FTLS is testable because, unlike Kumamoto and Ridgecrest, there is a real pre-Amatrice background and a Norcia-aftershock comparison phase. **B1 (relative drop) reproduces the RED light and then dissolves it:** the Aki foreshock-phase b = 0.72 is 27.5% below background (a textbook RED), but the incompleteness-robust b-positive (Van der Elst 2021) for the same phase is a normal 1.14 (Mc 0.20; invariant — all-magnitude 0.95, Mc 0.50 gives 1.13), so the Aki drop is the documented low-bias of MLE under the foreshock phase's time-varying aftershock incompleteness, not a real b-deficit. **B2 / operational window:** no prospective drop — the moving b-positive is flat (Theil-Sen +0.0001/day) and the short [Visso, Norcia] interval where an operational FTLS would actually run (Visso Mw5.9 is itself a foreshock 3.47 d before Norcia) is flat-to-RISING (1.09 to final-1 d 1.14 to final-0.5 d 1.21). **Rate channel:** a naive Amatrice-only Omori fit shows a huge final-day excess (predict 602, obs 4147, p 0.000), but that is entirely the Visso aftershock sequence — the physically-correct two-term Amatrice+Visso Omori superposition matches the final day to ratio 1.00 (p 0.46) and the model-free post-Visso rate profile DECAYS monotonically into Norcia (5000 to 4078/day, Spearman rho -1.000, final-day/preceding-day 0.94), so there is no acceleration to absorb. **B3 (spatial):** the nearest-neighbour b-positive field is unresolved — at NB3 4000 the Norcia patch sits at the field median (percentile 0.49-0.53 at K 100/200) and the field dispersion equals a location-shuffled null (p 1.000); the apparent low percentile at NB3 2000 was subsample ranking-noise. On an 894k-event Mc 0.20 catalogue this is a powered absence, not resolution-limited. Third independent / high-resolution null on the FTLS b-channel (n=3 with Kumamoto M6.5 to M7.3 and Ridgecrest M6.4 to M7.1), all null on a prospective temporal b-precursor, corroborating Dascher-Cousineau et al. (2020). Scope: the robust b-drop and rate-acceleration signatures are absent; the operational FTLS spatial mapping is only proxied by B3. Detail: `research/nucleation/RESULT_amatrice_norcia.md`. Opus-reviewed (SOUND-WITH-FIXES, all seven punch-list items applied).
- **Twenty-first test (2026-06-24): the b-value channel on the canonical NATURAL foreshock case — 2009 L'Aquila Mw6.3 — is null, after denser data overturns a sparse-catalogue false positive.** L'Aquila is the most-cited natural foreshock sequence (basis of the 2012 seismology trial), not a Gulia and Wiemer showcase, so this is an out-of-sample test of the FTLS b-drop and the famous L'Aquila rate acceleration. On a sparse aftershock-dominated ML catalogue (Zenodo 16535092; pre-mainshock Mc 1.20, only 506 foreshocks, mainshock clipped) the acute foreshock b-positive looked low (0.90) but was unresolved (n=192, CI [0.68,1.30] overlapping both background and aftershock) with no rate acceleration. Re-running on the foreshock-specific template-matched catalogue of Cabrera et al. (2022; Zenodo 4776701, 4,978 events, Mc 0.80, ~10x denser) dissolves it. **B1:** foreshock b-positive is normal in every phase (early-swarm 1.04, acute 1.11, and 1.01 with the largest-foreshock FS1 aftershock burst removed; Mc 0.5 and 0.7 agree) — no drop, so the sparse 0.90 was a high-completeness/small-sample artifact. **B2:** a non-overlapping moving b-positive is flat (Theil-Sen -0.0001/day, Spearman rho 0.015, p 0.937; the sparse-catalogue decline rho -0.799 was an autocorrelation artifact); a point-wise final-approach decline (b-positive 0.96 to 0.73 over the last 5 d to 6 h) is statistically unresolved (final-6 h CI [0.49,1.24], npos 28) and is the ordinary b of the 2009-04-05 M3.9-triggered burst (71 of the 79 final-half-day events post-date it; that burst b-positive 0.87). **Rate:** a chain of triggered Omori bursts (FS1 at -6.5 d, secondary at -3 d, M3.9 at -0.2 d) with an overall DECAY (Spearman rho -0.729, p 0.005), not the smooth acceleration the L'Aquila literature emphasises. **B3:** the foreshock b-field is spatially resolved at K=80 (magnitude-permutation control p<0.001) yet the nucleation patch sits at the field median (percentile 0.48) — no low-b asperity (the lower patch percentiles at larger K are in a statistically unresolved field). The denser data converts the sparse "suggestive" to a clean, honestly-bounded null — the "pursue denser data, do not punt" outcome. Fourth b-value-channel case (with Kumamoto, Ridgecrest, Norcia), all null, now including the canonical natural foreshock case. Scope: a case series of selected notable mainshocks, not a population test of FTLS skill; the b-drop and rate-acceleration signatures are absent under incompleteness-robust estimators and Omori-aware nulls, not a formal falsification of the operational FTLS. Detail: `research/nucleation/RESULT_laquila_ftls.md`. Opus-reviewed twice (SOUND-WITH-FIXES; denser-data + FS1-decomposition + B3 K-curve control + dropping the mis-specified uniform end-load test all applied).
- **Twenty-second test (2026-06-24): the b-value/rate channel on a subduction MEGATHRUST — 2014 Iquique Mw8.1, the canonical accelerated-nucleation foreshock sequence — shows no short-term FTLS SEISMICITY precursor.** Iquique (northern Chile) had a famous ~2-week migrating foreshock sequence with documented precursory slow slip (Ruiz et al. 2014; Kato and Nakagawa 2014), a ~270-day up-dip seismicity acceleration at Mc~3.8 (Kato et al. 2016), and a multi-year b-decline to ~0.6 with a terminal reversal (Schurr et al. 2014); this arc earlier found the precursory slow slip at/below open geodetic resolution. On the open Sippl et al. (2018) IPOC double-difference catalogue (GFZ; 101,602 events, source-box pre-mainshock Mc 2.60): **B1** the foreshock b-positive (0.60) is not anomalously low — the Aki 0.44 drop is incompleteness (robust to excluding the Mw6.6 aftershock burst, 0.62), and a completeness check shows the aftershock b-positive itself rises with Mc (0.59 to 0.77 over Mc 2.5 to 4.0); at clean Mc the foreshock b sits marginally below the aftershock and background (~0.05-0.08), directionally consistent with the Schurr 2014 multi-year decline but far too small and CI-overlapping to be an FTLS RED. **B2** prospective b is flat-to-rising (non-overlapping moving b-positive Spearman rho +0.857, consistent with Schurr terminal reversal), not dropping. **Rate** the acute-window (16 d, Mc 2.5) seismicity decays (Spearman rho -0.731), burst-driven (Mw6.6 then the 03-22/03-24 Mw6.1-6.4 foreshocks) with a quiet final week; an ETAS-lite Omori superposition (mu + triggers) finds the final 2-3 d consistent with triggered decay (ratio 1.10, Poisson p-excess 0.28) — no short-term acceleration excess over the cascade. Scope: this 16-day Mc-2.5 acute window does not reach, and does not refute, the Kato 2016 ~270-day Mc~3.8 up-dip template-matched acceleration. **B3** the foreshock b-field is spatially unresolved (magnitude-permutation control p 0.97-1.00) — no low-b nucleation asperity. The scoped conclusion: this is NOT "Iquique had no precursor" — its documented precursors (slow slip, repeating earthquakes, migration, the ~270-day acceleration, the multi-year b-decline) live in the aseismic/moment/migration/long-timescale/up-dip domain, at or below open resolution and outside the operational short-term FTLS b/rate seismicity channel, which does not capture this megathrust nucleation. Fifth b-value-channel case and the first subduction megathrust; the open offshore foreshock completeness floor is Mc ~2.5-2.7 (the densest open template catalogue, Kato 2016, is Mc~3.8), so the b-positive null is incompleteness-robust but the rate null is not projected below Mc 2.5. Detail: `research/nucleation/RESULT_iquique_ftls.md`. Opus-reviewed (SOUND-WITH-FIXES; rate-scope restriction, ETAS-superposition null, Schurr-2014 reframe, and channel-scoped wording all applied).
- **b-value channel synthesis (2026-06-24): five cases, the incompleteness mechanism, and the limit of b-positive.** Consolidating the five FTLS b-channel cases (Kumamoto, Ridgecrest, Norcia, L'Aquila, Iquique): the durable cross-case finding is that the foreshock-vs-aftershock b-positive difference is small in every case (~ -5%, bootstrap-CI-overlapping), an order of magnitude below the Aki "RED" drop against a quiescent background (-27% to -34% where a background is measurable), and robust to estimator nuisance parameters. A controlled split-half experiment on the dense Amatrice catalogue (identical true b in both halves by construction) shows the Aki RED is SUFFICIENTLY explained by an analysis-Mc artifact -- imposing a sharp elevated completeness on one half collapses its Aki from 0.88 to 0.18-0.45 at the fixed nominal Mc while b-positive stays 1.15-1.33. BUT a per-case check finds the foreshock-phase Mc is not systematically elevated above the aftershock Mc (Amatrice 0.0 vs 0.2; Iquique 2.4 vs 2.6), so the artifact explains the large foreshock-vs-background RED rather than the intrinsically small foreshock-vs-aftershock contrast; the durable claim is the b-positive comparative null, not one universal mechanism. The genuinely new methodological point: b-positive is robust to a sharp (even time-varying) completeness threshold but only APPROXIMATELY robust to a smooth magnitude-dependent roll-off (a logistic roll-off biases b-positive low, 1.21 to 0.56), and that bias direction is CONSERVATIVE for the nulls (a worse-completeness foreshock window would bias toward a spurious RED, yet none appeared). Continuous-magnitude catalogues also make the b-positive ABSOLUTE value dM-threshold-dependent (Amatrice foreshock b+ 1.12 at dM>0 vs 0.89 at dM>=0.1), though the foreshock-vs-aftershock comparison is common-mode stable. No new estimator is proposed; this diagnoses the FTLS RED across five cases and delineates the Van der Elst (2021) robustness boundary. Case series, not a population test of FTLS skill. Detail: `research/nucleation/RESULT_bvalue_synthesis.md`. Opus-reviewed (SOUND-WITH-FIXES; sufficiency-scoping, conservative-direction risk correction, per-case field analog, and dM-threshold sensitivity all applied).
- **Population test (2026-06-24): ~150 SoCal mainshocks (QTM) -- no robust FTLS foreshock b-RED, and the apparent RED is background-definition-dependent.** Generalising the five case studies from a case series to a population: on the dense SoCal QTM catalogue (Ross et al. 2019, 898k template-matched events 2008-2017) ~150-175 declustered M>=4 mainshocks were stacked and their foreshock b compared to background under Aki and b-positive. Against a WHOLE-REGION background the stacked foreshock shows a large Aki RED (-17 to -23%) but a b-positive INCREASE (+6 to +8%, the sign flips) -- the contrast is dominated by spatial-b heterogeneity (high-b geothermal vs low-b strike-slip zones), not a foreshock signal. Against a SPATIALLY-MATCHED quiet-time background (same fault-zone disks, identical within-sequence construction) the drop collapses to small values in both estimators (Aki -3 to -8%, b-positive -6 to -7%, sequence-bootstrap-significant but an order of magnitude below the whole-region RED and below any operational FTLS threshold), and even that residual is partly foreshock-window incompleteness (Aki 0.59 << b-positive 0.75 at M>=1.0, so a smooth-roll-off low-bias inflates it). Per sequence, a >10% RED occurs in only ~25-30% of cases. So at population scale the FTLS foreshock b-RED is not robust: its magnitude and even SIGN depend on the background definition, the Aki drop is incompleteness-inflated, and any real foreshock b decrease is small and operationally useless -- consistent with the five case nulls and the Dascher-Cousineau et al. (2020) parameter-sensitivity critique. Detail: `research/nucleation/RESULT_population_ftls.md`. Opus-reviewed (SOUND-WITH-FIXES; spatially-matched-background control, sequence-bootstrap CI, per-sequence distribution, and softened wording all applied -- the matched background exposed a spatial-b confound the whole-region comparison had hidden).
- **Spatial channel -- a POSITIVE result (2026-06-24): foreshocks migrate weakly but specifically toward the eventual mainshock.** Where the magnitude (b-value) and rate channels are null, the SPATIAL channel carries a real signal. On the SoCal QTM population (~52 M>=4 sequences with >=15 foreshocks), the per-sequence Spearman rho(foreshock time, distance-to-mainshock) is negative -- foreshocks get closer to the eventual hypocentre over time (mean rho -0.098, 67% of sequences, Wilcoxon p=0.006; robust in sign across an 8-cell parameter sweep). It survives every adversarial control: a synthetic uniform-in-space-and-time null run through the identical pipeline is clean (mean rho +0.005, p=0.85, so it is NOT a pipeline artifact); the contraction is SPECIFIC to the eventual mainshock location (-0.098) and absent against the foreshock centroid (+0.006) or a random point (-0.038); it survives controlling for magnitude (partial Spearman -0.106) and in a narrow magnitude band (-0.159), so it is not a later=larger=better-located artifact; and it survives in 3D hypocentral distance (-0.078). As a validity check the identical metric gives the OPPOSITE, expected sign for aftershocks (mean rho +0.121 = classic Omori-Utsu aftershock-zone expansion; fore-vs-aft Mann-Whitney p<1e-4). Honest scope: the effect is WEAK (rho ~ -0.1, ~ -0.19 km median approach, comparable to location precision) and operationally modest, and it is mechanism-agnostic (consistent with -- but not proof of -- slow-slip nucleation loading; not distinguishable here from cascade concentration onto the nucleation patch). It does NOT revive the FTLS b-RED (still null); it is a distinct spatial channel. This is the arc first positive precursor channel, generalising the qualitative Kato (2012/2016) megathrust foreshock migration to a crustal population. It REPLICATES in an independent region: Northern California (Waldhauser-Schaff DD catalogue, different fault systems) gives mainshock-ref rho -0.113 (Wilcoxon p<1e-4, n=146, mainshock-specific, synthetic-null-clean, magnitude/3D-robust, aftershock-expansion gate passed), even stronger than SoCal; the one offshore subduction test (northern Chile Sippl IPOC) is resolution-limited (fails the aftershock-expansion gate, exactly as a Mc-degraded SoCal catalogue does), so the signal is robust across dense crustal catalogues and not SoCal-specific. A THIRD independent catalogue in a different plate-boundary class -- Japan inland crustal seismicity (JUICE relocated Hi-net double-difference catalogue, Yano et al. 2017, 1.09M events 2001-2012; restricted to crustal depths to drop the subduction-interface/slab events that JUICE's M>=4 set still contains) -- also replicates: mainshock-ref rho -0.077 (depth<=20 km, n=235) to -0.086 (depth<=15 km, n=206), mainshock-specific (paired main-minus-centroid p 6e-4 / 5e-4, centroid and random refs NS), magnitude- and 3D-robust, synthetic null clean, aftershock-expansion gate passed (+0.11 to +0.14); the effect strengthens monotonically as the depth cut tightens toward purely crustal (-0.058 unfiltered -> -0.077 -> -0.086), i.e. deep contamination was diluting a real inland-crustal signal, approximately matching -- not claimed fully equal to -- the California range (a 2026-06-25 Opus review flagged the missing depth filter as a BLOCKER, then independently re-ran the scripts and signed off after the crustal-restriction fix). A pre-stated pre/post-2011-Tohoku split is directionally consistent with dilution of mainshock-specific migration by the M9-triggered nationwide surge (pre-2011 mainshock-ref -0.091) but the pre-period paired test is underpowered (n=112), so it is a supplement, not a claim. The spatial precursor thus holds across two fault systems on two continents and two crustal-faulting regimes (California transform + Japan inland reverse/strike-slip). A FOURTH crustal catalogue adds the THIRD faulting style -- NORMAL faulting (central Apennines, Italy; INGV bulletin, the L'Aquila 2009 / Amatrice-Norcia 2016 region): despite ABSOLUTE (non-DD) locations it PASSES the aftershock-expansion gate (n=62, p<1e-4 -- a first for a non-relocated catalogue, via the dense central-Italy network) and replicates the mainshock-specific migration at CA-range strength (mainshock-ref -0.104, PAIRED main-minus-centroid -0.065 p0.025, partial -0.104 p0.0098, synthetic null clean), and it SURVIVES excluding the dominant 2016-17 Amatrice-Norcia sequence (no_avn n=49, mainshock -0.125, paired p0.049 -- not a single-sequence artifact). M>=3.5 is the threshold (only 24 M>=4 mainshocks in 14 yr; M>=4 itself shows the same direction but is paired-underpowered at n=20). So foreshock migration is now mainshock-specific across all THREE crustal faulting styles -- transform (SoCal/NorCal), reverse/strike-slip (Japan), and normal (Italy) -- as ordinary M>=3.5-4 population statistics. Cross-REGIME (subduction megathrust): on the densest open subduction catalogue (Atacama, Chile; Munchmeyer et al. 2025, ML picks + GrowClust DD, Mc~1.5-2.0, far below the Sippl 2.6 that failed the gate), restricting to the plate INTERFACE now PASSES the aftershock-expansion gate (n=114, p1e-4 -- a genuine advance over Sippl's resolution-limited null), yet foreshock migration is NOT mainshock-specific there: the paired main-minus-centroid test is null (-0.002, p0.99, CI95 [-0.033,+0.031]), mainshock-ref and centroid-ref identical (both NS), synthetic/random clean -- only weak GENERAL contraction toward the cluster, no directed approach to the eventual hypocentre. Power: a crustal-STRONG specificity (~-0.05, JUICE-level) is rejected (~0.82), but a crustal-WEAK one (~-0.03, SoCal-level) is underpowered (~0.48) on this 3.3-year catalogue. So the crustal mainshock-specific migration does NOT extend to the ordinary subduction-megathrust population (intraslab-dominated all-class selections fail the gate entirely); this does not negate the qualitative Kato (2012/2016) migrations on individual GREAT-earthquake nucleations -- Atacama 2020-2024 hosted no great megathrust, so only ordinary M>=3.5 interface mainshocks were testable. Characterisation (both catalogues): the approach is SLOW (migrating-subset net rate ~0.1 km/day, one-to-two orders below SSE front speeds, so not a fast propagating front -- driver aseismic-vs-cascade undetermined), concentrates significantly in the second half of the window (paired Wilcoxon p=0.02 / p<1e-4), is magnitude-independent, and carries NO location-forecasting skill (neither recency-weighted nor trajectory-extrapolated foreshock centroids beat the all-foreshock centroid, which already locates the mainshock to ~3-4 km) -- a scientifically real but operationally negligible spatial precursor. **Definitive pooled estimate (4 catalogues, n=424 sequences):** grand mainshock-ref rho -0.099 (p=7.7e-14), paired mainshock-specificity -0.068 (p=1.6e-7), cross-region homogeneous (Kruskal-Wallis p=0.51) and leave-one-region-out stable -- consistent across transform + reverse/strike-slip + normal faulting on two continents; statistically distinct from the subduction megathrust (Atacama interface paired -0.005, CI excludes all crustal values). **Mechanism (5 catalogue probes + large-case geodesy + literature):** a propagating aseismic slip FRONT is REJECTED (geometry -- radial contraction, mainshock not at a leading edge; net speed ~0.1 km/day). The driver of the radial concentration, in the one well-resolved large case, is an aseismic transient slow-slip near the hypocentre that quasi-statically loads and facilitates nucleation (Yue et al. 2026, EPSL 681, GPS+tiltmeter, Kumamoto 'fourth foreshock mode'; Nature 2026 companion) -- this Mw~5.5 transient sits BELOW the project's GNSS detection floor (Mw~5.6-5.9), so an earlier GNSS-null 'aseismic-disfavoured' reading was WITHDRAWN as a sub-floor over-conclusion. The population radial geometry + temporal-not-spatial Z-BZ clustering are CONSISTENT with such aseismic-patch loading (with a subordinate triggered-seismicity response), though population transients (Mw~3-4) lie below all instruments and the catalogue data alone cannot separate aseismic loading from cascade. A direct geometry cross-check confirms the front-vs-radial DISCRIMINATION is real, not just a velocity-scale argument: applying the IDENTICAL FRONT/RADIAL diagnostic to the canonical documented megathrust front -- Iquique 2014 (Sippl IPOC) -- the documented along-strike foreshock propagation registers as FRONT at every documented-scale window (R>=50 km: directionality D 0.29-0.47 = 91st crustal percentile, mainshock edge-loaded EDGE 0.71-0.81, N-S trench-strike elongation; matched isotropic-radial null P=0.0000; interface-driven under depth stratification and time-shuffle-significant), whereas the crustal population MEDIAN is radial-leaning (D 0.136) -- the same code separates the two on DIRECTIONALITY, a single-case capability demonstration that the crustal radial population is a distinct geometric regime from documented propagating fronts rather than their signature (research/nucleation/RESULT_iquique_front_geometry.md). Honest scope: front vs radial is split by the PRESENCE of directionality, not the absence of contraction (Iquique also contracts, radial_rho -0.49), and the crustal upper-decile tail overlaps Iquique, so this is a distribution-shift contrast, not a binary non-overlapping split. A population-distribution analysis (n=299 crustal migrating sequences) sharpens this: the crustal directionality D is a single right-skewed mode with a REAL heavy tail (NOT bimodal -- a raw-D 2-Gaussian preference is a bounded-skew artifact that vanishes on the logit scale, dBIC +3.1), with ~17% front-like by the D+EDGE end-member rule and 24% exceeding their own-n isotropic-radial null (binomial p~1e-29, strengthening at large n = not estimation noise), and this directional minority is MAGNITUDE-INDEPENDENT (front-like vs radial mainshock M indistinguishable, Mann-Whitney p=0.41; D flat in M) so it is not a magnitude continuum toward the megathrust scale. The earlier uniformly-radial wording is accordingly corrected to RADIAL-DOMINANT: the pooled/median radial result is the majority behaviour, with a real but magnitude-independent, operationally-labelled front-like minority the average had hidden, and Iquique sits at/beyond that crustal directional tail. Detail: `research/nucleation/RESULT_foreshock_migration.md`. Opus-reviewed (initial verdict FLAWED pending controls; all four blocking controls -- synthetic null, mainshock-specific referencing, magnitude control, 3D -- run and PASSED, with effect-size and mechanism wording walked back).

- Strictly research — no productization or public alerting.

### 2026-06: OEF covariate exhaustion -- open-data physics saturates at the monthly horizon but is resolvable at 7 days

Operational forecasting (OEF) uses a compact ETAS feature set on a 2deg/121-cell grid (34-day M5+ probability). A systematic test (2026-06-20) of whether *any* open-data covariate family adds skill over this ETAS baseline, scored by walk-forward CSEP information gain:

**At the 34-day operational horizon, every orthogonal physical family is null.** The full 85-feature ML matrix adds only +0.0008 AUC (5/10 folds positive = noise); individually the seismic size-distribution (b-value, Aki MLE), the geodetic deformation channel (GNSS strain/transient, dAUC -0.0063), and the 38-feature electromagnetic/atmospheric/oceanic block (dAUC -0.0006, -89 nats) each fail to beat ETAS -- ETAS already captures the resolvable monthly skill.

**The saturation is a horizon-dilution artifact.** Precursory transients act on days-to-weeks, so a 34-day window averages them away. Re-running ETAS + a parsimonious 10-feature precursory-physics set (Coulomb stress transfer CFS, Pattern Informatics PI, accelerating moment release / Benioff, foreshock-magnitude trend, recurrence interval) across shorter horizons:

| Horizon | dAUC over ETAS | dCSEP info-gain | folds positive |
|---|---|---|---|
| 21 d | +0.0022 | +53 nats | 6/10 |
| 14 d | +0.0037 | +55 nats | 7/10 |
| **7 d** | **+0.0059** | **+69 nats** | **9/10** |
| 3 d | +0.0059 | +35 nats | 10/10 |

At the 7-day horizon (peak operational information gain) the 10 catalog-derived precursory-physics features add **+0.0059 AUC / +69 nats over ETAS, positive in 9/10 walk-forward folds (~3.7 sigma)** -- and *outperform* the full 85-feature matrix (+0.0045), so the signal is concentrated rather than an overfit. Geodetic and electromagnetic covariates stay net noise even at short horizon; the resolvable open-data lever is short-horizon Coulomb-stress / pattern-informatics / accelerating-moment-release physics. (Research finding; the monthly operational forecast cron is unchanged.)

## Automated Analysis (GitHub Actions)

Weekly analysis workflow fetches data from 7+ public APIs, runs 20 analysis scripts (Phase 1-4), and stores results as artifacts.

```bash
# Manual trigger
gh workflow run "Earthquake Correlation Analysis" \
  --repo yasumorishima/japan-geohazard-monitor \
  -f memo="Full analysis suite"
```

### Data fetch scripts

| Script | Source | Data |
|---|---|---|
| `fetch_earthquakes.py` | USGS GeoJSON | M3+ earthquakes (yearly chunks, retry with backoff) |
| `fetch_kp.py` | GFZ Potsdam | Kp geomagnetic index (2011-present) |
| `fetch_tec.py` | CODE (Bern) IONEX | Ionosphere TEC 2.5°×5° grid (event ±7d + random baseline) |
| `fetch_cmt.py` | GCMT NDK catalog | Focal mechanisms: strike/dip/rake for Japan M5+ (2011-present) |
| `fetch_gnss_tec.py` | Nagoya Univ. ISEE (AGRID2/GRID2 netCDF) | GNSS-TEC 0.5° grid, 1h temporal, 5.3M records (no auth, 2 hrs/day × 200 dates/run) |
| `fetch_modis_lst.py` | ORNL DAAC TESViS API | MODIS LST 1km: M5.5+ land epicenters ±14d + random control (rate limited) |
| `fetch_kakioka_ulf.py` | INTERMAGNET BGS GIN + WDC Kyoto | KAK/MMB/KNY 1-min geomagnetic: M6+ events ±7d (IAGA-2002 format) |
| `fetch_nmdb_cosmicray.py` | NMDB (Neutron Monitor Database) | Daily cosmic ray count rates: 9 stations (IRKT/OULU/PSNM/APTY/JUNG/ATHN/ROME/BKSN/AATB), 2011-present (no auth) |
| `fetch_cses_satellite.py` | INTERMAGNET BGS GIN + CSES-Limadou | KAK/MMB/KNY 1-min geomag → hourly downsample (2011-2026, 7-day batch) + CSES satellite EM (2018+, auth required) |
| `fetch_blitzortung.py` | Blitzortung.org + Univ. Bonn sferics | Lightning stroke counts aggregated to 2° grid cells (Japan region, `lightning` table) |
| `fetch_iss_lis_lightning.py` | NASA GHRC DAAC (Earthdata auth) | ISS LIS flash counts 2017-2023, 2° cells (`iss_lis_lightning` table, separate from Blitzortung) |
| `fetch_wwlln_thunder_hour.py` | NASA GHRC DAAC (Earthdata auth) | WWLLN Monthly Thunder Hour 2013-2025, 2° cells via max-aggregation from native 0.05° (`lightning_thunder_hour` table) |
| `fetch_movebank.py` | Movebank (Max Planck) | Animal GPS tracking in Japan region: movement speed/dispersion anomalies |
| `fetch_olr.py` | NOAA PSL THREDDS NCSS | Daily outgoing longwave radiation (2.5° grid, Japan region, no auth) |
| `fetch_iers_eop.py` | OBSPM Paris Observatory / USNO | Earth Orientation Parameters: LOD, polar motion (eopc04 + finals2000A fallback) |
| `fetch_solar_wind.py` | NASA OMNIWeb FTP | Hourly solar wind: Bz GSM, dynamic pressure, Dst (no auth) |
| `fetch_grace_gravity.py` | NASA PO.DAAC / GFZ ISDC | GRACE/GRACE-FO mascon gravity (Earthdata auth via `earthdata_auth.py`) |
| `fetch_omi_so2.py` | NASA GES DISC OPeNDAP | OMI SO2 column density Level 3 (Earthdata auth via `earthdata_auth.py`) |
| `fetch_smap_moisture.py` | NASA AppEEARS | SMAP L3 soil moisture 9km (Earthdata auth via `earthdata_auth.py`) |
| `fetch_tide_gauge.py` | UHSLC (Univ. Hawaii) | Fast Delivery hourly sea level (9 Japan stations, `.dat` format, no auth) |
| `fetch_ocean_color.py` | NASA OB.DAAC OPeNDAP | MODIS Aqua chlorophyll-a Level 3 (Earthdata auth via `earthdata_auth.py`) |
| `fetch_cloud_fraction.py` | NASA LAADS DAAC | MODIS Terra MOD08_D3 cloud fraction (Earthdata auth via `earthaccess` library) |
| `fetch_viirs_nighttime.py` | EOG / NASA LAADS | VIIRS Day/Night Band radiance composites (Earthdata auth via `earthdata_auth.py`) |
| `fetch_insar.py` | COMET LiCSAR | Sentinel-1 InSAR LOS velocity (Japan frames, no auth) |
| `fetch_goes_xray.py` | NOAA SWPC | GOES 1-8Å X-ray flux (solar flare proxy, no auth) |
| `fetch_goes_proton.py` | NOAA SWPC | GOES ≥10 MeV proton flux (SEP events, no auth) |
| `fetch_tidal_stress.py` | Pure calculation | Lunar + solar tidal shear stress at Japan (no external data) |
| `fetch_poes_particles.py` | NOAA SWPC | GOES ≥2 MeV electron flux (particle precipitation, no auth) |
| `earthdata_auth.py` | — | Shared NASA Earthdata auth: Bearer token + Basic Auth redirect fallback (OPeNDAP). LAADS DAAC requires `earthaccess` library instead (Bearer not honored on /archive/ or /opendap/) — see `fetch_cloud_fraction.py`. |
| `fetch_dart_pressure.py` | NOAA NDBC | DART ocean bottom pressure: 5 Japan-area stations, historical + realtime (no auth) |
| `fetch_ioc_sealevel.py` | IOC/VLIZ | Sea level monitoring: Japan coastal stations, REST API (no auth, 1 req/min) |
| ~~`fetch_snet_pressure.py`~~ | ~~NIED Hi-net~~ | **[DEPRECATED 2026-04-25, Phase 1 Step 4aa]** S-net BPR is unavailable via HinetPy; tombstone stub retained (see Phase 1 Step 4aa entry in the **Data Completeness Initiative** bullet). |
| `validate_data.py` | Local DB | **Data completeness validation**: checks all 30 tables for existence, row count, date range coverage. Outputs JSON report + human-readable summary. Runs twice per workflow (post-fetch + final) |
| ~~`load_raw_to_bq.py`~~ | ~~Local DB → BQ~~ | **[DEPRECATED 2026-05-11, PR #156]** SQLite → BigQuery loader retired after BQ Sandbox 10 GB free tier reached 98 % (`ioc_sea_level` growth) and audit found zero BQ READ paths in the codebase. Script retained in `scripts/` for reference. Primary persistence moved to RPi5 SSD (PR #157). |

### Analysis scripts

| Script | Phase | Method | Reference |
|---|---|---|---|
| `run_analysis.py` | 1 | b-value, TEC, multi-indicator (isolation, balanced sampling, bootstrap CI) | — |
| `coulomb_analysis.py` | 2 | Coulomb Failure Stress, Okada model, spatial control (shifted baseline) | Okada (1992), Toda & Stein (2011) |
| `etas_analysis.py` | 2 | Model-free regional rate anomaly + constrained ETAS residuals | Ogata (1988, 1998) |
| `cluster_analysis.py` | 2 | Nearest-neighbor distance clustering, foreshock detection (bootstrap, temporal stability) | Zaliapin & Ben-Zion (2013) |
| `validate_phase2.py` | 2.5 | Aftershock isolation + time delay + signal correlation + prospective test | — |
| `lurr_analysis.py` | 3 | Load-Unload Response Ratio from tidal stress classification | Yin et al. (2006) |
| `natural_time_analysis.py` | 3 | Natural time variance κ1 criticality detection (threshold 0.070) | Varotsos et al. (2011) |
| `nowcast_analysis.py` | 3 | Earthquake Potential Score from inter-event M3+ cycle counting | Rundle et al. (2016) |
| `modis_lst_analysis.py` | 3b | MODIS thermal IR anomaly: RST/RETIRA method, isolation, magnitude/depth dependence | Tramutoli (2005), Tronin (2006) |
| `ulf_analysis.py` | 3b | ULF spectral power, Sz/Sh polarization, Higuchi fractal dimension (nighttime only) | Hayakawa (2007), Hattori (2004) |
| `gnss_tec_analysis.py` | 3b | High-resolution GNSS-TEC (0.5°) anomaly at epicenters: day/night split, isolation filter, forward alarm evaluation | — |
| `pattern_informatics.py` | 4 | Pattern Informatics: seismicity pattern change detection on 0.5° grid, prospective test | Rundle (2003), Tiampo (2002) |
| `prospective_analysis.py` | 4 | **Forward-looking prediction**: ETAS residual + cumulative CFS + foreshock alarms + ML alarm. Cell-based base rate, Molchan score, information gain. Train 2011-2018, test 2019-2026 | Molchan (1991), Zechar & Jordan (2008), Ogata (1998) |
| `ml_prediction.py` | 8-14 | Multi-target ML (M5+/M5.5+/M6+): up to 79 features (dynamic selection across 22 groups) → **feature stability selection** (3-fold preliminary CV, permutation importance, auto-exclude unstable features) → HistGradientBoosting + **RandomForest + LogisticRegression** (diverse level-0) with class weighting, walk-forward CV, zone-specific ETAS MLE, 2-pass spatial smoothing, level-0 export for stacking + **spatial feature matrix export for ConvLSTM** (full Phase 9+ data). Phase 9: cosmic ray, geomag spectral. Phase 10/10b: OLR, Earth rotation, solar wind, GRACE gravity, SO2, soil moisture, tide gauge, ocean color, cloud fraction, nightlight, InSAR. Phase 11: X-ray, proton, tidal stress, particle precipitation. Phase 13: DART bottom pressure, IOC sea level, S-net seafloor pressure | van den Ende & Ampuero (2020), Matsuo & Heki (2011), Homola (2023), Baba (2020), Aoi (2020) |
| `export_csep.py` | 8 | CSEP-compatible XML/JSON forecast export from ML predictions | Schorlemmer et al. (2007) |
| `csep_benchmark.py` | 8 | CSEP benchmark: Uniform/Smoothed/RI/ETAS reference models + N/L/T-test + Molchan diagram | Helmstetter (2007), Rhoades (2004) |
| `stacking_analysis.py` | 8-14 | Ensemble stacking: up to 14-input level-0 (HistGBT×3 + RF×3 + LR×3 + physics×5) → logistic/isotonic meta-learner. Auto-fallback to 8 features when diverse models unavailable | Wolpert (1992) |
| `cosmic_ray_analysis.py` | 9 | Cosmic ray anomaly: 27-day solar rotation baseline deviation, 15-day trend (Homola lag), Forbush decrease detection, multi-station differential | Homola et al. (2023) |
| `export_feature_matrix.py` | 8-14 | 4D tensor export (timesteps×H×W×C) for ConvLSTM/GNN GPU training. Phase 14: also exported from ml_prediction.py with full Phase 9+ data (not zero-filled) | — |
| `colab/geohazard_convlstm.py` | 8+ | ConvLSTM spatiotemporal: 2-layer ConvLSTM + SE attention, AdamW + CosineAnnealingLR, walk-forward CV | Shi et al. (2015), DeVries et al. (2018) |
| `colab/geohazard_gnn.py` | 8+ | SeismoGNN: GATv2Conv×3 (4-head) + GRU temporal, fault-network graph (8-neighbor + tectonic zone edges), walk-forward CV | SeismoQuakeGNN (2025), Stein (1999) |

### Shared modules (`src/`)

| Module | Purpose |
|---|---|
| `physics.py` | Okada (1992) CFS, Wells & Coppersmith (1994) fault scaling, ETAS MLE (scipy L-BFGS-B), Dieterich (1994) rate-and-state, b-value (Aki-Utsu), tectonic zone classification, GNSS strain rate estimation, slow-slip transient detection |
| `features.py` | **78 features** with dynamic selection across **22 optional groups**: rate dynamics (acceleration, trend), zone-specific ETAS residuals, magnitude statistics (deficit, b-value trend), clustering (foreshock escalation, inter-event CV), rate-and-state CFS, Pattern Informatics, Benioff strain, GNSS crustal deformation (displacement, strain rate, SSE detection), enhanced spatial (neighbor CFS/ETAS/mag, zone rate anomaly, CFS rank, spatial gradient), **cosmic ray** (27-day baseline deviation, trend), **geomagnetic spectral** (ULF power, polarization, fractal dim), **OLR anomaly**, **Earth rotation** (LOD rate, polar motion speed), **solar wind** (Bz, dynamic pressure, Dst), **GRACE gravity** anomaly rate, **SO2 column** anomaly, **soil moisture** anomaly, **tide gauge** residual, **ocean color** chlorophyll anomaly, **cloud fraction** anomaly, **nightlight** airglow anomaly, **InSAR** deformation rate, **X-ray flux** (solar flare proxy), **proton flux** (SEP events), **tidal shear stress** + rate (lunar+solar), **particle precipitation** (Van Allen belt), **DART bottom pressure** (anomaly + rate), **IOC sea level** anomaly, **S-net seafloor pressure** anomaly. `get_active_feature_names()` auto-excludes groups with no data. **Performance**: bisect-based O(log n) window queries, per-day zone stats cache, deque histories — optimized for 100K+ extract() calls per target |
| `evaluation.py` | ROC-AUC, threshold evaluation (precision/recall/gain/IGPE/Molchan), walk-forward CV splits, isotonic calibration (PAV), reliability diagram, permutation importance, Molchan area skill score |
| `target_config.py` | Multi-target configuration: M5+/M5.5+/M6+ with per-target window, class weight, positive thresholds |
| `csep_format.py` | CSEP XML forecast generation: probability → GR-based rate per cell/magnitude/time bin |
| `stacking.py` | Ensemble stacking: level-0 registration (HistGBT + RF + LR × 3 targets + 5 physics = up to 14 features), logistic/isotonic meta-learner, walk-forward stacking with temporal leak prevention |

Results saved as JSON artifacts (90-day retention). Analysis runs every Monday 12:00 JST or on demand (400-min timeout). **Backfill** runs every 3 hours 24/7 (`backfill.yml`) for SO2/cloud/geomag continuous data ingestion with BQ upload + Discord/Issue alerts. As of 2026-04-18 it is split into 3 parallel jobs (`heavy` 290m + `light` 200m + `merge` 60m via `needs:`) so the GitHub 5h hard limit no longer cancels Upload-to-BigQuery — see Phase 1 Step 4e for the kill-chain analysis. **Data preservation**: DB checkpoint uploaded only after verified WAL flush passes (`flush_ok` guard). `validate_data.py` checks all 30 tables twice per run.

### Phase 5 ML Results (AUC 0.73, AdaBoost baseline)

| Metric | Train | Test |
|---|---|---|
| AUC-ROC | 0.7588 | 0.7334 |

| Feature | Single AUC | Ensemble weight |
|---|---|---|
| cfs_cumulative | **0.7151** | 23.3% (35 stumps) |
| pi_score | **0.7098** | 12.2% (28 stumps) |
| days_since_m5 | 0.6735 | 1.4% |
| rate_30d | 0.6311 | 6.2% |
| n_foreshock | 0.6062 | 8.7% |
| etas_residual | 0.5597 | 5.0% |
| b_value | 0.5166 | 41.0% (38 stumps, but ~random AUC) |

**Key insight**: CFS cumulative and Pattern Informatics are the strongest individual predictors. ETAS residual underperformed (AUC 0.56) due to fixed literature parameters — Phase 6 addresses this with MLE fitting. b-value consumed most ensemble weight (41%) despite near-random AUC (0.52), indicating AdaBoost overfitting.

### Phase 6 ML Results (AUC 0.746, HistGradientBoosting)

Major overhaul: 35 temporal features, sklearn HistGradientBoosting, walk-forward CV, zone-specific ETAS MLE, rate-and-state CFS, isotonic calibration.

| Metric | Phase 5 | Phase 6 | Change |
|---|---|---|---|
| AUC-ROC (train) | 0.759 | 0.822 | +0.063 |
| AUC-ROC (test) | 0.733 | **0.746** | **+0.013** |
| Walk-Forward CV mean AUC | — | **0.740 ± 0.016** | new |
| Molchan Skill | — | **0.425** | new (>0 = better than random) |

**Walk-Forward CV (9 folds)**: All folds AUC 0.71–0.77, std=0.016. Confirms no overfitting.

**ETAS MLE (7 tectonic zones — all converged)**:

| Zone | Branching Ratio | Interpretation |
|---|---|---|
| Hokkaido | 0.23 | Lowest aftershock activity |
| Tohoku Offshore | 0.42 | Moderate |
| Kanto-Tokai | 0.51 | Active subduction interface |
| Kyushu | **0.66** | Strongest aftershock chains |
| Nankai | alpha=1.9 | Large events trigger disproportionately |

**Top features (permutation importance)**:

| Rank | Feature | Importance | Single AUC |
|---|---|---|---|
| 1 | `cfs_cumulative_kpa` | **0.107** | 0.715 |
| 2 | `neighbor_rate_sum` | — | — |
| 3 | `days_since_m4` | — | — |
| 4 | `pi_score` | — | — |
| 5 | `cfs_recent_kpa` | — | — |

CFS cumulative remains the dominant predictor, consistent across Phase 5→6. The physics-based Coulomb stress signal is robust.

**Prospective evaluation (2019-2026)**: Combined alarm (ETAS+CFS+foreshock ≥2) gain = 7.79x, FA rate = 0.375 — consistent with Phase 4 results (7.8x).

**Remaining challenges**: Threshold precision-recall tradeoff is steep (thresh 0.5: recall 3.5%, precision 35.6%; thresh 0.2: recall 46%, precision 23.8%). ULF alarm gain = 0.

### Phase 7 Results (AUC 0.749, 47 features + spatial smoothing)

Expanded from 35 to 47 features to capture spatial correlation and crustal deformation signals:

| Category | New Features | Physical Motivation |
|---|---|---|
| GNSS crustal deformation (6) | displacement, acceleration, vertical rate, strain rate, anomaly count, transient (SSE) score | Slow-slip events precede megathrust earthquakes (Kato 2012); strain accumulation detectable by GEONET |
| Enhanced spatial (6) | neighbor CFS max, neighbor ETAS residual max, zone rate anomaly, zone CFS rank, spatial gradient, neighbor max magnitude | Earthquakes cluster spatially; stress transfer affects neighboring cells |

| Metric | Phase 6 | Phase 7 | Change |
|---|---|---|---|
| AUC-ROC (test) | 0.746 | **0.749** | +0.003 |
| Walk-Forward CV | 0.740 | **0.741** | +0.001 |

Additional changes: zone-specific ETAS parameters injected into feature extraction (was global), 2-pass Gaussian spatial smoothing of cell predictions. The +0.003 improvement indicates the feature engineering ceiling is being reached — motivating Phase 8's structural approach.

### Phase 8: Structural Overhaul

Phase 7 showed diminishing returns from feature engineering (+0.003 with 12 new features). Phase 8 attacks from 4 structural directions.

**Phase 8.0 results (multi-target + CSEP + stacking + ConvLSTM export)**:

| Target | CV AUC (pooled) | Test AUC | Notes |
|---|---|---|---|
| M5+ | 0.7413 | **0.7490** | No regression from Phase 7 (0.749) |
| M5.5+ | 0.6671 | — | New target, fewer positives |
| M6+ | 0.5858 | 0.6595 (smoothed) | Only 2.3% positive, spatial smoothing +0.052 |

Phase 8.0 revealed critical bugs in stacking:
- **Physics alarm AUC = 0.500 (constant)**: physics alarms were generated on a fixed 3-day grid while ML level-0 used different t_days precision → fuzzy matching ≈ 0% hit rate → all physics features defaulted to constants
- **Logistic stacking AUC = 0.27 (collapsed)**: constant physics features + unscaled feature values (ML prob 0-1 vs CFS 0-1000+ kPa) caused gradient explosion
- **Isotonic stacking AUC = 0.741**: survived by averaging all inputs (scale-invariant), but couldn't improve on ML alone
- **CSEP benchmark used single static forecast**: averaged all test-period predictions into one forecast, applied to all sliding windows

**Phase 8.1 fixes** (3 root causes addressed):
1. Physics alarm alignment: `export_physics_alarms()` now reads ML level-0 keys and generates features at exact same (cell, t_days) coordinates → match rate 0% → 100%
2. Logistic standardization: feature standardization (zero mean, unit variance) before gradient descent
3. Dynamic CSEP: per-window ML forecast reconstruction from level-0 predictions

**Initiative 1: ConvLSTM Spatiotemporal Neural Network** (Colab-ready)
- 2-layer ConvLSTM with channel attention (SE block) on 11×11×C spatial grid
- AdamW optimizer + CosineAnnealingLR + gradient clipping (max_norm=1.0)
- Input: 30 timesteps × 3 days = 90 days history (vs HistGBT's 7-day window)
- Walk-forward CV with same splits as HistGBT for fair comparison
- Feature matrix (109MB, 1790 steps × 11×11 × 79 features) exported and deployed to Google Drive
- Script: `colab/geohazard_convlstm.py`

**Initiative 1b: SeismoGNN (Graph Neural Network)** (Colab-ready, new)
- GATv2Conv × 3 layers with 4-head attention + per-node GRU temporal encoding (2-layer)
- Graph structure: 121 nodes (11×11 grid) with 8-connectivity + same-tectonic-zone edges
- Edge features: inverse distance, zone membership, direction encoding (sin/cos)
- Captures fault-network topology: Coulomb stress cascading follows tectonic structure, not Euclidean distance
- Same walk-forward CV splits and feature_matrix.json input as ConvLSTM/HistGBT
- Requires PyTorch Geometric (`pip install torch-geometric`)
- Script: `colab/geohazard_gnn.py`
- References: SeismoQuakeGNN (Frontiers in AI, 2025), Stein (1999) Nature — stress transfer

**3-model fair comparison** (same data, same CV splits):

| Model | Spatial Structure | Temporal Structure | Current AUC |
|---|---|---|---|
| HistGBT (baseline) | Cell-independent | 7-day statistics | **0.7485** |
| ConvLSTM | Regular grid CNN | 90-day LSTM | **0.8013** |
| SeismoGNN | Fault network graph | 90-day GRU | 0.7925 |

> Note: HistGBT 0.7485 above is from an earlier Phase-14 split and is not directly comparable; for all models on one identical 9-fold scheme see the 2026-06-07 benchmark below (HistGBT 0.7839 there).

**2026-06-06 walk-forward CV benchmark** (complete-data `feature_matrix.json`, 85 features, 1816 timesteps x 11x11 grid, 6-fold expanding-window, pooled AUC — the first valid retrain after PR #195/#196 restored the ML pipeline from a ~2.5-month `safe_connect` `NameError` regression that the `|| echo` non-fatal pattern had been masking):

| Model | Pooled AUC | Note |
|---|---|---|
| HistGBT (same splits) | 0.7518 | tree baseline |
| Bayesian Horseshoe (numpyro SVI) | 0.7643 | |
| L2 logistic (balanced) | 0.7761 | |
| L1 sparse logistic | 0.7794 | best single model; fold max 0.804 |
| rank-ensemble (0.7*L1 + 0.3*GBT) | **0.7834** | best overall |

**Key finding:** linear/sparse models beat HistGBT by ~2.8pt on *identical* splits — the precursor features (`polar_motion_speed`, `xray_flux_max_24h`, `geomag_fractal_dim`, proton/particle flux) carry linear signal the tree model fragmented, validating the data-completeness investment. Flat per-cell tabular models plateau at ~0.78 pooled (individual folds reach 0.81); robust 0.80 not yet crossed. Next levers: spatial-neighbour aggregation features, then the spatiotemporal ConvLSTM/GNN. All validation runs on a Raspberry Pi 5 (CPU) — no GPU required.

**Temporal augmentation (tested, ruled out):** adding per-cell 3-day deltas + acceleration (255 features) did *not* help — L1 pooled 0.7716 / ensemble 0.7780, slightly below the 85-feature version (folds 4-5 degraded). Crude differencing adds more noise than signal; spatial-neighbour aggregation (each cell + its 8 neighbours) is the next lever to try.

**Spatial-neighbour aggregation (tested, the strongest tabular lever so far):** appending each cell's 8-neighbour *gradient* (cell value minus neighbourhood mean; 85 extra columns -> 170 total) lifts the linear model materially. Same 6-fold expanding-window splits:

| Model (F + 8-neighbour gradient) | Pooled AUC | Folds (min..max) |
|---|---|---|
| L2 logistic (base 85 feat) | 0.7716 | 0.742..0.800 |
| L2 logistic (+gradient, 170 feat) | 0.7865 | 0.753..0.821 |
| **Elastic-net (+gradient, 170 feat)** | **0.7960** | 0.765..0.823 |
| ensemble (ENET 0.6 + spatial-GBT 0.2 + GBT 0.2) | **0.7987** | 0.767..0.825 |

Four of six folds clear 0.80; the two oldest folds (0.767, 0.781) hold the pooled score just short of a robust 0.80. Elastic-net hyperparameters are saturated (C 0.05-0.3 x l1_ratio 0.3-0.7 all give 0.7959-0.7961), and richer spatial features (neighbour std, 2-ring gradient) and SGD-elasticnet did *not* beat the simple 8-neighbour gradient. **Conclusion: the spatial gradient is the single most effective tabular feature (base 0.772 -> 0.796), confirming spatial structure is the right direction, but flat per-cell models top out at ~0.799 pooled. Crossing a robust 0.80 is the next lever to attempt with genuine spatiotemporal modelling (ConvLSTM/GNN, GPU) -- the third stage (now confirmed in the benchmark below -- ConvLSTM reaches pooled 0.8013).** Tabular runs on a Raspberry Pi 5 CPU; the neural runs are on a Kaggle T4.

**2026-06-07 spatiotemporal neural benchmark** (Kaggle Tesla T4, *identical* 9-fold year-based expanding-window walk-forward -- 5-yr initial / 1-yr step / 1-yr test -- all models on the same `feature_matrix.json`):

| Model | Pooled AUC | Mean AUC | Type |
|---|---|---|---|
| L2 logistic (85 feat) | 0.7747 | 0.7754 | flat tabular |
| HistGBT (85 feat) | 0.7839 | 0.7846 | flat tabular tree |
| HistGBT + 8-neighbour gradient | 0.7905 | 0.7917 | tabular + spatial |
| Elastic-net + 8-neighbour gradient | 0.7955 | 0.7957 | tabular + spatial |
| tabular ensemble (ENET + GBT) | 0.7992 | 0.7999 | flat-tabular ceiling |
| SeismoGNN (GATv2x3 + GRU) | 0.7925 | 0.7969 | graph spatiotemporal |
| **ConvLSTM (2-layer + SE attention, 90-day)** | **0.8013** | **0.8013** | grid spatiotemporal |

**ConvLSTM is the only model whose pooled and mean AUC both reach 0.80** (0.8013/0.8013; std 0.0124; 8.6 min on one T4). But read this honestly: the margin over the best flat-tabular ensemble (0.7992) is only +0.2pt, **within the per-fold std (~0.012) and therefore not statistically distinguishable** -- the spatiotemporal model *matches and marginally exceeds* the flat-tabular ceiling rather than decisively breaking it, and ~0.799 is essentially where both land. ConvLSTM does beat the graph model (0.7925), so for this regular 11x11x2-degree grid the grid-convolution receptive field at least matches the fault-network graph. **Caveats:** (1) all models share the same fold boundaries and `feature_matrix.json`, but inputs are *not* symmetric -- the neural models consume a 90-day history window (train mask starts 90 days in) while the tabular models use a single timestep; (2) ConvLSTM's lowest fold is 0.783, so 0.80 holds on the aggregate, not every fold; (3) its pooled and mean coincide at 0.8013 by rounding, not by independent measurement. Reproduced on Kaggle with `feature_matrix.json` as a private dataset; the only code fixes were `total_mem` -> `total_memory` and selecting a T4 (Kaggle's default P100/sm_60 is unsupported by the current PyTorch). A capacity bump (hidden_channels 64->128, patience 12) did *not* move the result (mean 0.8020 / pooled 0.8008, std 0.0147 -- within noise of the hidden-64 run), so ~0.80 is the genuine ceiling here across tabular, ConvLSTM, and GNN: at 2-degree resolution / 7-day M5+ horizon the signal tops out near AUC 0.80.

**2026-06-07 multi-scale temporal precursor features (tested, ruled out):** with model capacity exhausted at ~0.80, the next lever was redesigning the *temporal* side of the matrix. Each global precursor series (X-ray/proton flux, geomag fractal/ULF, polar motion, Dst, cosmic ray, solar wind, tidal/DART/IOC, S-net anomalies) was expanded into multi-window aggregations (7/30-day rolling mean and std, 30-day z-score, 30-day delta) and broadcast back to the active cells. Evaluated on the *same* 9-fold year-based walk-forward as the neural benchmark above. The harness reproduces the flat-tabular ceiling (base + 8-neighbour gradient pooled 0.7982, matching the 0.7992 above), so the comparison is fair.

| Config (same 9-fold split) | Pooled AUC | Mean | Std | Min fold |
|---|---|---|---|---|
| base 85 | 0.7880 | 0.7882 | 0.0148 | 0.753 |
| base + 8-neighbour gradient (ceiling) | 0.7982 | 0.7981 | 0.0148 | 0.765 |
| base + multi-scale precursor (broad, 170 feat) | 0.7814 | 0.7807 | 0.0285 | 0.731 |
| base + gradient + multi-scale (broad) | 0.7899 | 0.7894 | 0.0283 | 0.734 |
| base + gradient + multi-scale (focused, 12 feat) | 0.7970 | 0.7969 | 0.0152 | 0.764 |

The broad version *degrades* the ceiling by 0.8pt and nearly doubles per-fold std (0.0148 to 0.0283), with the damage concentrated in the linear model on specific held-out years (fold-1 and fold-8 elastic-net collapse to 0.65-0.70). A surgical version (only the L1-effective precursors, with stationary z-score and delta derivatives only) is a wash at 0.7970 versus 0.7982. **Conclusion:** the existing single-day precursor values already capture the usable signal, and daily multi-window temporal derivatives add year-to-year non-stationarity (solar-cycle and instrument drift) faster than predictive signal. The failure mode is non-stationarity, not insufficient temporal resolution, which confirms ~0.80 as the robust ceiling at 2-degree / 7-day M5+ across model capacity (tabular, ConvLSTM, GNN), spatial features (8-neighbour gradient), and now temporal multi-scale. Breaking it would require changing the prediction problem itself (grid resolution, horizon, or multi-task), not finer feature engineering.

**2026-06-07 ConvLSTM x tabular ensemble (tested, the first lever to move the aggregate above 0.80):** the spatiotemporal ConvLSTM (90-day grid history) and the flat-tabular ensemble (single-timestep per-cell) consume *asymmetric* inputs, so their errors should partially decorrelate. The ConvLSTM kernel was re-run on a Kaggle T4 with a per-fold prediction dump, and its per-cell test probabilities were joined to a fresh tabular run on the *same* 9-fold split by exact (fold, t_day, cell) key (132,616 rows, 0 misses, 0 label mismatches). Equal-weight per-fold rank average:

| Model (same 9-fold split) | Mean AUC | Pooled | Std | Min fold |
|---|---|---|---|---|
| tabular ensemble (ENET + GBT + 8-neighbour gradient) | 0.7981 | 0.7980 | 0.0148 | 0.765 |
| ConvLSTM (this run) | 0.8014 | 0.8015 | 0.0132 | 0.779 |
| **ConvLSTM x tabular (50/50 rank average)** | **0.8050** | **0.8051** | 0.0134 | 0.780 |

The ensemble beats *both* components (+0.7pt over tabular, +0.4pt over ConvLSTM) and lowers variance -- the first result this round to move the aggregate meaningfully above 0.80 rather than just touch it within noise, confirming the two model families carry complementary signal. Blend weight is not tuned (0.4/0.6 gives an identical 0.8051, and 50/50 is the headline). **Two honest caveats remain.** (1) The two hardest folds (the earliest test year with the least training data, and the latest) still sit at ~0.78, so this is a robust-0.80 *aggregate*, not robust on every fold. (2) The ConvLSTM selects its best epoch on the test fold (early stopping on test AUC), so the ConvLSTM and ensemble numbers carry mild optimism that the leak-free tabular does not -- a fully clean version would early-stop on a validation split. Net: ~0.805 aggregate is the best this configuration reaches, and crossing a per-fold-robust 0.80 on the hardest years would still require changing the prediction problem (resolution, horizon, or multi-task), not finer feature engineering.

**2026-06-07 horizon study -- reaching AUC 0.85 by forecasting a longer window:** ~0.80 is the ceiling for the *7-day* M5+ task across every model and feature lever tried. The remaining lever is the prediction problem itself. Earthquakes cluster strongly in time (ETAS / aftershock sequences), so a longer forecast window is intrinsically more predictable -- a legitimate monthly-hazard product rather than a trick. Re-labelling the same feature matrix at longer horizons (the union of the tiling 7-day windows is the exact n-day label) and re-running the same embargo-ed 9-fold walk-forward:

| Forecast horizon | Positive rate | GBT mean AUC | Min fold |
|---|---|---|---|
| 7 days | 9.8% | 0.789 | 0.753 |
| 13 days | 16% | 0.806 | 0.777 |
| 19 days | 21% | 0.821 | 0.789 |
| **34 days** | 31% | **0.853** | **0.818** |
| 58 days | 41% | 0.891 | 0.843 |
| 88 days | 49% | 0.920 | 0.872 |

At a **34-day horizon the tabular ensemble (ENET + GBT + 8-neighbour gradient) reaches mean and pooled AUC 0.854 with every fold above 0.80 (min 0.819)** -- a robust 0.85, tabular-only (no neural model). A 34-day train/test embargo (purging train timesteps whose forward label overlaps the test period) costs only 0.3pt (0.857 to 0.854), confirming the gain is real predictability, not label leakage. Longer windows climb further (58d ~0.89, 88d ~0.92). The 7-day and 34-day forecasts are simply different operational products -- 0.80 is the ceiling for the weekly forecast, and ~0.85 is reached for the monthly one.

**34-day ensemble (ConvLSTM + tabular):** re-running the ConvLSTM on a Kaggle T4 with the same 34-day relabelling and 34-day embargo, then rank-averaging its per-cell predictions with the tabular ensemble (exact (fold, t_day, cell) join, 132,616 rows, 0 misses, 0 label mismatches) lifts the monthly forecast to **mean and pooled AUC 0.859 with every fold above 0.82 (min 0.821)** -- a robust ~0.86, beating both components by ~0.5pt. The same test-set early-stopping caveat applies to the ConvLSTM component. Net result: weekly (7-day) M5+ forecasting tops out at AUC ~0.805, the monthly (34-day) forecast reaches ~0.86, and the decisive lever was the prediction horizon, not additional features.

**34-day ETAS-lite features (recent-M5 Omori triggering):** the dominant signal for medium-term M5+ forecasting is recent seismicity clustering. Adding per-cell Omori-decayed sums of past M5+ occurrences (self + 1-ring + 2-ring neighbourhoods times six time constants 7-250 days, strictly causal so no leakage) -- 18 features built only from the existing labels, no external catalogue -- lifts the 34-day tabular ensemble from 0.854 to **mean and pooled AUC 0.870 (min fold 0.830)**. Six self and neighbour Omori features alone already beat the 170-feature base-plus-gradient set (0.859), confirming recent-M5 clustering is the primary driver. The ConvLSTM (trained without these features) no longer adds value in the ensemble. Pushing beyond ~0.87 at this horizon would need the full small-event catalogue (magnitude-weighted ETAS productivity), since M5-only labels ignore that a single M6.5 triggers far more aftershocks than an M5.0.

**Full magnitude-weighted ETAS (tested, no gain over M5-Omori):** to push the 34-day forecast past 0.87, a magnitude-weighted ETAS intensity was built from an external USGS M4+ Japan catalogue (23,473 events 2009-2026, with the matrix epoch t0 recovered as 2011-01-01 by cross-correlating matrix M5+ labels against USGS M5+ counts, corr 0.63). Six ETAS intensity channels (productivity 10^(alpha(M-Mc)) times a Gaussian spatial kernel times Omori temporal decay, strictly causal, log1p) reach AUC 0.851 on their own, but add only +0.001 on top of the recent-M5 Omori features (0.870 to 0.871). The M5-label Omori features already capture the clustering signal, and the external M4+ catalogue (different completeness, timing and locations) adds noise rather than signal. **Conclusion: the 34-day (monthly) M5+ forecast plateaus at a robust AUC ~0.871 (min fold 0.831) across all tabular levers (base, spatial gradient, M5 Omori, magnitude-weighted ETAS). Reaching 0.90 is a ~55-58-day-horizon result (the sweep already shows ~0.89-0.90 there), not attainable at a 30-day horizon with this 2-degree cell and M5+ target framing -- the background (non-triggered) M5+ rate is the irreducible unpredictable component at one month.**

**Initiative 2: CSEP-Compatible Format + Benchmark**
- ML probability → CSEP XML rate forecast (2°×2° grid, 4 magnitude bins)
- 4 reference models: Uniform Poisson, Smoothed Seismicity (Helmstetter 2007), Relative Intensity (Rhoades 2004), Simple ETAS
- Statistical tests: N-test (Poisson consistency), L-test (log-likelihood), T-test (paired comparison), Molchan diagram
- Phase 8.1: per-window dynamic ML forecast, up to 80 sliding windows

**Initiative 3: Multi-Target Prediction (M5+, M5.5+, M6+)**
- Per-target prediction windows: M5+/M5.5+ = 7 days, M6+ = 14 days
- Class weighting for extreme imbalance (M6+: weight=10)
- Level-0 prediction export for downstream stacking

**Initiative 4: Ensemble Stacking (Physics × ML)**
- Up to 14-input level-0: HistGBT×3 + RandomForest×3 + LogisticRegression×3 + ETAS rate + CFS kPa + CFS rate-state + foreshock alarm + composite alarm count
- Level-1 meta-learner: Logistic regression (with standardization) / Isotonic regression
- Walk-forward stacking with temporal leak prevention
- Phase 8.1: exact key alignment between physics and ML predictions
- Phase 14: diverse models (RF + LR) added for genuine error diversity in level-0. Auto-fallback to 8 features when diverse predictions unavailable

### Phase 9: Non-Traditional Precursor Data Sources (47 → 56 features)

Phase 7-8 showed diminishing returns from seismological features (+0.003 per phase). Phase 9 introduces **physically independent data domains** — cosmic rays, animal behavior, lightning, and continuous geomagnetic monitoring — to break the AUC 0.74 ceiling through ensemble diversity.

| Data Source | Physical Mechanism | Reference | Features Added |
|---|---|---|---|
| **NMDB cosmic rays** | Crustal stress → geomagnetic field change → cosmic ray deflection (15-day lag) | Homola et al. (2023) J. Atmos. Sol.-Terr. Phys. 247:106068 | cosmic_ray_rate, cosmic_ray_anomaly, cosmic_ray_trend_15d |
| **INTERMAGNET hourly** | Continuous ULF monitoring enables spectral analysis: power, polarization, fractal dimension | Hattori (2004) NHESS; Hayakawa (2007) | geomag_ulf_power, geomag_polarization, geomag_fractal_dim |
| **Blitzortung lightning** | Lithosphere-Atmosphere-Ionosphere Coupling: radon → ionization → atmospheric E-field → lightning anomaly | Pulinets & Ouzounov (2011) NHESS 11:3247 | lightning_count_7d, lightning_anomaly |
| **Movebank animal GPS** | Animals detect pre-seismic EM emissions, radon, or infrasound 1-20 hours before M3.8+ | Wikelski et al. (2020) Ethology 126:931 | animal_speed_anomaly |
| **CSES satellite** | Ionospheric EM anomalies detected by Zhangheng-1 satellite (2018+) | Zhima et al. (2020) Space Weather | (best effort, auth required) |

**Phase 9.0 results (initial deployment — data source failures)**:

| Data Source | Status | Issue |
|---|---|---|
| NMDB cosmic rays | ✅ 14,685 records (3 stations; expanding to 9) | — |
| Blitzortung lightning | ❌ JSONDecodeError | Archive returns HTML (access restricted), not detected |
| INTERMAGNET hourly | ❌ HTTP 400 on all requests | 3 API parameter errors: `SamplesPerDay=24` (invalid), date format with TZ, wrong publicationState |
| Movebank animal GPS | ❌ No data | No public GPS tracking studies in Japan region |
| CSES satellite | ❌ Auth required | limadou.ssdc.asi.it registration needed |

With only cosmic ray data available and 6 zero-filled features injected as noise, **CV AUC dropped from 0.741 to 0.728** — a clear demonstration that constant-zero features degrade tree-based models.

Cosmic ray feature importance (small but positive): `cosmic_ray_rate` = 0.0062, `cosmic_ray_anomaly` = 0.0029.

**Phase 9.1 fixes (4 bugs + dynamic feature selection + metadata fix)**:

| Fix | Root Cause | Solution |
|---|---|---|
| INTERMAGNET API | `SamplesPerDay=24` doesn't exist; date format with `T00:00:00Z` rejected; `adj-or-rep` is not a valid publicationState | `samplesPerDay=1440` (minute data) + hourly downsample, `yyyy-mm-dd` only, `best-avail`. 7-day batch to reduce requests ~7x |
| Lightning SQL | Query references `mean_intensity_ka` column and `source` column — neither exists in the `lightning` table | Fixed to `mean_intensity`, removed `WHERE source != 'climatology'` |
| Blitzortung HTML | Archive returns HTML login page with HTTP 200, parsed as JSON → crash | Content-Type check + body prefix detection (`<!DOCTYPE`, `<html>`) |
| Zero-feature noise | Phase 9 features with no data default to 0.0, degrading model | `get_active_feature_names()` dynamically excludes feature groups whose data source returned empty |
| `metadata` NameError | `train_final_model()` used `metadata` variable but it was never passed as parameter | Added `metadata` parameter + caller updated. Crash prevented feature importance, level-0 export, and stacking |

**Phase 9.1 results**:

| Metric | Phase 8.1 | Phase 9.1 | Notes |
|---|---|---|---|
| CV AUC (pooled) | 0.741 | **0.7316** | INTERMAGNET geomag data added but not yet improving |
| Test AUC | 0.748 | **0.7452** | Stable on holdout set |
| Active features | 47 | **53/56** | 3 excluded: lightning, animal, cosmic_ray_trend |

INTERMAGNET: 36,000 hourly records (KAK/MMB/KNY × 500 days). Blitzortung: Sferics Bonn server unreachable (ECONNREFUSED), archive non-public. Lightning data currently unavailable from any free source.

CV fold AUCs: 0.738, 0.689, 0.721, 0.743, 0.766, 0.756, 0.733, 0.742, 0.726

### Phase 10: Unconventional Data Sources (65 features)

Phase 9 showed that non-traditional data can contribute (cosmic ray importance > 0), but most sources failed due to API issues. Phase 10 takes a different approach: **cast a wide net across physically independent domains** that are largely unexplored in earthquake ML. The hypothesis is that since nobody has successfully predicted earthquakes, conventional approaches are insufficient — signal may exist in overlooked data.

| Data Source | Physical Mechanism (speculative) | Access | Features |
|---|---|---|---|
| **NOAA OLR daily** | Crustal stress → radon → aerosol → cloud → OLR anomaly (LAIC model, broad-scale) | THREDDS NCSS, **no auth** | olr_anomaly |
| **IERS Earth Orientation** | LOD changes reflect angular momentum transfer → differential plate stress. **Novel in earthquake ML** | CSV download, **no auth** | lod_rate, polar_motion_speed |
| **NASA OMNIWeb solar wind** | Solar wind → magnetospheric compression → induced telluric currents → fault stress modulation. Richer than Kp (raw hourly Bz, pressure, Dst) | FTP, **no auth** | sw_bz_min_24h, sw_pressure_max_24h, dst_min_24h |
| **GRACE/GRACE-FO gravity** | Pre-seismic fluid migration → gravity change. Documented before 2011 Tohoku M9 (Matsuo & Heki 2011) | PO.DAAC OPeNDAP, Earthdata | gravity_anomaly_rate |
| **OMI SO2 column** | Tectonic stress → volcanic conduit permeability → degassing rate change | GES DISC OPeNDAP, Earthdata | so2_column_anomaly |
| **SMAP soil moisture** | Crustal strain → pore pressure → anomalous surface moisture near faults | AppEEARS API, Earthdata | soil_moisture_anomaly |

No-auth sources (OLR, EOP, solar wind, tide gauge, InSAR) are fetched immediately. Earthdata sources use `EARTHDATA_TOKEN` secret (configured) and are auto-excluded by dynamic feature selection if unavailable.

**Phase 10b: "Earth's screams" — listening to every channel**

The crust under stress doesn't just shake — it emits heat, changes gravity, alters ocean chemistry, modifies cloud patterns, and shifts the Earth's rotation. Phase 10b adds 5 additional channels:

| Data Source | Physical Mechanism | Access | Features |
|---|---|---|---|
| **UHSLC tide gauge** | Slow slip → seafloor displacement → coastal sea level anomaly | UHSLC CSV, **no auth** | tide_residual_anomaly |
| **MODIS ocean color** | Submarine hydrothermal/volcanic activity → nutrient upwelling → chlorophyll change | OB.DAAC OPeNDAP, Earthdata | ocean_color_anomaly |
| **MODIS cloud fraction** | Radon → ionization → condensation nuclei → linear cloud formation along faults (LAIC) | LAADS DAAC via `earthaccess` | cloud_fraction_anomaly |
| **VIIRS nighttime light** | Acoustic-gravity waves from pre-seismic ground motion → airglow modulation at 90km | EOG composites / LAADS, Earthdata | nightlight_anomaly |
| **Sentinel-1 InSAR** | Pre-seismic strain accumulation → mm-scale ground deformation (continuous spatial coverage vs GEONET point measurements) | COMET LiCSAR, **no auth** | insar_deformation_rate |

**Total: 70 features from 15 independent data domains.** Dynamic feature selection ensures only groups with actual data are used — no zero-filled noise.

**Phase 10/10b results (Run 23251928585 — success 2026-03-18)**:

| Metric | Phase 9.1 | Phase 10/10b | Notes |
|---|---|---|---|
| CV AUC (pooled) | **0.7316** | 0.7249 | **Regression**: noisy features from Solar Wind |
| Test AUC | 0.7452 | 0.7426 | Slight drop |
| Active features | 53/56 | **58/70** | 12 groups excluded (no data) |

11 new data sources, but **only Solar Wind succeeded**. All others failed:

| Source | Status | Root Cause |
|---|---|---|
| Solar Wind | ✅ | OMNI2 hourly data fetched |
| OLR | ❌ | NOAA NCEI THREDDS filename pattern wrong (all years 404) |
| IERS EOP | ❌ | datacenter.iers.org URL changed (404) |
| Tide gauge | ❌ | UHSLC CSV path doesn't exist (404) |
| GRACE/SO2/SMAP/Ocean/Cloud/Nightlight | ❌ | Earthdata Bearer token stripped on cross-origin redirect |
| InSAR | ❌ | LiCSAR has no Japan frames |

Stacking: Logistic AUC 0.7294, Isotonic 0.7157 — **both worse than best single model (0.7426)** due to correlated M5+/M5.5+/M6+ inputs.

CV fold AUCs: 0.704, 0.688, 0.735, 0.734, 0.760, 0.751, 0.721, 0.746, 0.712

### Phase 11: Space/Cosmic Data Sources (75 features)

4 additional space/cosmic data sources — all using publicly available data with no authentication:

| Data Source | Physical Mechanism | Access | Features |
|---|---|---|---|
| **GOES X-ray flux** | Solar flare → ionospheric disturbance → geomagnetically induced currents | NOAA SWPC JSON, **no auth** | xray_flux_max_24h |
| **GOES proton flux** | Solar energetic particle events → atmospheric ionization → telluric current anomalies | NOAA SWPC JSON, **no auth** | proton_flux_max_24h |
| **Tidal shear stress** | Lunar + solar tidal loading modulates fault stress (Cochran 2004). **Pure calculation, no external data** | Computed from ephemeris | tidal_shear_stress, tidal_stress_rate |
| **Particle precipitation** | Van Allen belt electron precipitation → ionospheric conductivity change → GIC | NOAA SWPC JSON, **no auth** | particle_precip_rate |

**Total: 75 features from 19 independent data domains.**

### Phase 12: Data Acquisition Infrastructure Overhaul + Performance Optimization

Phase 10/10b revealed that the data acquisition layer was fundamentally broken — not a configuration issue, but structural failures in URL patterns, authentication flow, and ML feature selection. Phase 12 addresses all three layers simultaneously.

**Data source fixes (12 files changed)**:

| Fix | Before | After |
|---|---|---|
| **OLR** | NOAA NCEI THREDDS per-year files (all 404) | NOAA PSL THREDDS NCSS single dataset (1974-present) |
| **IERS EOP** | datacenter.iers.org (404) + USNO (stale) | OBSPM Paris Observatory eopc04 (primary, daily updated) |
| **Tide gauge** | UHSLC CSV path (404) | UHSLC Fast Delivery `.dat` format |
| **Earthdata auth** | `Bearer` token in `Authorization` header (stripped by aiohttp on cross-origin redirect) | Shared `earthdata_auth.py`: intercept 302 redirect, send Bearer to URS, follow back with cookies |

**Data acquisition confirmed working (Phase 12 Run 23271449051)**:

All data fetch steps succeeded — OLR (1m37s), IERS EOP (2s), tide gauge (29m), GOES X-ray/Proton/Electron, tidal stress, GRACE (1m), SO2 (3m41s). The data infrastructure overhaul is validated.

**ML pipeline fix — feature stability selection**:

The Phase 10/10b regression (0.7316 → 0.7249) demonstrated that HistGradientBoosting's L2 regularization alone cannot prevent noisy features from hurting performance. Added a 2-stage approach:

1. **Stage 1: Stability pre-filter** — Quick 3-fold preliminary CV on 80% of data. For each fold, train lightweight model and compute permutation importance. Keep only features with importance > 0.001 in ≥ 2/3 folds. Base 35 features always retained.
2. **Stage 2: Standard CV** — Walk-forward CV and final model use only stable features.

This structurally prevents the "more features = worse AUC" problem that plagued Phase 9.0 and 10/10b.

**Phase 12b: FeatureExtractor performance optimization**:

Phase 12 Run timed out at "Run ML integrated prediction" step (~20 hours). Root cause: `extract()` is called ~100K+ times per target (cells × time steps × 3 targets), and multiple O(n) operations per call created O(n²) total complexity.

| Optimization | Before | After | Impact |
|---|---|---|---|
| Window queries (`_events_in_window`) | O(n) linear scan × 9 per call | O(log n) `bisect` on pre-sorted arrays | **Critical**: eliminates ~900K linear scans |
| Zone statistics | O(all_cells) scan per call | Per-day cache (computed once, shared across cells) | **Critical**: eliminates O(100K × 100) grid scans |
| Foreshock counting | O(9 cells × n) linear scan | O(9 × log n) `bisect` | High |
| Neighbor spatial (Section M) | Re-scans all 8 neighbors | Reuses Section J cached rates | Medium |
| ETAS prior extraction | O(n) list comprehension | O(log n) `bisect` slice | Medium |
| History structures | `list` with manual truncation | `deque(maxlen=N)` auto-truncation | Medium |
| Date string | `datetime` + `strftime` per call | Per-day cache (same for all cells) | Lower |
| GNSS transient | Full history scan | 180-day window limit | Lower |
| CFS rank within zone | `sorted()` + linear count per call | Pre-sorted list + `bisect_right` | Lower |

Expected speedup: **5-15x** on FeatureExtractor, enabling ML step to complete within the 6-hour timeout.

**Phase 12b result**: extract() runtime reduced from ~20 hours (timeout) to **12 minutes**. However, ML step crashed due to `deque` slice bug (`pi_hist[-3:]` → `TypeError: sequence index must be integer, not 'slice'`). Fixed in Phase 13 commit.

### Phase 13: Seafloor / Ocean Bottom Data Sources (79 features)

The seafloor is the highest-sensitivity domain for detecting pre-seismic deformation on subduction zones. Japan has the world's densest seafloor observation network, yet this data has been largely unexplored in earthquake ML.

| Data Source | Physical Mechanism | Access | Features |
|---|---|---|---|
| **NOAA DART** | Seafloor vertical displacement → bottom pressure change (sub-Pa) | NDBC HTTP, **no auth** | dart_pressure_anomaly, dart_pressure_rate |
| **IOC Sea Level** | Slow-slip → coastal sea level anomaly | IOC REST API, **no auth** | ioc_sealevel_anomaly |
| ~~**NIED S-net**~~ | ~~Sub-Pa pressure at Japan Trench subduction zone (150 stations)~~ | ~~HinetPy, **NIED registration**~~ | ~~snet_pressure_anomaly~~ **[DEPRECATED 2026-04-25, Phase 1 Step 4aa]** — HinetPy code `0120A` is acceleration, not pressure; no BPR access path exists. Active S-net contribution comes from `snet_waveform` (Phase 18+). |

DART stations near Japan: 21413 (Izu-Bonin, 30.5°N), 21418 (Japan Trench/Tohoku, 38.7°N), 21419 (Kuril, 44.4°N), 21416 (Kuril N, 48.1°N), 52404 (Philippine Sea/Ryukyu, 20.6°N).

S-net: 150 stations along the Japan Trench connected by fiber-optic cables. Water pressure gauges with sub-Pa precision at 10 Hz. Registration submitted 2026-03-19, awaiting approval.

References: Baba et al. (2020) Science 367:6478; Hino et al. (2014) EPSL 396:248; Aoi et al. (2020) EPS 72:126; Bürgmann (2018) Nature 553:1

**Data licensing**: All 19 data source policies documented in [DATA_LICENSES.md](DATA_LICENSES.md) with severity levels (🔴strict/🟡non-commercial/🟢citation/⚪public domain) and pre-publication checklist.

### Phase 13: Seafloor / Ocean Bottom Data — **CV AUC 0.7416** (best ever)

| Metric | Phase 10/10b | Phase 13 | Change |
|---|---|---|---|
| CV AUC (pooled) | 0.7249 | **0.7416** | **+0.0167** |
| Test AUC | 0.7426 | **0.7481** | +0.0055 |
| Active features | 58/70 | **64/79** | +6 (DART pressure) |

Recovery from Phase 10/10b regression — stability selection effectively filters noisy features while keeping informative ones. DART ocean bottom pressure data (3 stations, 10,603 records) contributed to the improvement. IOC sea level fetch crashed (None station codes → `AttributeError`), S-net requires NIED credentials (pending).

Stacking still underperforms best single model: Logistic 0.7404 vs HistGBT 0.7481 (−0.008). Correlated M5+/M5.5+/M6+ HistGBT predictions limit meta-learner diversity — Phase 14 addresses this.

### Phase 14: Diverse Stacking + ConvLSTM Export — Test AUC 0.7485

| Metric | Phase 13 | Phase 14 | Change |
|---|---|---|---|
| CV AUC (pooled) | **0.7416** | 0.7415 | −0.0001 |
| Test AUC | 0.7481 | **0.7485** | +0.0004 |
| Active features | 64/79 | 65/79 | +1 |

Stacking meta-learner with 14 diverse level-0 inputs (HistGBT×3 + RF×3 + LR×3 + physics×5):
- Logistic stacking: pooled AUC = 0.7484 (≒ base model, no improvement)
- Isotonic stacking: pooled AUC = 0.7213 (degraded)

ConvLSTM 4D feature matrix export (timesteps×11×11×65) completed. Ready for Colab GPU training.

**Key takeaway**: Stacking with correlated level-0 models does not improve on the best single model. Genuine diversity requires structurally different models (e.g., ConvLSTM spatiotemporal vs HistGBT tabular).

### Phase 14b: Data Acquisition Overhaul — 57→71+ active features

Phase 13 revealed that 15 out of 27 data sources had been silently failing (only 57/79 features had real data). Phase 14b systematically rewrites every broken fetch script, adds new sources, and verifies each with lightweight curl tests before committing:

| Source | Before (broken) | After (fixed) | Verified |
|---|---|---|---|
| **OLR** | PSL THREDDS NCSS (`accept=csv` unsupported, data through 2023) | NCEI CDR direct NetCDF download (through 2025, 2-day lag) | ✅ file listing |
| **GRACE gravity** | JPL PO.DAAC OPeNDAP (Earthdata 401) | GFZ GravIS RL06 TWS (public HTTPS, 496MB cached) | ✅ HEAD 200 |
| **Ocean color** | NASA OB.DAAC OPeNDAP (Earthdata 401, ended 2022) | CoastWatch ERDDAP `noaacwNPPN20S3ASCIDINEOF2kmDaily` (VIIRS+OLCI, 2018-present) | ✅ curl 2025 data |
| **Soil moisture** | NASA AppEEARS (Earthdata 401, ended 2022) | CPC ERDDAP (primary, 2011-present) + NOAA SMOPS (fallback, 2017-2022) | ✅ curl 2025 data |
| **Tide gauge** | UHSLC `.dat` files (404, URLs moved) | UHSLC ERDDAP `global_hourly_fast` (19 Japan stations, was 9) | ✅ curl 2025 data |
| **GOES X-ray** | LISIRD `goes_xrs_flare_daily` (endpoint removed) | LISIRD `noaa_goes16_xrs_1m` (2017+) + `goes15` (2011-2016), daily max | ✅ JSON both sats |
| **InSAR** | LiCSAR wrong frame IDs + broken catalog API | 34 correct Japan frames (Morishita 2021) + GeoTIFF parser + rasterio | ✅ JASMIN 200 |
| **IOC sea level** | `station.get("code")` crash on None values | None-safe parsing + dict/list response support + case-insensitive keys | ✅ station list |
| **Lightning** | Blitzortung archive restricted (no historical data) | **ISS LIS** via GHRC DAAC (2017-2023, CMR search + NetCDF) + **WWLLN Monthly Thunder Hour** (2013-2025 月次補完, GHRC `wwllnmth`) | ✅ CMR granules |
| **Nightlight** | Stub code (returned empty, 5% implemented) | **VNP46A4** HDF5 tile download + h5py parse (2012-present, annual) | ✅ LAADS catalog |
| **Cloud fraction** | Variable name `Cloud_Fraction_Mean_Mean` (wrong) | Fixed to `Cloud_Fraction_Mean` | ⏳ CI auth test |
| **SO2** | Filename pattern missing revision timestamp | OPeNDAP catalog-based filename discovery | ⏳ CI auth test |
| **Earthdata auth** | Bearer token stripped on cross-origin redirect (all OPeNDAP 401) | Username/password BasicAuth for URS redirect flow | ✅ secrets set |
| **Animal** | Movebank has no Japan GPS data | **Removed** (79→78 features) | — |

**Net result**: 11 broken sources fixed + 2 new sources (ISS LIS, VNP46A4) + 1 removed (animal). 8 sources switched to auth-free alternatives. All verified with curl before commit. Expected active features: **71-74/78** (from 57/79).

### Phase 15g Results — Test AUC 0.7540, 75 active features

| Metric | Phase 14 | Phase 15 | Phase 15g | Phase 15i | Change |
|---|---|---|---|---|---|
| CV AUC (pooled) | **0.7415** | 0.7411 | 0.7415 | 0.7417 | ±0 |
| Test AUC | 0.7485 | 0.7499 | **0.7540** | 0.7485 | −0.0055 |
| Active features | 65/79 | 70/78 | **75/78** | 76/78 | +11 |
| Stacking (logistic) | 0.7484 | — | — | 0.7458 | — |

**Note**: Phase 15h added SO2 (408K rows) and 15i fixed coordinate snapping, but AUC was unchanged — root cause identified: spatial features had <2% non-zero rate due to (1) event-driven fetch strategy (only M6+ ±7 day windows) and (2) monthly/annual data not expanded to daily lookup keys. **Phase 16 fixes both issues** — continuous daily fetch + temporal expansion in load functions.

**Data validation (Phase 15g: 25 OK / 4 EMPTY / 1 MISSING)**:

| Status | Tables |
|---|---|
| ✅ OK (25) | earthquakes, focal_mechanisms, tec, gnss_tec, geomag_kp, geomag_hourly, cosmic_ray, olr, earth_rotation, solar_wind, gravity_mascon, soil_moisture, ocean_color, goes_xray, goes_proton, tidal_stress, particle_flux, dart_pressure, ioc_sea_level, modis_lst, ulf_magnetic, cloud_fraction, iss_lis_lightning, **tide_gauge** (2.4M rows), **nightlight** (950 rows) |
| ❌ EMPTY (4) | so2_column, lightning, satellite_em, collector_status |
| ❌ MISSING (1) | snet_pressure (Phase 15g snapshot — later **deprecated 2026-04-25** in Phase 1 Step 4aa: HinetPy has no S-net BPR access path) |

Phase 15h: **SO2 408,351行取得成功** (0→408K, OPeNDAP parser fix + Hyrax approval) but AUC unchanged — **coordinate mismatch bug discovered**: 7 spatial data loaders (OLR, GRACE, SO2, soil moisture, ocean color, cloud fraction, nightlight) were using raw data source coordinates as lookup keys instead of snapping to the 2° prediction grid via `cell_key()`. All spatial features from these sources were silently zero despite having data in the DB. Fixed in Phase 15i.

Phase 15i (complete): Coordinate snap fix verified — SO2 non-zero rate improved from 0% to 2.0% (3,447/175,518), but AUC unchanged (0.7485). All spatial features confirmed active but with very low non-zero rates: OLR 96.4%, cloud 8.2%, SO2 2.0%, soil 1.1%, gravity 0.8%, ocean 0.5%, nightlight 0.1%.

**Root cause analysis**: Two independent bugs kept spatial features ineffective:
1. **Event-driven fetch**: SO2 and cloud_fraction only fetched ±7 days around M6+ earthquakes — no continuous baseline for anomaly detection
2. **Temporal resolution mismatch**: GRACE (monthly), soil moisture (monthly), nightlight (annual) data stored as single date entries, but feature extractor looks up daily date strings → 99%+ miss rate

Phase 16 (timeout): Continuous daily fetch + temporal expansion implemented. Fetch completed (SO2 2.3M rows 11.6% coverage, cloud 547K rows 21.7%) but **6-hour GitHub Actions hard limit hit before ML phase**. DB checkpoint (610MB) saved — data accumulation successful.

Phase 17 (cancelled): CI split into 2 jobs (fetch 350min + analyze 350min) to bypass 6h/job limit. Added `diagnose_data_gaps.py`. Run manually cancelled before completion.

Phase 18 (testing): **S-net waveform feature extraction** — replaced single `snet_pressure_anomaly` with 7 multi-scale waveform features from 151 ocean-bottom accelerometers (0120A, 100 Hz, 3-component). Features: RMS anomaly, H/V spectral ratio, low-freq power (slow-slip proxy), high-freq power (microseismicity), spectral slope, along-trench spatial gradient, per-segment max anomaly. Total features: 78 → 84. New DB table `snet_waveform` with incremental backfill (2016-08 to present). Discord progress notifications during fetch. Test run validating feature extraction + investigating 0120/B/C network codes for additional velocity data.

CSEP Benchmark: ML_HistGBT Molchan skill **0.9811** (best), beating Simple_ETAS (0.8713), Relative_Intensity (0.7745), Smoothed_Seismicity (0.2220).

Feature matrix exported to Google Drive for Colab GPU experiments. (Historical: also mirrored to BigQuery `geohazard.feature_matrix` until 2026-05-11; BQ pipeline retired per PR #156 / #157.)

### Roadmap

| Phase | Status | Goal |
|---|---|---|
| **Phase 12** | ✅ Complete | Data acquisition fixes + feature stability selection + FeatureExtractor 20h→12min |
| **Phase 13** | ✅ Complete | DART ✅, IOC ❌ (crash), S-net ❌ (auth). **CV 0.7416** (best). Stability selection validated |
| **Phase 14** | ✅ Complete | IOC fix + diverse stacking (RF/LR) + ConvLSTM full features. **Test AUC 0.7485** (best). Stacking ≒ base |
| **Phase 14b** | ✅ Complete | Data acquisition overhaul: 57→71+ features (see table above) |
| **Phase 15** | ✅ Complete | 70/78 active features. **Test AUC 0.7499** (best ever). Data preservation validated |
| **Phase 15b** | ✅ Complete | Earthdata Bearer auth rewrite + ISS LIS table fix + workflow 420min timeout. AUC 0.7499 maintained |
| **Phase 15c** | ✅ Complete | cloud_fraction ✅ (120K rows), ISS LIS ✅ (537 rows). Feature matrix export fixed (14h→sec) |
| **Phase 15d-f** | ✅ Complete | tide_gauge ✅ (2.4M rows), nightlight ✅ (950 rows), electron flux ✅ (80→3,316 rows). SO2 still EMPTY |
| **Phase 15g** | ✅ Complete | **Test AUC 0.7540** (best ever), 75 active features. electron flux SEISS L2 大幅増が効いた |
| **Phase 15h** | ✅ Complete | SO2パーサー修正 → **408,351行取得成功**（0→408K）。AUC変化なし（座標不一致で特徴量未反映と判明）。BQへfeature_matrix保管 |
| **Phase 15i** | ✅ Complete | 座標スナップ修正OK、SO2 0%→2%。但しAUC変化なし（非ゼロ率低すぎ）。根本原因: イベントベースfetch + 月次/年次データの日次lookup不整合 |
| **Phase 16** | ⏱️ Timeout | SO2/cloud連続日次fetch成功（SO2 2.3M行、cloud 547K行）、但し6h制限でMLに未到達。DB checkpoint保存済み |
| **Phase 17** | ❌ Cancelled | CI 2ジョブ分割 + ギャップ診断。手動キャンセル |
| **Phase 18** | ✅ Complete | **S-net波形特徴量**: 1→7特徴量（RMS/HV比/帯域パワー/スペクトル傾斜/空間勾配/セグメント最大anomaly）。75→84特徴量 |
| **BQ Integration** | 🛑 Retired (2026-05-11) | Sandbox 10 GB 上限 98 % 到達 + READ パスゼロにつき PR #156 で upload step を全削除。 過去の貢献: Phase 15h の座標ミスマッチバグを集計クエリで即座に発見。 一次保存先は RPi5 SSD (PR #157) へ移行 |
| **Bayesian Horseshoe** | 🧪 CV-tested | 1-fold smoke **0.8029 did not hold under 6-fold walk-forward CV** (numpyro SVI pooled AUC 0.7643). Top features (xray_flux, geomag_fractal, polar_motion) confirmed informative, but the single-fold 0.80 was optimistic — see the 2026-06-06 walk-forward benchmark above |
| **ConvLSTM** | 🟢 Colab-ready | Spatiotemporal neural network. Script + feature_matrix.json deployed to Drive |
| **SeismoGNN** | 🟢 Colab-ready | Graph Attention Network with fault-network topology. Script deployed to Drive |
| **Transformer** | 📋 Next | SafeNet-style multi-window features (7/14/30/90/365d) + attention (SafeNet, Sci. Reports 2025) |
| **PINN** | 📋 Next | Physics-Informed NN with Rate-State friction loss (Nature Comms 2023) |
| **Phase 19** | 🔄 Running | S-netマルチセンサー（0120速度+0120C高感度+0120A加速度）+ VLFスペクトル。84→92特徴量。ワークフロー修正: S-net前半移動+incremental save（タイムアウト時データ喪失防止）+SMAP無効化 |
| **S-net** | ✅ Active | NIED承認済。圧力チャンネル不在→**波形特徴量**に転換。0120A(加速度)確認済み、0120(速度)+0120C(高感度)をPhase 19で追加 |
| **Data Backfill** | 🔄 Running | `backfill.yml`: 全28+ fetcher を3時間毎cron（8スケジュール、24/7）で実行。 **fully cloud-native (PR #166、 2026-05-28)**: merge job は `ubuntu-latest` で走り、 scratch artifacts (light/modis/so2/cloud/snet/hinet) は runner の `/mnt/merge` (ephemeral disk ~74 GB) に展開、 結果 `geohazard.db` は **Hugging Face dataset [`yasumorishima/japan-geohazard`](https://huggingface.co/datasets/yasumorishima/japan-geohazard)** へ一次保存 (UTC 00:00 cron のみ + LFS history squash)。 GH Actions checkpoint artifact (30-day) が per-run working tier。 退役した RPi5 self-hosted runner + USB SSD primary tier (PR #157/#159) を置換。 Discord通知（coverage %） + 失敗時Issue自動作成。 100%到達でcron頻度削減 |
| **Operational OEF** | 🟢 Live | `scripts/operational_forecast.py`: monthly 34-day M5+ cell probabilities (calibrated ensemble), prospective commits to `forecasts/`, self-scoring after each window closes (AUC/Brier + CSEP information gain vs climatology). RPi5 cron, 1st of each month. Honest framing: OOS AUC 0.863 is mostly spatial climatology (0.854); the actionable signal is the above-normal ratio |
| **Skill decomposition + CSEP reframing** | ✅ Complete (2026-06) | Pooled-AUC skill = static climatology (0.862) + aftershock clustering; exogenous channels null; continuous regional ETAS +3.69 nats/event over climatology (see the 2026-06 sections under Analysis Results) |
| **Nucleation (raw waveforms)** | ✅ Complete - sixteen pre-registered tests (onshore arc + offshore S-net near-field), all null (occurrence rate, horizontal + depth-resolved 3D cross-correlation geometry, repeater/slow-slip family, b-value magnitude, below-catalogue tremor, dv/v medium-velocity, a self-supervised learned waveform embedding across Kumamoto + Iquique, and 5-min kinematic GNSS geodesy at the Kumamoto rupture) | SeisBench PhaseNet + PyOcto micro-catalogues from continuous waveforms (Kaggle GPU); Iquique 2014 pipeline validated (4,806 events, documented foreshock migration recovered); Tohoku 2011 Hi-net episode complete - pre-registered prospective test final verdict 0/3 after detection-sensitivity equalization (matched filter; see the 2026-06 nucleation section); case 2: fully-onshore Kumamoto 2016 — 2,608-event detection-uniform micro-catalogue, pre-registered verdict 0/3: aggregate criteria do not mark the mainshock even with full array resolution; seventh test: the below-catalogue masked-residual tremor channel is also null (0/3) - the one apparent migration PASS is exposed by a control as an amplitude-weighted aperture artifact (incoherent windows migrate identically); eighth test: a qualitatively different observable, seismic-velocity change (dv/v single-station autocorrelation), is also null (0/2, flat within inter-station noise over the 28 h foreshock window); fifth test: repeater/template-family slow-slip proxy null (0/3) after family-matched-filter equalization - the provisional migration PASS did not survive uniform detection sensitivity; sixth test: b-value channel (Gulia & Wiemer's own Kumamoto case) on network relative magnitudes - B1 (background drop) and B2 (b-vs-time decline) unmeasurable/SKIP and the two available trend bins run the wrong way, B3 PASS = the future hypocentre patch is a retrospective low-b asperity (0.76 vs 0.86, thin margin) but not a prospective temporal precursor; ninth test: a self-supervised (SimCLR) learned waveform-embedding probe on Kumamoto is null - the strongest apparent late drift of the whole arc survives a time-shuffle (C1) but fails the surrogate-prospective (C2) and spatial-null (C3) controls, exactly what the multi-control design was built to expose; tenth test: a multi-case extension of the same SSL harness to Iquique 2014 (Mw8.1 megathrust, literature-reported foreshock migration) is also null and cleaner - essentially no temporal drift toward the mainshock and no recovery of the geodetically-reported migration; eleventh test: the geodetic channel itself - NGL 5-min kinematic GNSS at GEONET stations on the Kumamoto rupture - is null too across model-free (raw and common-mode-filtered) statistics and a best-chance forward slip-model inversion: the apparent near-field transient is a network common-mode floor (far-field controls exceed equally), and an optimally grid-searched elastic dislocation explains less of the window net-displacement field (12%) than it does of quiet baseline windows (17% median), consistent with the inferred slow slip being at or below this method's prospective resolution; a 2014 Iquique positive control (documented cGPS slow slip) is also null on the identical pipeline, so the geodetic null is method-sensitivity-bounded, not a demonstrated absence; synthetic injection quantifies the floor at Mw ~5.6 (Kumamoto, dense onshore) to ~6.9 (Iquique, offshore) with the precursors beneath it, and a network-design calculation shows seafloor GNSS-A above the rupture would reach the ~Mw 6 precursor scale offshore - the null specifies the observing system needed rather than proving absence |

### Persistence Tiers

`backfill.yml` の merge job が出力する `geohazard.db` の保存階層 (PR #166、 2026-05-28 以降、 fully cloud-native):

| 階層 | 場所 | 役割 | 容量 | 保持 |
|---|---|---|---|---|
| Canonical (durable) | **Hugging Face dataset** [`yasumorishima/japan-geohazard`](https://huggingface.co/datasets/yasumorishima/japan-geohazard) `geohazard.db` (+ sidecar `geohazard.db.rowcounts.json`) | merge job が `scripts/hf_sync.py` で upload (integrity_check fail-closed + **row-count no-regression guard**: sidecar manifest と per-table 比較で degraded 上書き防止 — compaction は通過・テーブル脱落は拒否、 旧 byte-size guard を 2026-07-05 に置換 + 絶対下限 5 GB + `super_squash_history` で LFS 履歴を畳む) | LFS ~42 GB (2026-07-29 実測、 2026-07-05 の compaction 後に `ioc_sea_level` 等の backfill で再成長) | 永続 (public dataset) |
| Working (per-run) | **GH Actions artifact** `backfill-checkpoint-<run_id>` | merge job が常時 upload、 各 fetch job の restore step が次 run で読む incremental chain | 30 GB cap | **3 day rolling** (`e759fe5`、 2026-07-15 に 30→3 へ短縮。 この保持期間を超える run ドラウトが起きると chain が失効する — 2026-07-24 の事象を参照) |
| Scratch (during merge) | **`ubuntu-latest` ephemeral** `/mnt/merge/{light,modis,so2,cloud,snet,hinet}/geohazard.db` | `actions/download-artifact@v4` が merge job 中に展開 (root fs に dst、 `/mnt` に inputs を分散して ~33 GB ピーク (`shutil.copyfile(base, dst)`) を 2 FS に振り分け) | ~33 GB peak | 一時 (job 終了で破棄) |

> Hugging Face upload は **UTC 00:00 cron の1本のみ**で発火 (LFS 履歴肥大防止)。 targeted `workflow_dispatch` (`target != 'all'`) では Canonical / Working tier への upload を skip (該当 run が部分テーブルしか更新しないため、 他テーブルの状態を巻き戻すのを防ぐ)。 緊急時は `.github/workflows/reseed-checkpoint-from-hf.yml` で Canonical → Working chain を再シード可能。

#### Historical: GCP BigQuery Data Platform (2026-04-12 〜 2026-05-11、 retired)

GCP プロジェクト `data-platform-490901` の `geohazard` データセットへ feature matrix + メタデータ + 生データ全 31 テーブルを集約していました (`load_raw_to_bq.py` / `load_to_bq.py`)。 2026-05-11 に **PR #156** で upload step を全削除して retire:

- **理由**: Sandbox 10 GB 上限が `ioc_sea_level` 成長で 98 % 到達。 codebase audit (`grep -rln 'SELECT.*FROM.*geohazard\.'`) で BQ READ パスゼロを確認、 write-only archive 化していた。
- **貢献の記録**: Phase 15h の空間データソース座標ミスマッチバグ (`AVG(so2_column_anomaly) = 0.0`) を BQ 集計クエリで即座に発見、 Phase 15i で 7 ソース一括修正。
- **データ**: 2026-05-12 に dataset 全削除 (34 table + 2 view、 9.3 GB)。 SSD primary tier が PR #157 以降 4 連続 cron success で完動確認 + READ パスゼロにつき archive 不要と判断。 Sandbox quota 93% → 0% 解放。
- **後継**: 直後は RPi5 USB SSD primary tier (PR #157、 2026-05-11) へ移行。 その SSD も 2026-05-27 に物理故障し、 PR #166 で **Hugging Face dataset を新 Canonical tier** に再移行 (上記 Persistence Tiers 参照)。 ad-hoc 集計は HF から `hf download yasumorishima/japan-geohazard geohazard.db --repo-type dataset` で取得 → `sqlite3 ./geohazard.db` で直接クエリ。

### Not yet implemented

| Data | Blocker |
|---|---|
| Groundwater levels | 国交省水文水質DB prohibits programmatic access |
| S-net / DONET seafloor pressure | NIED approved (2026-03-23). Pressure channels absent in all 4 codes — using waveform features instead (Phase 18/19) |
| InSAR deformation | Code ready, LiCSAR JASMIN has Japan frame definitions but no processed interferograms (disabled 2026-03-20) |
| Blitzortung lightning | Archive access restricted (403). Sferics Bonn DNS 失効 (`sferics.uni-bonn.de`)。代替として ISS LIS (NASA GHRC, 2017-2023) + WWLLN Monthly Thunder Hour (NASA GHRC `wwllnmth`, 2013-2025 月次) で補完 |
| CSES satellite EM | Registration required at CSES data center |
| Radon / He isotopes | AIST monitoring data has limited public access |
| Hi-net waveforms | Research fetch workflow live (2026-06-10, `fetch-hinet-research.yml`: window-scoped SAC export for the nucleation study). Continuous bulk ingestion into the feature DB remains out of scope (volume) |
| VLF radio propagation | Research data only (Tokai/Chiba University) |
| Schumann resonance | No documented download API (HeartMath GCI live only) |
| CTBTO infrasound | IMS data restricted (vDEC contract) |

## Data Attribution

- Earthquake data: USGS, P2P地震情報, 気象庁
- Focal mechanisms: Global CMT Project (Ekström et al., 2012)
- AMeDAS / Volcano: 気象庁
- Geomagnetic: NOAA SWPC, GFZ Potsdam, WDC Kyoto (Kakioka Observatory)
- SST: NASA JPL MUR SST v4.1 via NOAA ERDDAP
- Ionosphere TEC: CODE (University of Bern), Nagoya University ISEE GNSS-TEC
- Land Surface Temperature: NASA MODIS MOD11A1 via LAADS DAAC
- GEONET: 国土地理院 (Geospatial Information Authority of Japan)
- Cosmic rays: NMDB (Neutron Monitor Database, nmdb.eu), operated by 9 NMDB stations (IRKT/OULU/PSNM/APTY/JUNG/ATHN/ROME/BKSN/AATB)
- Animal tracking: Movebank (movebank.org), Max Planck Institute of Animal Behavior
- Lightning: Blitzortung.org community lightning network, University of Bonn sferics archive, NASA WWLLN (Univ. of Washington) via GHRC DAAC, NASA ISS LIS via GHRC DAAC
- Satellite EM: CSES-Limadou (ASI/SSDC), INTERMAGNET (BGS Edinburgh GIN)
- Outgoing longwave radiation: NOAA Climate Data Record (CDR) OLR Daily
- Earth orientation: IERS (International Earth Rotation and Reference Systems Service)
- Solar wind: NASA OMNIWeb (SPDF/GSFC), ACE/DSCOVR/Wind spacecraft
- Gravity: NASA/DLR GRACE/GRACE-FO, JPL Mascon RL06.3v04 (PO.DAAC)
- Atmospheric SO2: NASA OMI OMSO2e Level 3 (GES DISC)
- Soil moisture: NASA SMAP L3 (NSIDC) via AppEEARS
- Tide gauge: University of Hawaii Sea Level Center (UHSLC) Research Quality
- Ocean color: NASA MODIS Aqua Level 3 chlorophyll-a (OB.DAAC)
- Cloud fraction: NASA MODIS Terra MOD08_D3 (LAADS DAAC)
- Nighttime light: VIIRS Day/Night Band (EOG, Colorado School of Mines / LAADS DAAC)
- InSAR: ESA Sentinel-1 via COMET LiCSAR (NERC/JASMIN)
- Ocean bottom pressure (DART): NOAA National Data Buoy Center (NDBC), public domain
- Sea level monitoring: Flanders Marine Institute (VLIZ); Intergovernmental Oceanographic Commission (IOC). Sea level station monitoring facility. DOI: [10.14284/482](https://doi.org/10.14284/482). **Commercial use prohibited.**
- Seafloor pressure (S-net): National Research Institute for Earth Science and Disaster Resilience (NIED). NIED Hi-net, DOI: [10.17598/NIED.0003](https://doi.org/10.17598/NIED.0003). **Citation, acknowledgment, and reprint submission required. Redistribution prohibited.** See [Hi-net terms](https://www.hinet.bosai.go.jp/about_data/?LANG=en)

## Data Usage Notes

Some data sources have specific usage requirements beyond standard academic citation:

| Source | License | Requirement |
|---|---|---|
| **NIED Hi-net/S-net** | Custom (strict) | Acknowledge NIED + all data-providing institutions. Send reprints to NIED (Tsukuba). Cite DOI: [10.17598/NIED.0003](https://doi.org/10.17598/NIED.0003). Cite Okada et al. (2004) doi:10.1186/BF03353076. **Redistribution prohibited. Non-compliance may result in service termination.** |
| **INTERMAGNET** | CC BY-NC 4.0 | Acknowledge: *"The results presented in this paper rely on data collected at magnetic observatories. We thank the national institutes that support them and INTERMAGNET for promoting high standards of magnetic observatory practice."* Send citations to INTERMAGNET Secretary. **Non-commercial only.** |
| **NMDB** | Non-commercial | Acknowledge: *"We acknowledge the NMDB database (www.nmdb.eu), founded under the European Union's FP7 programme (contract no. 213007) for providing data."* + per-station acknowledgments (see nmdb.eu/station). **Non-commercial only.** |
| **Global CMT** | Citation required | Cite: Ekström, G., M. Nettles, and A.M. Dziewoński (2012) Phys. Earth Planet. Inter. 200-201:1-9, doi:[10.1016/j.pepi.2012.04.002](https://doi.org/10.1016/j.pepi.2012.04.002) |
| **GFZ Kp index** | CC BY 4.0 | Cite GFZ as data source. DOI: [10.5880/Kp.0001](https://doi.org/10.5880/Kp.0001). Ref: Matzka et al. (2021) doi:[10.1029/2020SW002641](https://doi.org/10.1029/2020SW002641) |
| **Kakioka Observatory** | JMA terms | DOI assigned per dataset (see kakioka-jma.go.jp). Source: JMA. |
| **IOC Sea Level** | Non-commercial | Cite VLIZ/IOC with DOI: [10.14284/482](https://doi.org/10.14284/482). **Commercial use prohibited.** |
| **COMET LiCSAR** | Copernicus terms | Acknowledge: *"LiCSAR contains modified Copernicus Sentinel data [year] analysed by COMET. LiCSAR uses JASMIN."* Cite Lazecký et al. (2020) Remote Sensing. |
| **JMA (earthquake/AMeDAS/volcano)** | PDL1.0 (≈CC BY 4.0) | Source: Japan Meteorological Agency website. Meteorological Service Act restrictions apply to forecast services. |
| **GSI GEONET** | PDL1.0 | Source: GSI website (url). |
| **P2P地震情報** | CC BY 4.0 | Attribute 気象庁 for earthquake data (pre-2021/4/4). Commercial use OK. |
| **NOAA (DART/SWPC/NDBC/ERDDAP)** | Public domain | Do not imply NOAA endorsement. |
| **NASA (Earthdata sources)** | Open data | Cite specific datasets per NASA data policy. |
| **Nagoya Univ. ISEE GNSS-TEC** | ©Nagoya Univ. | Cite Shinbori et al. (2022) doi:[10.1029/2021JA029687](https://doi.org/10.1029/2021JA029687). Acknowledge IUGONET + NICT Science Cloud. List GNSS data providers. |
| **CODE (Univ. Bern) TEC** | Citation required | Cite Dach et al. (2024) DOI: [10.48350/197025](https://doi.org/10.48350/197025) |
| **Movebank** | Per-study license | Follow per-study license. Contact data owner for non-CC0 data. |
| **UHSLC** | ©UHSLC | Standard academic citation. SA stations require SANHO permission. |

## Related

Part of the [Realtime Open Data](https://github.com/yasumorishima/realtime-open-data) project collection.

## License

MIT

## Related (private)

- Forecasting research probe & analysis tooling: <https://github.com/yasumorishima/geohazard-research-private> (private)
