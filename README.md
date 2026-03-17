# Japan Geohazard Monitor

![Live Map](docs/screenshot.png)

Real-time monitoring dashboard for Japan's geophysical activity — earthquakes, volcanoes, atmospheric conditions, geomagnetism, ocean temperature, ionosphere, and crustal deformation — all overlaid on a single dark-themed interactive map with a correlation analysis panel.

9 async collectors run continuously on a Raspberry Pi 5, pulling data from 10 public APIs and storing it in SQLite. A FastAPI server renders a Leaflet.js dashboard with togglable layers and a time-synchronized correlation panel for cross-domain anomaly detection. Mobile responsive.

## Live

`http://100.77.198.48:8003` (Tailscale)

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
ssh yasu@100.77.198.48 "cd ~/japan-geohazard-monitor && sudo git pull && sudo docker-compose up -d --build"
```

## Phased Development

- **Phase 1** ✅ Earthquakes (3 sources: USGS, P2P, JMA)
- **Phase 2** ✅ Atmospheric (AMeDAS 1,286 stations) + Geomagnetic (NOAA SWPC GOES + Kp)
- **Phase 3** ✅ Volcanoes (JMA 117 active) + Ocean (NOAA ERDDAP MUR SST)
- **Phase 4** ✅ Ionosphere TEC (CODE Bern predicted IONEX) + GEONET crustal deformation (GSI SFTP, 218 stations)
- **Correlation** ✅ Time-synchronized 5-chart panel (earthquake/Kp/GOES/TEC/pressure)
- **Analysis Phase 1** ✅ b-value, TEC, Kp, multi-indicator grid search → all negative (aftershock/sampling artifacts)
- **Analysis Phase 2** ✅ Coulomb stress (lift 37.5 isolated), rate anomaly (lift 1.86), clustering (lift 4.12) — all survived aftershock isolation + prospective test (combined lift 20.66)
- **Analysis Phase 3a** ✅ LURR (❌), Natural Time (❌), Nowcasting (⚠️ lift 1.31) — catalog-based methods exhausted
- **Analysis Phase 3b** ✅ MODIS LST (❌), ULF magnetic (⚠️ data limited to 80 days), GNSS-TEC 0.5° (31K records)
- **Analysis Phase 4** ✅ **Prospective (forward-looking) prediction**: ETAS residual (gain 4.0x), foreshock (5.1x), cumulative CFS (2.4x), combined alarm (**7.8x, 62.5% precision**). Pattern Informatics (Molchan AUC 0.349)
- **Analysis Phase 5** ✅ ML integration: AdaBoost ensemble (11 features, pure Python) — AUC 0.73
- **Analysis Phase 6** ✅ ML overhaul: HistGradientBoosting (35 temporal features), walk-forward CV (0.740 ± 0.016), ETAS MLE per zone, rate-and-state CFS, isotonic calibration — **AUC 0.746**
- **Analysis Phase 7** ✅ Spatial correlation + GNSS + zone ETAS: 47 features (+6 GNSS crustal deformation, +6 enhanced spatial), zone-specific ETAS in feature extraction, 2-pass Gaussian spatial smoothing — **AUC 0.749 (CV 0.741)**
- **Analysis Phase 8** 🔄 Structural overhaul: multi-target (M5+/M5.5+/M6+), CSEP benchmark (4 reference models + N/L/T-test), ensemble stacking (8-input physics×ML meta-learner), ConvLSTM spatiotemporal neural network (Colab GPU)
- **Backfill** ✅ 2011-2026 M3+ earthquakes (29K), TEC (4M), Kp (44K), GCMT focal mechanisms
- **CI/CD** ✅ GitHub Actions weekly analysis workflow (fetch → analyze → artifact, 240min timeout)
- **Mobile** ✅ Responsive design (bottom sheet panel, touch-optimized controls)

## Analysis Results (2011-2026, 28K M3+ earthquakes, 4M TEC, 44K Kp, 31K GNSS-TEC, 345K ULF)

### Summary

Phase 1 indicators (b-value, Kp, low-res TEC) were all negative after bias correction. Phase 2 found 3 physics-based signals that survived aftershock isolation and prospective testing. **Phase 4 forward-looking evaluation achieved 62.5% precision (7.8x gain) by combining ETAS residual + cumulative CFS + foreshock alarms.**

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
| `fetch_gnss_tec.py` | Nagoya Univ. ISEE (AGRID2/GRID2 netCDF) | GNSS-TEC 0.5° grid, 1h temporal, 31K records (no auth, 2 hrs/day × 30 dates/run) |
| `fetch_modis_lst.py` | ORNL DAAC TESViS API | MODIS LST 1km: M5.5+ land epicenters ±14d + random control (rate limited) |
| `fetch_kakioka_ulf.py` | INTERMAGNET BGS GIN + WDC Kyoto | KAK/MMB/KNY 1-min geomagnetic: M6+ events ±7d (IAGA-2002 format) |

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
| `ml_prediction.py` | 8 | Multi-target ML (M5+/M5.5+/M6+): 47 features → HistGradientBoosting with class weighting, walk-forward CV, zone-specific ETAS MLE, 2-pass spatial smoothing, level-0 export for stacking | van den Ende & Ampuero (2020), Kato et al. (2012) |
| `export_csep.py` | 8 | CSEP-compatible XML/JSON forecast export from ML predictions | Schorlemmer et al. (2007) |
| `csep_benchmark.py` | 8 | CSEP benchmark: Uniform/Smoothed/RI/ETAS reference models + N/L/T-test + Molchan diagram | Helmstetter (2007), Rhoades (2004) |
| `stacking_analysis.py` | 8 | Ensemble stacking: 8-input level-0 (ML×3 + physics×5) → logistic/isotonic meta-learner | Wolpert (1992) |
| `export_feature_matrix.py` | 8 | 4D tensor export (timesteps×H×W×47) for ConvLSTM GPU training | — |

### Shared modules (`src/`)

| Module | Purpose |
|---|---|
| `physics.py` | Okada (1992) CFS, Wells & Coppersmith (1994) fault scaling, ETAS MLE (scipy L-BFGS-B), Dieterich (1994) rate-and-state, b-value (Aki-Utsu), tectonic zone classification, GNSS strain rate estimation, slow-slip transient detection |
| `features.py` | 47 temporal features: rate dynamics (acceleration, trend), zone-specific ETAS residuals, magnitude statistics (deficit, b-value trend), clustering (foreshock escalation, inter-event CV), rate-and-state CFS, Pattern Informatics, Benioff strain, GNSS crustal deformation (displacement, strain rate, SSE detection), enhanced spatial (neighbor CFS/ETAS/mag, zone rate anomaly, CFS rank, spatial gradient) |
| `evaluation.py` | ROC-AUC, threshold evaluation (precision/recall/gain/IGPE/Molchan), walk-forward CV splits, isotonic calibration (PAV), reliability diagram, permutation importance, Molchan area skill score |
| `target_config.py` | Multi-target configuration: M5+/M5.5+/M6+ with per-target window, class weight, positive thresholds |
| `csep_format.py` | CSEP XML forecast generation: probability → GR-based rate per cell/magnitude/time bin |
| `stacking.py` | Ensemble stacking: level-0 registration, logistic/isotonic meta-learner, walk-forward stacking with temporal leak prevention |

Results saved as JSON artifacts (90-day retention). Runs every Monday 12:00 JST or on demand (240-min timeout).

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

### Phase 8: Structural Overhaul (in progress)

Phase 7 showed diminishing returns from feature engineering (+0.003 with 12 new features). Phase 8 attacks from 4 structural directions:

**Initiative 1: ConvLSTM Spatiotemporal Neural Network**
- 2-layer ConvLSTM with channel attention on 11×11×47 spatial grid
- Captures spatial patterns that cell-independent HistGBT cannot learn
- Training via RPi5 → Google Drive → Colab T4 GPU pipeline
- Walk-Forward CV with same splits as HistGBT for fair comparison

**Initiative 2: CSEP-Compatible Format + Benchmark**
- ML probability → CSEP XML rate forecast (2°×2° grid, 4 magnitude bins)
- 4 reference models: Uniform Poisson, Smoothed Seismicity (Helmstetter 2007), Relative Intensity (Rhoades 2004), Simple ETAS
- Statistical tests: N-test (Poisson consistency), L-test (log-likelihood), T-test (paired comparison), Molchan diagram

**Initiative 3: Multi-Target Prediction (M5+, M5.5+, M6+)**
- Per-target prediction windows: M5+/M5.5+ = 7 days, M6+ = 14 days
- Class weighting for extreme imbalance (M6+: weight=10)
- Level-0 prediction export for downstream stacking

**Initiative 4: Ensemble Stacking (Physics × ML)**
- 8-input level-0: HistGBT×3 targets + ETAS rate + CFS kPa + CFS rate-state + foreshock alarm + composite alarm count
- Level-1 meta-learner: Logistic regression / Isotonic regression
- Walk-forward stacking with temporal leak prevention

### Not yet implemented

| Data | Blocker |
|---|---|
| Groundwater levels | 国交省水文水質DB prohibits programmatic access |
| S-net / DONET seafloor pressure | NIED data access registration required |
| Radon / He isotopes | AIST monitoring data has limited public access |
| Hi-net waveforms | NIED registration + large data volume |

## Data Attribution

- Earthquake data: USGS, P2P地震情報, 気象庁
- Focal mechanisms: Global CMT Project (Ekström et al., 2012)
- AMeDAS / Volcano: 気象庁
- Geomagnetic: NOAA SWPC, GFZ Potsdam, WDC Kyoto (Kakioka Observatory)
- SST: NASA JPL MUR SST v4.1 via NOAA ERDDAP
- Ionosphere TEC: CODE (University of Bern), Nagoya University ISEE GNSS-TEC
- Land Surface Temperature: NASA MODIS MOD11A1 via LAADS DAAC
- GEONET: 国土地理院 (Geospatial Information Authority of Japan)

## Related

Part of the [Realtime Open Data](https://github.com/yasumorishima/realtime-open-data) project collection.

## License

MIT
