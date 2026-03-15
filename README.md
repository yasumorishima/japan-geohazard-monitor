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

**Stack**: Python 3.12 / asyncio + aiohttp + asyncssh / aiosqlite / FastAPI + Uvicorn / Leaflet.js + Chart.js / Docker

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

SQLite with WAL mode. 8 tables:

- `earthquakes` — dedup by (source, event_id)
- `amedas` — dedup by (station_id, observed_at)
- `geomag_goes` — dedup by (time_tag, satellite)
- `geomag_kp` — dedup by time_tag
- `volcanoes` — upsert by volcano_code (one row per volcano)
- `sst` — dedup by (lat, lon, observed_at)
- `tec` — dedup by (lat, lon, epoch)
- `geonet` — dedup by (station_id, observed_at)

Auto-purge: records older than 90 days deleted on each collector cycle.

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
- **Analysis** ✅ Anomaly detection (±2σ), lag correlation, epicenter TEC, b-value, multi-indicator grid search
- **Backfill** ✅ 2011-2026 M3+ earthquakes (28K), TEC (728K + random baseline), Kp (44K)
- **CI/CD** ✅ GitHub Actions weekly analysis workflow (fetch → analyze → artifact, 120min timeout)
- **Mobile** ✅ Responsive design (bottom sheet panel, touch-optimized controls)

## Analysis Results (2011-2026, 28K M3+ earthquakes, 4M TEC, 44K Kp)

### Phase 1: Single indicators — all negative

No single indicator predicts earthquakes when tested with proper controls (aftershock isolation filter, large random baseline).

**b-value (Gutenberg-Richter) — ❌ Aftershock artifact**

| Window | Random b<0.7 | All M5+ b<0.7 | Isolated M5+ b<0.7 |
|---|---|---|---|
| 7-day | 16.9% | 90.0% | **15.2%** (= random) |
| 30-day | 42.6% | 91.6% | **39.5%** (= random) |
| 90-day | 72.2% | 84.6% | **55.1%** (noise range) |

The 90% "signal" without isolation was entirely aftershock clustering. With isolation filter: no difference from random.

**Epicenter TEC (raw) — ❌ Systematic bias**

| Condition | n | Mean σ |
|---|---|---|
| Random | 373 | -0.781 |
| Before M5+ | 494 | -0.222 |

Random TEC drops *more* than pre-earthquake TEC. The -0.781 bias comes from seasonal/diurnal/solar cycle patterns, not earthquakes.

**Multi-indicator grid search (100 combos) — ❌ No signal**

Best lift across 100 threshold combinations (b×5 × Kp×5 × TEC×4): 1.82 at n=17. Fixed threshold (b<0.7, Kp>2.5, TEC<-1): earthquake 22.1% vs random 21.4% — identical distributions.

### Phase 2: Advanced analysis — two candidate signals found (validation in progress)

**TEC with seasonal/diurnal correction — ⚠️ 3.6× lift (needs validation)**

After removing monthly-hourly climatology bias, the TEC direction reverses:

| Condition | Raw σ | Detrended σ | Spikes (σ>+1) |
|---|---|---|---|
| Random | -0.781 | +0.247 | 15.6% |
| Before M5+ | -0.222 | **+0.942** | **56.5%** |

Pre-earthquake TEC *increases* after detrending. Consistent with the LAIC (Lithosphere-Atmosphere-Ionosphere Coupling) model: radon emission → air ionization → electric field → ionosphere enhancement.

**Kp temporal profile — ⚠️ 4.4× lift at -12h (needs aftershock filter validation)**

| Lead time | Pre-EQ Kp | Random Kp | Kp > 3 |
|---|---|---|---|
| -7 days | 2.29 | 1.77 | 16% vs 14% |
| -5 days | 1.49 | 1.83 | 2% vs 17% |
| -3 days | 1.43 | 1.70 | 7% vs 13% |
| **-24h** | **3.07** | 1.71 | **56% vs 14%** |
| **-12h** | **3.41** | 1.72 | **62% vs 14%** |
| -3h | 3.03 | 1.76 | 50% vs 16% |

Kp normal/low 3-5 days before → rapid spike peaking at -12h. Needs aftershock isolation filter to confirm this isn't clustering contamination.

**Mutual Information: TEC → next-day earthquake = 4.7× shuffled baseline**

| Pair | MI | Shuffled mean | Ratio |
|---|---|---|---|
| Kp → next-day EQ | 0.000057 | 0.000242 | 0.24 |
| **TEC → next-day EQ** | **0.002075** | **0.000440** | **4.71** |

Nonlinear dependence between daily TEC and next-day M5+ earthquake occurrence. Linear correlation (Pearson) missed this entirely.

### Validation in progress

Four validation analyses are running to test whether the Phase 2 signals are real:

1. **Kp profile with aftershock isolation** — does the -12h spike survive filtering?
2. **TEC + Kp combined** — is simultaneous TEC spike + Kp elevation more predictive?
3. **Temporal stability** — same pattern in 2011-2018 AND 2019-2026?
4. **Magnitude dependence** — does signal strengthen for M6+, M7+?

### What's next

Existing methods (b-value thresholds, raw TEC comparison, simple correlation) have been tried for decades without success. The path forward is **new data sources and new analytical perspectives**, not repeating what hasn't worked.

**Signal validation (if Phase 2 confirmed):**
- Alternative TEC detrending (30-day rolling mean) for robustness check
- Bootstrap confidence intervals
- Solar flare (F10.7) covariate analysis — is Kp -12h spike solar-driven?
- Earthquake mechanism classification (thrust/normal/strike-slip)

**New data sources — novel perspectives no one has combined:**

| Data | Physical basis | Source |
|---|---|---|
| **GEONET GPS-TEC** | 1,300 GPS stations = orders of magnitude higher spatial resolution than IONEX 2.5°×5° grid. Can examine TEC directly above epicenter without spatial averaging dilution | GSI GEONET / NICT |
| **OLR (Outgoing Longwave Radiation)** | LAIC model intermediate step: radon → ionization → **surface heating** → atmospheric coupling → ionosphere. If TEC enhancement confirmed, surface thermal anomaly should precede it. **Independent cross-validation** | NOAA CDR OLR (daily, 2.5°, 1979-present) |
| **GRACE-FO gravity anomalies** | Slow-slip and fluid migration redistribute mass → gravity field changes. Combined with GEONET surface displacement = 3D subsurface view | NASA JPL GRACE-FO Level-3 (monthly, 1°) |
| **Solar wind (ACE/DSCOVR)** | Separate solar-driven Kp from earthquake-related Kp. If the -12h Kp spike disappears after controlling for solar wind → it's solar, not seismic | NOAA SWPC + NASA OMNI (1-min, 1995-present) |
| **Coulomb stress transfer** | Physics-based: each earthquake adds stress to surrounding faults. Predict which fault is next. Combine with TEC/Kp for multi-domain approach. No new data needed — computable from existing 28K earthquake catalog | Okada (1992) model |

**Also planned:**
- GEONET F5 coordinates 2011-2025 (crustal deformation backfill)
- SST historical data via ERDDAP
- GOES magnetometer historical data

**Dashboard:**
- Add b-value time-series chart to correlation panel
- Multi-indicator anomaly highlight display
- Update screenshot (still shows Phase 1)

## Automated Analysis (GitHub Actions)

Weekly analysis workflow fetches fresh data from public APIs and runs control experiments.

```bash
# Manual trigger
gh workflow run "Earthquake Correlation Analysis" \
  --repo yasumorishima/japan-geohazard-monitor \
  -f memo="multi-indicator test" \
  -f min_mag=5.0 \
  -f analysis_type=all
```

| Script | Purpose |
|---|---|
| `scripts/fetch_earthquakes.py` | M3+ earthquakes from USGS (yearly chunks) |
| `scripts/fetch_kp.py` | Full Kp history from GFZ Potsdam |
| `scripts/fetch_tec.py` | IONEX TEC from CODE (Bern) — `--mode event` (M6.5+ ±7d) or `--mode random` (baseline) |
| `scripts/run_analysis.py` | b-value (with isolation filter), epicenter TEC, multi-indicator grid search (100 combos) |

Results saved as JSON artifact (90-day retention). Runs every Monday 12:00 JST or on demand.

### Not yet implemented

| Data | Blocker |
|---|---|
| Groundwater levels | No unified API. 国交省水文水質DB explicitly prohibits programmatic access |
| INTERMAGNET ground magnetometers (KAK/MMB/KNY) | BGS GIN API currently down — using NOAA SWPC GOES as alternative |

## Data Attribution

- Earthquake data: USGS, P2P地震情報, 気象庁
- AMeDAS / Volcano: 気象庁
- Geomagnetic: NOAA Space Weather Prediction Center
- SST: NASA JPL MUR SST v4.1 via NOAA ERDDAP
- Ionosphere TEC: CODE, Astronomical Institute, University of Bern
- GEONET: 国土地理院 (Geospatial Information Authority of Japan)

## Related

Part of the [Realtime Open Data](https://github.com/yasumorishima/realtime-open-data) project collection.

## License

MIT
