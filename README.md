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
- **Analysis** ✅ Anomaly detection (±2σ), lag correlation, epicenter TEC, b-value
- **Backfill** ✅ 2011-2026 M3+ earthquakes (28,400), TEC (311K), Kp (18K)
- **CI/CD** ✅ GitHub Actions weekly analysis workflow (fetch → analyze → artifact)
- **Mobile** ✅ Responsive design (bottom sheet panel, touch-optimized controls)

## Analysis Results (2011-2026, 28,400 M3+ earthquakes)

No single indicator predicts earthquakes. This is consistent with the fact that no one has achieved earthquake prediction — if a single metric worked, it would already be in use.

### b-value (Gutenberg-Richter) — ❌ No signal with complete data

Initial analysis with M5+-only data appeared to show b-value drops before earthquakes (87% b<0.7 vs 23% random). **This was an artifact of incomplete data.** With M3+ complete data (28,400 events), the signal disappears:

| Condition | n | Mean b | b < 0.7 |
|---|---|---|---|
| Random dates | 1,000 | 0.591 | 72% |
| Before M4-4.9 | 300 | 0.600 | 74% |
| Before M5-5.9 | 300 | 0.590 | 76% |
| Before M7+ | 20 | 0.637 | 65% |

No statistically significant difference across any magnitude band. **Lesson: incomplete data produces false signals.**

### Epicenter TEC — ⚠️ Inconclusive

TEC within 5° of epicenter, 7-day baseline vs. 24h precursor.

| Condition | n | Mean σ | Negative % | Drops (σ<-1) | Spikes (σ>+1) |
|---|---|---|---|---|---|
| Random loc+time | 34 | -0.761 | 97% | 15% | 0% |
| Before M5+ | 400 | -0.075 | 31% | 0% | 0% |
| Before M7+ | 16 | -0.596 | 81% | 25% | 0% |

Random dates also show TEC drops (n=34 too small to be definitive). Signal may not be earthquake-specific.

### Global Lag Correlation — ❌ No signal

| Metric | M7+ peak r | M5+ peak r | Verdict |
|---|---|---|---|
| TEC (mean) | +0.01 | -0.05 | No signal (spatial averaging destroys it) |
| Kp | +0.02 | +0.04 | No signal |
| GOES | insufficient data | -0.46 | Needs more data |
| Pressure | insufficient data | insufficient data | No historical AMeDAS API |

### What's next

Single-indicator approaches have been exhausted by decades of seismology research. No one has achieved earthquake prediction because simple methods don't work. The path forward requires going beyond what existing papers have done.

**1. Multi-indicator simultaneous anomaly detection** (highest priority)
- Do b-value + TEC + Kp anomalies occurring *simultaneously* predict earthquakes better than any single indicator?
- Threshold combination search: vary b<0.6/0.7/0.8, Kp>3/4/5, TEC σ<-0.5/-1.0/-1.5
- Different time windows per indicator: b-value 90d + TEC 7d + Kp 48h

**2. Data expansion**
- TEC full-period backfill (currently only ±7d around M6.5+ events — random control n=34 is too small)
- SST historical data via ERDDAP for pre-earthquake SST anomaly verification
- GEONET F5 coordinates 2011-2025 for slow-slip detection before major earthquakes
- GOES magnetometer historical data (alternative source needed)

**3. Advanced analysis methods**
- Spatial clustering: subduction zone vs. inland fault vs. volcanic — different mechanisms may have different precursors
- Epicenter distance optimization: compare TEC at 1°/2°/5°/10° radius
- Solar/seasonal/diurnal TEC correction before earthquake correlation
- Nonlinear methods: Mutual Information, Transfer Entropy (Pearson only captures linear relationships)
- Time-series pattern recognition: detect TEC/Kp "shape" patterns (e.g., sharp drop → recovery)
- ML classification: all indicators as features → "M6+ within N days?" (extreme class imbalance challenge)

**4. Dashboard improvements**
- Add b-value time-series chart to correlation panel
- Multi-indicator anomaly highlight display
- Alert on simultaneous multi-indicator anomalies
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
| `scripts/fetch_tec.py` | IONEX TEC around M6.5+ events from CODE (Bern) |
| `scripts/run_analysis.py` | b-value, epicenter TEC, multi-indicator analysis with control experiments |

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
