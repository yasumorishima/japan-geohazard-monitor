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
- **Backfill** ✅ 2011-2026 M5+ earthquakes (8,072), TEC (311K), Kp (18K)
- **Mobile** ✅ Responsive design (bottom sheet panel, touch-optimized controls)

## Analysis Results (2011-2026, 8,072 earthquakes)

### b-value (Gutenberg-Richter) — ✅ Confirmed precursor

Sliding 30-day window maximum likelihood b-value. Normal ≈ 1.0; low values indicate stress buildup.

**Control experiment** (random dates vs. pre-earthquake):

| Condition | n | Mean b | b < 0.7 | b < 0.5 |
|---|---|---|---|---|
| Random dates | 318 | 1.255 | 23% | 5% |
| **Before M5+** | **290** | **0.667** | **87%** | **77%** |
| **Before M7+** | **20** | **0.626** | **80%** | **15%** |

b < 0.7 occurs 3.8× more often before M5+ earthquakes than on random dates. This is not chance.

Individual major earthquakes:

| Earthquake | b-value | Random baseline |
|---|---|---|
| M9.1 Tohoku (2011-03) | **0.512** | 1.255 |
| M6.6 Hokkaido Iburi (2018-09) | **0.435** | 1.255 |
| M7.6 Aomori (2025-12) | **0.521** | 1.255 |
| M7.0 Kumamoto (2016-04) | **0.586** | 1.255 |
| M7.5 Noto (2024-01) | **0.65** | 1.255 |

### Epicenter TEC — ⚠️ Inconclusive (needs more data)

TEC within 5° of epicenter, 7-day baseline vs. 24h precursor.

**Control experiment**:

| Condition | n | Mean σ | Negative % | Drops (σ<-1) | Spikes (σ>+1) |
|---|---|---|---|---|---|
| Random loc+time | 34 | -0.761 | 97% | 15% | 0% |
| Before M5+ | 400 | -0.075 | 31% | 0% | 0% |
| Before M7+ | 16 | -0.596 | 81% | 25% | 0% |

Random dates also show TEC drops, so the signal may not be earthquake-specific. However, random sample n=34 is too small for a definitive conclusion (TEC backfill only covers ±7 days around major events, limiting random sampling).

### Global Lag Correlation — ❌ No signal

| Metric | M7+ peak r | M5+ peak r | Verdict |
|---|---|---|---|
| TEC (mean) | +0.01 | -0.05 | No signal (spatial averaging destroys it) |
| Kp | +0.02 | +0.04 | No signal |
| GOES | insufficient data | -0.46 | Needs more data |
| Pressure | insufficient data | insufficient data | No historical AMeDAS API |

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
