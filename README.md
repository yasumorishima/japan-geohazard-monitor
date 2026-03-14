# Japan Geohazard Monitor

![Live Map](docs/screenshot.png)

Real-time monitoring dashboard for Japan's geophysical activity — earthquakes, volcanoes, atmospheric pressure, geomagnetism, ocean temperature, ionosphere, and crustal deformation — all overlaid on a single dark-themed interactive map.

The goal is to collect every publicly available geophysical data stream around Japan and visualize potential correlations with seismic activity.

## Architecture

```
8+ async collectors (independent intervals per source)
    → BaseCollector (retry, batch insert, health tracking)
    → SQLite (WAL mode, auto-purge)
    → FastAPI REST API (per-layer endpoints)
    → Leaflet.js (dark theme, togglable layers, correlation panel)
    → matplotlib snapshot → GitHub (auto-push)
```

## Data Sources

### Phase 1: Earthquakes (3 sources) ✅

| Source | Endpoint | Auth | Interval |
|---|---|---|---|
| USGS | `earthquake.usgs.gov` GeoJSON feed | None | 5 min |
| P2P地震情報 | `api.p2pquake.net/v2` JSON | None | 2 min |
| 気象庁 Bosai | `jma.go.jp/bosai/quake` JSON | None | 3 min |

### Phase 2: Atmospheric + Geomagnetic ✅

| Source | Endpoint | Auth | Interval |
|---|---|---|---|
| AMeDAS (~1,300 stations) | `jma.go.jp/bosai/amedas` JSON | None | 10 min |
| NOAA GOES Magnetometer | `services.swpc.noaa.gov` JSON | None | 15 min |
| Planetary Kp Index | `services.swpc.noaa.gov` JSON | None | 15 min |

### Phase 3: Volcanoes + Ocean ✅

| Source | Endpoint | Auth | Interval |
|---|---|---|---|
| JMA Bosai 活火山 (120 volcanoes) | `jma.go.jp/bosai/volcano` JSON | None | 15 min |
| NOAA ERDDAP MUR SST (0.5° grid) | `coastwatch.pfeg.noaa.gov` JSON | None | 6 hours |

### Phase 4: Crustal + Ionosphere

| Source | Endpoint | Auth | Interval |
|---|---|---|---|
| GEONET GNSS (1,300 stations) | GSI SFTP | Registration | Daily |
| Ionosphere TEC | CODE/Bern IONEX | None | 2 hours |
| Groundwater | Prefectural open data | Varies | Varies |

## Map Layers

| Layer | Visualization | Color Scheme |
|---|---|---|
| Earthquakes | CircleMarker (magnitude ∝ radius) | Depth: shallow=red → deep=blue |
| AMeDAS | CircleMarker per station (toggleable metric) | Temp/Pressure/Wind/Precip colormaps |
| Kp Index | Header badge (real-time) | Green < 4, Orange 4-6, Red > 6 |
| Volcanoes (Phase 3) | Triangle markers | Alert level: 1=white → 5=purple |
| Ocean SST (Phase 3) | Tile overlay | Cool-warm colormap |
| GNSS Displacement (Phase 4) | Arrow vectors | Magnitude-proportional length |
| Ionosphere TEC (Phase 4) | Grid overlay | Low=transparent → high=purple |

## Correlation Panel

Right-side collapsible panel with time-synchronized charts across all data sources, enabling visual detection of cross-domain anomalies (e.g., ionosphere TEC drop → geomagnetic disturbance → earthquake).

## Related

Part of the [Realtime Open Data](https://github.com/yasumorishima/realtime-open-data) project collection.

## License

MIT
