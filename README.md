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

### Phase 1: Earthquakes (3 sources)

| Source | Endpoint | Auth | Interval |
|---|---|---|---|
| USGS | `earthquake.usgs.gov` GeoJSON feed | None | 5 min |
| P2P地震情報 | `api.p2pquake.net/v2` JSON | None | 2 min |
| 気象庁 Bosai | `jma.go.jp/bosai/quake` JSON | None | 3 min |

### Phase 2: Atmospheric + Geomagnetic

| Source | Endpoint | Auth | Interval |
|---|---|---|---|
| AMeDAS (~1,300 stations) | `jma.go.jp/bosai/amedas` JSON | None | 10 min |
| INTERMAGNET (KAK/MMB/KNY) | `imag-data.bgs.ac.uk` IAGA-2002 | None | 30 min |

### Phase 3: Volcanoes + Ocean

| Source | Endpoint | Auth | Interval |
|---|---|---|---|
| 気象庁 火山警報 (111 volcanoes) | `xml.kishou.go.jp` Atom XML | None | 15 min |
| NOAA ERDDAP SST | `coastwatch.pfeg.noaa.gov` JSON | None | 6 hours |

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
| Volcanoes | Triangle markers | Alert level: 1=white → 5=purple |
| AMeDAS Pressure | Heatmap overlay | Diverging from 1013 hPa |
| Geomagnetism | Diamond markers → time-series chart | Click to view 24h X/Y/Z/F |
| Ocean SST | Tile overlay | Cool-warm colormap |
| GNSS Displacement | Arrow vectors | Magnitude-proportional length |
| Ionosphere TEC | Grid overlay | Low=transparent → high=purple |

## Correlation Panel

Right-side collapsible panel with time-synchronized charts across all data sources, enabling visual detection of cross-domain anomalies (e.g., ionosphere TEC drop → geomagnetic disturbance → earthquake).

## Related

Part of the [Realtime Open Data](https://github.com/yasumorishima/realtime-open-data) project collection.

## License

MIT
