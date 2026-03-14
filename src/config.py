"""Configuration constants for the Japan Geohazard Monitor."""

DB_PATH = "/app/data/geohazard.db"

# Japan bounding box for filtering
JAPAN_BBOX = {
    "min_lat": 20.0,
    "max_lat": 50.0,
    "min_lon": 120.0,
    "max_lon": 155.0,
}

# Collector intervals (seconds)
USGS_INTERVAL = 300       # 5 min
P2P_INTERVAL = 120        # 2 min
JMA_INTERVAL = 180        # 3 min

# Batch flush interval
BATCH_FLUSH_SEC = 5

# Data retention (days)
RETENTION_DAYS = 90
