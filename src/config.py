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
AMEDAS_INTERVAL = 600     # 10 min
GEOMAG_INTERVAL = 900     # 15 min (Kp updates every 3h, GOES every 1min)

# Batch flush interval
BATCH_FLUSH_SEC = 5

# Data retention (days)
RETENTION_DAYS = 90

# AMeDAS URLs
AMEDAS_LATEST_TIME_URL = "https://www.jma.go.jp/bosai/amedas/data/latest_time.txt"
AMEDAS_TABLE_URL = "https://www.jma.go.jp/bosai/amedas/const/amedastable.json"
AMEDAS_DATA_URL = "https://www.jma.go.jp/bosai/amedas/data/map/{timestamp}.json"

VOLCANO_INTERVAL = 900    # 15 min
SST_INTERVAL = 21600      # 6 hours

# NOAA SWPC Geomagnetic URLs
GOES_MAG_URL = "https://services.swpc.noaa.gov/json/goes/primary/magnetometers-1-day.json"
KP_INDEX_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"

# JMA Bosai Volcano URLs
VOLCANO_LIST_URL = "https://www.jma.go.jp/bosai/volcano/const/volcano_list.json"
VOLCANO_WARNING_URL = "https://www.jma.go.jp/bosai/volcano/data/warning.json"

# NOAA ERDDAP SST (MUR 0.01° daily, stride=50 → 0.5° grid)
SST_URL = ("https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.json"
           "?analysed_sst[(last)][(25):50:(45)][(125):50:(150)]")
