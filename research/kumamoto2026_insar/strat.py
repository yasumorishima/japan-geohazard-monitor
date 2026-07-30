import os, glob, numpy as np, rasterio
from rasterio.warp import transform as warp_transform

INSAR_DIR = os.environ.get('INSAR_DIR', 'insar')

EPI_LAT, EPI_LON = 32.60, 130.65   # 宇城市/氷川町 付近

files = sorted(glob.glob(INSAR_DIR + '/*/*_corr.tif'))
pre_f = [f for f in files if '20260728' not in f][0]
co_f  = [f for f in files if '20260728' in f][0]

with rasterio.open(pre_f) as dp, rasterio.open(co_f) as dc:
    rows, cols = min(dp.height, dc.height), min(dp.width, dc.width)
    pre = np.array(dp.read(1)[:rows, :cols]); co = np.array(dc.read(1)[:rows, :cols])
    tf, crs = dp.transform, dp.crs

water = np.zeros((rows, cols), bool)
for w in sorted(glob.glob(INSAR_DIR + '/*/*_water_mask.tif')):
    with rasterio.open(w) as dw:
        water |= (np.array(dw.read(1)[:rows, :cols]) == 0)

ok = (pre > 0) & (co > 0) & (~water) & (pre >= 0.3)
dcoh = pre - co

ex, ey = warp_transform('EPSG:4326', crs, [EPI_LON], [EPI_LAT])
ex, ey = ex[0], ey[0]
r, c = np.mgrid[0:rows, 0:cols]
X = tf.c + (c + 0.5) * tf.a
Y = tf.f + (r + 0.5) * tf.e
dist_km = np.hypot(X - ex, Y - ey) / 1000.0

print('epicentre UTM x=%.0f y=%.0f' % (ex, ey))
print('%-14s %9s %8s %8s %8s %8s' % ('band(km)', 'n', 'mean', 'median', 'p95', 'frac>0.3'))
for lo, hi in [(0,10),(10,20),(20,30),(30,50),(50,80),(80,120),(120,200)]:
    m = ok & (dist_km >= lo) & (dist_km < hi)
    n = int(m.sum())
    if n < 500:
        print('%-14s %9d  (too few)' % ('%d-%d' % (lo, hi), n)); continue
    d = dcoh[m]
    print('%-14s %9d %+8.3f %+8.3f %+8.3f %8.3f' % (
        '%d-%d' % (lo, hi), n, d.mean(), np.median(d), np.percentile(d, 95), (d > 0.3).mean()))
