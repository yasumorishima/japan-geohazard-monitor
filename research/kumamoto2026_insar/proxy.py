import os, glob, numpy as np, rasterio
from rasterio.warp import transform as warp_transform

INSAR_DIR = os.environ.get('INSAR_DIR', 'insar')

files = sorted(glob.glob(INSAR_DIR + '/*/*_corr.tif'))
pre_f = [f for f in files if '20260728' not in f][0]
co_f  = [f for f in files if '20260728' in f][0]
wm_f  = sorted(glob.glob(INSAR_DIR + '/*/*_water_mask.tif'))

with rasterio.open(pre_f) as dp, rasterio.open(co_f) as dc:
    rows = min(dp.height, dc.height); cols = min(dp.width, dc.width)
    pre = dp.read(1)[:rows, :cols]
    co  = dc.read(1)[:rows, :cols]
    prof = dp.profile.copy()
    tf = dp.transform

water = np.zeros((rows, cols), bool)
for w in wm_f:
    with rasterio.open(w) as dw:
        a = dw.read(1)[:rows, :cols]
        water |= (a == 0)   # HyP3 water_mask: 0 = water

valid = (pre > 0) & (co > 0) & (~water)
base_ok = valid & (pre >= 0.3)
dcoh = np.where(base_ok, pre - co, np.nan).astype('float32')

print('grid %dx%d  valid=%d (%.1f%%)  baseline_ok=%d (%.1f%%)' % (
    rows, cols, valid.sum(), 100*valid.sum()/valid.size,
    base_ok.sum(), 100*base_ok.sum()/valid.size))
print('coh_pre mean over baseline_ok = %.3f' % pre[base_ok].mean())
print('coh_co  mean over baseline_ok = %.3f' % co[base_ok].mean())
d = dcoh[base_ok]
for q in [50, 75, 90, 95, 99, 99.9]:
    print('  dcoh p%-5s = %+.3f' % (q, np.percentile(d, q)))
for t in [0.2, 0.3, 0.4, 0.5]:
    n = int((d > t).sum())
    print('  dcoh > %.1f : %7d px  (%.3f%%)  = %.1f km2' % (t, n, 100*n/d.size, n*0.0016))

prof.update(dtype='float32', count=1, nodata=np.nan, compress='deflate',
            height=rows, width=cols, tiled=True, blockxsize=256, blockysize=256)
out = INSAR_DIR + '/kumamoto2026_damage_proxy_dcoh.tif'
with rasterio.open(out, 'w', **prof) as dst:
    dst.write(dcoh, 1)
    dst.update_tags(TITLE='Kumamoto 2026 earthquake coherence-change damage proxy',
                    PRE_PAIR='20260704_20260716', CO_PAIR='20260716_20260728',
                    SENSOR='Sentinel-1D IW SLC path163 descending')
print('wrote', out)

# top clusters in lat/lon
idx = np.argsort(np.where(np.isnan(dcoh), -9, dcoh), axis=None)[::-1][:40000]
r, c = np.unravel_index(idx, dcoh.shape)
xs, ys = rasterio.transform.xy(tf, r, c)
lon, lat = warp_transform(prof['crs'], 'EPSG:4326', xs, ys)
print('top-40000 dcoh pixel centroid: lat %.4f lon %.4f' % (np.mean(lat), np.mean(lon)))
print('  lat range %.3f..%.3f  lon range %.3f..%.3f' % (min(lat), max(lat), min(lon), max(lon)))
