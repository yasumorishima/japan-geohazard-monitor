import os, glob, rasterio

INSAR_DIR = os.environ.get('INSAR_DIR', 'insar')
for f in sorted(glob.glob(INSAR_DIR + '/*/*_corr.tif')):
    with rasterio.open(f) as d:
        tag = 'CO ' if '20260728' in f else 'PRE'
        print(tag, d.crs, d.shape, 'res=%.1f' % d.res[0])
        print('    bounds', [round(v, 1) for v in d.bounds])
        print('    nodata', d.nodata, 'dtype', d.dtypes[0])
