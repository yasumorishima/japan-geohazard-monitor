import os, glob, numpy as np, rasterio, matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from rasterio.warp import transform as warp_transform

INSAR_DIR = os.environ.get('INSAR_DIR', 'insar')

for cand in ['Noto Sans CJK JP', 'Noto Serif CJK JP']:
    if any(f.name == cand for f in font_manager.fontManager.ttflist):
        plt.rcParams['font.family'] = cand; break
plt.rcParams['axes.unicode_minus'] = False

EPI_LAT, EPI_LON = 32.60, 130.65
R = 60_000  # crop half-width (m)

cf = [x for x in glob.glob(INSAR_DIR + '/*/*_corr.tif') if '20260728' in x][0]
pf = [x for x in glob.glob(INSAR_DIR + '/*/*_corr.tif') if '20260728' not in x][0]
lf = glob.glob(INSAR_DIR + '/*/*_los_disp.tif')[0]

with rasterio.open(cf) as dc, rasterio.open(pf) as dp, rasterio.open(lf) as dl:
    rows, cols = min(dc.height, dp.height), min(dc.width, dp.width)
    co = np.array(dc.read(1)[:rows, :cols]); pre = np.array(dp.read(1)[:rows, :cols])
    los = np.array(dl.read(1)[:rows, :cols])
    tf, crs = dc.transform, dc.crs

water = np.zeros((rows, cols), bool)
for w in sorted(glob.glob(INSAR_DIR + '/*/*_water_mask.tif')):
    with rasterio.open(w) as dw:
        water |= (np.array(dw.read(1)[:rows, :cols]) == 0)

ex, ey = warp_transform('EPSG:4326', crs, [EPI_LON], [EPI_LAT])
ex, ey = ex[0], ey[0]
c0 = int((ex - R - tf.c) / tf.a); c1 = int((ex + R - tf.c) / tf.a)
r0 = int((ey + R - tf.f) / tf.e); r1 = int((ey - R - tf.f) / tf.e)
c0, c1 = max(c0, 0), min(c1, cols); r0, r1 = max(r0, 0), min(r1, rows)
sl = (slice(r0, r1), slice(c0, c1))
ext = [(tf.c + c0*tf.a - ex)/1000, (tf.c + c1*tf.a - ex)/1000,
       (tf.f + r1*tf.e - ey)/1000, (tf.f + r0*tf.e - ey)/1000]

okc = (pre[sl] >= 0.3) & (co[sl] > 0) & (~water[sl])
dcoh = np.where(okc, pre[sl] - co[sl], np.nan)
losm = np.where((co[sl] >= 0.3) & (los[sl] != 0) & (~water[sl]), los[sl]*100, np.nan)

# --- Fig 1: LOS displacement ---
fig, ax = plt.subplots(figsize=(11, 9)); fig.subplots_adjust(left=.12, right=.88, top=.88, bottom=.12)
im = ax.imshow(losm, extent=ext, cmap='RdBu_r', vmin=-20, vmax=20, origin='upper')
ax.plot(0, 0, marker='*', ms=26, color='k', mec='w', mew=1.6, ls='none', label='震央')
ax.set_title('令和8年熊本地震の地殻変動（衛星視線方向）', fontsize=19, pad=18)
ax.set_xlabel('震央からの東西距離 (km)', fontsize=15); ax.set_ylabel('震央からの南北距離 (km)', fontsize=15)
ax.tick_params(labelsize=13); ax.legend(fontsize=13, loc='upper right')
cb = fig.colorbar(im, ax=ax, shrink=.82, pad=.03); cb.set_label('視線方向の変位 (cm)　＋は衛星に近づく', fontsize=14)
cb.ax.tick_params(labelsize=12)
fig.savefig(INSAR_DIR + '/fig1_los.png', dpi=140); plt.close(fig)

# --- Fig 2: damage proxy ---
fig, ax = plt.subplots(figsize=(11, 9)); fig.subplots_adjust(left=.12, right=.88, top=.88, bottom=.12)
im = ax.imshow(np.where(dcoh > 0.3, dcoh, np.nan), extent=ext, cmap='YlOrRd', vmin=0.3, vmax=0.8, origin='upper')
ax.plot(0, 0, marker='*', ms=26, color='k', mec='w', mew=1.6, ls='none', label='震央')
ax.set_title('地震をまたいで地表が変化した場所', fontsize=19, pad=18)
ax.set_xlabel('震央からの東西距離 (km)', fontsize=15); ax.set_ylabel('震央からの南北距離 (km)', fontsize=15)
ax.tick_params(labelsize=13); ax.legend(fontsize=13, loc='upper right')
cb = fig.colorbar(im, ax=ax, shrink=.82, pad=.03); cb.set_label('干渉性の低下量', fontsize=14); cb.ax.tick_params(labelsize=12)
fig.savefig(INSAR_DIR + '/fig2_damage.png', dpi=140); plt.close(fig)
print('figs written')
print('crop rows %d-%d cols %d-%d  extent(km) %s' % (r0, r1, c0, c1, [round(v,1) for v in ext]))
