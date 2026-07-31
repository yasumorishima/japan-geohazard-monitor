import os, glob, numpy as np, rasterio, matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patheffects import withStroke
from rasterio.warp import transform as warp_transform

INSAR_DIR = os.environ.get('INSAR_DIR', 'insar')

for cand in ['Noto Sans CJK JP', 'Noto Serif CJK JP']:
    if any(f.name == cand for f in font_manager.fontManager.ttflist):
        plt.rcParams['font.family'] = cand
        break
plt.rcParams['axes.unicode_minus'] = False

EPI = (130.65, 32.60)
# 位置は国土地理院 住所検索 API (msearch.gsi.go.jp) で取得した役所・役場の座標
CITIES = [
    ('熊本市', 130.708054, 32.803333),
    ('益城町', 130.816345, 32.791607),
    ('宇土市', 130.658478, 32.687294),
    ('美里町', 130.788895, 32.639721),
    ('宇城市', 130.684357, 32.647846),
    ('氷川町', 130.673615, 32.582500),
    ('八代市', 130.601807, 32.507500),
]
LON0, LON1, LAT0, LAT1 = 130.25, 131.15, 32.25, 33.00

cf = [x for x in glob.glob(INSAR_DIR + '/*/*_corr.tif') if '20260728' in x][0]
pf = [x for x in glob.glob(INSAR_DIR + '/*/*_corr.tif') if '20260728' not in x][0]
lf = glob.glob(INSAR_DIR + '/*/*_los_disp.tif')[0]

with rasterio.open(cf) as dc, rasterio.open(pf) as dp, rasterio.open(lf) as dl:
    rows, cols = min(dc.height, dp.height), min(dc.width, dp.width)
    co = np.array(dc.read(1)[:rows, :cols])
    pre = np.array(dp.read(1)[:rows, :cols])
    los = np.array(dl.read(1)[:rows, :cols])
    tf, crs = dc.transform, dc.crs

water = np.zeros((rows, cols), bool)
for w in sorted(glob.glob(INSAR_DIR + '/*/*_water_mask.tif')):
    with rasterio.open(w) as dw:
        water |= (np.array(dw.read(1)[:rows, :cols]) == 0)

# crop to the lat/lon box
xs, ys = warp_transform('EPSG:4326', crs, [LON0, LON1, LON0, LON1], [LAT0, LAT0, LAT1, LAT1])
c0 = max(int((min(xs) - tf.c) / tf.a), 0)
c1 = min(int((max(xs) - tf.c) / tf.a), cols)
r0 = max(int((max(ys) - tf.f) / tf.e), 0)
r1 = min(int((min(ys) - tf.f) / tf.e), rows)
sl = (slice(r0, r1), slice(c0, c1))
H, W = r1 - r0, c1 - c0

# per-pixel lon/lat of the crop (for pcolormesh: true geographic placement)
rr, cc = np.mgrid[r0:r1 + 1, c0:c1 + 1]
X = tf.c + cc * tf.a
Y = tf.f + rr * tf.e
LON, LAT = warp_transform(crs, 'EPSG:4326', X.ravel(), Y.ravel())
LON = np.array(LON).reshape(X.shape)
LAT = np.array(LAT).reshape(X.shape)

wat = water[sl]
okc = (pre[sl] >= 0.3) & (co[sl] > 0) & (~wat)
dcoh = pre[sl] - co[sl]
losm = np.where((co[sl] >= 0.3) & (los[sl] != 0) & (~wat), los[sl] * 100, np.nan)


def decorate(ax, title):
    # coastline: boundary of the water mask
    ax.contour(LON[:-1, :-1], LAT[:-1, :-1], wat.astype(float), levels=[0.5],
               colors='#444444', linewidths=0.7)
    ax.plot(*EPI, marker='*', ms=24, color='k', mec='w', mew=1.6, ls='none', zorder=6)
    ax.annotate('震央', EPI, textcoords='offset points', xytext=(0, -22), ha='center',
                fontsize=13, zorder=7,
                path_effects=[withStroke(linewidth=3.5, foreground='white')])
    for name, lo, la in CITIES:
        ax.plot(lo, la, 'o', ms=5, color='#111111', mec='w', mew=1.0, zorder=6)
        ax.annotate(name, (lo, la), textcoords='offset points', xytext=(7, 4),
                    fontsize=12.5, zorder=7,
                    path_effects=[withStroke(linewidth=3.5, foreground='white')])
    ax.set_xlim(LON0, LON1)
    ax.set_ylim(LAT0, LAT1)
    ax.set_aspect(1 / np.cos(np.deg2rad(32.6)))
    ax.set_title(title, fontsize=19, pad=16)
    ax.set_xlabel('東経', fontsize=14)
    ax.set_ylabel('北緯', fontsize=14)
    ax.tick_params(labelsize=12)


# --- Fig 1: deformation ---
fig, ax = plt.subplots(figsize=(11.5, 10))
fig.subplots_adjust(left=.10, right=.86, top=.90, bottom=.09)
m = ax.pcolormesh(LON, LAT, np.ma.masked_invalid(losm), cmap='RdBu_r',
                  vmin=-20, vmax=20, shading='flat', rasterized=True)
decorate(ax, '令和8年熊本地震にともなう地面の動き')
cb = fig.colorbar(m, ax=ax, shrink=.80, pad=.02)
cb.set_label('衛星の視線方向の変位 (cm)　＋が衛星に近づいた側', fontsize=13)
cb.ax.tick_params(labelsize=12)
fig.savefig(INSAR_DIR + '/fig1_los.png', dpi=140)
plt.close(fig)

# --- Fig 2: damage proxy aggregated to ~1 km cells ---
K = 25  # 25 x 40 m = 1 km
hh, ww = (H // K) * K, (W // K) * K
hit = ((dcoh > 0.3) & okc)[:hh, :ww].reshape(hh // K, K, ww // K, K).sum(axis=(1, 3))
tot = okc[:hh, :ww].reshape(hh // K, K, ww // K, K).sum(axis=(1, 3))
frac = np.where(tot >= 80, 100.0 * hit / np.maximum(tot, 1), np.nan)
LONc = LON[:hh + 1:K, :ww + 1:K]
LATc = LAT[:hh + 1:K, :ww + 1:K]

fig, ax = plt.subplots(figsize=(11.5, 10))
fig.subplots_adjust(left=.10, right=.86, top=.90, bottom=.09)
m = ax.pcolormesh(LONc, LATc, np.ma.masked_invalid(frac), cmap='YlOrRd',
                  vmin=5, vmax=70, shading='flat', rasterized=True)
decorate(ax, '地震をまたいで地表が変わった度合い（1km ごと）')
cb = fig.colorbar(m, ax=ax, shrink=.80, pad=.02)
cb.set_label('変化した画素の割合 (%)　地震と無関係な場所では 3〜5%', fontsize=13)
cb.ax.tick_params(labelsize=12)
fig.savefig(INSAR_DIR + '/fig2_damage.png', dpi=140)
plt.close(fig)

good = np.isfinite(frac)
print('1km cells with enough data: %d' % good.sum())
print('cells >=30%%: %d   >=50%%: %d   max %.1f%%' % (
    (frac[good] >= 30).sum(), (frac[good] >= 50).sum(), frac[good].max()))
print('figs v2 written')
