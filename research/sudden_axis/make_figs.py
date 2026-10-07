# -*- coding: utf-8 -*-
"""Figures for the sudden-earthquake (isolated mainshock) axis, drawn from the pre-registered run logs.

Every number below is copied from a run log of a pre-registered round (paths given per row) and was reproduced to
five digits by an independent recount in the post-run audit of that round.  Run: python make_figs.py
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e0"
BLUE, ORANGE, GRAY_BAND = "#2a78d6", "#eb6834", "#d9d8d2"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 13, "axes.edgecolor": MUTED, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK, "figure.facecolor": "white",
    "axes.facecolor": "white", "savefig.facecolor": "white",
})

# ---------------------------------------------------------------- figure 1: where the sudden axis stands
# (label, AUC, population note)
STAND = [
    ("Global arena\n(USGS, 2,280 cells)", 0.5785, "arm on isolated mainshocks (ACTIVE)"),
    ("Japan arena\n(JMA, 362 cells)", 0.7550, "catalogue-based model B0, isolated M5.5"),
    ("New Zealand arena\n(GeoNet, 100 cells)", 0.7548, "catalogue-based model B0, isolated M5.5"),
]
fig, ax = plt.subplots(figsize=(11, 5.2))
ys = list(range(len(STAND)))[::-1]
for y, (lab, v, note) in zip(ys, STAND):
    ax.barh(y, v - 0.5, left=0.5, height=0.42, color=BLUE)
    ax.text(v + 0.006, y, "%.3f" % v, va="center", ha="left", fontsize=14, color=INK, fontweight="bold")
    ax.text(0.505, y - 0.33, note, va="center", ha="left", fontsize=11.5, color=MUTED)
ax.axvline(0.90, color=ORANGE, lw=2, ls="--")
ax.text(0.902, ys[0] + 0.42, "goal 0.90", color=ORANGE, fontsize=14, va="bottom", ha="left", fontweight="bold")
ax.set_yticks(ys, [s[0] for s in STAND], fontsize=13)
ax.set_xlim(0.5, 1.0)
ax.set_ylim(-0.75, len(STAND) - 0.35)
ax.set_xlabel("AUC (0.5 = no skill)", fontsize=14)
fig.suptitle("Isolated mainshocks: skill so far is 0.58-0.76; the goal is 0.90", fontsize=17, x=0.01, ha="left", color=INK)
ax.grid(axis="x", color=GRID, lw=1)
ax.set_axisbelow(True)
for s in ("top", "right", "left"):
    ax.spines[s].set_visible(False)
ax.tick_params(axis="y", length=0)
fig.text(0.01, 0.01, "The three populations differ (catalogue, region, cell set); bars are not comparable with each other.",
         fontsize=11, color=MUTED)
fig.tight_layout(rect=(0, 0.04, 1, 1))
fig.savefig("fig1_where_we_stand.png", dpi=150)

# ---------------------------------------------------------------- figure 2: observation systems tested
# (round, observation system, statistic, pooled SA, largest verdict-floor sd, covered positives, log)
ROWS = [
    (293, "Land GNSS", "velocity change", 0.51355, 0.02876, 108, "reg292/pre293.log"),
    (293, "Land GNSS", "transient", 0.53354, 0.03063, 109, "reg292/pre293.log"),
    (294, "Ionospheric TEC", "|anomaly|", 0.46848, 0.03376, 84, "reg294/pre294.log"),
    (294, "Ionospheric TEC", "spread ratio", 0.51815, 0.03560, 84, "reg294/pre294.log"),
    (295, "Sea-surface temperature", "anomaly", 0.51636, 0.02465, 164, "reg295/pre295_run.log"),
    (295, "Sea-surface temperature", "spread ratio", 0.51773, 0.02405, 164, "reg295/pre295_run.log"),
    (296, "Ocean colour", "anomaly", 0.50336, 0.02411, 165, "reg296/pre296_run.log"),
    (296, "Ocean colour", "spread ratio", 0.50149, 0.02296, 165, "reg296/pre296_run.log"),
    (297, "Sea-surface height", "|anomaly|", 0.49595, 0.02469, 168, "reg297/pre297_run.log"),
    (297, "Sea-surface height", "spread ratio", 0.48457, 0.02399, 168, "reg297/pre297_run.log"),
    (298, "Satellite magnetic field", "Stouffer z", 0.47922, 0.02704, 128, "reg298/pre298_run.log"),
    (298, "Satellite magnetic field", "share > q90", 0.49659, 0.02615, 128, "reg298/pre298_run.log"),
    (299, "Outgoing longwave radiation", "anomaly", 0.51241, 0.02553, 175, "reg299/pre299_run.log"),
    (299, "Outgoing longwave radiation", "spread ratio", 0.50967, 0.02324, 175, "reg299/pre299_run.log"),
]
fig, ax = plt.subplots(figsize=(12, 8.6))
n = len(ROWS)
ys = list(range(n))[::-1]
for y, (rd, sysname, st, sa, sd, pos, _) in zip(ys, ROWS):
    ax.add_patch(plt.Rectangle((0.5 - 1.96 * sd, y - 0.32), 2 * 1.96 * sd, 0.64, color=GRAY_BAND, lw=0))
    ax.plot([sa], [y], "o", ms=10, color=BLUE, mec="white", mew=2, zorder=3)
    ax.text(0.632, y, "%.3f" % sa, va="center", ha="right", fontsize=12.5, color=INK)
labels = ["%s  -  %s\n(round %d, %d positives)" % (s, st, r, p) for r, s, st, _, _, p, _ in ROWS]
ax.set_yticks(ys, labels, fontsize=11.5)
ax.axvline(0.5, color=MUTED, lw=1.2)
ax.set_xlim(0.40, 0.64)
ax.set_ylim(-0.8, n - 0.2)
ax.set_xlabel("AUC on isolated M5.5 in cells without precedent (Japan + New Zealand pooled)", fontsize=14)
fig.suptitle("Seven catalogue-free observation systems: every result sits inside the chance band",
             fontsize=17, x=0.01, ha="left", color=INK)
ax.grid(axis="x", color=GRID, lw=1)
ax.set_axisbelow(True)
for s in ("top", "right", "left"):
    ax.spines[s].set_visible(False)
ax.tick_params(axis="y", length=0)
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
ax.legend(handles=[Line2D([], [], marker="o", ls="", ms=10, color=BLUE, mec="white", mew=2, label="observed AUC"),
                   Patch(color=GRAY_BAND, label="chance band: 0.5 +/- 1.96 x widest permutation-floor sd")],
          loc="lower left", fontsize=12, frameon=False, bbox_to_anchor=(0.0, -0.17), ncol=2)
fig.text(0.01, 0.005, "Each statistic was declared, with its direction, before any label was read; none was supported "
         "(smallest max floor p 0.22).", fontsize=11, color=MUTED)
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig("fig2_observation_systems.png", dpi=150)
print("ok")
