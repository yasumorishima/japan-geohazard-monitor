# -*- coding: utf-8 -*-
"""Figures for the sudden-earthquake (isolated mainshock) axis.

Inputs (the only source of the numbers):
  stand.csv    - where the axis stands per arena (figure 1)
  results.csv  - one row per declared statistic of each pre-registered round (figure 2); rows are appended by
                 add_round.py from the round's run log, never typed by hand
The GitHub Actions workflow sudden-axis-figures.yml reruns this script whenever either file changes and commits the
PNGs.  The script refuses malformed input (missing columns, values out of range, duplicate rows) instead of drawing.
Run: python make_figs.py
"""
import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

HERE = os.path.dirname(os.path.abspath(__file__))
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e0"
BLUE, ORANGE, GRAY_BAND = "#2a78d6", "#eb6834", "#d9d8d2"
GOAL = 0.90
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 13, "axes.edgecolor": MUTED, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK, "figure.facecolor": "white",
    "axes.facecolor": "white", "savefig.facecolor": "white",
})


def read(name, cols):
    with open(os.path.join(HERE, name), newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows, name + " is empty"
    missing = set(cols) - set(rows[0])
    assert not missing, (name, "missing columns", missing)
    return rows


def style(ax):
    ax.grid(axis="x", color=GRID, lw=1)
    ax.set_axisbelow(True)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(axis="y", length=0)


def fig1():
    rows = read("stand.csv", ("arena", "detail", "auc", "note"))
    for r in rows:
        r["auc"] = float(r["auc"])
        assert 0.0 <= r["auc"] <= 1.0, r
    fig, ax = plt.subplots(figsize=(11, 1.6 + 1.2 * len(rows)))
    ys = list(range(len(rows)))[::-1]
    for y, r in zip(ys, rows):
        ax.barh(y, r["auc"] - 0.5, left=0.5, height=0.42, color=BLUE)
        ax.text(r["auc"] + 0.006, y, "%.3f" % r["auc"], va="center", ha="left", fontsize=14, color=INK, fontweight="bold")
        ax.text(0.505, y - 0.33, r["note"], va="center", ha="left", fontsize=11.5, color=MUTED)
    ax.axvline(GOAL, color=ORANGE, lw=2, ls="--")
    ax.text(GOAL + 0.002, ys[0] + 0.42, "goal %.2f" % GOAL, color=ORANGE, fontsize=14, va="bottom", ha="left", fontweight="bold")
    ax.set_yticks(ys, ["%s\n(%s)" % (r["arena"], r["detail"]) for r in rows], fontsize=13)
    ax.set_xlim(0.5, 1.0)
    ax.set_ylim(-0.75, len(rows) - 0.35)
    ax.set_xlabel("AUC (0.5 = no skill)", fontsize=14)
    lo, hi = min(r["auc"] for r in rows), max(r["auc"] for r in rows)
    fig.suptitle("Isolated mainshocks: skill so far is %.2f-%.2f; the goal is %.2f" % (lo, hi, GOAL),
                 fontsize=17, x=0.01, ha="left", color=INK)
    style(ax)
    fig.text(0.01, 0.01, "The populations differ (catalogue, region, cell set); bars are not comparable with each other.",
             fontsize=11, color=MUTED)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(os.path.join(HERE, "fig1_where_we_stand.png"), dpi=150)
    plt.close(fig)


def fig2():
    cols = ("round", "system", "statistic", "auc", "floor_sd_max", "max_floor_p", "positives", "verdict", "log")
    rows = read("results.csv", cols)
    seen = set()
    for r in rows:
        r["round"], r["positives"] = int(r["round"]), int(r["positives"])
        r["auc"], r["sd"], r["p"] = float(r["auc"]), float(r["floor_sd_max"]), float(r["max_floor_p"])
        assert 0.0 < r["auc"] < 1.0 and 0.0 < r["sd"] < 0.2 and 0.0 < r["p"] <= 1.0 and r["positives"] > 0, r
        key = (r["round"], r["statistic"])
        assert key not in seen, ("duplicate row", key)
        seen.add(key)
    rows.sort(key=lambda r: r["round"])
    nsys = len({r["system"] for r in rows})
    n = len(rows)
    fig, ax = plt.subplots(figsize=(12, 1.9 + 0.48 * n))
    ys = list(range(n))[::-1]
    lo = min(min(r["auc"], 0.5 - 1.96 * r["sd"]) for r in rows) - 0.02
    hi = max(max(r["auc"], 0.5 + 1.96 * r["sd"]) for r in rows) + 0.06
    for y, r in zip(ys, rows):
        ax.add_patch(plt.Rectangle((0.5 - 1.96 * r["sd"], y - 0.32), 2 * 1.96 * r["sd"], 0.64, color=GRAY_BAND, lw=0))
        ax.plot([r["auc"]], [y], "o", ms=10, color=BLUE, mec="white", mew=2, zorder=3)
        ax.text(hi - 0.008, y, "%.3f" % r["auc"], va="center", ha="right", fontsize=12.5, color=INK)
    ax.set_yticks(ys, ["%s  -  %s\n(round %d, %d positives)" % (r["system"], r["statistic"], r["round"], r["positives"])
                       for r in rows], fontsize=11.5)
    ax.axvline(0.5, color=MUTED, lw=1.2)
    ax.set_xlim(lo, hi)
    ax.set_ylim(-0.8, n - 0.2)
    ax.set_xlabel("AUC on isolated M5.5 in cells without precedent (Japan + New Zealand pooled)", fontsize=14)
    outside = [r for r in rows if abs(r["auc"] - 0.5) > 1.96 * r["sd"]]
    supported = [r for r in rows if r["verdict"].startswith("SUPPORTED")]
    if not outside:
        head = "%d catalogue-free observation systems: every result sits inside the chance band" % nsys
    else:
        head = "%d catalogue-free observation systems: %d of %d results outside the chance band" % (nsys, len(outside), n)
    fig.suptitle(head, fontsize=17, x=0.01, ha="left", color=INK)
    style(ax)
    ax.legend(handles=[Line2D([], [], marker="o", ls="", ms=10, color=BLUE, mec="white", mew=2, label="observed AUC"),
                       Patch(color=GRAY_BAND, label="chance band: 0.5 +/- 1.96 x widest permutation-floor sd")],
              loc="lower left", fontsize=12, frameon=False, bbox_to_anchor=(0.0, -0.17), ncol=2)
    foot = ("Each statistic was declared, with its direction, before any label was read; " +
            ("none was supported (smallest max floor p %.2f)." % min(r["p"] for r in rows) if not supported
             else "%d supported: %s." % (len(supported), ", ".join("round %d %s" % (r["round"], r["statistic"]) for r in supported))))
    fig.text(0.01, 0.005, foot, fontsize=11, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(os.path.join(HERE, "fig2_observation_systems.png"), dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    fig1()
    fig2()
    print("ok")
