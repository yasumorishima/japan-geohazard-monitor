# -*- coding: utf-8 -*-
"""Figures for the sudden-earthquake (isolated mainshock) axis, SUDDEN-ALL.

Inputs (the only source of the numbers):
  stand.csv    - where the SUDDEN-ALL axis stands (figure 1); each value copied from a run log
  pilots.csv   - one row per non-catalogue source group read since pilot 960 (figure 2): coverage c, the largest
                 |partial S| and the end of its 95% interval, taken from the meta-analyses of the run logs
  results.csv  - the earlier figure 2 (AUC of observation systems on isolated M5.5, rounds 293-); kept, not drawn
The GitHub Actions workflow sudden-axis-figures.yml reruns this script whenever an input changes and commits the
PNGs.  The script refuses malformed input (missing columns, values out of range, duplicate rows) instead of drawing.
Run: python make_figs.py
"""
import csv
import os
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

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
    hi = max(r["auc"] for r in rows)
    fig.suptitle("Sudden axis (SUDDEN-ALL): isolated M>=6 within 34 days, all 2,280 cells\n"
                 "best so far %.3f; the goal is %.2f" % (hi, GOAL), fontsize=16, x=0.01, ha="left", color=INK)
    style(ax)
    fig.text(0.01, 0.01, "Positive day: an isolated M>=6 follows within 34 days; negative: no M>=5 within 34 days.\n"
             "Seen windows were used for selection; the early arena is a second reading.", fontsize=10.5, color=MUTED)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(os.path.join(HERE, "fig1_where_we_stand.png"), dpi=150)
    plt.close(fig)


DOMAINS = (  # fixed order = fixed colour (categorical slots 1-8)
    ("atmosphere", "#2a78d6"),
    ("ionosphere / space", "#eb6834"),
    ("ocean surface", "#1baf7a"),
    ("ocean interior", "#eda100"),
    ("solid Earth (geodesy, gravity, rotation, tides, core field)", "#e87ba4"),
    ("hydrology", "#008300"),
    ("volcanic and gas", "#6250d6"),
    ("seismic-waveform-derived", "#e34948"),
)
FORMS = {"day": "o", "cell-year": "s", "existence": "^"}
# Direct labels for the widest-coverage and the largest-effect points: group -> (short name, x / y offset in points).
# Groups not listed are not labelled; a listed group missing from pilots.csv is skipped.
LABELS = {"1001B": ("wind / PW / soil T", 8, 0), "1008S": ("soil moisture", 8, 0), "1018U": ("UV aerosol", -8, 0),
          "1015B": ("Argo", 8, 0), "1015A": ("Argo", 8, 0), "996": ("latent heat", 8, 0),
          "1028A": ("ocean colour", 8, 0), "983": ("altimetry", -8, 0), "975": ("core field", 8, 10),
          "972": ("GRACE", 8, 0), "974": ("Earth rotation", 8, -10)}
GATE_GAIN, GAIN_K = 0.0006, 0.018  # pilot gate: predicted gain 0.018 c S^2 >= 0.0006


def s_needed(c):
    return (GATE_GAIN / (GAIN_K * c)) ** 0.5


def fig2():
    cols = ("group", "round", "source", "domain", "form", "c", "s", "s_upper", "passes", "readings", "verdict", "log")
    rows = read("pilots.csv", cols)
    colour = dict(DOMAINS)
    seen = set()
    for r in rows:
        r["c"], r["s"], r["U"] = float(r["c"]), float(r["s"]), float(r["s_upper"])
        r["passes"], r["readings"], r["round"] = int(r["passes"]), int(r["readings"]), int(r["round"])
        assert 0.0 < r["c"] <= 1.0 and abs(r["s"]) < 3.0 and abs(r["s"]) <= r["U"] < 3.0, r
        assert 0 <= r["passes"] <= r["readings"] and r["readings"] > 0, r
        assert r["domain"] in colour and r["form"] in FORMS and r["verdict"] and r["log"].endswith(".log"), r
        assert r["group"] not in seen, ("duplicate row", r["group"])
        seen.add(r["group"])
    at_gate = [r for r in rows if abs(r["s"]) >= s_needed(r["c"])]
    fig, ax = plt.subplots(figsize=(12, 7.6))
    cs = [0.02 + 0.98 * k / 400 for k in range(401)]
    ax.fill_between(cs, [s_needed(c) for c in cs], 1.0, color="#f3f2ee", lw=0, zorder=0)
    ax.plot(cs, [s_needed(c) for c in cs], color=INK, lw=2, ls="--", zorder=1)
    ax.text(0.60, s_needed(0.60) + 0.03, "gate: 0.018 c S² = 0.0006\n(S %.3f at c = 1)" % s_needed(1.0),
            color=INK, fontsize=12, ha="left", va="bottom")
    for r in sorted(rows, key=lambda r: -r["U"]):
        col = colour[r["domain"]]
        ax.plot([r["c"], r["c"]], [abs(r["s"]), r["U"]], color=col, lw=1.6, alpha=0.75, zorder=2)
        ax.plot([r["c"] - 0.006, r["c"] + 0.006], [r["U"], r["U"]], color=col, lw=1.6, alpha=0.75, zorder=2)
        ax.plot([r["c"]], [abs(r["s"])], FORMS[r["form"]], ms=9, color=col, mec="white", mew=1.5, zorder=3)
    for r in rows:  # selective direct labels
        if r["group"] in LABELS:
            name, dx, dy = LABELS[r["group"]]
            ax.annotate("%s (%s)" % (name, r["group"]), (r["c"], abs(r["s"])), xytext=(dx, dy), textcoords="offset points",
                        fontsize=10.5, color=INK2, va="center", ha="left" if dx >= 0 else "right",
                        bbox=dict(fc="white", ec="none", alpha=0.8, pad=0.5), zorder=4)
    ax.set_xlim(0, 1.17)
    ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_ylim(0, 0.8)
    ax.set_xlabel("coverage c (share of seen positive rows the source reaches)", fontsize=13)
    ax.set_ylabel("largest |partial S| (sd), whisker to the 95% interval end", fontsize=13)
    ax.grid(color=GRID, lw=1)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    head = ("Non-catalogue sources read since pilot 960 (%d groups): %s" %
            (len(rows), "none reaches the gate" if not at_gate else "%d at the gate" % len(at_gate)))
    fig.suptitle(head, fontsize=16, x=0.01, ha="left", color=INK)
    dom_used = [d for d, _ in DOMAINS if any(r["domain"] == d for r in rows)]
    h1 = [Line2D([], [], marker="o", ls="", ms=9, color=colour[d], label=textwrap.fill(d, 32)) for d in dom_used]
    h2 = [Line2D([], [], marker=m, ls="", ms=9, color=MUTED, label=f) for f, m in FORMS.items()]
    leg = ax.legend(handles=h1, loc="upper right", fontsize=10.5, frameon=False, title="domain", title_fontsize=11,
                    alignment="left")
    ax.add_artist(leg)
    ax.legend(handles=h2, loc="upper left", bbox_to_anchor=(0.45, 1.0), fontsize=10.5, frameon=False, title="form",
              title_fontsize=11, alignment="left")
    fig.text(0.01, 0.01, "Points above the dashed curve would pass the pilot gate. Readings passing their nulls: %d of %d. "
             "Values from the meta-analyses of the run logs." % (sum(r["passes"] for r in rows),
                                                                  sum(r["readings"] for r in rows)),
             fontsize=10.5, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(os.path.join(HERE, "fig2_observation_systems.png"), dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    fig1()
    fig2()
    print("ok")
