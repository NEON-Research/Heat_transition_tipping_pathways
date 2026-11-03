#!/usr/bin/env python3
"""Horizontal five-step flowchart for the analysis-design section (wide aspect, sits inline at \\linewidth).

The five boxes mirror the five numbered steps in Section 'Analysis design'.

Drawn at the printed size (paper_style.TEXT_W wide) so its text is the same size as in the other
figures; positions are in inches on a single full-figure axis.

    python results_analysis/make_method_flowchart.py

Writes results/calib/fig_method_flowchart.png and copies it to the manuscript figures folder.
"""
import os
import shutil
import matplotlib; matplotlib.use("Agg")
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

import paper_style as ps

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.normpath(os.path.join(HERE, ".."))
OUT = os.path.join(ROOT, "results", "calib", "fig_method_flowchart.png")
PAPER = os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures")

BLUE="#cfe0ef"; ORANGE="#f7e0bd"; YELLOW="#f3ecc4"; TEAL="#c7e2e4"; PINK="#ecd3e0"; GREY="#e6e6e6"

FIG_W, FIG_H = ps.TEXT_W, 2.62          # inches; the axis spans the figure, so data units are inches
ps.apply()
fig = ps.figure(frac=1.0, height=FIG_H, layout="none")   # one hand-placed axis: nothing to lay out
ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, FIG_W); ax.set_ylim(0, FIG_H); ax.axis("off")

PITCH = 1.22                            # distance between box centres
W, H = 1.06, 0.80                       # stage box
CX = [0.74 + i * PITCH for i in range(5)]
XS = [c - W / 2 for c in CX]
Y = 0.88                                # bottom of the stage boxes
NUM_Y   = Y + H - 0.15
TITLE_Y = Y + 0.30
SUB_Y   = Y - 0.08
LBL_Y   = Y + H + 0.05
OBS_Y, OBS_H, OBS_W = Y + H + 0.50, 0.36, 1.40   # observed-data box (wider than a stage box)
LINE = ps.FS * 1.2 / 72                 # inches between two lines of a note
ITEM_GAP = 0.045                        # inches between two items of a note
ROUND = "round,pad=0.02,rounding_size=0.08"

def stage(x, num, title, items, fc):
    """items: the lines of the italic note under the box; an item that wraps keeps its lines
    closer together than the gap between items."""
    ax.add_patch(FancyBboxPatch((x, Y), W, H, boxstyle=ROUND, fc=fc, ec="#666", lw=0.8))
    ax.text(x+W/2, NUM_Y,   num,   ha="center", va="center", fontsize=ps.FS_PANEL, fontweight="bold",
            color="#555")
    ax.text(x+W/2, TITLE_Y, title, ha="center", va="center", linespacing=1.25)
    y = SUB_Y - LINE * 0.8                 # baseline of the first line
    for it in items:                       # fixed line pitch, so every column lines up the same
        for line in it.split("\n"):
            ax.text(x+W/2, y, line, ha="center", va="baseline", style="italic", color="#555")
            y -= LINE
        y -= ITEM_GAP

def arrow(x1, x2, label=None):
    ax.add_patch(FancyArrowPatch((x1+0.02, Y+H/2), (x2-0.02, Y+H/2), arrowstyle="-|>", mutation_scale=7,
                                 lw=1.0, color="#555", shrinkA=0, shrinkB=0))
    if label:
        ax.text((x1+x2)/2, LBL_Y, label, ha="center", va="bottom", style="italic", color="#444",
                linespacing=1.15)

# the notes are broken into shorter lines so five columns fit the text width; the numbers follow
# Sections 5-6 of main.tex (calibration v3, 25-set national subsample, 20 scenarios)
stage(XS[0], "1", "Calibration of\nplausible decision\nweights",
      ["history matching\n(GLUE)", "Noord-Brabant", "1000 sets \u2192\n124 retained"], BLUE)
stage(XS[1], "2", "Rank uncertain\nparameters",
      ["Morris elementary\neffects", "four weight sets", "Noord-Brabant"], ORANGE)
stage(XS[2], "3", "Quantify key\nparameters",
      ["Latin-hypercube\nsampling", "regression\ncoefficients"], YELLOW)
stage(XS[3], "4", "Run pathways\nand experiments",
      ["Netherlands", "25 sets \u00d7\n20 scenarios", "connection-cost and\nsplit-PBC runs"], TEAL)
stage(XS[4], "5", "Report in\nhousehold\nsegments",
      ["band \u00b7 sensitivity\n\u00b7 scenarios"], PINK)

arrow(XS[0]+W, XS[1], "retained\nensemble")
arrow(XS[1]+W, XS[2], "influential\nparameters")
arrow(XS[2]+W, XS[3])
arrow(XS[3]+W, XS[4])

# observed data feeding step 1
ax.add_patch(FancyBboxPatch((XS[0], OBS_Y), OBS_W, OBS_H, boxstyle=ROUND,
                            fc=GREY, ec="#777", lw=0.8, ls="--"))
ax.text(XS[0]+OBS_W/2, OBS_Y+OBS_H/2, "Observed heating mix\n2022–2024", ha="center", va="center",
        linespacing=1.25)
ax.add_patch(FancyArrowPatch((CX[0], OBS_Y-0.02), (CX[0], Y+H+0.02), arrowstyle="-|>",
                             mutation_scale=7, lw=1.0, color="#555", shrinkA=0, shrinkB=0))

os.makedirs(os.path.dirname(OUT), exist_ok=True)
ps.save(fig, OUT); print("wrote", OUT)
try:                       # copy to the manuscript (fails while the file is open in a viewer)
    shutil.copy2(OUT, os.path.join(PAPER, os.path.basename(OUT))); print("copied to", PAPER)
except OSError as e:
    print("could not copy to the manuscript folder (file open?):", e)
