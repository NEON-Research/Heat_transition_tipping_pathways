#!/usr/bin/env python3
"""Horizontal five-step flowchart for the analysis-design section (wide aspect, sits inline at \\linewidth).

The five boxes mirror the five numbered steps in Section 'Analysis design'.
"""
import os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.normpath(os.path.join(HERE, ".."))
OUT = os.path.join(ROOT, "results", "calib", "fig_method_flowchart.png")

BLUE="#cfe0ef"; ORANGE="#f7e0bd"; YELLOW="#f3ecc4"; TEAL="#c7e2e4"; PINK="#ecd3e0"; GREY="#e6e6e6"

fig, ax = plt.subplots(figsize=(12.6, 3.30)); ax.set_xlim(0, 1); ax.set_ylim(0.095, 1.005); ax.axis("off")

W, H, Y = 0.166, 0.34, 0.36
XS = [0.035, 0.227, 0.419, 0.611, 0.803]
NUM_Y   = Y + H - 0.085
TITLE_Y = Y + 0.105
SUB_Y   = Y - 0.055
LBL_Y   = Y + H + 0.045

def stage(x, num, title, sub, fc):
    ax.add_patch(FancyBboxPatch((x, Y), W, H, boxstyle="round,pad=0.006,rounding_size=0.03",
                                fc=fc, ec="#666", lw=1.0))
    ax.text(x+W/2, NUM_Y,   num,   ha="center", va="center", fontsize=13, fontweight="bold", color="#555")
    ax.text(x+W/2, TITLE_Y, title, ha="center", va="center", fontsize=9.6, linespacing=1.3)
    ax.text(x+W/2, SUB_Y,   sub,   ha="center", va="top", fontsize=7.4, style="italic",
            color="#555", linespacing=1.45)

def arrow(x1, x2, label=None):
    ax.add_patch(FancyArrowPatch((x1, Y+H/2), (x2, Y+H/2), arrowstyle="-|>", mutation_scale=13,
                                 lw=1.3, color="#555"))
    if label:
        ax.text((x1+x2)/2, LBL_Y, label, ha="center", va="bottom",
                fontsize=7.6, style="italic", color="#444", linespacing=1.35)

stage(XS[0], "1", "Calibration of\nplausible decision\nweights",
      "history matching (GLUE)\nNoord-Brabant\n100 sets \u2192 21 retained", BLUE)
stage(XS[1], "2", "Rank uncertain\nparameters",
      "Morris elementary effects\nweights at ensemble median\nNoord-Brabant", ORANGE)
stage(XS[2], "3", "Quantify key\nparameters",
      "Latin-hypercube sampling\nregression coefficients", YELLOW)
stage(XS[3], "4", "Run pathways and\nexperiments",
      "Netherlands\n21 sets \u00d7 16 scenarios\nknock-outs \u00b7 driver sweeps", TEAL)
stage(XS[4], "5", "Report in household\nsegments",
      "band \u00b7 sensitivity \u00b7 scenarios", PINK)

arrow(XS[0]+W, XS[1], "retained ensemble")
arrow(XS[1]+W, XS[2], "influential parameters")
arrow(XS[2]+W, XS[3])
arrow(XS[3]+W, XS[4])

# observed data feeding step 1
ax.add_patch(FancyBboxPatch((XS[0], 0.855), W, 0.115, boxstyle="round,pad=0.006,rounding_size=0.03",
                            fc=GREY, ec="#777", lw=1.0, ls="--"))
ax.text(XS[0]+W/2, 0.9125, "Observed heating mix\n2022–2024", ha="center", va="center",
        fontsize=8.4, linespacing=1.3)
ax.add_patch(FancyArrowPatch((XS[0]+W/2, 0.855), (XS[0]+W/2, Y+H), arrowstyle="-|>",
                             mutation_scale=12, lw=1.2, color="#555"))

os.makedirs(os.path.dirname(OUT), exist_ok=True)
fig.savefig(OUT, dpi=190, bbox_inches="tight"); print("wrote", OUT)
