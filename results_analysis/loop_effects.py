#!/usr/bin/env python3
"""Two views of what the scenario levers actually do, built for DIFFERENCES rather than levels.

The stacked-to-100% panels show each pathway correctly but spend almost all their ink on what the
scenarios have in COMMON: four bands summing to 100 in every panel, so a 3-point move is a sliver
and a 19-point move still looks like a slightly different slope. These two figures put the
difference itself on the y-axis, so the eye compares the thing the text is arguing about.

  a. fig_loop_levers.png      2050 outcome. One row per lever, a line from its LOW arm to its HIGH
                              arm in percentage points relative to that lever's own reference.
                              The LENGTH of the line is the lever's power; zero is the reference.
                              All four technologies share one x-axis, so lengths compare directly.

  b. fig_loop_divergence.png  When the pathways separate. Deviation from the reference over time,
                              one panel per technology, colour = lever, solid = HIGH arm,
                              dashed = LOW arm. A lever that only matters after 2035 looks
                              different here from one that bites immediately.

References. The learning levers are ensemble medians and are compared against the ensemble-median
baseline. The price arms are single runs at the central weight vector, so they are compared against
knockout_intact, the matching single run -- never against the ensemble median.

    python results_analysis/loop_effects.py --scope nl
"""
import argparse, csv, os
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
PAPER_FIGS_DIR = os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures")

TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE  = {"NATURAL_GAS_BOILER": "Natural gas", "HYBRID_HEAT_PUMP": "Hybrid heat pump",
         "ELECTRIC_HEAT_PUMP": "Electric heat pump", "DISTRICT_HEATING": "District heating"}

# Lever hues, Okabe-Ito. Validated: worst adjacent CVD dE 11.4, normal-vision 18.7 -- both pass.
# Technology identity is carried by the PANEL here, so these hues do not collide with the
# technology colours used in the stacked figures.
LEVER_COLOR = {"economic": "#E69F00", "social": "#009E73", "price": "#0072B2"}
LEVER_LABEL = {"economic": "Economic learning", "social": "Social learning",
               "price": "Gas\u2013electricity price ratio"}

# lever -> (reference panel, LOW panel, HIGH panel)
LEVERS = {
    "economic": ("baseline",        "economic_learning_factor_low",  "economic_learning_factor_high"),
    "social":   ("baseline",        "social_learning_factor_low",    "social_learning_factor_high"),
    # Energy price is a scenario now (its own column in the matrix), so this row is an ensemble
    # median against the ensemble baseline like the other two -- no more mixing a single run with
    # medians. The arms are the SPARK SPREAD corners: HIGH = gas high + electricity low.
    "price":    ("baseline",        "gas_low_elec_high",             "gas_high_elec_low"),
}
# Drawn top to bottom. Social first: it is the largest lever and the one the argument leads with,
# and this matches the row order of fig_scenario_mix_loops_grid so the two figures read alike.
ORDER = ["social", "economic", "price"]
INK, MUTED = "#222222", "#666666"


def load(cache):
    d = defaultdict(lambda: defaultdict(dict))
    for r in csv.DictReader(open(cache)):
        if r["tech"] not in TECHS: continue
        d[r["panel"]][int(r["year"])][r["tech"]] = float(r["share_pct"])
    return d


def tipping_year(series, tech="NATURAL_GAS_BOILER", level=50.0):
    """First year `tech` falls below `level` %; None if it never does."""
    for y in sorted(series):
        if series[y].get(tech, 100.0) < level: return y
    return None


def fig_levers(data, out, end):
    rows = [(lv, t) for lv in ORDER for t in [None]]
    fig, axes = plt.subplots(1, 4, figsize=(13.0, 3.5), sharey=True)
    xs = []
    for ax, tech in zip(axes, TECHS):
        for i, lv in enumerate(ORDER):
            ref, lo, hi = LEVERS[lv]
            if not all(k in data for k in (ref, lo, hi)): continue
            base = data[ref][end][tech]
            dlo = data[lo][end][tech] - base
            dhi = data[hi][end][tech] - base
            y = len(ORDER) - 1 - i
            c = LEVER_COLOR[lv]
            ax.plot([dlo, dhi], [y, y], color=c, lw=2.6, solid_capstyle="round", zorder=2)
            # LOW arm hollow, HIGH arm filled: the arm is readable without colour
            ax.plot([dlo], [y], "o", ms=9, mfc="white", mec=c, mew=2.2, zorder=3)
            ax.plot([dhi], [y], "o", ms=9, color=c, mec="white", mew=1.2, zorder=3)
            xs += [dlo, dhi]
            # Label OUTSIDE each end rather than above it: when the two arms land close together
            # (district heating, electric under social learning) centred labels collide.
            left, right = min(dlo, dhi), max(dlo, dhi)
            ax.annotate(f"{left:+.1f}", (left, y), xytext=(-7, 0), textcoords="offset points",
                        ha="right", va="center", fontsize=8.2, color=INK)
            ax.annotate(f"{right:+.1f}", (right, y), xytext=(7, 0), textcoords="offset points",
                        ha="left", va="center", fontsize=8.2, color=INK)
        ax.axvline(0, color=MUTED, lw=1.0, zorder=1)
        ax.set_yticks(range(len(ORDER)))
        ax.set_yticklabels([LEVER_LABEL[lv] for lv in reversed(ORDER)], fontsize=9.5)
        ax.set_ylim(-0.6, len(ORDER) - 0.4)
        ax.set_title(NICE[tech], fontsize=10.5, loc="left", color=INK)
        ax.grid(axis="x", alpha=.22); ax.set_axisbelow(True)
        for sp in ("top", "right", "left"): ax.spines[sp].set_visible(False)
        ax.tick_params(axis="y", length=0)
    pad = 0.20 * (max(xs) - min(xs))       # room for the outside labels
    for ax in axes:
        ax.set_xlim(min(xs) - pad, max(xs) + pad)
    fig.supxlabel(f"change in {end} share versus the reference pathway (percentage points)",
                  fontsize=9.5, y=0.02)
    handles = [Line2D([], [], marker="o", ls="none", ms=9, mfc="white", mec=INK, mew=2.2,
                      label="LOW arm"),
               Line2D([], [], marker="o", ls="none", ms=9, color=INK, label="HIGH arm")]
    fig.legend(handles=handles, loc="lower center", ncol=2, frameon=False, fontsize=9.5,
               bbox_to_anchor=(0.5, -0.08))
    fig.suptitle(f"Strength of each lever: {end} heating mix relative to the reference pathway",
                 fontsize=12, y=1.0)
    fig.tight_layout(rect=(0, 0.06, 1, 0.94))
    fig.savefig(out, dpi=170, bbox_inches="tight"); plt.close(fig)
    return out


def fig_divergence(data, out, start, end):
    fig, axes = plt.subplots(1, 4, figsize=(13.0, 3.4), sharey=True, sharex=True)
    for ax, tech in zip(axes, TECHS):
        for lv in ORDER:
            ref, lo, hi = LEVERS[lv]
            if not all(k in data for k in (ref, lo, hi)): continue
            yrs = [y for y in sorted(data[ref]) if start <= y <= end]
            c = LEVER_COLOR[lv]
            # HIGH arm = the end that favours heat pumps; for price that is gas dear / elec cheap
            for panel, ls in ((hi, "-"), (lo, "--")):
                d = [data[panel][y][tech] - data[ref][y][tech] for y in yrs]
                ax.plot(yrs, d, color=c, ls=ls, lw=2.0 if ls == "-" else 1.6,
                        solid_capstyle="round", zorder=2)
        ax.axhline(0, color=MUTED, lw=1.0, zorder=1)
        ax.set_title(NICE[tech], fontsize=10.5, loc="left", color=INK)
        ax.set_xlabel("year", fontsize=9.5)
        ax.grid(alpha=.22); ax.set_axisbelow(True)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    axes[0].set_ylabel("deviation from the reference\n(percentage points)", fontsize=9.5)
    handles = [Line2D([], [], color=LEVER_COLOR[lv], lw=2.4, label=LEVER_LABEL[lv]) for lv in ORDER]
    handles += [Line2D([], [], color=INK, lw=2.0, ls="-", label="HIGH arm"),
                Line2D([], [], color=INK, lw=1.6, ls="--", label="LOW arm")]
    fig.legend(handles=handles, loc="lower center", ncol=5, frameon=False, fontsize=9.5,
               bbox_to_anchor=(0.5, -0.04))
    fig.suptitle("When the pathways separate: deviation from the reference over time",
                 fontsize=12, y=1.0)
    fig.tight_layout(rect=(0, 0.08, 1, 0.94))
    fig.savefig(out, dpi=170, bbox_inches="tight"); plt.close(fig)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="nl")
    ap.add_argument("--start", type=int, default=2024)
    ap.add_argument("--end", type=int, default=2050)
    ap.add_argument("--copy-to", default=None)
    a = ap.parse_args()
    if a.copy_to is None:       # manuscript figures are national: never overwrite them from a province
        a.copy_to = PAPER_FIGS_DIR if a.scope.strip().lower() in ("nl", "netherlands") else ""

    # results/<tag>/figures, where the tag is the folder name the batch writes (province:Noord-Brabant
    # -> noord-brabant); the raw scope string is not a folder name
    k, _, n = a.scope.partition(":")
    scope_dir = "nl" if k.strip().lower() in ("nl", "netherlands") else (
        n.strip().lower().replace(" ", "-") if k.strip().lower() == "province" else "gemeente_" + n.strip().lower())
    outdir = os.path.join(ROOT, "results", scope_dir, "figures")
    cache = os.path.join(outdir, "scenario_mix_grid.csv")
    if not os.path.exists(cache):
        raise SystemExit(f"no cache at {cache} -- run scenario_mix_grid.py first")
    data = load(cache)

    print(f"{'lever':20s}{'technology':22s}{'ref':>8s}{'LOW':>9s}{'HIGH':>9s}{'swing':>9s}")
    for lv in ORDER:
        ref, lo, hi = LEVERS[lv]
        if not all(k in data for k in (ref, lo, hi)):
            print(f"{LEVER_LABEL[lv]:20s} -- missing run, skipped"); continue
        for tech in TECHS:
            b = data[ref][a.end][tech]
            dl, dh = data[lo][a.end][tech] - b, data[hi][a.end][tech] - b
            print(f"{LEVER_LABEL[lv]:20s}{NICE[tech]:22s}{b:8.1f}{dl:+9.1f}{dh:+9.1f}{abs(dh-dl):9.1f}")
    print("\nyear natural gas falls below 50 % of the stock:")
    for k in ["baseline", "knockout_intact"] + [p for lv in ORDER for p in LEVERS[lv][1:]]:
        if k in data:
            ty = tipping_year(data[k])
            print(f"  {k:34s}{ty if ty else 'never within the horizon'}")

    written = [fig_levers(data, os.path.join(outdir, "fig_loop_levers.png"), a.end),
               fig_divergence(data, os.path.join(outdir, "fig_loop_divergence.png"), a.start, a.end)]
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil
        for f in written: shutil.copy2(f, a.copy_to)
    print("\nwrote:"); [print("  ", f) for f in written]


if __name__ == "__main__":
    main()
