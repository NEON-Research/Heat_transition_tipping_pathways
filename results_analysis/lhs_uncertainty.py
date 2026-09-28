#!/usr/bin/env python3
"""Uncertainty propagation: how far the screened parameters move the 2050 mix when varied jointly.

Reads the Latin-hypercube output written by structural_sensitivity.py and draws, per technology,
the full range, the 5-95 and 25-75 percentile intervals and the median across the sample. Unlike
the regression summary this needs no model fitting, so interactions between the parameters are
included by construction.

The baseline is marked for reference, and it is worth looking at where it falls: the sampled
ranges are not centred on it, because each parameter is sampled across its whole plausible range
rather than around its default.

    python lhs_uncertainty.py --lhs ../results/calib1000/structural_lhs_median.csv
"""
import argparse, csv, os
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE  = {"NATURAL_GAS_BOILER": "Natural gas", "HYBRID_HEAT_PUMP": "Hybrid heat pump",
         "ELECTRIC_HEAT_PUMP": "Electric heat pump", "DISTRICT_HEATING": "District heating"}
COLOR = {"NATURAL_GAS_BOILER": "#999999", "HYBRID_HEAT_PUMP": "#56B4E9",
         "ELECTRIC_HEAT_PUMP": "#0072B2", "DISTRICT_HEATING": "#D55E00"}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lhs", default=os.path.join(ROOT, "results", "calib1000", "structural_lhs_median.csv"))
    ap.add_argument("--baseline", default="",
                    help="run to mark as a reference point. Off by default: a marker is only\n"
                         "comparable if the run was produced at the SAME decision weights the LHS\n"
                         "holds fixed, on the same scope. Marking a run at other weights mixes\n"
                         "preference and parameter effects in one symbol.")
    ap.add_argument("--out", default=None)
    ap.add_argument("--copy-to", default=os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures"))
    a = ap.parse_args()
    rows = list(csv.DictReader(open(a.lhs)))
    n = len(rows)

    base = {}
    if a.baseline and os.path.exists(a.baseline):
        from collections import defaultdict
        acc = defaultdict(float); it = set()
        MERGE = {"NATURAL_GAS_BLOCK": "NATURAL_GAS_BOILER"}
        for r in csv.DictReader(open(a.baseline)):
            if r["scenario_name"] != "baseline" or r["ownership"] != "TOTAL": continue
            if int(float(r["year"])) != 2050: continue
            it.add(r["iteration"])
            acc[MERGE.get(r["heating_system"], r["heating_system"])] += float(r["installed_current"])
        tot = sum(acc.values()) or 1
        base = {t: 100 * acc.get(t, 0.0) / tot for t in TECHS}

    fig, ax = plt.subplots(figsize=(9, 4.4))
    ys = np.arange(len(TECHS))[::-1]
    print(f"{'technology':22s}{'min':>7s}{'p5':>7s}{'p25':>7s}{'med':>7s}{'p75':>7s}{'p95':>7s}{'max':>7s}")
    for y, t in zip(ys, TECHS):
        v = np.array([float(r[t]) for r in rows])
        q = {k: np.percentile(v, k) for k in (5, 25, 50, 75, 95)}
        c = COLOR[t]
        ax.plot([v.min(), v.max()], [y, y], color=c, lw=1.4, alpha=.55, solid_capstyle="butt")
        for m in (v.min(), v.max()):
            ax.plot([m, m], [y - .13, y + .13], color=c, lw=1.4, alpha=.55)
        ax.add_patch(plt.Rectangle((q[5], y - .17), q[95] - q[5], .34, color=c, alpha=.30, lw=0))
        ax.add_patch(plt.Rectangle((q[25], y - .24), q[75] - q[25], .48, color=c, alpha=.75, lw=0))
        ax.plot([q[50], q[50]], [y - .26, y + .26], color="white", lw=2.4, zorder=4,
                path_effects=[pe.withStroke(linewidth=4.0, foreground="#333")])
        if base:
            ax.plot(base[t], y, marker="D", ms=6, mfc="white", mec="#222", mew=1.3, zorder=5)
        print(f"{NICE[t]:22s}{v.min():7.1f}{q[5]:7.1f}{q[25]:7.1f}{q[50]:7.1f}{q[75]:7.1f}{q[95]:7.1f}{v.max():7.1f}")
    ax.set_yticks(ys); ax.set_yticklabels([NICE[t] for t in TECHS], fontsize=10)
    ax.set_xlabel("share of dwellings in 2050 (%)", fontsize=10)
    ax.set_xlim(0, max(max(float(r[t]) for r in rows) for t in TECHS) * 1.06)
    ax.grid(axis="x", alpha=.25); ax.set_ylim(-.6, len(TECHS) - .4)
    for sp in ("top", "right", "left"): ax.spines[sp].set_visible(False)
    ax.tick_params(axis="y", length=0)
    handles = [plt.Rectangle((0, 0), 1, 1, color="#555", alpha=.75),
               plt.Rectangle((0, 0), 1, 1, color="#555", alpha=.30),
               plt.Line2D([0], [0], color="#555", lw=1.4, alpha=.55),
               plt.Line2D([0], [0], color="white", lw=2.4,
                          path_effects=[pe.withStroke(linewidth=4.0, foreground="#333")])]
    labs = ["25–75 %", "5–95 %", "full range", "median"]
    if base:
        handles.append(plt.Line2D([0], [0], marker="D", ms=6, mfc="white", mec="#222", mew=1.3, ls=""))
        labs.append("baseline (static prices)")
    ax.legend(handles, labs, ncol=5, fontsize=8.5, frameon=False,
              loc="lower center", bbox_to_anchor=(.5, -.30))
    ax.set_title(f"Spread of the 2050 mix across the {n} sampled parameter combinations",
                 fontsize=11.5, loc="left")
    fig.tight_layout()
    out = a.out or os.path.join(os.path.dirname(a.lhs), "fig_lhs_uncertainty.png")
    fig.savefig(out, dpi=150, bbox_inches="tight"); plt.close(fig)
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil; shutil.copy2(out, a.copy_to)
    print("\nwrote:", out)


if __name__ == "__main__":
    main()
