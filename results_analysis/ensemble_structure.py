#!/usr/bin/env python3
"""What the calibration pins down: a trade-off between decision weights, not the weights one by one.

The decision function is a chain of normalised weighted averages (Decision.java):
    PBC       = aff * affordability + (1 - aff) * effort
    intention = att * attitude + sn * social norm + (1 - att - sn) * PBC
    behaviour = it * intention + (1 - it) * PBC
so the effective weight of each driver in the final behaviour score is a product of the sampled
shares:
    cost path     = aff * ((1 - it) + it * (1 - att - sn))
    attitude path = it * att
    social path   = it * sn
    effort path   = (1 - aff) * ((1 - it) + it * (1 - att - sn))
(with the social learning factor at 1, as in the calibration's baseline scenario).

The script fits the line through the retained sets in the (attitude path, cost path) plane, reports
how well the distance to that line predicts retention, and compares how much the retention narrows
each single weight with how much it narrows the combination. No model runs.

    python results_analysis/ensemble_structure.py results/calib1000
    python results_analysis/ensemble_structure.py results/calib1000 --copy-to heat_transition_tipping_dynamics/figures

Writes <dir>/fig_ensemble_structure.png and <dir>/ensemble_structure.json.
"""
import argparse
import json
import os
import shutil

import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

KEEP, DROP, INK, SUB = "#2c7fb8", "#c8c8c8", "#222222", "#E69F00"
SHARES = [("shareAttitude", "attitude (of intention)"),
          ("shareSocialnorm", "social norm (of intention)"),
          ("shareAffordability", "affordability (of PBC)"),
          ("shareIntention", "intention (of behaviour)")]


def paths(S):
    g = lambda k: np.array([s["sampled"][k] for s in S])
    att, sn, aff, it = g("shareAttitude"), g("shareSocialnorm"), g("shareAffordability"), g("shareIntention")
    pbc_total = (1 - it) + it * (1 - att - sn)
    return {"cost": aff * pbc_total, "attitude": it * att, "social": it * sn,
            "effort": (1 - aff) * pbc_total}, {k: g(k) for k, _ in SHARES}


def width_ratio(v, keep, q=(0, 100)):
    """Width of the kept sets' range (min-max by default) as a share of the sampled one."""
    a, k = np.percentile(v, q), np.percentile(v[keep], q)
    return float((k[1] - k[0]) / (a[1] - a[0]))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("calib", nargs="?", default=os.path.join("results", "calib1000"),
                    help="folder holding calibration_search.json")
    ap.add_argument("--copy-to", default=None, help="also copy the figure into this folder")
    a = ap.parse_args()
    d = json.load(open(os.path.join(a.calib, "calibration_search.json"), encoding="utf-8"))
    S = d["samples"]
    keep = np.array([s["implausibility_max"] <= d["implaus_cut"] for s in S])
    P, W = paths(S)
    x, y = P["attitude"], P["cost"]

    # the ridge: least-squares line through the kept sets, cost = c0 + c1 * attitude
    c1, c0 = np.polyfit(x[keep], y[keep], 1)
    dist = np.abs(y - (c0 + c1 * x))
    resid_sd = float((y[keep] - (c0 + c1 * x[keep])).std())
    bins = [(0, .03), (.03, .06), (.06, .10), (.10, 9)]
    by_dist = [{"from": lo, "to": hi, "n": int(((dist >= lo) & (dist < hi)).sum()),
                "kept_pct": float(100 * keep[(dist >= lo) & (dist < hi)].mean())} for lo, hi in bins]
    index = y - c1 * x                                   # constant along the ridge
    ratios = [(lab, width_ratio(W[k], keep)) for k, lab in SHARES]
    ratios.append(("combination: cost + %.1f × attitude" % -c1, width_ratio(index, keep)))
    stats = {"n_sampled": len(S), "n_kept": int(keep.sum()),
             "ridge": {"cost_path_intercept": float(c0), "slope_per_attitude_path": float(c1),
                       "residual_sd": resid_sd},
             "corr_cost_attitude_kept": float(np.corrcoef(x[keep], y[keep])[0, 1]),
             "corr_cost_attitude_all": float(np.corrcoef(x, y)[0, 1]),
             "retention_by_distance": by_dist,
             "kept_width_share_min_max": dict(ratios),
             "effort_path_kept": [float(P["effort"][keep].min()), float(P["effort"][keep].max())],
             "effort_path_sampled": [float(P["effort"].min()), float(P["effort"].max())]}

    print(f"{keep.sum()} of {len(S)} sets kept")
    print(f"ridge through the kept sets: cost path = {c0:.3f} {c1:+.3f} x attitude path "
          f"(residual sd {resid_sd:.3f}); corr kept {stats['corr_cost_attitude_kept']:+.2f}, "
          f"all sampled {stats['corr_cost_attitude_all']:+.2f}")
    for b in by_dist:
        print(f"  distance {b['from']:.2f}-{min(b['to'], 1):.2f} from the ridge: n={b['n']:4d}  kept {b['kept_pct']:5.1f}%")
    print("width of the kept range as a share of the sampled range (min-max):")
    for lab, r in ratios:
        print(f"  {lab:32s} {100 * r:5.0f}%")

    # ------------------------------------------------------------------ figure
    # One panel for the paper: the retained sets against the rest, and the fitted line. The widths
    # (panel b in an earlier version) are printed above and stored in ensemble_structure.json.
    fig, ax = plt.subplots(figsize=(6.4, 5.0))
    xs = np.linspace(0, max(1.03 * x.max(), 0.45), 50)
    ax.scatter(x[~keep], y[~keep], s=9, color=DROP, lw=0, zorder=1)
    ax.scatter(x[keep], y[keep], s=16, color=KEEP, edgecolor="white", lw=0.5, zorder=3)
    ax.plot(xs, c0 + c1 * xs, color=INK, lw=1.4, zorder=2)
    # the scenario subsample, if one sits beside the search file: filled orange on top
    sub_path = os.path.join(a.calib, "calibration_subsample.json")
    n_sub = 0
    if os.path.exists(sub_path):
        sub = json.load(open(sub_path, encoding="utf-8"))
        key = lambda s: tuple(sorted((k, round(v, 9)) for k, v in s.items()))
        pos = {key(s["sampled"]): i for i, s in enumerate(S)}
        sel = [pos[key(e["sampled"])] for e in sub["retained"] if key(e["sampled"]) in pos]
        n_sub = len(sel)
        ax.scatter(x[sel], y[sel], s=24, color=SUB, edgecolor="white", lw=0.5, zorder=4)
    ax.set_xlim(0, xs[-1]); ax.set_ylim(0, 1.0)
    ax.set_xlabel("effective weight of attitude in the decision", fontsize=10)
    ax.set_ylabel("effective weight of cost in the decision", fontsize=10)
    ax.set_title("Cost against attitude weight in the sampled weight sets", fontsize=11, loc="left",
                 color=INK)
    ax.legend(handles=[Line2D([], [], marker="o", ls="", color=KEEP, markersize=6,
                              label=f"retained ({keep.sum()})"),
                       Line2D([], [], marker="o", ls="", color=DROP, markersize=5,
                              label=f"ruled out ({(~keep).sum()})"),
                       Line2D([], [], color=INK, lw=1.4,
                              label=f"fitted line: cost = {c0:.2f} − {-c1:.1f} × attitude")]
                      + ([Line2D([], [], marker="o", ls="", color=SUB, markersize=6.5,
                                 label=f"retained, in scenario subsample ({n_sub})")] if n_sub else []),
              loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=2 if n_sub else 3, frameon=False,
              fontsize=8.5, handletextpad=0.4, columnspacing=1.2)
    ax.grid(alpha=.2); ax.set_axisbelow(True)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    fig.tight_layout()
    out = os.path.join(a.calib, "fig_ensemble_structure.png")
    fig.savefig(out, dpi=170, bbox_inches="tight"); plt.close(fig)
    with open(os.path.join(a.calib, "ensemble_structure.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=1)
    print(f"\nwrote {out}\nwrote {os.path.join(a.calib, 'ensemble_structure.json')}")
    if a.copy_to:
        shutil.copy2(out, a.copy_to); print(f"copied to {a.copy_to}")


if __name__ == "__main__":
    main()
