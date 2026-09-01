#!/usr/bin/env python3
"""What the retained weight ensemble looks like on the INPUT side.

The output-side figures (overview_figures.py) show how far the 21 retained weight sets disagree
about the 2050 mix. This script asks the complementary question: which behavioural weights the
2022-2024 observation window actually pins down, and which it leaves free.

Three panels:

  A  prior vs retained marginals -- how much each sampled dimension is narrowed by history
     matching. A dimension whose retained spread is close to its prior spread is *not identified*
     by the data; it survives into the projections as parametric uncertainty.
  B  the retained sets in the plane of the two most strongly correlated dimensions, coloured by
     fit. A ridge here means equifinality: different weight combinations reproduce the same
     observed 2022-2024 mix, so no single "calibrated" parameterisation exists.
  C  rank correlation between each sampled weight and the 2050 technology shares. This is what
     carries the input spread into the output spread, and it identifies which behavioural
     assumption drives which route.

    python ensemble_inputs.py                     # baseline, NL outputs
    python ensemble_inputs.py --scenario social_learning_factor_high
"""
import argparse, glob, json, os, sys
import numpy as np, matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from scipy.stats import spearmanr

HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import overview_figures as ov
ROOT = ov.ROOT

DIMS = ["shareAffordability", "shareAttitude", "shareIntention", "shareSocialnorm"]
DNICE = {"shareAffordability": "Affordability\n$\\rightarrow$ PBC",
         "shareAttitude": "Attitude\n$\\rightarrow$ intention",
         "shareIntention": "Intention\n$\\rightarrow$ behaviour",
         "shareSocialnorm": "Social norm\n$\\rightarrow$ intention"}
SHORT = {k: k[5:] for k in DIMS}


def load_search(path):
    d = json.load(open(path))
    ret = sorted(d["retained"], key=lambda s: s["mad"])
    prior = np.array([[s["sampled"][k] for k in DIMS] for s in d["samples"]])
    post = np.array([[r["sampled"][k] for k in DIMS] for r in ret])
    return d, ret, prior, post


def link_runs(ret, scope_tag):
    """Match set* folders to retained records, verifying by the MAD encoded in the folder name."""
    paths = sorted(glob.glob(os.path.join(ROOT, "results", scope_tag, "calib", "set*",
                                          "simulation_results.csv")))
    if len(paths) != len(ret):
        raise SystemExit(f"{len(paths)} set folders but {len(ret)} retained records -- "
                         "the runs and the search json are out of sync")
    for p, r in zip(paths, ret):
        tag = os.path.basename(os.path.dirname(p))
        if abs(float(tag.split("mad")[1]) - r["mad"]) > 0.006:
            raise SystemExit(f"folder {tag} does not match retained MAD {r['mad']:.3f}")
    return paths


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="nl", help="scope of the RUNS (calibration is always NB)")
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--year", type=int, default=2050)
    a = ap.parse_args()
    tag = ov.tag(a.scope)

    d, ret, prior, post = load_search(os.path.join(ROOT, "results", "calib",
                                                   "calibration_search.json"))
    mad = np.array([r["mad"] for r in ret])
    paths = link_runs(ret, tag)
    runs = [ov.load(p, "TOTAL") for p in paths]
    tech = [t for t in ov.ORDER
            if np.mean([r[a.scenario][a.year][t] for r in runs]) > 0.5]
    Y = {t: np.array([r[a.scenario][a.year][t] for r in runs]) for t in tech}

    # ---- console summary -------------------------------------------------------------------
    print(f"calibrated on {d['scope']} | {len(prior)} sampled, {len(post)} retained "
          f"({100*len(post)/len(prior):.0f}%) at MAD < {d['tolerance']}")
    print(f"naive benchmark MAD {d['default_mad']:.2f}; retained MAD {mad.min():.2f}-{mad.max():.2f}\n")
    print(f"{'dimension':<20}{'prior sd':>10}{'ret. sd':>10}{'sd ratio':>10}  interpretation")
    ratios = {}
    for i, k in enumerate(DIMS):
        r = post[:, i].std() / prior[:, i].std(); ratios[k] = r
        verdict = ("identified" if r < 0.7 else
                   "weakly identified" if r < 0.9 else "NOT identified")
        print(f"{k:<20}{prior[:,i].std():10.3f}{post[:,i].std():10.3f}{r:10.2f}  {verdict}")

    # ---- figure ----------------------------------------------------------------------------
    fig = plt.figure(figsize=(13.6, 4.3))
    gs = GridSpec(1, 3, width_ratios=[1.25, 1.0, 1.15], wspace=0.34)

    # A. prior vs retained marginals, each dimension rescaled onto its own prior range
    axA = fig.add_subplot(gs[0])
    rng = np.random.default_rng(0)
    for i, k in enumerate(DIMS):
        lo, hi = prior[:, i].min(), prior[:, i].max()
        pn = (prior[:, i] - lo) / (hi - lo); qn = (post[:, i] - lo) / (hi - lo)
        y = len(DIMS) - 1 - i
        axA.plot([0, 1], [y, y], color="#dddddd", lw=9, solid_capstyle="butt", zorder=1)
        axA.scatter(pn, y + rng.uniform(-0.13, 0.13, len(pn)), s=9, color="#b0b0b0",
                    alpha=0.7, lw=0, zorder=2, label="sampled (prior)" if i == 0 else None)
        axA.scatter(qn, y + rng.uniform(-0.13, 0.13, len(qn)), s=34, color="#1E90FF",
                    edgecolor="white", lw=0.5, zorder=3,
                    label="retained (posterior)" if i == 0 else None)
        axA.text(1.03, y, f"sd ratio\n{ratios[k]:.2f}", va="center", fontsize=7.5,
                 color=("#b03030" if ratios[k] > 0.9 else "#555"))
    axA.set_yticks(range(len(DIMS))[::-1]); axA.set_yticklabels([DNICE[k] for k in DIMS], fontsize=8)
    axA.set_xlim(-0.03, 1.16); axA.set_ylim(-0.6, len(DIMS) - 0.4)
    axA.set_xlabel("position within the sampled range", fontsize=8.5)
    axA.set_title("A  What the 2022–2024 window pins down", fontsize=9.5, loc="left")
    axA.tick_params(labelsize=8); axA.legend(fontsize=7.5, loc="lower left",
                                             bbox_to_anchor=(0, -0.03), frameon=False)
    for s in ("top", "right"): axA.spines[s].set_visible(False)

    # B. the strongest correlated pair among the retained sets -> the equifinality ridge
    C = np.corrcoef(post.T); np.fill_diagonal(C, 0)
    i, j = np.unravel_index(np.abs(C).argmax(), C.shape)
    axB = fig.add_subplot(gs[1])
    sc = axB.scatter(post[:, i], post[:, j], c=mad, cmap="viridis_r", s=58,
                     edgecolor="white", lw=0.6)
    b, aa = np.polyfit(post[:, i], post[:, j], 1)
    xs = np.linspace(post[:, i].min(), post[:, i].max(), 20)
    axB.plot(xs, b * xs + aa, color="#b03030", lw=1.3, ls="--")
    axB.set_xlabel(SHORT[DIMS[i]], fontsize=8.5); axB.set_ylabel(SHORT[DIMS[j]], fontsize=8.5)
    axB.set_title(f"B  Equifinality ridge (r = {np.corrcoef(post.T)[i,j]:+.2f})",
                  fontsize=9.5, loc="left")
    axB.tick_params(labelsize=8); axB.grid(alpha=0.25, ls="--", lw=0.6)
    cb = fig.colorbar(sc, ax=axB, pad=0.02); cb.set_label("MAD (fit to 2022–2024)", fontsize=7.5)
    cb.ax.tick_params(labelsize=7)

    # C. weight -> 2050 share rank correlation
    axC = fig.add_subplot(gs[2])
    R = np.zeros((len(DIMS), len(tech))); P = np.zeros_like(R)
    for r_ in range(len(DIMS)):
        for c_ in range(len(tech)):
            R[r_, c_], P[r_, c_] = spearmanr(post[:, r_], Y[tech[c_]])
    im = axC.imshow(R, cmap="RdBu_r", vmin=-1, vmax=1, aspect="auto")
    for r_ in range(len(DIMS)):
        for c_ in range(len(tech)):
            star = "**" if P[r_, c_] < 0.01 else "*" if P[r_, c_] < 0.05 else ""
            axC.text(c_, r_, f"{R[r_,c_]:+.2f}{star}", ha="center", va="center", fontsize=7.8,
                     color="white" if abs(R[r_, c_]) > 0.55 else "#222")
    axC.set_xticks(range(len(tech)))
    axC.set_xticklabels([ov.NICE[t].replace(" heat pump", "\nheat pump").replace(" heating", "\nheating")
                         for t in tech], fontsize=7.8)
    axC.set_yticks(range(len(DIMS))); axC.set_yticklabels([SHORT[k] for k in DIMS], fontsize=8)
    axC.set_title(f"C  Weight $\\rightarrow$ {a.year} share ({a.scenario.replace('_',' ')})",
                  fontsize=9.5, loc="left")
    cb2 = fig.colorbar(im, ax=axC, pad=0.02); cb2.set_label("Spearman $\\rho$", fontsize=7.5)
    cb2.ax.tick_params(labelsize=7)

    fig.suptitle(f"The retained weight ensemble on the input side "
                 f"({len(post)} of {len(prior)} sets, calibrated on {d['scope'].split(':')[-1]})",
                 fontsize=11.5, y=1.02)
    figdir = os.path.join(ROOT, "results", tag, "figures"); os.makedirs(figdir, exist_ok=True)
    out = os.path.join(figdir, "fig_ensemble_inputs.png")
    fig.savefig(out, dpi=150, bbox_inches="tight"); plt.close(fig)

    # ---- machine-readable companion --------------------------------------------------------
    import csv as _csv
    tab = os.path.join(figdir, "ensemble_inputs.csv")
    with open(tab, "w", newline="") as fh:
        w = _csv.writer(fh)
        w.writerow(["set", "mad"] + DIMS + [f"{ov.NICE[t]}_{a.year}" for t in tech])
        for n, (r, p) in enumerate(zip(ret, paths)):
            w.writerow([os.path.basename(os.path.dirname(p)), f"{r['mad']:.3f}"]
                       + [f"{r['sampled'][k]:.4f}" for k in DIMS]
                       + [f"{Y[t][n]:.2f}" for t in tech])
    print(f"\n  wrote {out}\n  wrote {tab}")


if __name__ == "__main__":
    main()
