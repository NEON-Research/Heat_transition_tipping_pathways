#!/usr/bin/env python3
"""GLUE likelihood weighting of the retained weight ensemble.

WHY
---
The calibration currently uses only half of GLUE: sets are accepted or rejected at MAD < tolerance
and then treated as equally credible. That discards the information in the MAD ranking -- the best
retained set (MAD 0.31) and the worst (MAD 1.98) count the same. This script applies the other
half: an informal likelihood measure that grades the retained sets by fit, and reports the bands
as likelihood-weighted quantiles.

    L_i = max(0, 1 - MAD_i / MAD_ref) ** N        w_i = L_i / sum(L)

MAD_ref is `default_mad` from the calibration search -- the MAD scored by the ORIGINAL AnyLogic
default weights. So L = 1 is a perfect fit and L = 0 is "no better than the uncalibrated model".
N is a shaping exponent chosen by the analyst, not estimated from data; this is the well-known
subjective element of informal GLUE (Beven & Binley 1992; criticised by Stedinger et al. 2008).
It is handled the accepted way: pick a modest N, report it, and show the sensitivity.

SCOPE -- deliberate
-------------------
Weighting is applied to the BASELINE band only. Weights attach to parameter sets and act *within*
a scenario; they never rank one scenario against another, since scenarios are constructed policy
alternatives rather than competing hypotheses, and all scenarios share the same 21 sets and hence
the same fits. The cross-scenario figures stay equal-weighted on purpose: showing that a result
holds across the full retained range is a robustness claim whose force comes from the breadth.

CAVEAT
------
Calibration ran ONE replication per set, so MAD carries Monte Carlo noise. Sharp weighting (N>=2)
risks fitting that noise, and the effective sample size falls fast. N=1 is the default for both
reasons. Re-estimate MAD at ~5 replications before going higher.

    python glue_weighting.py                    # N=1, baseline, NL
    python glue_weighting.py --n 2 --scope nl
"""
import argparse, csv, glob, json, os, sys
import numpy as np, matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import overview_figures as ov
ROOT = ov.ROOT


from glue import likelihood, ess, wquantile           # shared with overview_figures.py


def _selftest():
    from glue import _selftest as _st
    print("  "+_st())


# ------------------------------------------------------------------ data
def load_ensemble(scope_tag):
    d = json.load(open(os.path.join(ROOT, "results", "calib", "calibration_search.json")))
    ret = sorted(d["retained"], key=lambda s: s["mad"])
    mad = np.array([r["mad"] for r in ret])
    paths = sorted(glob.glob(os.path.join(ROOT, "results", scope_tag, "calib", "set*",
                                          "simulation_results.csv")))
    if len(paths) != len(ret):
        raise SystemExit(f"{len(paths)} set folders vs {len(ret)} retained records -- out of sync")
    for p, r in zip(paths, ret):
        tag = os.path.basename(os.path.dirname(p))
        if abs(float(tag.split("mad")[1]) - r["mad"]) > 0.006:
            raise SystemExit(f"{tag} does not match retained MAD {r['mad']:.3f}")
    return d, ret, mad, paths


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="nl")
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--n", type=float, default=1.0, help="likelihood shaping exponent (default 1)")
    a = ap.parse_args()
    tag = ov.tag(a.scope)
    _selftest()

    d, ret, mad, paths = load_ensemble(tag)
    M0 = d["default_mad"]
    runs = [ov.load(p, "TOTAL") for p in paths]
    tech = [t for t in ov.ORDER
            if np.mean([r[a.scenario][2050][t] for r in runs]) > 0.5]
    years = sorted(runs[0][a.scenario])
    Y = {t: np.array([[r[a.scenario][y][t] for r in runs] for y in years]) for t in tech}  # year x set

    L = likelihood(mad, M0, a.n); w = L / L.sum()
    w0 = np.ones(len(mad)) / len(mad)
    print(f"\n  {len(mad)} retained sets | MAD {mad.min():.2f}-{mad.max():.2f} | "
          f"reference MAD (AnyLogic defaults) {M0:.2f}")
    print(f"  N = {a.n:g}  ->  ESS {ess(w):.1f} of {len(w)} | "
          f"weights {100*w.min():.1f}%-{100*w.max():.1f}% (best:worst = {w.max()/max(w.min(),1e-12):.0f}:1)")

    # ---- main figure: weighted vs unweighted band, per technology -------------------------
    fig, axes = plt.subplots(1, len(tech), figsize=(3.5 * len(tech), 3.4), sharey=True)
    axes = np.atleast_1d(axes)
    for ax, t in zip(axes, tech):
        lo0 = [wquantile(Y[t][i], w0, .05) for i in range(len(years))]
        hi0 = [wquantile(Y[t][i], w0, .95) for i in range(len(years))]
        md0 = [wquantile(Y[t][i], w0, .50) for i in range(len(years))]
        lo = [wquantile(Y[t][i], w, .05) for i in range(len(years))]
        hi = [wquantile(Y[t][i], w, .95) for i in range(len(years))]
        md = [wquantile(Y[t][i], w, .50) for i in range(len(years))]
        ax.fill_between(years, lo0, hi0, facecolor="none", edgecolor="#999", lw=0.9, ls=(0, (4, 2)),
                        label="equal weights, 5–95%")
        ax.fill_between(years, lo, hi, color=ov.COL[t], alpha=0.35, lw=0,
                        label=f"GLUE N={a.n:g}, 5–95%")
        ax.plot(years, md0, color="#777", lw=1.2, ls="--", label="equal-weight median")
        ax.plot(years, md, color=_dark(ov.COL[t]), lw=2.0, label="weighted median")
        ax.set_title(f"{ov.NICE[t]}\n2050: {md[-1]:.1f}%  [{lo[-1]:.1f}–{hi[-1]:.1f}]",
                     fontsize=9.5)
        ax.set_xlim(min(years), max(years)); ax.set_ylim(0, 100)
        ax.grid(alpha=0.3, ls="--", lw=0.6); ax.tick_params(labelsize=8)
        ax.set_xlabel("Year", fontsize=8.5)
    axes[0].set_ylabel("% of dwellings", fontsize=9)
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="lower center", ncol=4, fontsize=8.5, frameon=False,
               bbox_to_anchor=(0.5, -0.09))
    fig.suptitle(f"{a.scenario.replace('_',' ').capitalize()}: likelihood-weighted vs equal-weighted "
                 f"ensemble band  (N={a.n:g}, ESS {ess(w):.1f} of {len(w)})", fontsize=11.5)
    fig.tight_layout(rect=[0, 0.02, 1, 0.94])
    figdir = os.path.join(ROOT, "results", tag, "figures"); os.makedirs(figdir, exist_ok=True)
    f1 = os.path.join(figdir, "fig_glue_band.png")
    fig.savefig(f1, dpi=150, bbox_inches="tight"); plt.close(fig)

    # ---- diagnostic: the weights themselves, and ESS vs N ---------------------------------
    fig, (axL, axR) = plt.subplots(1, 2, figsize=(11.2, 3.3),
                                   gridspec_kw=dict(width_ratios=[1.9, 1.0]))
    names = [os.path.basename(os.path.dirname(p)).split("_")[0] for p in paths]
    axL.bar(range(len(w)), 100 * w, color=[plt.cm.viridis_r(m / mad.max()) for m in mad],
            edgecolor="white", lw=0.5)
    axL.axhline(100 / len(w), color="#b03030", ls="--", lw=1.2,
                label=f"equal weight ({100/len(w):.1f}%)")
    axL.set_xticks(range(len(w))); axL.set_xticklabels(names, rotation=90, fontsize=6.5)
    axL.set_ylabel("weight (%)", fontsize=9); axL.legend(fontsize=8, frameon=False)
    axL.set_title(f"Per-set likelihood weight, N={a.n:g} (sets ordered by fit)",
                  fontsize=9.5, loc="left")
    axL.tick_params(axis="y", labelsize=8)

    ns = np.linspace(0, 4, 41)
    axR.plot(ns, [ess(likelihood(mad, M0, x) / likelihood(mad, M0, x).sum()) for x in ns],
             color="#1E90FF", lw=2)
    axR.axvline(a.n, color="#b03030", ls="--", lw=1.2)
    axR.axhline(len(w), color="#999", ls=":", lw=1)
    axR.set_xlabel("shaping exponent N", fontsize=9)
    axR.set_ylabel("effective sample size", fontsize=9)
    axR.set_title("Ensemble cost of sharpening", fontsize=9.5, loc="left")
    axR.grid(alpha=0.3, ls="--", lw=0.6); axR.tick_params(labelsize=8); axR.set_ylim(0, len(w) + 1)
    fig.tight_layout()
    f2 = os.path.join(figdir, "fig_glue_weights.png")
    fig.savefig(f2, dpi=150, bbox_inches="tight"); plt.close(fig)

    # ---- N-sensitivity table ---------------------------------------------------------------
    f3 = os.path.join(figdir, "glue_sensitivity.csv")
    with open(f3, "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow(["scenario", "N", "ESS", "technology", "year", "p5", "median", "p95", "width"])
        print(f"\n  {a.scenario}, 2050 (median [5-95%]):")
        for N in (0, 1, 2, 3):
            LN = likelihood(mad, M0, N); wN = LN / LN.sum()
            line = f"    N={N}  ESS {ess(wN):4.1f} | "
            for t in tech:
                for y_i, y in enumerate(years):
                    if y not in (2030, 2040, 2050): continue
                    q = [wquantile(Y[t][y_i], wN, x) for x in (.05, .5, .95)]
                    wr.writerow([a.scenario, N, f"{ess(wN):.2f}", ov.NICE[t], y,
                                 f"{q[0]:.2f}", f"{q[1]:.2f}", f"{q[2]:.2f}", f"{q[2]-q[0]:.2f}"])
                    if y == 2050:
                        line += f"{ov.NICE[t][:8]} {q[1]:.1f} [{q[0]:.1f}-{q[2]:.1f}]  "
            print(line)

    # ---- per-set weights, for the appendix --------------------------------------------------
    f4 = os.path.join(figdir, "glue_weights.csv")
    with open(f4, "w", newline="") as fh:
        wr = csv.writer(fh); wr.writerow(["set", "mad", "likelihood", "weight_pct"])
        for p, m, li, wi in zip(paths, mad, L, w):
            wr.writerow([os.path.basename(os.path.dirname(p)), f"{m:.3f}",
                         f"{li:.4f}", f"{100*wi:.2f}"])
    for f in (f1, f2, f3, f4): print(f"  wrote {f}")


def _dark(c, f=0.62):
    import matplotlib.colors as mc
    r, g, b = mc.to_rgb(c); return (r * f, g * f, b * f)


if __name__ == "__main__":
    main()
