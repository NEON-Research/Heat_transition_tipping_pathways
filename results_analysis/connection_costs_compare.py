#!/usr/bin/env python3
"""
Paired comparison of a reference run and a counterfactual run (same weights, same seeds), e.g. the
connection-cost counterfactual (-Dht.connectionCostsOff=1) against the central weight set.

    python connection_costs_compare.py --ref results/nl/calib/set00_mad0.22/simulation_results.csv \
        --alt results/nl/conn_costs_off/simulation_results.csv --outdir results/nl/conn_costs_off/figures

Writes, into --outdir:
  cc_baseline_mix.png       stacked mix 2024-2050, reference | counterfactual (baseline scenario)
  cc_baseline_lines.png     share per technology, reference solid vs counterfactual dashed
  cc_ownership_2050.png     2050 mix per ownership group, paired bars (baseline)
  cc_scenarios_2050.png     2050 mix per scenario, paired bars       (only if > 1 scenario)
  cc_shift_heatmap.png      2050 shift (pp) per scenario x technology (only if > 1 scenario)
  cc_shares.csv             every share behind the figures
The gas block is merged into natural gas, as in scenario_mix_grid.py.
"""
import argparse, os, sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from scenario_mix_grid import MERGE, TECHS, NICE, COLOR, SCEN_LABEL

OWN = {"PRIVATELY_OWNED": "Owner-occupied", "PRIVATELY_RENTED": "Private rental",
       "SOCIAL_HOUSING": "Social housing", "HOME_OWNER_ASSOCIATION": "Owners' association (VvE)"}


def shares(path):
    """Share (%) of the stock per scenario, ownership, year and technology; ownership 'ALL' = total."""
    d = pd.read_csv(path)
    d = d[d.ownership != "TOTAL"].copy()
    d["tech"] = d.heating_system.replace(MERGE)
    keys = ["scenario_name", "ownership", "year", "tech"]
    g = (d.groupby(keys + ["iteration"]).installed_current.sum()   # boiler + block together
          .groupby(keys).mean())                                     # then mean over iterations
    tot = g.groupby(["scenario_name", "year", "tech"]).sum()
    tot = pd.concat({"ALL": tot}, names=["ownership"]).reorder_levels(g.index.names)
    g = pd.concat([g, tot])
    s = g / g.groupby(["scenario_name", "ownership", "year"]).transform("sum") * 100
    return s.unstack("tech").reindex(columns=TECHS).fillna(0)


def stack(ax, df, title):
    ys = [df[t].values for t in TECHS]
    ax.stackplot(df.index, ys, colors=[COLOR[t] for t in TECHS], edgecolor="white", linewidth=0.6)
    ax.set_xlim(df.index.min(), df.index.max()); ax.set_ylim(0, 100)
    ax.set_title(title, fontsize=11, loc="left")
    ax.grid(axis="y", color="#dddddd", lw=0.6); ax.set_axisbelow(True)
    for s in ("top", "right"): ax.spines[s].set_visible(False)


def legend(fig, extra=None):
    h = [plt.Rectangle((0, 0), 1, 1, color=COLOR[t]) for t in TECHS]
    l = [NICE[t] for t in TECHS]
    if extra: h += extra[0]; l += extra[1]
    fig.legend(h, l, loc="lower center", ncol=len(l), frameon=False, fontsize=9)


def paired_bars(ax, rows, ref, alt, labels):
    """Two horizontal stacked bars per row: reference (upper, faded) and counterfactual (lower)."""
    y = np.arange(len(rows))[::-1] * 1.0
    for off, df, a in ((0.2, ref, 0.45), (-0.2, alt, 1.0)):
        left = np.zeros(len(rows))
        for t in TECHS:
            v = df.loc[rows, t].values
            ax.barh(y + off, v, left=left, height=0.36, color=COLOR[t], alpha=a, edgecolor="white", lw=0.6)
            for yi, l0, vi in zip(y + off, left, v):
                if vi >= 7:
                    ax.text(l0 + vi / 2, yi, f"{vi:.0f}", ha="center", va="center", fontsize=7,
                            color="white" if (a == 1.0 and t != "HYBRID_HEAT_PUMP") else "black")
            left += v
    ax.set_yticks(np.r_[y + 0.2, y - 0.2])
    ax.set_yticklabels([f"{l} · reference" for l in labels] + ["no connection costs"] * len(labels),
                       fontsize=8)
    ax.set_xlim(0, 100); ax.set_xlabel("share of dwellings in 2050 (%)")
    for s in ("top", "right"): ax.spines[s].set_visible(False)


def band(a, sfx):
    """Across the weight ensemble (baseline scenario): median and 5-95 % band per technology, and
    the PAIRED 2050 shift of every set (same weights, same seeds, only the costs differ)."""
    names = sorted(n for n in os.listdir(a.band_alt)
                   if os.path.exists(os.path.join(a.band_alt, n, "simulation_results.csv"))
                   and os.path.exists(os.path.join(a.band_ref, n, "simulation_results.csv")))
    if not names: print("[band] no matching sets"); return
    get = lambda root, n: shares(os.path.join(root, n, "simulation_results.csv")).loc[("baseline", "ALL")]
    R = {n: get(a.band_ref, n) for n in names}
    A = {n: get(a.band_alt, n) for n in names}
    rows = []
    for n in names:
        for t in TECHS:
            rows.append((n, t, R[n].loc[a.year, t], A[n].loc[a.year, t]))
    pd.DataFrame(rows, columns=["set", "tech", "reference", "counterfactual"]).assign(
        shift_pp=lambda d: d.counterfactual - d.reference).round(3).to_csv(
        os.path.join(a.outdir, "cc_band_2050_by_set.csv"), index=False)

    # (a) median + 5-95 % per technology, one panel each
    fig, axs = plt.subplots(1, 4, figsize=(13, 3.6), sharey=True)
    for ax, t in zip(axs, TECHS):
        for D, ls, alpha in ((R, "-", 0.18), (A, (0, (4, 2)), 0.30)):
            M = pd.DataFrame({n: D[n][t] for n in names})
            lo, med, hi = M.quantile(0.05, axis=1), M.median(axis=1), M.quantile(0.95, axis=1)
            ax.fill_between(M.index, lo, hi, color=COLOR[t], alpha=alpha, lw=0)
            ax.plot(M.index, med, color=COLOR[t], lw=2, ls=ls)
        ax.set_title(NICE[t], loc="left", fontsize=10); ax.set_ylim(0, 100)
        ax.set_xlim(M.index.min(), M.index.max()); ax.grid(axis="y", color="#dddddd", lw=0.6)
        for s in ("top", "right"): ax.spines[s].set_visible(False)
    axs[0].set_ylabel("share of dwellings (%)")
    fig.legend([plt.Line2D([], [], color="#333", lw=2), plt.Line2D([], [], color="#333", lw=2, ls=(0, (4, 2)))],
               [f"{a.ref_label}: median, 5-95 % band", f"{a.alt_label}: median, 5-95 % band"],
               loc="lower center", ncol=2, frameon=False, fontsize=9)
    fig.suptitle(f"Baseline across {len(names)} weight sets{sfx}", x=0.01, ha="left", fontsize=11)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    fig.savefig(os.path.join(a.outdir, "cc_band_lines.png"), dpi=200); plt.close(fig)

    # (b) paired 2050 shift per set: one dot per weight set, median marked
    fig, ax = plt.subplots(figsize=(8, 3.4))
    rng = np.random.default_rng(0)
    for i, t in enumerate(TECHS):
        d = np.array([A[n].loc[a.year, t] - R[n].loc[a.year, t] for n in names])
        ax.scatter(d, i + rng.uniform(-0.12, 0.12, len(d)), s=22, color=COLOR[t],
                   edgecolor="white", lw=0.8, zorder=3)
        ax.plot([np.median(d)] * 2, [i - 0.28, i + 0.28], color="#222", lw=2, zorder=4)
        ax.text(max(d.max(), 0) + 1.5, i, f"median {np.median(d):+.1f} pp  (range {d.min():+.0f} to {d.max():+.0f})",
                va="center", fontsize=8, color="#333")
    ax.axvline(0, color="#999", lw=0.8)
    ax.set_yticks(range(len(TECHS))); ax.set_yticklabels([NICE[t] for t in TECHS], fontsize=9)
    ax.invert_yaxis(); ax.set_xlabel(f"change in {a.year} share (pp), per weight set")
    ax.set_xlim(min(-45, ax.get_xlim()[0]), 75)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    ax.set_title(f"Paired effect of removing connection costs, {len(names)} weight sets{sfx}",
                 loc="left", fontsize=10)
    fig.tight_layout()
    fig.savefig(os.path.join(a.outdir, "cc_band_shift_2050.png"), dpi=200); plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ref", required=True); ap.add_argument("--alt", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--ref-label", default="Reference (all costs)")
    ap.add_argument("--alt-label", default="No gas / DH connection costs")
    ap.add_argument("--scope-label", default="")
    ap.add_argument("--year", type=int, default=2050)
    ap.add_argument("--band-ref", default=None, help="folder of per-set reference runs (<set>/simulation_results.csv)")
    ap.add_argument("--band-alt", default=None, help="folder of the matching counterfactual runs, same set names")
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    R, A = shares(a.ref), shares(a.alt)
    scen = [s for s in R.index.get_level_values(0).unique() if s in set(A.index.get_level_values(0))]
    if "baseline" not in scen: sys.exit("no baseline scenario in both runs")
    sfx = f" - {a.scope_label}" if a.scope_label else ""
    pd.concat({"reference": R, "counterfactual": A}, names=["run"]).round(3).to_csv(
        os.path.join(a.outdir, "cc_shares.csv"))

    # 1. baseline mix, side by side
    r, c = R.loc[("baseline", "ALL")], A.loc[("baseline", "ALL")]
    fig, ax = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    stack(ax[0], r, a.ref_label); stack(ax[1], c, a.alt_label)
    ax[0].set_ylabel("share of dwellings (%)")
    for t in TECHS:
        ax[1].annotate(f"{c.loc[a.year, t]:.0f}%  ({c.loc[a.year, t] - r.loc[a.year, t]:+.0f} pp)",
                       (a.year, c.loc[a.year, TECHS[:TECHS.index(t)]].sum() + c.loc[a.year, t] / 2),
                       xytext=(4, 0), textcoords="offset points", va="center", fontsize=8)
    fig.suptitle(f"Baseline heating mix with and without connection costs{sfx}", x=0.01, ha="left", fontsize=12)
    legend(fig); fig.tight_layout(rect=(0, 0.07, 0.93, 0.95))
    fig.savefig(os.path.join(a.outdir, "cc_baseline_mix.png"), dpi=200); plt.close(fig)

    # 2. baseline lines per technology
    fig, ax = plt.subplots(figsize=(10, 4.8))
    for t in TECHS:
        ax.plot(r.index, r[t], color=COLOR[t], lw=2)
        ax.plot(c.index, c[t], color=COLOR[t], lw=2, ls=(0, (4, 2)))
    # end labels at the counterfactual's end value, pushed apart so none overlap (>= 4.5 pp)
    ends = sorted((c[t].iloc[-1], t) for t in TECHS)
    ys = []
    for v, _ in ends: ys.append(max(v, ys[-1] + 4.5) if ys else v)
    for (v, t), y in zip(ends, ys):
        ax.annotate(f"{NICE[t]}: {r[t].iloc[-1]:.0f}% → {v:.0f}%", (c.index[-1], v),
                    xytext=(c.index[-1] + 0.6, y), textcoords="data", va="center", fontsize=8,
                    color="#333333", annotation_clip=False,
                    arrowprops=dict(arrowstyle="-", color="#aaaaaa", lw=0.6) if abs(y - v) > 1 else None)
    ax.set_ylim(0, 100); ax.set_xlim(r.index.min(), r.index.max())
    ax.set_ylabel("share of dwellings (%)"); ax.grid(axis="y", color="#dddddd", lw=0.6)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    ax.legend([plt.Line2D([], [], color="#333", lw=2), plt.Line2D([], [], color="#333", lw=2, ls=(0, (4, 2)))],
              [a.ref_label, a.alt_label], frameon=False, loc="upper left", fontsize=9)
    ax.set_title(f"Baseline: share per technology{sfx}", loc="left", fontsize=11)
    fig.tight_layout(rect=(0, 0, 0.82, 1))
    fig.savefig(os.path.join(a.outdir, "cc_baseline_lines.png"), dpi=200); plt.close(fig)

    # 3. 2050 by ownership (baseline)
    owns = [o for o in OWN if ("baseline", o) in R.index.droplevel(2).unique()]
    rr = pd.DataFrame({o: R.loc[("baseline", o, a.year)] for o in owns}).T
    cc = pd.DataFrame({o: A.loc[("baseline", o, a.year)] for o in owns}).T
    fig, ax = plt.subplots(figsize=(9, 4.2))
    paired_bars(ax, owns, rr, cc, [OWN[o] for o in owns])
    ax.set_title(f"Baseline {a.year} per ownership group{sfx}", loc="left", fontsize=10)
    legend(fig); fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(os.path.join(a.outdir, "cc_ownership_2050.png"), dpi=200); plt.close(fig)

    if len(scen) > 1:
        order = [s for s in SCEN_LABEL if s in scen] + [s for s in scen if s not in SCEN_LABEL]
        rr = pd.DataFrame({s: R.loc[(s, "ALL", a.year)] for s in order}).T
        cc = pd.DataFrame({s: A.loc[(s, "ALL", a.year)] for s in order}).T
        lab = [SCEN_LABEL.get(s, s) for s in order]
        # 4. paired bars per scenario
        fig, ax = plt.subplots(figsize=(10, 0.55 * len(order) + 1.6))
        paired_bars(ax, order, rr, cc, lab)
        ax.set_title(f"{a.year} mix per scenario: reference (faded) vs no connection costs{sfx}",
                     loc="left", fontsize=10)
        legend(fig); fig.tight_layout(rect=(0, 0.04, 1, 1))
        fig.savefig(os.path.join(a.outdir, "cc_scenarios_2050.png"), dpi=200); plt.close(fig)
        # 5. shift heatmap (diverging, grey-white midpoint)
        dlt = (cc - rr)[TECHS]
        m = max(5, np.ceil(np.abs(dlt.values).max() / 5) * 5)
        fig, ax = plt.subplots(figsize=(8.5, 0.38 * len(order) + 1.8))
        im = ax.imshow(dlt.values, cmap="RdBu_r", vmin=-m, vmax=m, aspect="auto")
        for i in range(dlt.shape[0]):
            for j in range(dlt.shape[1]):
                v = dlt.values[i, j]
                ax.text(j, i, f"{v:+.1f}", ha="center", va="center", fontsize=8,
                        color="white" if abs(v) > 0.6 * m else "black")
        ax.set_xticks(range(len(TECHS)))
        ax.set_xticklabels([NICE[t].replace(" heat", "\nheat") for t in TECHS], fontsize=9)
        ax.xaxis.tick_top()
        ax.set_yticks(range(len(order))); ax.set_yticklabels(lab, fontsize=9)
        cb = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02); cb.set_label("pp change in 2050 share")
        fig.suptitle(f"Shift in {a.year} share from removing connection costs (pp){sfx}",
                     x=0.01, ha="left", fontsize=10)
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "cc_shift_heatmap.png"), dpi=200); plt.close(fig)

    if a.band_ref and a.band_alt:
        band(a, sfx)

    print(f"baseline {a.year} (ALL):")
    print(pd.DataFrame({"reference": r.loc[a.year], "counterfactual": c.loc[a.year],
                        "shift_pp": c.loc[a.year] - r.loc[a.year]}).round(1).to_string())
    print(f"figures -> {a.outdir}")


if __name__ == "__main__":
    main()
