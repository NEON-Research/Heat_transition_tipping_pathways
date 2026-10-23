#!/usr/bin/env python3
"""
What the deciding households would pick on cost, on modelled utility, and what they actually pick.

Reads the decision probe the engine writes when HT_DECPROBE=<csv> is set (Simulation.writeDecProbe):
per year and decider group, every decision cross-tabulated as (cost-optimal option, highest-utility
option, chosen option), weighted by households. Cost-optimal = cheapest possible option on EAC;
highest utility = highest modelled utility before the random term. Landlords and housing blocks
choose on EAC, so for them the two coincide; they differ only for owner-occupiers.

The shares are of DECISIONS in the year, not of the stock: an owner-occupier past 75 % of the
lifetime of their system reconsiders every year (and usually keeps it), so one dwelling can count
in several years.

    python decision_optimality.py --run reference=results/nl/decisions/dec_ref.csv \
        --run "no connection costs=results/nl/decisions/dec_off.csv" --outdir results/nl/decisions/figures

ENSEMBLE mode: one probe file per weight set (--ensemble "<glob>"), one row of three panels, each
the MEDIAN over the sets of the per-set shares (renormalised to 100, as in scenario_mix_grid.py).
--installed-only keeps only decisions that led to an installation (end of life, or a switch), so an
owner-occupier past 75 % of the lifetime who keeps the system is not counted (needs the probe's
`installed` column):

    python decision_optimality.py --ensemble "results/nl/decisions_band/set*/decisions.csv"         --installed-only --outdir results/nl/decisions_band/figures

Writes dec_mix_all.png, dec_mix_owners.png (stacked shares: cost-optimal | highest utility | chosen,
one row per run), dec_agreement.png (share of decisions where the chosen / highest-utility option is
the cost-optimal one) and dec_shares.csv.
"""
import argparse, os, sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from scenario_mix_grid import MERGE, TECHS, NICE, COLOR, draw

COLS = [("cost_optimal", "Cost-optimal"), ("top_utility", "Highest utility"), ("chosen", "Chosen")]


def load(path):
    d = pd.read_csv(path)
    for c, _ in COLS: d[c] = d[c].replace(MERGE)
    return d


def shares(d, col):
    g = d.groupby(["year", col]).households.sum().unstack(col).reindex(columns=TECHS).fillna(0)
    return g.div(g.sum(axis=1), axis=0) * 100


def mix_figure(runs, title, path, owners_only):
    """Rows = runs, columns = cost-optimal | highest utility | chosen, drawn with the scenario grid's
    own draw() so the figure matches the other scenario-mix figures panel for panel."""
    data, grid = {}, []
    for lab, d in runs:
        if owners_only: d = d[d.decider == "PRIVATELY_OWNED"]
        row = []
        for col, nice in COLS:
            key = f"{lab}: {nice[0].lower() + nice[1:]}"
            s = shares(d, col)
            data[key] = {int(y): {t: float(s.loc[y, t]) for t in TECHS} for y in s.index}
            row.append(key)
        grid.append(row)
    draw(data, None, 3, title, path, grid=grid, ylabel="% of decisions")


def ensemble_figure(files, title, path, installed_only, owners_only=False):
    """One row: cost-optimal | highest utility | chosen, each the median over the weight sets."""
    per = {c: [] for c, _ in COLS}
    for f in files:
        d = load(f)
        if installed_only:
            if "installed" not in d.columns:
                sys.exit(f"{f} has no 'installed' column: rerun with the current engine")
            d = d[d.installed == 1]
        if owners_only: d = d[d.decider == "PRIVATELY_OWNED"]
        for c, _ in COLS: per[c].append(shares(d, c))
    data, row, rows = {}, [], []
    for c, nice in COLS:
        med = pd.concat(per[c]).groupby(level=0).median()        # year x tech, median over sets
        med = med.div(med.sum(axis=1), axis=0) * 100              # medians do not sum to 100
        data[nice] = {int(y): {t: float(med.loc[y, t]) for t in TECHS} for y in med.index}
        row.append(nice)
        rows.append(med.stack().rename("share_pct").reset_index().rename(columns={c: "tech"}).assign(measure=c))
    draw(data, None, 3, title, path, grid=[row],
         ylabel="% of installing dwellings" if installed_only else "% of decisions")
    pd.concat(rows).round(3).to_csv(os.path.splitext(path)[0] + ".csv", index=False)
    print(f"{len(files)} weight sets -> {path}")


def agreement(d):
    """Per year: share of decisions where the chosen / highest-utility option is the cost-optimal one."""
    out = {}
    for col in ("chosen", "top_utility"):
        m = d.assign(hit=(d[col] == d.cost_optimal) * d.households).groupby("year")
        out[col] = m.hit.sum() / m.households.sum() * 100
    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="append", default=[], metavar="LABEL=CSV")
    ap.add_argument("--ensemble", default=None, help="glob of per-weight-set probe files (ensemble mode)")
    ap.add_argument("--installed-only", action="store_true",
                    help="ensemble mode: count only decisions that led to an installation")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--scope-label", default="")
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    if a.ensemble:
        import glob
        files = sorted(glob.glob(a.ensemble))
        if not files: sys.exit(f"no files match {a.ensemble}")
        ensemble_figure(files, "Cost-optimal, highest-utility and chosen heating system",
                        os.path.join(a.outdir, "dec_mix_all.png"), a.installed_only)
        ensemble_figure(files, "Cost-optimal, highest-utility and chosen heating system, owner-occupiers",
                        os.path.join(a.outdir, "dec_mix_owners.png"), a.installed_only, owners_only=True)
        return
    if not a.run: sys.exit("give --run LABEL=CSV (single runs) or --ensemble GLOB")
    runs = [(lab, load(p)) for lab, p in (r.split("=", 1) for r in a.run)]
    sfx = f" - {a.scope_label}" if a.scope_label else ""

    mix_figure(runs, "Cost-optimal, highest-utility and chosen heating system per decision",
               os.path.join(a.outdir, "dec_mix_all.png"), False)
    mix_figure(runs, "Cost-optimal, highest-utility and chosen heating system, owner-occupiers",
               os.path.join(a.outdir, "dec_mix_owners.png"), True)

    # agreement lines: owner-occupiers (utility deciders) and all
    fig, axs = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
    styles = ["-", (0, (4, 2)), ":", "-."]
    for ax, (owners, ttl) in zip(axs, ((True, "Owner-occupiers"), (False, "All decision makers"))):
        for (lab, d), ls in zip(runs, styles):
            g = agreement(d[d.decider == "PRIVATELY_OWNED"] if owners else d)
            ax.plot(g.index, g["chosen"], color="#0072B2", lw=2, ls=ls)
            if owners: ax.plot(g.index, g["top_utility"], color="#D55E00", lw=2, ls=ls)
        ax.set_title(ttl, loc="left", fontsize=10); ax.set_ylim(0, 100)
        ax.grid(axis="y", color="#dddddd", lw=0.6)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    axs[0].set_ylabel("% of decisions that match\nthe cost-optimal option")
    h = [plt.Line2D([], [], color="#0072B2", lw=2), plt.Line2D([], [], color="#D55E00", lw=2)] + \
        [plt.Line2D([], [], color="#555", lw=2, ls=ls) for (_, _), ls in zip(runs, styles)]
    fig.legend(h, ["chosen = cost-optimal", "highest utility = cost-optimal"] + [lab for lab, _ in runs],
               loc="lower center", ncol=2 + len(runs), frameon=False, fontsize=9)
    fig.suptitle(f"How often the decision is the cost-optimal one{sfx}", x=0.01, ha="left", fontsize=11)
    fig.tight_layout(rect=(0, 0.08, 1, 0.95))
    fig.savefig(os.path.join(a.outdir, "dec_agreement.png"), dpi=200); plt.close(fig)

    rows = []
    for lab, d in runs:
        for grp, dd in (("ALL", d), ("PRIVATELY_OWNED", d[d.decider == "PRIVATELY_OWNED"])):
            for col, _ in COLS:
                s = shares(dd, col).stack().rename("share_pct").reset_index().rename(columns={col: "tech"})
                rows.append(s.assign(run=lab, group=grp, measure=col))
    pd.concat(rows).round(3).to_csv(os.path.join(a.outdir, "dec_shares.csv"), index=False)
    for lab, d in runs:
        o = d[d.decider == "PRIVATELY_OWNED"]
        print(f"\n{lab}: owner-occupiers, 2030 and 2050 (% of decisions)")
        for y in (2030, 2050):
            print(y, pd.DataFrame({c: shares(o, c).loc[y] for c, _ in COLS}).round(1).to_string(), sep="\n")
    print(f"figures -> {a.outdir}")


if __name__ == "__main__":
    main()
