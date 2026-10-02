#!/usr/bin/env python3
"""Absolute-stock version of the baseline pathway: dwellings per technology, 2024-2050, with the
spread across the retained weight ensemble drawn as bands.

The stacked-to-100% panels (scenario_mix_grid.py) show composition and hide the ensemble entirely,
because they plot the median set. This one does the opposite: it plots the number of dwellings on
each technology, with the full min-max range and the interquartile range across the retained sets,
so the width of the preference uncertainty is visible per technology and comparable between them.

    python ensemble_band.py --scope nl
    python ensemble_band.py --scope nl --scenario social_learning_factor_high
    python ensemble_band.py --scope nl --panels          # one small panel per technology

Writes results/<scope>/figures/fig_ensemble_band_<scenario>.png and a tidy CSV beside it.
"""
import argparse, csv, glob, json, os, sys
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
PAPER_FIGS_DIR = os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures")

MERGE = {"NATURAL_GAS_BLOCK": "NATURAL_GAS_BOILER"}
TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE  = {"NATURAL_GAS_BOILER": "Natural gas", "HYBRID_HEAT_PUMP": "Hybrid heat pump",
         "ELECTRIC_HEAT_PUMP": "Electric heat pump", "DISTRICT_HEATING": "District heating"}
COLOR = {"NATURAL_GAS_BOILER": "#999999", "HYBRID_HEAT_PUMP": "#56B4E9",
         "ELECTRIC_HEAT_PUMP": "#0072B2", "DISTRICT_HEATING": "#D55E00"}


def tag(scope):
    k, _, n = scope.partition(":")
    if k.lower() in ("nl", "netherlands"): return "nl"
    return n.strip().lower().replace(" ", "-") if k == "province" else "gemeente_" + n.strip().lower()


def one_set(path, scen):
    """{year: {tech: dwellings}} for one weight set, averaged over MC iterations."""
    acc = defaultdict(lambda: defaultdict(float)); iters = set()
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            if r["scenario_name"] != scen or r["ownership"] != "TOTAL": continue
            iters.add(r["iteration"])
            acc[int(float(r["year"]))][MERGE.get(r["heating_system"], r["heating_system"])] \
                += float(r["installed_current"])
    n = max(len(iters), 1)
    return {y: {t: d.get(t, 0.0) / n for t in TECHS} for y, d in acc.items()}


def expected_sets(base, search):
    """Run folders are named set<i>_mad<mad> from the retained list (model/run.py). Rebuilding the
    names from the search file selects exactly this ensemble's runs and ignores any folders left
    behind by an earlier calibration that happen to sit in the same directory."""
    d = json.load(open(search))
    ret = d.get("retained") or []
    if not ret: sys.exit(f"[band] no retained sets in {search}")
    width = len(str(len(ret) - 1))
    want = [f"set{str(i).zfill(width)}_mad{e['mad']:.2f}" for i, e in enumerate(ret)]
    have = [w for w in want
            if os.path.exists(os.path.join(base, "calib", w, "simulation_results.csv"))]
    if not have:
        # a quick run with `--weights representative` (or central) writes folders named after the
        # representative keys of the same search file instead of set<i>_mad<mad>
        reps = list(d.get("representative", {})) + ["central"]
        have = [w for w in reps if os.path.exists(os.path.join(base, "calib", w, "simulation_results.csv"))]
        if have:
            print(f"[band] no full-ensemble folders; using the representative runs {', '.join(have)} "
                  f"-- a band over {len(have)} sets only")
            return [os.path.join(base, "calib", w, "simulation_results.csv") for w in have]
    if len(have) < len(want):
        print(f"[band] {len(have)} of {len(want)} sets present -- the pathway batch is still "
              f"running or was interrupted; the band is provisional")
    return [os.path.join(base, "calib", w, "simulation_results.csv") for w in have]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="nl")
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--panels", action="store_true", help="one panel per technology instead of one axes")
    ap.add_argument("--search", default=os.path.join(ROOT, "results", "calib1000", "calibration_search.json"),
                    help="calibration_search.json whose retained sets define the ensemble; only the\n"
                         "run folders belonging to it are read (results/<scope>/calib/ can hold the\n"
                         "leftovers of earlier ensembles, and globbing mixes them together)")
    ap.add_argument("--any-set", action="store_true",
                    help="ignore --search and read every set* folder found (not recommended)")
    ap.add_argument("--runs", default=None,
                    help="glob of result CSVs to read instead of results/<scope>/calib/, e.g. the\n"
                         "baseline of every retained set from select_subsample.py validate:\n"
                         "results/calib1000_v3/subsample_validation/set[0-9][0-9][0-9].csv")
    ap.add_argument("--outer", type=float, default=0,
                    help="outer band percentile: 0 = full min-max range (default), 5 = 5-95 %%")
    ap.add_argument("--tag", default="", help="suffix for the output file names")
    ap.add_argument("--copy-to", default=None)
    a = ap.parse_args()
    if a.copy_to is None:       # manuscript figures are national: never overwrite them from a province
        a.copy_to = PAPER_FIGS_DIR if a.scope.strip().lower() in ("nl", "netherlands") else ""
    base = os.path.join(ROOT, "results", tag(a.scope))
    outdir = os.path.join(base, "figures"); os.makedirs(outdir, exist_ok=True)

    if a.runs:
        sets = sorted(glob.glob(a.runs))
    else:
        sets = expected_sets(base, a.search) if not a.any_set else \
            sorted(glob.glob(os.path.join(base, "calib", "set*", "simulation_results.csv")))
    if not sets: sys.exit(f"[band] no weight-set runs under {base}/calib for this ensemble")
    print(f"[band] {len(sets)} weight sets, scenario '{a.scenario}'")

    series = defaultdict(lambda: defaultdict(list))     # tech -> year -> [one value per set]
    for i, p in enumerate(sets, 1):
        got = one_set(p, a.scenario)
        if not got:
            print(f"  [{i}/{len(sets)}] {os.path.basename(os.path.dirname(p))}: scenario absent, skipped")
            continue
        for y, d in got.items():
            for t in TECHS: series[t][y].append(d[t])
    years = sorted(series[TECHS[0]])
    if not years: sys.exit(f"[band] scenario '{a.scenario}' not found in any run")
    n_sets = len(series[TECHS[0]][years[0]])

    lo, hi = a.outer, 100 - a.outer
    stats = {t: {q: np.array([np.percentile(series[t][y], q) for y in years])
                 for q in (lo, 25, 50, 75, hi)} for t in TECHS}
    stats = {t: {**v, 0: v[lo], 100: v[hi]} for t, v in stats.items()}   # 0/100 = the outer band

    cache = os.path.join(outdir, f"ensemble_band_{a.scenario}{a.tag}.csv")
    with open(cache, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["tech", "year", "min" if not lo else f"p{lo:g}", "p25", "median", "p75",
                    "max" if not lo else f"p{hi:g}"])
        for t in TECHS:
            for j, y in enumerate(years):
                w.writerow([t, y] + [f"{stats[t][q][j]:.0f}" for q in (0, 25, 50, 75, 100)])

    mil = FuncFormatter(lambda v, _: f"{v/1e6:g}")
    def draw(ax, t, legend_lines):
        c = COLOR[t]
        ax.fill_between(years, stats[t][0], stats[t][100], color=c, alpha=0.15, linewidth=0)
        ax.fill_between(years, stats[t][25], stats[t][75], color=c, alpha=0.35, linewidth=0)
        ln, = ax.plot(years, stats[t][50], color=c, lw=2.2, label=NICE[t])
        legend_lines.append(ln)
        ax.yaxis.set_major_formatter(mil); ax.grid(alpha=.25)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)

    lines = []
    if a.panels:
        fig, axes = plt.subplots(2, 2, figsize=(10, 6.4), sharex=True)
        for ax, t in zip(axes.flat, TECHS):
            draw(ax, t, lines); ax.set_title(NICE[t], fontsize=10)
            ax.set_xlim(min(years), max(years)); ax.set_ylim(bottom=0)
            ax.set_ylabel("million dwellings", fontsize=9)
    else:
        fig, ax = plt.subplots(figsize=(9, 5.6))
        for t in TECHS: draw(ax, t, lines)
        ax.set_xlim(min(years), max(years)); ax.set_ylim(bottom=0)
        ax.set_ylabel("million dwellings"); ax.set_xlabel("year")
        ax.legend(handles=lines, fontsize=9.5, frameon=False, loc="upper right", ncol=2)
    fig.suptitle(f"Dwellings per heating technology, {a.scenario} scenario\n"
                 f"median across the {n_sets} retained weight sets; bands are the interquartile "
                 + ("and full range" if not lo else f"and {lo:g}-{hi:g} % range"), fontsize=11.5)
    fig.tight_layout(rect=(0, 0, 1, 0.94 if not a.panels else 0.93))
    f = os.path.join(outdir, f"fig_ensemble_band_{a.scenario}{a.tag}.png")
    fig.savefig(f, dpi=150, bbox_inches="tight"); plt.close(fig)
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil; shutil.copy2(f, a.copy_to)

    print(f"\n2050 spread across the ensemble ({n_sets} sets), thousand dwellings")
    l, h = ("min", "max") if not lo else (f"p{lo:g}", f"p{hi:g}")
    print(f"{'technology':22s}{l:>9s}{'p25':>9s}{'median':>9s}{'p75':>9s}{h:>9s}{h + '-' + l:>10s}")
    for t in TECHS:
        v = [stats[t][q][-1] / 1e3 for q in (0, 25, 50, 75, 100)]
        print(f"{NICE[t]:22s}" + "".join(f"{x:9.0f}" for x in v) + f"{v[4]-v[0]:10.0f}")
    print(f"\ncache -> {cache}\nwrote: {f}")


if __name__ == "__main__":
    main()
