#!/usr/bin/env python3
"""Stacked-to-100% heating-mix panels, 2024-2050: the full scenario grid and the per-section cuts.

One panel = one scenario, same axes everywhere, so panels are comparable by eye. The ensemble
scenarios show the MEDIAN over the retained weight sets (the band across sets is
fig_band_affordability); the price bracket and the loop knock-outs are single runs at the
central weights.

Views (each written as its own figure, so a results subsection can show only its own experiments):

  all           every scenario in the design (22 panels, full-width figure*)
  baseline      the reference pathway alone
  loops         the two learning loops: knock-outs, learning-factor scenarios, price bracket
  policy        rules, regulation and infrastructure: obligations, congestion, network expansion
  coordination  collective-actor alignment and the individual/collective technology settings

    python scenario_mix_grid.py --scope nl                    # extract + write every view
    python scenario_mix_grid.py --scope nl --no-extract       # re-plot from the cached CSV
    python scenario_mix_grid.py --scope nl --view loops       # one view only

Writes results/<scope>/figures/fig_scenario_mix_<view>.png plus the tidy cache
results/<scope>/figures/scenario_mix_grid.csv.
"""
import argparse, csv, glob, os, sys
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))

# block-heated gas dwellings are reported separately by the engine but are the same incumbent
# technology from the household's point of view, so they are folded into natural gas here.
MERGE = {"NATURAL_GAS_BLOCK": "NATURAL_GAS_BOILER"}
# stack order = the transition itself, incumbent at the bottom
TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE  = {"NATURAL_GAS_BOILER": "Natural gas", "HYBRID_HEAT_PUMP": "Hybrid heat pump",
         "ELECTRIC_HEAT_PUMP": "Electric heat pump", "DISTRICT_HEATING": "District heating"}
# Okabe-Ito, colour-blind safe. Grey incumbent; the two heat pumps share a blue family and are
# ordered light -> dark with the degree of electrification; district heating sits outside that
# family in vermillion because it is a different kind of option (collective, not individual).
COLOR = {"NATURAL_GAS_BOILER": "#999999", "HYBRID_HEAT_PUMP": "#56B4E9",
         "ELECTRIC_HEAT_PUMP": "#0072B2", "DISTRICT_HEATING": "#D55E00"}

SCEN_LABEL = {
    "baseline":                                  "Baseline",
    "individual_technologies":                   "Individual technologies only",
    "collective_technologies":                   "Collective technologies only",
    "actor_allignment_strategy":                 "Actor-alignment DH strategy",
    "policy_driven_dh_strategy":                 "Policy-driven DH strategy",
    "policy_driven_sha_strategy":                "Policy-driven SHA strategy",
    "dh_policy_based_connection_obligation":     "DH connection obligation",
    "individual_tech_dh_connection_obligation":  "DH obligation + individual tech",
    "collective_tech_dh_connection_obligation":  "DH obligation + collective tech",
    "grid_congestion_HP_ban":                    "Congestion: HP ban",
    "individual_tech_grid_congestion_ban":       "HP ban + individual tech",
    "collective_tech_grid_congestion_ban":       "HP ban + collective tech",
    "economic_learning_factor_low":              "Economic learning LOW",
    "economic_learning_factor_high":             "Economic learning HIGH",
    "social_learning_factor_low":                "Social learning LOW",
    "social_learning_factor_high":               "Social learning HIGH",
    "price_low":                                 "Energy price LOW",
    "price_high":                                "Energy price HIGH",
    "knockout_intact":                           "Reference (central weights)",
    "knockout_nolearn":                          "Economic learning severed",
    "knockout_nosocial":                         "Social learning severed",
    "knockout_nosalience":                       "Salience amplifier frozen",
    "knockout_nocongest":                        "No grid congestion",
}
ENSEMBLE = list(SCEN_LABEL)[:16]          # the scenario matrix, run across every retained set
PRICE    = ["price_low", "price_high"]
KNOCKOUT = ["knockout_intact", "knockout_nolearn", "knockout_nosocial",
            "knockout_nosalience", "knockout_nocongest"]
SINGLE   = set(PRICE) | set(KNOCKOUT)     # single runs at the central weights, not medians

BLOCKS = [("scenario matrix (ensemble median)", ENSEMBLE),
          ("energy-price bracket", PRICE),
          ("loop knock-outs", KNOCKOUT)]

# per-section cuts. Each results subsection shows only the experiments it argues from.
VIEWS = {
    "all":     (None, 4, "Heating-system mix 2024–2050, share of dwellings, by scenario"),
    "baseline": (["baseline"], 1,
                 "Baseline heating-system mix 2024–2050 (ensemble median)"),
    "loops":   (["baseline",
                 "economic_learning_factor_low", "economic_learning_factor_high",
                 "price_low", "price_high",
                 "social_learning_factor_low", "social_learning_factor_high"], 3,
                "Strength of the two learning loops, against the baseline"),
    # the knock-outs are a different kind of experiment -- single runs compared against their own
    # intact reference, not ensemble medians -- so they get their own row rather than being mixed
    # in among the scenario panels.
    "knockouts": (["knockout_intact", "knockout_nolearn", "knockout_nosocial",
                   "knockout_nosalience", "knockout_nocongest"], 5,
                  "Loop knock-outs against the intact reference (central weights)"),
    "policy":  (["baseline", "dh_policy_based_connection_obligation",
                 "individual_tech_dh_connection_obligation", "collective_tech_dh_connection_obligation",
                 "policy_driven_dh_strategy", "policy_driven_sha_strategy",
                 "grid_congestion_HP_ban", "individual_tech_grid_congestion_ban",
                 "collective_tech_grid_congestion_ban"], 3,
                "Rules, regulation and infrastructure"),
    "coordination": (["baseline", "actor_allignment_strategy", "policy_driven_sha_strategy",
                      "individual_technologies", "collective_technologies"], 3,
                     "Coordination between collective actors, and the technology settings"),
}


def tag(scope):
    k, _, n = scope.partition(":")
    if k.lower() in ("nl", "netherlands"): return "nl"
    return n.strip().lower().replace(" ", "-") if k == "province" else "gemeente_" + n.strip().lower()


def shares(path, want=None):
    """{scenario: {year: {tech: share%}}} from one simulation_results.csv (TOTAL ownership,
    averaged over MC iterations by construction, normalised to 100)."""
    acc = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            if r["ownership"] != "TOTAL": continue
            s = r["scenario_name"]
            if want and s not in want: continue
            te = MERGE.get(r["heating_system"], r["heating_system"])
            acc[s][int(float(r["year"]))][te] += float(r["installed_current"])
    out = {}
    for s, byyear in acc.items():
        out[s] = {}
        for y, d in byyear.items():
            tot = sum(d.values()) or 1.0
            out[s][y] = {te: 100.0 * d.get(te, 0.0) / tot for te in TECHS}
    return out


def extract(base, cache):
    rows = []
    sets = sorted(glob.glob(os.path.join(base, "calib", "*", "simulation_results.csv")))
    if sets:
        print(f"[grid] ensemble: {len(sets)} weight sets")
        pooled = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for i, p in enumerate(sets, 1):
            print(f"  [{i}/{len(sets)}] {os.path.basename(os.path.dirname(p))}", flush=True)
            for s, byyear in shares(p).items():
                for y, d in byyear.items():
                    for te, v in d.items(): pooled[s][y][te].append(v)
        for s, byyear in pooled.items():
            for y, d in byyear.items():
                med = {te: float(np.median(d.get(te, [0.0]))) for te in TECHS}
                tot = sum(med.values()) or 1.0        # medians do not sum to 100; renormalise
                for te in TECHS: rows.append((s, y, te, 100.0 * med[te] / tot))
    for extra in PRICE + KNOCKOUT:
        p = os.path.join(base, extra, "simulation_results.csv")
        if not os.path.exists(p):
            print(f"[grid] missing {extra} — panel skipped"); continue
        print(f"[grid] {extra}", flush=True)
        for y, d in shares(p, want={"baseline"}).get("baseline", {}).items():
            for te in TECHS: rows.append((extra, y, te, d[te]))
    with open(cache, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["panel", "year", "tech", "share_pct"]); w.writerows(rows)
    print(f"[grid] cache -> {cache}  ({len(rows)} rows)")


def load(cache):
    data = defaultdict(lambda: defaultdict(dict))
    for r in csv.DictReader(open(cache)):
        if r["tech"] not in TECHS: continue          # tolerate a cache written before the gas merge
        data[r["panel"]][int(r["year"])][r["tech"]] = float(r["share_pct"])
    return data


def draw(data, keys, nc, title, path, blocked=False, note=None):
    """One figure: `keys` laid out `nc` wide. blocked=True starts each design block on a new row."""
    if blocked:
        grid, row_block, blocks = [], [], [(nm, [k for k in ks if k in data]) for nm, ks in BLOCKS]
        blocks = [(nm, ks) for nm, ks in blocks if ks]
        for bi, (_, ks) in enumerate(blocks):
            for i in range(0, len(ks), nc):
                chunk = ks[i:i + nc]
                grid.append(chunk + [None] * (nc - len(chunk))); row_block.append(bi)
    else:
        keys = [k for k in keys if k in data]
        grid = [keys[i:i + nc] + [None] * (nc - len(keys[i:i + nc])) for i in range(0, len(keys), nc)]
        blocks, row_block = [], []
    if not any(k for row in grid for k in row): return None
    nr = len(grid)
    single = nr == 1 and nc == 1
    fig, axes = plt.subplots(nr, nc, figsize=(3.05 * nc + (2.0 if single else 0),
                                             2.45 * nr + (1.0 if single else 0)),
                             sharex=True, sharey=True, squeeze=False)
    for ax in axes.flat: ax.set_visible(False)
    for r in range(nr):
        for c in range(nc):
            key = grid[r][c]
            if key is None: continue
            ax = axes[r, c]; ax.set_visible(True)
            years = sorted(data[key])
            Y = np.array([[data[key][y].get(te, 0.0) for y in years] for te in TECHS])
            ax.stackplot(years, Y, colors=[COLOR[te] for te in TECHS], linewidth=0)
            ax.set_ylim(0, 100); ax.set_xlim(min(years), max(years))
            lab = SCEN_LABEL.get(key, key)
            if key in SINGLE and not blocked: lab += "*"
            ax.set_title(lab, fontsize=9.5 if single else 8.5, pad=3)
            ax.tick_params(labelsize=8 if single else 7.5)
            ax.set_xticks([y for y in (2024, 2030, 2040, 2050) if min(years) <= y <= max(years)])
            ax.set_yticks([0, 25, 50, 75, 100])
            for sp in ("top", "right"): ax.spines[sp].set_visible(False)
            if c == 0: ax.set_ylabel("% of dwellings", fontsize=9 if single else 8)
            # sharex only labels the last ROW; with ragged grids that strands whole columns
            if r == nr - 1 or grid[r + 1][c] is None: ax.tick_params(labelbottom=True)

    handles = [plt.Rectangle((0, 0), 1, 1, color=COLOR[te]) for te in TECHS]
    fig.legend(handles, [NICE[te] for te in TECHS], ncol=4, loc="lower center",
               frameon=False, fontsize=9.5, bbox_to_anchor=(0.5, -0.004))
    fig.suptitle(title, fontsize=12.5)
    fig.tight_layout(rect=(0.035 if blocked else 0.0, 0.055 if nr < 3 else 0.035, 1,
                           0.93 if nr < 3 else 0.955))
    if blocked:
        for bi, (name, _) in enumerate(blocks):
            rows = [r for r in range(nr) if row_block[r] == bi]
            pos = [axes[r, 0].get_position() for r in rows]
            ymid = (max(p_.y1 for p_ in pos) + min(p_.y0 for p_ in pos)) / 2
            fig.text(0.004, ymid, name, rotation=90, va="center", ha="left", fontsize=9.5, color="#333")
    if note:   # sits below the legend; bbox_inches="tight" pulls it back into the canvas
        fig.text(0.5, -0.035, note, ha="center", fontsize=8, color="#555")
    fig.savefig(path, dpi=150, bbox_inches="tight"); plt.close(fig)
    return path


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="nl")
    ap.add_argument("--no-extract", action="store_true", help="re-plot from the cached CSV")
    ap.add_argument("--view", default="all,baseline,loops,knockouts,policy,coordination",
                    help="comma-separated views to write: " + ", ".join(VIEWS))
    ap.add_argument("--copy-to", default=os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures"),
                    help="also copy the figures here (the LaTeX figures folder); '' to skip")
    a = ap.parse_args()
    base = os.path.join(ROOT, "results", tag(a.scope))
    outdir = os.path.join(base, "figures"); os.makedirs(outdir, exist_ok=True)
    cache = os.path.join(outdir, "scenario_mix_grid.csv")
    if not a.no_extract: extract(base, cache)
    if not os.path.exists(cache): sys.exit(f"[grid] no cache at {cache}")
    data = load(cache)
    if not data: sys.exit("[grid] cache holds no usable rows — re-run without --no-extract")

    note = ("* single run at the central weights; all other panels are the median "
            "over the retained weight ensemble")
    written = []
    for v in [x.strip() for x in a.view.split(",") if x.strip()]:
        if v not in VIEWS: sys.exit(f"[grid] unknown view '{v}'. Choose from {list(VIEWS)}")
        keys, nc, title = VIEWS[v]
        p = os.path.join(outdir, f"fig_scenario_mix_{v}.png")
        n = note if (keys and any(k in SINGLE for k in keys)) else None
        got = draw(data, keys, nc, title, p, blocked=(v == "all"), note=n)
        if got: written.append(got)
        else: print(f"[grid] view '{v}': no panels available, skipped")
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil
        for f in written: shutil.copy2(f, a.copy_to)
        print(f"[grid] copied {len(written)} figure(s) to {a.copy_to}")
    print("wrote:"); [print("  ", f) for f in written]


if __name__ == "__main__":
    main()
