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
import argparse, csv, glob, json, os, sys
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
PAPER_FIGS_DIR = os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures")

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
COLOR = {"NATURAL_GAS_BOILER": "#7F7F7F", "HYBRID_HEAT_PUMP": "#56B4E9",
         "ELECTRIC_HEAT_PUMP": "#0072B2", "DISTRICT_HEATING": "#D55E00"}

SCEN_LABEL = {
    "baseline":                                  "Baseline",
    # the two reinforcing loops
    "social_learning_factor_low":                "Social learning LOW",
    "social_learning_factor_high":               "Social learning HIGH",
    "economic_learning_factor_low":              "Economic learning LOW",
    "economic_learning_factor_high":             "Economic learning HIGH",
    "econ_low_social_high":                      "Econ LOW + social HIGH",
    "econ_high_social_low":                      "Econ HIGH + social LOW",
    # heat-network rollout
    "dh_connection_obligation":                  "Obligation, cost-based rollout",
    "dh_policy_based_connection_obligation":     "Obligation, policy-based rollout",
    # grid congestion
    "gas_low_elec_high":                         "Gas cheap, electricity dear",
    "gas_high_elec_low":                         "Gas dear, electricity cheap",
    "transition_fast":                           "All three levers, fast",
    "transition_slow":                           "All three levers, slow",
    "grid_congestion_HP_ban":                    "Congestion: HP ban",
    # coordination between collective actors
    "policy_driven_dh_strategy":                 "Policy-driven DH strategy",
    "policy_driven_sha_strategy":                "Policy-driven SHA strategy",
    "actor_allignment_strategy":                 "Actor-alignment DH strategy",
    # investment subsidies
    "no_subsidy":                                "No investment subsidy",
    "subsidy_phase_out":                         "Subsidy phased out at 20 % share",
    # district heating: connected dwellings may leave (every other scenario: they stay)
    "dh_exit_allowed":                           "Leaving DH allowed (review every 5 yr)",
    # price arms and loop knock-outs: -D flags on the baseline, run into their own folders
    "price_low":                                 "Energy price LOW",
    "price_high":                                "Energy price HIGH",
    "knockout_intact":                           "Reference (central weights)",
    "knockout_nolearn":                          "Economic learning severed",
    "knockout_nosocial":                         "Social learning severed",
    "knockout_nosalience":                       "Salience amplifier frozen",
    "knockout_nocongest":                        "No grid congestion",
}

# Retired from the scenario matrix (Scenario.RETIRED) because each moved several switches at once
# and could not be read as one effect. Labels are kept so an archived cache still plots.
RETIRED_LABEL = {
    "individual_technologies":                   "Individual technologies only",
    "collective_technologies":                   "Collective technologies only",
    "individual_tech_dh_connection_obligation":  "DH obligation + individual tech",
    "collective_tech_dh_connection_obligation":  "DH obligation + collective tech",
    "individual_tech_grid_congestion_ban":       "HP ban + individual tech",
    "collective_tech_grid_congestion_ban":       "HP ban + collective tech",
}
SCEN_LABEL.update(RETIRED_LABEL)

# Composite panels: one scenario taken from a price-arm folder, so a panel can combine a learning
# setting with a price arm. Key format "<folder>:<scenario_name>". The price folders were run with
# --scenario all at the SAME central weight vector as knockout_intact, so these are directly
# comparable with it (and not with the ensemble medians).
COMPOSITE = {
    # the user's crossed extremes: the two learning loops pulling against each other
    "price_low:econ_low_social_high":   "Econ learning LOW + social HIGH + price LOW",
    "price_high:econ_high_social_low":  "Econ learning HIGH + social LOW + price HIGH",
}
SCEN_LABEL.update(COMPOSITE)

# The scenario matrix, run across every retained weight set. Mirrors Scenario.NAMES; spelled out
# rather than sliced off SCEN_LABEL, which silently mis-selected whenever a label was added.
ENSEMBLE = ["baseline",
            "social_learning_factor_low", "social_learning_factor_high",
            "economic_learning_factor_low", "economic_learning_factor_high",
            "econ_low_social_high", "econ_high_social_low",
            "gas_low_elec_high", "gas_high_elec_low",
            "transition_fast", "transition_slow",
            "dh_connection_obligation", "dh_policy_based_connection_obligation",
            "grid_congestion_HP_ban",
            "policy_driven_dh_strategy", "policy_driven_sha_strategy",
            "actor_allignment_strategy",
            "no_subsidy", "subsidy_phase_out",
            "dh_exit_allowed"]
PRICE    = ["price_low", "price_high"]
KNOCKOUT = ["knockout_intact", "knockout_nolearn", "knockout_nosocial",
            "knockout_nosalience", "knockout_nocongest"]
# single runs at the central weight vector, not ensemble medians. The composite panels come from
# the price-arm folders, which were run at those same weights.
SINGLE   = set(PRICE) | set(KNOCKOUT) | set(COMPOSITE)

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
    # One figure per loop, so a results paragraph can show only the experiment it argues from.
    # These three are ensemble medians over the retained weight sets, like the baseline.
    "loop_econ":   (["baseline", "economic_learning_factor_low", "economic_learning_factor_high"], 3,
                    "Economic learning loop: baseline against a halved and a doubled learning rate"),
    "loop_social": (["baseline", "social_learning_factor_low", "social_learning_factor_high"], 3,
                    "Social learning loop: baseline against a halved and a doubled social weight"),
    # The price arms are single runs at the central weight vector, so the reference panel is the
    # matching single run (knockout_intact), NOT the ensemble median baseline.
    "loop_price":  (["knockout_intact", "price_low", "price_high"], 3,
                    "Energy-price arms, against the reference at the same weights"),
    # Both loops and the price arm moved together. Crossed: the loops pull against each other.
    "extremes":    (["baseline", "transition_slow", "transition_fast"], 3,
                    "All three levers moved together: the slowest and fastest transition"),
    # Aligned extremes, available from the existing runs.
    # --- the three policy comparisons, one per results paragraph -----------------------------
    # 1. Grid rollout. Under a connection obligation the mix is set by WHERE the network reaches,
    #    not by how households learn: the four crossed learning combinations span 0.4 pp of DH
    #    share, against ~20 pp between the two expansion rules.
    "rollout": (["baseline", "dh_connection_obligation",
                 "dh_policy_based_connection_obligation"], 3,
                "Heat-network rollout: a connection obligation under each expansion rule"),
    # 2. Grid congestion. The ban substitutes hybrids for electric heat pumps; it does not slow the
    #    exit from gas. The individual/collective variants are omitted deliberately -- their headline
    #    difference is the social-learning setting, which the loop figures already isolate.
    "congestion": (["baseline", "grid_congestion_HP_ban"], 2,
                   "Grid congestion: banning electric heat pumps in congested neighbourhoods"),
    # 3. Coordination between collective actors. Kept as four panels, not three: the social-housing
    #    strategy is what moves district heating, while the DH-expansion strategy moves it the other
    #    way, so the combined panel alone would be read as the sum of two positives.
    "coordination_policy": (["baseline", "policy_driven_dh_strategy", "policy_driven_sha_strategy",
                             "actor_allignment_strategy"], 4,
                            "Coordination between collective actors: heat-network planning and "
                            "social-housing strategy"),
    # 4. Investment subsidies: none at all, and a phase-out once a technology reaches 20 % of the stock.
    "subsidy": (["baseline", "no_subsidy", "subsidy_phase_out"], 3,
                "Investment subsidies: current, none, and phased out at a 20 % stock share"),
    # 5. District heating: every scenario keeps connected dwellings connected; this one lets them
    #    review every 5 years and leave on cost, to show what that assumption is worth.
    "dh_exit": (["baseline", "dh_exit_allowed"], 2,
                "District heating: connected dwellings stay (reference) or may leave"),
    # superseded by rollout / congestion / coordination_policy above, which take one lever at a
    # time; kept only so an archived cache can still be re-plotted.
    "policy_legacy": (["baseline", "dh_policy_based_connection_obligation",
                       "individual_tech_dh_connection_obligation",
                       "collective_tech_dh_connection_obligation",
                       "policy_driven_dh_strategy", "policy_driven_sha_strategy",
                       "grid_congestion_HP_ban", "individual_tech_grid_congestion_ban",
                       "collective_tech_grid_congestion_ban"], 3,
                      "Rules, regulation and infrastructure (legacy scenario set)"),
    "coordination_legacy": (["baseline", "actor_allignment_strategy", "policy_driven_sha_strategy",
                             "individual_technologies", "collective_technologies"], 3,
                            "Coordination and technology settings (legacy scenario set)"),
}

# Explicit-layout views. Each entry is a list of ROWS; None leaves that cell empty. Used where the
# reading order matters more than packing: the loop grid puts the reference once at the top left and
# lines the LOW arms up in one column and the HIGH arms in the next, so a lever is read across and
# an arm is read down. The third element is the in-figure footnote; "" means none, because the
# LaTeX caption carries the explanation instead.
GRID_VIEWS = {
    "loops_grid": (
        [["baseline",
          "social_learning_factor_low",    "social_learning_factor_high"],
         [None,
          "economic_learning_factor_low",  "economic_learning_factor_high"],
         [None,
          "gas_low_elec_high",             "gas_high_elec_low"]],
        "Heating-system mix 2024\u20132050 under each lever, against the baseline pathway",
        "",
    ),
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


def expected_sets(base, search):
    """Run folders are named set<i>_mad<mad> from the retained list (model/run.py). Rebuilding the
    names from the search file selects exactly this ensemble's runs -- results/<scope>/calib/ also
    holds the folders of earlier calibrations, and a bare glob silently mixes the ensembles."""
    d = json.load(open(search))
    ret = d.get("retained") or []
    if not ret: sys.exit(f"[grid] no retained sets in {search}")
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
            print(f"[grid] no full-ensemble folders; using the representative runs {', '.join(have)} "
                  f"-- medians over {len(have)} sets only")
            return [os.path.join(base, "calib", w, "simulation_results.csv") for w in have]
    if len(have) < len(want):
        print(f"[grid] {len(have)} of {len(want)} sets present -- the pathway batch is still "
              f"running or was interrupted; the medians are provisional")
    return [os.path.join(base, "calib", w, "simulation_results.csv") for w in have]


def extract(base, cache, search=None):
    rows = []
    sets = expected_sets(base, search) if search else \
        sorted(glob.glob(os.path.join(base, "calib", "*", "simulation_results.csv")))
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
    # composite panels: <folder>:<scenario_name>
    by_folder = defaultdict(list)
    for key in COMPOSITE:
        folder, _, scen = key.partition(":")
        by_folder[folder].append(scen)
    for folder, scens in by_folder.items():
        p = os.path.join(base, folder, "simulation_results.csv")
        if not os.path.exists(p):
            print(f"[grid] missing {folder} — composite panels skipped"); continue
        got = shares(p, want=set(scens))
        for scen in scens:
            if scen not in got:
                print(f"[grid] {folder} has no scenario '{scen}' — panel skipped "
                      f"(add it to Scenario.java and re-run the batch)")
                continue
            print(f"[grid] {folder}:{scen}", flush=True)
            for y, d in got[scen].items():
                for te in TECHS: rows.append((f"{folder}:{scen}", y, te, d[te]))
    with open(cache, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["panel", "year", "tech", "share_pct"]); w.writerows(rows)
    print(f"[grid] cache -> {cache}  ({len(rows)} rows)")


def load(cache):
    data = defaultdict(lambda: defaultdict(dict))
    for r in csv.DictReader(open(cache)):
        if r["tech"] not in TECHS: continue          # tolerate a cache written before the gas merge
        data[r["panel"]][int(r["year"])][r["tech"]] = float(r["share_pct"])
    return data


def draw(data, keys, nc, title, path, blocked=False, note=None, mark_single=True, grid=None):
    """One figure: `keys` laid out `nc` wide, or an explicit `grid` of rows (None = empty cell).
    blocked=True starts each design block on a new row."""
    if grid is not None:
        grid = [[(k if k in data else None) for k in row] for row in grid]
        nc = max(len(row) for row in grid)
        grid = [row + [None] * (nc - len(row)) for row in grid]
        blocks, row_block = [], []
    elif blocked:
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
            if key in SINGLE and not blocked and mark_single: lab += "*"
            ax.set_title(lab, fontsize=9.5 if single else 8.5, pad=3)
            ax.tick_params(labelsize=8 if single else 7.5)
            ax.set_xticks([y for y in (2024, 2030, 2040, 2050) if min(years) <= y <= max(years)])
            ax.set_yticks([0, 25, 50, 75, 100])
            for sp in ("top", "right"): ax.spines[sp].set_visible(False)
            # sharey only labels column 0; in a ragged grid that column can be empty, stranding the
            # whole row without a scale. Label the leftmost VISIBLE cell of each row instead.
            first_vis = next((i for i, k in enumerate(grid[r]) if k is not None), 0)
            if c == first_vis:
                ax.set_ylabel("% of dwellings", fontsize=9 if single else 8)
                ax.tick_params(labelleft=True)
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
    ap.add_argument("--search", default=os.path.join(ROOT, "results", "calib1000", "calibration_search.json"),
                    help="calibration_search.json whose retained sets define the ensemble; only its\n"
                         "run folders are read. Pass '' to read every folder under calib/ instead.")
    ap.add_argument("--view",
                    default="loops_grid,rollout,congestion,coordination_policy,extremes,subsidy,dh_exit",
                    # exactly the views main.tex includes, plus `extremes` for the combined-scenario
                    # TODO in the loops section. The other views still exist and can be asked for by
                    # name; they are off by default so the LaTeX figures folder does not fill up with
                    # panels the manuscript never shows, which then go stale without anyone noticing,
                    help="comma-separated views to write: " + ", ".join(list(VIEWS) + list(GRID_VIEWS)))
    ap.add_argument("--copy-to", default=None,
                    help="also copy the figures here (the LaTeX figures folder); '' to skip")
    a = ap.parse_args()
    if a.copy_to is None:       # manuscript figures are national: never overwrite them from a province
        a.copy_to = PAPER_FIGS_DIR if a.scope.strip().lower() in ("nl", "netherlands") else ""
    base = os.path.join(ROOT, "results", tag(a.scope))
    outdir = os.path.join(base, "figures"); os.makedirs(outdir, exist_ok=True)
    cache = os.path.join(outdir, "scenario_mix_grid.csv")
    if not a.no_extract: extract(base, cache, a.search or None)
    if not os.path.exists(cache): sys.exit(f"[grid] no cache at {cache}")
    data = load(cache)
    if not data: sys.exit("[grid] cache holds no usable rows — re-run without --no-extract")

    MIXED_NOTE = ("* single run at the central weights; all other panels are the median "
                  "over the retained weight ensemble")
    ALL_SINGLE_NOTE = ("all panels are single runs at the same central weight vector, "
                       "not medians over the weight ensemble")
    written = []
    for v in [x.strip() for x in a.view.split(",") if x.strip()]:
        if v in GRID_VIEWS:
            g, title, grid_note = GRID_VIEWS[v]
            keys = [k for row in g for k in row if k]
            nc = max(len(row) for row in g)
        elif v in VIEWS:
            g, grid_note = None, None
            keys, nc, title = VIEWS[v]
        else:
            sys.exit(f"[grid] unknown view '{v}'. Choose from {list(VIEWS) + list(GRID_VIEWS)}")
        p = os.path.join(outdir, f"fig_scenario_mix_{v}.png")
        # Only warn about mixing when the view actually mixes: a view whose panels are ALL single
        # runs is internally consistent and the asterisk would misdescribe it.
        present = [k for k in (keys or []) if k in data]
        if g is not None:
            # an explicit-grid view sets its own footnote; "" means deliberately none, so that the
            # LaTeX caption can carry the explanation instead
            n = grid_note or None
        elif present and all(k in SINGLE for k in present):
            n = ALL_SINGLE_NOTE
        elif present and any(k in SINGLE for k in present):
            n = MIXED_NOTE
        else:
            n = None
        got = draw(data, keys, nc, title, p, blocked=(v == "all"), note=n,
                   mark_single=(n is not ALL_SINGLE_NOTE), grid=g)
        if got: written.append(got)
        else: print(f"[grid] view '{v}': no panels available, skipped")
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil
        for f in written: shutil.copy2(f, a.copy_to)
        print(f"[grid] copied {len(written)} figure(s) to {a.copy_to}")
    print("wrote:"); [print("  ", f) for f in written]


if __name__ == "__main__":
    main()
