#!/usr/bin/env python3
"""Context figure for Section 2: the Dutch residential heating mix observed in 2023, next to 2050
scenarios of the Dutch grid operators, in the style of the results figures (horizontal stacked bars,
same technology colours as segment_analysis.py).

Default figure (fig1_heating_scenarios.png): the observed mix of 2023 and three Netbeheer Nederland
scenarios of 2025 that bound the choice of heating technology, labelled by that choice:
    Middle of the road                       = Koersvaste Middenweg
    Max. district heating and all-electric   = Eigen Vermogen
    Max. hybrid                              = Horizon Aanvoer
Options: --all  all eight scenarios of two studies (NBNL 2025, II3050 2023), unnamed and sorted;
         --range  the range across those eight per heating method.

Observed 2023: CBS, Woningen; hoofdverwarmingsinstallaties (table_heat_methods.xlsx), the Netherlands:
  natural gas   = individuele cv (79.3) + blokverwarming (5.5)
  hybrid        = elektrisch verwarmd met hoog gasverbruik (2.0)
  electric      = elektrisch verwarmd met laag (0.8) + zonder (4.1) gasverbruik
  district heat = stadsverwarming met hoog (0.0) + laag (0.7) + zonder (6.0) gasverbruik
  other         = type installatie onbekend (1.7)
Scenarios, 2050: the households_heater_*_share inputs of the public saved scenarios in the Energy
Transition Model (ETM): NBNL scenarios 2025 (ETM 2025.01) and II3050 v2 (current ETM). ETM counts the
units of each household heater as exactly this share times the number of residences (checked with the
*_number_of_units gqueries), so the shares are shares of homes, comparable with the CBS data. Fetched
from the ETM API (no key needed for public scenarios), cached in
results/etm_household_heating_scenarios.csv; --refresh fetches again.

    python results_analysis/fig_context_scenarios.py [--refresh] [--all] [--range]

Writes to results/figures_context/ and copies to the manuscript figures folder.
"""
import argparse
import csv
import json
import os
import shutil
import urllib.request

import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import paper_style as ps

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
OUTDIR = os.path.join(ROOT, "results", "figures_context")
PAPER = os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures")
CACHE = os.path.join(ROOT, "results", "etm_household_heating_scenarios.csv")

ETM25 = "https://2025-01.engine.energytransitionmodel.com"
ETMNOW = "https://engine.energytransitionmodel.com"
# (study, scenario, saved scenario id, engine host, engine scenario id), all end year 2050
SCENARIOS = [
    ("NBNL 2025", "Koersvaste Middenweg", 19919, ETM25, 506),
    ("NBNL 2025", "Eigen Vermogen", 19924, ETM25, 515),
    ("NBNL 2025", "Gezamenlijke Balans", 19928, ETM25, 523),
    ("NBNL 2025", "Horizon Aanvoer", 19932, ETM25, 531),
    ("II3050 2023", "Decentral Initiatives", 14552, ETMNOW, 1018149),
    ("II3050 2023", "National Leadership", 14553, ETMNOW, 1023366),
    ("II3050 2023", "European Integration", 14554, ETMNOW, 1002230),
    ("II3050 2023", "International Trade", 14555, ETMNOW, 1002232),
]
# default figure: three NBNL scenarios, labelled by the heating choice they represent
SELECTED = [("Koersvaste Middenweg", "Middle of the road"),
            ("Eigen Vermogen", "Max. district heating\nand all-electric"),
            ("Horizon Aanvoer", "Max. hybrid")]
STUDY_SHORT = {"NBNL 2025": "Netbeheer Nederland (2025)", "II3050 2023": "II3050 (2023)"}
STUDY_MARK = {"NBNL 2025": "o", "II3050 2023": "^"}
STUDY_LABEL = {"NBNL 2025": "Netbeheer Nederland scenarios (2025)", "II3050 2023": "II3050 scenarios (2023)"}

# ETM household heater -> detailed category
GROUP = {
    "network_gas": "gas", "combined_network_gas": "gas",
    "hybrid_heatpump_air_water_electricity": "hybrid",
    "hybrid_hydrogen_heatpump_air_water_electricity": "hybrid",
    "hybrid_crude_oil_heatpump_air_water_electricity": "hybrid",
    "heatpump_air_water_electricity": "electric", "heatpump_pvt_electricity": "electric",
    "heatpump_surface_water_water_ts_electricity": "electric",
    "heatpump_ground_water_electricity": "ground",
    "district_heating_ht_steam_hot_water": "dh", "district_heating_mt_steam_hot_water": "dh",
    "district_heating_lt_steam_hot_water": "dh",
    "electricity": "resistance",
    "combined_hydrogen": "other", "crude_oil": "other", "wood_pellets": "other", "coal": "other",
}
DETAIL = ["gas", "hybrid", "electric", "ground", "dh", "resistance", "other"]
PARTS = {"gas": ("Natural gas", "#999999", None),
         "hybrid": ("Hybrid heat pump", "#56B4E9", None),
         "electric": ("Electric heat pump", "#0072B2", None),
         "ground": ("of which ground-source", "#0072B2", "///"),
         "dh": ("District heating", "#D55E00", None),
         "resistance": ("Electric resistance", "#B3B3E6", None),
         "other": ("Other or unknown", "#DDDDDD", None)}
DARK = ("#0072B2", "#D55E00", "#999999")
OBSERVED = {"gas": 79.3 + 5.5, "hybrid": 2.0, "electric": 0.8 + 4.1, "ground": 0.0,
            "dh": 0.0 + 0.7 + 6.0, "resistance": 0.0, "other": 1.7}
# coarse categories for the --all / --range overviews
COARSE = {"gas": ["gas"], "hybrid": ["hybrid"], "electric": ["electric", "ground"], "dh": ["dh"],
          "other": ["resistance", "other"]}
CNICE = {"gas": "Natural gas", "hybrid": "Hybrid heat pump", "electric": "Electric heat pump",
         "dh": "District heating", "other": "Other"}
CCOLOR = {"gas": "#999999", "hybrid": "#56B4E9", "electric": "#0072B2", "dh": "#D55E00", "other": "#9A9AC8"}


def fetch():
    rows = []
    for study, name, saved, host, sid in SCENARIOS:
        with urllib.request.urlopen(f"{host}/api/v3/scenarios/{sid}/inputs", timeout=180) as r:
            inp = json.load(r)
        shares = {c: 0.0 for c in DETAIL}
        unknown = []
        for k, v in inp.items():
            if k.startswith("households_heater_") and k.endswith("_share"):
                tech = k[len("households_heater_"):-len("_share")]
                val = v.get("user") if v.get("user") is not None else v.get("default")
                val = float(val or 0.0)
                if tech in GROUP:
                    shares[GROUP[tech]] += val
                elif val > 0:
                    unknown.append((tech, val))
        if unknown:
            raise SystemExit(f"{name}: unmapped household heaters {unknown} -- add them to GROUP")
        rows.append({"study": study, "scenario": name, "saved_scenario": saved, "engine_scenario": sid,
                     **{c: round(shares[c], 4) for c in DETAIL}})
        print(f"{study:12s} {name:24s} " + "  ".join(f"{c} {shares[c]:5.1f}" for c in DETAIL))
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    with open(CACHE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    print("cached", os.path.relpath(CACHE, ROOT))


def save(fig, name):
    os.makedirs(OUTDIR, exist_ok=True)
    out = os.path.join(OUTDIR, name)
    ps.save(fig, out)
    print("wrote", os.path.relpath(out, ROOT))
    try:                       # copy to the manuscript (fails while the file is open in a viewer)
        shutil.copy2(out, os.path.join(PAPER, name)); print("copied to", os.path.relpath(PAPER, ROOT))
    except OSError as e:
        print("could not copy to the manuscript folder (file open?):", e)


def stacked(rows, cats, parts, name, title, height):
    """rows: [(label, {cat: share})]; first row is the observation, set apart by a gap."""
    y = np.array([0] + [i + 1.4 for i in range(len(rows) - 1)])
    ps.apply()
    fig, ax = ps.subplots(1, 1, frac=1.0, height=height); left = np.zeros(len(rows))
    for c in cats:
        lab, col, hatch = parts[c]
        v = np.array([r[1][c] for r in rows])
        if not v.any():
            continue
        ax.barh(y, v, left=left, color=col, height=0.7, hatch=hatch, edgecolor="white", linewidth=0.6,
                label=lab)
        for yi, x0, w in zip(y, left, v):
            if w >= 8 and not hatch:
                ax.text(x0 + w / 2, yi, f"{w:.0f}", ha="center", va="center",
                        color="white" if col in DARK else "#1a1a1a")
        left += v
    ax.set_yticks(y); ax.set_yticklabels([r[0] for r in rows]); ax.invert_yaxis()
    ax.set_xlim(0, 100); ax.set_xlabel("% of homes")
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    ps.title(fig, title)
    h, l = ax.get_legend_handles_labels()
    ps.legend_below(fig, h, l, ncol=min(4, len(l)))
    save(fig, name)


def coarse(s):
    return {c: sum(s[d] for d in ds) for c, ds in COARSE.items()}


def range_figure(scen):
    ps.apply()
    fig, ax = ps.subplots(1, 1, frac=1.0, height=2.9); rng = np.random.default_rng(3)
    obs = coarse(OBSERVED)
    for i, c in enumerate(COARSE):
        vals = np.array([coarse(s)[c] for s in scen])
        ax.barh(i, vals.max() - vals.min(), left=vals.min(), height=0.5, color=CCOLOR[c], alpha=0.25, linewidth=0)
        for s in scen:
            ax.scatter(coarse(s)[c], i + rng.uniform(-0.12, 0.12), marker=STUDY_MARK[s["study"]], s=30,
                       color=CCOLOR[c], edgecolor="white", linewidth=0.5, zorder=3)
        ax.plot([obs[c]] * 2, [i - 0.33, i + 0.33], color="#1a1a1a", lw=2.2, zorder=4)
    ax.set_yticks(range(len(COARSE))); ax.set_yticklabels([CNICE[c] for c in COARSE])
    ax.invert_yaxis(); ax.set_xlim(0, 100); ax.set_xlabel("% of homes")
    ax.grid(axis="x", alpha=0.25); ax.set_axisbelow(True)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    ps.title(fig, "Share of homes per heating method: observed in 2023 and scenarios for 2050")
    h = [plt.Line2D([], [], color="#1a1a1a", lw=2.2, label="Observed, 2023")]
    h += [plt.Line2D([], [], marker=m, ls="", color="#555555", markersize=6, label=STUDY_LABEL[st])
          for st, m in STUDY_MARK.items()]
    h += [plt.Rectangle((0, 0), 1, 1, color="#555555", alpha=0.25, label="Range across scenarios")]
    ps.legend_below(fig, h, ncol=2)
    save(fig, "fig1_heating_scenarios_range.png")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--refresh", action="store_true", help="fetch the scenarios from the ETM API again")
    ap.add_argument("--all", action="store_true", help="also write all eight scenarios, unnamed and sorted")
    ap.add_argument("--range", action="store_true", help="also write the range-per-method version")
    a = ap.parse_args()
    if a.refresh or not os.path.exists(CACHE) or "ground" not in open(CACHE, encoding="utf-8").readline():
        fetch()
    scen = [{**r, **{c: float(r[c]) for c in DETAIL}} for r in csv.DictReader(open(CACHE, encoding="utf-8"))]
    for s in scen:
        assert abs(sum(s[c] for c in DETAIL) - 100) < 0.6, f"{s['scenario']} does not sum to 100"
    by_name = {s["scenario"]: s for s in scen}

    rows = [("Observed, 2023", OBSERVED)] + [(f"{lab}, 2050", by_name[n]) for n, lab in SELECTED]
    stacked(rows, DETAIL, PARTS, "fig1_heating_scenarios.png",
            "Share of homes per heating method: observed and grid operators' scenarios", 2.6)
    if a.all:
        srt = sorted(scen, key=lambda s: (s["hybrid"], -(s["electric"] + s["ground"])))
        cparts = {c: (CNICE[c], CCOLOR[c], None) for c in COARSE}
        rows = [("Observed, 2023", coarse(OBSERVED))] + [(STUDY_SHORT[s["study"]], coarse(s)) for s in srt]
        stacked(rows, list(COARSE), cparts, "fig1_heating_scenarios_all.png",
                "Share of homes per heating method: observed in 2023 and 2050 scenarios", 3.6)
    if a.range:
        range_figure(scen)


if __name__ == "__main__":
    main()
