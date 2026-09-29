#!/usr/bin/env python3
"""Compare two sets of scenario results for the same region, e.g. before and after a model change.

Reads the folder layout that pathway_batch.py writes under results/<scope>/:
    calib/<set>/simulation_results.csv      the preference band (one folder per weight set)
    price_low|price_high/simulation_results.csv
    knockout_<name>/simulation_results.csv
and reports, per run group, scenario and technology, the share of dwellings (ownership TOTAL,
mean over iterations, median over weight sets) in the chosen years, old next to new, with the
difference. Differences of at least --flag percentage points are marked, so the sections of the
paper whose numbers moved stand out. Scenarios present in only one of the two are listed.

    python results_analysis/compare_runs.py results/noord-brabant_before_2026-09-29 results/noord-brabant
    python results_analysis/compare_runs.py OLD NEW --years 2030,2050 --flag 3 --by-ownership

Writes <NEW>/compare_to_<OLD name>.csv (tidy: group, scenario, ownership, year, technology, old,
new, diff).
"""
import argparse
import csv
import glob
import os
from collections import defaultdict

import numpy as np

TECHS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP",
         "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
SHORT = {"NATURAL_GAS_BOILER": "gas", "NATURAL_GAS_BLOCK": "gasblk", "HYBRID_HEAT_PUMP": "hybrid",
         "ELECTRIC_HEAT_PUMP": "ehp", "DISTRICT_HEATING": "dh"}


def shares(path, years, owners):
    """{(scenario, ownership, year): {tech: % share}} averaged over iterations."""
    acc = defaultdict(lambda: defaultdict(float))
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            y = int(r["year"])
            if y not in years or r["ownership"] not in owners:
                continue
            acc[(r["scenario_name"], r["ownership"], y, r["iteration"])][r["heating_system"]] += \
                float(r["installed_current"])
    per = defaultdict(list)
    for (sc, own, y, _it), d in acc.items():
        tot = sum(d.values()) or 1.0
        per[(sc, own, y)].append({t: 100.0 * d.get(t, 0.0) / tot for t in TECHS})
    return {k: {t: float(np.mean([v[t] for v in vs])) for t in TECHS} for k, vs in per.items()}


def load(root, years, owners):
    """{group: {(scenario, ownership, year): {tech: share}}}; the band is the median over sets."""
    out = {}
    sets = sorted(glob.glob(os.path.join(root, "calib", "*", "simulation_results.csv")))
    if sets:
        per_set = [shares(p, years, owners) for p in sets]
        keys = set().union(*per_set)
        out[f"ensemble (median of {len(sets)} sets)"] = {
            k: {t: float(np.median([s[k][t] for s in per_set if k in s])) for t in TECHS} for k in keys}
    for p in sorted(glob.glob(os.path.join(root, "*", "simulation_results.csv"))):
        group = os.path.basename(os.path.dirname(p))
        if group.startswith(("price_", "knockout_")):
            out[group] = shares(p, years, owners)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("old", help="results folder of the earlier run, e.g. results/noord-brabant_before_...")
    ap.add_argument("new", help="results folder of the new run, e.g. results/noord-brabant")
    ap.add_argument("--years", default="2030,2050")
    ap.add_argument("--flag", type=float, default=2.0, help="mark differences of at least this many pp")
    ap.add_argument("--by-ownership", action="store_true", help="also compare per ownership group")
    a = ap.parse_args()
    years = [int(y) for y in a.years.split(",")]
    owners = {"TOTAL"} | ({"PRIVATELY_OWNED", "PRIVATELY_RENTED", "SOCIAL_HOUSING",
                           "HOME_OWNER_ASSOCIATION"} if a.by_ownership else set())
    O, N = load(a.old, years, owners), load(a.new, years, owners)
    # the ensemble label carries the set count, which differs: match groups by their stem
    stem = lambda g: g.split(" (")[0]
    Os, Ns = {stem(g): (g, d) for g, d in O.items()}, {stem(g): (g, d) for g, d in N.items()}

    rows = []
    print(f"old: {a.old}\nnew: {a.new}\nshares of dwellings (%), ownership TOTAL unless stated; "
          f"'!' marks |diff| >= {a.flag} pp")
    for g in sorted(set(Os) | set(Ns)):
        if g not in Os or g not in Ns:
            print(f"\n[{g}] only in {'old' if g in Os else 'new'}")
            continue
        (go, do), (gn, dn) = Os[g], Ns[g]
        scen_o = {k[0] for k in do}; scen_n = {k[0] for k in dn}
        print(f"\n=== {g}   (old: {go}; new: {gn})")
        for sc in sorted(scen_o & scen_n):
            for own in sorted(owners):
                for y in years:
                    k = (sc, own, y)
                    if k not in do or k not in dn:
                        continue
                    cells = []
                    for t in TECHS:
                        d = dn[k][t] - do[k][t]
                        rows.append({"group": g, "scenario": sc, "ownership": own, "year": y,
                                     "technology": t, "old": round(do[k][t], 2),
                                     "new": round(dn[k][t], 2), "diff": round(d, 2)})
                        cells.append(f"{SHORT[t]} {do[k][t]:5.1f}>{dn[k][t]:5.1f}"
                                     f"{'!' if abs(d) >= a.flag else ' '}")
                    lab = sc if own == "TOTAL" else f"{sc} [{own.lower()}]"
                    print(f"  {lab[:44]:44s} {y}  " + "  ".join(cells))
        for sc in sorted(scen_o - scen_n): print(f"  (only in old) {sc}")
        for sc in sorted(scen_n - scen_o): print(f"  (only in new) {sc}")

    if rows:
        out = os.path.join(a.new, f"compare_to_{os.path.basename(os.path.normpath(a.old))}.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
        big = sorted((r for r in rows if abs(r["diff"]) >= a.flag), key=lambda r: -abs(r["diff"]))
        print(f"\n{len(big)} of {len(rows)} compared values moved by >= {a.flag} pp. Largest:")
        for r in big[:12]:
            print(f"  {r['diff']:+6.1f}  {r['group'][:14]:14s} {r['scenario'][:34]:34s} {r['ownership'][:10]:10s} "
                  f"{r['year']} {SHORT[r['technology']]}")
        print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
