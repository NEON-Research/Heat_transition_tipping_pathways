#!/usr/bin/env python3
"""Collapse the 21 national ensemble runs into one tidy CSV for the band figures.

Reads  results/nl/calib/set*/simulation_results.csv   (~5 MB each)
       results/calib/calibration_search.json
Writes results/calib/band_baseline_tidy.csv           set, year, tech, share, mad, 4 weight shares

Baseline scenario and ownership=TOTAL only; shares are computed after summing
installed_current over Monte-Carlo iterations, i.e. the mean share per year.
"""
import glob, json, os, re, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.normpath(os.path.join(HERE, ".."))
TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
COLS = ["scenario_name", "iteration", "year", "heating_system", "ownership", "installed_current"]
SHARES = ["shareAttitude", "shareSocialnorm", "shareAffordability", "shareIntention"]

def main(scope_tag="nl"):
    cal = json.load(open(os.path.join(ROOT, "results", "calib", "calibration_search.json")))
    ret = cal["retained"]
    sets = sorted(glob.glob(os.path.join(ROOT, "results", scope_tag, "calib", "set*", "simulation_results.csv")))
    if not sets:
        sys.exit(f"no set*/simulation_results.csv under results/{scope_tag}/calib")
    rows = []
    for p in sets:
        i = int(re.search(r"set(\d+)_", os.path.basename(os.path.dirname(p))).group(1))
        df = pd.read_csv(p, usecols=COLS)
        df = df[(df.scenario_name == "baseline") & (df.ownership == "TOTAL")]
        g = df.groupby(["year", "heating_system"])["installed_current"].sum().unstack(fill_value=0.0)
        share = 100 * g.div(g.sum(axis=1), axis=0)
        s = ret[i]["sampled"]
        for y, r in share.iterrows():
            for t in TECHS:
                rows.append({"set": i, "year": int(y), "tech": t, "share": float(r.get(t, 0.0)),
                             "mad": ret[i]["mad"], **{k: s[k] for k in SHARES}})
    out = os.path.join(ROOT, "results", "calib", "band_baseline_tidy.csv")
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"wrote {out} ({len(rows)} rows from {len(sets)} sets)")

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "nl")
