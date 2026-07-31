#!/usr/bin/env python3
"""
Replication analysis for the 2024-2050 SCENARIO pathways (the `converge` mode's companion).

`calibrate_weights.py converge` measures how many Monte-Carlo iterations the 3-year CALIBRATION
objective needs -- but it is scored against observed data, which stops in 2024. The 2024-2050
pathways are path-dependent (learning-curve + salience feedback amplify early noise), so they need
their own replication check, measured on the SPREAD of the end-year outcome across random worlds.

This reads a multi-iteration `simulation_results.csv`, and for each scenario x technology at the
target year computes the between-world standard deviation s of the % share and the textbook required
replications for a target confidence-interval half-width:

    n_required = ceil( (1.96 * s / target)^2 )      (Law & Kelton; same formula converge uses)

The binding technology (largest s) sets how many iterations the band needs.

    python pathway_convergence.py --input results/noord-brabant/calib/best_fit/simulation_results.csv
    python pathway_convergence.py --input <csv> --year 2050 --ownership TOTAL --scenario baseline
"""
import argparse
import csv
import math
from collections import defaultdict


def load(path, year, ownership, scenario):
    # (scenario_name, iteration) -> {heating_system: installed_current}
    cells = defaultdict(lambda: defaultdict(float))
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            if int(float(r["year"])) != year:
                continue
            if r["ownership"] != ownership:
                continue
            if scenario and r.get("scenario_name") != scenario and r.get("scenario") != scenario:
                continue
            key = (r.get("scenario_name", r.get("scenario", "?")), int(float(r["iteration"])))
            cells[key][r["heating_system"]] += float(r["installed_current"])
    return cells


def shares(cells):
    # (scenario) -> {technology: [share_per_iteration, ...]}
    by_scn = defaultdict(lambda: defaultdict(list))
    for (scn, _it), techs in sorted(cells.items()):
        total = sum(techs.values()) or 1.0
        for t, v in techs.items():
            by_scn[scn][t].append(100.0 * v / total)
    return by_scn


def sd(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def n_req(s, target):
    return math.ceil((1.96 * s / target) ** 2) if s > 0 else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", required=True, help="a multi-iteration simulation_results.csv")
    ap.add_argument("--year", type=int, default=2050, help="outcome year to test (default 2050)")
    ap.add_argument("--ownership", default="TOTAL", help="ownership row to test (default TOTAL)")
    ap.add_argument("--scenario", default=None, help="restrict to one scenario (default: all in file)")
    ap.add_argument("--targets", default="0.5,0.25,0.1",
                    help="CI half-widths in %%pts to size n for (default 0.5,0.25,0.1)")
    a = ap.parse_args()

    cells = load(a.input, a.year, a.ownership, a.scenario)
    if not cells:
        raise SystemExit(f"no rows for year={a.year} ownership={a.ownership} "
                         f"scenario={a.scenario} in {a.input}")
    n_iters = len({it for _scn, it in cells})
    targets = [float(x) for x in a.targets.split(",")]
    by_scn = shares(cells)

    print(f"\nPathway replication analysis — {a.input}")
    print(f"year {a.year}, ownership {a.ownership}, {n_iters} iteration(s) in file\n")
    if n_iters < 2:
        raise SystemExit("need >=2 iterations to estimate the spread — rerun with more --iterations.")

    worst = (None, None, 0.0)
    for scn in sorted(by_scn):
        print(f"[{scn}]")
        print(f"  {'technology':<20}{'mean %':>9}{'sd %pts':>9}"
              + "".join(f"{'n(+/-'+str(t)+')':>11}" for t in targets))
        for t in sorted(by_scn[scn], key=lambda k: -sd(by_scn[scn][k])):
            xs = by_scn[scn][t]
            s = sd(xs)
            m = sum(xs) / len(xs)
            print(f"  {t:<20}{m:>9.2f}{s:>9.3f}" + "".join(f"{n_req(s, tg):>11}" for tg in targets))
            if s > worst[2]:
                worst = (scn, t, s)
        print()

    scn, t, s = worst
    print(f"BINDING: '{t}' in scenario '{scn}' has the largest spread (sd = {s:.3f} %pts).")
    print("Iterations the band needs (driven by that technology):")
    for tg in targets:
        print(f"   +/- {tg:>4} %pts  ->  n = {n_req(s, tg)}")
    print("\nPick target precision below the differences you need to resolve between ensemble members\n"
          "or between scenarios. Run each retained weight set at that many --iterations for the band.")


if __name__ == "__main__":
    main()
