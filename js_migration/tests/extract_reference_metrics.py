"""
extract_reference_metrics.py
----------------------------
Reduce a full `simulation_results_*.csv` (the AnyLogic output) into a compact,
version-controllable set of *golden* summary metrics that the new engine must
reproduce (statistically) for the tests to pass.

The point is NOT to compare millions of raw rows bit-for-bit (impossible across
RNG implementations). It is to capture the model's *emergent behaviour*:

  * final-year installed shares per (scenario, heating_system, ownership)
  * a mid-horizon checkpoint (to catch trajectory/timing drift)
  * the tipping year per (scenario, heating_system): first year a technology
    crosses 25% / 50% of TOTAL stock

For each metric we store mean + std across iterations, so the golden-master
comparator can test the new engine's mean against `mean +/- k*std (+ abs floor)`.

Usage:
    python extract_reference_metrics.py <results.csv> [-o golden/baseline_metrics.json]
"""
import argparse, json, sys
import pandas as pd


TIP_THRESHOLDS = [0.25, 0.50]


def tipping_year(sub: pd.DataFrame, hs: str, threshold: float):
    """First year `hs` share of TOTAL stock >= threshold, averaged over iterations.
    Returns None if never crossed in any iteration."""
    years = []
    for _, g in sub.groupby("iteration"):
        tot = g.groupby("year")["installed_current"].sum()
        hsy = g[g.heating_system == hs].groupby("year")["installed_current"].sum()
        share = (hsy / tot).dropna()
        crossed = share[share >= threshold]
        if len(crossed):
            years.append(int(crossed.index.min()))
    if not years:
        return None
    s = pd.Series(years)
    return {"mean": round(float(s.mean()), 2), "std": round(float(s.std(ddof=0)), 2), "n": len(years)}


def summarise(df: pd.DataFrame) -> dict:
    fy = int(df.year.max())
    y0 = int(df.year.min())
    mid = y0 + (fy - y0) // 2
    heating_systems = sorted(df.heating_system.unique())
    scenarios = sorted(df.scenario_name.unique())
    ownerships = sorted(df.ownership.unique())

    out = {
        "meta": {
            "start_year": y0, "final_year": fy, "mid_year": mid,
            "heating_systems": heating_systems,
            "scenarios": scenarios,
            "ownerships": ownerships,
            "iterations": sorted(int(i) for i in df.iteration.unique()),
        },
        "final_year_installed": {},   # scenario -> ownership -> hs -> {mean,std,n}
        "mid_year_installed": {},
        "tipping_year": {},           # scenario -> hs -> threshold -> {mean,std,n}
    }

    def fill(target, year):
        d = df[df.year == year]
        for scn, gs in d.groupby("scenario_name"):
            target.setdefault(scn, {})
            for own, go in gs.groupby("ownership"):
                target[scn].setdefault(own, {})
                for hs, gh in go.groupby("heating_system"):
                    v = gh.installed_current.astype(float)
                    target[scn][own][hs] = {
                        "mean": round(float(v.mean()), 2),
                        "std": round(float(v.std(ddof=0)), 2),
                        "n": int(v.count()),
                    }

    fill(out["final_year_installed"], fy)
    fill(out["mid_year_installed"], mid)

    tot = df[df.ownership == "TOTAL"]
    for scn, gs in tot.groupby("scenario_name"):
        out["tipping_year"][scn] = {}
        for hs in heating_systems:
            out["tipping_year"][scn][hs] = {}
            for th in TIP_THRESHOLDS:
                out["tipping_year"][scn][hs][str(th)] = tipping_year(gs, hs, th)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results_csv")
    ap.add_argument("-o", "--out", default="golden/baseline_metrics.json")
    args = ap.parse_args()

    df = pd.read_csv(args.results_csv)
    required = {"scenario_name", "iteration", "year", "heating_system",
                "ownership", "installed_current"}
    missing = required - set(df.columns)
    if missing:
        sys.exit(f"ERROR: results CSV missing columns: {missing}")

    summary = summarise(df)
    summary["meta"]["source_file"] = args.results_csv
    with open(args.out, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Wrote {args.out}")
    print(f"  scenarios={len(summary['meta']['scenarios'])} "
          f"iterations={len(summary['meta']['iterations'])} "
          f"years {summary['meta']['start_year']}-{summary['meta']['final_year']}")


if __name__ == "__main__":
    main()
