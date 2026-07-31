#!/usr/bin/env python3
"""
Compare the ensemble of calibrated weight sets as a BAND (history-matching output).

`run.py --weights all|representative` writes one run per weight set under
results/<scope>/calib/<label>/simulation_results.csv. The plotting script draws each run in
isolation, so there is no cross-set view. This reads every calib/<label>/ run and, per scenario,
shows the spread across weight sets:

  * a summary CSV of the target-year share per (scenario, technology, weight set), and
  * per-technology time-series with the min-max band across weight sets shaded and the
    best_fit set drawn as the central line.

    python compare_weight_sets.py --scope province:Noord-Brabant
    python compare_weight_sets.py --dir results/noord-brabant/calib --scenario baseline --year 2050
"""
import argparse
import csv
import glob
import os
from collections import defaultdict


def tag(scope):
    s = scope.strip().lower()
    if s in ("nl", "netherlands"):
        return "nl"
    kind, name = s.split(":", 1)
    slug = name.strip().lower().replace(" ", "-")
    return slug if kind == "province" else "gemeente_" + slug


def load_run(path, ownership):
    """path/simulation_results.csv -> {scenario: {year: {tech: mean-share-% over iters}}}."""
    # (scenario, year, iter) -> {tech: installed}
    cells = defaultdict(lambda: defaultdict(float))
    for r in csv.DictReader(open(path)):
        if r["ownership"] != ownership:
            continue
        k = (r["scenario_name"], int(float(r["year"])), r["iteration"])
        cells[k][r["heating_system"]] += float(r["installed_current"])
    # mean share over iterations
    out = defaultdict(lambda: defaultdict(dict))
    grp = defaultdict(list)
    for (scn, yr, it), techs in cells.items():
        grp[(scn, yr)].append(techs)
    for (scn, yr), lst in grp.items():
        techs = set(t for d in lst for t in d)
        for t in techs:
            shares = [d.get(t, 0) / (sum(d.values()) or 1) * 100 for d in lst]
            out[scn][yr][t] = sum(shares) / len(shares)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default=None, help="province:<Name> | gemeente:<Name> | nl")
    ap.add_argument("--dir", default=None, help="results/<scope>/calib dir (overrides --scope)")
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--ownership", default="TOTAL")
    ap.add_argument("--year", type=int, default=2050, help="summary year for the CSV/table")
    ap.add_argument("--outdir", default=None, help="where to write comparison outputs (default: the calib dir)")
    a = ap.parse_args()

    calib = a.dir or os.path.join("results", tag(a.scope), "calib")
    runs = {}
    for p in sorted(glob.glob(os.path.join(calib, "*", "simulation_results.csv"))):
        label = os.path.basename(os.path.dirname(p))
        runs[label] = load_run(p, a.ownership)
    if not runs:
        raise SystemExit(f"no runs found under {calib}/*/simulation_results.csv "
                         f"(did you run `run.py --weights all|representative`?)")
    outdir = a.outdir or calib
    os.makedirs(outdir, exist_ok=True)
    print(f"{len(runs)} weight set(s): {', '.join(runs)}")

    # --- summary CSV: target-year share per (scenario, technology, weight set) + band ---
    scn = a.scenario
    techs = sorted({t for lab in runs for t in runs[lab].get(scn, {}).get(a.year, {})})
    summ = os.path.join(outdir, f"compare_{scn}_{a.year}_{a.ownership}.csv")
    with open(summ, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["technology"] + list(runs) + ["min", "max", "spread"])
        print(f"\n[{scn}] {a.ownership} share at {a.year} (%):")
        print("  " + "technology".ljust(20) + "".join(l[:12].rjust(13) for l in runs) + "   min   max  spread")
        for t in techs:
            vals = [runs[l].get(scn, {}).get(a.year, {}).get(t, 0.0) for l in runs]
            lo, hi = min(vals), max(vals)
            w.writerow([t] + [f"{v:.2f}" for v in vals] + [f"{lo:.2f}", f"{hi:.2f}", f"{hi-lo:.2f}"])
            print("  " + t.ljust(20) + "".join(f"{v:>12.1f}%" for v in vals) + f"{lo:>6.1f}{hi:>6.1f}{hi-lo:>7.1f}")
    print(f"\nwrote {summ}")

    # --- per-technology band plot over time (optional: needs matplotlib) ---
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"[plot] matplotlib unavailable ({e}); summary CSV written, skipping figure.")
        return
    years = sorted({y for lab in runs for y in runs[lab].get(scn, {})})
    techs_all = sorted({t for lab in runs for y in runs[lab].get(scn, {}) for t in runs[lab][scn][y]})
    n = len(techs_all)
    fig, axes = plt.subplots((n + 1) // 2, 2, figsize=(12, 3 * ((n + 1) // 2)), squeeze=False)
    for i, t in enumerate(techs_all):
        ax = axes[i // 2][i % 2]
        band_lo, band_hi = [], []
        for y in years:
            vals = [runs[l].get(scn, {}).get(y, {}).get(t, 0.0) for l in runs]
            band_lo.append(min(vals)); band_hi.append(max(vals))
        ax.fill_between(years, band_lo, band_hi, alpha=0.25, label="ensemble band")
        for l in runs:
            ys = [runs[l].get(scn, {}).get(y, {}).get(t, 0.0) for y in years]
            ax.plot(years, ys, lw=(2.2 if l == "best_fit" else 0.9),
                    label=l if (l == "best_fit" or len(runs) <= 4) else None)
        ax.set_title(t); ax.set_ylabel("% share"); ax.grid(alpha=0.3)
    axes[0][0].legend(fontsize=8)
    fig.suptitle(f"{scn} — {a.ownership}: technology share, band across {len(runs)} weight set(s)")
    fig.tight_layout()
    out = os.path.join(outdir, f"compare_{scn}_{a.ownership}_band.png")
    fig.savefig(out, dpi=110)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
