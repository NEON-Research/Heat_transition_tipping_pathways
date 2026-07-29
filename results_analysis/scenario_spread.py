#!/usr/bin/env python3
"""
Cross-scenario SPREAD of the 2050 technology mix (S0 in MODEL_TODOS).

For a given year (default 2050) and ownership (default TOTAL), compute each heating system's share
of dwellings per scenario (mean over Monte-Carlo iterations), then summarise how much that share
varies ACROSS the 16 scenarios: min / max / range / stdev, plus which scenario is the min and max.
This shows *where* the scenarios actually diverge (big range) vs coincide (small range).

Prints a table and writes a strip figure (one dot per scenario per technology).

Usage:
  python scenario_spread.py --input <abs>/simulation_results.csv --outdir <abs>/plots
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HEATING = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP",
           "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
SHORT = {"NATURAL_GAS_BOILER": "gas boiler", "NATURAL_GAS_BLOCK": "gas block",
         "HYBRID_HEAT_PUMP": "hybrid HP", "ELECTRIC_HEAT_PUMP": "electric HP",
         "DISTRICT_HEATING": "district heating"}


def shares(df, year, ownership):
    """-> DataFrame index=scenario, columns=heating system, values=% share at `year`/`ownership`."""
    d = df[(df["year"] == year) & (df["ownership"] == ownership)]
    # mean over iterations of installed_current, per scenario x heating system
    piv = (d.groupby(["scenario_name", "heating_system"])["installed_current"].mean()
             .unstack("heating_system"))
    piv = piv[[c for c in HEATING if c in piv.columns]]
    return 100.0 * piv.div(piv.sum(axis=1), axis=0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--year", type=int, default=2050)
    ap.add_argument("--ownership", default="TOTAL")
    a = ap.parse_args()

    df = pd.read_csv(a.input)
    sh = shares(df, a.year, a.ownership)

    # ---- spread table ----
    print(f"\nCross-scenario spread of {a.year} technology mix (ownership={a.ownership}, "
          f"{sh.shape[0]} scenarios), % of dwellings:\n")
    print(f"{'technology':<18}{'min':>7}{'max':>7}{'range':>7}{'stdev':>7}{'mean':>7}   (min → max scenario)")
    rows = []
    for hs in [c for c in HEATING if c in sh.columns]:
        s = sh[hs]
        lo, hi = s.min(), s.max()
        rows.append((SHORT[hs], lo, hi, hi - lo, s.std(), s.mean(),
                     s.idxmin(), s.idxmax()))
    for name, lo, hi, rng, sd, mn, sc_lo, sc_hi in sorted(rows, key=lambda r: -r[3]):
        print(f"{name:<18}{lo:>7.1f}{hi:>7.1f}{rng:>7.1f}{sd:>7.1f}{mn:>7.1f}   {sc_lo}  →  {sc_hi}")

    # ---- strip figure: one dot per scenario per technology ----
    fig, ax = plt.subplots(figsize=(9, 5.5))
    cols = [c for c in HEATING if c in sh.columns]
    rng_order = sorted(cols, key=lambda c: sh[c].max() - sh[c].min(), reverse=True)
    for i, hs in enumerate(rng_order):
        vals = sh[hs].values
        x = np.random.RandomState(0).normal(i, 0.06, len(vals))
        ax.plot([i, i], [vals.min(), vals.max()], color="#bbb", lw=6, solid_capstyle="round", zorder=1)
        ax.scatter(x, vals, s=28, zorder=2, color="#1f77b4", alpha=0.8)
        ax.scatter([i], [vals.mean()], marker="_", s=600, color="crimson", zorder=3)
    ax.set_xticks(range(len(rng_order)))
    ax.set_xticklabels([SHORT[c] for c in rng_order])
    ax.set_ylabel(f"share of dwellings at {a.year} (%)")
    ax.set_title(f"Cross-scenario spread of the {a.year} technology mix ({a.ownership})\n"
                 f"one dot per scenario · grey bar = min–max range · red = mean", fontsize=11)
    ax.grid(True, axis="y", alpha=0.3, linestyle="--")
    os.makedirs(a.outdir, exist_ok=True)
    out = os.path.join(a.outdir, "scenario_spread.png")
    plt.tight_layout()
    plt.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
