#!/usr/bin/env python3
"""Summarise the HT_COSTPROBE output: what homeowners would have chosen on cost alone,
against what they actually chose through the full behavioural function.

Run the engine with the probe enabled, e.g.

    set HT_COSTPROBE=results\\calib\\costprobe.csv
    python model\\run.py --scope province:Noord-Brabant --scenario baseline --iterations 1

then

    python results_analysis\\cost_vs_choice.py results/calib/costprobe.csv
"""
import sys, os
import pandas as pd, numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

NICE = {"NATURAL_GAS_BOILER": "gas boiler", "NATURAL_GAS_BLOCK": "gas block",
        "HYBRID_HEAT_PUMP": "hybrid HP", "ELECTRIC_HEAT_PUMP": "electric HP",
        "DISTRICT_HEATING": "district heating"}
INK, MUTED = "#222222", "#666666"

def main(path, out=None):
    d = pd.read_csv(path)
    d = d.groupby(["year", "cheapest_on_cost", "chosen"], as_index=False).agg(
        households=("households", "sum"),
        prem=("mean_eac_premium_eur", "mean"))
    tot = d.groupby("year").households.sum()
    same = d[d.cheapest_on_cost == d.chosen].groupby("year").households.sum().reindex(tot.index, fill_value=0)

    print(f"\n{'year':>6}{'deciding':>10}{'cost-optimal':>14}{'share':>8}{'mean premium (EUR/yr)':>24}")
    for y in tot.index:
        sub = d[(d.year == y) & (d.cheapest_on_cost != d.chosen)]
        w = (sub.prem * sub.households).sum() / max(sub.households.sum(), 1)
        print(f"{y:>6}{tot[y]:>10,.0f}{same[y]:>14,.0f}{100*same[y]/tot[y]:>7.1f}%{w:>24,.0f}")

    last = d[d.year == d.year.max()]
    piv = last.pivot_table(index="cheapest_on_cost", columns="chosen",
                           values="households", aggfunc="sum", fill_value=0)
    print(f"\ncheapest option (rows) vs chosen option (columns), {int(d.year.max())}:")
    print(piv.rename(index=NICE, columns=NICE).to_string())

    fig, ax = plt.subplots(figsize=(7.2, 4.0))
    ax.plot(tot.index, 100 * same / tot, color="#2c7fb8", lw=2, marker="o", ms=4)
    ax.set_ylim(0, 100); ax.set_xlabel("year", fontsize=9.5, color=INK)
    ax.set_ylabel("% of deciding homeowners choosing the\ncheapest available option",
                  fontsize=9.5, color=INK)
    ax.set_title("How often the behavioural choice coincides with the cost-optimal one",
                 fontsize=11, color=INK, loc="left")
    ax.grid(alpha=0.25, lw=0.6); ax.tick_params(labelsize=9, colors=MUTED)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    for sp in ("left", "bottom"): ax.spines[sp].set_color("#bbbbbb")
    fig.tight_layout()
    out = out or os.path.join(os.path.dirname(path) or ".", "fig_cost_vs_choice.png")
    fig.savefig(out, dpi=190, bbox_inches="tight"); print(f"\nwrote {out}")

if __name__ == "__main__":
    if len(sys.argv) < 2: sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
