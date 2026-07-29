#!/usr/bin/env python3
"""
District-heating focused analysis. Three separate graphs (stacked in one figure), each with:
  x-axis = year, y-axis = the metric, one line per scenario.

  1. % of neighbourhoods with a DH grid          (nbh_with_dh_perc)
  2. % of dwellings connected to DH              (DH installed_current / all dwellings)
  3. total dwellings connected to DH             (DH installed_current, TOTAL ownership)

Values are the mean over Monte-Carlo iterations. Purpose: see whether "low DH" is a grid-supply
problem (panel 1) or a connection problem (panels 2/3), and how the scenarios differ.

Usage:
  python dh_analysis.py --input <abs path>/simulation_results.csv --outdir <abs path>/plots
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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", required=True, help="absolute path to simulation_results.csv")
    ap.add_argument("--outdir", required=True, help="directory to write the figure into")
    ap.add_argument("--min-year", type=int, default=2025, help="drop the 2024 seed row by default")
    a = ap.parse_args()

    df = pd.read_csv(a.input)
    df = df[(df["ownership"] == "TOTAL") & (df["year"] >= a.min_year)].copy()

    # total dwellings per (scenario, iteration, year) = sum of installed_current over heating systems
    tot = (df.groupby(["scenario_name", "iteration", "year"])["installed_current"].sum()
             .rename("total_dw").reset_index())
    dh = df[df["heating_system"] == "DISTRICT_HEATING"][
        ["scenario_name", "iteration", "year", "installed_current", "nbh_with_dh_perc"]
    ].rename(columns={"installed_current": "dh_dw"})
    m = dh.merge(tot, on=["scenario_name", "iteration", "year"])
    m["nbh_dh_pct"] = m["nbh_with_dh_perc"] * 100.0
    m["dh_conn_pct"] = 100.0 * m["dh_dw"] / m["total_dw"]

    # mean over iterations
    agg = (m.groupby(["scenario_name", "year"])[["nbh_dh_pct", "dh_conn_pct", "dh_dw"]]
             .mean().reset_index())

    scenarios = sorted(agg["scenario_name"].unique())
    cmap = plt.get_cmap("tab20")
    colors = {s: cmap(i % 20) for i, s in enumerate(scenarios)}

    panels = [
        ("nbh_dh_pct", "Neighbourhoods with a DH grid (%)", "% of neighbourhoods"),
        ("dh_conn_pct", "Dwellings connected to DH (%)", "% of dwellings"),
        ("dh_dw", "Dwellings connected to DH (count)", "dwellings"),
    ]
    fig, axes = plt.subplots(3, 1, figsize=(10, 13), sharex=True)
    for ax, (col, title, ylabel) in zip(axes, panels):
        for s in scenarios:
            d = agg[agg["scenario_name"] == s].sort_values("year")
            ax.plot(d["year"], d[col], label=s, color=colors[s], linewidth=1.8)
        ax.set_title(title, fontsize=12)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.grid(True, alpha=0.3, linestyle="--")
    axes[-1].set_xlabel("Year", fontsize=11)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=8, frameon=False,
               bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("District heating: grid supply vs. dwelling connections, by scenario",
                 fontsize=14, y=0.995)
    plt.tight_layout(rect=[0, 0.10, 1, 0.98])

    os.makedirs(a.outdir, exist_ok=True)
    out = os.path.join(a.outdir, "dh_analysis.png")
    plt.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("wrote", out)

    # console summary (2050) so the numbers are checkable without opening the image
    last = agg[agg["year"] == agg["year"].max()].sort_values("dh_conn_pct", ascending=False)
    print(f"\n2050 summary ({int(agg['year'].max())}):")
    print(f"{'scenario':<42}{'nbhDH%':>8}{'conn%':>8}{'conn#':>10}")
    for _, r in last.iterrows():
        print(f"{r['scenario_name']:<42}{r['nbh_dh_pct']:>8.1f}{r['dh_conn_pct']:>8.2f}{r['dh_dw']:>10.0f}")


if __name__ == "__main__":
    main()
