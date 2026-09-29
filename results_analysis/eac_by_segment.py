#!/usr/bin/env python3
"""Which heating option is cheapest, for whom? Equivalent annual cost (EAC) of every option per
dwelling, summarised per segment (ownership x archetype x heat-demand tier).

Computes, through the engine itself (jshell on the compiled classes, so the numbers are exactly the
model's Economics.computeEAC), the EAC of each option a dwelling may choose: gas boiler, gas block
(only for dwellings already on block heating), hybrid and electric heat pump, and district heating
(only where a grid exists). Prices are set as Simulation.updateEnergyPrices sets them for --year
and --price-t; investment costs are the starting values (no learning yet), which, if anything,
understates heat pumps later on. Everything is computed twice: as in the model, and without the
fixed gas connection charge, to show what that one term does to the ranking.

    python results_analysis/eac_by_segment.py --scope province:Noord-Brabant --year 2030

Writes to results/<tag>/figures/: eac_by_dwelling.csv (large), eac_by_segment.csv and
fig_eac_by_segment.png. The figure shows, for the largest segments, the share of dwellings for which
each option is the cheapest (with the fixed gas charge) and, as a marker, the electric heat pump's
share without it.
"""
import argparse
import os
import subprocess
import sys

import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
ENGINE = os.path.join(ROOT, "model", "engine-java")
CP = os.path.join(ENGINE, "build", "classes", "java", "main")
OPTS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP",
        "DISTRICT_HEATING"]
NICE = {"NATURAL_GAS_BOILER": "gas boiler", "NATURAL_GAS_BLOCK": "gas block",
        "HYBRID_HEAT_PUMP": "hybrid heat pump", "ELECTRIC_HEAT_PUMP": "electric heat pump",
        "DISTRICT_HEATING": "district heating"}
COLOR = {"NATURAL_GAS_BOILER": "#7f7f7f", "NATURAL_GAS_BLOCK": "#bdbdbd",
         "HYBRID_HEAT_PUMP": "#56b4e9", "ELECTRIC_HEAT_PUMP": "#0072b2", "DISTRICT_HEATING": "#d55e00"}

JSH = r'''
import heattransition.*;
import java.util.*;
var ag = StockLoader.load("__STOCK__", new Rng(1), 1, __START__);
int year = __YEAR__; double t = __T__;
var hs = HeatingSystemData.freshSpecs();
Economics.InsulationCost ins = (d, l) -> ag.vesta.insulationCost(d.archetype, d.constructionYear, d.energyLabel, l, d.livingAreaM2);
HeatingSystem[] OPT = { HeatingSystem.NATURAL_GAS_BOILER, HeatingSystem.NATURAL_GAS_BLOCK, HeatingSystem.HYBRID_HEAT_PUMP, HeatingSystem.ELECTRIC_HEAT_PUMP, HeatingSystem.DISTRICT_HEATING };
void prices(boolean gasFixed) {
  for (var type : HeatingSystem.values()) { var h = hs.get(type);
    h.primaryCostPerKWh = EnergyPrices.variable(h.primarySource, year, t);
    h.secondaryCostPerKWh = EnergyPrices.variable(h.secondarySource, year, t);
    double fx = 0;
    if (h.gasConnection && gasFixed) fx += EnergyPrices.gasFixed(year, t);
    if (h.heatConnection) fx += DistrictHeatingCosts.annualFixed();
    h.fixedAnnualCost = fx; }
}
boolean possible(HeatingSystem o, Dwelling d) {
  if (o == HeatingSystem.DISTRICT_HEATING) return d.hasDistrictHeatingGrid;
  if (o == HeatingSystem.NATURAL_GAS_BLOCK) return d.currentType == HeatingSystem.NATURAL_GAS_BLOCK;
  if (o == HeatingSystem.NATURAL_GAS_BOILER) return d.currentType != HeatingSystem.NATURAL_GAS_BLOCK;
  return true;
}
var all = new ArrayList<Dwelling>(); all.addAll(ag.homeowners); all.addAll(ag.landlords);
for (var b : ag.socialBlocks) all.addAll(b.households); for (var b : ag.hoaBlocks) all.addAll(b.households);
var pw = new java.io.PrintWriter("__OUT__");
pw.print("segment,ownership,archetype,demand_kwh,label,current");
for (var o : OPT) pw.print("," + o + "," + o + "_nogasfix");
pw.println();
long[][] eac = new long[all.size()][OPT.length * 2];
for (int pass = 0; pass < 2; pass++) {
  prices(pass == 0);
  for (int i = 0; i < all.size(); i++) { var d = all.get(i);
    for (int k = 0; k < OPT.length; k++)
      eac[i][2 * k + pass] = possible(OPT[k], d) ? Economics.computeEAC(hs.get(OPT[k]), d, ins) : -1; }
}
for (int i = 0; i < all.size(); i++) { var d = all.get(i);
  pw.print(Segments.contextSegment(d) + "," + d.ownership + "," + d.archetype + "," + Math.round(d.heatDemandKWh) + "," + d.energyLabel + "," + d.currentType);
  for (int j = 0; j < OPT.length * 2; j++) pw.print("," + (eac[i][j] < 0 ? "" : String.valueOf(eac[i][j])));
  pw.println(); }
pw.close();
/exit
'''


def tag(scope):
    k, _, n = scope.partition(":")
    if k.strip().lower() in ("nl", "netherlands"): return "nl"
    return n.strip().lower().replace(" ", "-") if k.strip().lower() == "province" else "gemeente_" + n.strip().lower()


def compute(a, out_csv):
    stock = os.path.join(ROOT, "model", "data", "stock", f"{tag(a.scope).replace('-', '-')}_dwellings.csv")
    if not os.path.exists(stock):
        sys.exit(f"no stock at {stock}")
    jsh = os.path.join(os.path.dirname(out_csv), "_eac_by_segment.jsh")
    fwd = lambda p: p.replace("\\", "/")
    open(jsh, "w", encoding="ascii").write(
        JSH.replace("__STOCK__", fwd(stock)).replace("__START__", str(a.start))
           .replace("__YEAR__", str(a.year)).replace("__T__", str(a.price_t)).replace("__OUT__", fwd(out_csv)))
    print(f"[eac] computing EACs for every dwelling in {a.scope} ({a.year}, price arm t={a.price_t}) ...", flush=True)
    r = subprocess.run(["jshell", f"-J-Xmx{a.xmx}", f"-R-Xmx{a.xmx}", "--class-path", CP, jsh],
                       cwd=ENGINE, capture_output=True, text=True)
    os.remove(jsh)
    if not os.path.exists(out_csv) or r.returncode != 0:
        sys.exit("[eac] jshell failed:\n" + r.stdout[-2000:] + r.stderr[-2000:])


def cheapest(df, suffix=""):
    cols = [o + suffix for o in OPTS]
    E = df[cols].to_numpy(dtype=float)
    E = np.where(np.isnan(E), np.inf, E)
    return np.array(OPTS)[E.argmin(axis=1)]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--year", type=int, default=2030, help="price year")
    ap.add_argument("--price-t", type=float, default=0.5, help="price arm: 0 low, 0.5 central, 1 high")
    ap.add_argument("--start", type=int, default=2024, help="stock start year (system ages)")
    ap.add_argument("--top", type=int, default=18, help="segments shown in the figure (largest)")
    ap.add_argument("--xmx", default="10g")
    ap.add_argument("--reuse", action="store_true", help="reuse eac_by_dwelling.csv if present")
    a = ap.parse_args()
    outdir = os.path.join(ROOT, "results", tag(a.scope), "figures"); os.makedirs(outdir, exist_ok=True)
    raw = os.path.join(outdir, "eac_by_dwelling.csv")
    if not (a.reuse and os.path.exists(raw)):
        compute(a, raw)
    df = pd.read_csv(raw)
    df["cheapest"] = cheapest(df)
    df["cheapest_nogasfix"] = cheapest(df, "_nogasfix")
    df["ehp_minus_hhp"] = df["ELECTRIC_HEAT_PUMP"] - df["HYBRID_HEAT_PUMP"]
    df["ehp_minus_hhp_nogasfix"] = df["ELECTRIC_HEAT_PUMP_nogasfix"] - df["HYBRID_HEAT_PUMP_nogasfix"]

    n = len(df)
    print(f"\n{n:,} dwellings, prices {a.year}, arm t={a.price_t}. Cheapest option (share of dwellings):")
    print(f"{'':22s}{'with gas fixed charge':>24s}{'without':>12s}")
    for o in OPTS:
        print(f"  {NICE[o]:20s}{100 * (df.cheapest == o).mean():20.1f} %{100 * (df.cheapest_nogasfix == o).mean():10.1f} %")
    print(f"\nEAC electric minus hybrid heat pump (EUR/yr): median {df.ehp_minus_hhp.median():+.0f} "
          f"(without gas fixed charge {df.ehp_minus_hhp_nogasfix.median():+.0f})")

    g = df.groupby("segment")
    seg = pd.DataFrame({"dwellings": g.size(),
                        **{f"cheapest_{o}": g["cheapest"].apply(lambda s, o=o: 100 * (s == o).mean()) for o in OPTS},
                        "cheapest_EHP_nogasfix": g["cheapest_nogasfix"].apply(lambda s: 100 * (s == "ELECTRIC_HEAT_PUMP").mean()),
                        "median_ehp_minus_hhp": g["ehp_minus_hhp"].median(),
                        "median_ehp_minus_hhp_nogasfix": g["ehp_minus_hhp_nogasfix"].median(),
                        **{f"median_eac_{o}": g[o].median() for o in OPTS}}).sort_values("dwellings", ascending=False)
    seg.to_csv(os.path.join(outdir, "eac_by_segment.csv"))
    print(f"\nlargest segments: share where the electric heat pump is cheapest, and the median EAC gap to the hybrid")
    print(f"{'segment':42s}{'dwellings':>10s}{'EHP cheapest':>14s}{'no gas fix':>12s}{'EHP-HHP':>10s}{'no gas fix':>12s}")
    for s_, r in seg.head(a.top).iterrows():
        print(f"{s_:42s}{int(r.dwellings):10,d}{r.cheapest_ELECTRIC_HEAT_PUMP:13.0f}%{r.cheapest_EHP_nogasfix:11.0f}%"
              f"{r.median_ehp_minus_hhp:+10.0f}{r.median_ehp_minus_hhp_nogasfix:+12.0f}")

    # figure: two panels sharing the segment axis
    top = seg.head(a.top)[::-1]
    labels = [s.replace("PRIVATELY_OWNED", "owner").replace("PRIVATELY_RENTED", "private rent")
               .replace("SOCIAL_HOUSING", "social").replace("HOME_OWNER_ASSOCIATION", "HOA")
               .replace("-dem", ", demand ").replace("-", ", ").lower() for s in top.index]
    y = np.arange(len(top))
    fig, (ax, bx) = plt.subplots(1, 2, figsize=(12.5, 0.36 * len(top) + 1.8), sharey=True,
                                 gridspec_kw={"width_ratios": [1.5, 1]})
    left = np.zeros(len(top))
    for o in OPTS:
        v = top[f"cheapest_{o}"].to_numpy()
        ax.barh(y, v, left=left, color=COLOR[o], height=0.72, edgecolor="white", linewidth=0.8)
        left += v
    ax.set_yticks(y); ax.set_yticklabels(labels, fontsize=8.5)
    ax.set_xlim(0, 100); ax.set_xlabel("share of dwellings for which the option is cheapest (%)", fontsize=9.5)
    ax.set_title(f"a   Cheapest option per segment ({a.year} prices)", fontsize=10.5, loc="left")
    bx.axvline(0, color="#888888", lw=0.8)
    bx.scatter(top["median_ehp_minus_hhp"], y, color=COLOR["ELECTRIC_HEAT_PUMP"], s=28, zorder=3)
    bx.scatter(top["median_ehp_minus_hhp_nogasfix"], y, facecolor="white", edgecolor=COLOR["ELECTRIC_HEAT_PUMP"],
               s=28, zorder=3)
    for yi, (a1, a2) in enumerate(zip(top["median_ehp_minus_hhp"], top["median_ehp_minus_hhp_nogasfix"])):
        bx.plot([a1, a2], [yi, yi], color="#bbbbbb", lw=1, zorder=2)
    bx.set_xlabel("median EAC electric minus hybrid heat pump (EUR/yr)\n< 0: electric is cheaper", fontsize=9.5)
    bx.set_title("b   Gap between the two heat pumps", fontsize=10.5, loc="left")
    for axis in (ax, bx):
        axis.grid(axis="x", alpha=.25); axis.set_axisbelow(True)
        for sp in ("top", "right"): axis.spines[sp].set_visible(False)
    handles = [plt.Rectangle((0, 0), 1, 1, color=COLOR[o]) for o in OPTS]
    labs = [NICE[o] for o in OPTS]
    handles += [Line2D([], [], marker="o", ls="", color=COLOR["ELECTRIC_HEAT_PUMP"], markersize=6),
                Line2D([], [], marker="o", ls="", markerfacecolor="white",
                       markeredgecolor=COLOR["ELECTRIC_HEAT_PUMP"], markersize=6)]
    labs += ["gap with gas fixed charge", "gap without gas fixed charge"]
    fig.legend(handles, labs, ncol=4, loc="lower center", frameon=False, fontsize=8.5, bbox_to_anchor=(0.5, -0.02))
    nf = 100 * (df.cheapest_nogasfix == "ELECTRIC_HEAT_PUMP").mean()
    fig.text(0.5, -0.045, f"Without the fixed gas connection charge the electric heat pump is the cheapest option "
                          f"for {nf:.1f} % of all dwellings (with it: "
                          f"{100 * (df.cheapest == 'ELECTRIC_HEAT_PUMP').mean():.1f} %). Investment costs "
                          f"at their starting values (no learning).", ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fp = os.path.join(outdir, "fig_eac_by_segment.png")
    fig.savefig(fp, dpi=160, bbox_inches="tight"); plt.close(fig)
    print(f"\nwrote {fp}\nwrote {os.path.join(outdir, 'eac_by_segment.csv')}")


if __name__ == "__main__":
    main()
