#!/usr/bin/env python3
"""The two exogenous/endogenous cost paths the model runs on: retail energy prices and learned capex.

Left panel  : retail gas and electricity prices 2024-2050 under the KEV central path (the engine
              default) with the LOW/HIGH bracket as a band. Computed from the growth rates actually
              compiled into the engine, read out of Constants.java and structural_sensitivity.py, so
              the figure cannot silently disagree with what the model uses.
Right panel : capital cost per installation, from a run's loop_state file. This one is endogenous --
              it falls through learning-by-doing, so it depends on how much gets installed and is
              therefore scenario- and weight-specific. The run it came from is named in the title.

    python price_capex_paths.py
    python price_capex_paths.py --loop ../results/nl/price_high/simulation_results_loop_state.csv
"""
import argparse, csv, os, re, sys
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
CONST = os.path.join(ROOT, "model", "engine-java", "src", "main", "java", "heattransition", "Constants.java")
BASE = {"gas": 0.14, "elec": 0.32}          # energy_source_data.csv, retail EUR/kWh
NICE = {"NATURAL_GAS_BOILER": "Gas boiler", "NATURAL_GAS_BLOCK": "Gas block heating",
        "HYBRID_HEAT_PUMP": "Hybrid heat pump", "ELECTRIC_HEAT_PUMP": "Electric heat pump",
        "DISTRICT_HEATING": "District heating"}
COLOR = {"NATURAL_GAS_BOILER": "#999999", "NATURAL_GAS_BLOCK": "#555555",
         "HYBRID_HEAT_PUMP": "#56B4E9", "ELECTRIC_HEAT_PUMP": "#0072B2",
         "DISTRICT_HEATING": "#D55E00"}
GAS_C, ELEC_C = "#999999", "#0072B2"


PRICE_TABLE = os.path.join(ROOT, "model", "data", "reference", "energy_prices_by_year.csv")


def price_paths():
    """Read the engine's own price table so the figure cannot drift from what the model ran.

    Returns {arm: {'gas': {year: EUR/kWh}, 'elec': {...}, 'heat': {...}}} in constant 2024 euros,
    or None when the table is missing (the caller then says so rather than drawing a guess)."""
    if not os.path.exists(PRICE_TABLE):
        return None
    out = {}
    with open(PRICE_TABLE, encoding="utf-8") as fh:
        rows = [ln for ln in fh if not ln.lstrip().startswith("#")]
    for r in csv.DictReader(rows):
        y, arm = int(r["year"]), r["arm"]
        d = out.setdefault(arm, {"gas": {}, "elec": {}, "heat": {}})
        d["gas"][y]  = float(r["gas_retail_eur_kwh"])
        d["elec"][y] = float(r["elec_retail_eur_kwh"])
        d["heat"][y] = float(r["heat_variable_eur_kwh"])
    return out


def fixed_costs():
    """{year: (gas_fixed, heat_fixed)} EUR/yr from the central arm, constant 2024 euros."""
    if not os.path.exists(PRICE_TABLE):
        return {}
    with open(PRICE_TABLE, encoding="utf-8") as fh:
        rows = [ln for ln in fh if not ln.lstrip().startswith("#")]
    return {int(r["year"]): (float(r["gas_fixed_eur_year"]), float(r["heat_fixed_eur_year"]))
            for r in csv.DictReader(rows) if r["arm"] == "central"}


def capex(path, scen="baseline"):
    acc = defaultdict(lambda: defaultdict(list))
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            if r.get("scenario_name") != scen: continue
            v = float(r["learned_capex"])
            if v <= 0: continue                      # year-0 rows are written before initialisation
            acc[r["heating_system"]][int(float(r["year"]))].append(v)
    return {t: {y: float(np.mean(v)) for y, v in sorted(d.items())} for t, d in acc.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--loop", default=os.path.join(ROOT, "results", "nl", "knockout_intact",
                                                   "simulation_results_loop_state.csv"))
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--start", type=int, default=2024)
    ap.add_argument("--end", type=int, default=2050)
    ap.add_argument("--out", default=None)
    ap.add_argument("--copy-to", default=os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures"))
    a = ap.parse_args()

    P = price_paths()
    if P is None:
        sys.exit(f"price table not found: {PRICE_TABLE}\n"
                 "run results_analysis/build_energy_price_table.py first")
    yrs = [y for y in sorted(P["central"]["gas"]) if a.start <= y <= a.end]
    fx = fixed_costs()
    print(f"price paths from {os.path.relpath(PRICE_TABLE, ROOT)}  ({yrs[0]}-{yrs[-1]})")
    for fuel, lab in (("gas", "Natural gas"), ("elec", "Electricity")):
        print(f"  {lab:12s} {a.end}:  low {P['low'][fuel][a.end]:.4f}   "
              f"central {P['central'][fuel][a.end]:.4f}   high {P['high'][fuel][a.end]:.4f}")
    if fx:
        g, h = fx[a.end]
        print(f"  fixed connection charge {a.end}:  gas {g:,.0f} EUR/yr   heat {h:,.0f} EUR/yr"
              f"   (electricity cancels between options)")

    fig, (axP, axC) = plt.subplots(1, 2, figsize=(12.4, 4.8))
    for lab, fuel, c in (("Electricity", "elec", ELEC_C), ("Natural gas", "gas", GAS_C)):
        lo = [P["low"][fuel][y] for y in yrs]
        hi = [P["high"][fuel][y] for y in yrs]
        cen = [P["central"][fuel][y] for y in yrs]
        axP.fill_between(yrs, lo, hi, color=c, alpha=.16, lw=0)
        axP.plot(yrs, cen, color=c, lw=2.3, label=f"{lab} (central)")
        for v in (lo, hi):
            axP.plot(yrs, v, color=c, lw=1.0, ls="--", alpha=.75)
        axP.annotate(f"{cen[-1]:.3f}", (yrs[-1], cen[-1]),
                     xytext=(4, 0), textcoords="offset points", fontsize=8.5, color=c, va="center")
    axP.set_ylabel("retail price (€/kWh, constant 2024 €)"); axP.set_xlabel("year")
    axP.set_xlim(a.start, a.end + 2); axP.set_ylim(bottom=0)
    axP.set_title("a. Retail energy prices — central arm, with the low–high band", fontsize=10.5, loc="left")
    axP.legend(fontsize=9, frameon=False, loc="center left")
    axP.grid(alpha=.25)

    cx = capex(a.loop, a.scenario) if os.path.exists(a.loop) else {}
    if cx:
        placed = []      # the two gas systems end on the same capex; stagger their end labels
        for t, d in sorted(cx.items(), key=lambda kv: -list(kv[1].values())[0]):
            ys = sorted(d); v = [d[y] for y in ys]
            axC.plot(ys, v, color=COLOR.get(t, "#333"), lw=2.2, label=NICE.get(t, t))
            dy = 0.0
            while any(abs(v[-1] + dy - q) < 180 for q in placed): dy -= 200
            placed.append(v[-1] + dy)
            axC.annotate(f"{100*(v[-1]/v[0]-1):+.0f}%", (ys[-1], v[-1] + dy), xytext=(4, 0),
                         textcoords="offset points", fontsize=8.5, color=COLOR.get(t, "#333"), va="center")
        axC.set_ylabel("capital cost per installation (€)"); axC.set_xlabel("year")
        axC.set_xlim(min(ys), max(ys) + 2); axC.set_ylim(bottom=0)
        axC.legend(fontsize=9, frameon=False, loc="center left")
        run = os.path.basename(os.path.dirname(a.loop))
        axC.set_title(f"b. Capital cost after learning-by-doing — {a.scenario}, {run}",
                      fontsize=10.5, loc="left")
        axC.grid(alpha=.25)
        print("\ncapital cost, first year -> 2050")
        for t, d in sorted(cx.items()):
            ys = sorted(d)
            print(f"  {NICE.get(t,t):22s} {d[ys[0]]:8,.0f} -> {d[ys[-1]]:8,.0f}  ({100*(d[ys[-1]]/d[ys[0]]-1):+5.1f} %)")
    else:
        axC.set_visible(False)
        print(f"[warn] no loop_state at {a.loop}; price panel only")
    for ax in (axP, axC):
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    fig.tight_layout()
    out = a.out or os.path.join(ROOT, "results", "nl", "figures", "fig_price_capex_paths.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fig.savefig(out, dpi=150, bbox_inches="tight"); plt.close(fig)
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil; shutil.copy2(out, a.copy_to)
    print("\nwrote:", out)


if __name__ == "__main__":
    main()
