#!/usr/bin/env python3
"""At which investment cost and learning rate does the hybrid heat pump overtake the electric one?

Runs the BASELINE 2024-2050 at the central weight set (select_subsample.screen_indices) over a grid of
  * the electric heat pump's investment cost relative to the data (-Dht.capexMultEhp; the hybrid's
    stays at the data value unless --hhp-mult is given), and
  * learning-rate variants for the two heat pumps (-Dht.learningRateHhp / -Dht.learningRateEhp):
    as in the data, and equal rates for both,
and reports the 2050 share of each heat pump, their difference, and by interpolation the electric
capex multiplier at which the two are equal (the dominance switch) per learning variant.

    python results_analysis/hp_dominance_sweep.py --scope province:Noord-Brabant
    python results_analysis/hp_dominance_sweep.py --ehp-mult 0.8,1,1.2,1.4,1.6,1.8 --learning data,5/5,10/10,20/20

Each run is one iteration (~35 s for Noord-Brabant) and is kept, so a rerun skips what exists.
Writes results/<tag>/hp_sweep/: one CSV per run, hp_dominance_sweep.csv and fig_hp_dominance.png.
"""
import argparse
import csv
import json
import os
import subprocess
import sys

import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
CP = os.path.join(ROOT, "model", "engine-java", "build", "classes", "java", "main")
sys.path.insert(0, HERE)
from select_subsample import screen_indices                      # noqa: E402

TECHS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP",
         "DISTRICT_HEATING"]
DATA_CAPEX = {"HYBRID_HEAT_PUMP": 6000, "ELECTRIC_HEAT_PUMP": 9000}     # medium unit, for the ratio axis


def tag(scope):
    k, _, n = scope.partition(":")
    if k.strip().lower() in ("nl", "netherlands"): return "nl"
    return n.strip().lower().replace(" ", "-") if k.strip().lower() == "province" else "gemeente_" + n.strip().lower()


def learning_variants(spec):
    """'data' or 'H/E' in percent per doubling, e.g. '5/5' -> hybrid 5 %, electric 5 %."""
    out = []
    for v in spec.split(","):
        v = v.strip()
        if v == "data":
            out.append(("as data (hybrid 5 %, electric 10 %)", None, None))
        else:
            h, e = (float(x) / 100 for x in v.split("/"))
            out.append((f"hybrid {100*h:g} %, electric {100*e:g} %", h, e))
    return out


def shares_2050(path, year):
    acc = {t: 0.0 for t in TECHS}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["ownership"] == "TOTAL" and r["scenario_name"] == "baseline" and int(r["year"]) == year:
                acc[r["heating_system"]] += float(r["installed_current"])
    tot = sum(acc.values()) or 1.0
    return {t: 100 * v / tot for t, v in acc.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--search", default=os.path.join(ROOT, "results", "calib1000", "calibration_search.json"))
    ap.add_argument("--ehp-mult", default="0.8,1.0,1.2,1.4,1.6", help="electric heat pump capex multipliers")
    ap.add_argument("--hhp-mult", type=float, default=1.0, help="hybrid capex multiplier (held fixed)")
    ap.add_argument("--learning", default="data,5/5,10/10,20/20",
                    help="learning variants: 'data' and/or hybrid/electric %% per doubling, e.g. 10/10")
    ap.add_argument("--year", type=int, default=2050)
    ap.add_argument("--xmx", default="8g")
    a = ap.parse_args()

    ret = json.load(open(a.search, encoding="utf-8"))["retained"]
    _, c, src = screen_indices(a.search)
    weights = ret[c]["weights"]
    stock = os.path.join(ROOT, "model", "data", "stock", f"{tag(a.scope)}_dwellings.csv")
    outdir = os.path.join(ROOT, "results", tag(a.scope), "hp_sweep"); os.makedirs(outdir, exist_ok=True)
    mults = [float(x) for x in a.ehp_mult.split(",")]
    variants = learning_variants(a.learning)
    print(f"[hp] central set retained:{c} ({src}); {len(mults)} capex x {len(variants)} learning = "
          f"{len(mults) * len(variants)} runs of the baseline, {a.scope}")

    rows = []
    for li, (lab, lh, le) in enumerate(variants):
        for m in mults:
            name = f"L{li}_ehp{m:g}_hhp{a.hhp_mult:g}".replace(".", "p")
            out = os.path.join(outdir, name + ".csv")
            if not os.path.exists(out):
                props = {**weights, "capexMultEhp": m, "capexMultHhp": a.hhp_mult}
                if lh is not None: props["learningRateHhp"] = lh
                if le is not None: props["learningRateEhp"] = le
                cmd = ["java", f"-Xmx{a.xmx}"] + [f"-Dht.{k}={v}" for k, v in props.items()] + \
                      ["-cp", CP, "heattransition.Cli", "--real", stock, "--scenario", "baseline",
                       "--iterations", "1", "--start", "2024", "--end", str(a.year), "--out", out]
                print(f"[hp] {lab:34s} electric capex x{m:g}", flush=True)
                if subprocess.run(cmd, capture_output=True, text=True).returncode != 0:
                    sys.exit(f"engine run failed: {name}")
            s = shares_2050(out, a.year)
            rows.append({"learning": lab, "ehp_capex_mult": m, "hhp_capex_mult": a.hhp_mult,
                         "capex_ratio_ehp_hhp": DATA_CAPEX["ELECTRIC_HEAT_PUMP"] * m /
                                                (DATA_CAPEX["HYBRID_HEAT_PUMP"] * a.hhp_mult),
                         **{t: round(v, 2) for t, v in s.items()},
                         "ehp_minus_hhp": round(s["ELECTRIC_HEAT_PUMP"] - s["HYBRID_HEAT_PUMP"], 2)})
    with open(os.path.join(outdir, "hp_dominance_sweep.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)

    print(f"\n{a.year} share of dwellings (%), baseline, central weights, hybrid capex x{a.hhp_mult:g}")
    print(f"{'learning':36s}{'EHP capex':>10s}{'ratio':>7s}{'hybrid':>9s}{'electric':>10s}{'gas':>7s}{'DH':>6s}")
    switch = {}
    for lab, *_ in variants:
        rs = [r for r in rows if r["learning"] == lab]
        for r in rs:
            print(f"{lab:36s}{'x' + format(r['ehp_capex_mult'], 'g'):>10s}{r['capex_ratio_ehp_hhp']:7.2f}"
                  f"{r['HYBRID_HEAT_PUMP']:9.1f}{r['ELECTRIC_HEAT_PUMP']:10.1f}{r['NATURAL_GAS_BOILER']:7.1f}"
                  f"{r['DISTRICT_HEATING']:6.1f}")
        x = np.array([r["ehp_capex_mult"] for r in rs]); d = np.array([r["ehp_minus_hhp"] for r in rs])
        cross = [x[i] + (x[i + 1] - x[i]) * d[i] / (d[i] - d[i + 1])
                 for i in range(len(x) - 1) if d[i] > 0 >= d[i + 1]]
        switch[lab] = cross[0] if cross else None
    print("\nelectric capex multiplier at which the hybrid overtakes the electric heat pump in "
          f"{a.year} (interpolated):")
    for lab, v in switch.items():
        print(f"  {lab:36s}" + (f"x{v:.2f}  (electric EUR {9000 * v:,.0f} vs hybrid EUR {6000 * a.hhp_mult:,.0f})"
                                if v else "no switch within the tested range"))

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    for lab, *_ in variants:
        rs = [r for r in rows if r["learning"] == lab]
        ax.plot([r["capex_ratio_ehp_hhp"] for r in rs], [r["ehp_minus_hhp"] for r in rs], marker="o", lw=1.8,
                label=lab)
    ax.axhline(0, color="#555555", lw=0.9)
    ax.axvline(1.5 / a.hhp_mult, color="#999999", lw=0.8, ls=":")
    ax.text(1.5 / a.hhp_mult, ax.get_ylim()[1], " data", va="top", fontsize=8.5, color="#666666")
    ax.set_xlabel("investment cost electric / hybrid heat pump (medium unit)", fontsize=10)
    ax.set_ylabel(f"{a.year} share electric minus hybrid (pp)\n> 0: electric dominates", fontsize=10)
    ax.set_title("Where the hybrid heat pump overtakes the electric one", fontsize=11, loc="left")
    ax.legend(title="learning rate per doubling", fontsize=8.5, title_fontsize=8.5, frameon=False,
              loc="upper center", bbox_to_anchor=(0.5, -0.17), ncol=2)
    ax.grid(alpha=.25); ax.set_axisbelow(True)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    fp = os.path.join(outdir, "fig_hp_dominance.png")
    fig.savefig(fp, dpi=160, bbox_inches="tight"); plt.close(fig)
    print(f"\nwrote {fp}\nwrote {os.path.join(outdir, 'hp_dominance_sweep.csv')}")


if __name__ == "__main__":
    main()
