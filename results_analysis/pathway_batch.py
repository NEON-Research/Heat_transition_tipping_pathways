#!/usr/bin/env python3
"""
Overnight PATHWAY job: the preference band + the coupled energy-price bracket. One command, unattended.

  1. Preference band  : run.py --weights all  (every retained weight set) x --scenario all  -> the
                        2024-2050 band across preference uncertainty (results/<scope>/calib/<set>/).
  2. Price bracket    : the calibrated central weights (best_fit) run at coupled LOW and HIGH energy
                        prices (static is already the baseline in step 1) -> price robustness
                        (results/<scope>/price_low|price_high/).
  3. Compare figure   : compare_weight_sets.py over the ensemble -> per-scenario band CSV + plot.

Continues on a step failure; logs to results/<scope>/. This is the companion to structural_batch.py
(which does the structural sensitivity); together they cover categories A (weights), D (prices) and
B/C (structural).

    python pathway_batch.py --scope province:Noord-Brabant
    python pathway_batch.py --scope province:Noord-Brabant --iterations 5 --price-scenario baseline
"""
import argparse, json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
RUNPY = os.path.join(ROOT, "model", "run.py")
COMPARE = os.path.join(HERE, "compare_weight_sets.py")
SEARCH = os.path.join(ROOT, "results", "calib", "calibration_search.json")
# coupled price ends (KEV 2025 Table 10 2030 bandwidth -> retail; gas & electricity move together)
PRICE = {"low":  {"ht.gasPriceGrowth": -0.0100, "ht.elecPriceGrowth": -0.0157},
         "high": {"ht.gasPriceGrowth":  0.0262, "ht.elecPriceGrowth":  0.0141}}

def tag(scope):
    s = scope.strip().lower()
    if s in ("nl", "netherlands"): return "nl"
    kind, name = s.split(":", 1); slug = name.strip().lower().replace(" ", "-")
    return slug if kind == "province" else "gemeente_" + slug

def run(cmd, log):
    print(f"\n$ {' '.join(str(c) for c in cmd)}", flush=True)
    with open(log, "w") as fh:
        rc = subprocess.run([str(c) for c in cmd], stdout=fh, stderr=subprocess.STDOUT).returncode
    print("  " + "\n  ".join(open(log).read().splitlines()[-3:]))
    print(f"  [{'ok' if rc==0 else 'FAILED rc='+str(rc)} -> {log}]")
    return rc

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--iterations", type=int, default=3, help="MC iters per scenario (band spread dominates; 3-5 is enough)")
    ap.add_argument("--ensemble-weights", default="all", help="run.py --weights value for the band (all | representative)")
    ap.add_argument("--price-weights", default="best_fit", help="representative set to run the price bracket at")
    ap.add_argument("--price-scenario", default="all", help="--scenario for the price runs (all | baseline)")
    ap.add_argument("--no-band", action="store_true")
    ap.add_argument("--no-price", action="store_true")
    a = ap.parse_args()
    resdir = os.path.join(ROOT, "results", tag(a.scope)); os.makedirs(resdir, exist_ok=True)
    t0 = time.time()

    # 1. preference band
    if not a.no_band:
        run([sys.executable, RUNPY, "--scope", a.scope, "--scenario", "all",
             "--iterations", a.iterations, "--analyze", "--weights", a.ensemble_weights],
            os.path.join(resdir, "log_band.txt"))

    # 2. coupled price bracket at the calibrated central weights (weights via --prop so --out is free)
    if not a.no_price:
        w = json.load(open(SEARCH))["representative"][a.price_weights]["weights"]
        wprops = []
        for k, v in w.items(): wprops += ["--prop", f"ht.{k}={v}"]
        for lvl, gp in PRICE.items():
            props = list(wprops)
            for k, v in gp.items(): props += ["--prop", f"{k}={v}"]
            out = os.path.join(resdir, f"price_{lvl}", "simulation_results.csv")
            run([sys.executable, RUNPY, "--scope", a.scope, "--scenario", a.price_scenario,
                 "--iterations", a.iterations, "--analyze", "--out", out] + props,
                os.path.join(resdir, f"log_price_{lvl}.txt"))

    # 3. ensemble comparison figure
    if not a.no_band:
        run([sys.executable, COMPARE, "--scope", a.scope, "--scenario", "baseline"],
            os.path.join(resdir, "log_compare.txt"))

    print(f"\n[batch] done in {(time.time()-t0)/3600:.2f} h. Band in {resdir}/calib/, "
          f"price runs in {resdir}/price_low|price_high/, logs in {resdir}/")

if __name__ == "__main__":
    main()
