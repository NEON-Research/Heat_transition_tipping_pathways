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
ANALYZE_ALL = os.path.join(HERE, "analyze_all.py")
PAPER_FIGS  = os.path.join(HERE, "paper_figures.py")
MIX_GRID    = os.path.join(HERE, "scenario_mix_grid.py")
SEARCH_DEFAULT = os.path.join(ROOT, "results", "calib", "calibration_search.json")
# coupled price ends (KEV 2025 Table 10 2030 bandwidth -> retail; gas & electricity move together)
PRICE = {"low":  {"ht.gasPriceGrowth": -0.0100, "ht.elecPriceGrowth": -0.0157},
         "high": {"ht.gasPriceGrowth":  0.0262, "ht.elecPriceGrowth":  0.0141}}
# Knock-out runs: disable ONE reinforcing loop at a time, everything else identical (same seeds,
# stock, weights). The gap versus the intact run is that loop's causal contribution -- the evidence
# that the transition depends on the feedback, rather than merely correlating with it (Q8).
# NOTE on the social knock-out: `salienceFreeze` pins only the salience AMPLIFIER and leaves each
# household's peer composition free to evolve, so the diffusion feedback keeps running and the run
# is not a knock-out of social learning (Constants.java says so explicitly). `peerFreeze` is the
# real one: it blocks notifyPeers, so peer counts stay at their t0 values and the subjective norm
# stops responding to what neighbours do. The batch used salienceFreeze up to 2026-08-31.
KNOCKOUT = {"nolearn":   {"ht.learningRateMult": 0},      # economic learning loop severed
            "nosocial":  {"ht.peerFreeze": 1},            # social learning loop severed (diffusion)
            "nocongest": {"ht.congestionOff": 1}}         # grid constraint removed (control)
# opt-in extra: bounds the salience amplifier alone, which is a weaker claim than the loop knock-out
KNOCKOUT_EXTRA = {"nosalience": {"ht.salienceFreeze": 1}}

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

def resolve_weights(search_json, which):
    """Same semantics as structural_sensitivity.get_weights, so the price bracket and the
    parameter screen can be run at the SAME point in weight space.

      median        marginal median of the retained ensemble (what the screen uses by default)
      retained:<i>  an actual retained set, ranked by fit (0 = best)
      <key>         a representative set stored by calibrate_weights (best_fit, low_/high_<axis>)
    """
    d = json.load(open(search_json))
    ret = d.get("retained") or d.get("samples")
    if which == "median":
        import statistics
        keys = list(ret[0]["weights"])
        return {k: float(statistics.median([e["weights"][k] for e in ret])) for k in keys}
    if which.startswith("retained:"):
        return dict(ret[int(which.split(":", 1)[1])]["weights"])
    reps = d.get("representative", {})
    if which in reps:
        return dict(reps[which]["weights"])
    raise SystemExit(f"unknown --price-weights '{which}'. Use median, retained:<i>, or one of {list(reps)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--iterations", type=int, default=3, help="MC iters per scenario (band spread dominates; 3-5 is enough)")
    ap.add_argument("--ensemble-weights", default="all", help="run.py --weights value for the band (all | representative)")
    ap.add_argument("--price-weights", default="median",
                    help="weight point for the price bracket: 'median' (the marginal median of the\n"
                         "retained ensemble, i.e. the same point the structural screen uses),\n"
                         "'retained:<i>' for an actual retained set, or a representative key\n"
                         "(best_fit / low_shareAffordability / high_shareAffordability)")
    ap.add_argument("--xmx", default=None,
                    help="JVM heap for every engine run (nl scale needs ~48g; run.py defaults to 8g)")
    ap.add_argument("--price-scenario", default="all", help="--scenario for the price runs (all | baseline)")
    ap.add_argument("--no-band", action="store_true")
    ap.add_argument("--no-price", action="store_true")
    ap.add_argument("--no-knockout", action="store_true",
                    help="skip the loop knock-out runs (causal evidence for the tipping mechanisms)")
    ap.add_argument("--search", default=SEARCH_DEFAULT,
                    help="calibration search JSON holding the retained ensemble "
                         "(default: results/calib/calibration_search.json)")
    ap.add_argument("--knockout-salience", action="store_true",
                    help="also run the salience-amplifier freeze (bounds the amplifier, not the loop)")
    ap.add_argument("--knockout-scenario", default="baseline",
                    help="scenario for the knock-out runs (default baseline; 'all' for the full matrix)")
    a = ap.parse_args()
    if not os.path.exists(a.search):
        sys.exit(f"[batch] no calibration search file at {a.search}")
    SEARCH = a.search
    resdir = os.path.join(ROOT, "results", tag(a.scope)); os.makedirs(resdir, exist_ok=True)
    XMX = ["--xmx", a.xmx] if a.xmx else []   # forwarded to every run.py invocation
    t0 = time.time()

    # 1. preference band
    if not a.no_band:
        run([sys.executable, RUNPY, "--scope", a.scope, "--scenario", "all",
             "--iterations", a.iterations, "--analyze", "--weights", a.ensemble_weights,
             "--weights-file", SEARCH] + XMX,
            os.path.join(resdir, "log_band.txt"))

    # 2. coupled price bracket at the calibrated central weights (weights via --prop so --out is free)
    if not a.no_price:
        w = resolve_weights(SEARCH, a.price_weights)
        wprops = []
        for k, v in w.items(): wprops += ["--prop", f"ht.{k}={v}"]
        for lvl, gp in PRICE.items():
            props = list(wprops)
            for k, v in gp.items(): props += ["--prop", f"{k}={v}"]
            out = os.path.join(resdir, f"price_{lvl}", "simulation_results.csv")
            run([sys.executable, RUNPY, "--scope", a.scope, "--scenario", a.price_scenario,
                 "--iterations", a.iterations, "--analyze", "--out", out] + XMX + props,
                os.path.join(resdir, f"log_price_{lvl}.txt"))

    # 2b. knock-out runs at the calibrated central weights: causal evidence per reinforcing loop
    if not a.no_knockout:
        w = resolve_weights(SEARCH, a.price_weights)
        wprops = []
        for k, v in w.items(): wprops += ["--prop", f"ht.{k}={v}"]
        ko_runs = dict(KNOCKOUT)
        if a.knockout_salience: ko_runs.update(KNOCKOUT_EXTRA)
        for lab, flags in ko_runs.items():
            props = list(wprops)
            for k, v in flags.items(): props += ["--prop", f"{k}={v}"]
            out = os.path.join(resdir, f"knockout_{lab}", "simulation_results.csv")
            run([sys.executable, RUNPY, "--scope", a.scope, "--scenario", a.knockout_scenario,
                 "--iterations", a.iterations, "--out", out] + XMX + props,
                os.path.join(resdir, f"log_knockout_{lab}.txt"))
        # intact reference at the same weights, for the comparison
        out = os.path.join(resdir, "knockout_intact", "simulation_results.csv")
        run([sys.executable, RUNPY, "--scope", a.scope, "--scenario", a.knockout_scenario,
             "--iterations", a.iterations, "--out", out] + XMX + wprops,
            os.path.join(resdir, "log_knockout_intact.txt"))
        # mechanism report: loop state + segments + the knock-out table
        ko = [f"{lab}={os.path.join(resdir, f'knockout_{lab}', 'simulation_results.csv')}"
              for lab in ko_runs]
        run([sys.executable, os.path.join(HERE, "mechanism_analysis.py"),
             "--run", out, "--knockout"] + ko + ["--outdir", os.path.join(resdir, "mechanism")],
            os.path.join(resdir, "log_mechanism.txt"))

    # 3. full cross-set analysis: per-scenario bands + global figures (Morris, price triptych, EHP)
    if not a.no_band:
        run([sys.executable, ANALYZE_ALL, "--scope", a.scope],
            os.path.join(resdir, "log_analyze.txt"))
        # paper figures (the affordability-coloured ensemble band + structural screen) live in a
        # separate script because they cut across scopes; run them so one command yields every figure
        run([sys.executable, PAPER_FIGS, "--scope", a.scope, "--calib", os.path.dirname(SEARCH)],
            os.path.join(resdir, "log_paper_figures.txt"))
        # small multiples: stacked-to-100% mix 2024-2050, one panel per scenario (ensemble median,
        # price bracket, knock-outs) -- the replacement for the single-year price triptych
        run([sys.executable, MIX_GRID, "--scope", a.scope],
            os.path.join(resdir, "log_mix_grid.txt"))

    print(f"\n[batch] done in {(time.time()-t0)/3600:.2f} h.\n"
          f"  band      -> {resdir}/calib/\n"
          f"  prices    -> {resdir}/price_low|price_high/\n"
          f"  knock-out -> {resdir}/knockout_*/ + mechanism report in {resdir}/mechanism/\n"
          f"  figures   -> {resdir}/figures/ (per-scenario bands in figures/scenarios/)")

if __name__ == "__main__":
    main()
