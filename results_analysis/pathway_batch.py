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
LOOP_EFFECTS = os.path.join(HERE, "loop_effects.py")
SEARCH_DEFAULT = os.path.join(ROOT, "results", "calib", "calibration_search.json")
# Coupled price arms. The engine reads the whole price path from
# model/data/reference/energy_prices_by_year.csv (built by build_energy_price_table.py); -Dht.priceT
# picks the arm, 0 = low, 0.5 = central (the default), 1 = high. Gas and electricity move together,
# as they did under the previous pair of growth rates.
PRICE = {"low":  {"ht.priceT": 0.0},
         "high": {"ht.priceT": 1.0}}
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

def newest_search():
    """The most recently written calibration_search.json under results/. Picking the newest and
    printing it is safer than a fixed path: a hardcoded default silently keeps pointing at a
    superseded calibration long after a new one has been run."""
    import glob as _glob
    c = _glob.glob(os.path.join(ROOT, "results", "*", "calibration_search.json"))
    if not c:
        sys.exit("[batch] no calibration_search.json under results/ -- run calibrate_weights first")
    return max(c, key=os.path.getmtime)


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

      central       the central retained set (select_subsample.screen_indices): the default, and
                    the same set the LHS runs at
      median        marginal median of the retained ensemble (a synthetic combination that need
                    not itself be plausible; kept for comparison only)
      retained:<i>  an actual retained set, ranked by fit (0 = best)
      <key>         a representative set stored by calibrate_weights (best_fit, low_/high_<axis>)
    """
    d = json.load(open(search_json))
    ret = d.get("retained") or d.get("samples")
    if which == "central":
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from select_subsample import screen_indices
        _, c, src = screen_indices(search_json)
        print(f"[batch] central set: retained:{c} ({src})")
        return dict(ret[c]["weights"])
    if which == "median":
        import statistics
        keys = list(ret[0]["weights"])
        return {k: float(statistics.median([e["weights"][k] for e in ret])) for k in keys}
    if which.startswith("retained:"):
        return dict(ret[int(which.split(":", 1)[1])]["weights"])
    reps = d.get("representative", {})
    if which in reps:
        return dict(reps[which]["weights"])
    raise SystemExit(f"unknown --price-weights '{which}'. Use central, median, retained:<i>, or one of {list(reps)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", required=True,
                    help="nl | province:<Name> | gemeente:<Name>. Required: this job costs hours and\n"
                         "writes to results/<scope>/, so it must not have a default that silently\n"
                         "sends a national run to the calibration province.")
    ap.add_argument("--iterations", type=int, default=3, help="MC iters per scenario (band spread dominates; 3-5 is enough)")
    ap.add_argument("--ensemble-weights", default="all", help="run.py --weights value for the band (all | representative)")
    ap.add_argument("--price-weights", default="central",
                    help="weight set for the price bracket and the knock-outs: 'central' (default:\n"
                         "the central retained set, the same set the LHS runs at), 'retained:<i>',\n"
                         "a representative key (best_fit / low_/high_shareAffordability), or\n"
                         "'median' (marginal median; synthetic, for comparison only)")
    ap.add_argument("--xmx", default=None,
                    help="JVM heap for every engine run (nl scale needs ~48g; run.py defaults to 8g)")
    ap.add_argument("--price-scenario", default="all", help="--scenario for the price runs (all | baseline)")
    ap.add_argument("--no-band", action="store_true")
    ap.add_argument("--no-price", action="store_true")
    ap.add_argument("--no-knockout", action="store_true",
                    help="skip the loop knock-out runs (causal evidence for the tipping mechanisms)")
    ap.add_argument("--search", default=None,
                    help="calibration search JSON holding the retained ensemble. Default: the most\n"
                         "recently written calibration_search.json under results/ -- a fixed default\n"
                         "goes stale the moment a new calibration is run, and the weights then differ\n"
                         "silently from the ensemble everything else uses.")
    ap.add_argument("--knockout-salience", action="store_true",
                    help="also run the salience-amplifier freeze (bounds the amplifier, not the loop)")
    ap.add_argument("--knockout-scenario", default="baseline",
                    help="scenario for the knock-out runs (default baseline; 'all' for the full matrix)")
    a = ap.parse_args()
    SEARCH = a.search or newest_search()
    if not os.path.exists(SEARCH):
        sys.exit(f"[batch] no calibration search file at {SEARCH}")
    resdir = os.path.join(ROOT, "results", tag(a.scope)); os.makedirs(resdir, exist_ok=True)
    XMX = ["--xmx", a.xmx] if a.xmx else []   # forwarded to every run.py invocation
    t0 = time.time()

    # Echo what was actually resolved, and record it next to the output. Every provenance problem
    # in this project has come from a run whose scope or ensemble was not what the reader assumed.
    n_ret = len((json.load(open(SEARCH)).get("retained") or []))
    steps = [n for n, on in (("band", not a.no_band), ("price", not a.no_price),
                             ("knock-out", not a.no_knockout)) if on]
    print("=" * 72)
    print(f"  scope      {a.scope}  ->  results/{tag(a.scope)}/")
    print(f"  ensemble   {os.path.relpath(SEARCH, ROOT)}  ({n_ret} retained sets)")
    print(f"  steps      {', '.join(steps) or 'none'}   iterations {a.iterations}")
    print("=" * 72, flush=True)
    manifest = {"scope": a.scope, "tag": tag(a.scope), "search": os.path.relpath(SEARCH, ROOT),
                "n_retained": n_ret, "iterations": a.iterations, "steps": steps,
                "price_weights": a.price_weights, "ensemble_weights": a.ensemble_weights,
                "started": time.strftime("%Y-%m-%d %H:%M:%S")}
    with open(os.path.join(resdir, "run_manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2)

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
        # segment heatmap (seg_clusters_context_2050.png): written into mechanism/ BEFORE the
        # mechanism report, which copies it to the manuscript for a national run
        run([sys.executable, os.path.join(HERE, "segment_clusters.py"), "--run", out,
             "--segment-type", "context", "--outdir", os.path.join(resdir, "mechanism")],
            os.path.join(resdir, "log_segment_clusters.txt"))
        # mechanism report: loop state + segments + the knock-out table
        ko = [f"{lab}={os.path.join(resdir, f'knockout_{lab}', 'simulation_results.csv')}"
              for lab in ko_runs]
        run([sys.executable, os.path.join(HERE, "mechanism_analysis.py"),
             "--run", out, "--knockout"] + ko + ["--outdir", os.path.join(resdir, "mechanism")],
            os.path.join(resdir, "log_mechanism.txt"))

    # 3. figures. The two band-dependent scripts need the ensemble runs, but the scenario-mix
    #    grid reads the price and knock-out folders too, so it must also run after a --no-band
    #    job -- otherwise a knock-out re-run silently leaves its own figure stale.
    if not a.no_band:
        run([sys.executable, ANALYZE_ALL, "--scope", a.scope],
            os.path.join(resdir, "log_analyze.txt"))
        # paper figures (the affordability-coloured ensemble band + structural screen) live in a
        # separate script because they cut across scopes; run them so one command yields every figure
        run([sys.executable, PAPER_FIGS, "--scope", a.scope, "--calib", os.path.dirname(SEARCH)],
            os.path.join(resdir, "log_paper_figures.txt"))
    if not (a.no_band and a.no_price and a.no_knockout):
        # small multiples: stacked-to-100% mix 2024-2050, one panel per scenario (ensemble median,
        # price bracket, knock-outs) -- the replacement for the single-year price triptych
        run([sys.executable, MIX_GRID, "--scope", a.scope, "--search", SEARCH],
            os.path.join(resdir, "log_mix_grid.txt"))
        # loop effects: the divergence-from-baseline figure main.tex includes, plus the lever chart.
        # Reads the cache scenario_mix_grid.py just wrote, so it must run AFTER it.
        run([sys.executable, LOOP_EFFECTS, "--scope", a.scope],
            os.path.join(resdir, "log_loop_effects.txt"))

    print(f"\n[batch] done in {(time.time()-t0)/3600:.2f} h.\n"
          f"  band      -> {resdir}/calib/\n"
          f"  prices    -> {resdir}/price_low|price_high/\n"
          f"  knock-out -> {resdir}/knockout_*/ + mechanism report in {resdir}/mechanism/\n"
          f"  figures   -> {resdir}/figures/ (per-scenario bands in figures/scenarios/)")

if __name__ == "__main__":
    main()
