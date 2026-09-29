#!/usr/bin/env python3
"""
Structural (non-weight) sensitivity of the 2024-2050 PATHWAY, run at FIXED calibrated weights.

The weight calibration (calibrate_weights.py) covers preference uncertainty and is scored against
observed 2022-24. This script screens the *other* uncertainty categories — behavioural-dynamics and
techno-economic parameters — on the 2050 outcome, holding the decision weights at the retained-ensemble
median so "which weights fit" is not confounded with "how the dynamics play out" (see
CALIBRATION_AND_VALIDATION.md sec 6). Baseline scenario only, by default.

Two modes (the recommended workflow is morris FIRST, then lhs on the survivors):
  morris  — elementary-effects screen over all factors -> mu* ranking (which matter)
  lhs     — Latin-hypercube sample over a chosen subset -> objective CSV + standardized regression
            coefficients (quantify the key few)

    python structural_sensitivity.py morris --scope province:Noord-Brabant --trajectories 8 --iterations 3
    python structural_sensitivity.py lhs    --scope province:Noord-Brabant --samples 80 --iterations 5 \
        --factors salienceSteepness,salienceThreshold,gumbelScaleEac,capexMultHp,energyPrice
"""
import argparse, csv, json, os, subprocess, sys, tempfile
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
MODEL = os.path.join(ROOT, "model")
ENGINE = os.path.join(MODEL, "engine-java")
STOCK = os.path.join(MODEL, "data", "stock")
ENGINE_CP = os.environ.get("HT_ENGINE_CP", os.path.join(MODEL, "engine-java", "build", "classes", "java", "main"))
SYSTEMS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]

# Energy price is ONE coupled factor (gas & electricity move together — power price is gas-linked at
# the margin). t in [0,1] selects the arm of model/data/reference/energy_prices_by_year.csv:
# 0 = low, 0.5 = central, 1 = high, linearly interpolated in between. The engine reads the whole
# path from that table, so there is nothing to map here beyond passing t through.
#
# This replaces the earlier pair of constant growth rates. A single CAGR escalated the energy tax and
# the network charge at the commodity's rate and could not express KEV's ramp-to-2030-then-flat
# shape; the table separates commodity from tax and varies only the commodity.
def price_map(t):
    return {"priceT": t}

# name -> (low, high, map(actual)->{-Dht key: value}). Only parameters the user retained (efficiency,
# discount, lifetime, subsidy, network size are deliberately excluded and held fixed).
FACTORS = {
    "salienceSteepness": (10.0, 50.0, lambda v: {"salienceSteepness": v}),
    "salienceThreshold": (0.15, 0.45, lambda v: {"salienceThreshold": v}),
    "salienceK":         (5.0, 20.0,  lambda v: {"salienceK": v}),
    "gumbelScaleEac":    (5.0, 50.0,  lambda v: {"gumbelScaleEac": v}),
    "gumbelScaleUtil":   (0.005, 0.05, lambda v: {"gumbelScaleUtil": v}),
    "lifetimeJitterSd":  (0.0, 3.0,   lambda v: {"lifetimeJitterSd": v}),
    # heat-pump capex on the Milieu Centraal price level (data/heating_system_capex.csv): 0.8 a further fall
    # of market prices by a fifth, 1.3 ~ the CE Delft/Arcadis midpoint for the electric heat pump
    "capexMultHp":       (0.8, 1.3,   lambda v: {"capexMultHp": v}),
    "capexMultDh":       (0.85, 1.15, lambda v: {"capexMultDh": v}),
    # learning rate multiplier on the 10 % per doubling of both heat pumps: 0 (UK installed costs,
    # Renaldi et al. 2021) to 2.8 = 28 % (historical heat-pump prices, Weiss et al. 2008)
    "learningRateMult":  (0.0, 2.8,   lambda v: {"learningRateMult": v}),
    "socialLearningMult": (0.5, 2.0,  lambda v: {"socialLearningMult": v}),
    "energyPrice":       (0.0, 1.0,   price_map),   # coupled gas+electricity
    # years between reviews of a dwelling on district heating (no end of life there); default 5
    "dhReviewYears":     (1.0, 10.0,  lambda v: {"dhReviewYears": int(round(v))}),
    # non-monetary barrier to leaving DH, EUR one-off; default 0 (cost only). 3000 ~ P90 and
    # 6500 ~ P99 of the barrier DH households need before leaving stops paying
    "dhExitBarrier":     (0.0, 6500.0, lambda v: {"dhExitBarrier": v}),
    # behaviour (hard-coded before; defaults = the model as it was)
    "sustainabilityHhp": (2.0, 4.0,   lambda v: {"sustainabilityHhp": v}),       # data 3 (scale 1-5)
    "attitudeAlpha":     (3.0, 7.0,   lambda v: {"attitudeAlpha": v}),           # Beta(a,2): mean 0.60-0.78
    "effortSwitchMult":  (0.5, 1.25,  lambda v: {"effortSwitchMult": v}),        # switching effort 0.5/0.8
    "opportunityFraction": (0.6, 0.9, lambda v: {"opportunityFraction": v}),     # data 0.75
    "networkSize":       (10.0, 50.0, lambda v: {"networkSize": int(round(v))}), # data 25
    "networkShareSimilar": (0.5, 0.95, lambda v: {"networkShareSimilar": v}),    # data 0.8
    # the two heat pumps separately (group 'hp_split'; NOT in the default screen): capex on top of
    # capexMultHp, and each one's learning rate per doubling (data: 0.10 for both)
    "capexMultHhp":      (0.7, 1.3,   lambda v: {"capexMultHhp": v}),
    "capexMultEhp":      (0.7, 1.6,   lambda v: {"capexMultEhp": v}),
    "learningRateHhp":   (0.0, 0.25,  lambda v: {"learningRateHhp": v}),
    "learningRateEhp":   (0.0, 0.25,  lambda v: {"learningRateEhp": v}),
}
HP_SPLIT = ["capexMultHhp", "capexMultEhp", "learningRateHhp", "learningRateEhp"]

# NOTE on socialLearningMult. It scales the subjective-norm weight: wSn = slf * wSnBase, and only
# the PRODUCT enters the intention. wSnBase is itself a calibrated weight, and the retained ensemble
# already spans it 0.104-0.438 (a 4.2x range). Sweeping slf over the SCENARIO interval [0.5, 2] is
# therefore not an independent uncertainty: at the ensemble median it moves wSn over 0.137-0.547,
# largely retracing ground the ensemble already covers. It is included because the paper treats the
# social channel as a scenario LEVER -- "how far could an intervention strengthen or weaken peer
# influence" -- on the same footing as the price arms and the economic learning rate, not as an
# unknown to be propagated. Reported mu* must be read that way, and it partly double-counts the
# ensemble spread that the screen averages over.

# Named subsets for --factors. "structural" is the set the scenarios do NOT explore: energy price and
# the learning rate are scenario arms, and heat-pump / district-heating capex has no sourced uncertainty
# range, so quantifying it here would report a rank that follows from an assumed interval.
FACTOR_GROUPS = {
    "all":        [k for k in FACTORS if k not in HP_SPLIT],
    # the hybrid-versus-electric split: which of capex and learning, and for which heat pump, decides it
    "hp_split":   HP_SPLIT,
    # The scenario levers: quantities with an external range and a policy reading, carried as
    # scenario arms rather than propagated as uncertainty. Screening them together is what
    # justifies that split.
    "levers":     ["energyPrice", "learningRateMult", "socialLearningMult",
                   "capexMultHp", "capexMultDh"],
    # The structural set: modelling choices with no external anchor. Varying these says how much
    # the result depends on assumptions that cannot be observed.
    "structural": ["gumbelScaleUtil", "gumbelScaleEac", "salienceThreshold",
                   "salienceSteepness", "salienceK", "lifetimeJitterSd", "dhReviewYears",
                   "dhExitBarrier"] + ["sustainabilityHhp", "attitudeAlpha", "effortSwitchMult",
                   "opportunityFraction", "networkSize", "networkShareSimilar"],
    # behavioural parameters outside the calibrated weights
    "behaviour":  ["sustainabilityHhp", "attitudeAlpha", "effortSwitchMult", "opportunityFraction",
                   "networkSize", "networkShareSimilar"],
}

# Readable names for reporting. Keys are the -Dht flags; values are what a reader should see.
FACTOR_LABEL = {
    "capexMultHp":       "Heat-pump capital cost",
    "capexMultDh":       "District-heating connection cost",
    "energyPrice":       "Energy price path (gas + electricity)",
    "learningRateMult":  "Economic learning rate",
    "socialLearningMult": "Social learning strength",
    "gumbelScaleUtil":   "Choice randomness (utility)",
    "gumbelScaleEac":    "Choice randomness (cost)",
    "salienceThreshold": "Salience: novelty threshold",
    "salienceK":         "Salience: novelty steepness",
    "salienceSteepness": "Salience: momentum steepness",
    "lifetimeJitterSd":  "Equipment lifetime spread",
    "dhReviewYears":     "District-heating review interval",
    "dhExitBarrier":     "District-heating exit barrier",
    "sustainabilityHhp": "Hybrid heat pump: perceived sustainability",
    "attitudeAlpha":     "Attitude distribution (mean)",
    "effortSwitchMult":  "Effort of switching",
    "opportunityFraction": "Opportunity trigger (share of lifetime)",
    "networkSize":       "Peer network size",
    "networkShareSimilar": "Peer network homophily",
    "capexMultHhp":      "Hybrid heat-pump capital cost",
    "capexMultEhp":      "Electric heat-pump capital cost",
    "learningRateHhp":   "Hybrid heat-pump learning rate",
    "learningRateEhp":   "Electric heat-pump learning rate",
}

def scope_tag(scope):
    s = scope.strip().lower()
    if s in ("nl", "netherlands"): return "nl"
    kind, name = s.split(":", 1)
    slug = name.strip().lower().replace(" ", "-")
    return slug if kind == "province" else "gemeente_" + slug

def get_weights(path, which):
    """which = 'central' (the central retained set, select_subsample.screen_indices; default),
    'median' (marginal median of the retained ensemble: synthetic, for comparison only), a
    representative key (best_fit / low_shareAffordability / high_shareAffordability), or
    'retained:<i>'."""
    d = json.load(open(path))
    ret = d.get("retained") or d.get("samples")
    if which == "central":
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from select_subsample import screen_indices
        _, c, src = screen_indices(path)
        return dict(ret[c]["weights"]), f"central set retained[{c}] ({src})"
    if which == "median":
        keys = list(ret[0]["weights"])
        return {k: float(np.median([e["weights"][k] for e in ret])) for k in keys}, "retained-ensemble median"
    reps = d.get("representative", {})
    if which in reps:
        return dict(reps[which]["weights"]), which
    if which.startswith("retained:"):
        i = int(which.split(":", 1)[1]); return dict(ret[i]["weights"]), f"retained[{i}]"
    sys.exit(f"unknown --weights '{which}'. Use central, median, {', '.join(reps)}, or retained:<i>")

def dflags_for(x_actual):
    """x_actual: {factor: actual value} -> merged -Dht dict."""
    out = {}
    for f, v in x_actual.items():
        out.update(FACTORS[f][2](v))
    return out

def run_point(scope, base_weights, x_actual, iterations, xmx, scenario):
    stock = os.path.join(STOCK, f"{scope_tag(scope)}_dwellings.csv")
    if not os.path.exists(stock):
        sys.exit(f"stock not found: {stock} (provision with model/run.py first)")
    out = os.path.join(tempfile.gettempdir(), f"struct_{os.getpid()}_{np.random.randint(1e9)}.csv")
    props = {**base_weights, **dflags_for(x_actual)}
    cmd = ["java", f"-Xmx{xmx}"] + [f"-Dht.{k}={v}" for k, v in props.items()]
    cmd += ["-cp", ENGINE_CP, "heattransition.Cli", "--real", stock, "--scenario", scenario,
            "--iterations", str(iterations), "--start", "2024", "--end", "2050", "--out", out]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[-1500:], file=sys.stderr); sys.exit("engine run failed")
    acc = {s: 0.0 for s in SYSTEMS}
    with open(out, newline="") as f:
        for row in csv.DictReader(f):
            if row["ownership"] == "TOTAL" and int(float(row["year"])) == 2050:
                acc[row["heating_system"]] += float(row["installed_current"])
    os.remove(out)
    tot = sum(acc.values()) or 1.0
    mix = {s: 100.0 * acc[s] / tot for s in SYSTEMS}
    mix["HEATPUMP"] = mix["HYBRID_HEAT_PUMP"] + mix["ELECTRIC_HEAT_PUMP"]
    mix["NONGAS"] = 100.0 - mix["NATURAL_GAS_BOILER"] - mix["NATURAL_GAS_BLOCK"]
    return mix

# The objective is the full 2050 MODAL SPLIT (the 4 technologies that carry share). Morris/LHS report
# a factor x technology sensitivity matrix, plus a TOTAL = sum over technologies of |effect| (L1) that
# captures how much a factor reshuffles the WHOLE mix -- so a factor that swaps hybrid<->electric
# without touching gas still registers. --sort picks which column ranks the table.
MIX_KEYS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
SHORT = {"NATURAL_GAS_BOILER": "gas", "HYBRID_HEAT_PUMP": "hybrid",
         "ELECTRIC_HEAT_PUMP": "electric", "DISTRICT_HEATING": "dh"}
SORTCOL = {"total": None, "gas": "NATURAL_GAS_BOILER", "hybrid": "HYBRID_HEAT_PUMP",
           "electric": "ELECTRIC_HEAT_PUMP", "dh": "DISTRICT_HEATING"}

def scale(names, unit_vec):
    return {n: FACTORS[n][0] + unit_vec[i] * (FACTORS[n][1] - FACTORS[n][0]) for i, n in enumerate(names)}

def do_morris(a, names, w):
    k = len(names); rng = np.random.default_rng(a.seed)
    delta = a.levels / (2.0 * (a.levels - 1))
    ee = {n: {t: [] for t in MIX_KEYS} for n in names}; runs = 0
    for tr in range(a.trajectories):
        base = rng.integers(0, a.levels // 2, size=k) / (a.levels - 1.0)
        order = rng.permutation(k); x = base.copy()
        m_prev = run_point(a.scope, w, scale(names, x), a.iterations, a.xmx, a.scenario); runs += 1
        for j in order:
            x2 = x.copy(); x2[j] = x2[j] + delta if x2[j] + delta <= 1.0 else x2[j] - delta
            m = run_point(a.scope, w, scale(names, x2), a.iterations, a.xmx, a.scenario); runs += 1
            for t in MIX_KEYS:
                ee[names[j]][t].append((m[t] - m_prev[t]) / (x2[j] - x[j]))
            x, m_prev = x2, m
        print(f"  trajectory {tr+1}/{a.trajectories} ({runs} runs)", flush=True)
    # per (factor x technology) mu*, and a TOTAL = sum_t |mu*|
    mus = {n: {t: float(np.mean(np.abs(ee[n][t]))) for t in MIX_KEYS} for n in names}
    total = {n: sum(mus[n].values()) for n in names}
    col = SORTCOL[a.sort]
    order = sorted(names, key=lambda n: -(total[n] if col is None else mus[n][col]))
    print(f"\nStructural Morris — 2050 modal split, {a.scenario}, {runs} runs, weights={a.weights}")
    print(f"  mu* per factor x technology (%pt change per full-range move); TOTAL = sum|mu*| over the mix")
    print(f"  {'factor':<20}" + "".join(f"{SHORT[t]:>10}" for t in MIX_KEYS) + f"{'TOTAL':>10}")
    for n in order:
        print(f"  {n:<20}" + "".join(f"{mus[n][t]:>10.2f}" for t in MIX_KEYS) + f"{total[n]:>10.2f}")
    if a.outdir:
        os.makedirs(a.outdir, exist_ok=True)
        json.dump({"scenario": a.scenario, "runs": runs,
                   "mu_star": {n: mus[n] for n in names}, "total": total},
                  open(os.path.join(a.outdir, f"structural_morris_{a.weights.replace(':','')}.json"), "w"), indent=2)
        print(f"\nwrote structural_morris_{a.weights.replace(':','')}.json")

def do_lhs(a, names, w):
    k = len(names); rng = np.random.default_rng(a.seed)
    if a.iterations < 5:
        print(f"[warn] lhs with --iterations {a.iterations}: the 2050 mix carries ~0.4 %pt MC noise per\n"
              f"       share, which attenuates SRC toward 0. Use 5-10 for quantification.\n")
    cut = (np.arange(a.samples)[:, None] + rng.random((a.samples, k))) / a.samples
    for j in range(k): cut[:, j] = rng.permutation(cut[:, j])
    rows = []
    for i in range(a.samples):
        x = cut[i]; mix = run_point(a.scope, w, scale(names, x), a.iterations, a.xmx, a.scenario)
        rows.append((x.copy(), mix))
        if (i + 1) % 10 == 0: print(f"  {i+1}/{a.samples}", flush=True)
    X = np.array([r[0] for r in rows]); Xs = (X - X.mean(0)) / (X.std(0) + 1e-12)
    print(f"\nStructural LHS — 2050 modal split, {a.samples} samples, {a.scenario}, weights={a.weights}")
    print(f"  standardized regression coefficients (|SRC| = share of 2050 variance; sign = direction)")
    print(f"  {'factor':<20}" + "".join(f"{SHORT[t]:>10}" for t in MIX_KEYS))
    src = {}
    r2 = {}
    for t in MIX_KEYS:
        Y = np.array([r[1][t] for r in rows]); Ys = (Y - Y.mean()) / (Y.std() + 1e-12)
        b, *_ = np.linalg.lstsq(Xs, Ys, rcond=None); src[t] = b
        r2[t] = 1 - ((Ys - Xs @ b) ** 2).sum() / ((Ys ** 2).sum() + 1e-12)
    agg = {n: sum(abs(src[t][i]) for t in MIX_KEYS) for i, n in enumerate(names)}
    for n in sorted(names, key=lambda n: -agg[n]):
        i = names.index(n)
        print(f"  {n:<20}" + "".join(f"{src[t][i]:>10.2f}" for t in MIX_KEYS))
    print(f"  {'model R^2':<20}" + "".join(f"{r2[t]:>10.2f}" for t in MIX_KEYS))
    if a.outdir:
        os.makedirs(a.outdir, exist_ok=True)
        p = os.path.join(a.outdir, f"structural_lhs_{a.weights.replace(':','')}.csv")
        with open(p, "w", newline="") as fh:
            wr = csv.writer(fh); wr.writerow(names + list(MIX_KEYS))
            for x, mix in rows:
                wr.writerow([f"{FACTORS[names[j]][0]+x[j]*(FACTORS[names[j]][1]-FACTORS[names[j]][0]):.5f}" for j in range(k)]
                            + [f"{mix[t]:.3f}" for t in MIX_KEYS])
        print(f"\nwrote {p}  (for Sobol / partial-dependence post-hoc)")

def ensure_built(skip):
    """Rebuild the engine so a screen never runs on stale classes (the -Dht flags of a source change
    are silently ignored by an old build -> factors show mu*=0). Uses the Gradle wrapper."""
    if skip:
        print("[struct] --skip-build: using existing classes"); return
    gw = os.path.join(ENGINE, "gradlew.bat" if os.name == "nt" else "gradlew")
    if not os.path.exists(gw):
        print("[struct] no gradle wrapper; using existing classes"); return
    print("[struct] building engine (gradlew build)...", flush=True)
    if subprocess.run([gw, "build"], cwd=ENGINE).returncode != 0:
        sys.exit("[struct] build failed -- fix before screening")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["morris", "lhs"])
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--sort", default="total", choices=list(SORTCOL),
                    help="which column ranks the factor table (default: total = whole-mix influence)")
    ap.add_argument("--factors", default=",".join(k for k in FACTORS if k not in HP_SPLIT), help="comma list, or a named group: " + "|".join(FACTOR_GROUPS) +
                         ". 'structural' is everything the scenarios do not explore.")
    ap.add_argument("--weights", default="central",
                    help="weight set to hold fixed: central (default) | best_fit | "
                         "high_shareAffordability | low_shareAffordability | retained:<i> | median "
                         "(synthetic, comparison only)")
    ap.add_argument("--weights-file", default=None,
                    help="calibration search JSON to take --weights from. Default: the most recently\n"
                         "written calibration_search.json under results/. A fixed default goes stale\n"
                         "the moment a new calibration is run, and the screen then silently sits at a\n"
                         "different point in weight space than everything else.")
    ap.add_argument("--iterations", type=int, default=1,
                    help="MC iters per point. morris ranking is robust at 1 (effects >> the ~0.4%%pt "
                         "2050 noise); lhs quantification needs 5-10.")
    ap.add_argument("--trajectories", type=int, default=8, help="morris trajectories")
    ap.add_argument("--levels", type=int, default=4, help="morris grid levels")
    ap.add_argument("--samples", type=int, default=80, help="lhs samples")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--xmx", default="8g")
    ap.add_argument("--skip-build", action="store_true", help="skip the gradlew rebuild (only if just built)")
    ap.add_argument("--outdir", default=None)
    a = ap.parse_args()
    if not a.weights_file:
        import glob as _glob
        c = _glob.glob(os.path.join(ROOT, "results", "*", "calibration_search.json"))
        if not c: sys.exit("[struct] no calibration_search.json under results/")
        a.weights_file = max(c, key=os.path.getmtime)
    print(f"weights file: {os.path.relpath(a.weights_file, ROOT)}")
    ensure_built(a.skip_build)
    if not os.path.exists(os.path.join(ENGINE_CP, "heattransition", "Cli.class")):
        sys.exit(f"engine not built at {ENGINE_CP} — run `gradlew build` in model/engine-java first")
    names = (list(FACTOR_GROUPS[a.factors]) if a.factors in FACTOR_GROUPS
             else [n.strip() for n in a.factors.split(",") if n.strip()])
    bad = [n for n in names if n not in FACTORS]
    if bad: sys.exit(f"unknown factor(s): {bad}. Valid: {list(FACTORS)}")
    w, wlabel = get_weights(a.weights_file, a.weights)
    print(f"weights held at {wlabel}: " + ", ".join(f"{k}={v:.3f}" for k, v in w.items()))
    print(f"factors ({len(names)}): {names}\n")
    (do_morris if a.mode == "morris" else do_lhs)(a, names, w)

if __name__ == "__main__":
    main()
