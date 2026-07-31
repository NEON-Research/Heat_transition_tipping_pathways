#!/usr/bin/env python3
"""
Calibrate / screen the TPB decision weights against observed 2022-2024 adoption (CBS maatwerk).

WHY ONLY 2022-2024: CBS changed method before 2022, so this is the only internally consistent
window. It is therefore used as a **broad plausibility range**, not a precise fit target: we keep
weight sets that reproduce the observed direction+magnitude of change within a tolerance, rather
than optimising to a single "best" point (weights are non-identifiable from 3 years of data).

Setup: initialise the engine in 2022 from the OBSERVED 2022 state (-Dht.heatingYear=2022, reading the
gasCV_2022..dh_2022 columns of the combined neighborhoods.csv), run to 2024, and score the simulated
2023 AND 2024 technology mixes against observed, for the calibration region.
The engine is rebuilt first (--skip-build to opt out) so a sweep never uses stale classes.

Modes
  evaluate  one weight set (default = AL defaults) -> error
  search    random / Latin-hypercube sample of weight space -> ranked table + retained ensemble
  morris    Morris elementary effects -> which weights matter most (uses the baseline weights)

What is varied: the TPB formulas are NORMALISED weighted averages, so we sample the SHARES within
each group (they sum to 1) rather than raw weights -- 4 free parameters, directly interpretable, and
a Morris step means the same thing everywhere in the space:
  shareAttitude, shareSocialnorm   -> sharePbc    = 1 - the two   (intention group)
  shareAffordability               -> shareEffort = 1 - it        (PBC group)
  shareIntention                   -> sharePbcBeh = 1 - it        (behaviour group)
Defaults reproduce the AnyLogic weights exactly (1/3 each; 0.714/0.286; 0.5/0.5).
HT_RAW_WEIGHTS=1 falls back to sampling raw weights with a fixed numeraire.

Examples
  python calibrate_weights.py evaluate --scope province:Noord-Brabant
  python calibrate_weights.py search   --scope province:Noord-Brabant --samples 60 --iterations 2
  python calibrate_weights.py morris   --scope province:Noord-Brabant --levels 4 --trajectories 8
"""
import argparse
import csv
import itertools
import json
import os
import subprocess
import sys
import tempfile

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
MODEL = os.path.join(ROOT, "model")
REF = os.path.join(MODEL, "data", "reference")
STOCK = os.path.join(MODEL, "data", "stock")
ENGINE = os.path.join(MODEL, "engine-java")
ENGINE_CP = os.environ.get("HT_ENGINE_CP",
                           os.path.join(MODEL, "engine-java", "build", "classes", "java", "main"))

SYSTEMS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP",
           "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
OBS_KEY = {"NATURAL_GAS_BOILER": "gasCV", "NATURAL_GAS_BLOCK": "gasBlock",
           "HYBRID_HEAT_PUMP": "hhp", "ELECTRIC_HEAT_PUMP": "ehp", "DISTRICT_HEATING": "dh"}

# ---------------------------------------------------------------------------------------------
# WEIGHTS TO VARY -- sampled as NORMALISED SHARES (compositional parameterisation).
#
# Every TPB formula in the engine is a normalised weighted average, so only the shares within each
# group matter:
#     intention        = (att*wAtt + sn*wSn*rate + pbc*wPbc) / (wAtt + wSn + wPbc)
#     pbc              = ((1-eacNorm)*wAff + (1-effort)*wEff)  / (wAff + wEff)
#     perceivedUtility = (intention*wInt + pbc*wPbcBeh)        / (wInt + wPbcBeh)
#
# We therefore sample the SHARES directly (each group sums to 1). Two advantages over sampling raw
# weights:
#   1. A Morris step of e.g. 0.05 always means the same thing. With raw weights the same absolute
#      step is a huge compositional change when the weights sum to 0.6 and a tiny one when they sum
#      to 3.0 -- which is why the first screen came back with sigma > mu* for every factor.
#   2. Shares are what TPB actually interprets ("attitude 25 %, norm 50 %, control 25 %"), so the
#      sampled parameter, the engine input and the reported number are all the same quantity --
#      shares are passed straight through as weights and the engine's normalisation is a no-op.
#
# Degrees of freedom: a group of k shares has k-1 free dimensions, so the LAST member of each group
# is the residual (1 - the others). Four sampled parameters in total:
#     intention group : shareAttitude, shareSocialnorm   -> sharePbc      = residual
#     pbc group       : shareAffordability               -> shareEffort   = residual
#     behaviour group : shareIntention                   -> sharePbcBeh   = residual
#
# NOTE the effects within a group are inherently RELATIVE: raising attitude's share necessarily
# lowers the others. An elementary effect here reads "reallocate weight toward X (away from the
# residual)" -- which is the meaningful behavioural question, but it does mean the within-group
# effects are linearly dependent and should be read as a ranking, not as independent contributions.
#
# HT_RAW_WEIGHTS=1 falls back to sampling raw (unnormalised) weights, for comparison.
# ---------------------------------------------------------------------------------------------
SHARE_MODE = not os.environ.get("HT_RAW_WEIGHTS")
SEED_OFFSET = [0]        # set from --seed-offset in main(); engine world-seed shift

# sampled share -> (default, low, high)
WEIGHTS = {
    "shareAttitude":      (1/3, 0.15, 0.60),   # intention group; residual = sharePbc
    "shareSocialnorm":    (1/3, 0.15, 0.60),
    "shareAffordability": (0.5/0.7, 0.30, 0.90),  # pbc group; residual = shareEffort
    "shareIntention":     (0.5, 0.20, 0.80),   # behaviour group; residual = sharePbcBeh
}
MIN_RESIDUAL = 0.10          # keep every implied share meaningfully positive
FIXED = {}

if not SHARE_MODE:           # legacy: sample raw weights with a fixed numeraire per group
    FIXED = {"wAttitudeToIntention": 0.5, "wAffordabilityToPbc": 0.5}
    WEIGHTS = {
        "wSocialnormToIntention": (0.5, 0.1, 2.0),
        "wPbcToIntention":        (0.5, 0.1, 2.0),
        "wEffortToPbc":           (0.2, 0.05, 1.5),
        "wIntentionToBehavior":   (0.5, 0.1, 2.0),
    }


def shares_to_weights(p):
    """Map sampled shares -> engine weights. Each group is renormalised to sum to 1 (the engine
    divides by the same sum, so passing shares is exact). Returns engine -Dht.<name> values."""
    if not SHARE_MODE:
        return dict(p)
    a, sn = p["shareAttitude"], p["shareSocialnorm"]
    if a + sn > 1.0 - MIN_RESIDUAL:                 # keep the residual (sharePbc) positive
        scale = (1.0 - MIN_RESIDUAL) / (a + sn)
        a, sn = a * scale, sn * scale
    pbc = 1.0 - a - sn
    aff = min(max(p["shareAffordability"], MIN_RESIDUAL), 1.0 - MIN_RESIDUAL)
    inten = min(max(p["shareIntention"], MIN_RESIDUAL), 1.0 - MIN_RESIDUAL)
    return {
        "wAttitudeToIntention": round(a, 6),
        "wSocialnormToIntention": round(sn, 6),
        "wPbcToIntention": round(pbc, 6),
        "wAffordabilityToPbc": round(aff, 6),
        "wEffortToPbc": round(1.0 - aff, 6),
        "wIntentionToBehavior": round(inten, 6),
        "wPbcToBehavior": round(1.0 - inten, 6),
    }


# ---------------------------------------------------------------- observed targets

def scope_tag(scope):
    s = scope.strip()
    if s.lower() in ("nl", "all", "netherlands"):
        return "nl"
    kind, name = s.split(":", 1)
    slug = name.lower().replace(" ", "_")
    return slug if kind.lower() == "province" else "gemeente_" + slug


def region_buurten(scope):
    """buurtcode -> number of dwellings in the region's stock (for dwelling-weighted aggregation)."""
    path = stock_path(scope)
    if not os.path.exists(path):
        sys.exit(f"stock CSV not found: {path}\nProvision it first: python model/run.py --scope {scope} ...")
    counts = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            c = row.get("buurtcode")
            if c:
                counts[c] = counts.get(c, 0) + 1
    return counts


def stock_path(scope, calib=False):
    tag = scope_tag(scope) + ("_calib" if calib else "")
    return os.path.join(STOCK, f"{tag}_dwellings.csv")


def complete_buurten(years=(2022, 2024)):
    """buurtcodes whose CBS data is complete in ALL given years."""
    path = os.path.join(REF, "observed_heating_by_year.csv")
    if not os.path.exists(path):
        sys.exit("observed_heating_by_year.csv missing -> run model/data-export/scripts/export_observed_heating.py")
    seen = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["complete"] == "1" and int(r["year"]) in years:
                seen.setdefault(r["buurtcode"], set()).add(int(r["year"]))
    return {b for b, ys in seen.items() if ys >= set(years)}


def ensure_buurt_filter(years=(2022, 2024)):
    """Write (once) the list of buurten with complete CBS data in all `years`. The ENGINE reads this
    via -Dht.buurtFilter and simulates only those neighbourhoods, so it covers exactly the buurten the
    observed aggregate is computed over. (Doing it engine-side avoids duplicating the stock CSV --
    the NL stock is ~790 MB.) Without this, the ~30 % of buurten with suppressed CBS cells would be
    simulated but absent from the observed average: an apples-to-oranges comparison."""
    path = os.path.join(REF, "calibration_buurten.txt")
    keep = sorted(complete_buurten(years))
    if not (os.path.exists(path) and len(open(path).read().splitlines()) - 1 == len(keep)):
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"# buurten with complete CBS data in {'+'.join(map(str, years))} "
                    f"(generated by calibrate_weights.py)\n")
            f.write("\n".join(keep) + "\n")
        print(f"[calib] buurt filter: {len(keep):,} CBS-complete neighbourhoods -> {os.path.basename(path)}")
    return path


def observed_mix(scope, year):
    """Observed share per system for the region at `year`, **dwelling-weighted** using the model's
    per-buurt dwelling counts (the engine's TOTAL is dwelling-weighted, so an unweighted mean over
    buurten would not be comparable). Restricted to buurten complete in the calibration years."""
    counts = region_buurten(scope)
    keep = complete_buurten()
    path = os.path.join(REF, "observed_heating_by_year.csv")
    tot = {s: 0.0 for s in SYSTEMS}
    dwellings = 0
    nb = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            b = r["buurtcode"]
            if r["year"] != str(year) or b not in keep or b not in counts:
                continue
            w = counts[b]
            for s in SYSTEMS:
                tot[s] += float(r[OBS_KEY[s]]) * w
            dwellings += w
            nb += 1
    if nb == 0:
        sys.exit(f"no complete observed rows for {scope} in {year}")
    ssum = sum(tot.values()) or 1.0
    return {s: 100.0 * v / ssum for s, v in tot.items()}, nb     # renormalised (drops 'onbekend')


def ensure_built(skip=False):
    """Compile the engine before a sweep. Without this the script would run whatever is in
    build/classes/java/main, which silently produces results from an older model version (or fails,
    e.g. looking for a reference file that has since been retired)."""
    if skip:
        print("[calib] --skip-build: using already-compiled classes")
        return
    gw = os.path.join(ENGINE, "gradlew.bat" if os.name == "nt" else "gradlew")
    if not os.path.exists(gw):
        print("[calib] no Gradle wrapper found; using already-compiled classes")
        return
    print("[calib] building engine (gradlew build)...")
    if subprocess.run([gw, "build"], cwd=ENGINE).returncode != 0:
        sys.exit("[calib] engine build failed -- fix before calibrating")


# ---------------------------------------------------------------- engine run

def run_engine(scope, weights, iterations, start, end, xmx="6g", years=None):
    """Run the engine with the given weights; return {year: mix(%)} for each year in `years`
    (default: every year from start to end, so the objective can score 2023 AND 2024)."""
    stock = stock_path(scope)
    bfilter = ensure_buurt_filter()
    out = os.path.join(tempfile.gettempdir(), f"calib_{os.getpid()}.csv")
    cmd = ["java", f"-Xmx{xmx}",
           f"-Dht.seedOffset={SEED_OFFSET[0]}",
           f"-Dht.heatingYear={start}",
           f"-Dht.buurtFilter={bfilter}"]      # heatingYear picks the observed start-year state
    cmd += [f"-Dht.{k}={v}" for k, v in {**FIXED, **shares_to_weights(weights)}.items()]
    cmd += ["-cp", ENGINE_CP, "heattransition.Cli",
            "--real", stock, "--scenario", "baseline",
            "--iterations", str(iterations), "--start", str(start), "--end", str(end),
            "--out", out]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout[-2000:], r.stderr[-2000:], file=sys.stderr)
        sys.exit("engine run failed")
    # TOTAL mix per requested year, summed over iterations
    wanted = set(years) if years else set(range(start, end + 1))
    acc = {y: {s: 0.0 for s in SYSTEMS} for y in wanted}
    with open(out, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["ownership"] != "TOTAL":
                continue
            y = int(row["year"])
            if y in wanted:
                acc[y][row["heating_system"]] += float(row["installed_current"])
    os.remove(out)
    norm = lambda d: {k: 100.0 * v / (sum(d.values()) or 1.0) for k, v in d.items()}
    return {y: norm(d) for y, d in acc.items()}


def objective_per_iteration(scope, weights, a, iterations=None):
    """Objective computed SEPARATELY for each Monte-Carlo iteration (each = one random world), so the
    between-world standard deviation can be estimated. Needed for the replication analysis."""
    n = iterations or a.iterations
    ys = scored_years(a.start, a.end)
    stock = stock_path(scope)
    bfilter = ensure_buurt_filter()
    out = os.path.join(tempfile.gettempdir(), f"calibit_{os.getpid()}.csv")
    cmd = ["java", f"-Xmx{a.xmx}", f"-Dht.heatingYear={a.start}", f"-Dht.buurtFilter={bfilter}",
           f"-Dht.seedOffset={a.seed_offset}"]
    cmd += [f"-Dht.{k}={v}" for k, v in {**FIXED, **shares_to_weights(weights)}.items()]
    cmd += ["-cp", ENGINE_CP, "heattransition.Cli", "--real", stock, "--scenario", "baseline",
            "--iterations", str(n), "--start", str(a.start), "--end", str(a.end), "--out", out]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout[-2000:], r.stderr[-2000:], file=sys.stderr); sys.exit("engine run failed")
    acc = {}
    with open(out, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["ownership"] != "TOTAL":
                continue
            y, it = int(row["year"]), int(row["iteration"])
            if y in ys:
                acc.setdefault((it, y), {s2: 0.0 for s2 in SYSTEMS})[row["heating_system"]] += float(row["installed_current"])
    os.remove(out)
    per_it = []
    for it in sorted({k[0] for k in acc}):
        mads = []
        for y in ys:
            d = acc[(it, y)]; tot = sum(d.values()) or 1.0
            mads.append(error({k: 100.0 * v / tot for k, v in d.items()}, observed_mix(scope, y)[0]))
        per_it.append(float(np.mean(mads)))
    return per_it


def check_start_state(scope, iterations, start, end, xmx, tol=2.0):
    """Sanity gate: the simulated START-year mix must resemble the observed one, otherwise the
    residual we calibrate on is contaminated by an initialisation artefact rather than behaviour.
    (Utrecht showed a +4 %pt district-heating overshoot this way; Noord-Brabant is clean.)"""
    obs0, _ = observed_mix(scope, start)
    w = {k: v[0] for k, v in WEIGHTS.items()}
    sim0 = run_engine(scope, w, iterations, start, end, xmx, years=[start])[start]
    print(f"\nStart-year ({start}) initialisation check — simulated vs observed:")
    worst = 0.0
    for s in SYSTEMS:
        d = sim0[s] - obs0[s]
        worst = max(worst, abs(d))
        flag = "  <-- check" if abs(d) > tol else ""
        print(f"  {s:<22}{obs0[s]:>7.1f}{sim0[s]:>8.1f}{d:>+7.1f}{flag}")
    if worst > tol:
        print(f"  WARNING: start-year deviation {worst:.1f} %pts > {tol} — the calibration residual "
              f"may be an initialisation artefact, not behaviour. Consider another region.")
    else:
        print(f"  OK: all systems within {tol} %pts at the start year.")
    return worst


FOCUS = ("NATURAL_GAS_BOILER", "ELECTRIC_HEAT_PUMP", "HYBRID_HEAT_PUMP")


def error(sim, obs, focus=FOCUS):
    """Mean absolute deviation (%pts) for one year. `focus` = the systems that move in 2022-24."""
    return float(np.mean([abs(sim[s] - obs[s]) for s in focus]))


def scored_years(start, end):
    """Years the objective is scored on: everything AFTER the initialisation year."""
    return list(range(start + 1, end + 1))


def objective(scope, weights, a):
    """Mean MAD across all scored years (2023 and 2024) -- uses the whole observed trajectory,
    not just the end point, so a run that overshoots then corrects is not rewarded."""
    ys = scored_years(a.start, a.end)
    sim = run_engine(scope, weights, a.iterations, a.start, a.end, a.xmx, years=ys)
    return float(np.mean([error(sim[y], observed_mix(scope, y)[0]) for y in ys]))


# ---------------------------------------------------------------- modes

def do_evaluate(a):
    w = {k: v[0] for k, v in WEIGHTS.items()}
    ys = scored_years(a.start, a.end)
    sim = run_engine(a.scope, w, a.iterations, a.start, a.end, a.xmx, years=ys)
    print(f"\nInitialised from observed {a.start}; scored on {', '.join(map(str, ys))} "
          f"(AL default weights):")
    mads = []
    for y in ys:
        obs, n = observed_mix(a.scope, y)
        e = error(sim[y], obs); mads.append(e)
        print(f"\n  {y}  ({n} buurten)      observed  simulated     diff")
        for sname in SYSTEMS:
            mark = " *" if sname in FOCUS else "  "
            print(f"  {sname:<22}{obs[sname]:>10.1f}{sim[y][sname]:>11.1f}{sim[y][sname]-obs[sname]:>+9.1f}{mark}")
        print(f"  MAD (scored systems *): {e:.2f} %pts")
    print(f"\nOBJECTIVE = mean MAD over {len(ys)} scored years: {np.mean(mads):.2f} %pts")
    if not a.no_start_check:
        check_start_state(a.scope, a.iterations, a.start, a.end, a.xmx, a.start_tol)


def sample_lhs(rng, n):
    """Latin-hypercube sample in the weight box."""
    keys = list(WEIGHTS)
    cut = (np.arange(n)[:, None] + rng.random((n, len(keys)))) / n
    for j in range(len(keys)):
        rng.shuffle(cut[:, j])
    out = []
    for i in range(n):
        out.append({k: WEIGHTS[k][1] + cut[i, j] * (WEIGHTS[k][2] - WEIGHTS[k][1])
                    for j, k in enumerate(keys)})
    return out


def do_search(a):
    """LHS over the share space -> history matching: retain the sets whose 2023+2024 trajectory is
    within `--tolerance` of observed, report the retained ranges, and pick a few REPRESENTATIVE sets
    to carry into the full 2024-2050 scenario runs (so the pathway band costs N runs, not `samples`)."""
    if not a.no_start_check:
        check_start_state(a.scope, a.iterations, a.start, a.end, a.xmx, a.start_tol)
    rng = np.random.default_rng(a.seed)
    samples = sample_lhs(rng, a.samples)
    keys = list(WEIGHTS)

    rows = []
    for i, p in enumerate(samples, 1):
        e = objective(a.scope, p, a)
        eff = shares_to_weights(p)              # effective shares after the residual/bounds fix-up
        rows.append({"mad": e, "sampled": dict(p), "weights": eff})
        print(f"  [{i}/{len(samples)}] MAD={e:6.2f}  "
              + " ".join(f"{k.replace('share','')[:4]}={p[k]:.2f}" for k in keys), flush=True)
    rows.sort(key=lambda r: r["mad"])

    base = objective(a.scope, {k: v[0] for k, v in WEIGHTS.items()}, a)
    keep = [r for r in rows if r["mad"] <= a.tolerance]
    print(f"\n{'='*78}\nAL defaults score MAD = {base:.2f} %pts")
    print(f"best sampled       MAD = {rows[0]['mad']:.2f} %pts")
    print(f"retained (MAD <= {a.tolerance}): {len(keep)}/{len(rows)} weight sets")

    if not keep:
        print("nothing retained -- widen --tolerance or the sampled ranges")
        return

    print(f"\nRetained ranges (effective shares):\n{'share':<22}{'min':>8}{'median':>9}{'max':>8}{'default':>9}")
    for w in ("wAttitudeToIntention", "wSocialnormToIntention", "wPbcToIntention",
              "wAffordabilityToPbc", "wEffortToPbc", "wIntentionToBehavior", "wPbcToBehavior"):
        v = sorted(r["weights"][w] for r in keep)
        dflt = shares_to_weights({k: x[0] for k, x in WEIGHTS.items()})[w]
        print(f"{w:<22}{v[0]:>8.3f}{v[len(v)//2]:>9.3f}{v[-1]:>8.3f}{dflt:>9.3f}")

    # --- representative sets for the 2050 scenario runs -------------------------------------
    # actual evaluated points (not synthetic medians): the best fit plus the retained extremes of
    # the dominant axis, so the pathway band spans the plausible range of that lever.
    axis = a.axis if a.axis in keys else "shareAffordability"
    by_axis = sorted(keep, key=lambda r: r["sampled"][axis])
    reps = {"best_fit": rows[0], f"low_{axis}": by_axis[0], f"high_{axis}": by_axis[-1]}
    print(f"\nRepresentative sets for the 2024-2050 runs (band over '{axis}'):")
    for name, r in reps.items():
        print(f"  {name:<26} MAD={r['mad']:.2f}  " +
              " ".join(f"{k}={v:.3f}" for k, v in r["weights"].items() if k != "wPbcToBehavior"))
        print(f"{'':28}-D flags: " + " ".join(f"-Dht.{k}={v}" for k, v in r["weights"].items()))

    if a.outdir:
        os.makedirs(a.outdir, exist_ok=True)
        out = os.path.join(a.outdir, "calibration_search.json")
        json.dump({"scope": a.scope, "tolerance": a.tolerance, "default_mad": base,
                   "samples": rows, "retained": keep, "representative": reps}, open(out, "w"), indent=1)
        print(f"\nwrote {out}")


def do_converge(a):
    """REPLICATION ANALYSIS: how many Monte-Carlo iterations (random worlds) are needed?

    Standard practice in stochastic simulation (Law & Kelton, *Simulation Modeling and Analysis*;
    for ABMs see Lorscheid et al. 2012, Secchi & Seri 2017): run n independent replications, estimate
    the between-replication standard deviation s, and choose n so the confidence-interval half-width
    on the mean is below the precision you need:

        half-width  =  t(0.975, n-1) * s / sqrt(n)        ->      n_required = (1.96 * s / target)^2

    Reported below per iteration count, together with the running mean, so both the textbook
    criterion and a simple stability check are visible."""
    w = {k: v[0] for k, v in WEIGHTS.items()}
    n_max = max(int(x) for x in a.iter_list.split(","))
    print(f"\nReplication analysis at default weights ({a.scope}, {a.start}->{a.end}, "
          f"seedOffset={a.seed_offset})")
    per_it = objective_per_iteration(a.scope, w, a, iterations=n_max)
    print(f"\nper-world objective (MAD, one value per random world): "
          + ", ".join(f"{v:.3f}" for v in per_it))
    print(f"\n{'n':>4}{'mean':>9}{'sd':>8}{'std err':>9}{'95% CI half-width':>20}")
    for n in [int(x) for x in a.iter_list.split(",")]:
        if n > len(per_it):
            continue
        v = np.array(per_it[:n]); m, sd = v.mean(), v.std(ddof=1) if n > 1 else 0.0
        se = sd / np.sqrt(n) if n > 1 else float("nan")
        hw = 1.96 * se if n > 1 else float("nan")
        print(f"{n:>4}{m:>9.3f}{sd:>8.3f}{se:>9.3f}{hw:>20.3f}")
    sd_all = np.array(per_it).std(ddof=1)
    print(f"\nBetween-world SD = {sd_all:.3f} %pts. Iterations needed for a given precision:")
    for target in (0.5, 0.25, 0.1):
        print(f"   +/- {target:>4} %pts  ->  n = {int(np.ceil((1.96 * sd_all / target) ** 2)):>4}")
    print("\nChoose the precision from what the analysis must resolve: it should be well below the\n"
          "retention --tolerance, and below the differences between weight sets you want to rank.\n"
          "NOTE this is the CALIBRATION objective (3-year run, province aggregate). A 2024-2050\n"
          "pathway is path-dependent -- learning-curve feedback amplifies early differences -- so the\n"
          "scenario runs need MORE replications. Check those on the spread of the 2050 outcome.")


def do_morris(a):
    """Morris elementary effects: mu* (mean |EE|) ranks which weights matter."""
    obs, _ = observed_mix(a.scope, a.end)
    if not a.no_start_check:
        check_start_state(a.scope, a.iterations, a.start, a.end, a.xmx, a.start_tol)
    keys = list(WEIGHTS)
    k = len(keys)
    rng = np.random.default_rng(a.seed)
    delta = a.levels / (2.0 * (a.levels - 1))
    ee = {key: [] for key in keys}
    runs = 0
    for t in range(a.trajectories):
        base = rng.integers(0, a.levels // 2, size=k) / (a.levels - 1.0)   # in [0,1]
        order = rng.permutation(k)
        x = base.copy()

        def as_w(vec):
            return {key: WEIGHTS[key][1] + vec[j] * (WEIGHTS[key][2] - WEIGHTS[key][1])
                    for j, key in enumerate(keys)}

        y_prev = objective(a.scope, as_w(x), a)
        runs += 1
        for j in order:
            x2 = x.copy()
            x2[j] = x2[j] + delta if x2[j] + delta <= 1.0 else x2[j] - delta
            y = objective(a.scope, as_w(x2), a)
            runs += 1
            ee[keys[j]].append((y - y_prev) / (x2[j] - x[j]))
            x, y_prev = x2, y
        print(f"  trajectory {t+1}/{a.trajectories} done ({runs} runs)", flush=True)

    print(f"\nMorris elementary effects ({a.trajectories} trajectories, {runs} model runs)")
    print(f"{'weight':<26}{'mu*':>9}{'mu':>9}{'sigma':>9}   (mu* = influence, sigma = interaction/non-linearity)")
    stats = []
    for key in keys:
        v = np.array(ee[key])
        stats.append((key, float(np.mean(np.abs(v))), float(np.mean(v)), float(np.std(v))))
    for key, mus, mu, sd in sorted(stats, key=lambda s: -s[1]):
        print(f"{key:<26}{mus:>9.2f}{mu:>9.2f}{sd:>9.2f}")
    if a.outdir:
        os.makedirs(a.outdir, exist_ok=True)
        p = os.path.join(a.outdir, "morris_effects.json")
        json.dump([{"weight": s[0], "mu_star": s[1], "mu": s[2], "sigma": s[3]} for s in stats],
                  open(p, "w"), indent=1)
        print(f"wrote {p}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["evaluate", "search", "morris", "converge"])
    ap.add_argument("--scope", default="province:Noord-Brabant",
                    help="calibration region (representative on stock + heating mix)")
    ap.add_argument("--start", type=int, default=2022)
    ap.add_argument("--end", type=int, default=2024)
    ap.add_argument("--iterations", type=int, default=2, help="MC iterations per evaluation")
    ap.add_argument("--samples", type=int, default=40, help="search: LHS samples")
    ap.add_argument("--tolerance", type=float, default=2.0, help="search: MAD %%pts to retain a set")
    ap.add_argument("--levels", type=int, default=4, help="morris: grid levels")
    ap.add_argument("--trajectories", type=int, default=8, help="morris: number of trajectories")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--seed-offset", type=int, default=0,
                    help="shift the engine's world seeds; use a different value to validate a\ncalibrated weight set on FRESH worlds (out-of-sample over stochastic realisations)")
    ap.add_argument("--xmx", default="6g")
    ap.add_argument("--start-tol", type=float, default=2.0,
                    help="max acceptable start-year deviation per system (%%pts)")
    ap.add_argument("--no-start-check", action="store_true", help="skip the start-year sanity gate")
    ap.add_argument("--skip-build", action="store_true", help="don't rebuild the engine first")
    ap.add_argument("--iter-list", default="1,2,3,5,8",
                    help="converge: iteration counts to test")
    ap.add_argument("--axis", default="shareAffordability",
                    help="search: share to span with the representative sets (Morris' top lever)")
    ap.add_argument("--outdir", default=None)
    a = ap.parse_args()
    SEED_OFFSET[0] = a.seed_offset
    ensure_built(a.skip_build)
    {"evaluate": do_evaluate, "search": do_search, "morris": do_morris,
     "converge": do_converge}[a.mode](a)


if __name__ == "__main__":
    main()
