#!/usr/bin/env python3
"""
Calibrate / screen the TPB decision weights against observed 2022-2024 adoption (CBS maatwerk).

WHY ONLY 2022-2024: CBS changed method before 2022, so this is the only internally consistent
window. It is therefore used as a **broad plausibility range**, not a precise fit target: we keep
weight sets that reproduce the observed direction+magnitude of change within a tolerance, rather
than optimising to a single "best" point (weights are non-identifiable from 3 years of data).

Setup: start the engine in 2022 from the OBSERVED 2022 state (nbh_heating_2022.csv), run to 2024,
and compare the simulated 2024 technology mix with the observed 2024 mix for the calibration region.

Modes
  evaluate  one weight set (default = AL defaults) -> error
  search    random / Latin-hypercube sample of weight space -> ranked table + retained ensemble
  morris    Morris elementary effects -> which weights matter most (uses the baseline weights)

Weights varied (engine reads them via -Dht.<name>; see Constants.p):
  wAttitudeToIntention, wSocialnormToIntention, wPbcToIntention,
  wAffordabilityToPbc, wEffortToPbc, wIntentionToBehavior

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
ENGINE_CP = os.environ.get("HT_ENGINE_CP",
                           os.path.join(MODEL, "engine-java", "build", "classes", "java", "main"))

SYSTEMS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP",
           "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
OBS_KEY = {"NATURAL_GAS_BOILER": "gasCV", "NATURAL_GAS_BLOCK": "gasBlock",
           "HYBRID_HEAT_PUMP": "hhp", "ELECTRIC_HEAT_PUMP": "ehp", "DISTRICT_HEATING": "dh"}

# weight name -> (default, low, high) plausible bounds for sampling/screening
WEIGHTS = {
    "wAttitudeToIntention":   (0.5, 0.1, 1.0),
    "wSocialnormToIntention": (0.5, 0.1, 1.0),
    "wPbcToIntention":        (0.5, 0.1, 1.0),
    "wAffordabilityToPbc":    (0.5, 0.1, 1.0),
    "wEffortToPbc":           (0.2, 0.05, 0.8),
    "wIntentionToBehavior":   (0.5, 0.1, 1.0),
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
    """buurtcodes belonging to the scope, taken from the scope's stock CSV."""
    path = os.path.join(STOCK, f"{scope_tag(scope)}_dwellings.csv")
    if not os.path.exists(path):
        sys.exit(f"stock CSV not found: {path}\nProvision it first: python model/run.py --scope {scope} ...")
    codes = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            c = row.get("buurtcode")
            if c:
                codes.add(c)
    return codes


def observed_mix(scope, year):
    """Observed share per system for the region, dwelling-weighted over complete buurten."""
    codes = region_buurten(scope)
    path = os.path.join(REF, "observed_heating_by_year.csv")
    if not os.path.exists(path):
        sys.exit("observed_heating_by_year.csv missing -> run model/data-export/scripts/export_observed_heating.py")
    tot = {s: 0.0 for s in SYSTEMS}
    n = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["year"] != str(year) or r["complete"] != "1" or r["buurtcode"] not in codes:
                continue
            for s in SYSTEMS:
                tot[s] += float(r[OBS_KEY[s]])
            n += 1
    if n == 0:
        sys.exit(f"no complete observed rows for {scope} in {year}")
    mix = {s: 100.0 * tot[s] / n for s in SYSTEMS}
    ssum = sum(mix.values()) or 1.0
    return {s: 100.0 * v / ssum for s, v in mix.items()}, n     # renormalised to 100 %


# ---------------------------------------------------------------- engine run

def run_engine(scope, weights, iterations, start, end, xmx="6g", quiet=True):
    """Run the engine with the given weights; return the simulated final-year mix (%)."""
    stock = os.path.join(STOCK, f"{scope_tag(scope)}_dwellings.csv")
    out = os.path.join(tempfile.gettempdir(), f"calib_{os.getpid()}.csv")
    cmd = ["java", f"-Xmx{xmx}",
           "-Dht.nbhHeating=nbh_heating_2022.csv"]                      # start from observed 2022
    cmd += [f"-Dht.{k}={v}" for k, v in weights.items()]
    cmd += ["-cp", ENGINE_CP, "heattransition.Cli",
            "--real", stock, "--scenario", "baseline",
            "--iterations", str(iterations), "--start", str(start), "--end", str(end),
            "--out", out]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout[-2000:], r.stderr[-2000:], file=sys.stderr)
        sys.exit("engine run failed")
    # final-year TOTAL mix, mean over iterations
    tot = {s: 0.0 for s in SYSTEMS}
    cnt = 0
    with open(out, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["ownership"] != "TOTAL" or int(row["year"]) != end:
                continue
            tot[row["heating_system"]] += float(row["installed_current"])
            cnt += 1
    os.remove(out)
    s = sum(tot.values()) or 1.0
    return {k: 100.0 * v / s for k, v in tot.items()}


def error(sim, obs, focus=("NATURAL_GAS_BOILER", "ELECTRIC_HEAT_PUMP", "HYBRID_HEAT_PUMP")):
    """Mean absolute deviation (%pts). `focus` = the systems that actually move in 2022-24."""
    return float(np.mean([abs(sim[s] - obs[s]) for s in focus]))


# ---------------------------------------------------------------- modes

def do_evaluate(a):
    obs, n = observed_mix(a.scope, a.end)
    w = {k: v[0] for k, v in WEIGHTS.items()}
    sim = run_engine(a.scope, w, a.iterations, a.start, a.end, a.xmx)
    print(f"\nObserved {a.end} ({n} buurten) vs simulated ({a.start}->{a.end}, AL default weights):\n")
    print(f"{'system':<22}{'observed':>10}{'simulated':>11}{'diff':>8}")
    for s in SYSTEMS:
        print(f"{s:<22}{obs[s]:>10.1f}{sim[s]:>11.1f}{sim[s]-obs[s]:>+8.1f}")
    print(f"\nMAD (gas/EHP/HHP): {error(sim, obs):.2f} %pts")


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
    obs, n = observed_mix(a.scope, a.end)
    rng = np.random.default_rng(a.seed)
    samples = sample_lhs(rng, a.samples)
    rows = []
    for i, w in enumerate(samples, 1):
        sim = run_engine(a.scope, w, a.iterations, a.start, a.end, a.xmx)
        e = error(sim, obs)
        rows.append((e, w, sim))
        print(f"  [{i}/{len(samples)}] MAD={e:6.2f}  " +
              " ".join(f"{k[1:4]}={v:.2f}" for k, v in w.items()), flush=True)
    rows.sort(key=lambda r: r[0])
    print(f"\nBest 5 of {len(rows)} (observed {a.end}: " +
          ", ".join(f"{s.split('_')[0][:4]} {obs[s]:.1f}" for s in SYSTEMS) + ")")
    for e, w, sim in rows[:5]:
        print(f"  MAD={e:5.2f}  " + " ".join(f"{k}={v:.3f}" for k, v in w.items()))
    keep = [r for r in rows if r[0] <= a.tolerance]
    print(f"\nRetained (MAD <= {a.tolerance} %pts): {len(keep)}/{len(rows)} weight sets "
          f"-> behaviourally-plausible ensemble")
    out = os.path.join(a.outdir, "calibration_search.json") if a.outdir else None
    if out:
        os.makedirs(a.outdir, exist_ok=True)
        json.dump([{"mad": e, "weights": w, "sim": s} for e, w, s in rows], open(out, "w"), indent=1)
        print(f"wrote {out}")


def do_morris(a):
    """Morris elementary effects: mu* (mean |EE|) ranks which weights matter."""
    obs, _ = observed_mix(a.scope, a.end)
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

        y_prev = error(run_engine(a.scope, as_w(x), a.iterations, a.start, a.end, a.xmx), obs)
        runs += 1
        for j in order:
            x2 = x.copy()
            x2[j] = x2[j] + delta if x2[j] + delta <= 1.0 else x2[j] - delta
            y = error(run_engine(a.scope, as_w(x2), a.iterations, a.start, a.end, a.xmx), obs)
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
    ap.add_argument("mode", choices=["evaluate", "search", "morris"])
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
    ap.add_argument("--xmx", default="6g")
    ap.add_argument("--outdir", default=None)
    a = ap.parse_args()
    {"evaluate": do_evaluate, "search": do_search, "morris": do_morris}[a.mode](a)


if __name__ == "__main__":
    main()
