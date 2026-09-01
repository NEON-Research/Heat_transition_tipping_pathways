#!/usr/bin/env python3
"""Social-learning dose-response + a TRUE social knock-out.

Two experiments, both on the baseline scenario at calibrated central weights:

  sweep     vary the social-learning multiplier continuously (-Dht.socialLearningMult). The
            subjective norm is proportional to the share of peers already using a technology, so at
            the start of the transition it favours the incumbent. Too little social influence and an
            entrant never accumulates a peer base; too much and the incumbent's initial majority is
            locked in. The sweep locates the level -- if any -- that minimises the 2050 gas share.

  knockout  compare the intact model against (a) -Dht.peerFreeze=1, which freezes each household's
            peer composition and so genuinely severs the diffusion loop, and (b)
            -Dht.salienceFreeze=1, which pins only the amplifier and leaves peer share free to
            evolve. Reporting both makes clear how much of the loop each experiment actually removes.

    python social_sweep.py --scope nl --iterations 2 --xmx 48g
    python social_sweep.py --scope province:Noord-Brabant --levels 0,0.5,1,1.5,2
"""
import argparse, csv, json, os, subprocess, sys, tempfile, time
from collections import defaultdict
import numpy as np, matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
STOCK = os.path.join(ROOT, "model", "data", "stock")
ENGINE = os.path.join(ROOT, "model", "engine-java")
ENGINE_CP = os.environ.get("HT_ENGINE_CP",
                           os.path.join(ENGINE, "build", "classes", "java", "main"))
TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE = {"NATURAL_GAS_BOILER": "Gas boiler", "HYBRID_HEAT_PUMP": "Hybrid HP",
        "ELECTRIC_HEAT_PUMP": "Electric HP", "DISTRICT_HEATING": "District heating"}

def ensure_built(skip):
    """Rebuild before running. This script invokes java directly, so without a rebuild it silently
    uses stale classes and any newly added -Dht flag is ignored -- producing runs that differ only
    in flags the old build never knew about, i.e. identical results."""
    if skip:
        print("[social] --skip-build: using existing classes"); return
    gw = os.path.join(ENGINE, "gradlew.bat" if os.name == "nt" else "gradlew")
    if not os.path.exists(gw):
        print("[social] no gradle wrapper; using existing classes"); return
    print("[social] building engine (gradlew build)...", flush=True)
    if subprocess.run([gw, "build"], cwd=ENGINE).returncode != 0:
        sys.exit("[social] build failed -- fix before sweeping")


def tag(scope):
    k, _, n = scope.partition(":")
    if k.lower() in ("nl", "netherlands"): return "nl"
    return n.strip().lower().replace(" ", "-") if k == "province" else "gemeente_" + n.strip().lower()

def weights(path, which="best_fit"):
    d = json.load(open(path))
    return d["representative"][which]["weights"]

def run_point(scope, w, props, iterations, xmx, out):
    stock = os.path.join(STOCK, f"{tag(scope)}_dwellings.csv")
    if not os.path.exists(stock): sys.exit(f"stock not found: {stock}")
    cmd = ["java", f"-Xmx{xmx}"] + [f"-Dht.{k}={v}" for k, v in w.items()] \
        + [f"-Dht.{k}={v}" for k, v in props.items()] \
        + ["-cp", ENGINE_CP, "heattransition.Cli", "--real", stock, "--scenario", "baseline",
           "--iterations", str(iterations), "--out", out]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[-1500:], file=sys.stderr); sys.exit("engine run failed")
    return mix2050(out)

def mix2050(path):
    by = defaultdict(lambda: defaultdict(float))
    for r in csv.DictReader(open(path)):
        if r["ownership"] == "TOTAL" and int(float(r["year"])) == 2050:
            by[r["iteration"]][r["heating_system"]] += float(r["installed_current"])
    out = {}
    for te in TECHS:
        out[te] = float(np.mean([100*d.get(te, 0)/(sum(d.values()) or 1) for d in by.values()]))
    return out

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="nl")
    ap.add_argument("--levels", default="0,0.25,0.5,0.75,1.0,1.25,1.5,2.0,3.0",
                    help="social-learning multipliers to sweep (scenario LOW/MED/HIGH = 0.5/1/2)")
    ap.add_argument("--iterations", type=int, default=2)
    ap.add_argument("--xmx", default="48g")
    ap.add_argument("--weights", default="best_fit")
    ap.add_argument("--weights-file", default=os.path.join(ROOT, "results", "calib", "calibration_search.json"))
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--outdir", default=None)
    a = ap.parse_args()
    ensure_built(a.skip_build)
    outdir = a.outdir or os.path.join(ROOT, "results", tag(a.scope), "social")
    os.makedirs(outdir, exist_ok=True)
    w = weights(a.weights_file, a.weights)
    tmp = os.path.join(tempfile.gettempdir(), f"social_{os.getpid()}.csv")
    t0 = time.time()

    # ---- 1. dose-response sweep -------------------------------------------------------------
    levels = [float(x) for x in a.levels.split(",")]
    rows = []
    for i, m in enumerate(levels):
        mx = run_point(a.scope, w, {"socialLearningMult": m}, a.iterations, a.xmx, tmp)
        rows.append((m, mx))
        print(f"  [{i+1}/{len(levels)}] SLF={m:<5} gas={mx['NATURAL_GAS_BOILER']:5.1f}% "
              f"hyb={mx['HYBRID_HEAT_PUMP']:5.1f}% elec={mx['ELECTRIC_HEAT_PUMP']:5.1f}% "
              f"dh={mx['DISTRICT_HEATING']:5.1f}%", flush=True)

    # ---- 2. knock-outs ----------------------------------------------------------------------
    ko = {}
    for lab, props in (("intact", {}), ("peerFreeze", {"peerFreeze": 1}),
                       ("salienceFreeze", {"salienceFreeze": 1}), ("noLearning", {"learningRateMult": 0})):
        ko[lab] = run_point(a.scope, w, props, a.iterations, a.xmx, tmp)
        print(f"  [knock-out] {lab:<15} gas={ko[lab]['NATURAL_GAS_BOILER']:5.1f}% "
              f"hyb={ko[lab]['HYBRID_HEAT_PUMP']:5.1f}% elec={ko[lab]['ELECTRIC_HEAT_PUMP']:5.1f}%", flush=True)
    if os.path.exists(tmp): os.remove(tmp)

    # ---- outputs ----------------------------------------------------------------------------
    with open(os.path.join(outdir, "social_sweep.csv"), "w", newline="") as fh:
        wr = csv.writer(fh); wr.writerow(["social_learning_mult"] + TECHS)
        for m, mx in rows: wr.writerow([m] + [f"{mx[t]:.3f}" for t in TECHS])
    with open(os.path.join(outdir, "social_knockout.csv"), "w", newline="") as fh:
        wr = csv.writer(fh); wr.writerow(["run"] + TECHS)
        for lab, mx in ko.items(): wr.writerow([lab] + [f"{mx[t]:.3f}" for t in TECHS])

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    xs = [m for m, _ in rows]
    for te in TECHS:
        axes[0].plot(xs, [mx[te] for _, mx in rows], marker="o", ms=4, label=NICE[te])
    gas = [mx["NATURAL_GAS_BOILER"] for _, mx in rows]
    bestx = xs[int(np.argmin(gas))]
    axes[0].axvline(bestx, ls=":", color="k", lw=1)
    axes[0].annotate(f"min gas at {bestx:g}", (bestx, max(gas)*0.9), fontsize=8)
    axes[0].set_xlabel("social-learning multiplier"); axes[0].set_ylabel("2050 share (%)")
    axes[0].set_title("Dose-response: social influence has an optimum"); axes[0].grid(alpha=.3)
    axes[0].legend(fontsize=7)
    labs = list(ko); x = np.arange(len(labs)); bottom = np.zeros(len(labs))
    cols = ["#7f7f7f", "#d62728", "#1f77b4", "#ff7f0e"]
    for j, te in enumerate(TECHS):
        v = [ko[l][te] for l in labs]
        axes[1].bar(x, v, bottom=bottom, label=NICE[te], color=cols[j]); bottom += np.array(v)
    axes[1].set_xticks(x); axes[1].set_xticklabels(labs, fontsize=8, rotation=15)
    axes[1].set_ylabel("2050 share (%)"); axes[1].set_title("Loop knock-outs")
    fig.tight_layout()
    fp = os.path.join(outdir, "fig_social_sweep.png"); fig.savefig(fp, dpi=140, bbox_inches="tight")

    if len(set(round(g, 3) for g in gas)) == 1:
        print("\n  [ERROR] every sweep level returned an identical mix. The engine is ignoring "
              "-Dht.socialLearningMult, which means the build is stale or the flag is unsupported. "
              "Rebuild (drop --skip-build) and re-run; do not use these numbers.", file=sys.stderr)
    print(f"\n  minimum 2050 gas share at social-learning multiplier = {bestx:g} "
          f"({min(gas):.1f}%); at 0 it is {gas[0]:.1f}% and at {xs[-1]:g} it is {gas[-1]:.1f}%")
    print(f"  wrote {outdir}/social_sweep.csv, social_knockout.csv, fig_social_sweep.png")
    print(f"  done in {(time.time()-t0)/3600:.2f} h")

if __name__ == "__main__":
    main()
