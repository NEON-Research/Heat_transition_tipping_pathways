#!/usr/bin/env python3
"""
Scope-aware run orchestrator for the Java heat-transition model.

    python run.py --scope <spec> [engine args...]

<spec> is one of:
    nl                     whole Netherlands (~9.7M dwellings; large + memory-heavy)
    province:<Name>        e.g. province:Limburg, province:Noord-Brabant
    gemeente:<Name>        a municipality, e.g. gemeente:Maastricht

What it does:
    0. BUILD + TEST the engine (gradlew build) so a run never uses stale classes  [--skip-build to skip]
    1. resolve the scope to a stock CSV  (data/stock/<tag>_dwellings.csv)
    2. if that CSV is missing, PROVISION it by running the export from households.db
    3. run the Java engine (Cli) on it, writing the results CSV under the top-level
       results/ folder, forwarding extra engine args (--scenario, --iterations, --start, ...)
         default:      results/<scope>/simulation_results.csv          (overwritten each run)
         --timestamp:  results/<scope>/<yyyymmdd_hhmmss>/...            (archived, never overwritten;
                       a results/<scope>/LATEST file points at the newest run)
    4. with --analyze: plot that CSV into <run folder>/plots/ (failure-isolated)

Outputs live OUTSIDE the code tree: inputs = data/, code = model/, outputs = results/ (git-ignored).
Everything is resolved relative to this file, so it works from any working directory.

Examples:
    python run.py --scope province:Limburg --scenario baseline --iterations 20 --analyze
    python run.py --scope nl --scenario all --iterations 5 --analyze
    python run.py --scope gemeente:Maastricht --scenario all --out results/maastricht.csv
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))            # model/
ROOT = os.path.dirname(HERE)                                 # repo root
STOCK_DIR = os.path.join(HERE, "data", "stock")
RESULTS_DIR = os.path.join(ROOT, "results")                  # top-level outputs (git-ignored)
EXPORT = os.path.join(HERE, "data-export", "scripts", "export_stock.py")
ENGINE = os.path.join(HERE, "engine-java")
ANALYZE = os.path.join(ROOT, "results_analysis", "script_results.py")
SEARCH_JSON = os.path.join(RESULTS_DIR, "calib", "calibration_search.json")


def load_weight_sets(name, path):
    """Resolve --weights <name> against a calibration_search.json into a list of
    (label, {weight: value}) pairs. Accepts a representative key (best_fit,
    low_shareAffordability, high_shareAffordability), `retained:<i>` for a single
    retained set, or `all` to sweep every retained set."""
    if not os.path.exists(path):
        sys.exit(f"[run] --weights given but no search file at {path}\n"
                 f"      run the calibration first: python results_analysis/calibrate_weights.py search ...")
    with open(path, encoding="utf-8") as fh:
        d = json.load(fh)
    reps, retained = d.get("representative", {}), d.get("retained", [])
    if name in ("representative", "reps"):
        if not reps:
            sys.exit(f"[run] no representative sets in {path}")
        out, seen = [], set()
        for key, e in reps.items():
            sig = tuple(sorted(e["weights"].items()))   # dedupe identical sets (e.g. best_fit == low_*)
            if sig in seen:
                print(f"[run] skipping '{key}' (identical weights to an earlier representative set)")
                continue
            seen.add(sig)
            out.append((key, e["weights"]))
        return out
    if name == "all":
        if not retained:
            sys.exit(f"[run] no retained sets in {path}")
        width = len(str(len(retained) - 1))
        return [(f"set{str(i).zfill(width)}_mad{e['mad']:.2f}", e["weights"])
                for i, e in enumerate(retained)]
    if name.startswith("retained:"):
        i = int(name.split(":", 1)[1])
        if not (0 <= i < len(retained)):
            sys.exit(f"[run] retained index {i} out of range (0..{len(retained)-1})")
        return [(f"retained{i}", retained[i]["weights"])]
    if name in reps:
        return [(name, reps[name]["weights"])]
    valid = ", ".join(list(reps) + ["retained:<i>", "all"])
    sys.exit(f"[run] unknown --weights '{name}'. Choose one of: {valid}")


def scope_tag(scope):
    """scope -> filename tag (must match export_stock.py)."""
    s = scope.strip()
    if s.lower() in ("nl", "all", "netherlands"):
        return "nl"
    if ":" not in s:
        sys.exit(f"bad --scope '{scope}'. Use nl | province:<Name> | gemeente:<Name>")
    kind, name = s.split(":", 1)
    slug = name.lower().replace(" ", "_")
    if kind.lower() == "province":
        return slug
    if kind.lower() in ("gemeente", "municipality"):
        return "gemeente_" + slug
    sys.exit(f"unknown scope kind '{kind}'. Use province: or gemeente:")


def find_classpath():
    """Gradle build first (build/classes/java/main), then a manual `javac -d build`."""
    for cp in (os.path.join(ENGINE, "build", "classes", "java", "main"),
               os.path.join(ENGINE, "build")):
        if os.path.exists(os.path.join(cp, "heattransition", "Cli.class")):
            return cp
    return None


def gradlew():
    name = "gradlew.bat" if os.name == "nt" else "gradlew"
    p = os.path.join(ENGINE, name)
    return p if os.path.exists(p) else None


def analysis_python():
    """Interpreter for script_results.py. Prefer the results_analysis venv (has pandas/matplotlib);
    fall back to whatever is running run.py."""
    for rel in (os.path.join("Scripts", "python.exe"), os.path.join("bin", "python")):
        p = os.path.join(ROOT, "results_analysis", ".venv", rel)
        if os.path.exists(p):
            return p
    return sys.executable


def run_analysis(results_csv):
    """Plot the results CSV. Failure-isolated: a plotting error never fails the (valid) sim run."""
    if not os.path.exists(ANALYZE):
        print(f"[analyze] skipped: {ANALYZE} not found")
        return
    outdir = os.path.join(os.path.dirname(results_csv), "plots")
    py = analysis_python()
    print(f"[analyze] {py} {ANALYZE} --input {results_csv} --outdir {outdir}")
    try:
        r = subprocess.run([py, ANALYZE, "--input", results_csv, "--outdir", outdir])
        if r.returncode != 0:
            print(f"[analyze] plotting exited {r.returncode} (sim output is still valid): {results_csv}")
    except Exception as e:
        print(f"[analyze] plotting failed ({type(e).__name__}: {e}); sim output is still valid: {results_csv}")


def ensure_built(skip):
    """Compile + test the engine so a run never uses stale classes. Uses the Gradle wrapper
    (`gradlew build`, which runs the JUnit + SelfTest checks). Falls back to whatever is already
    compiled if the wrapper isn't present (or if --skip-build is passed for fast iteration)."""
    if skip:
        print("[run] --skip-build: using already-compiled classes without rebuilding")
        return
    gw = gradlew()
    if not gw:
        print("[run] no Gradle wrapper found (run gradle-bootstrap.ps1 once); using existing classes")
        return
    print("[run] building + testing engine (gradlew build)...")
    if subprocess.run([gw, "build"], cwd=ENGINE).returncode != 0:
        sys.exit("[run] build or tests FAILED -- fix before running")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", required=True, help="nl | province:<Name> | gemeente:<Name>")
    ap.add_argument("--force-export", action="store_true", help="re-provision the stock CSV even if it exists")
    ap.add_argument("--skip-build", action="store_true", help="skip the gradlew build+test step (fast iteration)")
    ap.add_argument("--xmx", default="8g", help="JVM max heap (nl-scale needs a lot; default 8g)")
    ap.add_argument("--out", default=None,
                    help="results CSV path (default: results/<scope>/[<timestamp>/]simulation_results.csv)")
    ap.add_argument("--timestamp", action="store_true",
                    help="archive this run in its own results/<scope>/<yyyymmdd_hhmmss>/ folder "
                         "(default: overwrite results/<scope>/ — better for dev iteration)")
    ap.add_argument("--analyze", action="store_true",
                    help="after a successful run, plot the results CSV into <run folder>/plots/")
    ap.add_argument("--weights", default=None,
                    help="run with a calibrated weight set from calibration_search.json: a "
                         "representative key (best_fit / low_shareAffordability / "
                         "high_shareAffordability), `retained:<i>`, or `all` to sweep every "
                         "retained set. Each set's -Dht.* flags are passed to the engine.")
    ap.add_argument("--weights-file", default=SEARCH_JSON,
                    help=f"calibration search JSON to read --weights from (default: {SEARCH_JSON})")
    ap.add_argument("--prop", action="append", default=[], metavar="KEY=VALUE",
                    help="inject a JVM system property, e.g. --prop ht.gasPriceGrowth=0.0262 "
                         "(repeatable). Use for energy-price paths and other -Dht.* overrides.")
    args, engine_args = ap.parse_known_args()

    tag = scope_tag(args.scope)
    stock_csv = os.path.join(STOCK_DIR, f"{tag}_dwellings.csv")

    # weight sets to run: one plain run, or one/many calibrated sets from the search JSON.
    #   (label, {weight: value})   label=None -> no -Dht flags (engine defaults)
    weight_sets = load_weight_sets(args.weights, args.weights_file) if args.weights else [(None, None)]
    if args.out and len(weight_sets) > 1:
        sys.exit("[run] --out cannot be combined with --weights all (each set needs its own file); "
                 "drop --out and the sets are written under results/<scope>/calib/<label>/")

    def results_path_for(label):
        # explicit --out wins (single run); labelled sets go under results/<tag>/calib/<label>/;
        # otherwise the usual results/<tag>/[<timestamp>/]simulation_results.csv
        if args.out and label is None:
            return os.path.abspath(args.out)
        run_dir = os.path.join(RESULTS_DIR, tag)
        if label is not None:
            run_dir = os.path.join(run_dir, "calib", label)
        elif args.timestamp:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_dir = os.path.join(run_dir, stamp)
            os.makedirs(os.path.join(RESULTS_DIR, tag), exist_ok=True)
            with open(os.path.join(RESULTS_DIR, tag, "LATEST"), "w") as fh:
                fh.write(stamp + "\n")
        return os.path.abspath(os.path.join(run_dir, "simulation_results.csv"))

    # 0. build + test the engine first (fail fast on code errors, never run stale classes)
    ensure_built(args.skip_build)

    # 1-2. provision if missing
    if args.force_export or not os.path.exists(stock_csv):
        print(f"[run] stock for scope '{args.scope}' not found -> provisioning {os.path.basename(stock_csv)}")
        r = subprocess.run([sys.executable, EXPORT, "--scope", args.scope],
                           cwd=os.path.dirname(EXPORT))
        if r.returncode != 0 or not os.path.exists(stock_csv):
            sys.exit("[run] export failed (is households.db present? it is git-ignored and must exist locally)")
    else:
        print(f"[run] using existing stock: {stock_csv}")

    # 3. run the engine, once per weight set
    cp = find_classpath()
    if not cp:
        sys.exit(f"[run] no compiled engine found under {ENGINE}\\build. Build it first:\n"
                 f"      cd engine-java  &&  .\\gradlew.bat build")

    worst_rc = 0
    for idx, (label, weights) in enumerate(weight_sets):
        results_csv = results_path_for(label)
        os.makedirs(os.path.dirname(results_csv), exist_ok=True)
        dflags = [f"-Dht.{k}={v}" for k, v in (weights or {}).items()]
        dflags += [f"-D{p}" for p in args.prop]   # generic JVM property passthrough (e.g. price paths)
        if label is not None:
            print(f"[run] weight set {idx+1}/{len(weight_sets)}: {label}")
        cmd = ["java", f"-Xmx{args.xmx}", *dflags, "-cp", cp, "heattransition.Cli",
               "--real", stock_csv, "--out", results_csv] + engine_args
        print("[run] " + " ".join(cmd))
        rc = subprocess.run(cmd).returncode
        # 4. optional analysis — only on a clean run; never fails an otherwise-valid simulation
        if rc == 0 and args.analyze:
            run_analysis(results_csv)
        worst_rc = worst_rc or rc
    sys.exit(worst_rc)


if __name__ == "__main__":
    main()
