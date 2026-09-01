#!/usr/bin/env python3
"""
Overnight structural-sensitivity pipeline: one command, runs unattended.

  1. Rebuild the engine once.
  2. Morris screen at each weight vector (default: median, high_shareAffordability, best_fit) --
     the ranking + the weight-stability check.
  3. Pick the survivors = top-K factors by mean mu* TOTAL across those runs.
  4. LHS quantification on the survivors at the median weights (more samples + iters -> clean SRC).

Each step shells out to structural_sensitivity.py, logs to results/calib/, and continues on a
step failure (so a single bad run does not abort the night). Summary printed at the end.

    python structural_sensitivity_batch.py --scope province:Noord-Brabant
    python structural_batch.py --scope province:Noord-Brabant --weights median,best_fit --topk 5 --no-lhs
"""
import argparse, json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "structural_sensitivity.py")

def run(cmd, log):
    print(f"\n$ {' '.join(cmd)}", flush=True)
    with open(log, "w") as fh:
        rc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT).returncode
    tail = open(log).read().splitlines()[-4:]
    print("  " + "\n  ".join(tail))
    print(f"  [{'ok' if rc==0 else 'FAILED rc='+str(rc)} -> {log}]")
    return rc

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--weights", default="median,high_shareAffordability,best_fit",
                    help="comma list of weight vectors to run Morris at")
    ap.add_argument("--trajectories", type=int, default=8)
    ap.add_argument("--morris-iters", type=int, default=1)
    ap.add_argument("--topk", type=int, default=5, help="survivors carried into the LHS")
    ap.add_argument("--lhs-samples", type=int, default=80)
    ap.add_argument("--lhs-iters", type=int, default=8)
    ap.add_argument("--lhs-weights", default="median", help="weight vector for the LHS quantification")
    ap.add_argument("--no-lhs", action="store_true")
    ap.add_argument("--search", "--weights-file", dest="search", default=None,
                    help="calibration_search.json holding the ensemble the weight vectors are read\n"
                         "from (default: results/calib/calibration_search.json). --outdir defaults to\n"
                         "this file's directory, so a sweep in results/calib500 writes its screen there.")
    ap.add_argument("--outdir", default=None)
    a = ap.parse_args()
    search = os.path.abspath(a.search) if a.search else os.path.join(HERE, "..", "results", "calib", "calibration_search.json")
    if not os.path.exists(search):
        sys.exit(f"[batch] no calibration search at {search}")
    outdir = os.path.abspath(a.outdir) if a.outdir else os.path.dirname(search)
    os.makedirs(outdir, exist_ok=True)
    print(f"[batch] weights from {search}\n[batch] outputs to  {outdir}")
    vectors = [w.strip() for w in a.weights.split(",") if w.strip()]
    t0 = time.time(); results = {}

    for i, wv in enumerate(vectors):
        cmd = [sys.executable, SCRIPT, "morris", "--scope", a.scope,
               "--trajectories", str(a.trajectories), "--iterations", str(a.morris_iters),
               "--weights", wv, "--weights-file", search, "--outdir", outdir]
        if i > 0: cmd.append("--skip-build")          # build once, on the first run
        rc = run(cmd, os.path.join(outdir, f"log_morris_{wv.replace(':','')}.txt"))
        jf = os.path.join(outdir, f"structural_morris_{wv.replace(':','')}.json")
        if rc == 0 and os.path.exists(jf):
            results[wv] = json.load(open(jf))["total"]

    if not results:
        sys.exit("[batch] every Morris run failed -- check the logs, nothing to feed the LHS.")

    # survivors = top-K by mean mu* TOTAL across the successful weight vectors
    factors = list(next(iter(results.values())))
    mean_total = {f: sum(r.get(f, 0.0) for r in results.values()) / len(results) for f in factors}
    ranked = sorted(factors, key=lambda f: -mean_total[f])
    survivors = ranked[:a.topk]
    print("\n=== Morris summary (mean mu* TOTAL across weight vectors) ===")
    for f in ranked:
        star = " <- survivor" if f in survivors else ""
        print(f"  {f:<20}{mean_total[f]:>8.2f}{star}")

    if not a.no_lhs:
        cmd = [sys.executable, SCRIPT, "lhs", "--scope", a.scope, "--samples", str(a.lhs_samples),
               "--iterations", str(a.lhs_iters), "--factors", ",".join(survivors),
               "--weights", a.lhs_weights, "--weights-file", search,
               "--skip-build", "--outdir", outdir]
        run(cmd, os.path.join(outdir, "log_lhs.txt"))

    print(f"\n[batch] done in {(time.time()-t0)/3600:.2f} h. Outputs + logs in {outdir}")

if __name__ == "__main__":
    main()
