#!/usr/bin/env python3
"""One command for the whole unattended chain: re-sample the ensemble, screen the parameters,
draw the figures. Start it and go to bed.

    python results_analysis\\run_overnight.py --outdir results\\calib500b

Steps, in order, each logged into --outdir:
  1. calibrate_weights.py search   -- Latin-hypercube sweep + history matching  (~5-6 h at 500)
  2. pick three weight vectors from the RETAINED ensemble, spanning the social-norm axis
     (indices are ranks within the new ensemble, so they cannot be reused from an old run)
  3. structural_batch.py           -- Morris at each vector, then LHS on the survivors  (~8 h)
  4. make_ensemble_figures.py      -- fit / weights / observed panels

A failed step is reported and the chain stops there, so a bad sweep never feeds a screen.
Use --dry-run to see the commands, --no-lhs / --no-screen / --no-figures to trim the night.
"""
import argparse, json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
PY_  = sys.executable


def step(name, cmd, log, dry):
    print(f"\n{'='*78}\n[{time.strftime('%H:%M:%S')}] {name}\n$ {' '.join(cmd)}", flush=True)
    if dry:
        return 0
    t0 = time.time()
    with open(log, "w", encoding="utf-8") as fh:
        rc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT).returncode
    mins = (time.time() - t0) / 60
    tail = [l for l in open(log, encoding="utf-8", errors="replace").read().splitlines() if l.strip()][-4:]
    print("  " + "\n  ".join(tail))
    print(f"  [{'ok' if rc == 0 else 'FAILED rc=' + str(rc)} in {mins:.0f} min -> {log}]", flush=True)
    return rc


def pick_vectors(search, n=3):
    """Three points spanning the social-norm axis of the retained ensemble: the best fit plus the
    best-fitting sets at the low and high ends. Social norm is the axis that matters because the
    salience parameters act only through the subjective norm, so their influence is conditional
    on it -- that is the whole reason for repeating the screen."""
    ret = json.load(open(search))["retained"]
    if not ret:
        sys.exit("[overnight] nothing retained -- loosen the retention rule before screening")
    order = sorted(range(len(ret)), key=lambda i: ret[i]["sampled"]["shareSocialnorm"])
    picks, seen = [], set()
    for i in [0, order[0], order[-1]]:                  # best fit, lowest sn, highest sn
        if i not in seen:
            seen.add(i); picks.append(i)
    for i in order:                                     # top up if the ensemble is tiny
        if len(picks) >= n: break
        if i not in seen: seen.add(i); picks.append(i)
    print("\n[overnight] screen vectors chosen from the new ensemble:")
    for i in picks:
        s = ret[i]["sampled"]
        print(f"    retained:{i:<3} MAD={ret[i]['mad']:.2f} "
              f"att={s['shareAttitude']:.2f} sn={s['shareSocialnorm']:.2f} "
              f"aff={s['shareAffordability']:.2f} int={s['shareIntention']:.2f}")
    return [f"retained:{i}" for i in picks[:n]]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--outdir", default=os.path.join(ROOT, "results", "calib500b"))
    ap.add_argument("--samples", type=int, default=500)
    ap.add_argument("--iterations", type=int, default=1, help="MC replications per weight set")
    ap.add_argument("--trajectories", type=int, default=8)
    ap.add_argument("--topk", type=int, default=6, help="factors carried into the LHS")
    ap.add_argument("--lhs-samples", type=int, default=80)
    ap.add_argument("--lhs-iters", type=int, default=8)
    ap.add_argument("--xmx", default="8g")
    ap.add_argument("--skip-search", action="store_true",
                    help="reuse the calibration_search.json already in --outdir and go straight to\n"
                         "the screen and figures (for when the sweep is done but the screen is not)")
    ap.add_argument("--no-screen", action="store_true")
    ap.add_argument("--no-lhs", action="store_true")
    ap.add_argument("--no-figures", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    out = os.path.abspath(a.outdir); os.makedirs(out, exist_ok=True)
    search = os.path.join(out, "calibration_search.json")
    if os.path.exists(search) and not a.skip_search and not a.dry_run:
        sys.exit(f"[overnight] {search} already exists -- choose a fresh --outdir so the old "
                 f"ensemble is not overwritten, or pass --skip-search to reuse it")
    if a.skip_search and not os.path.exists(search) and not a.dry_run:
        sys.exit(f"[overnight] --skip-search but no sweep at {search}")
    t0 = time.time()
    print(f"[overnight] scope {a.scope}\n[overnight] outputs {out}")

    if a.skip_search:
        print(f"[overnight] --skip-search: reusing the existing sweep at {search}")
        rc = 0
    else:
        rc = step("1/4  calibration sweep",
                  [PY_, os.path.join(HERE, "calibrate_weights.py"), "search",
                   "--scope", a.scope, "--samples", str(a.samples),
                   "--iterations", str(a.iterations), "--xmx", a.xmx, "--outdir", out],
                  os.path.join(out, "log_search.txt"), a.dry_run)
    if rc: sys.exit("[overnight] sweep failed -- stopping before the screen")

    if not a.no_screen:
        vectors = pick_vectors(search) if not a.dry_run else ["retained:0,retained:i,retained:j"]
        cmd = [PY_, os.path.join(HERE, "structural_batch.py"), "--scope", a.scope,
               "--search", search, "--weights", ",".join(vectors),
               "--trajectories", str(a.trajectories), "--topk", str(a.topk),
               "--lhs-samples", str(a.lhs_samples), "--lhs-iters", str(a.lhs_iters),
               "--outdir", out]
        if a.no_lhs: cmd.append("--no-lhs")
        step("2/4  parameter screen (Morris, then LHS)", cmd,
             os.path.join(out, "log_structural_batch.txt"), a.dry_run)

    if not a.no_figures:
        step("3/4  ensemble figures",
             [PY_, os.path.join(HERE, "make_ensemble_figures.py"), out],
             os.path.join(out, "log_figures.txt"), a.dry_run)
        step("4/4  paper figures",
             [PY_, os.path.join(HERE, "paper_figures.py"), "--scope", a.scope, "--calib", out],
             os.path.join(out, "log_paper_figures.txt"), a.dry_run)

    print(f"\n[overnight] finished in {(time.time()-t0)/3600:.1f} h. Everything is in {out}")


if __name__ == "__main__":
    main()
