#!/usr/bin/env python3
"""Carry scenario runs over to a new subsample: copy the run folder of every weight set that is in both
the old and the new subsample to the name the new subsample gives it, with a weights.json, so that
`pathway_batch.py --resume` (run.py --skip-existing) only runs the new sets.

Run folders are named set<i>_mad<mad> after the position i in the subsample's 'retained' list
(model/run.py), so a set that stays usually gets a different name.

    python results_analysis/reuse_subsample_runs.py --old-sub results/calib1000_v3/calibration_subsample_maximin.json \
        --new-sub results/calib1000_v3/calibration_subsample.json --old-runs results/nl_subsample_maximin --scope nl
"""
import argparse, json, os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))


def names(sub):
    R = json.load(open(sub, encoding="utf-8"))["retained"]
    w = len(str(len(R) - 1))
    return {e["retained_index_full"]: (f"set{str(i).zfill(w)}_mad{e['mad']:.2f}", e["weights"]) for i, e in enumerate(R)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--old-sub", required=True)
    ap.add_argument("--new-sub", required=True)
    ap.add_argument("--old-runs", required=True, help="results folder of the old subsample (holds calib/)")
    ap.add_argument("--scope", required=True, help="tag of the new results folder, e.g. nl")
    a = ap.parse_args()
    old, new = names(a.old_sub), names(a.new_sub)
    dst_root = os.path.join(ROOT, "results", a.scope, "calib")
    os.makedirs(dst_root, exist_ok=True)
    reused = 0
    for k, (new_name, w) in sorted(new.items()):
        if k not in old:
            continue
        old_name, w_old = old[k]
        if w_old != w:
            sys.exit(f"retained {k}: weights differ between the two subsample files")
        src = os.path.join(a.old_runs, "calib", old_name)
        if not os.path.exists(os.path.join(src, "simulation_results.csv")):
            print(f"  retained {k}: no results in {src}, will be run"); continue
        dst = os.path.join(dst_root, new_name)
        if os.path.exists(dst):
            sys.exit(f"{dst} exists already; not overwriting")
        shutil.copytree(src, dst)
        json.dump(w, open(os.path.join(dst, "weights.json"), "w", encoding="utf-8"), indent=1)
        print(f"  retained {k:3d}: {old_name} -> {new_name}")
        reused += 1
    print(f"{reused} of {len(new)} sets reused; {len(new) - reused} to run")


if __name__ == "__main__":
    main()
