#!/usr/bin/env python3
"""The numbers quoted in the paper's LHS paragraph, recomputed from the LHS output and logged.

Reads structural_lhs_<weights>.csv (written by structural_sensitivity.py lhs) and reports:
  - the spread of each technology's 2050 share (5/25/50/75/95 %, min-max);
  - standardised regression coefficients (SRC) of the six factors;
  - the linear change in each share when a factor moves across its sampled range;
  - how much variation a linear model explains, and how much squares and pairwise interactions add
    (leave-one-out R2, so the extra terms cannot flatter the fit), with the strongest interactions;
  - for comparison, the spread of the 2050 baseline across all retained weight sets
    (subsample_validation/setNNN.csv, if present).

    python results_analysis/lhs_report.py results/calib1000_v3

Writes <dir>/log_lhs_report.txt.
"""
import argparse
import csv
import glob
import os
import sys

import numpy as np

T = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
SHORT = {"NATURAL_GAS_BOILER": "gas boiler", "HYBRID_HEAT_PUMP": "hybrid", "ELECTRIC_HEAT_PUMP": "electric",
         "DISTRICT_HEATING": "district heating"}


class Tee:
    def __init__(self, path):
        self.f = open(path, "w", encoding="utf-8")

    def write(self, s):
        sys.__stdout__.write(s); self.f.write(s)

    def flush(self):
        sys.__stdout__.flush(); self.f.flush()


def fit(A, y):
    A1 = np.column_stack([np.ones(len(y)), A]); b, *_ = np.linalg.lstsq(A1, y, rcond=None)
    return 1 - ((y - A1 @ b) ** 2).sum() / ((y - y.mean()) ** 2).sum(), b


def loo(A, y):
    A1 = np.column_stack([np.ones(len(y)), A]); p = np.empty(len(y))
    for k in range(len(y)):
        m = np.arange(len(y)) != k
        b, *_ = np.linalg.lstsq(A1[m], y[m], rcond=None); p[k] = A1[k] @ b
    return 1 - ((y - p) ** 2).sum() / ((y - y.mean()) ** 2).sum()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("calib", help="folder holding structural_lhs_*.csv")
    ap.add_argument("--csv", default=None, help="the LHS file (default: the only structural_lhs_*.csv in calib)")
    a = ap.parse_args()
    path = a.csv or (glob.glob(os.path.join(a.calib, "structural_lhs_*.csv")) or [None])[0]
    if not path:
        sys.exit(f"no structural_lhs_*.csv in {a.calib}")
    sys.stdout = Tee(os.path.join(a.calib, "log_lhs_report.txt"))
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    F = [k for k in rows[0] if k not in T and k not in ("NATURAL_GAS_BLOCK", "NONGAS")]
    X = np.array([[float(r[f]) for f in F] for r in rows])
    Y = np.array([[float(r[t]) for t in T] for r in rows])
    Z = (X - X.mean(0)) / X.std(0)
    print(f"{os.path.basename(path)}: {len(rows)} samples, factors {F}")
    print(f"largest correlation between sampled factors: "
          f"{np.max(np.abs(np.corrcoef(Z.T) - np.eye(len(F)))):.2f}")

    print("\n2050 share (%)            min     p5    p25 median    p75    p95    max")
    for j, t in enumerate(T):
        q = np.percentile(Y[:, j], [0, 5, 25, 50, 75, 95, 100])
        print(f"  {SHORT[t]:20s}" + "".join(f"{v:7.1f}" for v in q))

    print("\nstandardised regression coefficients (SRC)")
    print(f"  {'':22s}" + "".join(f"{SHORT[t]:>17s}" for t in T))
    for i, f in enumerate(F):
        print(f"  {f:22s}" + "".join(
            f"{fit(Z, (Y[:, j] - Y[:, j].mean()) / Y[:, j].std())[1][i + 1]:17.2f}" for j in range(len(T))))

    print("\nlinear change in the 2050 share (pp) when a factor moves from its lowest to highest sampled value")
    for i, f in enumerate(F):
        print(f"  {f:22s}" + "".join(
            f"{fit(X, Y[:, j])[1][i + 1] * (X[:, i].max() - X[:, i].min()):+17.1f}" for j in range(len(T))))

    sq = Z ** 2
    pairs = [(i, k) for i in range(len(F)) for k in range(i + 1, len(F))]
    inter = np.column_stack([Z[:, i] * Z[:, k] for i, k in pairs])
    print("\nexplained variation (R2)   linear in-sample  linear LOO  +squares LOO  +squares+interactions LOO")
    for j, t in enumerate(T):
        y = Y[:, j]
        print(f"  {SHORT[t]:22s}{fit(Z, y)[0]:15.2f}{loo(Z, y):12.2f}{loo(np.column_stack([Z, sq]), y):14.2f}"
              f"{loo(np.column_stack([Z, sq, inter]), y):26.2f}")
    print("\nstrongest single interactions (gain in LOO R2 when added to the linear model)")
    for j, t in enumerate(T):
        y = Y[:, j]; base = loo(Z, y)
        g = sorted(((loo(np.column_stack([Z, Z[:, i] * Z[:, k]]), y) - base, F[i], F[k]) for i, k in pairs),
                   reverse=True)[:3]
        print(f"  {SHORT[t]:18s} " + "; ".join(f"{x} x {z} {v:+.2f}" for v, x, z in g))

    vdir = os.path.join(a.calib, "subsample_validation")
    sets = sorted(glob.glob(os.path.join(vdir, "set[0-9][0-9][0-9].csv")))
    if sets:
        Yw = []
        for p in sets:
            acc, tot = {}, 0.0
            for r in csv.DictReader(open(p, newline="", encoding="utf-8")):
                if r["ownership"] == "TOTAL" and r["year"] == "2050" and r.get("scenario_name", "baseline") == "baseline":
                    acc[r["heating_system"]] = acc.get(r["heating_system"], 0) + float(r["installed_current"])
                    tot += float(r["installed_current"])
            Yw.append([100 * acc.get(t, 0) / tot for t in T])
        Yw = np.array(Yw)
        print(f"\nfor comparison: 2050 baseline across the {len(sets)} retained weight sets (default parameters)")
        print("                           min     p5    p25 median    p75    p95    max")
        for j, t in enumerate(T):
            q = np.percentile(Yw[:, j], [0, 5, 25, 50, 75, 95, 100])
            print(f"  {SHORT[t]:20s}" + "".join(f"{v:7.1f}" for v in q))
    print(f"\nwrote {os.path.join(a.calib, 'log_lhs_report.txt')}")


if __name__ == "__main__":
    main()
