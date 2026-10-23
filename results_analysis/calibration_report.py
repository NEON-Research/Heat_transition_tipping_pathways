#!/usr/bin/env python3
"""Every number quoted in the paper's calibration section, recomputed and logged.

Reads the calibration (calibration_search.json), its subsample (calibration_subsample.json) and the
2024-2050 baseline of every retained set (subsample_validation/setNNN.csv, written by
select_subsample.py validate). One extra engine run per retained set (2022-2024, exactly as in the
calibration) adds what the stored results do not hold: the share of homeowner decisions that do not
fall on the option with the highest modelled utility (engine diagnostic HT_NONMAX) and the change in
gas boilers per ownership category. Those runs are cached in <dir>/calibration_report_runs.csv;
delete the file (or pass --rerun) to repeat them.

    python results_analysis/calibration_report.py results/calib1000_v3 --jobs 3

Writes <dir>/log_calibration_report.txt (everything printed) and <dir>/calibration_report_runs.csv.

Sections
  A  fit: observed mix, deviations of the retained and all sampled sets, the plateau
  B  retention criterion: the uncertainty terms, the deviation they admit, sensitivity table
  C  residuals of the retained sets and stricter criteria
  D  decision weights: ranges, widths, retention by range, the cost-attitude line
  E  noise scale: retention by range, share of decisions not on the highest utility
  F  who removes the gas boilers in 2022-2024 (by ownership)
  G  2050 outcomes across the retained sets: how much the weights and the noise scale explain
  H  the 25-set subsample: method, screen sets, how well it reproduces the full ensemble
"""
import argparse
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import calibrate_weights as cw                      # noqa: E402  (engine call, observed data)
import select_subsample as ss                       # noqa: E402  (shares, screen sets)

T = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
SHORT = {"NATURAL_GAS_BOILER": "gas boiler", "HYBRID_HEAT_PUMP": "hybrid", "ELECTRIC_HEAT_PUMP": "electric",
         "DISTRICT_HEATING": "district heating"}
Y = ["2023", "2024"]
SH = ["shareAttitude", "shareSocialnorm", "shareAffordability", "shareIntention"]
OWNERS = ["PRIVATELY_OWNED", "PRIVATELY_RENTED", "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION", "TOTAL"]


class Tee:
    def __init__(self, path):
        self.f = open(path, "w", encoding="utf-8")

    def write(self, s):
        sys.__stdout__.write(s); self.f.write(s)

    def flush(self):
        sys.__stdout__.flush(); self.f.flush()


def head(title):
    print(f"\n{'=' * 100}\n{title}\n{'=' * 100}")


def effective(L):
    g = lambda k: np.array([s["sampled"][k] for s in L])
    a, sn, af, it = (g(k) for k in SH)
    pbc = (1 - it) + it * (1 - a - sn)
    return {"attitude": it * a, "cost": af * pbc, "social": it * sn, "effort": (1 - af) * pbc}


def deviations(s, obs):
    return {(y, t): s["mix"][y][t] - obs[y][t] for y in Y for t in T}


def imax(s, obs, sdmc, sdobs, disc):
    return max(abs(s["mix"][y][t] - obs[y][t]) / np.sqrt(sdmc[t] ** 2 + sdobs[y][t] ** 2 + disc ** 2)
               for y in Y for t in T)


def r2(X, y):
    X1 = np.column_stack([np.ones(len(y)), X]); b, *_ = np.linalg.lstsq(X1, y, rcond=None)
    return 1 - ((y - X1 @ b) ** 2).sum() / ((y - y.mean()) ** 2).sum()


def loo_r2(X, y):
    X1 = np.column_stack([np.ones(len(y)), X]); pred = np.empty(len(y))
    for k in range(len(y)):
        m = np.arange(len(y)) != k
        b, *_ = np.linalg.lstsq(X1[m], y[m], rcond=None); pred[k] = X1[k] @ b
    return 1 - ((y - pred) ** 2).sum() / ((y - y.mean()) ** 2).sum()


def quad(X):
    """All linear terms, squares and pairwise products."""
    return np.column_stack([X] + [(X[:, i] * X[:, j])[:, None] for i in range(X.shape[1])
                                  for j in range(i, X.shape[1])])


# ------------------------------------------------------------------ engine runs (sections E, F)

def engine_run(i, e, scope, iterations, xmx):
    """One calibration run (2022 -> 2024) for retained set i, with the HT_NONMAX diagnostic."""
    stock, bfilter = cw.stock_path(scope), cw.ensure_buurt_filter()
    out = os.path.join(tempfile.gettempdir(), f"calreport_{os.getpid()}_{i}.csv")
    cmd = ["java", f"-Xmx{xmx}", f"-Dht.seedOffset={cw.SEED_OFFSET[0]}", "-Dht.heatingYear=2022",
           f"-Dht.buurtFilter={bfilter}"]
    cmd += [f"-Dht.{k}={v}" for k, v in {**cw.CALIB_FIXED, **cw.FIXED, **e["weights"]}.items()]
    cmd += ["-cp", cw.ENGINE_CP, "heattransition.Cli", "--real", stock, "--scenario", "baseline",
            "--iterations", str(iterations), "--start", "2022", "--end", "2024", "--out", out]
    r = subprocess.run(cmd, capture_output=True, text=True, env=dict(os.environ, HT_NONMAX="1"))
    if r.returncode != 0:
        raise RuntimeError(f"set {i}: engine failed\n{r.stderr[-1500:]}")
    dec = non = 0
    for m in re.finditer(r"NONMAX (\d+) decisions=(\d+) nonmax=(\d+)", r.stdout):
        if m.group(1) in Y:
            dec += int(m.group(2)); non += int(m.group(3))
    acc = {}
    with open(out, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            k = (row["ownership"], row["year"], row["heating_system"])
            acc[k] = acc.get(k, 0.0) + float(row["installed_current"])
    os.remove(out)
    tot = sum(v for (o, y, _), v in acc.items() if o == "TOTAL" and y == "2022")
    row = {"retained_index": i, "lambda": e["sampled"]["gumbelScaleUtil"], "decisions_2023_24": dec,
           "nonmax_2023_24": non, "nonmax_share_pct": 100 * non / max(dec, 1)}
    for o in OWNERS:
        a, b = acc.get((o, "2022", T[0]), 0.0), acc.get((o, "2024", T[0]), 0.0)
        row[f"gas_change_pp_{o}"] = 100 * (b - a) / tot
    tot24 = sum(v for (o, y, _), v in acc.items() if o == "TOTAL" and y == "2024")
    row["sim_gas_2024_pct"] = 100 * acc.get(("TOTAL", "2024", T[0]), 0.0) / tot24
    return row


def engine_rows(cal, R, scope, iterations, xmx, jobs, rerun):
    path = os.path.join(cal, "calibration_report_runs.csv")
    have = {}
    if os.path.exists(path) and not rerun:
        with open(path, newline="", encoding="utf-8") as f:
            have = {int(r["retained_index"]): {k: float(v) for k, v in r.items()} for r in csv.DictReader(f)}
    todo = [i for i in range(len(R)) if i not in have]
    if todo:
        cw.ensure_built(False)
        print(f"[engine] {len(todo)} calibration runs (2022-2024, {iterations} iterations, {jobs} in parallel) ...",
              flush=True)
        done = 0
        with ThreadPoolExecutor(max_workers=jobs) as ex:
            for row in ex.map(lambda i: engine_run(i, R[i], scope, iterations, xmx), todo):
                have[row["retained_index"]] = row; done += 1
                if done % 10 == 0 or done == len(todo):
                    print(f"[engine]   {done}/{len(todo)}", flush=True)
                    rows = [have[k] for k in sorted(have)]
                    with open(path, "w", newline="", encoding="utf-8") as f:
                        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    return [have[i] for i in range(len(R))], path


# ------------------------------------------------------------------ report

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("calib", nargs="?", default=os.path.join(ROOT, "results", "calib1000_v3"))
    ap.add_argument("--jobs", type=int, default=2, help="parallel engine runs (each takes --xmx memory)")
    ap.add_argument("--xmx", default="6g")
    ap.add_argument("--rerun", action="store_true", help="repeat the engine runs even if cached")
    ap.add_argument("--no-engine", action="store_true", help="skip sections E (decisions) and F")
    a = ap.parse_args()
    cal = a.calib if os.path.isabs(a.calib) else os.path.join(ROOT, a.calib)
    sys.stdout = Tee(os.path.join(cal, "log_calibration_report.txt"))

    d = json.load(open(os.path.join(cal, "calibration_search.json"), encoding="utf-8"))
    S, R = d["samples"], d["retained"]
    obs, sdmc, sdobs, disc, cut = d["observed"], d["sd_mc"], d["sd_obs"], d["sd_disc"], d["implaus_cut"]
    scope = d["scope"]
    keep = np.array([s["implausibility_max"] < cut for s in S])
    assert keep.sum() == len(R), "retained list and implausibility flags disagree"
    print(f"calibration: {os.path.relpath(cal, ROOT)}  scope {scope}  iterations {d['iterations']}")

    # ---------------------------------------------------------------- A
    head("A  Fit to the observed 2022-2024 heating mix")
    obs22, _ = cw.observed_mix(scope, 2022)
    print(f"{'observed (%)':22s}{'2022':>8s}{'2023':>8s}{'2024':>8s}{'change 22-24':>14s}")
    for t in T:
        print(f"  {SHORT[t]:20s}{obs22[t]:8.2f}{obs['2023'][t]:8.2f}{obs['2024'][t]:8.2f}{obs['2024'][t] - obs22[t]:+14.2f}")
    print(f"  gas boiler change 2022-23 {obs['2023'][T[0]] - obs22[T[0]]:+.2f}, 2023-24 {obs['2024'][T[0]] - obs['2023'][T[0]]:+.2f} pp")
    persist = np.mean([abs(obs[y][t] - obs22[t]) for y in Y for t in T])
    mad = np.array([np.mean([abs(v) for v in deviations(s, obs).values()]) for s in S])
    worst = np.array([max(abs(v) for v in deviations(s, obs).values()) for s in S])
    print(f"\nsampled {len(S)}, retained {keep.sum()} (max implausibility < {cut})")
    print(f"mean absolute deviation (mean over 2 years x 4 technologies, pp):")
    print(f"  retained {mad[keep].min():.2f}-{mad[keep].max():.2f} (median {np.median(mad[keep]):.2f});"
          f" all sampled median {np.median(mad):.2f}, p10 {np.percentile(mad, 10):.2f}, p90 {np.percentile(mad, 90):.2f}")
    print(f"  default weights {d['default_mad']:.2f};  a model that stays at the 2022 mix {persist:.2f}"
          f" (beaten by {100 * (mad < persist).mean():.0f} % of all sampled sets)")
    print(f"worst deviation (largest of the 8 scored outputs, the y-axis of fig_ensemble_fit, pp):")
    print(f"  retained {worst[keep].min():.2f}-{worst[keep].max():.2f};  all sampled median {np.median(worst):.2f}")
    ws = np.sort(worst)
    print("  ranked: " + ", ".join(f"#{r}: {ws[r - 1]:.2f}" for r in (124, 125, 200, 300, 400, 500, 600, 650, 700, 800)))
    plateau = (worst >= 2.8) & (worst <= 3.0)
    which = {}
    for s, p in zip(S, plateau):
        if p:
            dv = deviations(s, obs); k = max(dv, key=lambda k: abs(dv[k]))
            lab = f"{k[0]} {SHORT[k[1]]} {'too high' if dv[k] > 0 else 'too low'}"
            which[lab] = which.get(lab, 0) + 1
    print(f"  plateau 2.8-3.0 pp: {plateau.sum()} sets ({100 * plateau.mean():.0f} %); worst output there: "
          + ", ".join(f"{k} {v}" for k, v in sorted(which.items(), key=lambda kv: -kv[1])))

    # ---------------------------------------------------------------- B
    head("B  Retention criterion (implausibility)")
    print(f"sd_MC per technology (pp): " + ", ".join(f"{SHORT[t]} {sdmc[t]:.3f}" for t in T))
    print(f"sd_obs (pp): " + ", ".join(f"{y}: {min(sdobs[y][t] for t in T):.2f}-{max(sdobs[y][t] for t in T):.2f}" for y in Y))
    print(f"sd_disc {disc} pp, cutoff I < {cut}")
    adm = [cut * np.sqrt(sdmc[t] ** 2 + sdobs[y][t] ** 2 + disc ** 2) for y in Y for t in T]
    print(f"admitted deviation per scored output: {min(adm):.2f}-{max(adm):.2f} pp")
    print(f"\nretained sets by discrepancy allowance and cutoff (recomputed | stored):")
    stored = {(r["sd_disc"], r["cutoff"]): r["n_retained"] for r in d.get("retention_sensitivity", [])} \
        if isinstance(d.get("retention_sensitivity"), list) and d["retention_sensitivity"] and isinstance(d["retention_sensitivity"][0], dict) else {}
    print(f"  {'sd_disc':>8s}" + "".join(f"{'I<' + str(c):>12s}" for c in (2.5, 3.0, 3.5)))
    for dd in (0.25, 0.5, 1.0):
        cells = []
        for c in (2.5, 3.0, 3.5):
            n = sum(imax(s, obs, sdmc, sdobs, dd) < c for s in S)
            cells.append(f"{n:5d} | {stored.get((dd, c), '-')!s:>4s}")
        print(f"  {dd:8.2f}" + "".join(f"{c:>12s}" for c in cells))

    # ---------------------------------------------------------------- C
    head("C  Residuals of the retained sets (simulated - observed, pp) and stricter criteria")
    for y in Y:
        for t in T:
            r = np.array([s["mix"][y][t] - obs[y][t] for s, k in zip(S, keep) if k])
            print(f"  {y} {SHORT[t]:18s} mean {r.mean():+.2f}  range {r.min():+.2f} to {r.max():+.2f}"
                  f"  ({100 * (r > 0).mean():.0f} % of sets too high)")
    E_all = effective(S)
    lam_all = np.array([s["sampled"]["gumbelScaleUtil"] for s in S])
    print(f"\n  {'criterion':28s}{'sets':>6s}{'eff. attitude':>16s}{'eff. cost':>12s}{'lambda':>16s}{'max MAD':>9s}")
    for dd, c in [(0.5, 3.0), (0.25, 3.0), (0.5, 2.5), (0.25, 2.5)]:
        k = np.array([imax(s, obs, sdmc, sdobs, dd) < c for s in S])
        print(f"  {'sd_disc ' + str(dd) + ', I < ' + str(c):28s}{k.sum():6d}"
              f"{E_all['attitude'][k].min():8.2f}-{E_all['attitude'][k].max():.2f}"
              f"{E_all['cost'][k].min():6.2f}-{E_all['cost'][k].max():.2f}"
              f"{lam_all[k].min():9.4f}-{lam_all[k].max():.4f}{mad[k].max():9.2f}")

    # ---------------------------------------------------------------- D
    head("D  Decision weights")
    g = lambda key: np.array([s["sampled"][key] for s in S])

    def width(v, log=False):
        v = np.log(v) if log else v
        p, q = (v.min(), v.max()), (v[keep].min(), v[keep].max())
        return (q[1] - q[0]) / (p[1] - p[0])
    print(f"  {'weight (share within stage)':30s}{'sampled':>14s}{'retained':>14s}{'width min-max':>15s}")
    for k in SH:
        v = g(k)
        print(f"  {k:30s}{v.min():8.2f}-{v.max():.2f}{v[keep].min():8.2f}-{v[keep].max():.2f}{100 * width(v):14.0f}%")
    v = lam_all
    print(f"  {'noise scale lambda':30s}{v.min():8.3f}-{v.max():.3f}{v[keep].min():8.3f}-{v[keep].max():.3f}"
          f"{100 * width(v, log=True):14.0f}% (log scale)")
    x, yv = E_all["attitude"], E_all["cost"]
    c1, c0 = np.polyfit(x[keep], yv[keep], 1)
    res = yv[keep] - (c0 + c1 * x[keep])
    idx = yv - c1 * x
    p, q = (idx.min(), idx.max()), (idx[keep].min(), idx[keep].max())
    print(f"  {'cost + %.1f x attitude' % -c1:30s}{'':28s}{100 * (q[1] - q[0]) / (p[1] - p[0]):14.0f}%")
    print(f"\nretention rate per fifth of the sampled range:")
    for k in SH + ["gumbelScaleUtil"]:
        v = g(k)
        edges = np.geomspace(v.min(), v.max(), 6) if k == "gumbelScaleUtil" else np.linspace(v.min(), v.max(), 6)
        rates = [100 * keep[(v >= lo) & (v <= hi)].mean() for lo, hi in zip(edges[:-1], edges[1:])]
        print(f"  {k:20s}" + "  ".join(f"{r:5.1f}%" for r in rates)
              + ("   (log-spaced)" if k == "gumbelScaleUtil" else ""))
    print(f"\ncost-attitude line through the retained sets: cost = {c0:.3f} {c1:+.3f} x attitude")
    print(f"  correlation retained {np.corrcoef(x[keep], yv[keep])[0, 1]:+.3f}, all sampled {np.corrcoef(x, yv)[0, 1]:+.3f}")
    print(f"  residual sd {res.std():.3f}; share of its variance explained by log(lambda) "
          f"{np.corrcoef(np.log(lam_all[keep]), res)[0, 1] ** 2:.2f}")
    mid = (lam_all[keep] >= 0.015) & (lam_all[keep] <= 0.025)
    print(f"  residual sd among retained sets with lambda 0.015-0.025: {res[mid].std():.3f} (n={mid.sum()})")
    dist = np.abs(yv - (c0 + c1 * x))
    for lo, hi in [(0, .03), (.03, .06), (0, .06), (.06, .10), (.10, 9)]:
        m = (dist >= lo) & (dist < hi)
        print(f"  sampled sets {lo:.2f}-{min(hi, 1):.2f} from the line: {m.sum():4d}, retained {100 * keep[m].mean():5.1f} %")
    print(f"  retained effective attitude {x[keep].min():.2f}-{x[keep].max():.2f}, cost {yv[keep].min():.2f}-{yv[keep].max():.2f}")

    # ---------------------------------------------------------------- E, F
    if not a.no_engine:
        rows, path = engine_rows(cal, R, scope, d["iterations"], a.xmx, a.jobs, a.rerun)
        head("E  Noise scale: share of homeowner decisions 2023-2024 not on the highest modelled utility")
        nm = np.array([r["nonmax_share_pct"] for r in rows]); lr = np.log([r["lambda"] for r in rows])
        print(f"  all {len(rows)} retained sets: {nm.min():.1f}-{nm.max():.1f} % (median {np.median(nm):.1f}, "
              f"p10 {np.percentile(nm, 10):.1f}, p90 {np.percentile(nm, 90):.1f}); correlation with log(lambda) "
              f"{np.corrcoef(lr, nm)[0, 1]:+.2f}")
        chk = np.array([abs(r["sim_gas_2024_pct"] - R[i]["mix"]["2024"][T[0]]) for i, r in enumerate(rows)])
        print(f"  reproducibility check: 2024 gas share of these runs vs the stored calibration, max |diff| {chk.max():.3f} pp")
        head("F  Change in gas boilers 2022-2024 by ownership (pp of all dwellings), all retained sets")
        tot = np.array([r["gas_change_pp_TOTAL"] for r in rows])
        own = np.array([r["gas_change_pp_PRIVATELY_OWNED"] for r in rows])
        cost_only = np.array([r["gas_change_pp_PRIVATELY_RENTED"] + r["gas_change_pp_SOCIAL_HOUSING"] for r in rows])
        print(f"  total {tot.min():+.2f} to {tot.max():+.2f};  homeowners {own.min():+.2f} to {own.max():+.2f};"
              f"  landlords + housing associations {cost_only.min():+.2f} to {cost_only.max():+.2f}")
        sh = 100 * own / tot
        print(f"  homeowner share of the decline: {sh.min():.0f}-{sh.max():.0f} % (median {np.median(sh):.0f} %)")
        print(f"  observed decline {obs['2024'][T[0]] - obs22[T[0]]:+.2f} pp; minus the cost-only part (median "
              f"{np.median(cost_only):+.2f}) leaves {obs['2024'][T[0]] - obs22[T[0]] - np.median(cost_only):+.2f} pp for homeowners")
        print(f"\n  per-set values: {os.path.relpath(path, ROOT)}")

    # ---------------------------------------------------------------- G
    head("G  2050 share across the retained sets (baseline, subsample_validation runs)")
    vdir = os.path.join(cal, "subsample_validation")
    Y50, nits = [], set()
    for i in range(len(R)):
        acc, tot = {}, {}
        with open(os.path.join(vdir, f"set{i:03d}.csv"), newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["ownership"] != "TOTAL" or r["year"] != "2050" or r.get("scenario_name", "baseline") != "baseline":
                    continue
                it = r["iteration"]; tot[it] = tot.get(it, 0.0) + float(r["installed_current"])
                acc[(it, r["heating_system"])] = float(r["installed_current"])
        nits.add(len(tot))
        Y50.append(np.mean([[100 * acc.get((it, t), 0.0) / tot[it] for t in T] for it in tot], axis=0))
    Y50 = np.array(Y50)
    print(f"  iterations per set: {sorted(nits)}; run dates: {os.path.relpath(vdir, ROOT)}")
    Er = effective(R)
    Ew = np.column_stack([Er["attitude"], Er["cost"], Er["social"], Er["effort"]])
    Wr = np.array([[e["sampled"][k] for k in SH] for e in R])
    Lr = np.log([e["sampled"]["gumbelScaleUtil"] for e in R])
    print(f"  R2 of the 2050 share on the calibrated parameters (LOO = leave-one-out):")
    print(f"  {'':18s}{'mean':>6s}{'min':>6s}{'max':>6s} | {'linear shares':>13s}{'+lam':>6s} | "
          f"{'quad. eff. weights':>19s}{'LOO':>6s} | {'quad. eff.+lam':>15s}{'LOO':>6s} | corr log(lam)")
    for j, t in enumerate(T):
        yv = Y50[:, j]
        qe, ql = quad(Ew), quad(np.column_stack([Ew, Lr]))
        print(f"  {SHORT[t]:18s}{yv.mean():6.1f}{yv.min():6.1f}{yv.max():6.1f} | {r2(Wr, yv):13.2f}"
              f"{r2(np.column_stack([Wr, Lr]), yv):6.2f} | {r2(qe, yv):19.2f}{loo_r2(qe, yv):6.2f} | "
              f"{r2(ql, yv):15.2f}{loo_r2(ql, yv):6.2f} | {np.corrcoef(Lr, yv)[0, 1]:+.2f}")
    print("  quadratic = all linear terms, squares and pairwise products of the four effective weights"
          " (attitude, cost, social norm, effort) and, where stated, log(lambda)")
    # the plain comparison used in the text: most vs least random choice at similar weights
    lam_r = np.exp(Lr)
    lo, hi = lam_r <= np.percentile(lam_r, 100 / 3), lam_r >= np.percentile(lam_r, 200 / 3)
    Z = np.column_stack([Er["attitude"], Er["cost"], Er["social"]]); Z = (Z - Z.mean(0)) / Z.std(0)
    lo_i = np.where(lo)[0]
    pairs = [(h, lo_i[np.argmin(np.linalg.norm(Z[lo_i] - Z[h], axis=1))]) for h in np.where(hi)[0]]
    print(f"\n  most vs least random choice at similar weights: each set in the upper third of lambda"
          f" (>= {np.percentile(lam_r, 200 / 3):.4f}) paired with the set in the lower third (<= {np.percentile(lam_r, 100 / 3):.4f})"
          f" nearest in standardised effective attitude, cost and social-norm weight; {len(pairs)} pairs,"
          f" median distance {np.median([np.linalg.norm(Z[h] - Z[l]) for h, l in pairs]):.2f} sd")
    for j, t in enumerate(T):
        diff = np.array([Y50[h, j] - Y50[l, j] for h, l in pairs])
        print(f"    {SHORT[t]:18s} 2050 share, more random minus less random: median {np.median(diff):+5.1f} pp,"
              f" mean {diff.mean():+5.1f}, positive in {100 * (diff > 0).mean():.0f} % of pairs")
    print(f"    (unpaired, median of upper vs lower third: " + ", ".join(
        f"{SHORT[t]} {np.median(Y50[hi, j]) - np.median(Y50[lo, j]):+.1f}" for j, t in enumerate(T)) + ")")

    # ---------------------------------------------------------------- H
    head("H  The scenario subsample")
    sub = json.load(open(os.path.join(cal, "calibration_subsample.json"), encoding="utf-8"))
    info, scr = sub["subsample"], sub["screen_sets"]
    chosen = [e["retained_index_full"] for e in sub["retained"]]
    print(f"  {info['n']} of {info['n_full']} retained sets; method: {info['method']}")
    if "n_reused" in info:
        print(f"  {info['n_reused']} of them already had scenario runs ({info.get('reused_from')})")
    print(f"  Morris screening sets (indices into retained): {scr['morris_full_index']}; LHS / central set: {scr['lhs_full_index']}")
    morris, central = ss.screen_sets(R)
    print(f"  recomputed over all retained sets: Morris {morris}, central {central}; best fit {int(np.argmin([e['mad'] for e in R]))}")
    _, X = ss.standardised(R)
    dist = np.linalg.norm(X, axis=1); c = scr["lhs_full_index"]
    print(f"  the central set used, retained[{c}], ranks {int((dist < dist[c]).sum()) + 1} of {len(R)} by distance to the"
          f" centre of all retained sets (standardised attitude, social-norm, effort weight and log lambda)")
    mm = os.path.join(cal, "calibration_subsample_maximin.json")
    if os.path.exists(mm):
        first = [e["retained_index_full"] for e in json.load(open(mm, encoding="utf-8"))["retained"]]
        print(f"  it is the central set of the first, space-filling subsample ({os.path.basename(mm)}):"
              f" {ss.screen_sets(R, among=first)[1]}")
    for lab, i in [("best fit / central", scr["lhs_full_index"])] + [("Morris", i) for i in scr["morris_full_index"]]:
        print(f"    {lab:18s} retained[{i:3d}]  eff. attitude {Er['attitude'][i]:.2f}  cost {Er['cost'][i]:.2f}"
              f"  social {Er['social'][i]:.2f}  lambda {R[i]['sampled']['gumbelScaleUtil']:.3f}")
    years = [2030, 2035, 2040, 2045, 2050]
    rows_ = [ss.baseline_shares(os.path.join(vdir, f"set{i:03d}.csv"), years) for i in range(len(R))]
    matched = [int(y) for y in re.findall(r"(\d{4})", info["method"].split("shares in")[1].split("best")[0])]
    print(f"  matched years {matched}; others held out")
    print(f"  {'':12s}{'all 124: p5 / median / p95':>30s}{'subsample: p5 / median / p95':>32s}{'max |diff|':>12s}")
    diffs = {"matched": {"median": [], "p5p95": []}, "held out": {"median": [], "p5p95": []}}
    for y in years:
        for tt in ss.OUT_TECHS:
            v = np.array([r[f"{y}{tt}"] for r in rows_])
            f_, s_ = np.percentile(v, [5, 50, 95]), np.percentile(v[chosen], [5, 50, 95])
            grp = "matched" if y in matched else "held out"
            diffs[grp]["median"].append((abs(s_[1] - f_[1]), f"{y} {tt}"))
            diffs[grp]["p5p95"] += [(abs(s_[0] - f_[0]), f"{y} {tt} p5"), (abs(s_[2] - f_[2]), f"{y} {tt} p95")]
            print(f"  {y} {tt:6s}{f_[0]:12.1f}{f_[1]:8.1f}{f_[2]:8.1f}{s_[0]:14.1f}{s_[1]:8.1f}{s_[2]:8.1f}"
                  f"{np.abs(s_ - f_).max():12.1f}{'   held out' if grp == 'held out' else ''}")
    for grp, dd in diffs.items():
        if dd["median"]:
            mm, pp = max(dd["median"]), max(dd["p5p95"])
            pp2 = sorted(dd["p5p95"])[-2]
            print(f"  {grp}: medians within {mm[0]:.1f} pp ({mm[1]}); 5/95 % within {pp[0]:.1f} pp ({pp[1]})"
                  f", next largest {pp2[0]:.1f} pp ({pp2[1]})")
    print(f"\nwrote {os.path.relpath(os.path.join(cal, 'log_calibration_report.txt'), ROOT)}")


if __name__ == "__main__":
    main()
