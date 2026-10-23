#!/usr/bin/env python3
"""Pick a representative subsample of the retained weight sets for the scenario runs, and check it.

WHY. The calibration can retain more sets than the scenario runs can afford at national scale. The
retained sets are not spread evenly: they lie on a line in the plane of the effective cost and
attitude weights (ensemble_structure.py), and the position on that line -- cost-driven to
attitude-driven households -- is the main way they differ. The subsample must therefore cover that
line from end to end, and the social-norm weight, which the 2022-24 data leaves largely open but
which drives tipping once heat-pump shares rise.

HOW (select). Every retained set is placed in a space of three effective weights in the final
decision (products of the sampled shares, see ensemble_structure.py): attitude path (= position on
the line), social-norm path and effort path, each standardised over the retained sets. The
subsample starts from three anchors -- the best-fitting set and the two ends of the line -- and is
then grown by a space-filling (greedy maximin) pick: each next set is the retained set whose
distance to its nearest already-chosen set is largest. This fills the retained cloud evenly,
reaches its corners and never picks two near-identical sets. The chosen sets count equally: the
retained sets are 'not ruled out' rather than a probability sample, so their density carries no
evidence and is not reweighted. (Each entry still records how many retained sets it is the nearest
chosen set for, subsample_represents, as a diagnostic.)

    python results_analysis/select_subsample.py select   results/calib1000 --n 25
    python results_analysis/select_subsample.py validate results/calib1000 --scope province:Noord-Brabant

select   writes <dir>/calibration_subsample.json: the search file with 'retained' replaced by the
         subsample (each entry carries retained_index_full and subsample_represents), plus
         <dir>/fig_subsample.png. Use it as --weights-file / --search for run.py and pathway_batch.py.
validate runs the BASELINE 2024-2050 once for every retained set (1 iteration each, resumable,
         outputs in <dir>/subsample_validation/) and compares the full band with the subsample band:
         the subsample passes when, per technology and year, its min-max covers the full ensemble's
         5-95 % range (within --cover-tol pp). The subsample and full medians are reported for
         information.
"""
import argparse
import copy
import csv
import json
import os
import shutil
import subprocess
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
ENGINE_CP = os.environ.get("HT_ENGINE_CP",
                           os.path.join(ROOT, "model", "engine-java", "build", "classes", "java", "main"))
STOCK = os.path.join(ROOT, "model", "data", "stock")
TECHS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP",
         "DISTRICT_HEATING"]


def effective(r):
    """Effective weights in the final decision (see ensemble_structure.py)."""
    s = r["sampled"]
    att, sn, aff, it = s["shareAttitude"], s["shareSocialnorm"], s["shareAffordability"], s["shareIntention"]
    pbc_total = (1 - it) + it * (1 - att - sn)
    return {"attitude": it * att, "social": it * sn, "effort": (1 - aff) * pbc_total,
            "cost": aff * pbc_total}


def standardised(R):
    """Retained sets in standardised (attitude, social-norm, effort) effective-weight space, plus
    log(noise scale) when the calibration sampled it (gumbelScaleUtil): it sets how strongly the
    utility differences decide, so sets that differ only in it can behave very differently."""
    E = [effective(r) for r in R]
    cols = [[e[k] for k in ("attitude", "social", "effort")] for e in E]
    if all("gumbelScaleUtil" in r["sampled"] for r in R):
        cols = [c + [float(np.log(r["sampled"]["gumbelScaleUtil"]))] for c, r in zip(cols, R)]
    if all("shareInvest" in r["sampled"] for r in R):     # split PBC: how the cost weight divides
        cols = [c + [r["sampled"]["shareInvest"] / (r["sampled"]["shareInvest"] + r["sampled"]["shareRunning"])]
                for c, r in zip(cols, R)]
    X = np.array(cols)
    return E, (X - X.mean(0)) / X.std(0)


def screen_sets(R, among=None):
    """Retained sets (indices into R) for the parameter screen, taken from `among` (the subsample):
      morris: best fit, both ends of the cost-attitude line (lowest / highest effective attitude
              weight) and the highest effective social-norm weight -- the Morris ranking is repeated
              at each, because the influence of the salience parameters runs through the social norm
              and the influence of the cost parameters through the position on the line;
      lhs:    the central set, the retained set closest to the centre of the retained sets in the
              standardised space (an actual evaluated set, unlike a vector of marginal medians)."""
    E, X = standardised(R)
    among = list(range(len(R))) if among is None else list(among)
    best = min(among, key=lambda i: R[i]["mad"])
    lo = min(among, key=lambda i: E[i]["attitude"])
    hi = max(among, key=lambda i: E[i]["attitude"])
    sn = max(among, key=lambda i: E[i]["social"])
    central = min(among, key=lambda i: float(np.linalg.norm(X[i])))
    return list(dict.fromkeys([best, lo, hi, sn])), central


def screen_indices(search_path):
    """The screen sets as indices into the 'retained' list of `search_path`, which may be the full
    calibration_search.json or calibration_subsample.json. Every script resolves the names 'central'
    and 'morris' through this function, so the price arms, knock-outs, Morris and LHS all use the
    same sets. Returns (morris_indices, central_index, source)."""
    d = json.load(open(search_path, encoding="utf-8"))
    ret = d.get("retained") or []
    if not ret:
        sys.exit(f"no retained sets in {search_path}")
    ss = d.get("screen_sets")
    if ss is not None:                                   # the subsample file itself
        pos = {e["retained_index_full"]: i for i, e in enumerate(ret)}
        return [pos[i] for i in ss["morris_full_index"]], pos[ss["lhs_full_index"]], "subsample"
    sub = os.path.join(os.path.dirname(os.path.abspath(search_path)), "calibration_subsample.json")
    if os.path.exists(sub):                              # full file with its subsample beside it
        s = json.load(open(sub, encoding="utf-8"))
        ok = s["subsample"]["n_full"] == len(ret) and all(
            e["weights"] == ret[e["retained_index_full"]]["weights"] for e in s["retained"])
        if not ok:
            sys.exit(f"{sub} does not belong to {search_path} (was the calibration rerun?). "
                     f"Rerun: python results_analysis/select_subsample.py select {os.path.dirname(sub)}")
        ss = s["screen_sets"]
        return list(ss["morris_full_index"]), ss["lhs_full_index"], "subsample beside the search file"
    morris, central = screen_sets(ret)                   # no subsample: pick among all retained
    return morris, central, "all retained sets (no subsample file)"


def greedy_maximin(X, anchors, n):
    chosen = list(dict.fromkeys(anchors))
    dmin = np.min([np.linalg.norm(X - X[c], axis=1) for c in chosen], axis=0)
    while len(chosen) < min(n, len(X)):
        nxt = int(np.argmax(dmin))
        chosen.append(nxt)
        dmin = np.minimum(dmin, np.linalg.norm(X - X[nxt], axis=1))
    return chosen


def do_select(a):
    src = os.path.join(a.calib, "calibration_search.json")
    d = json.load(open(src, encoding="utf-8"))
    R = d["retained"]
    if len(R) <= a.n:
        sys.exit(f"only {len(R)} retained sets: no subsample needed")
    E, X = standardised(R)
    att = np.array([e["attitude"] for e in E])
    best = int(np.argmin([r["mad"] for r in R]))
    anchors = [best, int(np.argmin(att)), int(np.argmax(att))]
    chosen = greedy_maximin(X, anchors, a.n)
    # weight = number of retained sets whose nearest chosen set it is
    near = np.argmin(np.array([np.linalg.norm(X - X[c], axis=1) for c in chosen]), axis=0)
    counts = np.bincount(near, minlength=len(chosen))

    out = copy.deepcopy(d)
    sub = []
    for j, c in enumerate(chosen):
        e = dict(R[c]); e["subsample_represents"] = int(counts[j]); e["retained_index_full"] = int(c)
        sub.append(e)
    out["retained"] = sub
    out["subsample"] = {"source": os.path.relpath(src, ROOT), "n": len(chosen), "n_full": len(R),
                        "method": "greedy maximin in standardised (attitude, social-norm, effort) "
                                  "effective-weight space; anchors: best fit and both ends of the "
                                  "cost-attitude line; sets count equally",
                        "anchors_full_index": anchors}
    morris, central = screen_sets(R, chosen)
    out["screen_sets"] = {"morris_full_index": morris, "lhs_full_index": central,
                          "note": "indices into 'retained' of the full calibration_search.json"}
    dst = os.path.join(a.calib, "calibration_subsample.json")
    json.dump(out, open(dst, "w", encoding="utf-8"), indent=1)

    print(f"{len(chosen)} of {len(R)} retained sets selected (anchors: best fit #{best}, "
          f"line ends #{anchors[1]} and #{anchors[2]})")
    print(f"each set is the nearest chosen set for {counts.min()}-{counts.max()} retained sets (diagnostic)")
    print(f"{'':26s}{'full min-max':>16s}{'subsample min-max':>20s}")
    for k in ("attitude", "social", "effort", "cost"):
        v = np.array([e[k] for e in E]); w = v[chosen]
        print(f"  {k + ' path':24s}{v.min():8.2f}-{v.max():.2f}{w.min():12.2f}-{w.max():.2f}")
    for k in ("shareAttitude", "shareSocialnorm", "shareAffordability", "shareIntention"):
        v = np.array([r["sampled"][k] for r in R]); w = v[chosen]
        print(f"  {k:24s}{v.min():8.2f}-{v.max():.2f}{w.min():12.2f}-{w.max():.2f}")
    print("parameter-screen sets (from the subsample):")
    for lab, i in [("Morris", j) for j in morris] + [("LHS (central)", central)]:
        e = E[i]
        print(f"  {lab:14s} retained:{i:<4d} attitude {e['attitude']:.2f}  cost {e['cost']:.2f}  "
              f"social {e['social']:.2f}  effort {e['effort']:.2f}  MAD {R[i]['mad']:.2f}")
    print(f"\nwrote {dst}")
    _plot(a.calib, d, E, chosen)


def _plot(calib, d, E, chosen):
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    x = np.array([e["attitude"] for e in E]); y = np.array([e["cost"] for e in E])
    s = np.array([e["social"] for e in E])
    fig, ax = plt.subplots(figsize=(6.4, 5.0))
    ax.scatter(x, y, s=14, color="#c8c8c8", lw=0, zorder=1)
    sc = ax.scatter(x[chosen], y[chosen], s=44, c=s[chosen], cmap="Blues", vmin=0, vmax=s.max(),
                    edgecolor="#222222", lw=0.6, zorder=3)
    ax.set_xlabel("effective weight of attitude in the decision", fontsize=10)
    ax.set_ylabel("effective weight of cost in the decision", fontsize=10)
    ax.set_title(f"Subsample of {len(chosen)} of the {len(E)} retained sets", fontsize=11, loc="left")
    cb = fig.colorbar(sc, ax=ax, fraction=0.04, pad=0.02)
    cb.set_label("effective weight of social norm", fontsize=9)
    ax.legend(handles=[Line2D([], [], marker="o", ls="", color="#c8c8c8", markersize=5, label="retained"),
                       Line2D([], [], marker="o", ls="", markerfacecolor="#9ecae1",
                              markeredgecolor="#222222", markersize=7, label="selected")],
              loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=2, frameon=False, fontsize=8.5)
    ax.grid(alpha=.2); ax.set_axisbelow(True)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    out = os.path.join(calib, "fig_subsample.png")
    fig.savefig(out, dpi=170, bbox_inches="tight"); plt.close(fig)
    print(f"wrote {out}")


OUT_TECHS = ["NAT", "HYB", "ELE", "DIS"]          # gas boiler + block merged
Q_MATCH = np.array([5, 25, 50, 75, 95])


def baseline_shares(path, years):
    """{'<year><TECH>': % of dwellings} for the baseline in one result CSV (gas block merged into gas),
    plus 'tip50', the first year natural gas is below 50 % (2051 if never)."""
    acc = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["ownership"] == "TOTAL" or r.get("scenario_name", "baseline") != "baseline":
                continue
            t = r["heating_system"]; t = "NAT" if t.startswith("NATURAL_GAS") else t[:3]
            d = acc.setdefault(int(r["year"]), {k: 0.0 for k in OUT_TECHS})
            d[t] += float(r["installed_current"])
    sh = {y: {t: 100 * v / sum(d.values()) for t, v in d.items()} for y, d in acc.items()}
    out = {f"{y}{t}": sh[y][t] for y in years for t in OUT_TECHS}
    out["tip50"] = next((y for y in sorted(sh) if sh[y]["NAT"] < 50), 2051)
    return out


def do_select_outcome(a):
    """Pick n retained sets whose baseline outcomes in the validation region reproduce the quantiles
    (5/25/50/75/95 %) of ALL retained sets, per technology, in the --match years. Equal weights, so
    plain medians and percentiles over the subsample stand in for those of the full ensemble. Needs
    `validate` to have run every retained set. The screen sets of the existing subsample are kept, so
    the Morris/LHS results stay valid; --prefer favours sets that already have scenario runs."""
    src = os.path.join(a.calib, "calibration_search.json")
    d = json.load(open(src, encoding="utf-8")); R = d["retained"]
    vdir = os.path.join(a.calib, "subsample_validation")
    match_years = [int(y) for y in a.match.split(",")]
    check_years = [int(y) for y in a.check.split(",") if y]
    missing = [i for i in range(len(R)) if not os.path.exists(os.path.join(vdir, f"set{i:03d}.csv"))]
    if missing:
        sys.exit(f"{len(missing)} retained sets have no validation run: run 'validate' first")
    rows = [baseline_shares(os.path.join(vdir, f"set{i:03d}.csv"), match_years + check_years) for i in range(len(R))]
    cols = [f"{y}{t}" for y in match_years for t in OUT_TECHS]
    chk = [f"{y}{t}" for y in check_years for t in OUT_TECHS] + ["tip50"]
    V = {c: np.array([r[c] for r in rows]) for c in cols + chk}
    qf = {c: np.percentile(V[c], Q_MATCH) for c in V}
    sd = {c: V[c].std() or 1.0 for c in V}

    def mismatch(idx, cs):
        return float(sum((((np.percentile(V[c][idx], Q_MATCH) - qf[c]) / sd[c]) ** 2).sum() for c in cs))

    cur_path = os.path.join(a.calib, "calibration_subsample.json")
    cur = json.load(open(cur_path, encoding="utf-8")) if os.path.exists(cur_path) else None
    if cur is not None and cur.get("screen_sets"):
        ss = cur["screen_sets"]
        keep = list(dict.fromkeys(ss["morris_full_index"] + [ss["lhs_full_index"]]))
        screen = {"morris_full_index": ss["morris_full_index"], "lhs_full_index": ss["lhs_full_index"],
                  "note": ss.get("note", "indices into 'retained' of the full calibration_search.json")}
    else:
        morris, central = screen_sets(R)
        keep = list(dict.fromkeys(morris + [central]))
        screen = {"morris_full_index": morris, "lhs_full_index": central,
                  "note": "indices into 'retained' of the full calibration_search.json"}
    prefer = set()
    if a.prefer:
        prefer = {e["retained_index_full"] for e in json.load(open(a.prefer, encoding="utf-8"))["retained"]}

    def objective(idx):
        return mismatch(idx, cols) + a.reuse_penalty * sum(1 for i in idx if i not in prefer)

    rng = np.random.default_rng(a.seed)
    free = [i for i in range(len(R)) if i not in keep]
    best, best_o = None, np.inf
    for restart in range(a.restarts):
        if restart == 0 and prefer:
            start = keep + [i for i in sorted(prefer) if i not in keep]
            start = start[:a.n] + list(rng.choice([i for i in free if i not in start], max(0, a.n - len(start)), replace=False))
        else:
            start = keep + list(rng.choice(free, a.n - len(keep), replace=False))
        idx, o, improved = list(start), objective(start), True
        while improved:                                   # first-improvement swaps until none helps
            improved = False
            for pos in range(len(keep), a.n):
                for k in rng.permutation(free):
                    if k in idx:
                        continue
                    trial = idx.copy(); trial[pos] = int(k)
                    ot = objective(trial)
                    if ot < o - 1e-12:
                        idx, o, improved = trial, ot, True
                        break
        if o < best_o:
            best, best_o = sorted(int(i) for i in idx), o
    chosen = best

    # diagnostic: for how many retained sets is each chosen set the nearest in outcome space
    Z = np.array([[(V[c][i] - V[c].mean()) / sd[c] for c in cols] for i in range(len(R))])
    near = np.argmin(np.array([np.linalg.norm(Z - Z[c], axis=1) for c in chosen]), axis=0)
    counts = np.bincount(near, minlength=len(chosen))

    if cur is not None:
        bak = os.path.join(a.calib, "calibration_subsample_" + cur.get("subsample", {}).get("method_tag", "maximin") + ".json")
        if not os.path.exists(bak):
            shutil.copy2(cur_path, bak); print(f"previous subsample kept as {bak}")
    out = copy.deepcopy(d)
    out["retained"] = []
    for j, c in enumerate(chosen):
        e = dict(R[c]); e["subsample_represents"] = int(counts[j]); e["retained_index_full"] = int(c)
        out["retained"].append(e)
    out["subsample"] = {"source": os.path.relpath(src, ROOT), "n": len(chosen), "n_full": len(R),
                        "method_tag": "outcome",
                        "method": f"quantile matching: the {len(chosen)} retained sets whose baseline "
                                  f"{a.scope} shares in {a.match} best reproduce the 5/25/50/75/95 % "
                                  f"quantiles of all {len(R)} retained sets, per technology (local search, "
                                  f"{a.restarts} restarts, seed {a.seed}); screen sets kept; sets count equally",
                        "validation_dir": os.path.relpath(vdir, ROOT),
                        "mismatch_selected": mismatch(chosen, cols), "mismatch_check": mismatch(chosen, chk)}
    if prefer:
        out["subsample"]["reused_from"] = os.path.relpath(a.prefer, ROOT)
        out["subsample"]["n_reused"] = len(set(chosen) & prefer)
    out["screen_sets"] = screen
    json.dump(out, open(cur_path, "w", encoding="utf-8"), indent=1)

    print(f"{len(chosen)} of {len(R)} retained sets; kept screen sets {keep}"
          + (f"; {len(set(chosen) & prefer)} already have runs in {os.path.basename(a.prefer)}" if prefer else ""))
    print(f"quantile mismatch (sum of squared standardised quantile errors): matched years "
          f"{mismatch(chosen, cols):.3f}, held-out {mismatch(chosen, chk):.3f}")
    if cur is not None:
        ci = [e["retained_index_full"] for e in cur["retained"]]
        print(f"   previous subsample:                               matched years "
              f"{mismatch(ci, cols):.3f}, held-out {mismatch(ci, chk):.3f}")
    print(f"\n{'':8s}{'all retained p5/med/p95':>26s}{'subsample p5/med/p95':>26s}")
    for c in cols + chk:
        f, s = np.percentile(V[c], [5, 50, 95]), np.percentile(V[c][chosen], [5, 50, 95])
        print(f"{c:8s}{f[0]:10.1f}{f[1]:8.1f}{f[2]:8.1f}{s[0]:10.1f}{s[1]:8.1f}{s[2]:8.1f}"
              + ("   held out" if c in chk else ""))
    print(f"\nwrote {cur_path}")
    E, _ = standardised(R)
    _plot(a.calib, d, E, chosen)


def scope_tag(scope):
    s = scope.strip().lower()
    if s in ("nl", "netherlands"): return "nl"
    kind, name = s.split(":", 1)
    slug = name.replace(" ", "_")
    return slug if kind == "province" else "gemeente_" + slug


def mix_2050(path, years):
    acc = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            y = int(r["year"])
            if r["ownership"] == "TOTAL" and y in years:
                acc.setdefault(y, {t: 0.0 for t in TECHS})[r["heating_system"]] += float(r["installed_current"])
    return {y: {t: 100 * v / sum(d.values()) for t, v in d.items()} for y, d in acc.items()}


def run_full_baselines(full, outdir, scope, xmx, jobs, end=2050):
    """Baseline 2024-<end>, 1 iteration, for every retained set into <outdir>/set<i>.csv; sets whose
    file exists are skipped (resumable). Runs `jobs` engines in parallel (each ~1-1.5 GB for a
    province)."""
    from concurrent.futures import ThreadPoolExecutor
    stock = os.path.join(STOCK, f"{scope_tag(scope)}_dwellings.csv")
    todo = [i for i in range(len(full)) if not os.path.exists(os.path.join(outdir, f"set{i:03d}.csv"))]

    def one(i):
        out = os.path.join(outdir, f"set{i:03d}.csv")
        cmd = ["java", f"-Xmx{xmx}"] + [f"-Dht.{k}={v}" for k, v in full[i]["weights"].items()] + \
              ["-cp", ENGINE_CP, "heattransition.Cli", "--real", stock, "--scenario", "baseline",
               "--iterations", "1", "--start", "2024", "--end", str(end), "--out", out]
        rc = subprocess.run(cmd, capture_output=True, text=True).returncode
        print(f"[validate] set {i} {'done' if rc == 0 else f'FAILED rc={rc}'}", flush=True)
        return i, rc

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
        failed = [i for i, rc in ex.map(one, todo) if rc != 0]
    if failed:
        sys.exit(f"engine run failed for sets {failed}")


def do_validate(a):
    full = json.load(open(os.path.join(a.calib, "calibration_search.json"), encoding="utf-8"))["retained"]
    subp = os.path.join(a.calib, "calibration_subsample.json")
    if not os.path.exists(subp):
        sys.exit("run 'select' first")
    sub = json.load(open(subp, encoding="utf-8"))["retained"]
    outdir = os.path.join(a.calib, "subsample_validation"); os.makedirs(outdir, exist_ok=True)
    years = [int(y) for y in a.years.split(",")]
    run_full_baselines(full, outdir, a.scope, a.xmx, a.jobs, end=max(years))
    mixes = [mix_2050(os.path.join(outdir, f"set{i:03d}.csv"), years) for i in range(len(full))]

    idx = [e["retained_index_full"] for e in sub]
    rows, ok = [], True
    print(f"\nbaseline, {a.scope}: full ensemble ({len(full)}) vs subsample ({len(sub)}), % of dwellings")
    print(f"{'':24s}{'year':>5s}{'full P5-P95':>15s}{'full min-max':>15s}{'sub min-max':>15s}"
          f"{'full med':>10s}{'sub med':>10s}  covers")
    for y in years:
        for t in TECHS:
            v = np.array([m[y][t] for m in mixes]); s = v[idx]
            p5, p95 = np.percentile(v, [5, 95])
            smed = float(np.median(s))
            cover = s.min() <= p5 + a.cover_tol and s.max() >= p95 - a.cover_tol
            ok &= cover
            rows.append({"year": y, "technology": t, "full_p5": p5, "full_p95": p95, "full_min": v.min(),
                         "full_max": v.max(), "sub_min": s.min(), "sub_max": s.max(),
                         "full_median": float(np.median(v)), "sub_median": smed,
                         "covers_p5_p95": bool(cover)})
            print(f"  {t:22s}{y:>5d}{p5:8.1f}-{p95:5.1f}{v.min():9.1f}-{v.max():5.1f}{s.min():9.1f}-{s.max():5.1f}"
                  f"{np.median(v):10.1f}{smed:10.1f}  {'yes' if cover else 'NO'}")
    res = os.path.join(a.calib, "subsample_validation.csv")
    with open(res, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0])); wr.writeheader(); wr.writerows(rows)
    print(f"\n{'PASS' if ok else 'FAIL'}: subsample {'represents' if ok else 'does not represent'} the full "
          f"ensemble band (covers every 5-95 % range)\nwrote {res}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["select", "validate", "select-outcome"])
    ap.add_argument("calib", nargs="?", default=os.path.join(ROOT, "results", "calib1000"))
    ap.add_argument("--n", type=int, default=25, help="select: subsample size")
    ap.add_argument("--scope", default="province:Noord-Brabant", help="validate: region")
    ap.add_argument("--years", default="2030,2050", help="validate: years compared")
    ap.add_argument("--cover-tol", type=float, default=0.1,
                    help="validate: slack (pp) on the 5-95 %% coverage, so a range of a few tenths "
                         "of a point does not fail on rounding")
    ap.add_argument("--xmx", default="8g")
    ap.add_argument("--jobs", type=int, default=1,
                    help="validate: engine runs in parallel (a province run needs ~1.5 GB; 6 is fine)")
    ap.add_argument("--match", default="2030,2050", help="select-outcome: years whose quantiles are matched")
    ap.add_argument("--check", default="2040", help="select-outcome: held-out years, reported only")
    ap.add_argument("--prefer", default=None,
                    help="select-outcome: a subsample JSON whose sets already have scenario runs; sets in\n"
                         "it cost --reuse-penalty less, so they are kept when it barely matters")
    ap.add_argument("--reuse-penalty", type=float, default=0.005,
                    help="select-outcome: mismatch charged per set NOT in --prefer (0 = ignore reuse)")
    ap.add_argument("--restarts", type=int, default=40, help="select-outcome: local-search restarts")
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    {"select": do_select, "validate": do_validate, "select-outcome": do_select_outcome}[a.mode](a)


if __name__ == "__main__":
    main()
