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
    """Retained sets in standardised (attitude, social-norm, effort) effective-weight space."""
    E = [effective(r) for r in R]
    X = np.array([[e[k] for k in ("attitude", "social", "effort")] for e in E])
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


def do_validate(a):
    full = json.load(open(os.path.join(a.calib, "calibration_search.json"), encoding="utf-8"))["retained"]
    subp = os.path.join(a.calib, "calibration_subsample.json")
    if not os.path.exists(subp):
        sys.exit("run 'select' first")
    sub = json.load(open(subp, encoding="utf-8"))["retained"]
    outdir = os.path.join(a.calib, "subsample_validation"); os.makedirs(outdir, exist_ok=True)
    stock = os.path.join(STOCK, f"{scope_tag(a.scope)}_dwellings.csv")
    years = [int(y) for y in a.years.split(",")]
    mixes = []
    for i, r in enumerate(full):
        out = os.path.join(outdir, f"set{i:03d}.csv")
        if not os.path.exists(out):
            cmd = ["java", f"-Xmx{a.xmx}"] + [f"-Dht.{k}={v}" for k, v in r["weights"].items()] + \
                  ["-cp", ENGINE_CP, "heattransition.Cli", "--real", stock, "--scenario", "baseline",
                   "--iterations", "1", "--start", "2024", "--end", str(max(years)), "--out", out]
            print(f"[validate] {i + 1}/{len(full)}", flush=True)
            rc = subprocess.run(cmd, capture_output=True, text=True).returncode
            if rc != 0:
                sys.exit(f"engine run failed for set {i}")
        mixes.append(mix_2050(out, years))

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
    ap.add_argument("mode", choices=["select", "validate"])
    ap.add_argument("calib", nargs="?", default=os.path.join(ROOT, "results", "calib1000"))
    ap.add_argument("--n", type=int, default=25, help="select: subsample size")
    ap.add_argument("--scope", default="province:Noord-Brabant", help="validate: region")
    ap.add_argument("--years", default="2030,2050", help="validate: years compared")
    ap.add_argument("--cover-tol", type=float, default=0.1,
                    help="validate: slack (pp) on the 5-95 %% coverage, so a range of a few tenths "
                         "of a point does not fail on rounding")
    ap.add_argument("--xmx", default="8g")
    a = ap.parse_args()
    (do_select if a.mode == "select" else do_validate)(a)


if __name__ == "__main__":
    main()
