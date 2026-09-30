"""Which parameters can the 2022-2024 observations constrain? A Morris screening on the CALIBRATION FIT.

WHY. The decision weights are calibrated on 2022-2024; the other parameters are screened afterwards
on the 2024-2050 pathway at FIXED calibrated weights (structural_sensitivity.py). That split is only
sound for parameters the observation window cannot constrain: varying a parameter that does shape
2022-2024 adoption would move the model away from the fit its weights were calibrated to, so part of
its apparent influence on 2050 would reflect a model that no longer reproduces the data. Such a
parameter belongs in the calibration instead (calibrate_weights.EXTRA). This script decides which is
which by measurement rather than argument.

HOW. The same Morris design as structural_sensitivity.py (trajectories through the factor ranges of
its FACTORS table), but every point is run exactly as a calibration run: initialised from the observed
2022 state, simulated 2022-2024 on the CBS-complete buurten, district-heating dwellings kept
connected. The outcome is the simulated 2023 and 2024 share of each scored technology; mu* per factor
is the mean absolute change of that share (%pt) per full-range move, and mu*(MAD) the same for the
calibration objective.

READING. A factor constrains the fit when its mu* on some scored share is comparable to the deviation
the history matching admits (about 1.7 %pt at I < 3; sd_disc = 0.5 %pt is the model-discrepancy unit).
The verdict column uses mu*max >= --threshold (default 0.5 %pt = sd_disc): 'calibrate' for those,
'screen' for the rest. The district-heating review interval and exit barrier are left out: during the
calibration district-heating dwellings do not review (calibrate_weights.CALIB_FIXED).

  python results_analysis/fit_screen.py results/calib1000_v2 --weights central
"""
import argparse, json, os, sys
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import calibrate_weights as cw                      # noqa: E402  calibration run setup + observations
import structural_sensitivity as ss                 # noqa: E402  factor table + weight lookup

EXCLUDE = ["dhReviewYears", "dhExitBarrier"] + ss.HP_SPLIT


def run_fit(scope, weights, x_actual, a):
    """One calibration-style run with factor overrides -> {year: mix(%)} for the scored years."""
    fixed_backup = dict(cw.CALIB_FIXED)
    try:
        cw.CALIB_FIXED.update(ss.dflags_for(x_actual))   # run_engine adds CALIB_FIXED as -Dht flags
        ys = cw.scored_years(a.start, a.end)
        return cw.run_engine(scope, weights, a.iterations, a.start, a.end, a.xmx, years=ys)
    finally:
        cw.CALIB_FIXED.clear(); cw.CALIB_FIXED.update(fixed_backup)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("calib", help="calibration folder with calibration_search.json")
    ap.add_argument("--weights", default="central", help="central | retained:<i> | a representative key")
    ap.add_argument("--scope", default="province:Noord-Brabant")
    ap.add_argument("--factors", default=None, help="comma list (default: every screened factor)")
    ap.add_argument("--trajectories", type=int, default=8)
    ap.add_argument("--levels", type=int, default=4)
    ap.add_argument("--iterations", type=int, default=2)
    ap.add_argument("--start", type=int, default=2022)
    ap.add_argument("--end", type=int, default=2024)
    ap.add_argument("--threshold", type=float, default=0.5, help="%%pt of mu* that marks 'calibrate'")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--xmx", default="6g")
    a = ap.parse_args()

    search = os.path.join(a.calib, "calibration_search.json")
    # the weight dict of a retained set; the calibration-only keys (e.g. a calibrated noise scale)
    # are engine flags too and are passed through unchanged
    raw, label = ss.get_weights(search, a.weights)
    names = a.factors.split(",") if a.factors else [k for k in ss.FACTORS if k not in EXCLUDE]
    bad = [n for n in names if n not in ss.FACTORS]
    if bad:
        sys.exit(f"unknown factor(s) {bad}")

    # run_engine maps sampled shares -> weights; give it the shares of the chosen set
    d = json.load(open(search, encoding="utf-8"))
    ret = d.get("retained") or []
    match = [r for r in ret if r["weights"] == raw]
    if not match:
        sys.exit(f"could not find the sampled shares of {label} in {search}")
    shares = dict(match[0]["sampled"])
    for n in names:                    # a screened factor must not be overridden by the set's own value
        shares.pop(n, None)

    ys = cw.scored_years(a.start, a.end)
    obs = {y: cw.observed_mix(a.scope, y)[0] for y in ys}
    keys = [(y, t) for y in ys for t in cw.FOCUS]
    print(f"[fit] {label}; {len(names)} factors, {a.trajectories} trajectories "
          f"-> {a.trajectories * (len(names) + 1)} runs of {a.start}-{a.end}", flush=True)

    def outcome(x_unit):
        sim = run_fit(a.scope, shares, ss.scale(names, x_unit), a)
        out = {f"{y}:{t}": sim[y][t] for (y, t) in keys}
        out["MAD"] = float(np.mean([cw.error(sim[y], obs[y]) for y in ys]))
        return out

    rng = np.random.default_rng(a.seed)
    k = len(names); delta = a.levels / (2.0 * (a.levels - 1))
    ee = {n: {} for n in names}; runs = 0
    for tr in range(a.trajectories):
        x = rng.integers(0, a.levels // 2, size=k) / (a.levels - 1.0)
        prev = outcome(x); runs += 1
        for j in rng.permutation(k):
            x2 = x.copy(); x2[j] = x2[j] + delta if x2[j] + delta <= 1.0 else x2[j] - delta
            cur = outcome(x2); runs += 1
            for o in cur:
                ee[names[j]].setdefault(o, []).append((cur[o] - prev[o]) / (x2[j] - x[j]))
            x, prev = x2, cur
        print(f"  trajectory {tr + 1}/{a.trajectories} ({runs} runs)", flush=True)

    mu = {n: {o: float(np.mean(np.abs(v))) for o, v in ee[n].items()} for n in names}
    share_keys = [f"{y}:{t}" for (y, t) in keys]
    rows = []
    for n in names:
        mmax = max(mu[n][o] for o in share_keys)
        arg = max(share_keys, key=lambda o: mu[n][o])
        rows.append({"factor": n, "low": ss.FACTORS[n][0], "high": ss.FACTORS[n][1],
                     "mu_star_MAD": mu[n]["MAD"], "mu_star_max_share": mmax, "max_share": arg,
                     "verdict": "calibrate" if mmax >= a.threshold else "screen",
                     **{f"mu_star_{o}": mu[n][o] for o in share_keys}})
    rows.sort(key=lambda r: -r["mu_star_max_share"])
    print(f"\nFit-window Morris ({runs} runs, {label}): mu* in %pt per full-range move; "
          f"verdict at {a.threshold} %pt")
    print(f"  {'factor':<22}{'range':>16}{'mu*(MAD)':>10}{'mu*max':>9}  {'on':<28}verdict")
    for r in rows:
        print(f"  {r['factor']:<22}{r['low']:>7g}-{r['high']:<8g}{r['mu_star_MAD']:>10.2f}"
              f"{r['mu_star_max_share']:>9.2f}  {r['max_share']:<28}{r['verdict']}")
    import csv
    p = os.path.join(a.calib, "fit_screen.csv")
    with open(p, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0])); wr.writeheader(); wr.writerows(rows)
    print(f"\nwrote {p}")


if __name__ == "__main__":
    main()
