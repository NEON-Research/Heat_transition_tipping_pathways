"""
compare_to_golden.py
--------------------
Golden-master (characterization) test: does the NEW engine's output reproduce
the ORIGINAL AnyLogic model's emergent behaviour?

It does NOT require bit-identical numbers. The original model is stochastic
(beta-distributed attitudes, Gumbel/RUM choice) and Java's RNG stream cannot be
reproduced exactly in another language/engine. Instead we compare *summary
metrics* (from extract_reference_metrics.py) with a statistical tolerance:

    |new_mean - golden_mean|  <=  k * golden_std  +  rel * golden_mean  +  abs_floor

Run the NEW engine so it emits a `simulation_results_*.csv` in the SAME schema
as the AnyLogic export, ideally with several iterations per scenario, then:

    python compare_to_golden.py <new_engine_results.csv> \
        --golden golden/baseline_metrics.json

Exit code 0 = pass, 1 = fail (CI-friendly). A human-readable table is printed.

Tuning knobs (see DEFAULTS): widen `k` while the engine is young, tighten as it
matures. `--min-share` ignores negligible technologies (<1% stock) whose
relative variance is meaningless.
"""
import argparse, json, sys
import pandas as pd

DEFAULTS = dict(
    k=4.0,          # allowed multiples of golden std
    rel=0.05,       # + 5% of golden mean
    abs_floor=500,  # + absolute floor (dwellings) so near-zero techs don't fail
    tip_tol_years=2.0,   # tipping-year tolerance
    min_share=0.01,      # ignore techs below 1% of final stock
)

RESET, RED, GREEN, DIM = "\033[0m", "\033[31m", "\033[32m", "\033[2m"


def load_new(path):
    df = pd.read_csv(path)
    need = {"scenario_name", "iteration", "year", "heating_system",
            "ownership", "installed_current"}
    missing = need - set(df.columns)
    if missing:
        sys.exit(f"ERROR: new results CSV missing columns: {missing}")
    return df


def new_final_stats(df, year):
    d = df[df.year == year]
    out = {}
    for (scn, own, hs), g in d.groupby(["scenario_name", "ownership", "heating_system"]):
        out[(scn, own, hs)] = float(g.installed_current.astype(float).mean())
    return out


def new_tipping(df, scn, hs, threshold):
    sub = df[(df.scenario_name == scn) & (df.ownership == "TOTAL")]
    years = []
    for _, g in sub.groupby("iteration"):
        tot = g.groupby("year")["installed_current"].sum()
        hsy = g[g.heating_system == hs].groupby("year")["installed_current"].sum()
        share = (hsy / tot).dropna()
        crossed = share[share >= threshold]
        if len(crossed):
            years.append(int(crossed.index.min()))
    return sum(years) / len(years) if years else None


def band(mean, std, cfg):
    return cfg["k"] * std + cfg["rel"] * abs(mean) + cfg["abs_floor"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("new_results_csv")
    ap.add_argument("--golden", default="golden/baseline_metrics.json")
    for key, val in DEFAULTS.items():
        ap.add_argument(f"--{key.replace('_','-')}", type=type(val), default=val)
    ap.add_argument("--scenario", help="only check this scenario_name")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    cfg = {k: getattr(args, k) for k in DEFAULTS}

    golden = json.load(open(args.golden))
    fy = golden["meta"]["final_year"]
    new = load_new(args.new_results_csv)
    new_stats = new_final_stats(new, fy)

    fails, checks = [], 0
    print(f"\nGolden-master comparison  (final year {fy})")
    print(f"  band = {cfg['k']}*std + {cfg['rel']*100:.0f}%*mean + {cfg['abs_floor']}\n")

    # --- final-year installed shares (TOTAL ownership only for headline) ---
    scen_list = [args.scenario] if args.scenario else golden["meta"]["scenarios"]
    for scn in scen_list:
        gscn = golden["final_year_installed"].get(scn, {}).get("TOTAL", {})
        total = sum(v["mean"] for v in gscn.values()) or 1
        printed_header = False
        for hs, gv in sorted(gscn.items(), key=lambda x: -x[1]["mean"]):
            if gv["mean"] / total < cfg["min_share"]:
                continue
            checks += 1
            nm = new_stats.get((scn, "TOTAL", hs))
            tol = band(gv["mean"], gv["std"], cfg)
            if nm is None:
                ok = False; delta = float("nan")
            else:
                delta = nm - gv["mean"]; ok = abs(delta) <= tol
            if not ok or args.verbose:
                if not printed_header:
                    print(f"{scn}"); printed_header = True
                col = GREEN if ok else RED
                nms = "MISSING" if nm is None else f"{nm:9.0f}"
                print(f"  {col}{'PASS' if ok else 'FAIL'}{RESET} {hs:20s} "
                      f"golden={gv['mean']:9.0f}±{gv['std']:6.0f}  new={nms}  "
                      f"{DIM}Δ={delta:+8.0f} tol=±{tol:.0f}{RESET}")
            if not ok:
                fails.append((scn, "final_share", hs))

    # --- tipping years ---
    tip_fail_header = False
    for scn in scen_list:
        for hs, ths in golden["tipping_year"].get(scn, {}).items():
            for th, gv in ths.items():
                if gv is None:
                    continue
                checks += 1
                nt = new_tipping(new, scn, hs, float(th))
                if nt is None:
                    ok = False; d = float("nan")
                else:
                    d = nt - gv["mean"]; ok = abs(d) <= cfg["tip_tol_years"] + gv["std"]
                if not ok or args.verbose:
                    if not tip_fail_header:
                        print("\nTipping years"); tip_fail_header = True
                    col = GREEN if ok else RED
                    nts = "never" if nt is None else f"{nt:.1f}"
                    print(f"  {col}{'PASS' if ok else 'FAIL'}{RESET} {scn:34s} {hs:20s} "
                          f"@{float(th)*100:.0f}%  golden={gv['mean']:.1f}±{gv['std']:.1f} new={nts}")
                if not ok:
                    fails.append((scn, f"tip@{th}", hs))

    print(f"\n{'-'*60}")
    if fails:
        print(f"{RED}FAILED{RESET}: {len(fails)}/{checks} checks outside tolerance")
        for scn, kind, hs in fails[:40]:
            print(f"   - {scn} / {kind} / {hs}")
        sys.exit(1)
    print(f"{GREEN}PASSED{RESET}: all {checks} checks within tolerance")
    sys.exit(0)


if __name__ == "__main__":
    main()
