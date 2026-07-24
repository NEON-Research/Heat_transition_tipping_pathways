"""
compare_engines.py
------------------
Compare the Java engine against the JS engine on the same scenario.

    python compare_engines.py <java_results.csv> <js_results.csv> [--al <al_results.csv>]

These two are the SAME algorithm in two languages, so unlike the AL comparison there is no
"acceptable" systematic difference. They differ only through the RNG stream, so:

  * means should agree to well inside each engine's own spread
  * a difference larger than ~2 sd on a low-variance owner type (PRIVATELY_OWNED,
    PRIVATELY_RENTED, TOTAL) is a porting slip in one of them, not noise
  * SOCIAL_HOUSING and HOME_OWNER_ASSOCIATION are genuinely noisy (block-level decisions are
    correlated across many dwellings), so allow more room there

Pass --al to print AnyLogic alongside as the reference both are chasing.
"""
import argparse
import csv
import statistics
from collections import defaultdict

OWNERS = ["TOTAL", "PRIVATELY_OWNED", "PRIVATELY_RENTED",
          "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION"]
SYSTEMS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP",
           "NATURAL_GAS_BLOCK", "DISTRICT_HEATING"]

# owner types whose block-level correlation makes them intrinsically noisy
NOISY = {"SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION"}


def load(path, scenario="baseline"):
    """iteration -> ownership -> system -> installed, at the final year"""
    rows = list(csv.DictReader(open(path)))
    if not rows:
        raise SystemExit(f"{path} is empty")
    final = max(int(r["year"]) for r in rows)
    per = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for r in rows:
        if int(r["year"]) != final:
            continue
        if r.get("scenario_name", scenario) != scenario:
            continue
        per[r["iteration"]][r["ownership"]][r["heating_system"]] += float(r["installed_current"])
    return per, final


def shares(per, owner, system):
    out = []
    for d in per.values():
        tot = sum(d[owner].values())
        if tot:
            out.append(100 * d[owner][system] / tot)
    return out


def ms(v):
    if not v:
        return float("nan"), 0.0
    return statistics.mean(v), (statistics.stdev(v) if len(v) > 1 else 0.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("java_csv")
    ap.add_argument("js_csv")
    ap.add_argument("--al", help="optional AnyLogic results for reference")
    ap.add_argument("--scenario", default="baseline")
    args = ap.parse_args()

    ja, fy_j = load(args.java_csv, args.scenario)
    js, fy_s = load(args.js_csv, args.scenario)
    al = load(args.al, args.scenario)[0] if args.al else None

    if fy_j != fy_s:
        print(f"WARNING: final years differ (java {fy_j}, js {fy_s})")

    print(f"\nscenario={args.scenario}  final year={fy_j}")
    print(f"java n={len(ja)}   js n={len(js)}" + (f"   AL n={len(al)}" if al else ""))

    hdr = f"{'owner':22s} {'system':20s} {'java':>13s} {'js':>13s} {'diff':>7s}"
    if al:
        hdr += f" {'AL':>13s}"
    print("\n" + hdr)

    problems = []
    for o in OWNERS:
        for s in SYSTEMS:
            jv, sv = shares(ja, o, s), shares(js, o, s)
            if not jv and not sv:
                continue
            jm, jsd = ms(jv)
            sm, ssd = ms(sv)
            if max(jm, sm) < 0.4:
                continue
            diff = jm - sm

            # tolerance: the noisier of the two engines, floored so tiny-sd cases
            # don't flag on rounding
            tol = max(2 * max(jsd, ssd), 0.5)
            if o in NOISY:
                tol = max(tol, 2.0)
            bad = abs(diff) > tol
            if bad:
                problems.append((o, s, round(diff, 2), round(tol, 2)))

            line = (f"{o:22s} {s:20s} {jm:8.2f}+-{jsd:4.2f} {sm:8.2f}+-{ssd:4.2f} "
                    f"{diff:+7.2f}")
            if al:
                am, asd = ms(shares(al, o, s))
                line += f" {am:8.2f}+-{asd:4.2f}"
            if bad:
                line += "  <<<"
            print(line)

    print("\n" + "=" * 70)
    if problems:
        print("ENGINES DISAGREE beyond tolerance:")
        for o, s, d, t in problems:
            print(f"  {o} / {s}: java - js = {d:+} (tolerance +-{t})")
        print("\nThese are the same algorithm, so this is a porting slip, not noise.")
        print("Check the mechanisms most recently changed in one engine but not the other.")
        raise SystemExit(1)
    print("Engines agree within tolerance on every owner/system.")
    print("(Agreement here means the two ports match each other — whether they match")
    print(" AnyLogic is a separate question; pass --al to see all three.)")


if __name__ == "__main__":
    main()
