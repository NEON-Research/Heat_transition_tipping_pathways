"""
compare_learning_curve.py
-------------------------
Compare the AnyLogic learning-curve export against the JS/Java engine's.

    python compare_learning_curve.py <al_learning_curve.csv> <engine_learning_curve.csv>

Both files are one row per (year, heating_system) with:
    installed_current, installed_cumulative, initial_units, capex_factor, capex_medium

Year-0 block economics already match between the two models to within 0.1%. What differs is
WHEN capex starts falling, and the learning curve is the only mechanism that can move it.
So the decisive question is not "is the formula right" but "do the two models start learning
at the same time, from the same base".

The report answers three things in order:

  1. BASE       -- initial_units. If these differ, every doubling downstream is wrong.
  2. COUNTER    -- installed_cumulative. Does it count install EVENTS, or stock-plus-installs?
                   AL guards with `if(unitsInstalled > initialUnitsInstalled)`, so a counter
                   that starts at zero produces NO learning until it climbs past the year-0
                   stock -- a threshold that yields late, steep tipping.
  3. LEARNING   -- capex_factor over time. This is what the blocks actually feel.
"""
import csv
import sys
from collections import defaultdict

FOCUS = ["HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP"]   # the technologies that tip


def load(path):
    out = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            out[(int(r["year"]), r["heating_system"])] = r
    return out


def num(r, k):
    try:
        return float(r[k])
    except (KeyError, TypeError, ValueError):
        return float("nan")


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    al, me = load(sys.argv[1]), load(sys.argv[2])
    common = sorted(set(al) & set(me))
    if not common:
        sys.exit("No overlapping (year, heating_system) rows — check both files ran the same "
                 "years and a single iteration.")

    systems = sorted({k[1] for k in common})
    years = sorted({k[0] for k in common})
    print(f"\nmatched {len(common)} rows | {len(systems)} systems | years {years[0]}-{years[-1]}")

    # ---- 1. base ------------------------------------------------------------
    print("\n--- 1. BASE (initial_units) -----------------------------------")
    print(f"{'system':22s} {'AL':>10s} {'engine':>10s}  verdict")
    base_bad = []
    for s in systems:
        k = (years[0], s)
        a, m = num(al[k], "initial_units"), num(me[k], "initial_units")
        ok = a == m or (a and abs(m - a) / a < 0.01)
        if not ok:
            base_bad.append(s)
        print(f"{s:22s} {a:10.0f} {m:10.0f}  {'ok' if ok else '<-- DIFFERS'}")

    # ---- 2. counter ---------------------------------------------------------
    print("\n--- 2. COUNTER (installed_cumulative) -------------------------")
    print("Does the counter start at the stock, or at zero?")
    print(f"{'system':22s} {'yr':>5s} {'AL cum':>10s} {'eng cum':>10s} {'AL/stock':>9s} {'eng/stock':>10s}")
    for s in FOCUS:
        if s not in systems:
            continue
        for y in years[:3]:
            k = (y, s)
            a, m = num(al[k], "installed_cumulative"), num(me[k], "installed_cumulative")
            ac, mc = num(al[k], "installed_current"), num(me[k], "installed_current")
            print(f"{s:22s} {y:5d} {a:10.0f} {m:10.0f} "
                  f"{(a / ac if ac else float('nan')):9.2f} {(m / mc if mc else float('nan')):10.2f}")

    # ---- 3. learning --------------------------------------------------------
    print("\n--- 3. LEARNING (capex_factor; 1.000 = no learning yet) -------")
    for s in FOCUS:
        if s not in systems:
            continue
        print(f"\n{s}")
        print(f"{'year':>6s} {'AL factor':>11s} {'eng factor':>11s} {'Δ':>8s}   {'AL capex':>10s} {'eng capex':>10s}")
        al_flat_until = None
        me_flat_until = None
        for y in years:
            k = (y, s)
            a, m = num(al[k], "capex_factor"), num(me[k], "capex_factor")
            if a >= 0.9999:
                al_flat_until = y
            if m >= 0.9999:
                me_flat_until = y
            if y % 3 == 0 or y in (years[0], years[-1]):
                print(f"{y:6d} {a:11.4f} {m:11.4f} {m - a:+8.4f}   "
                      f"{num(al[k], 'capex_medium'):10.0f} {num(me[k], 'capex_medium'):10.0f}")
        print(f"   learning starts: AL {'after ' + str(al_flat_until) if al_flat_until else 'immediately'}"
              f" | engine {'after ' + str(me_flat_until) if me_flat_until else 'immediately'}")
        # A real threshold means capex holds flat for SEVERAL years, not just the first.
        # In year 0 cumulative == initial by construction, so a single flat year proves nothing.
        al_flat_years = sum(1 for y in years if num(al[(y, s)], "capex_factor") >= 0.9999)
        me_flat_years = sum(1 for y in years if num(me[(y, s)], "capex_factor") >= 0.9999)
        if al_flat_years >= 3 and me_flat_years == 0:
            print(f"   >>> THRESHOLD: AL holds capex flat for {al_flat_years} years while the "
                  "engine discounts from year 1.\n       Seed the engine's cumulative counter "
                  "at zero, not at the year-0 stock.")
        elif al_flat_years <= 1 and me_flat_years <= 1:
            print("   no threshold effect: both models begin learning immediately "
                  "(year-0 flatness is just cumulative == initial by construction).")

    # ---- verdict ------------------------------------------------------------
    print("\n" + "=" * 63)
    if base_bad:
        print(f"BASE DIFFERS on: {', '.join(base_bad)}")
        print("  -> fix initial_units first; every doubling downstream depends on it.")
    else:
        print("Base agrees. If capex_factor still diverges, the difference is in WHEN")
        print("learning starts (the cumulative counter), not in the formula or the base.")
    print("\nReminder: the endpoint is not the test. Both models reach ~50% hybrid by 2050.")
    print("What matters is the mid-run trajectory (2028-2040), because blocks decide rarely")
    print("and lock their choice in for a full 12-year lifetime.")


if __name__ == "__main__":
    main()
