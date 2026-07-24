"""
compare_block_decisions.py
--------------------------
Compare the AnyLogic block decision trace against the JS/Java engine's.

    python compare_block_decisions.py <al_trace.csv> <engine_trace.csv>

Both files are one row per (block, year, heating-system option). Blocks are matched on
`block_key` (smallest member numid), which is stable across engines wherever block
membership agrees.

The point is to separate three very different explanations for the social-housing gap:

  1. ELIGIBILITY  -- is_possible / n_impossible differ.  A single ineligible dwelling
     vetoes an option for the entire block (AL ANDs isPossible across all households),
     so this produces a stuck SUBSET of blocks rather than a uniform drift.
  2. ECONOMICS    -- avg_eac differs.  The option was on the table but priced wrong.
  3. RNG          -- both match, `chosen` differs.  Gumbel noise; not a bug.

Only (1) and (2) are defects. (3) is expected and should be reported, not fixed.
"""
import csv
import sys
from collections import defaultdict

OPTIONS = ["NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP",
           "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]


def load(path):
    """key: (block_key, year, option) -> row"""
    out = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            out[(r["block_key"], int(r["year"]), r["option"])] = r
    return out


def pct(n, d):
    return f"{100 * n / d:5.1f}%" if d else "    -"


def median(xs):
    if not xs:
        return float("nan")
    s = sorted(xs)
    return s[len(s) // 2]


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    al, me = load(sys.argv[1]), load(sys.argv[2])

    common = sorted(set(al) & set(me))
    if not common:
        sys.exit("No overlapping (block_key, year, option) rows — check block_key is populated "
                 "in both files, and that the AL trace was written from a single iteration.")

    print(f"\nAL rows {len(al)} | engine rows {len(me)} | matched {len(common)}")
    al_blocks = {k[0] for k in al}
    me_blocks = {k[0] for k in me}
    print(f"blocks: AL {len(al_blocks)}, engine {len(me_blocks)}, "
          f"shared {len(al_blocks & me_blocks)}")

    # ---- 1. eligibility -----------------------------------------------------
    print("\n--- 1. ELIGIBILITY (is_possible) ------------------------------")
    print(f"{'option':22s} {'agree':>7s} {'AL poss':>8s} {'eng poss':>9s}  verdict")
    elig_defect = []
    for opt in OPTIONS:
        ks = [k for k in common if k[2] == opt]
        if not ks:
            continue
        agree = sum(1 for k in ks if al[k]["is_possible"] == me[k]["is_possible"])
        alp = sum(1 for k in ks if al[k]["is_possible"] == "1")
        mep = sum(1 for k in ks if me[k]["is_possible"] == "1")
        bad = agree < len(ks) * 0.99
        if bad:
            elig_defect.append(opt)
        print(f"{opt:22s} {pct(agree, len(ks)):>7s} {pct(alp, len(ks)):>8s} "
              f"{pct(mep, len(ks)):>9s}  {'<-- DIVERGES' if bad else 'ok'}")

    # how many households do the blocking, on each side
    print(f"\n{'option':22s} {'AL n_impossible':>16s} {'engine':>9s}   (median over blocked rows)")
    for opt in OPTIONS:
        ks = [k for k in common if k[2] == opt]
        a = [int(al[k]["n_impossible"]) for k in ks if int(al[k]["n_impossible"]) > 0]
        m = [int(me[k]["n_impossible"]) for k in ks if int(me[k]["n_impossible"]) > 0]
        if a or m:
            print(f"{opt:22s} {median(a):16.0f} {median(m):9.0f}   "
                  f"(rows blocked: AL {len(a)}, engine {len(m)})")

    # ---- 2. economics -------------------------------------------------------
    print("\n--- 2. ECONOMICS (avg_eac, both-possible rows only) -----------")
    print(f"{'option':22s} {'median Δ%':>10s} {'p90 Δ%':>8s} {'n':>7s}  verdict")
    econ_defect = []
    for opt in OPTIONS:
        ds = []
        for k in (k for k in common if k[2] == opt):
            if al[k]["is_possible"] != "1" or me[k]["is_possible"] != "1":
                continue
            a, m = float(al[k]["avg_eac"]), float(me[k]["avg_eac"])
            if a:
                ds.append(100 * (m - a) / a)
        if not ds:
            continue
        ds.sort()
        med, p90 = median(ds), ds[int(0.9 * (len(ds) - 1))]
        bad = abs(med) > 1.0
        if bad:
            econ_defect.append(opt)
        print(f"{opt:22s} {med:+9.2f}% {p90:+7.2f}% {len(ds):7d}  "
              f"{'<-- DIVERGES' if bad else 'ok'}")

    # ---- 3. outcome ---------------------------------------------------------
    print("\n--- 3. OUTCOME (chosen) ---------------------------------------")
    dec = defaultdict(lambda: [0, 0])   # block/year -> [al_choice, me_choice]
    al_choice, me_choice = {}, {}
    for k in common:
        if al[k]["chosen"] == "1":
            al_choice[(k[0], k[1])] = k[2]
        if me[k]["chosen"] == "1":
            me_choice[(k[0], k[1])] = k[2]
    shared = set(al_choice) & set(me_choice)
    same = sum(1 for k in shared if al_choice[k] == me_choice[k])
    print(f"decisions compared {len(shared)} | same choice {pct(same, len(shared))}")

    mix = defaultdict(lambda: [0, 0])
    for k in shared:
        mix[al_choice[k]][0] += 1
        mix[me_choice[k]][1] += 1
    print(f"\n{'option':22s} {'AL chosen':>10s} {'engine':>8s}")
    for opt in OPTIONS:
        if mix[opt][0] or mix[opt][1]:
            print(f"{opt:22s} {mix[opt][0]:10d} {mix[opt][1]:8d}")

    # trigger mix — are we meeting blocks at the same point in their lifecycle?
    print("\n--- trigger mix ------------------------------------------------")
    for name, src in (("AL", al), ("engine", me)):
        t = defaultdict(int)
        for k, r in src.items():
            if k[2] == OPTIONS[0]:
                t[r["trigger"]] += 1
        tot = sum(t.values()) or 1
        print(f"{name:8s} " + "  ".join(f"{k}={pct(v, tot).strip()}" for k, v in sorted(t.items())))

    # ---- verdict ------------------------------------------------------------
    print("\n" + "=" * 63)
    if elig_defect:
        print(f"ELIGIBILITY DEFECT on: {', '.join(elig_defect)}")
        print("  -> check f_getHeatingMethodPossibility: DH grid coverage, and the")
        print("     APARTMENT test for NATURAL_GAS_BLOCK. One blocking dwelling vetoes")
        print("     the whole block, so this strands a subset of blocks on gas.")
    if econ_defect:
        print(f"ECONOMICS DEFECT on: {', '.join(econ_defect)}")
        print("  -> avg_eac is off with the option still eligible; suspect the")
        print("     insulation retrofit charge and which blocks it applies to.")
    if not elig_defect and not econ_defect:
        print("No structural defect: eligibility and EAC both agree.")
        print("Remaining difference in `chosen` is the Gumbel draw — i.e. RNG, not a bug.")
        print("If the aggregate gap survives this, it is upstream of the block decision")
        print("(block membership / ownership assignment), not inside it.")


if __name__ == "__main__":
    main()
