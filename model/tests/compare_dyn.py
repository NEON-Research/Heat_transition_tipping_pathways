#!/usr/bin/env python3
"""
Diff the per-year DECISION-TIME dynamics (learned invest + salience per heating type) between the
engine and AnyLogic, to pin the first year and technology where the dynamics diverge.

Both sides emit one line per heating type per year, at the START of the year's decisions:
    ENGDYN <year> <type> invest=.. salience=.. cumulative=.. initial=..     (engine, HT_DYN=1)
    ALDYN  <year> <type> invest=.. salience=..                              (AnyLogic, HT_DYN=1)

Because both emit before that year's learning/salience update, ENGDYN <year> and ALDYN <year> are
the exact values that year's decisions use, so they should be equal when the models agree. Year 1
(2025) is the base state (factor 1.0) in both.

If multiple iterations are present, values are averaged per (year, type).

Usage:
    python compare_dyn.py eng_dyn.log al_dyn.log
    python compare_dyn.py eng_dyn.log al_dyn.log --type HYBRID_HEAT_PUMP --tol-invest 5 --tol-sal 0.01
"""
import argparse
import os
import re
import sys
from collections import defaultdict

LINE_RE = re.compile(r"^(ENGDYN|ALDYN)\s+(\d{4})\s+(\S+)\s+(.*)$")


def read_text(path):
    with open(path, "rb") as fh:
        raw = fh.read()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", errors="replace")
    if raw.count(b"\x00") > len(raw) // 4:
        return raw.decode("utf-16-le", errors="replace")
    return raw.decode("utf-8", errors="replace")


def parse(paths):
    # sums[(src, year, type)][field] = [total, count]
    sums = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))
    for path in paths:
        if not os.path.exists(path):
            print(f"warning: skipping missing file '{path}'", file=sys.stderr)
            continue
        matched = 0
        for raw in read_text(path).splitlines():
            line = raw.replace(",", ".").strip()
            m = LINE_RE.match(line)
            if not m:
                continue
            tag, year, htype, rest = m.groups()
            src = "ENG" if tag == "ENGDYN" else "AL"
            for tok in rest.split():
                if "=" not in tok:
                    continue
                k, v = tok.split("=", 1)
                try:
                    val = float(v)
                except ValueError:
                    continue
                key = (src, int(year), htype)
                sums[key][k][0] += val
                sums[key][k][1] += 1
            matched += 1
        if matched == 0:
            print(f"warning: no ENGDYN/ALDYN lines in '{path}' ({os.path.getsize(path)} bytes)",
                  file=sys.stderr)
    return sums


def mean(sums, key, field):
    tot, cnt = sums[key][field]
    return tot / cnt if cnt else None


def main():
    ap = argparse.ArgumentParser(description="Per-year dynamics diff (engine vs AnyLogic)")
    ap.add_argument("logs", nargs="+", help="engine ENGDYN log and AL ALDYN log (any order)")
    ap.add_argument("--type", default=None, help="restrict to one heating type (e.g. HYBRID_HEAT_PUMP)")
    ap.add_argument("--tol-invest", type=float, default=2.0, help="EUR tolerance on invest (default 2)")
    ap.add_argument("--tol-sal", type=float, default=0.005, help="tolerance on salience (default 0.005)")
    args = ap.parse_args()

    sums = parse(args.logs)
    if not sums:
        print("No ENGDYN/ALDYN lines found.", file=sys.stderr)
        sys.exit(1)

    years = sorted({k[1] for k in sums})
    types = sorted({k[2] for k in sums})
    if args.type:
        types = [t for t in types if t == args.type]

    first_div = {}
    for htype in types:
        print(f"\n=== {htype} ===")
        print(f"{'year':>6} | {'ENG_inv':>9}{'AL_inv':>9}{'dInv':>8} | {'ENG_sal':>9}{'AL_sal':>9}{'dSal':>9}  flag")
        for y in years:
            ei = mean(sums, ("ENG", y, htype), "invest")
            ai = mean(sums, ("AL", y, htype), "invest")
            es = mean(sums, ("ENG", y, htype), "salience")
            as_ = mean(sums, ("AL", y, htype), "salience")
            if ei is None or ai is None:
                continue
            dinv = ei - ai
            dsal = (es - as_) if (es is not None and as_ is not None) else None
            flag = ""
            if abs(dinv) > args.tol_invest or (dsal is not None and abs(dsal) > args.tol_sal):
                flag = "  <-- DIVERGES"
                first_div.setdefault(htype, y)
            ds = f"{dsal:>9.4f}" if dsal is not None else f"{'-':>9}"
            es_s = f"{es:>9.4f}" if es is not None else f"{'-':>9}"
            as_s = f"{as_:>9.4f}" if as_ is not None else f"{'-':>9}"
            print(f"{y:>6} | {ei:>9.0f}{ai:>9.0f}{dinv:>8.0f} | {es_s}{as_s}{ds}{flag}")

    print("\n--- first year of divergence per technology ---")
    if not first_div:
        print("  none within tolerance -- dynamics agree across all years/types.")
    for htype in types:
        print(f"  {htype:22s} {first_div.get(htype, 'agrees')}")


if __name__ == "__main__":
    main()
