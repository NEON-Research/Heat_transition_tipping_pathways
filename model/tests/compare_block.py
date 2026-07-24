#!/usr/bin/env python3
"""
Compare per-year BLOCK decisions (what social / HOA blocks choose each year) between the Java
engine and AnyLogic, to locate whether the social-housing hybrid excess is a real choice
difference and, if so, in which years.

Engine side: run Cli with HT_BLOCK=1 -> stdout ENGBLOCK lines:
    ENGBLOCK <year> <SOCIAL|HOA> triggered=N households=N notTopEAC=N chosen: TYPE=N TYPE=N ...

AnyLogic side: AL already collects a block-decision trace (f_recordBlockDecision); it just isn't
written. Add one call  f_exportBlockDecisionTrace();  next to f_exportLearningCurve(); (Main.java
~line 3829), run BASELINE 1 iteration, and it writes results/block_decision_trace_*.csv with
columns: year, block_type, ..., trigger, option, avg_eac, is_possible, n_impossible, chosen.

Usage:
    python compare_block.py eng_block.log results/block_decision_trace_YYYYMMDD_HHMMSS.csv
    python compare_block.py eng_block.log al_trace.csv --type SOCIAL
"""
import argparse
import csv
import os
import re
import sys
from collections import defaultdict

ENG_RE = re.compile(r"^ENGBLOCK\s+(\d{4})\s+(SOCIAL|HOA)\s+triggered=(\d+)\s+households=(\d+)\s+notTopEAC=(\d+)\s+chosen:(.*)$")


def read_text(path):
    with open(path, "rb") as fh:
        raw = fh.read()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    if raw[:3] == b"\xef\xbb\xbf":
        return raw.decode("utf-8-sig", errors="replace")
    if raw.count(b"\x00") > len(raw) // 4:
        return raw.decode("utf-16-le", errors="replace")
    return raw.decode("utf-8", errors="replace")


def norm_bt(s):
    s = (s or "").strip().upper()
    return "SOCIAL" if s.startswith("SOC") else ("HOA" if s.startswith("HOA") else s)


def load_engine(path):
    # data[(year,bt)] = {'trig':n,'hh':n,'nottop':n,'chosen':{type:count}}
    data = defaultdict(lambda: {"trig": 0, "hh": 0, "nottop": 0, "chosen": defaultdict(int)})
    for line in read_text(path).splitlines():
        m = ENG_RE.match(line.strip())
        if not m:
            continue
        year, bt, trig, hh, nottop, rest = m.groups()
        d = data[(int(year), norm_bt(bt))]
        d["trig"] += int(trig); d["hh"] += int(hh); d["nottop"] += int(nottop)
        for tok in rest.split():
            if "=" in tok:
                t, c = tok.split("=", 1)
                d["chosen"][t] += int(c)
    return data


def load_al_trace(path):
    """AL trace is one row per (block, year, option). Group by (block_key,year); the chosen option
    is the row with chosen=1; deterministic top = min avg_eac among is_possible=1."""
    rows = list(csv.DictReader(open(path, encoding="utf-8", errors="replace")))
    # group
    blocks = defaultdict(list)  # (year, block_key) -> list of option rows
    for r in rows:
        blocks[(r["year"], r.get("block_key") or r.get("buurtcode"))].append(r)
    data = defaultdict(lambda: {"trig": 0, "hh": 0, "nottop": 0, "chosen": defaultdict(int)})
    for (year, _bk), opts in blocks.items():
        bt = norm_bt(opts[0].get("block_type"))
        chosen = next((o["option"] for o in opts if o.get("chosen") in ("1", "1.0", "true", "True")), None)
        if chosen is None:
            continue
        # deterministic cheapest among possible
        poss = [(float(o["avg_eac"]), o["option"]) for o in opts
                if o.get("is_possible") in ("1", "1.0", "true", "True")]
        top = min(poss)[1] if poss else None
        d = data[(int(year), bt)]
        d["trig"] += 1
        try: d["hh"] += int(float(opts[0].get("n_households", 0)))
        except: pass
        d["chosen"][chosen] += 1
        if top is not None and chosen != top:
            d["nottop"] += 1
    return data


def main():
    ap = argparse.ArgumentParser(description="Per-year block decision diff (engine vs AnyLogic)")
    ap.add_argument("logs", nargs=2, help="engine ENGBLOCK log, and AL block_decision_trace CSV (any order)")
    ap.add_argument("--type", default=None, help="restrict to SOCIAL or HOA")
    args = ap.parse_args()

    # auto-detect which arg is which
    eng = al = None
    for p in args.logs:
        if not os.path.exists(p):
            print(f"warning: missing {p}", file=sys.stderr); continue
        head = read_text(p)[:200]
        if "ENGBLOCK" in head or p.endswith(".log"):
            eng = load_engine(p)
        else:
            al = load_al_trace(p)
    if not eng or not al:
        print("need one ENGBLOCK log and one AL block_decision_trace CSV", file=sys.stderr); sys.exit(1)

    types = [args.type.upper()] if args.type else ["SOCIAL", "HOA"]
    years = sorted({y for (y, bt) in list(eng) + list(al)})
    HS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING", "NATURAL_GAS_BLOCK"]

    for bt in types:
        print(f"\n=== {bt} blocks: triggered / chose-HYBRID / notTopEAC, by year ===")
        print(f"{'year':>6} | {'ENG trig':>8}{'AL trig':>8} | {'ENG hyb':>8}{'AL hyb':>7} | {'ENG nT':>7}{'AL nT':>6}")
        e_hyb_tot = a_hyb_tot = 0
        for y in years:
            e = eng.get((y, bt)); a = al.get((y, bt))
            if not e and not a:
                continue
            et = e["trig"] if e else 0; at = a["trig"] if a else 0
            eh = e["chosen"].get("HYBRID_HEAT_PUMP", 0) if e else 0
            ah = a["chosen"].get("HYBRID_HEAT_PUMP", 0) if a else 0
            en = e["nottop"] if e else 0; an = a["nottop"] if a else 0
            e_hyb_tot += eh; a_hyb_tot += ah
            print(f"{y:>6} | {et:>8}{at:>8} | {eh:>8}{ah:>7} | {en:>7}{an:>6}")
        print(f"  total blocks choosing HYBRID over all years:  ENGINE={e_hyb_tot}  AL={a_hyb_tot}"
              f"  (diff {e_hyb_tot - a_hyb_tot:+d})")


if __name__ == "__main__":
    main()
