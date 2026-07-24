#!/usr/bin/env python3
"""
Three-way term-level comparison of the HT_DIAG probe across the JS engine, the Java engine,
and AnyLogic.

Each engine emits, at 2025/2030/2035, one line per system (gas, hybrid) of the form:

    <TAG> <year> <system> n=.. att=.. sn=.. pbc=.. eacNorm=.. salience=.. minEAC=.. maxEAC=..

where TAG is  JSDIAG (JS)  |  DIAG (Java)  |  ALDIAG (AnyLogic).

Lines are averaged over however many iterations appear in each log, so you can point it at a
20-iteration run and it collapses to the mean. Java prints European decimals (0,285); this
script normalizes commas to dots before parsing, so all three sources line up.

Usage:
    python compare_diag.py js.log java.log al.log
    python compare_diag.py run_all.log                 # one file with all three tags mixed
    python compare_diag.py js.log java.log --tol 0.02   # flag terms differing by more than tol

Any number of files may be passed; the source is detected from each line's tag, not the
filename, so a single combined log works too. Missing sources are simply omitted from the table.
"""
import argparse
import os
import re
import sys
from collections import defaultdict

TAG_TO_ENGINE = {"JSDIAG": "JS", "DIAG": "Java", "ALDIAG": "AL"}
METRICS = ["n", "att", "sn", "pbc", "eacNorm", "salience", "minEAC", "maxEAC"]
# absolute tolerance for the [0,1]-ish TPB terms; the count/EAC metrics use a relative tolerance
ABS_METRICS = {"att", "sn", "pbc", "eacNorm", "salience"}
REL_TOL = 0.05  # 5% for n / minEAC / maxEAC

LINE_RE = re.compile(r"^(JSDIAG|DIAG|ALDIAG)\s+(\d{4})\s+(gas|hybrid)\b(.*)$")


def read_text(path):
    """Read a log file, tolerating UTF-8, UTF-8-BOM, and UTF-16 (PowerShell's `>` and `2>`
    redirection writes UTF-16 by default, which is why UTF-8-only reads find no lines)."""
    with open(path, "rb") as fh:
        raw = fh.read()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", errors="replace")
    if raw.count(b"\x00") > len(raw) // 4:      # lots of nulls => UTF-16 without a BOM
        return raw.decode("utf-16-le", errors="replace")
    return raw.decode("utf-8", errors="replace")


def parse(paths):
    # sums[(engine, year, system)][metric] = [total, count]
    sums = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))
    counts = defaultdict(int)
    for path in paths:
        if not os.path.exists(path):
            print(f"warning: skipping missing file '{path}'", file=sys.stderr)
            continue
        matched = 0
        for raw in read_text(path).splitlines():
            line = raw.replace(",", ".").strip()  # normalize EU decimals
            m = LINE_RE.match(line)
            if not m:
                continue
            tag, year, system, rest = m.groups()
            engine = TAG_TO_ENGINE[tag]
            key = (engine, int(year), system)
            for tok in rest.split():
                if "=" not in tok:
                    continue
                k, v = tok.split("=", 1)
                if k not in METRICS:
                    continue
                try:
                    val = float(v)
                except ValueError:
                    continue
                sums[key][k][0] += val
                sums[key][k][1] += 1
            counts[engine] += 1
            matched += 1
        if matched == 0:
            print(f"warning: no DIAG/JSDIAG/ALDIAG lines matched in '{path}' "
                  f"(file has {os.path.getsize(path)} bytes -- check it isn't empty)", file=sys.stderr)
    return sums


def mean(sums, key, metric):
    tot, cnt = sums[key][metric]
    return tot / cnt if cnt else None


def flag(metric, values):
    """Return '' if all present engines agree within tolerance, else '  <-- DIVERGES'."""
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    if metric in ABS_METRICS:
        return "  <-- DIVERGES" if (hi - lo) > TOL else ""
    # relative for n / minEAC / maxEAC
    base = max(abs(lo), 1e-9)
    return "  <-- DIVERGES" if (hi - lo) / base > REL_TOL else ""


def main():
    ap = argparse.ArgumentParser(description="3-way HT_DIAG term comparison (JS / Java / AL)")
    ap.add_argument("logs", nargs="+", help="one or more log files containing DIAG/JSDIAG/ALDIAG lines")
    ap.add_argument("--tol", type=float, default=0.02, help="abs tolerance for TPB terms (default 0.02)")
    args = ap.parse_args()

    global TOL
    TOL = args.tol

    sums = parse(args.logs)
    if not sums:
        print("No DIAG/JSDIAG/ALDIAG lines found in:", ", ".join(args.logs), file=sys.stderr)
        sys.exit(1)

    engines = [e for e in ("JS", "Java", "AL") if any(k[0] == e for k in sums)]
    years = sorted({k[1] for k in sums})
    systems = ["gas", "hybrid"]

    def fmt(metric, val):
        if val is None:
            return "     -"
        if metric in ("n", "minEAC", "maxEAC"):
            return f"{val:10.0f}"
        return f"{val:10.3f}"

    header_cols = "".join(f"{e:>10}" for e in engines)
    print(f"\nSources: {', '.join(engines)}   (means over all iterations in the logs)")
    print(f"Tolerance: abs {TOL} for TPB terms, rel {REL_TOL:.0%} for n/EAC\n")

    artifact_notes = []
    for year in years:
        for system in systems:
            print(f"=== {year}  {system} " + "=" * 46)
            print(f"{'metric':>10}{header_cols}   flag")
            for metric in METRICS:
                vals = [mean(sums, (e, year, system), metric) for e in engines]
                row = "".join(fmt(metric, v) for v in vals)
                print(f"{metric:>10}{row}{flag(metric, vals)}")
            # n-outlier guard: a decision count >> the others is the signature of a probe
            # artifact (e.g. an accumulator summing several years), NOT a model divergence.
            ns = [(e, mean(sums, (e, year, system), "n")) for e in engines]
            ns = [(e, v) for e, v in ns if v]
            if len(ns) >= 2:
                lo = min(v for _, v in ns)
                for e, v in ns:
                    if v > 1.5 * lo:
                        artifact_notes.append(
                            f"  {year} {system}: {e} n={v:.0f} is {v/lo:.1f}x the min ({lo:.0f}) "
                            f"-- likely a PROBE ARTIFACT (counter not reset per year), not a model divergence")
            print()

    if artifact_notes:
        print("!! n-count outliers detected (check the probe before blaming the model):")
        for note in artifact_notes:
            print(note)
        print()


if __name__ == "__main__":
    main()
