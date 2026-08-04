#!/usr/bin/env python3
"""Which owner-and-dwelling combinations end up on which heating method?

Reads <run>_segments.csv and draws a heatmap of the 2050 technology mix per segment, sorted so
that similar segments sit together. Answers "who ends up on what" directly, and reveals the
clusters (e.g. owner-occupied ground-access dwellings -> electric; stacked/rented -> hybrid or DH).

    python segment_clusters.py --run <path>/simulation_results.csv --segment-type context
    python segment_clusters.py --run <path>/simulation_results.csv --segment-type dwelling --top 30
"""
import argparse, csv, os
from collections import defaultdict
import numpy as np, matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt

TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE  = {"NATURAL_GAS_BOILER":"Gas", "HYBRID_HEAT_PUMP":"Hybrid HP",
         "ELECTRIC_HEAT_PUMP":"Electric HP", "DISTRICT_HEATING":"DH"}

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", required=True)
    ap.add_argument("--segment-type", default="context", choices=["context","dwelling","rogers"])
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--year", type=int, default=2050)
    ap.add_argument("--min-dwellings", type=int, default=500, help="hide tiny segments")
    ap.add_argument("--top", type=int, default=40, help="show the N largest segments")
    ap.add_argument("--filter", default=None, help="only segments containing this string")
    ap.add_argument("--outdir", default=None)
    a = ap.parse_args()
    path = a.run.replace(".csv", "") + "_segments.csv"
    outdir = a.outdir or os.path.join(os.path.dirname(a.run), "mechanism")
    os.makedirs(outdir, exist_ok=True)

    stock = defaultdict(lambda: defaultdict(list))
    for r in csv.DictReader(open(path)):
        if r["segment_type"] != a.segment_type or r["scenario_name"] != a.scenario: continue
        if int(float(r["year"])) != a.year: continue
        stock[r["segment"]][r["heating_system"]].append(float(r["stock"]))
    if not stock:
        raise SystemExit(f"no '{a.segment_type}' segments for {a.scenario} {a.year} in {path}")

    rows = []
    for seg, d in stock.items():
        if a.filter and a.filter not in seg: continue
        m = {t: float(np.mean(v)) for t, v in d.items()}
        tot = sum(m.values())
        if tot < a.min_dwellings: continue
        rows.append((seg, tot, [100*m.get(t, 0)/tot for t in TECHS]))
    rows.sort(key=lambda r: -r[1])
    rows = rows[:a.top]
    # order segments by their mix so clusters sit together (electric-heavy at the top)
    rows.sort(key=lambda r: (-r[2][2], -r[2][1]))
    labels = [f"{s}  (n={int(n):,})" for s, n, _ in rows]
    M = np.array([v for _, _, v in rows])

    fig, ax = plt.subplots(figsize=(7.2, max(3.5, 0.28*len(rows))))
    im = ax.imshow(M, cmap="YlGnBu", aspect="auto", vmin=0, vmax=100)
    ax.set_xticks(range(len(TECHS))); ax.set_xticklabels([NICE[t] for t in TECHS], fontsize=8)
    ax.set_yticks(range(len(rows))); ax.set_yticklabels(labels, fontsize=6.5)
    for i in range(len(rows)):
        for j in range(len(TECHS)):
            if M[i, j] >= 5:
                ax.text(j, i, f"{M[i,j]:.0f}", ha="center", va="center", fontsize=6,
                        color="white" if M[i, j] > 55 else "black")
    ax.set_title(f"{a.year} heating mix per {a.segment_type} segment ({a.scenario})", fontsize=9)
    fig.colorbar(im, ax=ax, shrink=.6, label="% of segment")
    tagf = f"_{a.filter}" if a.filter else ""
    fp = os.path.join(outdir, f"seg_clusters_{a.segment_type}{tagf}_{a.year}.png")
    fig.savefig(fp, dpi=150, bbox_inches="tight"); plt.close(fig)

    csvp = os.path.join(outdir, f"seg_clusters_{a.segment_type}{tagf}_{a.year}.csv")
    with open(csvp, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["segment", "dwellings"] + [NICE[t] for t in TECHS])
        for seg, n, v in rows: w.writerow([seg, int(n)] + [f"{x:.2f}" for x in v])
    print(f"wrote {fp}\nwrote {csvp}")
    print(f"\n{'segment':<46}{'n':>9}" + "".join(f"{NICE[t]:>13}" for t in TECHS))
    for seg, n, v in rows[:12]:
        print(f"{seg:<46}{int(n):>9,}" + "".join(f"{x:>12.1f}%" for x in v))

if __name__ == "__main__":
    main()
