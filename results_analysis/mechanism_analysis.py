#!/usr/bin/env python3
"""Tipping-mechanism analysis: combines <run>_loop_state.csv and <run>_segments.csv.

Answers, per mechanism: does the reinforcing loop fire, when does it tip, and WHICH GROUPS does it
move? Produces:
  1. loop_state    -- learned capex / salience vs adoption rate, with the inflection (tipping) year
  2. segment_scurve-- adoption S-curve per Rogers segment, ordering test, and per-segment tipping year
  3. drivers       -- which TPB term carried the chosen option, per segment over time
  4. knockout      -- if knock-out runs are supplied, the causal contribution of each loop

    python mechanism_analysis.py --run ../results/noord-brabant/calib/set00_mad0.21/simulation_results.csv
    python mechanism_analysis.py --run <base.csv> --knockout nolearn=<a.csv> nosocial=<b.csv>
"""
import argparse, csv, os, re
from collections import defaultdict
import numpy as np, matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt

HP = ["HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP"]
# (key, csv column, axis label, filename) -- every decision term recorded for the CHOSEN option
DRIVERS = [("att","avg_att"), ("sn","avg_sub_norm"), ("pbc","avg_pbc"),
           ("util","avg_util"), ("intent","avg_intention"), ("eac","avg_eac")]
TECH_COLOR = {"NATURAL_GAS_BOILER":"#7f7f7f", "HYBRID_HEAT_PUMP":"#d62728",
              "ELECTRIC_HEAT_PUMP":"#1f77b4", "DISTRICT_HEATING":"#ff7f0e"}
DRIVER_META = {"att":   ("Attitude toward the chosen option", "mech_drivers_attitude.png"),
               "sn":    ("Subjective norm of the chosen option", "mech_drivers.png"),
               "pbc":   ("Perceived behavioural control of the chosen option", "mech_drivers_pbc.png"),
               "util":  ("Perceived utility of the chosen option", "mech_drivers_utility.png"),
               "intent":("Intention toward the chosen option", "mech_drivers_intention.png"),
               "eac":   ("Equivalent annual cost of the chosen option (EUR)", "mech_drivers_eac.png")}
NICE = {"HYBRID_HEAT_PUMP":"Hybrid HP","ELECTRIC_HEAT_PUMP":"Electric HP",
        "NATURAL_GAS_BOILER":"Gas boiler","DISTRICT_HEATING":"District heating"}

def sibling(run, suffix):
    return run.replace(".csv", "") + suffix

def load_loop(path, scen="baseline"):
    """-> {tech: {year: {field: mean over iterations}}}"""
    acc = defaultdict(lambda: defaultdict(list))
    for r in csv.DictReader(open(path)):
        if r["scenario_name"] != scen: continue
        acc[(r["heating_system"], int(r["year"]))]["capex"].append(float(r["learned_capex"]))
        acc[(r["heating_system"], int(r["year"]))]["sal"].append(float(r["salience"]))
        acc[(r["heating_system"], int(r["year"]))]["cum"].append(float(r["cumulative_installs"]))
        acc[(r["heating_system"], int(r["year"]))]["inst"].append(float(r["installed_annually"]))
    out = defaultdict(dict)
    for (t, y), d in acc.items():
        row = {k: float(np.mean(v)) for k, v in d.items()}
        if row.get("capex", 0) <= 0:   # the t0 row carries no decision-time loop state yet
            continue
        out[t][y] = row
    return out

def load_seg(path, segtype="rogers", scen="baseline"):
    """-> {segment: {year: {tech: stock}}} and driver terms."""
    stock = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    drv = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for r in csv.DictReader(open(path)):
        if r["segment_type"] != segtype or r["scenario_name"] != scen: continue
        y, s, t = int(r["year"]), r["segment"], r["heating_system"]
        stock[s][y][t].append(float(r["stock"]))
        n = float(r["considered"])
        if n > 0:
            # keyed per TECHNOLOGY: the subjective norm is technology-specific, so collapsing across
            # technologies mixes (say) the norm of gas among laggards with that of electric among
            # innovators, and jumps discontinuously when a segment's dominant choice switches.
            for k, c in DRIVERS:
                if c in r and r[c] not in ("", None):
                    drv[s][y][f"{k}|{t}"].append((float(r[c]) * n, n))
    S = {s: {y: {t: float(np.mean(v)) for t, v in yy.items()} for y, yy in d.items()} for s, d in stock.items()}
    D = {}
    for s, d in drv.items():
        D[s] = {}
        for y, yy in d.items():
            row = {}
            for k, v in yy.items():
                tot = sum(b for _, b in v)
                row[k] = (sum(a for a, _ in v) / tot) if tot else 0.0
                row["n|" + k.split("|", 1)[1]] = tot
            D[s][y] = row
    return S, D

def tipping_year(years, vals):
    """Year of maximum acceleration (2nd derivative) -- the inflection where growth becomes
    self-sustaining. Returns None if the series never accelerates."""
    if len(vals) < 5: return None
    d2 = np.gradient(np.gradient(np.array(vals, float)))
    i = int(np.argmax(d2))
    return years[i] if d2[i] > 0 else None

def seglabel(name):
    """'1_innovators' -> 'innovators'; other segmentations are left as-is."""
    return name.split("_", 1)[1] if re.match(r"^\d+_", name) else name


def suffixed(name, segtype):
    """Keep the rogers filenames stable; tag other segmentations so runs do not overwrite."""
    if segtype == "rogers": return name
    base, ext = name.rsplit(".", 1)
    return f"{base}_{segtype}.{ext}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", required=True, help="a simulation_results.csv (siblings _loop_state/_segments are read)")
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--segment-type", default="rogers", choices=["rogers","dwelling","context"],
                    help="which segmentation to trace (dwelling/context have many segments; see --top-segments)")
    ap.add_argument("--filter", default=None,
                    help="only segments whose name contains this (e.g. PRIVATELY_OWNED)")
    ap.add_argument("--select", default="diverse", choices=["diverse","largest"],
                    help="which segments to keep: 'diverse' spans the outcome range (default), "
                         "'largest' takes the most populous (often near-duplicates)")
    ap.add_argument("--top-segments", type=int, default=8,
                    help="for dwelling/context: keep only the N largest segments so the lines stay readable")
    ap.add_argument("--knockout", nargs="*", default=[], metavar="LABEL=CSV")
    ap.add_argument("--min-dwellings", type=int, default=3000,
                    help="ignore segments smaller than this when selecting")
    ap.add_argument("--min-adopters", type=int, default=100,
                    help="hide segment-technology lines with fewer adopters per year (small-sample noise)")
    ap.add_argument("--outdir", default=None)
    a = ap.parse_args()
    outdir = a.outdir or os.path.join(os.path.dirname(a.run), "mechanism")
    os.makedirs(outdir, exist_ok=True)

    loop = load_loop(sibling(a.run, "_loop_state.csv"), a.scenario)
    segS, segD = load_seg(sibling(a.run, "_segments.csv"), a.segment_type, a.scenario)
    if not segS:
        raise SystemExit(f"no '{a.segment_type}' segments found in {sibling(a.run,'_segments.csv')} "
                         f"(runs made before this segmentation existed only carry 'rogers'/'dwelling')")
    if a.filter:
        segS = {k: v for k, v in segS.items() if a.filter in k}
        segD = {k: v for k, v in segD.items() if a.filter in k}
        if not segS: raise SystemExit(f"no segments matching '{a.filter}'")
    if a.segment_type != "rogers":
        # 100+ dwelling/context segments cannot be read as lines, so thin them
        last = max(next(iter(segS.values())).keys())
        size = {s0: sum(segS[s0][last].values()) for s0 in segS}
        big = [k for k in size if size[k] >= a.min_dwellings]
        if a.select == "largest":
            keep = sorted(big, key=size.get, reverse=True)[:a.top_segments]
        else:
            # 'diverse': among segments of a usable size, take those spanning the outcome range --
            # ranking by population alone returns near-duplicates of the commonest archetype
            def mix(k):
                d = segS[k][last]; tot = sum(d.values()) or 1
                return np.array([d.get(t, 0)/tot for t in
                                 ["NATURAL_GAS_BOILER","HYBRID_HEAT_PUMP","ELECTRIC_HEAT_PUMP","DISTRICT_HEATING"]])
            pts = {k: mix(k) for k in big}
            keep = [max(pts, key=lambda k: pts[k][2])]                 # most-electric anchor
            while len(keep) < min(a.top_segments, len(pts)):           # then greedily the most different
                keep.append(max((k for k in pts if k not in keep),
                                key=lambda k: min(np.linalg.norm(pts[k]-pts[j]) for j in keep)))
        keep = set(keep)
        segS = {k: v for k, v in segS.items() if k in keep}
        segD = {k: v for k, v in segD.items() if k in keep}
        print(f"[{a.segment_type}] {a.select}: showing {len(segS)} of {len(size)} segments"
              + (f" (filter='{a.filter}')" if a.filter else "")
              + f" — {100*sum(size[k] for k in keep)/sum(size.values()):.0f}% of the matched dwellings")

    # ---- 1. loop state vs adoption -------------------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for t in HP:
        yrs = sorted(loop[t]); cap = [loop[t][y]["capex"] for y in yrs]
        inst = [loop[t][y]["inst"] for y in yrs]; sal = [loop[t][y]["sal"] for y in yrs]
        axes[0].plot(yrs, cap, label=NICE[t]); axes[1].plot(yrs, inst, label=NICE[t])
        ty = tipping_year(yrs, inst)
        if ty: axes[1].axvline(ty, ls=":", lw=1, color="k"); axes[1].annotate(f"tip {ty}", (ty, max(inst)*.9), fontsize=8)
    axes[0].set_title("Learned capital cost (economic loop)"); axes[0].set_ylabel("EUR/unit")
    axes[1].set_title("Annual installations + tipping year"); axes[1].set_ylabel("installs/yr")
    for ax in axes: ax.grid(alpha=.3); ax.legend(fontsize=8)
    f1 = os.path.join(outdir, ("mech_loop_state.png" if a.segment_type=="rogers" else "mech_loop_state_"+a.segment_type+".png")); fig.savefig(f1, dpi=130, bbox_inches="tight"); plt.close(fig)

    # ---- 2. per-segment S-curves + ordering test ------------------------------------------------
    segs = sorted(segS)
    # one panel per technology so hybrid / electric / district heating can be compared directly
    PANELS = [("HYBRID_HEAT_PUMP", "Hybrid heat pump"),
              ("ELECTRIC_HEAT_PUMP", "Electric heat pump"),
              ("DISTRICT_HEATING", "District heating")]
    fig, axes = plt.subplots(1, len(PANELS), figsize=(6*len(PANELS), 4.6), sharey=False)
    order_ok = []
    for i, (tech, title) in enumerate(PANELS):
        for s in segs:
            yrs = sorted(segS[s])
            v = [100*segS[s][y].get(tech, 0)/(sum(segS[s][y].values()) or 1) for y in yrs]
            axes[i].plot(yrs, v, label=seglabel(s))
        axes[i].set_title(f"{title} — share of own segment")
        axes[i].set_xlabel("year"); axes[i].set_ylabel("% of segment")
        axes[i].grid(alpha=.3); axes[i].legend(fontsize=7)
    # ordering test uses total heat-pump uptake (the diffusion signal), mid-horizon
    for s in segs:
        yrs = sorted(segS[s]); mid = yrs[len(yrs)//2]
        order_ok.append(100*sum(segS[s][mid].get(t, 0) for t in HP)/(sum(segS[s][mid].values()) or 1))
    f2 = os.path.join(outdir, ("mech_segment_scurves.png" if a.segment_type=="rogers" else "mech_segment_scurves_"+a.segment_type+".png")); fig.savefig(f2, dpi=130, bbox_inches="tight"); plt.close(fig)
    monotone = all(order_ok[i] >= order_ok[i+1] for i in range(len(order_ok)-1))

    # ---- 3. driver decomposition per segment ---------------------------------------------------
    DP = [("NATURAL_GAS_BOILER","Gas boiler"),("HYBRID_HEAT_PUMP","Hybrid HP"),
          ("ELECTRIC_HEAT_PUMP","Electric HP"),("DISTRICT_HEATING","District heating")]
    driver_figs = []
    for key, (label, fname) in DRIVER_META.items():
        # skip terms absent from this run's CSV (e.g. intention on runs from an older engine)
        if not any(f"{key}|{t}" in segD[s0][y] for s0 in segs for y in segD.get(s0, {}) for t, _ in DP):
            continue
        fig, axes = plt.subplots(1, len(DP), figsize=(5.2*len(DP), 4.4), sharey=True)
        for i, (tech, title) in enumerate(DP):
            for s0 in segs:
                yrs = [y for y in sorted(segD.get(s0, {}))
                       if f"{key}|{tech}" in segD[s0][y] and segD[s0][y].get(f"n|{tech}", 0) >= a.min_adopters]
                if not yrs: continue
                axes[i].plot(yrs, [segD[s0][y][f"{key}|{tech}"] for y in yrs],
                             label=seglabel(s0), marker=".", ms=3)
            axes[i].set_title(title); axes[i].set_xlabel("year"); axes[i].grid(alpha=.3)
        axes[0].set_ylabel(label + "\n(among its adopters)"); axes[0].legend(fontsize=7)
        fig.suptitle(f"{label} — per technology and adopter segment "
                     f"(only cells with >= {a.min_adopters} adopters that year)", fontsize=10)
        fp = os.path.join(outdir, fname if a.segment_type=="rogers" else fname.rsplit(".",1)[0]+"_"+a.segment_type+".png"); fig.savefig(fp, dpi=130, bbox_inches="tight"); plt.close(fig)
        driver_figs.append(fp)
    # transposed view for utility: one panel per adopter segment, technologies as lines -- shows
    # within a segment which option carried the highest perceived utility over time.
    # NOTE: utility is recorded for the option ACTUALLY CHOSEN, so a technology only appears in a
    # segment's panel in years that segment adopted it; this is not a like-for-like comparison of
    # all alternatives available to that segment.
    if any(f"util|{t}" in segD[s0][y] for s0 in segs for y in segD.get(s0, {}) for t, _ in DP):
        fig, axes = plt.subplots(1, len(segs), figsize=(2.6*len(segs), 3.4), sharey=True)
        if len(segs) == 1: axes = [axes]
        seen = {}
        for i, s0 in enumerate(segs):
            for tech, title in DP:
                yrs = [y for y in sorted(segD.get(s0, {}))
                       if f"util|{tech}" in segD[s0][y] and segD[s0][y].get(f"n|{tech}", 0) >= a.min_adopters]
                if not yrs: continue
                ln, = axes[i].plot(yrs, [segD[s0][y][f"util|{tech}"] for y in yrs],
                                   color=TECH_COLOR[tech], lw=1.6)
                seen.setdefault(title, ln)          # one legend entry per technology, any panel
            axes[i].set_title(seglabel(s0), fontsize=7)
            axes[i].tick_params(labelsize=7); axes[i].grid(alpha=.3)
            axes[i].set_xlabel("year", fontsize=8)
        axes[0].set_ylabel("perceived utility\nof the chosen option", fontsize=8)
        # figure-level legend so technologies plotted only in later panels still appear
        fig.legend(seen.values(), seen.keys(), loc="lower center", ncol=len(seen),
                   fontsize=8, frameon=False, bbox_to_anchor=(0.5, -0.06))
        fig.suptitle(f"Perceived utility by adopter segment (cells with >= {a.min_adopters} adopters)",
                     fontsize=9)
        fp = os.path.join(outdir, ("mech_utility_by_segment.png" if a.segment_type=="rogers" else "mech_utility_by_segment_"+a.segment_type+".png"))
        fig.savefig(fp, dpi=150, bbox_inches="tight"); plt.close(fig); driver_figs.append(fp)

    f3 = driver_figs[0] if driver_figs else None

    # ---- 4. knock-out comparison ---------------------------------------------------------------
    ko_lines = []
    if a.knockout:
        def mix2050(p):
            acc = defaultdict(list)
            for r in csv.DictReader(open(p)):
                if r["scenario_name"]==a.scenario and r["ownership"]=="TOTAL" and int(float(r["year"]))==2050:
                    acc[r["heating_system"]].append(float(r["installed_current"]))
            m = {k: float(np.mean(v)) for k, v in acc.items()}; tot = sum(m.values()) or 1
            return {k: 100*v/tot for k, v in m.items()}
        base = mix2050(a.run)
        ko_lines.append(("both loops on", base))
        for spec in a.knockout:
            lab, path = spec.split("=", 1)
            ko_lines.append((lab, mix2050(path)))

    # ---- report --------------------------------------------------------------------------------
    rep = os.path.join(outdir, ("mechanism_report.txt" if a.segment_type=="rogers" else "mechanism_report_"+a.segment_type+".txt"))
    with open(rep, "w") as fh:
        def w(x=""): fh.write(x + "\n"); print(x)
        w(f"MECHANISM ANALYSIS — {a.scenario} — {a.run}")
        w()
        w("1. LOOP STATE (economic learning)")
        for t in HP:
            yrs = sorted(loop[t]); c0, c1 = loop[t][yrs[0]]["capex"], loop[t][yrs[-1]]["capex"]
            cum1 = loop[t][yrs[-1]]["cum"]; ty = tipping_year(yrs, [loop[t][y]["inst"] for y in yrs])
            w(f"   {NICE[t]:14s} capex {c0:,.0f} -> {c1:,.0f} ({100*(c1/c0-1):+.1f}%), "
              f"cumulative {cum1:,.0f}, tipping year {ty}")
        w()
        w("2. ADOPTER SEGMENTS (Rogers) — 2050 share of own segment")
        w(f"   {'segment':<18}{'hybrid':>9}{'electric':>10}{'DH':>8}{'all HP':>9}{'gas':>8}")
        for s in segs:
            d = segS[s][2050]; tot = sum(d.values()) or 1
            w(f"   {s:<18}"
              f"{100*d.get('HYBRID_HEAT_PUMP',0)/tot:>8.1f}%"
              f"{100*d.get('ELECTRIC_HEAT_PUMP',0)/tot:>9.1f}%"
              f"{100*d.get('DISTRICT_HEATING',0)/tot:>7.1f}%"
              f"{100*sum(d.get(t,0) for t in HP)/tot:>8.1f}%"
              f"{100*d.get('NATURAL_GAS_BOILER',0)/tot:>7.1f}%")
        if a.segment_type == "rogers":
            w(f"   S-curve ordering (innovators lead -> laggards trail): {'PASS' if monotone else 'CHECK'}")
        w()
        if ko_lines:
            w("3. KNOCK-OUT TEST (causal contribution, 2050 mix)")
            techs = ["NATURAL_GAS_BOILER","HYBRID_HEAT_PUMP","ELECTRIC_HEAT_PUMP","DISTRICT_HEATING"]
            w(f"   {'run':<22}" + "".join(f"{NICE[t][:11]:>13}" for t in techs))
            for lab, m in ko_lines:
                w(f"   {lab:<22}" + "".join(f"{m.get(t,0):>12.1f}%" for t in techs))
        w()
        w("figures:"); [w(f"   {x}") for x in [f1, f2] + driver_figs]
    print(f"\nwrote {rep}")

if __name__ == "__main__":
    main()
