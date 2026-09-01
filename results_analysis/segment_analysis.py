#!/usr/bin/env python3
"""Who ends up on what: 2050 heating mix by ownership, by dwelling characteristics, and within
the owner-occupied stock alone.

Answers three questions the aggregate pathways cannot:
  1. how much of the outcome is ownership (decision rule) rather than dwelling (technical fit);
  2. within owner-occupied dwellings -- where the full behavioural function applies -- do heat
     demand and dwelling type select between the low-capex/high-opex and high-capex/low-opex
     options as the cost logic predicts;
  3. which dwelling attribute (size, construction era, insulation label) separates the outcome most.

Reads results/<scope>/simulation_results_segments.csv (segment_type 'context' = ownership x
archetype x demand tier, 'dwelling' = archetype x size x era x label).

    python segment_analysis.py --scope nl
"""
import argparse, csv, os, re, sys
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
MERGE = {"NATURAL_GAS_BLOCK": "NATURAL_GAS_BOILER"}
TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE  = {"NATURAL_GAS_BOILER": "Natural gas", "HYBRID_HEAT_PUMP": "Hybrid heat pump",
         "ELECTRIC_HEAT_PUMP": "Electric heat pump", "DISTRICT_HEATING": "District heating"}
COLOR = {"NATURAL_GAS_BOILER": "#999999", "HYBRID_HEAT_PUMP": "#56B4E9",
         "ELECTRIC_HEAT_PUMP": "#0072B2", "DISTRICT_HEATING": "#D55E00"}
# archetype-size-era-label, where the era token itself contains a hyphen (1975-1999)
DWL_RE = re.compile(r"^(?P<arch>[A-Z]+)-(?P<size>small|medium|large)-"
                    r"(?P<era>pre1975|1975-1999|post2000)-(?P<label>label[A-Z]+)$")
OWN_NICE = {"PRIVATELY_OWNED": "Owner-occupied", "PRIVATELY_RENTED": "Privately rented",
            "SOCIAL_HOUSING": "Social housing", "HOME_OWNER_ASSOCIATION": "Owner association"}
DEM_NICE = {"demLow": "low demand", "demMed": "medium demand", "demHigh": "high demand"}
# adopter categories are assigned to owner-occupier households only (Segments.assignRogers)
ROG_NICE = {"1_innovators": "Innovators", "2_early_adopters": "Early adopters",
            "3_early_majority": "Early majority", "4_late_majority": "Late majority",
            "5_laggards": "Laggards"}


def tag(scope):
    k, _, n = scope.partition(":")
    if k.lower() in ("nl", "netherlands"): return "nl"
    return n.strip().lower().replace(" ", "-") if k == "province" else "gemeente_" + n.strip().lower()


def read(path, year=2050, scen="baseline"):
    """{segment_type: {segment: {tech: stock}}} plus the mean decision terms per segment."""
    stock = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    terms = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            if r["scenario_name"] != scen or int(float(r["year"])) != year: continue
            te = MERGE.get(r["heating_system"], r["heating_system"])
            stock[r["segment_type"]][r["segment"]][te] += float(r["stock"])
            for c in ("avg_eac", "avg_att", "avg_sub_norm", "avg_pbc"):
                if r.get(c):
                    try: terms[r["segment_type"]][r["segment"]][c].append(float(r[c]))
                    except ValueError: pass
    return stock, terms


def shares(d):
    tot = sum(d.values()) or 1.0
    return {te: 100.0 * d.get(te, 0.0) / tot for te in TECHS}


def group(stock_by_seg, keyfn):
    """Aggregate segments into coarser groups by keyfn(segment) -> label (None to drop)."""
    out = defaultdict(lambda: defaultdict(float)); size = defaultdict(float)
    for seg, d in stock_by_seg.items():
        k = keyfn(seg)
        if k is None: continue
        for te, v in d.items(): out[k][te] += v
        size[k] += sum(d.values())
    return {k: shares(v) for k, v in out.items()}, dict(size)


def bar_panel(ax, labels, data, title, sizes=None):
    y = np.arange(len(labels)); left = np.zeros(len(labels))
    for te in TECHS:
        v = np.array([data[l][te] for l in labels])
        ax.barh(y, v, left=left, color=COLOR[te], height=0.75); left += v
    ax.set_yticks(y)
    tick = [f"{l}  ({sizes[l]/1e3:,.0f}k)" if sizes else l for l in labels]
    ax.set_yticklabels(tick, fontsize=8)
    ax.invert_yaxis(); ax.set_xlim(0, 100); ax.set_xlabel("% of segment stock, 2050", fontsize=8)
    ax.set_title(title, fontsize=9.5); ax.tick_params(labelsize=8)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="nl")
    ap.add_argument("--year", type=int, default=2050)
    ap.add_argument("--scenario", default="baseline")
    ap.add_argument("--copy-to", default=os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures"))
    a = ap.parse_args()
    base = os.path.join(ROOT, "results", tag(a.scope))
    src = os.path.join(base, "simulation_results_segments.csv")
    if not os.path.exists(src): sys.exit(f"[seg] no segments file at {src}")
    outdir = os.path.join(base, "figures"); os.makedirs(outdir, exist_ok=True)
    stock, terms = read(src, a.year, a.scenario)
    ctx, dwl = stock.get("context", {}), stock.get("dwelling", {})
    if not ctx: sys.exit("[seg] no 'context' segments in the file")

    def report(title, data, sizes):
        print(f"\n=== {title} ===")
        print(f"{'segment':38s} {'dwellings':>11s} " + "".join(f"{NICE[t][:9]:>10s}" for t in TECHS))
        for l in sorted(data, key=lambda k: -data[k]["ELECTRIC_HEAT_PUMP"]):
            print(f"{l:38s} {sizes[l]:11,.0f} " + "".join(f"{data[l][t]:10.1f}" for t in TECHS))

    # 1 ---- ownership
    own, own_n = group(ctx, lambda s: s.split("-")[0])
    report("2050 mix by ownership", own, own_n)
    sep_own = max(own[k]["ELECTRIC_HEAT_PUMP"] for k in own) - min(own[k]["ELECTRIC_HEAT_PUMP"] for k in own)

    # 2 ---- within owner-occupied: demand tier and archetype
    po = {s: d for s, d in ctx.items() if s.startswith("PRIVATELY_OWNED-")}
    dem, dem_n = group(po, lambda s: DEM_NICE.get(s.split("-")[2], s.split("-")[2]))
    arc, arc_n = group(po, lambda s: s.split("-")[1].title())
    report("owner-occupied, by heat demand", dem, dem_n)
    report("owner-occupied, by archetype", arc, arc_n)
    sep_dem = max(dem[k]["ELECTRIC_HEAT_PUMP"] for k in dem) - min(dem[k]["ELECTRIC_HEAT_PUMP"] for k in dem)

    # 2b --- demand tier WITHIN a single archetype, so the demand effect is not archetype in
    #        disguise (heat demand and dwelling type are strongly correlated in the stock)
    for arche in ("TERRACED", "SEMIDETACHED", "APARTMENT"):
        sub = {s_: d for s_, d in po.items() if s_.split("-")[1] == arche}
        if not sub: continue
        g, n = group(sub, lambda s_: DEM_NICE.get(s_.split("-")[2], s_.split("-")[2]))
        report(f"owner-occupied {arche.lower()} only, by heat demand", g, n)

    # 2c --- adopter category (owner-occupiers only)
    rog_raw = stock.get("rogers", {})
    rog, rog_n = ({}, {})
    if rog_raw:
        rog, rog_n = group(rog_raw, lambda s_: ROG_NICE.get(s_, s_))
        report("by adopter category (owner-occupiers only)", rog, rog_n)
        seps_rog = (max(rog[k]["ELECTRIC_HEAT_PUMP"] for k in rog)
                    - min(rog[k]["ELECTRIC_HEAT_PUMP"] for k in rog))
    else:
        seps_rog = float("nan")

    # 3 ---- dwelling attributes (all ownerships pooled; the file does not cross them)
    seps = {}
    attr = {}
    if dwl:
        def part(seg, field):
            m = DWL_RE.match(seg); return m.group(field) if m else None
        for name, field in (("size", "size"), ("construction era", "era"),
                            ("insulation label", "label")):
            g, n = group(dwl, lambda s, f=field: part(s, f))
            attr[name] = (g, n); report(f"by {name} (all ownerships)", g, n)
            seps[name] = (max(g[k]["ELECTRIC_HEAT_PUMP"] for k in g)
                          - min(g[k]["ELECTRIC_HEAT_PUMP"] for k in g))
        g, n = group(dwl, lambda s: (part(s, "arch") or "").title() or None); attr["archetype"] = (g, n)
        seps["archetype"] = max(g[k]["ELECTRIC_HEAT_PUMP"] for k in g) - min(g[k]["ELECTRIC_HEAT_PUMP"] for k in g)

    print("\n=== how far each attribute separates the 2050 electric heat-pump share (pp) ===")
    print(f"{'ownership (4 classes)':34s} {sep_own:6.1f}")
    print(f"{'adopter category, owner-occ. only':34s} {seps_rog:6.1f}")
    print(f"{'heat demand, owner-occupied only':34s} {sep_dem:6.1f}")
    for k, v in sorted(seps.items(), key=lambda kv: -kv[1]):
        print(f"{k + ' (all ownerships)':34s} {v:6.1f}")

    # ---- figure: five cuts, ordered by how much they separate the outcome
    fig, axes = plt.subplots(3, 2, figsize=(12.5, 10.2))
    bar_panel(axes[0, 0],
              [OWN_NICE.get(k, k) for k in sorted(own, key=lambda k: -own[k]["ELECTRIC_HEAT_PUMP"])],
              {OWN_NICE.get(k, k): v for k, v in own.items()},
              "a. Ownership — which decision rule applies",
              {OWN_NICE.get(k, k): v for k, v in own_n.items()})
    rog_order = [ROG_NICE[k] for k in ("1_innovators", "2_early_adopters", "3_early_majority",
                                      "4_late_majority", "5_laggards")]
    if rog:
        bar_panel(axes[0, 1], [o for o in rog_order if o in rog], rog,
                  "b. Adopter category (owner-occupiers only)", rog_n)
    else:
        axes[0, 1].set_visible(False)
    order = ["low demand", "medium demand", "high demand"]
    bar_panel(axes[1, 0], [o for o in order if o in dem], dem,
              "c. Owner-occupied, by heat demand", dem_n)
    bar_panel(axes[1, 1], sorted(arc, key=lambda k: -arc[k]["ELECTRIC_HEAT_PUMP"]), arc,
              "d. Owner-occupied, by dwelling type", arc_n)
    if "insulation label" in attr:
        g, n = attr["insulation label"]
        bar_panel(axes[2, 0], sorted(g), g, "e. Insulation label (all ownership classes)", n)
    else:
        axes[2, 0].set_visible(False)
    axes[2, 1].set_visible(False)
    handles = [plt.Rectangle((0, 0), 1, 1, color=COLOR[t]) for t in TECHS]
    fig.legend(handles, [NICE[t] for t in TECHS], ncol=4, loc="lower center", frameon=False,
               fontsize=9.5, bbox_to_anchor=(0.5, 0.005))
    fig.suptitle(f"{a.year} heating mix by segment, {a.scenario} scenario", fontsize=13)
    fig.tight_layout(rect=(0, 0.035, 1, 0.955))
    f = os.path.join(outdir, "fig_segments_ownership_dwelling.png")
    fig.savefig(f, dpi=150, bbox_inches="tight"); plt.close(fig)
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil; shutil.copy2(f, a.copy_to)
    print("\nwrote:", f)


if __name__ == "__main__":
    main()
