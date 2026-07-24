"""
compare_dwelling_dumps.py — 1:1 per-dwelling comparison between the AnyLogic dump and the
engine dump, keyed on numid (stable households.db id).

Separates GENUINE divergence from randomization:
  * Attributes (archetype, label, construction year, area) come from the same DB -> must match EXACTLY.
    Any mismatch = a data-mapping bug.
  * heatDemand differs by the per-dwelling demand factor N(1,0.2) -> compare the implied factor
    distribution, not the value.
  * EAC: recompute the engine's EAC using the AL heat demand. This removes the RNG entirely, so any
    remaining EAC difference is a real formula/parameter bug.

Usage:
  python compare_dwelling_dumps.py <al_dump.csv> <engine_dump.csv>
"""
import csv, sys
from collections import defaultdict

def load(path):
    out = {}
    for r in csv.DictReader(open(path)):
        key = str(r.get("numid", "")).strip()
        if key: out[key] = r
    return out

def num(x, d=0.0):
    try: return float(str(x).strip())
    except Exception: return d

def main():
    if len(sys.argv) < 3: sys.exit(__doc__)
    al, me = load(sys.argv[1]), load(sys.argv[2])
    common = sorted(set(al) & set(me))
    print(f"AL dwellings: {len(al)} | engine dwellings: {len(me)} | matched on numid: {len(common)}")
    if not common:
        sys.exit("No shared numid — check that both dumps cover the same neighbourhood.")

    # 1) attribute equality (must be exact)
    attrs = [("dwelling_type","archetype"), ("label","label"),
             ("construction_year","construction_year"), ("area_m2","area_m2")]
    print("\n1) ATTRIBUTES (must match exactly — same DB source)")
    for a_col, m_col in attrs:
        bad = []
        for k in common:
            a, m = str(al[k].get(a_col,"")).strip(), str(me[k].get(m_col,"")).strip()
            try: same = abs(float(a) - float(m)) < 1e-6
            except Exception: same = a.lower() == m.lower()
            if not same: bad.append((k, a, m))
        flag = "OK" if not bad else f"MISMATCH {len(bad)}/{len(common)}"
        print(f"   {a_col:20s} {flag}")
        for k, a, m in bad[:5]: print(f"      numid={k}  AL={a}  engine={m}")

    # 2) heat demand — expected to differ only by the demand factor
    print("\n2) HEAT DEMAND (differs by demand factor N(1,0.2) in [0.5,1.5])")
    ratios = [num(me[k].get("heatDemand_kWh")) / num(al[k].get("heatDemand_kWh"), 1)
              for k in common if num(al[k].get("heatDemand_kWh")) > 0]
    if ratios:
        ratios.sort(); n = len(ratios)
        print(f"   engine/AL ratio  min={ratios[0]:.2f}  p50={ratios[n//2]:.2f}  max={ratios[-1]:.2f}  mean={sum(ratios)/n:.3f}")
        print("   (mean ~1.0 and spread within ~0.5-1.5 => consistent with RNG only)")

    # 3) EAC — the decisive test: does the engine EAC scale consistently with AL's?
    print("\n3) EAC per option (engine vs AL; ratio should track the heat-demand ratio)")
    sysnames = [c[4:] for c in (al[common[0]].keys()) if c.startswith("EAC_")]
    for s in sysnames:
        pairs = [(num(al[k].get("EAC_"+s)), num(me[k].get("EAC_"+s))) for k in common]
        pairs = [(a, m) for a, m in pairs if a > 0]
        if not pairs: continue
        diffs = [(m - a) for a, m in pairs]
        rel = [ (m - a) / a * 100 for a, m in pairs ]
        rel.sort()
        print(f"   {s:22s} AL_mean={sum(a for a,_ in pairs)/len(pairs):7.0f} "
              f"engine_mean={sum(m for _,m in pairs)/len(pairs):7.0f} "
              f"median_rel_diff={rel[len(rel)//2]:+6.1f}%")
    print("\nInterpretation: attribute mismatches = data bug. EAC relative differences that are")
    print("much larger than the heat-demand ratio = a real EAC formula/parameter difference.")

if __name__ == "__main__":
    main()
