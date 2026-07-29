"""Extract the real Limburg dwelling stock from households.db (SQLite, 9.7M rows NL-wide)
and compute per-dwelling heat demand using the model's VestaMAIS archetype method
(J_Dwelling.f_setAnnualEnergyDemandFromVestaMAIS / f_getDBValue). Faithful mappings:
  woning_type -> OL_DwellingType (J_Dwelling.f_setDwellingType)
  energieklasse -> label a..g (constructor; blank -> archetype default_label)
  construction year -> archetype row via bouwjaar_min/max
  spaceHeating = (vrv_{label}_asl + vrv_{label}_opp * area)/3.6*1000  [GJ->kWh]
  dhwBase      =  vww_asl /3.6*1000
The stochastic energyDemandFactor ~ N(1,0.2) in [0.5,1.5] is applied at RUNTIME by the
engine (with its seeded RNG), so we export the deterministic space & dhw components:
  heatDemand = f*space + f*f*dhwBase   (matches the nested factor in the Java).
Output: out/limburg_dwellings.csv  (one row per dwelling).
Also exports ownership inputs so the engine assigns ownership with its own RNG.
"""
import sqlite3, json, csv, os, argparse

# Scope-aware stock export. --scope is one of:
#   nl                         whole country (~9.7M dwellings; large CSV + memory-heavy engine run)
#   province:<Name>            e.g. province:Limburg, province:Noord-Brabant
#   gemeente:<Name>            a municipality, e.g. gemeente:Maastricht
# (RES regions need a gemeente->RES mapping file; not built in yet.)
ap = argparse.ArgumentParser(description="Export a scoped dwelling stock from households.db to a CSV the engine reads.")
ap.add_argument("--db",    default="../../../data/households.db", help="path to households.db (top-level data/, git-ignored)")
ap.add_argument("--ref",   default="../../data/reference", help="reference lookup dir (archetype tables)")
ap.add_argument("--out",   default="../../data/stock", help="output dir for the generated stock CSV")
ap.add_argument("--scope", default="province:Limburg", help="nl | province:<Name> | gemeente:<Name>")
args = ap.parse_args()
HH, REF, OUT, SCOPE = args.db, args.ref, args.out, args.scope
os.makedirs(OUT, exist_ok=True)

# scope -> SQL filter + output filename
def scope_filter(scope):
    s = scope.strip()
    if s.lower() in ("nl", "all", "netherlands"):
        return "", (), "nl"
    if ":" not in s:
        raise SystemExit(f"bad --scope '{scope}'. Use nl | province:<Name> | gemeente:<Name>")
    kind, name = s.split(":", 1)
    col = {"province": "provincienaam", "gemeente": "gemeentenaam", "municipality": "gemeentenaam"}.get(kind.lower())
    if not col:
        raise SystemExit(f"unknown scope kind '{kind}'. Use province: or gemeente:")
    slug = name.lower().replace(" ", "_")
    # provinces unprefixed (limburg_dwellings.csv), municipalities prefixed to avoid clashes
    tag = slug if kind.lower() == "province" else "gemeente_" + slug
    return col + "=? AND ", (name,), tag

SCOPE_WHERE, SCOPE_PARAMS, SCOPE_TAG = scope_filter(SCOPE)

def _num(v):   # CSV values are strings; restore numbers (leave type_str/type_ol/labels as strings)
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() and "." not in v and "e" not in v.lower() else f
    except ValueError:
        return v

with open(os.path.join(REF, "dwellings_demand_insulation.csv"), newline="", encoding="utf-8") as _f:
    arch = [{k: _num(v) for k, v in row.items()} for row in csv.DictReader(_f)]

TYPE_MAP = {
    "Appartement": "APARTMENT", "Maisonnette": "APARTMENT", "Portiekwoning": "APARTMENT",
    "Flatwoning (overig)": "HIGHRISE", "Galerijwoning": "HIGHRISE",
    "Rijwoning hoek": "CORNER",
    "Rijwoning tussen": "TERRACED",
    "Twee-onder-een-kap / rijwoning hoek": "SEMIDETACHED", "Twee-onder-één-kap": "SEMIDETACHED",
    "Vrijstaande woning": "DETACHED",
    "unknown": "TERRACED",
}   # anything else (incl. blank) -> TERRACED (Java default)

def dwelling_type(woning_type):
    return TYPE_MAP.get(woning_type, "TERRACED")

def label_letter(energieklasse):
    e = (energieklasse or "").strip()
    if e in ("A++++", "A+++", "A++", "A+", "A", "A+++++"): return "a"
    if e == "B": return "b"
    if e == "C": return "c"
    if e == "D": return "d"
    if e == "E": return "e"
    if e == "F": return "f"
    if e == "G": return "g"
    return ""   # blank -> use archetype default_label

# index archetype rows by type_ol, with (min,max) bands
by_type = {}
for r in arch:
    by_type.setdefault(r["type_ol"], []).append(r)
for t in by_type:
    by_type[t].sort(key=lambda r: r["bouwjaar_min"])

def arch_row(type_ol, year):
    for r in by_type.get(type_ol, []):
        if r["bouwjaar_min"] <= year <= r["bouwjaar_max"]:
            return r
    return by_type.get(type_ol, [None])[0]

def demand_components(type_ol, label, year, area):
    r = arch_row(type_ol, year)
    if r is None: return 0.0, 0.0
    lab = label or (r.get("default_label") or "n")
    lab = lab.lower()
    asl = r.get(f"vrv_{lab}_asl"); opp = r.get(f"vrv_{lab}_opp")
    if asl is None: asl = r.get("vrv_n_asl", 0.0)
    if opp is None: opp = r.get("vrv_n_opp", 0.0)
    space = (asl + opp * area) / 3.6 * 1000.0
    vww = r.get("vww_asl", 0.0) or 0.0
    dhw = vww / 3.6 * 1000.0
    return space, dhw


# f_insulationLabelLetterToNumber: a=1 (best) .. g=7 (worst); upgrade needed if to < from
_LN = {"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"n":4}
def label_num(l): return _LN.get((l or "n").lower(), 4)

def insulation_cost(type_ol, from_label, to_label, area):
    """f_getInsulationCosts: (minTotal+maxTotal)/2, min/max = asl + opp*area. 0 if no upgrade."""
    fl = (from_label or "n").lower()
    if label_num(to_label) >= label_num(fl):   # already as good or better -> no upgrade
        return 0.0
    r = arch_row(type_ol, year_for_arch)
    if r is None: return 0.0
    col = f"ki_s{fl}{to_label}"
    a1 = r.get(col+"_min_asl"); o1 = r.get(col+"_min_opp")
    a2 = r.get(col+"_max_asl"); o2 = r.get(col+"_max_opp")
    if a1 is None or a2 is None: return 0.0
    mn = a1 + (o1 or 0)*area; mx = a2 + (o2 or 0)*area
    return (mn + mx)/2

con = sqlite3.connect(f"file:{HH}?mode=ro", uri=True); cur = con.cursor()
q = ("""SELECT numid, postcode, oppervlakte, pand_bouwjaar, energieklasse, woning_type, buurtcode,
              pc6_eigendomssituatie_perc_koop, pc6_eigendomssituatie_perc_huur,
              pc6_eigendomssituatie_aantal_woningen_corporaties
       FROM households WHERE """ + SCOPE_WHERE + "gebruiksdoelen='woonfunctie'")
params = SCOPE_PARAMS
path = os.path.join(OUT, SCOPE_TAG + "_dwellings.csv")
n = 0
with open(path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id","numid","pc6","buurtcode","dwelling_type","label","construction_year",
                "area_m2","space_heat_kwh","dhw_base_kwh","insul_to_b","insul_to_c",
                "perc_koop","perc_huur","aantal_corp"])
    for (numid, pc6, area, year, ekl, wtype, buurt, koop, huur, corp) in cur.execute(q, params):
        area = float(area or 0)
        area = min(500.0, max(20.0, area))   # AL clamps livingArea to [20, 500]
        year = int(year or 0)
        if year > 2025: year = 2025
        dt = dwelling_type(wtype)
        lab = label_letter(ekl)
        # effective label (blank -> archetype default_label), used for demand + insulation
        r0 = arch_row(dt, year)
        eff_lab = (lab or (r0.get("default_label") if r0 else "") or "n").lower()
        global year_for_arch; year_for_arch = year
        space, dhw = demand_components(dt, eff_lab, year, area)
        insul_b = insulation_cost(dt, eff_lab, "b", area)
        insul_c = insulation_cost(dt, eff_lab, "c", area)
        w.writerow([n, numid, pc6, buurt, dt, eff_lab, year, round(area,1),
                    round(space,1), round(dhw,1), round(insul_b,1), round(insul_c,1),
                    int(koop or 0), int(huur or 0), int(corp or 0)])
        n += 1
con.close()
print(f"Exported {n} dwellings (scope={SCOPE}) -> {path}  ({os.path.getsize(path)//1024//1024} MB)")
