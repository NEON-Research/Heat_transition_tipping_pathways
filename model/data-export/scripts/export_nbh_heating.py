"""Export NBH_HOUSEHOLD_HEATING_METHOD2023 -> per-buurtcode heating-method percentages,
mapped to the model's 5 systems exactly as f_setHeatingMethodNeighborhoodData:
  gasCV = individuele_cv/100 ; gasBlock = blok_verwarming/100
  EHP = (elektr. zonder + laag gas)/100 ; HHP = elektr. hoog gas/100
  DH  = (stadsverw. hoog+laag+zonder)/100 ; hasDHgrid = DH>0
Missing (-99999) -> 0. Output: out/nbh_heating.json  { buurtcode: {gasCV,gasBlock,ehp,hhp,dh,hasDHgrid} }
"""
import re, json, os
DB = "../../../deprecated/Heat transition tipping pathways/database/db.script"
OUT = "../out"; os.makedirs(OUT, exist_ok=True)
txt = open(DB, encoding="utf-8", errors="replace").read()

def z(v):  # -99999 / negatives -> 0
    return 0.0 if v is None or v < 0 else v

def num(x):
    x = x.strip()
    if x == "NULL": return None
    try: return float(x.replace("E0",""))
    except: return None

out = {}
for m in re.finditer(r"INSERT INTO (?:PUBLIC\.)?NBH_HOUSEHOLD_HEATING_METHOD2023 VALUES\((.*)\)\s*$", txt, re.M):
    # split respecting quotes
    parts=[]; cur=""; q=False
    for ch in m.group(1):
        if ch=="'": q=not q; cur+=ch
        elif ch=="," and not q: parts.append(cur); cur=""
        else: cur+=ch
    parts.append(cur)
    buurt = parts[1].strip().strip("'")
    icv=z(num(parts[6])); blok=z(num(parts[7]))
    sv_h=z(num(parts[8])); sv_l=z(num(parts[9])); sv_z=z(num(parts[10]))
    el_h=z(num(parts[11])); el_l=z(num(parts[12])); el_z=z(num(parts[13]))
    dh=(sv_h+sv_l+sv_z)/100
    out[buurt]={"gasCV":icv/100,"gasBlock":blok/100,"ehp":(el_z+el_l)/100,"hhp":el_h/100,
                "dh":dh,"hasDHgrid":dh>0}
json.dump(out, open(os.path.join(OUT,"nbh_heating.json"),"w"))
print(f"exported {len(out)} neighbourhoods -> out/nbh_heating.json")
