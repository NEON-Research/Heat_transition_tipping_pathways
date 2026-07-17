"""Export the small HSQLDB reference tables from database/db.script to JSON.
DWELLINGS_DEMAND_INSULATION (archetype energy-demand + insulation costs),
HEATING_SYSTEM_DATA, ENERGY_SOURCE_DATA. Pure text parsing — no DB engine needed."""
import json, re, sys, os

DB = sys.argv[1] if len(sys.argv) > 1 else \
    "../../../Heat transition tipping pathways/database/db.script"
OUT = sys.argv[2] if len(sys.argv) > 2 else "../out"
os.makedirs(OUT, exist_ok=True)

txt = open(DB, encoding="utf-8", errors="replace").read()

def columns(table):
    m = re.search(r'CREATE (?:MEMORY |CACHED )?TABLE PUBLIC\.' + table + r'\((.*?)\)\s*(?:$|\n)',
                  txt, re.M)
    body = m.group(1)
    cols = []
    depth = 0; cur = ""
    for ch in body:            # split on commas not inside parens
        if ch == '(' : depth += 1
        if ch == ')' : depth -= 1
        if ch == ',' and depth == 0:
            cols.append(cur); cur = ""
        else:
            cur += ch
    cols.append(cur)
    return [c.strip().split()[0] for c in cols]

def parse_values(s):
    """Split a SQL VALUES(...) tuple honouring quotes."""
    out = []; cur = ""; q = False; depth = 0
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "'" :
            if q and i+1 < len(s) and s[i+1] == "'":  # escaped quote
                cur += "'"; i += 2; continue
            q = not q; cur += ch
        elif ch == ',' and not q and depth == 0:
            out.append(cur); cur = ""
        else:
            cur += ch
        i += 1
    out.append(cur)
    return out

def conv(v):
    v = v.strip()
    if v == "NULL": return None
    if v.startswith("'") and v.endswith("'"):
        return v[1:-1].replace("''", "'")
    v2 = v.replace("E0", "")            # HSQLDB double literals: 1.5E0
    try:
        f = float(v2)
        return int(f) if f.is_integer() and "." not in v2 and "E" not in v else f
    except ValueError:
        return v

def export(table):
    cols = columns(table)
    rows = []
    for m in re.finditer(r'INSERT INTO (?:PUBLIC\.)?' + table + r' VALUES\((.*)\)\s*$', txt, re.M):
        vals = [conv(x) for x in parse_values(m.group(1))]
        rows.append(dict(zip([c.lower() for c in cols], vals)))
    path = os.path.join(OUT, table.lower() + ".json")
    json.dump(rows, open(path, "w"), indent=1)
    print(f"  {table}: {len(rows)} rows -> {path}")
    return rows

print("Exporting reference tables:")
for t in ["DWELLINGS_DEMAND_INSULATION", "HEATING_SYSTEM_DATA", "ENERGY_SOURCE_DATA"]:
    export(t)
