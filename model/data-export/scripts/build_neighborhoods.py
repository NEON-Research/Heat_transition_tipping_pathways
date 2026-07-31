"""Build ONE combined neighbourhood table for the engine, merging every neighbourhood-level source
in the top-level data/ folder into model/data/reference/neighborhoods.csv:

  neighborhoods_data_2023.csv                      CBS buurt attributes (a_hh, a_lan_ha, g_ele, ...)
  nbh_policy_plan_2023.csv                         TVW policy plan / start+end year / infra
  EVsPerPC.csv                                     PC4 -> BEV share
      ^ the above three are merged by export_neighborhoods.py, which this script runs first
  Hoofdverwarmingsinstallaties_woningen_2022_2024  heating-method shares per buurt for 2022/23/24
  neighborhood_shapes_2023.xlsx                    buurtnaam / wijkcode / gemeentecode (geometry skipped)

Added columns:
  gasCV_<yr>, gasBlock_<yr>, ehp_<yr>, hhp_<yr>, dh_<yr>   share (0-1) per year 2022/2023/2024
  complete_<yr>                                            1 = no CBS cell suppression that year
  complete_all                                             1 = usable in every year (calibration set)
  buurtnaam, wijkcode, gemeentecode                        labels from the shapes file

Existing columns are preserved, so the engine's current loader keeps working.
Requires: openpyxl.  Usage: python build_neighborhoods.py
"""
import csv, os, subprocess, sys
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", "..", ".."))
DATA = os.environ.get("HT_DATA_DIR", os.path.join(ROOT, "data"))
REF = os.path.normpath(os.path.join(HERE, "..", "..", "data", "reference"))
OUT = os.path.join(REF, "neighborhoods.csv")
YEARS = ("2022", "2023", "2024")
SHARES = ("gasCV", "gasBlock", "ehp", "hhp", "dh")


def val(v):
    if v is None:
        return None
    s = str(v).strip()
    if s in (".", "", "x", ".."):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def heating_by_year():
    """buurtcode -> {year: {share: value, 'complete': 0/1}} from the CBS maatwerk table."""
    src = os.path.join(DATA, "Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx")
    wb = openpyxl.load_workbook(src, read_only=True, data_only=True)
    ws = wb["Tabel 2"]
    out = {}
    for r in ws.iter_rows(min_row=5, values_only=True):
        if str(r[3]).strip() != "Buurt":
            continue
        code, year = r[1], str(r[2]).strip()
        if not code or year not in YEARS:
            continue
        icv, blok = val(r[6]), val(r[7])
        sv = [val(r[i]) for i in (8, 9, 10)]          # stadsverwarming hoog/laag/zonder
        elh, ell, elz = val(r[11]), val(r[12]), val(r[13])
        complete = None not in (icv, blok, elh, ell, elz)   # suppressed sv just means "no DH here"
        z = lambda x: 0.0 if x is None else x
        v = {"gasCV": z(icv), "gasBlock": z(blok), "ehp": z(ell) + z(elz),
             "hhp": z(elh), "dh": sum(z(x) for x in sv)}
        tot = sum(v.values())
        if complete and tot > 0:      # renormalise away 'Type installaties onbekend' (~2 %)
            v = {k: x / tot for k, x in v.items()}
        else:
            v = {k: x / 100 for k, x in v.items()}
        v["complete"] = 1 if complete else 0
        out.setdefault(str(code), {})[year] = v
    wb.close()
    return out


def shape_labels():
    """buurtcode -> (buurtnaam, wijkcode, gemeentecode); geometry columns are ignored."""
    src = os.path.join(DATA, "neighborhood_shapes_2023.xlsx")
    if not os.path.exists(src):
        print(f"  (shapes file absent, skipping labels: {src})")
        return {}
    wb = openpyxl.load_workbook(src, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = ws.iter_rows(values_only=True)
    hdr = [str(h) if h is not None else "" for h in next(rows)]
    ix = {name: hdr.index(name) for name in ("buurtcode", "buurtnaam", "wijkcode", "gemeenteco")
          if name in hdr}
    out = {}
    if "buurtcode" in ix:
        for r in rows:
            code = r[ix["buurtcode"]]
            if code:
                out[str(code)] = {k: (r[i] if r[i] is not None else "")
                                  for k, i in ix.items() if k != "buurtcode"}
    wb.close()
    return out


def main():
    # 1. base table (CBS attributes + policy plan + EV share) via the existing, tested exporter
    print("Building neighbourhood table:")
    print("  [1/3] base attributes + policy plan + EV share (export_neighborhoods.py)")
    r = subprocess.run([sys.executable, os.path.join(HERE, "export_neighborhoods.py")],
                       cwd=HERE, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stdout + r.stderr)
    with open(OUT, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        base_cols = list(rows[0].keys()) if rows else []

    # 2. join heating shares per year + shape labels
    print("  [2/3] heating shares 2022/2023/2024 + completeness flags")
    heat = heating_by_year()
    labels = shape_labels()
    new_cols = [f"{s}_{y}" for y in YEARS for s in SHARES] + [f"complete_{y}" for y in YEARS] \
               + ["complete_all"] + (["buurtnaam", "wijkcode", "gemeentecode"] if labels else [])
    for row in rows:
        b = row["buurtcode"]
        h = heat.get(b, {})
        for y in YEARS:
            hv = h.get(y)
            for s in SHARES:
                row[f"{s}_{y}"] = round(hv[s], 6) if hv else ""
            row[f"complete_{y}"] = hv["complete"] if hv else 0
        row["complete_all"] = 1 if all(h.get(y, {}).get("complete") for y in YEARS) else 0
        if labels:
            lb = labels.get(b, {})
            row["buurtnaam"] = lb.get("buurtnaam", "")
            row["wijkcode"] = lb.get("wijkcode", "")
            row["gemeentecode"] = lb.get("gemeenteco", "")

    print("  [3/3] writing " + os.path.relpath(OUT, ROOT))
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=base_cols + [c for c in new_cols if c not in base_cols])
        w.writeheader()
        w.writerows(rows)

    # coverage: how much of the housing stock the calibration subset actually represents
    def hh(rs):
        t = 0
        for x in rs:
            try:
                t += float(x.get("a_hh") or 0)
            except ValueError:
                pass
        return t
    tot, comp = hh(rows), hh([r for r in rows if r["complete_all"] == 1])
    nb_c = sum(1 for r in rows if r["complete_all"] == 1)
    print(f"\n{len(rows):,} neighbourhoods, {tot:,.0f} households")
    print(f"  usable in all of {'/'.join(YEARS)}: {nb_c:,} buurten ({100*nb_c/len(rows):.1f} %) "
          f"covering {comp:,.0f} households ({100*comp/tot:.1f} % of the stock)")
    print("  -> suppressed buurten are small, so household coverage far exceeds buurt coverage")


if __name__ == "__main__":
    main()
