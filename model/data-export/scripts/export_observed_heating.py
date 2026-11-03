"""Extract observed heating-method shares per neighbourhood (buurt) for 2022-2025 from the CBS
maatwerk tables, for CALIBRATION of the TPB weights.

Sources (top-level data/), both sheet 'Tabel 2':
    Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx   2022, 2023, 2024
    https://www.cbs.nl/nl-nl/maatwerk/2025/34/hoofdverwarmingsinstallaties-woningen-2022-2024
    Hoofdverwarmingsinstallaties_woningen_2025.xlsx        2025 (optional; skipped if absent)
    CBS PR004609, July 2026. Same method as 2022-2024, EXCEPT district heating: from 2025 CBS has
    address-level heat deliveries, so the 2024 -> 2025 change in dh is partly a method change.
    Other layout: no year column, codes in column 0, shares stored as text.

CBS changed method before 2022, so ONLY 2022 onwards is a consistent series -- see ANALYSIS/MODEL_TODOS:
use it as a broad plausibility range, not a precise fit target.

Mapping to the model's 5 systems (same rule as export_nbh_heating.py / AL
f_setHeatingMethodNeighborhoodData):
    gasCV    = Individuele CV                                            -> NATURAL_GAS_BOILER
    gasBlock = Blok-verwarming                                           -> NATURAL_GAS_BLOCK
    dh       = Stadsverwarming (hoog + laag + zonder gasverbruik)        -> DISTRICT_HEATING
    hhp      = Hoofdzakelijk elektrisch met HOOG gasverbruik             -> HYBRID_HEAT_PUMP
    ehp      = Hoofdzakelijk elektrisch (laag + zonder gasverbruik)      -> ELECTRIC_HEAT_PUMP
Suppressed cells ('.') -> missing; a buurt-year is kept only if the main columns are present.

Outputs (model/data/reference/):
  observed_heating_by_year.csv   buurtcode,year,gasCV,gasBlock,ehp,hhp,dh,complete
  nbh_heating_2022.csv           same schema as nbh_heating.csv -> use as the 2022 INITIAL state
                                 so a calibration run can start in 2022 and be scored on 2024.

Requires: openpyxl
"""
import csv, os, sys
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", "..", ".."))
DATA = os.environ.get("HT_DATA_DIR", os.path.join(ROOT, "data"))
OUT = os.path.normpath(os.path.join(HERE, "..", "..", "data", "reference"))
os.makedirs(OUT, exist_ok=True)

# 0-based column indices in 'Tabel 2' (header on row 4, data from row 5), per source file.
# year: column index, or the fixed year of a single-year table. sv = stadsverwarming hoog/laag/zonder.
SOURCES = [
    ("Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx", True,
     dict(code=1, year=2, kind=3, icv=6, blok=7, sv=(8, 9, 10), el_h=11, el_l=12, el_z=13)),
    ("Hoofdverwarmingsinstallaties_woningen_2025.xlsx", False,
     dict(code=0, year="2025", kind=1, icv=4, blok=5, sv=(6, 7, 8), el_h=9, el_l=10, el_z=11)),
]


def val(v):
    """CBS suppression '.' / blank -> None; else float (the 2025 table stores text, '75,6')."""
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if s in (".", "", "x", ".."):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def read_source(path, c):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb["Tabel 2"]
    recs = []
    for r in ws.iter_rows(min_row=5, values_only=True):
        if len(r) <= c["el_z"] or str(r[c["kind"]]).strip() != "Buurt":
            continue
        code = r[c["code"]]
        year = c["year"] if isinstance(c["year"], str) else str(r[c["year"]]).strip()
        if not code:
            continue
        icv, blok = val(r[c["icv"]]), val(r[c["blok"]])
        sv = [val(r[i]) for i in c["sv"]]
        elh, ell, elz = val(r[c["el_h"]]), val(r[c["el_l"]]), val(r[c["el_z"]])
        # 'complete' = the individually-modelled shares are present. Stadsverwarming is suppressed
        # ('.') in most buurten precisely BECAUSE there is no district heating there, so a missing
        # sv value is treated as 0 and does not make the row incomplete.
        complete = None not in (icv, blok, elh, ell, elz)
        z = lambda x: 0.0 if x is None else x
        dh = sum(z(x) for x in sv)
        recs.append({
            "buurtcode": str(code), "year": year,
            "gasCV": z(icv) / 100, "gasBlock": z(blok) / 100,
            "ehp": (z(ell) + z(elz)) / 100, "hhp": z(elh) / 100,
            "dh": dh / 100, "complete": int(bool(complete)),
        })
    wb.close()
    return recs


def main():
    recs = []
    for name, required, cols in SOURCES:
        path = os.path.join(DATA, name)
        if not os.path.exists(path):
            if required:
                sys.exit(f"source not found: {path}")
            print(f"skipping {name}: not found")
            continue
        recs += read_source(path, cols)

    p1 = os.path.join(OUT, "observed_heating_by_year.csv")
    with open(p1, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["buurtcode", "year", "gasCV", "gasBlock", "ehp", "hhp", "dh", "complete"])
        w.writeheader(); w.writerows(recs)
    years = sorted({r["year"] for r in recs})
    print(f"observed_heating_by_year.csv: {len(recs)} buurt-year rows, years {years}")

    # 2022 initial state, in the same schema the engine reads for nbh_heating.
    #
    # NB shares are RENORMALISED to sum to 1 over the five modelled systems for rows with usable
    # data. CBS shares otherwise fall short of 100 % because of (a) the 'Type installaties onbekend'
    # category (~2 %) and (b) cell suppression. The engine assigns any shortfall to the gas boiler
    # (`while (total < households) reqNGB++`), which would silently inflate gas AND make the state
    # differ from the observed mix we score against (which is renormalised the same way).
    # Rows with no usable data are written through unchanged -> engine falls back to gas, as before.
    p2 = os.path.join(OUT, "nbh_heating_2022.csv")
    n = norm = 0
    keys = ("gasCV", "gasBlock", "ehp", "hhp", "dh")
    with open(p2, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["buurtcode", "gasCV", "gasBlock", "ehp", "hhp", "dh", "hasDHgrid"])
        for r in recs:
            if r["year"] != "2022":
                continue
            v = {k: r[k] for k in keys}
            s = sum(v.values())
            if r["complete"] and s > 0:
                v = {k: x / s for k, x in v.items()}
                norm += 1
            w.writerow([r["buurtcode"]] + [v[k] for k in keys] + ["true" if v["dh"] > 0 else "false"])
            n += 1
    print(f"nbh_heating_2022.csv: {n} neighbourhoods ({norm} renormalised to sum 1; "
          f"the rest keep raw shares and fall back to gas)")


if __name__ == "__main__":
    main()
