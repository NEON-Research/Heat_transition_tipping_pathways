"""Export per-buurtcode heating-method percentages to nbh_heating.csv, sourced from the top-level
data/ spreadsheet (_neighborhoods_householdHeatingMethods_2023.xlsx, sheet
'nbh_householdHeatingMethod2023') -- the single source of truth, NOT the deprecated AL dump.

Mapped to the model's 5 systems exactly as AL f_setHeatingMethodNeighborhoodData:
  gasCV    = Individuele CV / 100
  gasBlock = Blok-verwarming / 100
  DH       = (Stadsverwarming hoog + laag + zonder) / 100 ;  hasDHgrid = DH > 0
  HHP      = Elektrisch met hoog gas / 100
  EHP      = (Elektrisch zonder + Elektrisch met laag gas) / 100
Missing (-99999 / negative) -> 0.

Output: model/data/reference/nbh_heating.csv  { buurtcode: {gasCV,gasBlock,ehp,hhp,dh,hasDHgrid} }
Requires: openpyxl
"""
import csv, os
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", "..", ".."))                        # repo root
SRC = os.path.join(os.environ.get("HT_DATA_DIR", os.path.join(ROOT, "data")),
                   "_neighborhoods_householdHeatingMethods_2023.xlsx")
OUT = os.path.normpath(os.path.join(HERE, "..", "..", "data", "reference"))           # model/data/reference
os.makedirs(OUT, exist_ok=True)

# column indices in sheet 'nbh_householdHeatingMethod2023' (0-based): the first block of percentages
COL = dict(buurt=0, icv=5, blok=6, sv_h=7, sv_l=8, sv_z=9, el_h=10, el_l=11, el_z=12)


def z(v):  # -99999 / negatives / missing -> 0
    return float(v) if isinstance(v, (int, float)) and v >= 0 else 0.0


def main():
    if not os.path.exists(SRC):
        raise SystemExit(f"source not found: {SRC} (set HT_DATA_DIR or check top-level data/)")
    wb = openpyxl.load_workbook(SRC, read_only=True, data_only=True)
    ws = wb["nbh_householdHeatingMethod2023"]
    out = {}
    for r in ws.iter_rows(min_row=2, values_only=True):        # row 1 is the header
        buurt = r[COL["buurt"]]
        if buurt is None or not str(buurt).strip():
            continue
        icv, blok = z(r[COL["icv"]]), z(r[COL["blok"]])
        sv = z(r[COL["sv_h"]]) + z(r[COL["sv_l"]]) + z(r[COL["sv_z"]])
        el_h, el_l, el_z = z(r[COL["el_h"]]), z(r[COL["el_l"]]), z(r[COL["el_z"]])
        dh = sv / 100
        out[str(buurt)] = {"gasCV": icv / 100, "gasBlock": blok / 100,
                           "ehp": (el_z + el_l) / 100, "hhp": el_h / 100,
                           "dh": dh, "hasDHgrid": dh > 0}
    wb.close()
    path = os.path.join(OUT, "nbh_heating.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["buurtcode", "gasCV", "gasBlock", "ehp", "hhp", "dh", "hasDHgrid"])
        for buurt, v in out.items():
            w.writerow([buurt, v["gasCV"], v["gasBlock"], v["ehp"], v["hhp"], v["dh"],
                        "true" if v["hasDHgrid"] else "false"])
    print(f"exported {len(out)} neighbourhoods  <- {os.path.basename(SRC)}  -> {path}")


if __name__ == "__main__":
    main()
