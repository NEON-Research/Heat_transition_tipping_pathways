"""Export the engine's reference tables to CSV, sourced from the ORIGINAL spreadsheets in the
top-level data/ folder (the single source of truth) -- NOT the deprecated AnyLogic database dump.
All three are read by the engine (HeatingSystemData + Vesta).

  data/_heating_system_data.xlsx
      sheet 'heating_system_data'  -> heating_system_data.csv     (HeatingSystemData.freshSpecs)
      sheet 'energy_source_data'   -> energy_source_data.csv       (HeatingSystemData: EUR/kWh costs)
  data/_energy_demand_and_insulation_costs_VestaMAIS.xlsx
      sheet 'dwellings_demand_insulation' -> dwellings_demand_insulation.csv   (Vesta: insulation cost)

Output goes to model/data/reference/ where the engine reads it. Column headers are the snake_case of
the spreadsheet headers (requiredDistributionSystem -> required_distribution_system,
costs_eur_per_kWh -> costs_eur_per_k_wh).

Requires: openpyxl   (pip install openpyxl --break-system-packages)
Usage:    python export_reference_tables.py            # uses the paths below
"""
import csv, os, re, sys
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))                              # model/data-export/scripts
ROOT = os.path.normpath(os.path.join(HERE, "..", "..", ".."))                 # repo root
DATA = os.environ.get("HT_DATA_DIR", os.path.join(ROOT, "data"))              # top-level data/ (source)
REF = os.path.normpath(os.path.join(HERE, "..", "..", "data", "reference"))   # model/data/reference (output)
os.makedirs(REF, exist_ok=True)

VALID_HEATING = {"NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP",
                 "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"}


def snake(s):
    """requiredEnergyLabel -> required_energy_label ; costs_eur_per_kWh -> costs_eur_per_k_wh."""
    return re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', str(s)).lower()


def conv(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) if isinstance(v, float) and v.is_integer() else v
    s = str(v).strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        f = float(s)
        return int(f) if f.is_integer() and "." not in s and "e" not in s.lower() else f
    except ValueError:
        return s


def export(xlsx, sheet, out_name, keep=None):
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    rows = list(wb[sheet].iter_rows(values_only=True))
    wb.close()
    header = [snake(h) if h is not None else None for h in rows[0]]
    cols = [h for h in header if h is not None]
    out_rows = []
    for row in rows[1:]:
        first = row[0]
        if first is None or str(first).strip() == "":        # drop blank rows
            continue
        if keep is not None and first not in keep:            # drop junk / unused types
            continue
        rec = {}
        for k, v in zip(header, row):
            if k is not None:
                c = conv(v)
                rec[k] = "" if c is None else c
        out_rows.append(rec)
    path = os.path.join(REF, out_name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(out_rows)
    print(f"  {out_name}: {len(out_rows)} rows  <- {os.path.basename(xlsx)}::{sheet}")


def main():
    hs = os.path.join(DATA, "_heating_system_data.xlsx")
    vesta = os.path.join(DATA, "_energy_demand_and_insulation_costs_VestaMAIS.xlsx")
    for f in (hs, vesta):
        if not os.path.exists(f):
            sys.exit(f"source spreadsheet not found: {f}\n(set HT_DATA_DIR or check the top-level data/ folder)")
    print(f"Exporting reference tables from {DATA} -> {REF}:")
    export(hs, "heating_system_data", "heating_system_data.csv", keep=VALID_HEATING)
    export(hs, "energy_source_data", "energy_source_data.csv")
    export(vesta, "dwellings_demand_insulation", "dwellings_demand_insulation.csv")


if __name__ == "__main__":
    main()
