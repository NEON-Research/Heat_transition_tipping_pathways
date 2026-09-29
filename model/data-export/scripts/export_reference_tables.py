"""Export the engine's reference tables to CSV, sourced from the ORIGINAL spreadsheets in the
top-level data/ folder (the single source of truth) -- NOT the deprecated AnyLogic database dump.
All three are read by the engine (HeatingSystemData + Vesta).

  data/_heating_system_data.xlsx
      sheet 'heating_system_data'  -> heating_system_data.csv     (HeatingSystemData.freshSpecs)
      sheet 'energy_source_data'   -> energy_source_data.csv       (HeatingSystemData: EUR/kWh costs)
  data/_energy_demand_and_insulation_costs_VestaMAIS.xlsx
      sheet 'dwellings_demand_insulation' -> dwellings_demand_insulation.csv   (Vesta: insulation cost)
  data/scenario_settings.csv       -> scenario_settings.csv        (Scenario, validated copy)
  data/district_heating_costs.csv  -> district_heating_costs.csv   (DistrictHeatingCosts, ACM tariffs)
  data/subsidies.csv               -> subsidies.csv                (Subsidies, ISDE etc. per year)

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


# Network-connection flags per heating system. These drive the fixed annual connection charge in
# Economics.computeEAC (a dwelling that leaves gas stops paying the gas standing charge; district
# heating brings its own). Added here rather than in the spreadsheet so a re-export cannot silently
# drop them; if the sheet grows its own gas_connection / heat_connection columns those win.
CONNECTIONS = {
    "NATURAL_GAS_BOILER": (1, 0),
    "NATURAL_GAS_BLOCK":  (1, 0),
    "HYBRID_HEAT_PUMP":   (1, 0),
    "ELECTRIC_HEAT_PUMP": (0, 0),
    "DISTRICT_HEATING":   (0, 1),
}


def add_connection_flags(cols, out_rows):
    """Append gas_connection / heat_connection unless the sheet already supplies them."""
    for name, idx in (("gas_connection", 0), ("heat_connection", 1)):
        if name in cols:
            continue
        cols.append(name)
        for rec in out_rows:
            t = rec.get("type")
            if t in CONNECTIONS:
                rec[name] = CONNECTIONS[t][idx]
            else:   # unknown type: fall back to the energy sources it uses
                src = {rec.get("energy_source_primary"), rec.get("energy_source_secondary")}
                rec[name] = int("NATURAL_GAS" in src) if idx == 0 else int("HEAT" in src)


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
    if out_name == "heating_system_data.csv":
        add_connection_flags(cols, out_rows)
    path = os.path.join(REF, out_name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(out_rows)
    print(f"  {out_name}: {len(out_rows)} rows  <- {os.path.basename(xlsx)}::{sheet}")


LEVEL = {"LOW", "MEDIUM", "HIGH"}
STRATEGY = {"COST_BASED", "POLICY_BASED"}
BOOL = {"TRUE", "FALSE", "0", "1"}
# energy_prices is a spark-spread arm, not a level -- its own vocabulary so it cannot be
# confused with the LOW/MEDIUM/HIGH learning factors.
PRICE_ARM = {"GAS_LOW_ELEC_HIGH", "CENTRAL", "GAS_HIGH_ELEC_LOW"}
SCEN_COLS = ["scen_id", "scen_name", "social_learning_factor", "economic_learning_factor",
             "grid_reinforcement_rate", "energy_prices", "DH_construction_time", "DH_expansion_strategy",
             "SHA_strategy", "DH_connection_obligation", "Grid_congestion_HP_ban", "subsidy_policy", "status"]
SUBSIDY_POLICY = {"CURRENT", "NONE", "PHASE_OUT"}


def export_scenarios():
    """Validate data/scenario_settings.csv and copy it to the reference dir the engine reads.

    The engine loads its scenario matrix from data/reference/scenario_settings.csv and parses it
    strictly -- an unknown enum value or a bad status aborts the run. The same checks run here so a
    mistake is caught at export time, in front of whoever made it, rather than an hour into a batch.
    """
    src = os.path.join(DATA, "scenario_settings.csv")
    if not os.path.exists(src):
        sys.exit(f"scenario matrix not found: {src}")
    with open(src, encoding="utf-8-sig") as fh:
        raw = [ln for ln in fh if not ln.lstrip().startswith("#")]
    rows = list(csv.DictReader(raw))
    if not rows:
        sys.exit(f"{src}: no rows")
    for c in SCEN_COLS:
        if c not in rows[0]:
            sys.exit(f"{src}: missing column '{c}'")
    seen, active = set(), 0
    for i, r in enumerate(rows, start=2):
        name = (r["scen_name"] or "").strip()
        def bad(msg): sys.exit(f"{src} line {i} ({name or '?'}): {msg}")
        if not name: bad("empty scen_name")
        if name in seen: bad("duplicate scen_name")
        seen.add(name)
        if not str(r["scen_id"]).strip().isdigit(): bad(f"scen_id must be an integer, got {r['scen_id']!r}")
        if not str(r["DH_construction_time"]).strip().isdigit():
            bad(f"DH_construction_time must be an integer, got {r['DH_construction_time']!r}")
        for c in ("social_learning_factor", "economic_learning_factor", "grid_reinforcement_rate"):
            if r[c].strip().upper() not in LEVEL: bad(f"{c} must be LOW/MEDIUM/HIGH, got {r[c]!r}")
        if r["energy_prices"].strip().upper() not in PRICE_ARM:
            bad(f"energy_prices must be one of {sorted(PRICE_ARM)} (a spark-spread arm, not a price "
                f"level), got {r['energy_prices']!r}")
        for c in ("DH_expansion_strategy", "SHA_strategy"):
            if r[c].strip().upper() not in STRATEGY: bad(f"{c} must be COST_BASED/POLICY_BASED, got {r[c]!r}")
        for c in ("DH_connection_obligation", "Grid_congestion_HP_ban"):
            if r[c].strip().upper() not in BOOL: bad(f"{c} must be TRUE/FALSE, got {r[c]!r}")
        if r["subsidy_policy"].strip().upper() not in SUBSIDY_POLICY:
            bad(f"subsidy_policy must be one of {sorted(SUBSIDY_POLICY)}, got {r['subsidy_policy']!r}")
        st = r["status"].strip().lower()
        if st not in ("active", "retired"): bad(f"status must be active/retired, got {r['status']!r}")
        if st == "active": active += 1
    if not active:
        sys.exit(f"{src}: no active scenarios -- '--scenario all' would run nothing")
    dst = os.path.join(REF, "scenario_settings.csv")
    with open(src, encoding="utf-8-sig") as a, open(dst, "w", encoding="utf-8", newline="") as b:
        b.write(a.read())
    print(f"  scenario_settings.csv: {len(rows)} rows ({active} active, {len(rows)-active} retired)"
          f"  <- data/scenario_settings.csv")


DH_COMPONENTS = {"connection_fee": "one-off", "disconnection_fee": "one-off",
                 "fixed_charge": "annual", "metering": "annual", "delivery_set_rental": "annual"}


def export_district_heating_costs():
    """Validate data/district_heating_costs.csv (ACM tariffs, source in its '#' header) and copy it,
    comments included, to the reference dir. Every component must be present exactly once with a
    non-negative number and the expected annual/one-off type."""
    src = os.path.join(DATA, "district_heating_costs.csv")
    if not os.path.exists(src):
        sys.exit(f"district heating costs not found: {src}")
    with open(src, encoding="utf-8-sig") as fh:
        text = fh.read()
    rows = list(csv.DictReader(ln for ln in text.splitlines() if not ln.lstrip().startswith("#")))
    seen = {}
    for r in rows:
        c = (r.get("component") or "").strip()
        if c not in DH_COMPONENTS: sys.exit(f"{src}: unknown component {c!r}")
        if c in seen: sys.exit(f"{src}: duplicate component {c!r}")
        try:
            v = float(r["value"])
        except (TypeError, ValueError):
            sys.exit(f"{src}: {c} value must be a number, got {r.get('value')!r}")
        if v < 0: sys.exit(f"{src}: {c} must be >= 0")
        if (r.get("applies") or "").strip() != DH_COMPONENTS[c]:
            sys.exit(f"{src}: {c} must be '{DH_COMPONENTS[c]}', got {r.get('applies')!r}")
        seen[c] = v
    missing = set(DH_COMPONENTS) - set(seen)
    if missing: sys.exit(f"{src}: missing components {sorted(missing)}")
    with open(os.path.join(REF, "district_heating_costs.csv"), "w", encoding="utf-8", newline="") as b:
        b.write(text)
    print(f"  district_heating_costs.csv: {len(seen)} components  <- data/district_heating_costs.csv")


SUBSIDY_TECH = {"NATURAL_GAS_BOILER", "NATURAL_GAS_BLOCK", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP",
                "DISTRICT_HEATING"}
SUBSIDY_OWN = {"PRIVATELY_OWNED", "PRIVATELY_RENTED", "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION"}


def export_subsidies():
    """Validate data/subsidies.csv (sources in its '#' header) and copy it, comments included, to
    the reference dir: known technology and ownership, integer year, non-negative amount, no
    duplicate (year, technology, ownership)."""
    src = os.path.join(DATA, "subsidies.csv")
    if not os.path.exists(src):
        sys.exit(f"subsidy table not found: {src}")
    with open(src, encoding="utf-8-sig") as fh:
        text = fh.read()
    rows = list(csv.DictReader(ln for ln in text.splitlines() if not ln.lstrip().startswith("#")))
    seen = set()
    for i, r in enumerate(rows, start=1):
        key = (r.get("year"), r.get("technology"), r.get("ownership"))
        def bad(msg): sys.exit(f"{src} data row {i}: {msg}")
        if r.get("technology") not in SUBSIDY_TECH: bad(f"unknown technology {r.get('technology')!r}")
        if r.get("ownership") not in SUBSIDY_OWN: bad(f"unknown ownership {r.get('ownership')!r}")
        if not str(r.get("year", "")).strip().isdigit(): bad(f"year must be an integer, got {r.get('year')!r}")
        try:
            if float(r["amount_eur"]) < 0: bad("negative amount")
        except (TypeError, ValueError):
            bad(f"amount_eur must be a number, got {r.get('amount_eur')!r}")
        if key in seen: bad(f"duplicate {key}")
        seen.add(key)
    with open(os.path.join(REF, "subsidies.csv"), "w", encoding="utf-8", newline="") as b:
        b.write(text)
    print(f"  subsidies.csv: {len(rows)} rows  <- data/subsidies.csv")


HP_CAPEX_COLS = ["fixed_eur", "eur_per_kw", "full_load_hours", "capacity_share", "kw_min", "kw_max"]


def export_heating_system_capex():
    """Validate data/heating_system_capex.csv (sources in its '#' header) and copy it, comments
    included, to the reference dir: one row each for the two heat pumps, the gas boiler and the gas
    block, numbers (eur_per_kw may be 0, the others positive), kw_min <= kw_max."""
    src = os.path.join(DATA, "heating_system_capex.csv")
    if not os.path.exists(src):
        sys.exit(f"heating-system capex table not found: {src}")
    with open(src, encoding="utf-8-sig") as fh:
        text = fh.read()
    rows = list(csv.DictReader(ln for ln in text.splitlines() if not ln.lstrip().startswith("#")))
    techs = [r.get("technology") for r in rows]
    need = ["ELECTRIC_HEAT_PUMP", "HYBRID_HEAT_PUMP", "NATURAL_GAS_BLOCK", "NATURAL_GAS_BOILER"]
    if sorted(techs) != need:
        sys.exit(f"{src}: need exactly one row each for {need}, got {techs}")
    for r in rows:
        try:
            v = {c: float(r[c]) for c in HP_CAPEX_COLS}
        except (KeyError, TypeError, ValueError):
            sys.exit(f"{src}: {r.get('technology')}: all of {HP_CAPEX_COLS} must be numbers")
        if v["eur_per_kw"] < 0 or min(x for c, x in v.items() if c != "eur_per_kw") <= 0 or v["kw_min"] > v["kw_max"]:
            sys.exit(f"{src}: {r.get('technology')}: values must be positive (eur_per_kw >= 0) and kw_min <= kw_max")
    with open(os.path.join(REF, "heating_system_capex.csv"), "w", encoding="utf-8", newline="") as b:
        b.write(text)
    print(f"  heating_system_capex.csv: {len(rows)} rows  <- data/heating_system_capex.csv")


def main():
    hs = os.path.join(DATA, "heating_system_data.xlsx")
    vesta = os.path.join(DATA, "energy_demand_and_insulation_costs_VestaMAIS.xlsx")
    for f in (hs, vesta):
        if not os.path.exists(f):
            sys.exit(f"source spreadsheet not found: {f}\n(set HT_DATA_DIR or check the top-level data/ folder)")
    print(f"Exporting reference tables from {DATA} -> {REF}:")
    export(hs, "heating_system_data", "heating_system_data.csv", keep=VALID_HEATING)
    export(hs, "energy_source_data", "energy_source_data.csv")
    export(vesta, "dwellings_demand_insulation", "dwellings_demand_insulation.csv")
    export_scenarios()
    export_district_heating_costs()
    export_subsidies()
    export_heating_system_capex()


if __name__ == "__main__":
    main()
