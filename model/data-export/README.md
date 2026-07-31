# Data export (top-level `data/` → engine-ready reference + stock files)

Turns the project's source data into the portable CSVs the Java engine loads. **Everything is
sourced from the spreadsheets/CSVs in the repo-root `data/` folder** — the deprecated AnyLogic
HSQLDB dump (`db.script`) is no longer used.

```
data/*.xlsx | *.csv | households.db   →   model/data/reference/*.csv        (small lookups)
                                          model/data/stock/*_dwellings.csv  (per-scope stock)
```

## Scripts (`scripts/`)

| script | reads (from `data/`) | writes (to `model/data/`) |
|---|---|---|
| `export_reference_tables.py` | `heating_system_data.xlsx`, `energy_demand_and_insulation_costs_VestaMAIS.xlsx` | `reference/heating_system_data.csv`, `reference/energy_source_data.csv`, `reference/dwellings_demand_insulation.csv` |
| `build_neighborhoods.py` | `neighborhoods_data_2023.csv`, `nbh_policy_plan_2023.csv`, `EVsPerPC.csv`, `Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx`, `neighborhood_shapes_2023.xlsx` | `reference/neighborhoods.csv` — **combined**: attributes + policy plan + EV share + heating shares per year + completeness flags + labels |
| `export_neighborhoods.py` | `neighborhoods_data_2023.csv`, `nbh_policy_plan_2023.csv`, `EVsPerPC.csv` | `reference/neighborhoods.csv` (base only; normally invoked *by* `build_neighborhoods.py`) |
| `export_observed_heating.py` | `Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx` | `reference/observed_heating_by_year.csv`, `reference/nbh_heating_2022.csv` (calibration — see `results_analysis/CALIBRATION_AND_VALIDATION.md`) |
| `export_stock.py` | `households.db` (SQLite, 9.7 M NL dwellings) | `stock/<scope>_dwellings.csv`; `--scope nl \| province:X \| gemeente:Y` |

Requires `openpyxl` for the spreadsheet readers (`pip install openpyxl`).

## Typical run

```bash
cd scripts
python export_reference_tables.py     # heating systems, energy sources, VestaMAIS archetypes
python build_neighborhoods.py         # combined neighbourhood table (+ prints household coverage)
python export_observed_heating.py     # observed 2022-24 series, for calibration
python export_stock.py --scope province:Noord-Brabant     # ~35 s for a province
```

`model/run.py` provisions the stock automatically for the scope you ask for, so
`export_stock.py` rarely needs running by hand.

## Notes

- **`out/` is obsolete** — nothing writes there any more; outputs go to `model/data/reference` and
  `model/data/stock`. The folder (and its stub `.gitignore`) can be deleted; the ignore rules live in
  the repo-root `.gitignore`.
- **Initial heating shares come from `neighborhoods.csv`** (columns `gasCV_<yr> … dh_<yr>`). The
  engine picks the vintage with `-Dht.heatingYear=2022|2023|2024` (default 2023); the calibration
  uses 2022. The old separate `nbh_heating.csv` and its exporter are **retired** — the old
  file can be deleted.
- **Missing data:** a neighbourhood with no usable shares (CBS privacy suppression) is left out of the
  map and falls back to the default **100 % gas boiler**, matching AnyLogic. Shares are *not* zeroed,
  which would leave a neighbourhood with no heating system at all. Numeric attributes read from
  `neighborhoods.csv` (`a_pau`, `g_ele`, `a_lan_ha`, …) *do* parse to **0** when blank; `a_lan_ha == 0`
  is guarded in the DH-density calculation.
- Source-file names must match exactly. They lost their leading `_` at some point, and the scripts now
  expect the un-prefixed names (e.g. `neighborhoods_data_2023.csv`).

## Faithful mappings encoded (with source)

- `woning_type` → archetype: `J_Dwelling.f_setDwellingType` (Appartement/Maisonnette/Portiekwoning →
  APARTMENT; Flatwoning/Galerij → HIGHRISE; Rijwoning hoek → CORNER; Rijwoning tussen → TERRACED;
  Twee-onder-* → SEMIDETACHED; Vrijstaand → DETACHED; blank/unknown → TERRACED).
- `energieklasse` → label a..g (A* → a, blank → archetype `default_label`).
- Heat demand: `space = (vrv_{label}_asl + vrv_{label}_opp × area)/3.6×1000`,
  `dhwBase = vww_asl/3.6×1000` (GJ→kWh), archetype row by `type_ol` + construction-year band
  (`f_getDBValue`). Runtime factor `f ~ TruncNormal(1, 0.2)` in [0.5, 1.5] is applied in the engine:
  `heatDemand = f·space + f²·dhwBase` (`f_setAnnualEnergyDemandFromVestaMAIS`).
- Living area clamped to [20, 500] m², matching AnyLogic.
- Ownership: `f_getDwellingOwnership`.
- Heating-method shares → the model's 5 systems, per `f_setHeatingMethodNeighborhoodData`.

## Validation

- **Ownership split matches the golden almost exactly**: the faithful formula on the real Limburg data
  gives PRIVATELY_OWNED 67.0 % / SOCIAL 18.7 % / RENTED 14.3 %, vs the golden run's 65.6 / 19.4 / 15.0.
- **Heat demand is realistic**: median ~10,100 kWh, mean ~13,000 kWh per dwelling.
- **CBS coverage** (printed by `build_neighborhoods.py`): 9,317 of 15,350 buurten are usable in all of
  2022–2024 (60.7 %), but they cover **89.1 % of NL households** — suppressed buurten are small.
