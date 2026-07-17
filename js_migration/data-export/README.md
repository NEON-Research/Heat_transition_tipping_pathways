# Real-data export (HSQLDB + households.db -> engine-ready)

Turns the model's native data into portable files the Java/JS engines load, replacing the
synthetic stock. All faithful to the AnyLogic data-loading code (cited below).

## Scripts (`scripts/`)
- `export_reference_tables.py` — parses `database/db.script` (HSQLDB text dump) and writes
  `out/dwellings_demand_insulation.json` (66 archetypes: energy demand + insulation costs),
  `out/heating_system_data.json`, `out/energy_source_data.json`.
- `export_limburg_stock.py` — reads `households.db` (SQLite, 9.7M NL dwellings), filters
  **Limburg** (the golden-run region: 620,904 dwellings), and computes per-dwelling heat
  demand with the model's VestaMAIS method, writing `out/limburg_dwellings.csv` (~41 MB).

Run:
```bash
cd scripts
python3 export_reference_tables.py     # needs the db.script path (default relative)
python3 export_limburg_stock.py        # ~35s
```

## Faithful mappings encoded (with source)
- `woning_type` -> archetype: `J_Dwelling.f_setDwellingType` (Appartement/Maisonnette/
  Portiekwoning->APARTMENT; Flatwoning/Galerij->HIGHRISE; Rijwoning hoek->CORNER; Rijwoning
  tussen->TERRACED; Twee-onder-*->SEMIDETACHED; Vrijstaand->DETACHED; blank/unknown->TERRACED).
- `energieklasse` -> label a..g (A*->a, blank -> archetype `default_label`); constructor.
- heat demand: `space = (vrv_{label}_asl + vrv_{label}_opp*area)/3.6*1000`,
  `dhwBase = vww_asl/3.6*1000` (GJ->kWh), archetype row by `type_ol` + construction-year band
  (`f_getDBValue`). Runtime factor `f ~ TruncNormal(1, 0.2) in [0.5,1.5]` applied in the engine:
  `heatDemand = f*space + f*f*dhwBase` (`f_setAnnualEnergyDemandFromVestaMAIS`).
- ownership: `f_getDwellingOwnership` (odd-looking but validated below).

## Validation
- **Ownership split matches the golden almost exactly.** The faithful formula on the real
  Limburg data yields PRIVATELY_OWNED 67.0%, SOCIAL 18.7%, RENTED 14.3% vs the golden run's
  65.6% / 19.4% / 15.0%. Strong evidence the export + mappings are right.
- **Heat demand is realistic**: median ~10,100 kWh, mean ~13,000 kWh (typical Dutch dwelling).

## Loading into the engines
- JS:   `node run.js --real ../data-export/out/limburg_dwellings.csv --scenario baseline`
- Java: `gradle run --args="--real ../data-export/out/limburg_dwellings.csv --scenario baseline"`
Both keep PRIVATELY_OWNED dwellings (homeowner core), assign ownership + sample the demand
factor with the engine's seeded RNG.

## Key finding — why homeowner-only doesn't yet match golden adoption
On the real stock the homeowner core reaches ~11% heat pumps by 2050 vs the golden ~96%.
The gap is structural, not a data bug: the model's tipping feedback (the learning curve that
lowers heat-pump capex, and the salience that amplifies the social norm) is driven by
**cumulative installs across ALL owner types**. With only homeowners installing, the shared
learning curve barely moves, heat-pump costs stay high, and the transition never ignites.

**Next step for parity:** add the other decision agents (social-housing blocks, landlords)
so their installs feed the shared learning-curve + salience state, and set the initial
heating mix/age from `NBH_HOUSEHOLD_HEATING_METHOD2023` rather than all-gas. Also filter to
residential (`gebruiksdoelen='woonfunctie'`) + valid neighbourhood to match the golden count.
