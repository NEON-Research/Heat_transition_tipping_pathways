# Model Reference

A per-class and per-formula map of the current AnyLogic model, so the engine can be rebuilt
and traced with confidence. Formulas are transcribed in runnable form in
`tests/heat_model_ref.py`; this document is the human-readable index.

## Java classes (the model substance)

| Class | ~lines | Role |
|---|---|---|
| `J_HomeOwner` | 546 | Homeowner decision agent: triggers, TPB utility, RUM choice, peer network |
| `J_Dwelling` | 460 | Building: energy demand/insulation, heating system, ownership link |
| `J_HousingBlock` / `J_SocialHousingBlock` | 322 / 315 | Collective decision units (SHA/HOA blocks) |
| `J_Household` | 311 | Base household: installs system, records annual values |
| `J_HeatingSystemOptionsGlobal` | 300 | Per-technology economics: capex, EAC bounds, learning curve, salience |
| `J_ResultsPerHeatingMethod` | 181 | Aggregation per heating system |
| `J_AnnualValues` | 150 | Per-year counters (considered/installed/removed) |
| `J_ResultsPerOwnerType` | 147 | Aggregation per ownership type |
| `J_Neighborhood` | 91 | Neighbourhood characteristics |
| `J_HeatingSystemOption` | 89 | A single option's scored attributes (EAC, PBC, SN, utility) |
| `J_Municipality` | 70 | Municipality metadata |
| `J_AdoptionData` / `J_ResultsData` | 67 / 64 | Adoption records / results container |
| `J_HeatingSystem` | 47 | Type + age (age drives triggers) |
| `J_ScenarioConfig` | 45 | Scenario switches |
| `J_PostalCode6` / `J_Renter` | 20 / 19 | PC6 grouping / renter |

## Enums (ordered lists)

- `OL_HeatingSystem`: `NATURAL_GAS_BOILER`, `NATURAL_GAS_BLOCK`, `HYBRID_HEAT_PUMP`,
  `ELECTRIC_HEAT_PUMP`, `DISTRICT_HEATING`.
- `OL_HouseholdOwnership`: `PRIVATELY_OWNED`, `PRIVATELY_RENTED`, `SOCIAL_HOUSING`,
  `HOME_OWNER_ASSOCIATION` (+ `TOTAL` in outputs).
- `OL_EnergyCarrier`: primary/secondary energy sources per technology.

## Key functions on `Main` / `Startup_agent`

| Function | Role |
|---|---|
| `f_runSimulation` | annual loop 2024→2050 |
| `f_adoptionProces` | per-year: age update, DH expansion, SHA/HOA/landlord/homeowner decisions, insulation, aggregation, cost+salience update |
| `f_energyCalculations` | hourly household energy demand/supply |
| `f_initializeHouseholdsFromDatabase` / `f_initializeDwellingsFromDatabase` | build objects from DB |
| `f_setNetworkSmallWorld` / `f_setNetworkRandom` / `f_setNetworkSimilar` | build peer networks |
| `f_updateGlobalCostsHeatingMethods` | apply learning curve to capex |
| `f_updateSalienceFactorPerTechnology` | recompute salience from cumulative installs |
| `f_writeResultsToCSV` / `f_writeExampleDwellingsToCSV` | outputs |
| `f_varyWeights` | built-in weight sweep (a ready-made sensitivity design) |
| `f_getAndRunScenarios` / `f_loadScenarios` | scenario batch driver |

## Decision formulas (see `tests/heat_model_ref.py` for exact code)

```
attitude      = 1 - |householdAttitude - techSustainabilityScoreNorm|
effort        = 0.2 (same) | 0.8 (needs low-temp retrofit) | 0.5 (else)
EAC_norm      = (EAC - minEAC) / (maxEAC - minEAC)
PBC           = ((1-EAC_norm)·0.5 + (1-effort)·0.2) / (0.5+0.2)
subjectiveNorm= min(1, peerShare · (1 + salienceFactor))
intention     = (attitude·0.5 + subjectiveNorm·weightSN·techRate + PBC·0.5) / (0.5+weightSN+0.5)
                where weightSN = socialLearningFactor · 0.5
utility       = (intention·0.5 + PBC·0.5) / (0.5+0.5)
choice        = argmax( utility + 0.02 · (-ln(-ln(U))) )        # RUM / Gumbel
```

## Feedback loops

**Salience** (`setSalienceFactor`, constants k=10, threshold=0.3, steepness=30):
```
novelty  = 1 / (1 + e^{k·(share - threshold)})     # high when share low
decay    = share
momentum = 1 / (1 + e^{-steepness·(share - prevShare)})   # 1 rising, 0 falling
salience = momentum·novelty + (1-momentum)·decay
```

**Learning curve** (`f_updateCapexFromLearningCurve`):
```
doublings = log2(cumulativeInstalls / initialInstalls)
capex     = initialCapex · (1 - learningRate·policyMultiplier)^doublings   # only if growing
```

## Triggers

- **End-of-life:** `age ≥ lifetime`.
- **Opportunity:** intended as `age / lifetime > 0.75` (fires past 75% of life). The original
  AnyLogic code used integer operands ⇒ integer division ⇒ effectively `age ≥ lifetime` (a
  bug). **Fixed** in `engine/src/decision.js` (real division); a `legacyIntDivision` flag
  reproduces the old behaviour for golden validation. See APPROACH.md §5.

## Scenario switches (`_scenario_settings.csv`, 16 scenarios)

`social_learning_factor` (LOW/MED/HIGH), `economic_learning_factor` (LOW/MED/HIGH),
`grid_reinforcement_rate`, `DH_construction_time`, `DH_expansion_strategy`
(COST_BASED/POLICY_BASED), `SHA_strategy`, `DH_connection_obligation` (bool),
`Grid_congestion_HP_ban` (bool).

## Output schema (`simulation_results_*.csv`)

`scenario, scenario_name, iteration, year, nbh_in_grid_congestion_perc, nbh_with_dh_perc,
heating_system, ownership, installed_current, installed_annually, removed_annually,
installed_cumulative, considered_annually, avg_att, avg_util, avg_sub_norm, avg_eac,
avg_pbc, SLF, ELF, GRR, DHCT, DHES, SHAES, DHCO, GCHPB`

The engine must emit exactly this so the golden-master comparator works unchanged.

## Data tables (HSQLDB `database/db.script`)

`DWELLINGS_DEMAND_INSULATION`, `HEATING_SYSTEM_DATA`, `ENERGY_SOURCE_DATA`,
`NEIGHBORHOOD_DATA_SELECTION`, `NEIGHBORHOOD_SHAPE`, `NBH_HOUSEHOLD_HEATING_METHOD2023`,
`NBH_POLICY_PLAN_2023`, `MUNICIPALITIES` (+ AnyLogic internal `AL_*` tables to ignore).
Source spreadsheets: `_*.xlsx` in the model folder. Geodata: `geodata/*.shp`.
