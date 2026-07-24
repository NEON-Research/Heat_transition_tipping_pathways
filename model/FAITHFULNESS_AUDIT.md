# Faithfulness audit — where the port diverges from the AnyLogic model

Principle: the engine must be an **exact copy** of the AL model. If output only matches after
nudging a number, that's a bug, not a calibration. This audit lists concrete places where the
current port diverges from the AL *logic* (not its parameters). Every item below suppresses
homeowner heat-pump adoption — together they explain the "homeowners under-shoot" gap without
touching a single parameter value. Source citations are to the AL `.alp` Java.

## 1. Peer network is wrong (biggest suspect)
- **AL:** default `v_networkType = SMALL_WORLD_SIMILAR` (`f_setNetwork` → `f_setNetworkSimilar`).
  Homeowners are connected preferentially to peers with *similar sustainability attitude*
  (homophily), on a small-world topology.
- **Port:** a crude uniform-random k-regular graph.
- **Why it matters:** homophily clusters green homeowners together, so an early adopter's
  neighbours are also green → the subjective norm for heat pumps rises fast *within a cluster*
  → local tipping cascades. A random graph averages everyone out and damps the cascade. This
  is very likely the main reason the snowball doesn't take off.
- **Fix:** port `f_setNetworkSimilar` (+ `f_checkNetworkSimilarity`, `f_setNetworkAssumptions`)
  exactly, including the network size/rewiring parameters.

## 2. Exogenous insulation is missing
- **AL:** `f_setExogeneousInsulation` runs every year (`f_adoptionProces` step 9).
  `J_Dwelling.updateExogeneousInsulation`: if label worse than A and >15 yr since last
  renovation, 5%/yr chance to improve **2 label steps**; if the new label ≤ b the dwelling
  becomes **low-temperature ready** (`hasLowTemperatureInhouseHeatDistribution = true`).
- **Port:** omitted entirely (labels are frozen).
- **Why it matters:** over 27 years this steadily upgrades the stock, which (a) drops the
  heat-pump *effort* from 0.8 → 0.5 once a dwelling is low-temp ready, (b) zeroes the
  insulation cost in the heat-pump EAC once the label already meets b/c, and (c) lowers heat
  demand. All three make heat pumps progressively more attractive to homeowners. Without it,
  every house stays permanently at the high-effort / high-cost barrier.
- **Fix:** port `updateExogeneousInsulation`. Requires the engine to (re)compute heat demand
  and insulation cost from the archetype table when a label changes — see the architecture
  note below.

## 3. Initial heating mix is all-gas; AL seeds from neighbourhood data
- **AL:** `f_setHouseholdDefaultHeatingSystem` assigns each dwelling's *initial* system from
  the neighbourhood percentages `p_gasCV_perc`, `p_hybridHeatPump_perc`,
  `p_electricHeatPump_perc`, `p_districtHeating_perc`, `p_gasBlockHeating_perc` (from
  `NBH_HOUSEHOLD_HEATING_METHOD2023`). The 2024 stock is **not** all gas.
- **Port:** every dwelling starts on `NATURAL_GAS_BOILER`.
- **Why it matters:** the existing heat-pump/DH dwellings are the initial seed — they give
  non-zero peer counts (so the subjective norm for heat pumps isn't ~0 at t=0) and non-zero
  cumulative installs (so the learning curve/salience start off the floor). Starting all-gas
  removes the seed the cascade grows from.
- **Fix:** export `NBH_HOUSEHOLD_HEATING_METHOD2023` and assign initial systems per
  neighbourhood exactly as `f_setHouseholdDefaultHeatingSystem` does.

## 4. Home-owner-association (HOA) blocks are missing
- **AL:** a 4th owner type `HOME_OWNER_ASSOCIATION` (`c_homeOwnerAssociationBlocks`).
  Owner-occupied apartments are grouped into HOA blocks that decide **collectively on cost**
  (`f_adoptHeatingMethodHOA` → `getStochasticChoice` on EAC), like social housing.
- **Port:** these apartments are treated as individual TPB homeowners (so they rarely switch).
- **Why it matters:** as a cost-based collective, HOA apartments switch to heat pumps like the
  social-housing blocks do; leaving them as individual homeowners both understates adoption and
  changes the ownership split vs the golden (which has a HOME_OWNER_ASSOCIATION column).
- **Fix:** port the HOA grouping (`f_setDefaultDataHomeOwnerAssociationBlocks`,
  `f_addHouseholdToHOABlock`) and block decision.

## Architecture note (enables #2 and #3)
The export currently *precomputes* per-dwelling heat demand and insulation cost at the initial
label. To model exogenous insulation faithfully, the engine must recompute these from the
archetype table (`dwellings_demand_insulation.json`, only 66 rows) using the dwelling's
*current* `(dwellingType, constructionYear, label, area)` — exactly `f_getDBValue` +
`f_getEAC`/`f_setAnnualEnergyDemandFromVestaMAIS`. This is both more faithful and cleaner than
shipping frozen numbers. Ship the archetype JSON with the engine and compute on the fly.

## Not divergences (checked, ruled out)
- **Energy-price trend:** `ENERGY_SOURCE_DATA.COST_TREND` exists but is not applied in the
  decision code — prices are flat, so the port is already correct here.
- **Initial system age:** AL uses `random × lifetime` (or `startYear − yearLastRenovation`);
  the port's `random(0..lifetime)` matches the former.
- **Core decision maths** (attitude, PBC, subjective norm, intention, utility, salience,
  learning curve, EAC/TCO): verified identical (the spec tests pin these).

## Suggested fix order (highest expected impact first)
1. SMALL_WORLD_SIMILAR network (#1)
2. Exogenous insulation + on-the-fly demand/insulation from archetype (#2 + architecture)
3. Initial heating mix from neighbourhood data (#3)
4. HOA blocks (#4)

After each fix, re-run and compare to the **corrected-trigger** AnyLogic golden — expecting
the homeowner curve to move toward the AL result with no parameter changes.

---

# Implementation status

New corrected-trigger golden is in place: `simulation_results_20260717_101427.csv`
(baseline homeowners reach EHP 54% / HHP 42% / gas 3% by 2050). Golden metrics rebuilt from it.

**ALL FOUR now implemented (JS engine + Java, shared browser-safe builder for the dashboard).**
Effect on homeowners, cumulatively (baseline, 1-in-20 sample): 10% → 20% (network) → 41%
(+ exogenous insulation + insulation-cost bug fix) → **65% heat pumps** (+ initial mix + HOA).
Golden is ~96% (EHP 54 / HHP 42). Remaining gap: ~35% of homeowners still stay on gas vs the
golden's 3% — the snowball doesn't fully complete for low-attitude owners. Candidate residual
differences to chase next: exact network edge count/rewiring, the EHP-vs-HHP split (my owners
skew EHP, golden skews HHP — likely the hybrid's lower retrofit/insulation barrier), and
initial system-age distribution. No parameter was tuned; all values come from the AL data.

### #1 SMALL_WORLD_SIMILAR network — DONE (JS + Java)
Ported faithfully into `modelFull.js` and `Simulation.java`. Constants from
`f_setNetworkAssumptions`: `networkSizeMean=25`, `shareLocalContacts=0.8`,
`shareSimilarContacts=0.8`; 6 attitude categories on mean ± 2sd/1sd bands; per-municipality
(buurtcode minus last 4 chars) local bias; per-homeowner size `round(N(0.5,0.05)∈[0,1]·25)`,
bidirectional edges. Measured effect on the sample: homeowner electric-HP roughly **doubled
(10% → 20%)** — confirms it's a real lever, but not sufficient alone (expected).

### #2 Exogenous insulation — TODO (needs on-the-fly demand/insulation)
Exact logic (`J_Dwelling.updateExogeneousInsulation`, run every year for non-block
households + each block):
```
if labelNum(energyLabel) > 1 and (year - yearLastRenovation) > 15:
    if rand() < 0.05:
        newNum = max(1, labelNum - 2)
        energyLabel = numToLetter(newNum)      # 1=a..7=g
        yearLastRenovation = year
        if newNum <= 2: hasLowTemp = true
```
`yearLastRenovation` initial = constructionYear. To flow through to demand/effort/EAC, the
engine must recompute space+dhw and insulation cost from `dwellings_demand_insulation.json`
using the *current* label (ship the 66-row archetype JSON; port `f_getDBValue` +
`f_setAnnualEnergyDemandFromVestaMAIS` + `f_getInsulationCosts`).

### #3 Initial heating mix — TODO (needs NBH export)
Per neighbourhood, from `NBH_HOUSEHOLD_HEATING_METHOD2023` (keyed by buurtcode):
`p_gasCV_perc = individuele_cv/100`, `p_gasBlockHeating_perc = blok_verwarming/100`,
DH % = sum of `stadsverwarming_*`, electric % = sum of `elektrisch_verwarmd_*`. Then
`f_setHouseholdDefaultHeatingSystem` assigns required counts per neighbourhood (social block
first, then NGBlock to apartments→terraced→random forming HOA blocks, then DH/EHP/HHP, gas
as fallback). Export the table per buurtcode and port the assignment verbatim.

### #4 HOA blocks — TODO (built during #3)
`f_addHouseholdToHOABlock`: NGBlock apartments/terraced among owners/renters are pulled out
of the individual lists into `blockHeatingHOA` (one per neighbourhood), ownership set to
HOME_OWNER_ASSOCIATION; the block decides collectively on cost (`f_adoptHeatingMethodHOA` →
`getStochasticChoice` on EAC), like social housing. Emit a HOME_OWNER_ASSOCIATION ownership
column in output to match the golden.

**Recommendation:** hold the Java rerun until #2–4 are also in — checking now would only show
partial progress (~20% homeowner HP vs the golden ~54%). #2 (exogenous insulation) is the next
biggest lever.
