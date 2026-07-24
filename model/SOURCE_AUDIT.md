# Source audit — engine vs. generated AnyLogic Java

Audited against `deprecated/Heat transition tipping pathways_BUILD/src.generated/` (55 files, ~21,800
lines). This supersedes everything previously inferred from the `.alp` XML, which hid agent
bodies I could not read reliably.

**Method note.** AnyLogic keeps dead overloads and commented-out blocks in the generated source.
Twice now a plain grep landed on an unused variant and nearly produced a wrong "fix". Every
finding below was confirmed at the **live call site**, not just at the definition.

---

## A. Verified matching — no action

| mechanism | source | verdict |
|---|---|---|
| EAC / TCO | `J_Household.f_getEAC` | exact: size tiers (<6000 / >10000 kWh), low-temp distribution add-on, insulation guard, energy cost with secondary carrier, discount loop, `roundToInt(TCO / annuityFactor)` |
| annuity factor | `J_Household` L290 | `(1 - (1+r)^-lifetime) / r` — identical |
| insulation charge | `J_Household.f_getEAC` | AL charges only when `requiredLabelNum < currentLabelNum`; engine's `insulationCost` returns 0 in that case — equivalent |
| effort | `J_HomeOwner.f_getEffort` | 0.2 same-type / 0.8 needs low-temp / 0.5 otherwise |
| PBC | `f_getPBC` | `((1-EACnorm)*wEAC + (1-effort)*wEffort) / (wEAC + wEffort)` |
| subjective norm | `f_getSubjectiveNorm(type, hsog)` | `min(1.0, sharePeers * (1 + salienceFactor))` |
| intention | `f_getIntention` | `(att*wAtt + SN*wSN*SLR + PBC*wPBC) / (wAtt + wSN + wPBC)` |
| **wSN scaling** | `J_HomeOwner` L203 | `weight_SN = p_socialLearningFactor * p_weight_socialNormToIntention` — the scenario factor scales the weight *and* the denominator. Engine matches. |
| perceived utility | `f_getPerceivedUtility` | `(intention*wIB + PBC*wPB) / (wIB + wPB)` |
| salience factor | `J_HeatingSystemOptionsGlobal.setSalienceFactor` | `momentum*novelty + (1-momentum)*decay`, novelty `1/(1+e^{k(share-threshold)})`, momentum `1/(1+e^{-steepness*delta})` |
| learning curve | `f_updateCapexFromLearningCurve` | base + formula confirmed by the learning-curve export; capex within 1–4% |
| dwelling eligibility | `J_Dwelling.f_getHeatingMethodPossibility` | obligation → DH → congestion-EHP → NGB/APARTMENT, in that early-returning order |

> `J_HousingBlock.f_getHeatingMethodPossibility` exists but is **dead on the live path** — block
> eligibility comes from the *dwelling's* copy via `hh.getHeatingSystemOptionsEAC()`. It lacks the
> NGB/APARTMENT rule. Don't "fix" the wrong one.

---

## B. Fixed this session

| # | divergence | resolution |
|---|---|---|
| 1 | Block `hasTrigger()` — I had added an opportunity trigger by generalising from `J_Household` | `J_HousingBlock.hasTrigger()` is **end-of-life only**. Reverted, with a comment warning against unifying the two. |
| 2 | Block exogenous insulation propagated only when the label changed | AL's propagation loop sits inside the >15-year condition but **outside** the `rand < 0.05` branch — every dwelling is overwritten with the block label *every* year. Ported faithfully. |
| 3 | HOA fill tier 1 checked `APARTMENT` only | AL uses `APARTMENT || HIGHRISE`. Limburg has more highrise (62k) than apartments (48k), so the fill was spilling into the terraced/random tiers. Fixed — closed the HOA gap from z = −3.0 to −0.8. |
| 4 | Block average label used `Math.round` | AL's `roundToInt(sum / size)` is int/int → floor. Behind `LEGACY_LABEL=1`; see the note below. |
| 5 | `runFull.js` ignored `--seed` | Hardcoded `DEFAULT_SEED + it - 1`. Fixed — mattered because Monte Carlo is the point of this engine. |

**Current parity** — engine vs AL baseline (AL n=20, engine n=2), z = (eng - AL) / AL sd:

| owner | verdict |
|---|---|
| TOTAL, all systems | z −2.0 … +2.2 — within noise |
| PRIVATELY_OWNED | z −1.1 … +0.6 — within noise |
| SOCIAL_HOUSING | z −1.2 … +1.5 — within noise |
| HOME_OWNER_ASSOCIATION | z −1.7 … +1.8 — within noise |
| DISTRICT_HEATING (all owners) | z −0.6 … −1.4 — within noise |
| **PRIVATELY_RENTED gas** | **z = −11.0** (30.6 vs 32.2 ± 0.15) — open |

AL's baseline sds (n=20): TOTAL gas 0.70, PO gas 0.17, **PR gas 0.15**, SH gas 2.82, HOA gas 5.39.
Block-based owners are 20-35x noisier than individual ones, so PR's huge z is a 1.6 pp gap
against a very stable mean, not a large error.

**Open: PRIVATELY_RENTED.** Ruled out so far — the trigger (AL `Landlord.f_adoptHeatingMethod`
is end-of-life only, engine matches) and the Gumbel/RUM choice (AL traceln reports 9% non-top-EAC
LL decisions; engine measures 9% on 146k EAC decisions). So the decision rule is right and the
difference is upstream, in the landlord population or its attributes.

---

## C. Missing mechanisms — not yet ported

### C1. DONE — District heating expansion (ported 2026-07-21)

`src/districtHeating.js` ports `DistrictHeatingCompany` (both COST_BASED and POLICY_BASED),
fed by `data-export/scripts/export_neighborhoods.py`. Result: baseline DH **0.4% -> 0.77%**
against AL's 0.80 +- 0.09 (z = -0.3).

It also closed the HOA electric-heat-pump anomaly as a side effect (4.5% -> 1.1% vs 1.0 +- 0.95).
Those were one bug, not two: with no grid expansion, district heating never entered the block
option set and the engine picked EHP in its place.

<details><summary>original description</summary>

`DistrictHeatingCompany.f_DHExpansionPlanCosts()` runs under COST_BASED, i.e. **always**. A
neighbourhood gains a grid when heat density ≥ 600 GJ/ha, scheduled at a random year up to 2045.
The engine treats `hasDHgrid` as static — this is the entire DH gap (AL 0.8% vs engine 0.4%
baseline; 39.5% vs 7.2% under connection obligation).

Full spec in `GRID_AND_DH_PORT_SPEC.md`. Two traps:
- the heat-demand sum covers homeowners + landlord renters + social block, **excluding HOA blocks**
- `v_districtHeatYearOfOperation` is a scheduling latch; the `else if` only fires on later years
</details>

### C2. Grid congestion — gates 3 scenarios

`GZ_Neighborhood.f_energyCalculations` + `DSO.f_reinforceNeighborhoods`. Velander baseload
(`0.23e-3·E + 0.016·√E`) + heat pumps × 0.9 simultaneity + EVs, against `p_gridCapacity_kW`.
DSO reinforces 100 neighbourhoods/year, FIFO, clearing congestion on entry.

### C3. Policy plans — gates 6 scenarios — DATA-DRIVEN (earlier claim corrected)

I previously wrote that policy plans were synthetic. **That was wrong.** They come from
`_nbh_policy_plan_2023.csv` (already in the repo, 14,516 rows), joined on `buurtcode`:

```java
int startYear = (policyPlanStartYear == 0) ? v_year : policyPlanStartYear;   // start_jaar
int endYear   = (policyPlanEndYear   == 0) ? 2050   : policyPlanEndYear;     // eind_jaar
x.p_policyPlanYear = (int) uniform_discr(startYear, endYear);

if      (installatie.contains("Wnet")) x.p_policyPlan = DISTRICT_HEATING;
else if (installatie.contains("eHP"))  x.p_policyPlan = ELECTRIC_HEAT_PUMP;
else if (installatie.contains("hHP"))  x.p_policyPlan = HYBRID_HEAT_PUMP;

if (infra.contains("W") && policyPlanStartYear == 0) v_nbhsWithoutDHStartYear++;
```

So the *technology* is from data; only the *year* is randomised, and only within the CSV's
window. Neighbourhoods with `start_jaar == 0` feed `v_nbhsWithoutDHStartYear`, which drives
`chanceOfDHBeingInstalled` in the POLICY_BASED expansion — so that counter must be built during
loading, not derived later.

### C4. EV load — fully traced

```
_EVsPerPC.csv (PC4, perc_HPEV_BEV) -> pc4BevMap
bevShare          = pc4BevMap.get(p_postalCode)   // see D3 -- always null in practice
electricCars2023  = roundToInt(bevShare * p_cars)
f_setEVsSteepnessFactor: k = -ln(1/share2023 - 1) / (2023 - 2035),  p_EVAdoptionFactor = k
f_getEVs()        = p_cars / (1 + exp(-k * (year - 2035)))          // logistic, inflection 2035
```

Missing-data sentinels (`-99999`) are cleaned by `f_returnZeroIfDoubleIsSmallerThanZero`.

### D4. `g_ele` and `g_gas` are suppressed for EVERY neighbourhood

Both columns are `-99999` in all 14,421 rows of `_neighborhoods_data_2023.csv` — CBS privacy
suppression at buurt level. Consequences:

- `p_averageElectricityConsumptionTotal` = 0 everywhere, so the **Velander household baseload
  contributes nothing**: `PmaxHouseholds = 0.23e-3*0 + 0.016*sqrt(0) = 0`. Grid load is heat
  pumps + EVs only. (Before the 2026-07-21 fix it was NaN, which disabled congestion entirely.)
- Meanwhile `f_setGridCapacity` DOES include a baseload (`p_PmaxPerHousehold * households *
  simultaneity`). So capacity is sized for baseload+HP+EV while load counts only HP+EV —
  congestion is systematically under-detected.
- `p_averageGasConsumptionTotal` is never read outside boilerplate; it is dead data.

Not a port issue (the engine reproduces 0 faithfully) but a modelling gap worth deciding on:
either source electricity consumption elsewhere, or drop the baseload from capacity too so the
two sides are consistent.

---

## D. Two things to check in the AnyLogic model itself

### D1. `f_setGridCapacity` ends with integer division

```java
int gridCap = roundToInt((PmaxHouseholds + PmaxHeatPumps + PmaxEVs) * (1 + safetyMargin) / cosPhi);
return gridCap / 100000;          // <-- int / int
```

The method returns `double`, but both operands are `int`, so this truncates. Any neighbourhood
under 100,000 kW returns **0**, which would make `v_maxGridLoad_kW > p_gridCapacity_kW` true
everywhere and congest the entire stock permanently.

Yet `nbh_in_grid_congestion_perc` reads 0.0 in every year of every scenario. Those two facts
cannot both be innocent. Either capacity is 0 and congestion is silently saturated, or something
upstream compensates. **If capacity really is 0, the three `grid_congestion_ban` scenarios are not
testing what they appear to test.** Same int-division family as the age/lifetime and block-label
issues.

### D3. `pc4BevMap` lookup can never succeed — EV data is silently unused

```java
public HashMap<Integer, Double> pc4BevMap;      // Integer keys
double p_postalCode;                             // ...but the lookup key is a double
Double bevShare = pc4BevMap.get(x.p_postalCode);
if (bevShare == null) bevShare = 0.084;          // fallback ALWAYS taken
```

`Map.get(Object)` autoboxes the `double` to a `Double`, and a `Double` never `.equals()` an
`Integer`. The lookup returns null for every neighbourhood, so **every** one gets the 0.084
Dutch-2023-average fallback and `_EVsPerPC.csv` has no effect on any result.

Fix is `pc4BevMap.get((int) x.p_postalCode)`. Note this *changes results* for the congestion
scenarios (EV load becomes spatially varied). Until it is fixed, the engine should hardcode
0.084 to stay faithful.

### D2. Block average label (item B4)

`int avgLabelNb = roundToInt(sumLabelNumber / households.size());` — int/int, so `roundToInt` is
a no-op and the result floors. Since labels run a=1…g=7, this systematically awards blocks a
*better* label than the true mean. Probably a bug; correcting it shifts HOA noticeably, so the
golden reference needs regenerating if you change it. Engine supports both via `LEGACY_LABEL`.

---

## E. Scenario coverage

All 16 wired in `runFull.js`, transcribed from the results columns (`SLF, ELF, GRR, DHCT, DHES,
SHAES, DHCO, GCHPB`) rather than guessed. Only 5 levers vary; `GRR` and `DHCT` are constant.

- **runnable now (7):** baseline, both SLF variants, both ELF variants, `individual_technologies`,
  `individual_tech_dh_connection_obligation` (DHCO implemented, but its DH result is wrong until C1 lands)
- **needs C1/C2/C3 (9):** everything with POLICY_BASED strategies or the congestion ban

Confirmed constants: `requiredHeatDemand = 600 GJ/ha`, `planAndConstructionTime = 5 y`,
`DSORenovationCapacity = 100 nbh/y`, `simultaneityHeatPump = 0.9`, `PmaxElectricHP = 5 kW`,
`PmaxHybridHP = 2.5 kW`, `PmaxEV = 3.7 kW`, `simultaneityEV = 0.2`, `PmaxPerHousehold = 2.5 kW`.

---

## F. Neighbourhood CSV — 26 active columns

From `neighborhood_data_selection` (commented-out assignments excluded), plus `buurtcode` to join:

```
a_hh, g_ele, g_ele_ap, g_ele_tw, g_ele_hw, g_ele_2w, g_ele_vw,
g_gas, g_gas_ap, g_tw, g_gas_hw, g_gas_2w, g_gas_vw,
p_stadsv, a_bedv, a_bed_a, a_bed_bf, a_bed_gi, a_bed_hj,
a_bed_kl, a_bed_mn, a_bed_oq, a_bed_ru, a_pau, a_lan_ha, pst_mvp
```

Load-bearing for C1/C2: `a_lan_ha` (surface area), `a_hh` (households), `g_ele` (electricity),
`a_pau` (cars). `p_gridCapacity_kW` is **computed**, not loaded — no column needed.

All source files are already in the repo — no new export needed:

| file | rows | feeds |
|---|---|---|
| `_neighborhoods_data_2023.csv` | 14,421 | the 26 columns above |
| `_nbh_policy_plan_2023.csv` | 14,515 | `p_policyPlan`, `p_policyPlanYear`, `v_nbhsWithoutDHStartYear` |
| `_EVsPerPC.csv` | 4,070 | `pc4BevMap` (inert — see D3) |

`a_pau` = cars, `a_lan_ha` = land area (ha), `a_hh` = households, `g_ele` = avg electricity.
Watch the `-99999` sentinels and the UTF-8 BOM on the header row of all three.

---

## G. Suggested order

1. Neighbourhood CSV (section F) — unblocks everything else
2. Port C1 (DH expansion) — fixes the baseline DH gap and 4 scenarios
3. Resolve D1, then port C2 (congestion) — 3 scenarios
4. Port C3 (policy plans) — 6 scenarios; no external data needed
5. 10-iteration runs both sides, compare distributions rather than single draws

On (5): the engine's block-level spread looked much narrower than AL's (sd ≈ 0.7 vs 4.1) on a
2-iteration sample. If that holds with more iterations it is its own discrepancy — matching means
would be hiding a structural difference in stochasticity. Worth settling before declaring parity.
