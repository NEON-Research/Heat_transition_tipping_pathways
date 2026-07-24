# DH expansion + grid congestion — port spec

Traced from the generated Java in `deprecated/Heat transition tipping pathways_BUILD/src.generated/`,
which is authoritative (the .alp XML hid these in agent bodies I never read).

Both mechanisms are **missing entirely** from the JS/Java engine. Together they gate 7 of the
16 scenarios, and DH expansion also affects the baseline.

---

## 1. District heating expansion — `DistrictHeatingCompany`

### Initial grid (`Main`, neighbourhood setup)

```java
if(x.p_districtHeating_perc > 0){ x.v_hasDistrictHeatingGrid = true; }
else                            { x.v_hasDistrictHeatingGrid = false; }
```

This matches the engine's `hasDHgrid` derived from the DH share, so the *starting* state is
probably right (engine: 1051 / 14421 = 7.3%). Verify against AL's `nbh_with_dh_perc` at 2024.

### COST_BASED expansion — `f_DHExpansionPlanCosts()` — **runs in the baseline**

```java
for(GZ_Neighborhood nbh : main.pop_GZ_Neighborhoods){
    if(!nbh.v_hasDistrictHeatingGrid && nbh.v_districtHeatYearOfOperation == 0){
        // heat demand of homeowners + landlord renters + social block (NOT HOA blocks)
        double dhHeatDemandTotal_kWh = sum over those households of getHeatDemand_kWh();
        double dhHeatDemand_GJperHa = dhHeatDemandTotal_kWh * 3.6 / 1000 / nbh.p_surfaceAreaLand;

        if( dhHeatDemand_GJperHa >= p_requiredHeatDemand_GJPerHa ){       // = 600
            int latestPossibleYear = roundToInt(2045 - main.v_year - p_planAndConstructionTime_years);
            if(latestPossibleYear > 0){
                nbh.v_districtHeatYearOfOperation = main.v_year + p_planAndConstructionTime_years
                                                  + main.randomInstance.nextInt(0, latestPossibleYear);
            } else {
                nbh.v_districtHeatYearOfOperation = main.v_year + p_planAndConstructionTime_years;
            }
        }
    }
    else if (main.v_year == nbh.v_districtHeatYearOfOperation){
        nbh.v_hasDistrictHeatingGrid = true;
    }
}
```

Note the households summed are `c_homeOwners + c_rentersLandlords + socialHousingBlock` —
**HOA blocks are excluded**. Easy to get wrong.

### POLICY_BASED expansion — `f_DHExpansionPlanPolicy()`

```java
double chanceOfDHBeingInstalled = main.v_nbhsWithoutDHStartYear / main.pop_GZ_Neighborhoods.size();
for(nbh) {
    if(!nbh.v_hasDistrictHeatingGrid && nbh.p_policyPlan == DISTRICT_HEATING){
        if(nbh.p_policyPlanYear > 0){
            if(nbh.p_policyPlanYear + p_planAndConstructionTime_years == main.v_year)
                nbh.v_hasDistrictHeatingGrid = true;
        } else {                                   // no start year given -> random
            if( uniform(0,1) < chanceOfDHBeingInstalled ) nbh.v_hasDistrictHeatingGrid = true;
        }
    }
}
```

Requires `p_policyPlan` (a heating system per neighbourhood, from the TVW strategy) and
`p_policyPlanYear`.

---

## 2. Grid congestion — `GZ_Neighborhood.f_energyCalculations` + `DSO`

Runs every year, after the adoption process.

```java
v_maxGridLoad_kW = f_getGridImpact();
if( v_maxGridLoad_kW > p_gridCapacity_kW ){
    v_hasGridCongestion = true;
    main.pop_DSOs.get(0).c_neighborhoodsWaitingToBeReinforced.add(this);
    main.pop_DSOs.get(0).c_neighborhoodsNotReinforced.remove(this);
}
```

### `f_getGridImpact()` — Velander baseload + heat pumps + EVs

```java
double alfa = 0.23e-3, beta = 0.016;
double JaarverbruikAansluitingen = p_households * p_averageElectricityConsumptionTotal;
double PmaxHouseholds = alfa * JaarverbruikAansluitingen + beta * sqrt(JaarverbruikAansluitingen);

// heat pumps: social block counted as a whole, plus homeowners + landlord renters individually
double PmaxHeatPumps = electricHeatPumps * p_simultaneityHeatPump * p_PmaxPerElectricHeatPump
                     + hybridHeatPumps  * p_simultaneityHeatPump * p_PmaxPerHybridHeatPump;

double PmaxEVs = f_getEVs() * p_PmaxPerEV * p_simultaneityEV;
```

Same asymmetry: the social block contributes via `socialHousingBlock.getHeatingMethod()` for all
its households; HOA blocks are not counted.

### `DSO.f_reinforceNeighborhoods(yearIndex)`

```java
c_neighborhoodsReinforced.addAll(c_neighborhoodsBeingReinforced);
c_neighborhoodsBeingReinforced.clear();
while (c_neighborhoodsBeingReinforced.size() < p_DSORenovationCapacity_nbhPerYear + 1
       && c_neighborhoodsWaitingToBeReinforced.size() > 0){
    GZ_Neighborhood n = c_neighborhoodsWaitingToBeReinforced.get(0);   // FIFO
    c_neighborhoodsBeingReinforced.add(n);
    c_neighborhoodsWaitingToBeReinforced.remove(n);
    n.v_hasGridCongestion = false;
}
```

`p_DSORenovationCapacity_nbhPerYear = 100`. Note the `+1` and the FIFO order — both matter for
exact reproduction. Congestion clears the year a neighbourhood enters reinforcement.

---

## 3. Data the engine does not currently export

`nbh_heating.json` has only `{gasCV, gasBlock, ehp, hhp, dh, hasDHgrid}`. The neighbourhood
population is created with far more (`Main.add_pop_GZ_Neighborhoods`):

| field | needed for |
|---|---|
| `p_surfaceAreaLand` | DH cost-based expansion (GJ/ha) |
| `p_gridCapacity_kW` | congestion threshold |
| `p_households` | Velander baseload |
| `p_averageElectricityConsumptionTotal` | Velander baseload |
| `p_cars`, `p_EVAdoptionFactor` | EV load |
| `p_policyPlan`, `p_policyPlanYear` | POLICY_BASED DH + SHA |

### Main-level parameters — ALL RESOLVED

| parameter | value | source |
|---|---|---|
| `p_requiredHeatDemand_GJPerHa` | **600** | Main default |
| `p_planAndConstructionTime_years` | **5** | = `p_dhConstructionTime_years` <- `currentScenario.dhConstructionTime`; the results column `DHCT` is 5 in all 16 scenarios |
| `p_DSORenovationCapacity_nbhPerYear` | **100** | Main default |
| `p_simultaneityHeatPump` | **0.9** | phasetophase 13.1 |
| `p_PmaxPerElectricHeatPump` | **5 kW** | phasetophase 13.1 |
| `p_PmaxPerHybridHeatPump` | **2.5 kW** | Main default |
| `p_PmaxPerEV` | **3.7 kW** | Main default |
| `p_simultaneityEV` | **0.2** | Main default |

Velander constants are inline in `f_getGridImpact`: `alfa = 0.23e-3`, `beta = 0.016`.

### Neighbourhood DB columns still to export

Filled in `Main.f_setNeighborhoodData` from the `neighborhood_data_selection` table:

| GZ_Neighborhood field | DB column | needed for |
|---|---|---|
| `p_surfaceAreaLand` | `a_lan_ha` | DH cost-based expansion (GJ/ha) |
| `p_averageElectricityConsumptionTotal` | `g_ele` | Velander baseload |
| `p_gridCapacity_kW` | (locate near the others) | congestion threshold |
| `p_households` | (locate) | Velander baseload |
| `p_cars`, `p_EVAdoptionFactor` | (locate) | EV load |
| `p_policyPlan`, `p_policyPlanYear` | (locate) | POLICY_BASED DH + SHA |

Two confirmed so far by reading `Main.java` around line 2805/2841. The rest sit in the same
block — worth grabbing the whole `neighborhood_data_selection` row rather than column by column,
since the engine will likely need more of it later anyway.

---

## Suggested order

1. Extend the neighbourhood export with the table above (one SQL change).
2. Port cost-based DH expansion — fixes the baseline DH gap (AL 0.8% vs engine 0.4%) and
   unblocks the connection-obligation scenarios.
3. Port congestion + DSO — unblocks the three `grid_congestion_ban` scenarios.
4. Port policy plans — unblocks the six POLICY_BASED scenarios.

Steps 2 and 3 are self-contained given the data. Step 4 needs the TVW policy-plan source.
