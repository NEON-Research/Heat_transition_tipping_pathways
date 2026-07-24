// districtHeating.js — faithful port of AnyLogic's DistrictHeatingCompany agent.
//
// Traced from Heat transition tipping pathways_BUILD/src.generated/.../DistrictHeatingCompany.java
// Runs once per year, AFTER the adoption process (Main.f_adoptionProces step 2).
//
// This is NOT a policy-scenario-only mechanism: COST_BASED expansion runs in the BASELINE,
// so the district-heating grid grows in every scenario. Treating hasDHgrid as static was the
// entire baseline DH gap (AL 0.8% vs engine 0.4%).

export const DH_REQUIRED_HEAT_DEMAND_GJ_PER_HA = 600;   // Main p_requiredHeatDemand_GJPerHa
export const DH_PLAN_AND_CONSTRUCTION_YEARS = 5;        // = scenario dhConstructionTime (DHCT)
const DH_LATEST_OPERATIONAL_YEAR = 2045;                // hardcoded in f_DHExpansionPlanCosts

/**
 * f_DHExpansionPlanCosts — build where heat density justifies it.
 *
 *   dhHeatDemand_GJperHa = sum(kWh) * 3.6 / 1000 / surfaceAreaLand
 *   if >= 600: schedule operation for year + planTime + rand(0, 2045 - year - planTime)
 *   else if year == scheduled: the grid goes live
 *
 * Two things that are easy to get wrong and both matter:
 *  - the demand sum covers homeowners + landlord renters + the social block, but NOT HOA blocks
 *  - `dhYearOfOperation` is a latch: the `else if` means a neighbourhood that has been
 *    scheduled is never re-evaluated, it only waits for its year
 */
function expansionPlanCosts(neighbourhoods, year, rng, planYears) {
  for (const nbh of neighbourhoods) {
    if (!nbh.hasDH && nbh.dhYearOfOperation === 0) {
      if (!nbh.surfaceAreaLand) continue;          // guard: a_lan_ha == 0 would divide by zero
      let kWh = 0;
      for (const d of nbh.dhDwellings) kWh += d.heatDemandKWh;
      const gjPerHa = kWh * 3.6 / 1000 / nbh.surfaceAreaLand;

      if (gjPerHa >= DH_REQUIRED_HEAT_DEMAND_GJ_PER_HA) {
        const latest = Math.round(DH_LATEST_OPERATIONAL_YEAR - year - planYears);
        nbh.dhYearOfOperation = latest > 0
          ? year + planYears + rng.int(0, latest)
          : year + planYears;
      }
    } else if (year === nbh.dhYearOfOperation) {
      nbh.hasDH = true;
    }
  }
}

/**
 * f_DHExpansionPlanPolicy — build where the TVW plan says district heating.
 *
 *   chance = v_nbhsWithoutDHStartYear / nNeighbourhoods
 *   with a start year : goes live at policyPlanYear + planTime
 *   without           : each year, `chance` probability of going live
 *
 * `nbhsWithoutDHStartYear` counts neighbourhoods whose infra contains "W" and whose
 * start_jaar is 0 — it must be built during loading, not derived here.
 */
function expansionPlanPolicy(neighbourhoods, year, rng, planYears, nbhsWithoutDHStartYear) {
  const chance = neighbourhoods.length ? nbhsWithoutDHStartYear / neighbourhoods.length : 0;
  for (const nbh of neighbourhoods) {
    if (!nbh.hasDH && nbh.policyPlan === 'DISTRICT_HEATING') {
      if (nbh.policyPlanYear > 0) {
        if (nbh.policyPlanYear + planYears === year) nbh.hasDH = true;
      } else if (rng.next() < chance) {
        nbh.hasDH = true;
      }
    }
  }
}

/** f_DHExpansion — dispatch on the scenario's DH expansion strategy. */
export function dhExpansion(neighbourhoods, year, rng, opts = {}) {
  const strategy = opts.strategy || 'COST_BASED';
  const planYears = opts.planYears ?? DH_PLAN_AND_CONSTRUCTION_YEARS;

  if (strategy === 'POLICY_BASED') {
    expansionPlanPolicy(neighbourhoods, year, rng, planYears, opts.nbhsWithoutDHStartYear || 0);
  } else {
    expansionPlanCosts(neighbourhoods, year, rng, planYears);
  }

  // Propagate to the dwellings, which is where f_getHeatingMethodPossibility reads it.
  // Only ever set true — AL never revokes a grid once built.
  let withDH = 0;
  for (const nbh of neighbourhoods) {
    if (nbh.hasDH) {
      withDH++;
      for (const d of nbh.allDwellings) d.hasDistrictHeatingGrid = true;
    }
  }
  return neighbourhoods.length ? withDH / neighbourhoods.length : 0;   // nbh_with_dh_perc
}

/**
 * Draw each neighbourhood's policy plan year, once per run.
 * AL: startYear = (start_jaar == 0) ? currentYear : start_jaar
 *     endYear   = (eind_jaar  == 0) ? 2050        : eind_jaar
 *     policyPlanYear = uniform_discr(startYear, endYear)
 * Drawn per run, so it is a genuine source of run-to-run variance — don't precompute it
 * in the data export.
 */
export function drawPolicyPlanYears(neighbourhoods, rng, startYear) {
  let nbhsWithoutDHStartYear = 0;
  for (const nbh of neighbourhoods) {
    const s = nbh.policyStartJaar > 0 ? nbh.policyStartJaar : startYear;
    const e = nbh.policyEindJaar > 0 ? nbh.policyEindJaar : 2050;
    // uniform_discr(a,b) is INCLUSIVE of both ends; rng.int(lo,hi) is exclusive of hi.
    // (nextInt(0,bound) above IS exclusive, so the two calls differ deliberately.)
    nbh.policyPlanYear = e >= s ? rng.int(s, e + 1) : s;
    if (nbh.policyInfraW && nbh.policyStartJaar === 0) nbhsWithoutDHStartYear++;
  }
  return nbhsWithoutDHStartYear;
}
