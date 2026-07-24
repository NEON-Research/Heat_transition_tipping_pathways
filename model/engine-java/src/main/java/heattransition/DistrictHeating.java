package heattransition;

import java.util.List;

/**
 * Faithful port of AnyLogic's DistrictHeatingCompany agent.
 * Runs once per year, after the adoption process (Main.f_adoptionProces step 2).
 *
 * COST_BASED expansion runs in the BASELINE, not just in policy scenarios — treating the
 * district-heating grid as static was the entire baseline DH gap (AL 0.8% vs engine 0.4%),
 * and also caused HOA blocks to pick electric heat pumps where AL gives them district heating.
 */
public final class DistrictHeating {

    public static final double REQUIRED_HEAT_DEMAND_GJ_PER_HA = 600;  // p_requiredHeatDemand_GJPerHa
    public static final int PLAN_AND_CONSTRUCTION_YEARS = 5;          // scenario dhConstructionTime
    private static final int LATEST_OPERATIONAL_YEAR = 2045;          // hardcoded in AL

    private DistrictHeating() {}

    /**
     * f_DHExpansionPlanCosts — build where heat density justifies it.
     *
     * dhYearOfOperation is a LATCH: the else-if means a scheduled neighbourhood is never
     * re-evaluated, it only waits for its year.
     */
    private static void planCosts(List<Neighbourhood> nbhs, int year, Rng rng, int planYears) {
        for (Neighbourhood n : nbhs) {
            if (!n.hasDH && n.dhYearOfOperation == 0) {
                if (n.surfaceAreaLand <= 0) continue;      // a_lan_ha == 0 would divide by zero
                double kWh = 0;
                for (Dwelling d : n.dhDwellings) kWh += d.heatDemandKWh;
                double gjPerHa = kWh * 3.6 / 1000 / n.surfaceAreaLand;

                if (gjPerHa >= REQUIRED_HEAT_DEMAND_GJ_PER_HA) {
                    int latest = (int) Math.round((double) LATEST_OPERATIONAL_YEAR - year - planYears);
                    n.dhYearOfOperation = latest > 0
                        ? year + planYears + rng.nextInt(0, latest)   // nextInt is exclusive of bound
                        : year + planYears;
                }
            } else if (year == n.dhYearOfOperation) {
                n.hasDH = true;
            }
        }
    }

    /** f_DHExpansionPlanPolicy — build where the TVW plan says district heating. */
    private static void planPolicy(List<Neighbourhood> nbhs, int year, Rng rng,
                                   int planYears, int nbhsWithoutDHStartYear) {
        double chance = nbhs.isEmpty() ? 0 : (double) nbhsWithoutDHStartYear / nbhs.size();
        for (Neighbourhood n : nbhs) {
            if (!n.hasDH && n.policyPlan.equals("DISTRICT_HEATING")) {
                if (n.policyPlanYear > 0) {
                    if (n.policyPlanYear + planYears == year) n.hasDH = true;
                } else if (rng.next() < chance) {
                    n.hasDH = true;
                }
            }
        }
    }

    /** f_DHExpansion — dispatch on the scenario strategy. Returns nbh_with_dh_perc. */
    public static double expand(List<Neighbourhood> nbhs, int year, Rng rng,
                                String strategy, int planYears, int nbhsWithoutDHStartYear) {
        if ("POLICY_BASED".equals(strategy)) {
            planPolicy(nbhs, year, rng, planYears, nbhsWithoutDHStartYear);
        } else {
            planCosts(nbhs, year, rng, planYears);
        }

        // Propagate to dwellings — that is where f_getHeatingMethodPossibility reads it.
        // Only ever set true; AL never revokes a grid once built.
        int withDH = 0;
        for (Neighbourhood n : nbhs) {
            if (n.hasDH) {
                withDH++;
                for (Dwelling d : n.allDwellings) d.hasDistrictHeatingGrid = true;
            }
        }
        return nbhs.isEmpty() ? 0 : (double) withDH / nbhs.size();
    }

    /**
     * Draw each neighbourhood's policy plan year, ONCE per run (AL does this during setup).
     * Returns v_nbhsWithoutDHStartYear, which drives chanceOfDHBeingInstalled.
     *
     * uniform_discr(a,b) is INCLUSIVE of both ends, unlike nextInt(a,b).
     */
    public static int drawPolicyPlanYears(List<Neighbourhood> nbhs, Rng rng, int startYear) {
        int withoutStart = 0;
        for (Neighbourhood n : nbhs) {
            int s = n.policyStartJaar > 0 ? n.policyStartJaar : startYear;
            int e = n.policyEindJaar > 0 ? n.policyEindJaar : 2050;
            n.policyPlanYear = e >= s ? rng.nextInt(s, e + 1) : s;
            if (n.policyInfraW && n.policyStartJaar == 0) withoutStart++;
        }
        return withoutStart;
    }
}
