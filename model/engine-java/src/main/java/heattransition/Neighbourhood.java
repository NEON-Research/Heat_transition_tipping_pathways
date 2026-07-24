package heattransition;

import java.util.ArrayList;
import java.util.List;

/**
 * GZ_Neighborhood, reduced to what the district-heating company and (later) the DSO need.
 *
 * dhDwellings = homeowners + landlord renters + social-block members.
 * AL's f_DHExpansionPlanCosts EXCLUDES HOA blocks from the heat-demand sum, so the two lists
 * are deliberately different. allDwellings is used only to propagate the grid flag.
 */
public final class Neighbourhood {
    public final String buurt;

    public final List<Dwelling> dhDwellings = new ArrayList<>();   // DH heat-demand basis (no HOA)
    public final List<Dwelling> allDwellings = new ArrayList<>();  // everyone, for flag propagation

    /** v_hasDistrictHeatingGrid — true at t0 when p_districtHeating_perc > 0 */
    public boolean hasDH;
    /** v_districtHeatYearOfOperation — scheduling latch; 0 means "not yet planned" */
    public int dhYearOfOperation = 0;

    public double surfaceAreaLand = 0;      // p_surfaceAreaLand (a_lan_ha)
    public int households = 0;              // p_households (a_hh)
    public double avgElectricityKWh = 0;    // p_averageElectricityConsumptionTotal (g_ele)
    public int cars = 0;                    // p_cars (a_pau)

    public String policyPlan = "NONE";      // p_policyPlan
    public int policyStartJaar = 0;
    public int policyEindJaar = 0;
    public boolean policyInfraW = false;
    public int policyPlanYear = 0;          // drawn per run, see DistrictHeating.drawPolicyPlanYears

    public Neighbourhood(String buurt) { this.buurt = buurt; }
}
