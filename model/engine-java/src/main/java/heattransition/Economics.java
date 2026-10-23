package heattransition;

/** Equivalent Annual Cost (EAC) of a heating system for a dwelling.
 *  Faithful to J_Household.f_getEAC: discounted TCO annualised via the annuity factor. */
public final class Economics {
    private Economics() {}

    public interface InsulationCost { double cost(Dwelling d, String toLabel); }

    public static double investmentCostsSupply(HeatingSystemSpec hs, double heatDemandKWh) {
        if (!Double.isNaN(hs.investFixed)) {      // heating_system_capex.csv: fixed + per kW
            double kw = Math.max(hs.kwMin, Math.min(hs.kwMax, hs.kwPerKWh * heatDemandKWh));
            return hs.investFixed + hs.investPerKw * kw;
        }
        if (heatDemandKWh < 6000) return hs.investSmall;
        if (heatDemandKWh > 10000) return hs.investLarge;
        return hs.investMedium;
    }

    public static long computeEAC(HeatingSystemSpec hs, Dwelling d, InsulationCost insulationCost) {
        double[] c = costParts(hs, d, insulationCost);
        double r = hs.discountRate;
        double tco = 0;
        for (int t = 0; t < hs.lifetime; t++) {
            if (t == 0) tco += c[0];
            tco += c[1] / Math.pow(1 + r, t);
        }
        double annuityFactor = (1 - Math.pow(1 + r, -hs.lifetime)) / r;
        return Math.round(tco / annuityFactor);
    }

    /** The two parts the EAC is built from: {net upfront investment (investment minus subsidy, EUR
     *  one-off), annual running cost (energy + maintenance + fixed connection charges, EUR/yr)}.
     *  The EAC annuitises the first over the lifetime and adds the second. The split PBC
     *  (Constants.PBC_SPLIT) scores the two separately. For district heating the investment is the
     *  connection fee minus the ISDE warmtenet for a dwelling not yet connected, 0 for one that is. */
    public static double[] costParts(HeatingSystemSpec hs, Dwelling d, InsulationCost insulationCost) {
        // District heating has no equipment of the household's own: the delivery set is rented and
        // the network belongs to the operator. So its one-off costs are the ACM connection fee for a
        // dwelling that newly connects (minus the subsidy), nothing for a dwelling that is already
        // connected, and the disconnection fee on top of any other option for a dwelling that
        // leaves, plus the non-monetary exit barrier (Constants.DH_EXIT_BARRIER). The annual DH
        // costs are in fixedAnnualCost (DistrictHeatingCosts.annualFixed).
        boolean onDH = d.currentType == HeatingSystem.DISTRICT_HEATING;
        // investment subsidy of the simulated year for this technology and ownership group
        // (Subsidies, from data/reference/subsidies.csv; the spreadsheet's subsidy column is unused)
        double investment, subsidy = Subsidies.amount(hs.type, d.ownership);
        if (hs.type == HeatingSystem.DISTRICT_HEATING) {
            // connectionCostsOff: no connection fee, so no ISDE warmtenet toward it either
            if (onDH || Constants.CONNECTION_COSTS_OFF) { investment = 0; subsidy = 0; }
            else investment = Constants.CAPEX_MULT_DH * DistrictHeatingCosts.connectionFee();
        } else {
            investment = investmentCostsSupply(hs, d.heatDemandKWh);
            if (onDH) investment += (Constants.CONNECTION_COSTS_OFF ? 0 : DistrictHeatingCosts.disconnectionFee())
                    + Constants.DH_EXIT_BARRIER;
        }
        if (hs.requiresLowTemp && !d.hasLowTemp) investment += hs.distributionSystemCost;

        // Insulation upgrade to the system's required label. Call unconditionally and let
        // Vesta return 0 when no upgrade is needed (labelNum(to) >= labelNum(from)) -- exactly
        // as the JS engine does. Do NOT re-guard here with Constants.labelToNumber: that method
        // uses the REVERSED scale (a=7..g=1) while Vesta.labelNum uses (a=1..g=7), so the old
        // `req < cur` test was inverted and silently skipped insulation for every dwelling that
        // actually needed it -- collapsing the hybrid/electric EAC window (see HANDOVER 2c).
        if (hs.requiredLabel != null && !hs.requiredLabel.equals("no")) {
            investment += insulationCost.cost(d, hs.requiredLabel);
        }

        // Annual running cost = energy + maintenance + the FIXED connection charge for this
        // technology. The fixed term is what makes leaving gas worth something on its own: a
        // dwelling on an electric heat pump drops the gas connection, a hybrid keeps paying it,
        // and a district-heating connection carries its own (larger) standing charge. The
        // electricity connection is common to every option and is therefore not charged.
        double energyPerYear = hs.primaryCostPerKWh * d.heatDemandKWh * hs.fractionPrimary / hs.efficiencyPrimary;
        if (hs.secondarySource != null && !hs.secondarySource.equals("NOT_APPLICABLE")) {
            energyPerYear += hs.secondaryCostPerKWh * d.heatDemandKWh * hs.fractionSecondary / hs.efficiencySecondary;
        }

        return new double[]{ investment - subsidy, hs.maintenance + hs.fixedAnnualCost + energyPerYear };
    }
}
