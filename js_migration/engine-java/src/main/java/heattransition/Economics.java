package heattransition;

/** Equivalent Annual Cost (EAC) of a heating system for a dwelling.
 *  Faithful to J_Household.f_getEAC: discounted TCO annualised via the annuity factor. */
public final class Economics {
    private Economics() {}

    public interface InsulationCost { double cost(Dwelling d, String toLabel); }

    public static double investmentCostsSupply(HeatingSystemSpec hs, double heatDemandKWh) {
        if (heatDemandKWh < 6000) return hs.investSmall;
        if (heatDemandKWh > 10000) return hs.investLarge;
        return hs.investMedium;
    }

    public static long computeEAC(HeatingSystemSpec hs, Dwelling d, InsulationCost insulationCost) {
        double investment = investmentCostsSupply(hs, d.heatDemandKWh);
        if (hs.requiresLowTemp && !d.hasLowTemp) investment += hs.distributionSystemCost;

        if (hs.requiredLabel != null && !hs.requiredLabel.equals("no")) {
            int req = Constants.labelToNumber(hs.requiredLabel);
            int cur = Constants.labelToNumber(d.energyLabel);
            if (req < cur) investment += insulationCost.cost(d, hs.requiredLabel);
        }

        double energyPerYear = hs.primaryCostPerKWh * d.heatDemandKWh * hs.fractionPrimary / hs.efficiencyPrimary;
        if (hs.secondarySource != null && !hs.secondarySource.equals("NOT_APPLICABLE")) {
            energyPerYear += hs.secondaryCostPerKWh * d.heatDemandKWh * hs.fractionSecondary / hs.efficiencySecondary;
        }

        double r = hs.discountRate;
        double tco = 0;
        for (int t = 0; t < hs.lifetime; t++) {
            if (t == 0) tco += investment - hs.subsidy;
            tco += (hs.maintenance + energyPerYear) / Math.pow(1 + r, t);
        }
        double annuityFactor = (1 - Math.pow(1 + r, -hs.lifetime)) / r;
        return Math.round(tco / annuityFactor);
    }
}
