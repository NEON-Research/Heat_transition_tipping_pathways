package heattransition;

import java.util.List;

/** Adopter and dwelling segmentation, used to trace WHO drives each tipping mechanism.
 *
 *  Rogers (1962/2003) adopter categories are assigned by percentile of an adoption-propensity
 *  index built from the three household-level drivers the model actually represents:
 *  sustainability attitude, the climate-concern of the peer network (social exposure), and the
 *  technical readiness of the dwelling (insulation label). The canonical Rogers shares are
 *  innovators 2.5 %, early adopters 13.5 %, early majority 34 %, late majority 34 %, laggards 16 %.
 *
 *  Dwelling segments are a coarse cross of the characteristics that drive the EAC, so the
 *  economic-learning mechanism can be traced to the dwelling types it reaches first. */
public final class Segments {
    private Segments() {}

    public static final String[] ROGERS = {"1_innovators","2_early_adopters","3_early_majority",
                                           "4_late_majority","5_laggards"};
    /** Percentile FLOOR of each category, from the top: innovators are the highest-propensity 2.5 %,
     *  then early adopters (next 13.5 %), early majority (34 %), late majority (34 %), laggards (16 %). */
    private static final double[] FLOOR = {0.975, 0.84, 0.50, 0.16, 0.0};

    /** Propensity index in [0,1]: attitude + network climate-concern + dwelling readiness. */
    public static double propensity(Dwelling d, double networkMeanAttitude) {
        double label = 1.0 - (Vesta.labelNum(d.energyLabel) - 1) / 6.0;   // a=1 -> 1.0, g=7 -> 0.0
        return Constants.SEG_W_ATTITUDE * d.attitude
             + Constants.SEG_W_NETWORK  * networkMeanAttitude
             + Constants.SEG_W_LABEL    * label;
    }

    /** Assign Rogers categories by percentile rank of the propensity index. */
    public static void assignRogers(List<Dwelling> homeowners) {
        if (homeowners.isEmpty()) return;
        double[] v = homeowners.stream().mapToDouble(d -> d.propensity).sorted().toArray();
        double[] cut = new double[FLOOR.length];
        for (int i = 0; i < FLOOR.length; i++)
            cut[i] = v[(int) Math.min(v.length - 1, Math.round(FLOOR[i] * (v.length - 1)))];
        for (Dwelling d : homeowners) {
            int k = 0;                                   // highest propensity -> innovators
            while (k < FLOOR.length - 1 && d.propensity < cut[k]) k++;
            d.segRogers = ROGERS[k];
        }
    }

    /** Context segment: ownership x archetype x heat-demand tier. The demand tiers are the model's
     *  own investment tiers (Economics.investmentCostsSupply), so the grouping lines up with the
     *  cost structure that drives the decision. This is the cut used to ask which kind of
     *  owner-and-dwelling combination ends up on which heating method. */
    public static String contextSegment(Dwelling d) {
        String own = d.ownership == null ? "NA" : d.ownership;
        String arch = d.archetype == null ? "NA" : d.archetype;
        String dem = d.heatDemandKWh < 6000 ? "demLow"
                   : (d.heatDemandKWh > 10000 ? "demHigh" : "demMed");
        return own + "-" + arch + "-" + dem;
    }

    /** Coarse dwelling segment: archetype x floor-area band x construction-era x insulation band. */
    public static String dwellingSegment(Dwelling d) {
        String type = d.archetype == null ? "NA" : d.archetype;
        String area = d.livingAreaM2 < 75 ? "small" : (d.livingAreaM2 < 150 ? "medium" : "large");
        String era  = d.constructionYear < 1975 ? "pre1975"
                    : (d.constructionYear < 2000 ? "1975-1999" : "post2000");
        int ln = Vesta.labelNum(d.energyLabel);
        String ins = ln <= 2 ? "labelAB" : (ln <= 4 ? "labelCD" : "labelEFG");
        return type + "-" + area + "-" + era + "-" + ins;   // "-" not "|": | is the map-key delimiter
    }
}
