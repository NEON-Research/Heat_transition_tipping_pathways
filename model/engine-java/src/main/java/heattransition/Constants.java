package heattransition;

import java.util.Map;

/** Calibration constants, extracted verbatim from the AnyLogic model.
 *  Mirrors ../../tests/heat_model_ref.py and the JS engine's constants.js. */
public final class Constants {
    private Constants() {}

    /** Runtime override: -Dht.<key>=<value> (system property) or HT_<KEY> (env), else the AL default.
     *  Lets sensitivity sweeps / calibration vary the weights without recompiling. */
    static double p(String key, double dflt) {
        String v = System.getProperty("ht." + key);
        if (v == null) v = System.getenv("HT_" + key.toUpperCase().replace('.', '_'));
        if (v == null || v.isEmpty()) return dflt;
        try { return Double.parseDouble(v); } catch (NumberFormatException e) { return dflt; }
    }

    // Startup_agent.f_setDefaultWeights()  (overridable: -Dht.wAffordabilityToPbc=... etc.)
    public static final double WEIGHT_AFFORDABILITY_TO_PBC   = p("wAffordabilityToPbc", 0.5);
    public static final double WEIGHT_EFFORT_TO_PBC          = p("wEffortToPbc", 0.2);
    public static final double WEIGHT_ATTITUDE_TO_INTENTION  = p("wAttitudeToIntention", 0.5);
    public static final double WEIGHT_PBC_TO_INTENTION       = p("wPbcToIntention", 0.5);
    public static final double WEIGHT_SOCIALNORM_TO_INTENTION = p("wSocialnormToIntention", 0.5);
    public static final double WEIGHT_INTENTION_TO_BEHAVIOR  = p("wIntentionToBehavior", 0.5);
    public static final double WEIGHT_PBC_TO_BEHAVIOR        = p("wPbcToBehavior", 0.5);

    // Startup_agent.f_initializeMain()
    public static final double GUMBEL_SCALE_UTIL = p("gumbelScaleUtil", 0.02);
    public static final double GUMBEL_SCALE_EAC  = p("gumbelScaleEac", 20);

    // J_HeatingSystemOptionsGlobal salience S-curve constants
    public static final double SALIENCE_K = p("salienceK", 10.0);
    public static final double SALIENCE_THRESHOLD = p("salienceThreshold", 0.3);
    public static final double SALIENCE_STEEPNESS = p("salienceSteepness", 30.0);

    // Stochastic equipment lifetime: each installed system's end-of-life age is drawn ~ N(lifetime,
    // sd) clamped to lifetime +/- maxDev, redrawn on every (re)install, for ALL heating methods. This
    // smears the deterministic-lifetime cohorts (e.g. the HOA end-of-life "echo" where a whole cohort
    // re-decides in lockstep). Set -Dht.lifetimeJitterSd=0 to recover the AnyLogic-faithful
    // deterministic lifetime.
    public static final double LIFETIME_JITTER_SD  = p("lifetimeJitterSd", 1.0);
    public static final int    LIFETIME_JITTER_MAX = (int) p("lifetimeJitterMax", 3.0);

    // Energy-price paths: real annual growth applied to the RETAIL price of each fuel, price(y) =
    // p0*(1+g)^(y-startYear), for all simulation years (myopic: each year's EAC uses that year's price
    // held flat over the equipment horizon). Defaults 0 = today's static prices (parity). Scenario
    // values are derived from KEV 2025 Table 10 wholesale 2030 bandwidth passed through additively to
    // the retail base (taxes/network fixed) -- see CALIBRATION_AND_VALIDATION.md sec 6.5. HEAT (DH)
    // follows the gas growth ("niet meer dan anders"). Only gas & electricity are varied.
    public static final double GAS_PRICE_GROWTH  = p("gasPriceGrowth", 0.0);
    public static final double ELEC_PRICE_GROWTH = p("elecPriceGrowth", 0.0);

    // Techno-economic sensitivity multipliers (default 1.0 = data as-is). capexMultHp scales the
    // investment cost of the heat pumps (hybrid + electric); learningRateMult scales every
    // technology's economic_learning_rate (the capex-fall speed). For structural sensitivity screens.
    public static final double CAPEX_MULT_HP     = p("capexMultHp", 1.0);
    public static final double LEARNING_RATE_MULT = p("learningRateMult", 1.0);

    // f_learningFactorToMultiplier(): LOW/MEDIUM/HIGH -> multiplier
    public static final Map<String, Double> LEARNING_MULTIPLIER =
            Map.of("LOW", 0.5, "MEDIUM", 1.0, "HIGH", 2.0);

    public static final int DEFAULT_START_YEAR = 2024;
    public static final int DEFAULT_END_YEAR   = 2050;
    public static final long DEFAULT_SEED      = 1;

    public static final int MAX_SUSTAINABILITY_SCORE = 5;

    /** Energy-label letter -> number (A best = 7). J_Dwelling.f_insulationLabelLetterToNumber. */
    public static int labelToNumber(String label) {
        switch (label == null ? "" : label.toLowerCase()) {
            case "a": return 7; case "b": return 6; case "c": return 5;
            case "d": return 4; case "e": return 3; case "f": return 2; case "g": return 1;
            default:  return 5;
        }
    }
}
