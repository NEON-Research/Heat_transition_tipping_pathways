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

    // Knock-out switch for the SOCIAL-LEARNING loop: freeze salience at its initialisation value so
    // the subjective norm no longer responds to adoption. Used for the causal loop test (Q8); note
    // this differs from zeroing the social-norm weight, which changes the decision structure itself.
    public static final boolean SALIENCE_FREEZE = p("salienceFreeze", 0.0) != 0.0;

    /** Continuous override of the social-learning multiplier (scenario LOW/MEDIUM/HIGH = 0.5/1/2).
     *  Negative = use the scenario value. Lets the social channel be swept rather than only stepped,
     *  so the level at which social reinforcement flips from entrenching the incumbent to
     *  accelerating the entrant can be located. */
    public static final double SOCIAL_LEARNING_MULT = p("socialLearningMult", -1.0);

    /** TRUE social-learning knock-out: freeze each household's peer composition at its initial value,
     *  so the subjective norm no longer responds to what neighbours do. `salienceFreeze` only pins the
     *  amplifier and leaves peer share free to evolve, so it does NOT sever the diffusion loop. */
    public static final boolean PEER_FREEZE = p("peerFreeze", 0.0) != 0.0;

    // ---- Electricity grid / DSO (GridModel) ----------------------------------------------------
    // Baseload is derived from the dwellings themselves (NOT the privacy-suppressed CBS g_ele).
    public static final double BASELOAD_KWH_PER_DWELLING = p("baseloadKWhPerDwelling", 2500.0);
    public static final double BASELOAD_PEAK_FACTOR      = p("baseloadPeakFactor", 3.0);   // avg kW -> peak kW
    public static final double SIMULTANEITY_BASE         = p("simultaneityBase", 0.20);
    public static final double PMAX_ELECTRIC_HEAT_PUMP   = p("pmaxElectricHeatPump", 3.0); // kW at design
    public static final double SIMULTANEITY_HEAT_PUMP    = p("simultaneityHeatPump", 0.80);
    public static final double PMAX_EV                   = p("pmaxEv", 3.7);               // kW charger
    public static final double SIMULTANEITY_EV           = p("simultaneityEv", 0.30);
    public static final double GRID_DESIGN_MARGIN        = p("gridDesignMargin", 1.20);
    /** Per-dwelling design capacity floor (AL's capacityPerHousehold_kW): grids are dimensioned to a
     *  standard connection allowance per dwelling, not only to the measured t0 load. Prevents small
     *  neighbourhoods from being congested by a single extra heat pump. */
    public static final double CAPACITY_PER_DWELLING_KW  = p("capacityPerDwellingKW", 1.50);
    /** Optional round-up of capacity to a discrete transformer size (kW). Default 0 = off: rounding
     *  to 50 kW steps over-provisioned small neighbourhoods and delayed congestion unrealistically. */
    public static final double CAPACITY_STEP_KW          = p("capacityStepKW", 0.0);
    // EV logistic adoption (AL S-curve): share of the neighbourhood car fleet that is electric.
    public static final double EV_SHARE_2050    = p("evShare2050", 0.85);
    public static final double EV_MIDPOINT_YEAR = p("evMidpointYear", 2035.0);
    public static final double EV_STEEPNESS     = p("evSteepness", 0.30);
    // Share of congested neighbourhoods the DSO can reinforce per year, by scenario rate.
    public static final Map<String, Double> GRID_REINFORCE_RATE =
            Map.of("SLOW", p("grrSlow", 0.05), "MEDIUM", p("grrMedium", 0.15), "FAST", p("grrFast", 0.40));
    /** Knock-out: disable the congestion mechanism entirely (grids never constrain). */
    public static final boolean CONGESTION_OFF = p("congestionOff", 0.0) != 0.0;

    // ---- Adopter segmentation (Segments.java) --------------------------------------------------
    // Weights of the adoption-propensity index behind the Rogers categories.
    public static final double SEG_W_ATTITUDE = p("segWAttitude", 0.5);
    public static final double SEG_W_NETWORK  = p("segWNetwork", 0.3);
    public static final double SEG_W_LABEL    = p("segWLabel", 0.2);

    // Stochastic equipment lifetime: each installed system's end-of-life age is drawn ~ N(lifetime,
    // sd) clamped to lifetime +/- maxDev, redrawn on every (re)install, for ALL heating methods. This
    // smears the deterministic-lifetime cohorts (e.g. the HOA end-of-life "echo" where a whole cohort
    // re-decides in lockstep). Set -Dht.lifetimeJitterSd=0 to recover the AnyLogic-faithful
    // deterministic lifetime.
    public static final double LIFETIME_JITTER_SD  = p("lifetimeJitterSd", 1.0);
    public static final int    LIFETIME_JITTER_MAX = (int) p("lifetimeJitterMax", 3.0);

    // ---- Energy prices + fixed connection costs (EnergyPrices) ----------------------------------
    // data/reference/energy_prices_by_year.csv holds, per year and per arm, the retail price split
    // into commodity + tax, and the annual connection charges. It is required: a missing or
    // malformed table aborts the run.
    //
    // priceT selects the arm continuously so an LHS can sweep it: 0 = low (gas low / elec high),
    // 0.5 = central, 1 = high (gas high / elec low).
    /** Override the scenario's energy-price arm. Negative (the default) = use the scenario's own
     *  energy_prices column, so the matrix controls it; 0/0.5/1 forces low/central/high. */
    public static final double PRICE_T = p("priceT", -1.0);
    /** Override the scenario's DH planning+construction time, in years. Negative = use the
     *  scenario value. Together with dhDensityThreshold this is the rollout speed/extent knob. */
    public static final double DH_CONSTRUCTION_YEARS = p("dhConstructionYears", -1);
    /** A dwelling on district heating has no end of life of its own (the network and delivery set
     *  belong to the operator). Instead it reviews its heating every this many years since it
     *  connected, and leaves only if another option wins including the disconnection fee.
     *  Default 5; a sensitivity parameter. */
    public static final int DH_REVIEW_YEARS = (int) p("dhReviewYears", 5);
    /** Drop the fixed connection charges from the EAC (isolates the price-path change on its own). */
    public static final boolean FIXED_COSTS_OFF = p("fixedCostsOff", 0.0) != 0.0;

    // Techno-economic sensitivity multipliers (default 1.0 = data as-is).
    //   capexMultHp  scales the investment cost of the heat pumps (hybrid + electric)
    //   capexMultDh  scales the household connection cost of district heating, so the collective
    //                route carries the same cost uncertainty as the individual ones. (The network
    //                business-case threshold in DistrictHeating is a separate parameter.)
    //   learningRateMult scales EVERY technology's economic_learning_rate (the capex-fall speed).
    public static final double CAPEX_MULT_HP     = p("capexMultHp", 1.0);
    public static final double CAPEX_MULT_DH     = p("capexMultDh", 1.0);
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
