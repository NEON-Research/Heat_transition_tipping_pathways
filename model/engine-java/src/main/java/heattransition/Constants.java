package heattransition;

import java.util.Map;

/** Calibration constants, extracted verbatim from the AnyLogic model.
 *  Mirrors ../../tests/heat_model_ref.py and the JS engine's constants.js. */
public final class Constants {
    private Constants() {}

    // Startup_agent.f_setDefaultWeights()
    public static final double WEIGHT_AFFORDABILITY_TO_PBC = 0.5;
    public static final double WEIGHT_EFFORT_TO_PBC        = 0.2;
    public static final double WEIGHT_ATTITUDE_TO_INTENTION = 0.5;
    public static final double WEIGHT_PBC_TO_INTENTION      = 0.5;
    public static final double WEIGHT_SOCIALNORM_TO_INTENTION = 0.5;
    public static final double WEIGHT_INTENTION_TO_BEHAVIOR = 0.5;
    public static final double WEIGHT_PBC_TO_BEHAVIOR       = 0.5;

    // Startup_agent.f_initializeMain()
    public static final double GUMBEL_SCALE_UTIL = 0.02;
    public static final double GUMBEL_SCALE_EAC  = 20;

    // J_HeatingSystemOptionsGlobal salience S-curve constants
    public static final double SALIENCE_K = 10.0;
    public static final double SALIENCE_THRESHOLD = 0.3;
    public static final double SALIENCE_STEEPNESS = 30.0;

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
