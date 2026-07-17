package heattransition;

/** Pure decision functions, transcribed from J_HomeOwner / J_HeatingSystemOptionsGlobal.
 *  Mirrors ../../tests/heat_model_ref.py and the JS engine's decision.js exactly.
 *  No state, no side effects — trivially unit-testable. */
public final class Decision {
    private Decision() {}

    public static double normalizedValue(double v, double mn, double mx) { return (v - mn) / (mx - mn); }

    /** attitude = 1 - |householdAttitude - technologySustainabilityScoreNorm| */
    public static double attitudeValue(double att, double scoreNorm) { return 1 - Math.abs(att - scoreNorm); }

    /** effort: 0.2 same system | 0.8 needs low-temp retrofit | 0.5 otherwise */
    public static double effort(boolean typeIsCurrent, boolean requiresLowTemp, boolean dwellingHasLowTemp) {
        if (typeIsCurrent) return 0.2;
        if (requiresLowTemp && !dwellingHasLowTemp) return 0.8;
        return 0.5;
    }

    /** PBC = weighted mean of (1-EAC_norm) and (1-effort) */
    public static double pbc(double eacNorm, double effortVal, double wEac, double wEffort) {
        return ((1 - eacNorm) * wEac + (1 - effortVal) * wEffort) / (wEac + wEffort);
    }
    public static double pbc(double eacNorm, double effortVal) {
        return pbc(eacNorm, effortVal, Constants.WEIGHT_AFFORDABILITY_TO_PBC, Constants.WEIGHT_EFFORT_TO_PBC);
    }

    /** subjectiveNorm = min(1, peerShare * (1 + salience)) */
    public static double subjectiveNorm(int peerCount, int networkSize, double salience) {
        return Math.min(1.0, ((double) peerCount / networkSize) * (1 + salience));
    }

    /** intention; weightSN = socialLearningFactor * weight_socialNormToIntention (denominator);
     *  per-technology socialLearningRate multiplies only the numerator SN term. */
    public static double intention(double att, double sn, double pbcVal,
                                   double socialLearningFactor, double perTechRate,
                                   double wAtt, double wSnBase, double wPbcToInt) {
        double wSn = socialLearningFactor * wSnBase;
        double num = att * wAtt + sn * wSn * perTechRate + pbcVal * wPbcToInt;
        return num / (wAtt + wSn + wPbcToInt);
    }
    public static double intention(double att, double sn, double pbcVal,
                                   double socialLearningFactor, double perTechRate) {
        return intention(att, sn, pbcVal, socialLearningFactor, perTechRate,
                Constants.WEIGHT_ATTITUDE_TO_INTENTION, Constants.WEIGHT_SOCIALNORM_TO_INTENTION,
                Constants.WEIGHT_PBC_TO_INTENTION);
    }

    /** perceived utility = weighted mean of intention and PBC */
    public static double perceivedUtility(double intentionVal, double pbcVal, double wInt, double wPbc) {
        return (intentionVal * wInt + pbcVal * wPbc) / (wInt + wPbc);
    }
    public static double perceivedUtility(double intentionVal, double pbcVal) {
        return perceivedUtility(intentionVal, pbcVal,
                Constants.WEIGHT_INTENTION_TO_BEHAVIOR, Constants.WEIGHT_PBC_TO_BEHAVIOR);
    }

    /** salience factor: novelty S-curve blended with decay by momentum */
    public static double salienceFactor(double currentShare, double previousShare,
                                        double k, double threshold, double steepness) {
        double novelty = 1 / (1 + Math.exp(k * (currentShare - threshold)));
        double momentum = 1 / (1 + Math.exp(-steepness * (currentShare - previousShare)));
        return momentum * novelty + (1 - momentum) * currentShare;
    }
    public static double salienceFactor(double currentShare, double previousShare) {
        return salienceFactor(currentShare, previousShare,
                Constants.SALIENCE_K, Constants.SALIENCE_THRESHOLD, Constants.SALIENCE_STEEPNESS);
    }

    /** learning-curve capex multiplier = (1 - rate*policy)^doublings, 1.0 if not growing */
    public static double capexLearningFactor(double units, double initialUnits,
                                             double learningRate, double policyMultiplier) {
        if (initialUnits == 0) initialUnits = 1;
        if (units > initialUnits) {
            double rate = learningRate * policyMultiplier;
            double doublings = Math.log(units / initialUnits) / Math.log(2);
            return Math.pow(1 - rate, doublings);
        }
        return 1.0;
    }

    // --- Triggers -----------------------------------------------------------
    public static boolean hasEndOfLifeTrigger(int age, int lifetime) { return age >= lifetime; }

    /** CORRECTED opportunity trigger: real division -> fires past 75% of life.
     *  legacyIntDivision=true reproduces the original AnyLogic integer-division bug
     *  (only fired at age>=lifetime) for validating a faithful port vs the old golden. */
    public static boolean hasOpportunityTrigger(int age, int lifetime, boolean legacyIntDivision) {
        if (legacyIntDivision) return (age / lifetime) > 0.75;   // int/int — the original bug
        return ((double) age / lifetime) > 0.75;                 // corrected
    }

    /** RUM score = utility + scale * Gumbel noise (choose argmax over feasible options). */
    public static double rumScore(double utility, double gumbelNoise, double scale) {
        return utility + scale * gumbelNoise;
    }
    public static double rumScore(double utility, double gumbelNoise) {
        return rumScore(utility, gumbelNoise, Constants.GUMBEL_SCALE_UTIL);
    }
}
