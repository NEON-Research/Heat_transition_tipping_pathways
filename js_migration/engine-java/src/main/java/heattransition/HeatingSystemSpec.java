package heattransition;

/** Per-technology economics + behaviour. Mutable fields (invest*, salienceFactor, min/maxEAC)
 *  change during a run via the learning curve and salience feedback; the initial* are frozen. */
public final class HeatingSystemSpec {
    public final HeatingSystem type;
    public double investSmall, investMedium, investLarge;
    public final double initialInvestSmall, initialInvestMedium, initialInvestLarge;
    public final double distributionSystemCost;
    public final boolean requiresLowTemp;
    public final String requiredLabel;
    public final double maintenance;
    public final int lifetime;
    public final double discountRate;
    public final double economicLearningRate;
    public final String primarySource, secondarySource;
    public final double efficiencyPrimary, efficiencySecondary, fractionPrimary, fractionSecondary;
    public final double primaryCostPerKWh, secondaryCostPerKWh;
    public final double subsidy;
    public final int sustainabilityScore;
    public final double socialLearningRate;

    public double sustainabilityScoreNorm;
    public double salienceFactor = 0;
    public double minEAC = Double.POSITIVE_INFINITY, maxEAC = Double.NEGATIVE_INFINITY;
    public double initialUnits = 1;

    public HeatingSystemSpec(HeatingSystem type, double investSmall, double investMedium, double investLarge,
                             double distributionSystemCost, boolean requiresLowTemp, String requiredLabel,
                             double maintenance, int lifetime, double discountRate, double economicLearningRate,
                             String primarySource, String secondarySource, double efficiencyPrimary,
                             double efficiencySecondary, double fractionPrimary, double fractionSecondary,
                             double primaryCostPerKWh, double secondaryCostPerKWh, double subsidy,
                             int sustainabilityScore, double socialLearningRate) {
        this.type = type;
        this.investSmall = investSmall; this.investMedium = investMedium; this.investLarge = investLarge;
        this.initialInvestSmall = investSmall; this.initialInvestMedium = investMedium; this.initialInvestLarge = investLarge;
        this.distributionSystemCost = distributionSystemCost;
        this.requiresLowTemp = requiresLowTemp; this.requiredLabel = requiredLabel;
        this.maintenance = maintenance; this.lifetime = lifetime; this.discountRate = discountRate;
        this.economicLearningRate = economicLearningRate;
        this.primarySource = primarySource; this.secondarySource = secondarySource;
        this.efficiencyPrimary = efficiencyPrimary; this.efficiencySecondary = efficiencySecondary;
        this.fractionPrimary = fractionPrimary; this.fractionSecondary = fractionSecondary;
        this.primaryCostPerKWh = primaryCostPerKWh; this.secondaryCostPerKWh = secondaryCostPerKWh;
        this.subsidy = subsidy; this.sustainabilityScore = sustainabilityScore;
        this.socialLearningRate = socialLearningRate;
        this.sustainabilityScoreNorm = (double) sustainabilityScore / Constants.MAX_SUSTAINABILITY_SCORE;
    }
}
