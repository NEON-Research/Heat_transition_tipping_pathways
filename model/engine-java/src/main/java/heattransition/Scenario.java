package heattransition;

/** Scenario switches, from the AnyLogic scenario matrix (= JS SCEN table). Learning factors and
 *  the DH/SHA strategy + obligation/congestion flags. Use {@link #byName(String)} to build one. */
public final class Scenario {
    public int scenId = 1;
    public String scenName = "baseline";
    public String socialLearningFactor = "MEDIUM";
    public String economicLearningFactor = "MEDIUM";
    public String gridReinforcementRate = "MEDIUM";
    public int dhConstructionTime = 5;
    public String dhExpansionStrategy = "COST_BASED";   // or POLICY_BASED
    public String shaStrategy = "COST_BASED";           // or POLICY_BASED (social-housing strategy)
    public boolean dhConnectionObligation = false;
    public boolean gridCongestionHpBan = false;

    public Scenario() {}
    public Scenario(int id, String name, String slf, String elf) {
        this.scenId = id; this.scenName = name; this.socialLearningFactor = slf; this.economicLearningFactor = elf;
    }

    /** All 16 scenario names, in AnyLogic order (scen_id 11 is intentionally skipped, as in AL). */
    public static final String[] NAMES = {
        "baseline",
        "social_learning_factor_low", "social_learning_factor_high",
        "economic_learning_factor_low", "economic_learning_factor_high",
        "policy_driven_dh_strategy", "policy_driven_sha_strategy", "actor_allignment_strategy",
        "grid_congestion_HP_ban", "dh_policy_based_connection_obligation",
        "individual_technologies", "collective_technologies",
        "individual_tech_grid_congestion_ban", "collective_tech_grid_congestion_ban",
        "individual_tech_dh_connection_obligation", "collective_tech_dh_connection_obligation",
    };

    /** Build the scenario for a name (mirrors JS runFull SCEN). Throws on an unknown name. */
    public static Scenario byName(String name) {
        Scenario s = new Scenario();
        s.scenName = name;
        switch (name) {
            case "baseline":                                 s.scenId = 1;  break;
            case "social_learning_factor_low":               s.scenId = 2;  s.socialLearningFactor = "LOW";  break;
            case "social_learning_factor_high":              s.scenId = 3;  s.socialLearningFactor = "HIGH"; break;
            case "economic_learning_factor_low":             s.scenId = 4;  s.economicLearningFactor = "LOW";  break;
            case "economic_learning_factor_high":            s.scenId = 5;  s.economicLearningFactor = "HIGH"; break;
            case "policy_driven_dh_strategy":                s.scenId = 6;  s.dhExpansionStrategy = "POLICY_BASED"; break;
            case "policy_driven_sha_strategy":               s.scenId = 7;  s.shaStrategy = "POLICY_BASED"; break;
            case "actor_allignment_strategy":                s.scenId = 8;  s.dhExpansionStrategy = "POLICY_BASED"; s.shaStrategy = "POLICY_BASED"; break;
            case "grid_congestion_HP_ban":                   s.scenId = 9;  s.gridCongestionHpBan = true; break;
            case "dh_policy_based_connection_obligation":    s.scenId = 10; s.dhExpansionStrategy = "POLICY_BASED"; s.dhConnectionObligation = true; break;
            case "individual_technologies":                  s.scenId = 12; s.socialLearningFactor = "HIGH"; s.economicLearningFactor = "HIGH"; break;
            case "collective_technologies":                  s.scenId = 13; s.socialLearningFactor = "LOW";  s.economicLearningFactor = "LOW";  s.dhExpansionStrategy = "POLICY_BASED"; s.shaStrategy = "POLICY_BASED"; break;
            case "individual_tech_grid_congestion_ban":      s.scenId = 14; s.socialLearningFactor = "HIGH"; s.economicLearningFactor = "HIGH"; s.gridCongestionHpBan = true; break;
            case "collective_tech_grid_congestion_ban":      s.scenId = 15; s.socialLearningFactor = "LOW";  s.economicLearningFactor = "LOW";  s.dhExpansionStrategy = "POLICY_BASED"; s.shaStrategy = "POLICY_BASED"; s.gridCongestionHpBan = true; break;
            case "individual_tech_dh_connection_obligation": s.scenId = 16; s.socialLearningFactor = "HIGH"; s.economicLearningFactor = "HIGH"; s.dhConnectionObligation = true; break;
            case "collective_tech_dh_connection_obligation": s.scenId = 17; s.socialLearningFactor = "LOW";  s.economicLearningFactor = "LOW";  s.dhExpansionStrategy = "POLICY_BASED"; s.shaStrategy = "POLICY_BASED"; s.dhConnectionObligation = true; break;
            default: throw new IllegalArgumentException("unknown scenario: " + name
                + " (known: " + String.join(", ", NAMES) + ")");
        }
        return s;
    }
}
