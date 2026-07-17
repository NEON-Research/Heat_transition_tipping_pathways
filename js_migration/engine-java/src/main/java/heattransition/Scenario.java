package heattransition;

/** Scenario switches (subset used by the homeowner core), from _scenario_settings.csv. */
public final class Scenario {
    public int scenId = 1;
    public String scenName = "baseline";
    public String socialLearningFactor = "MEDIUM";
    public String economicLearningFactor = "MEDIUM";
    public String gridReinforcementRate = "MEDIUM";
    public int dhConstructionTime = 5;
    public String dhExpansionStrategy = "COST_BASED";
    public String shaStrategy = "COST_BASED";
    public boolean dhConnectionObligation = false;
    public boolean gridCongestionHpBan = false;

    public Scenario() {}
    public Scenario(int id, String name, String slf, String elf) {
        this.scenId = id; this.scenName = name; this.socialLearningFactor = slf; this.economicLearningFactor = elf;
    }
}
