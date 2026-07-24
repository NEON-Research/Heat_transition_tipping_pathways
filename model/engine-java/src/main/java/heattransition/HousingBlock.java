package heattransition;

import java.util.ArrayList;
import java.util.List;

/** A social-housing block: households that share one heating system and decide together
 *  (whole block switches at once on cost, at end-of-life). Mirrors J_HousingBlock. */
public final class HousingBlock {
    public final String buurt;
    public String blockType = "SOCIAL";   // "SOCIAL" or "HOA" -- set by the loader (for probes/reporting)
    public final List<Dwelling> households = new ArrayList<>();
    public HeatingSystem currentType = HeatingSystem.NATURAL_GAS_BOILER;
    public int age;
    public String energyLabel = "n";
    public int yearLastRenovation = 1980;
    public boolean hasLowTemp = false;
    public HousingBlock(String buurt, int age) { this.buurt = buurt; this.age = age; }
}
