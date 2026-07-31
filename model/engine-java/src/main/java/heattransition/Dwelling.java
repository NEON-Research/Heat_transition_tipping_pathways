package heattransition;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** A dwelling. Mutable state: currentType, age, peer network, and (via exogenous insulation)
 *  energyLabel / hasLowTemp / yearLastRenovation. */
public final class Dwelling {
    public final int id;
    public HeatingSystem currentType;
    public int age;
    public int lifeDraw;                          // stochastic end-of-life age for the CURRENT system
    public final double heatDemandKWh;          // frozen at initial label (AL does not recompute)
    public String energyLabel;                  // mutable: exogenous insulation upgrades it
    public final double livingAreaM2;
    public final String dwellingType;
    public boolean hasLowTemp;                   // mutable: true once label reaches a/b
    // mutable: DistrictHeatingCompany.f_DHExpansion sets this true when the neighbourhood's
    // grid goes live. Was final while the grid was treated as static.
    public boolean hasDistrictHeatingGrid;
    public boolean hasGridCongestion = false;    // set per-year by the DSO/congestion model
    public final double attitude;

    public String archetype = "TERRACED";
    public int constructionYear = 1980;
    public int yearLastRenovation = 1980;

    public List<Dwelling> network = new ArrayList<>();
    public Map<HeatingSystem, Integer> peerCounts = new EnumMap<>(HeatingSystem.class);

    public String ownership = "PRIVATELY_OWNED";
    public double insulToB = 0, insulToC = 0;
    public String numid = "";
    public String buurt = "NA";
    public int category = 0, networkSize = 0;

    public Dwelling(int id, HeatingSystem currentType, int age, double heatDemandKWh, String energyLabel,
                    double livingAreaM2, String dwellingType, boolean hasLowTemp,
                    boolean hasDistrictHeatingGrid, double attitude) {
        this.id = id; this.currentType = currentType; this.age = age;
        this.heatDemandKWh = heatDemandKWh; this.energyLabel = energyLabel;
        this.livingAreaM2 = livingAreaM2; this.dwellingType = dwellingType;
        this.hasLowTemp = hasLowTemp; this.hasDistrictHeatingGrid = hasDistrictHeatingGrid;
        this.attitude = attitude;
    }
}
