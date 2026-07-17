package heattransition;

import java.util.EnumMap;
import java.util.Map;

/** REAL values exported from the AnyLogic HEATING_SYSTEM_DATA / ENERGY_SOURCE_DATA tables
 *  (database/db.script). Costs EUR, demand kWh. Energy domestic costs (EUR/kWh):
 *  NATURAL_GAS 0.14, ELECTRICITY 0.32, HEAT 0.14. requiresLowTemp = (REQUIRED_DIST == 'LT'). */
public final class HeatingSystemData {
    private HeatingSystemData() {}
    private static final double GAS = 0.14, ELEC = 0.32, HEAT = 0.14;

    /** Fresh set of specs (mutable state per simulation, so build a new one per run). */
    public static Map<HeatingSystem, HeatingSystemSpec> freshSpecs() {
        Map<HeatingSystem, HeatingSystemSpec> m = new EnumMap<>(HeatingSystem.class);
        m.put(HeatingSystem.NATURAL_GAS_BOILER, new HeatingSystemSpec(
                HeatingSystem.NATURAL_GAS_BOILER, 1500, 2250, 3000, 0, false, "no",
                0, 12, 0.02, 0.01, "NATURAL_GAS", "NOT_APPLICABLE",
                0.99, 0, 1.0, 0, GAS, 0, 0, 1, 0.5));
        m.put(HeatingSystem.NATURAL_GAS_BLOCK, new HeatingSystemSpec(
                HeatingSystem.NATURAL_GAS_BLOCK, 1500, 2250, 3000, 0, false, "no",
                0, 12, 0.02, 0.01, "NATURAL_GAS", "NOT_APPLICABLE",
                0.99, 0, 1.0, 0, GAS, 0, 0, 1, 0.5));
        m.put(HeatingSystem.HYBRID_HEAT_PUMP, new HeatingSystemSpec(
                HeatingSystem.HYBRID_HEAT_PUMP, 5000, 6000, 7000, 5000, true, "c",
                0, 15, 0.02, 0.05, "ELECTRICITY", "NATURAL_GAS",
                3.0, 0.99, 0.8, 0.2, ELEC, GAS, 4000, 3, 1.0));
        m.put(HeatingSystem.ELECTRIC_HEAT_PUMP, new HeatingSystemSpec(
                HeatingSystem.ELECTRIC_HEAT_PUMP, 7500, 9000, 12000, 5000, true, "b",
                0, 15, 0.02, 0.1, "ELECTRICITY", "NOT_APPLICABLE",
                3.0, 0, 1.0, 0, ELEC, 0, 4000, 5, 1.0));
        m.put(HeatingSystem.DISTRICT_HEATING, new HeatingSystemSpec(
                HeatingSystem.DISTRICT_HEATING, 5250.24, 5250.24, 5250.24, 0, false, "no",
                0, 30, 0.03, 0.05, "HEAT", "NOT_APPLICABLE",
                0.8, 0, 1.0, 0, HEAT, 0, 3775, 3, 0.5));
        // min-max sustainability normalization across all systems (AL f_setHeatingSystemOptions)
        int minS = Integer.MAX_VALUE, maxS = Integer.MIN_VALUE;
        for (HeatingSystemSpec sp : m.values()) { minS = Math.min(minS, sp.sustainabilityScore); maxS = Math.max(maxS, sp.sustainabilityScore); }
        for (HeatingSystemSpec sp : m.values()) sp.sustainabilityScoreNorm = (double) (sp.sustainabilityScore - minS) / (maxS - minS);
        return m;
    }
}
