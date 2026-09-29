package heattransition;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.EnumMap;
import java.util.Map;

/** Subsidies: amount per year / technology / ownership, the three policies, and its use in the EAC. */
class SubsidiesTest {

    // Subsidies holds per-simulation state (policy, year) and other tests run simulations, so every
    // test starts from a known state and leaves the default behind.
    @BeforeEach @AfterEach void reset() { Subsidies.start("CURRENT", 2024); Subsidies.setYear(2024, Map.of()); }

    private static Map<HeatingSystem, Double> shares(double ehp) {
        Map<HeatingSystem, Double> m = new EnumMap<>(HeatingSystem.class);
        for (HeatingSystem t : HeatingSystem.values()) m.put(t, 0.0);
        m.put(HeatingSystem.ELECTRIC_HEAT_PUMP, ehp);
        return m;
    }

    @Test void currentFollowsTheTableAndHoldsTheLastYear() {
        Subsidies.start("CURRENT", 2022);
        Subsidies.setYear(2022, shares(0));
        assertEquals(2550, Subsidies.amount(HeatingSystem.HYBRID_HEAT_PUMP, "PRIVATELY_OWNED"), 1e-9);
        assertEquals(3325, Subsidies.amount(HeatingSystem.DISTRICT_HEATING, "PRIVATELY_OWNED"), 1e-9);
        Subsidies.setYear(2024, shares(0));
        assertEquals(3775, Subsidies.amount(HeatingSystem.DISTRICT_HEATING, "PRIVATELY_OWNED"), 1e-9);
        Subsidies.setYear(2040, shares(0));                       // after the table: 2025 held
        assertEquals(2825, Subsidies.amount(HeatingSystem.ELECTRIC_HEAT_PUMP, "PRIVATELY_OWNED"), 1e-9);
        assertEquals(0, Subsidies.amount(HeatingSystem.ELECTRIC_HEAT_PUMP, "SOCIAL_HOUSING"), 1e-9);
        assertEquals(0, Subsidies.amount(HeatingSystem.NATURAL_GAS_BOILER, "PRIVATELY_OWNED"), 1e-9);
    }

    @Test void noneGivesNothing() {
        Subsidies.start("NONE", 2022);
        Subsidies.setYear(2023, shares(0));
        assertEquals(0, Subsidies.amount(HeatingSystem.ELECTRIC_HEAT_PUMP, "PRIVATELY_OWNED"), 1e-9);
    }

    @Test void phaseOutStopsForGoodAtTheThreshold() {
        Subsidies.start("PHASE_OUT", 2022);
        Subsidies.setYear(2030, shares(Constants.SUBSIDY_PHASE_OUT_SHARE - 0.01));
        assertEquals(2825, Subsidies.amount(HeatingSystem.ELECTRIC_HEAT_PUMP, "PRIVATELY_OWNED"), 1e-9);
        var now = Subsidies.setYear(2031, shares(Constants.SUBSIDY_PHASE_OUT_SHARE));
        assertTrue(now.contains(HeatingSystem.ELECTRIC_HEAT_PUMP));
        assertEquals(0, Subsidies.amount(HeatingSystem.ELECTRIC_HEAT_PUMP, "PRIVATELY_OWNED"), 1e-9);
        Subsidies.setYear(2032, shares(0));                       // share falls back: stays phased out
        assertEquals(0, Subsidies.amount(HeatingSystem.ELECTRIC_HEAT_PUMP, "PRIVATELY_OWNED"), 1e-9);
        assertEquals(1925, Subsidies.amount(HeatingSystem.HYBRID_HEAT_PUMP, "PRIVATELY_OWNED"), 1e-9);
    }

    @Test void subsidyLowersTheEacByItsAnnuity() {
        var h = HeatingSystemData.freshSpecs().get(HeatingSystem.ELECTRIC_HEAT_PUMP);
        Dwelling d = new Dwelling(0, HeatingSystem.NATURAL_GAS_BOILER, 0, 10000, "c", 80, "TERRACED", false, false, 0.5);
        Economics.InsulationCost none = (dd, l) -> 0;
        d.ownership = "SOCIAL_HOUSING";                 // no heat-pump subsidy
        long without = Economics.computeEAC(h, d, none);
        d.ownership = "PRIVATELY_OWNED";                // ISDE 3150 in 2024
        long with = Economics.computeEAC(h, d, none);
        double af = (1 - Math.pow(1 + h.discountRate, -h.lifetime)) / h.discountRate;
        assertEquals(3150 / af, without - with, 1.0);
    }
}
