package heattransition;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.Map;

/** District heating: ACM connection / disconnection fees in the EAC, and the periodic review that
 *  replaces the end of life for a dwelling on district heating. */
class DistrictHeatingCostsTest {

    private static Dwelling dwelling(HeatingSystem current) {
        return new Dwelling(0, current, 0, 10000, "c", 80, "TERRACED", false, true, 0.5);
    }
    private static double annuity(HeatingSystemSpec h) {
        return (1 - Math.pow(1 + h.discountRate, -h.lifetime)) / h.discountRate;
    }
    private static final Economics.InsulationCost NO_INSULATION = (d, l) -> 0;

    @Test void acm2025Values() {
        assertEquals(5251.45, DistrictHeatingCosts.connectionFee(), 1e-9);
        assertEquals(5250.99, DistrictHeatingCosts.disconnectionFee(), 1e-9);
        assertEquals(577.48 + 32.80 + 150.49, DistrictHeatingCosts.annualFixed(), 1e-9);
    }

    @Test void connectedDwellingPaysNoConnectionFeeOrSubsidy() {
        Map<HeatingSystem, HeatingSystemSpec> hs = HeatingSystemData.freshSpecs();
        HeatingSystemSpec dh = hs.get(HeatingSystem.DISTRICT_HEATING);
        long newcomer = Economics.computeEAC(dh, dwelling(HeatingSystem.NATURAL_GAS_BOILER), NO_INSULATION);
        long connected = Economics.computeEAC(dh, dwelling(HeatingSystem.DISTRICT_HEATING), NO_INSULATION);
        double expected = (DistrictHeatingCosts.connectionFee() - dh.subsidy) / annuity(dh);
        assertEquals(expected, newcomer - connected, 1.0);
    }

    @Test void leavingDistrictHeatingAddsTheDisconnectionFeeAndExitBarrier() {
        Map<HeatingSystem, HeatingSystemSpec> hs = HeatingSystemData.freshSpecs();
        for (HeatingSystem t : new HeatingSystem[]{ HeatingSystem.NATURAL_GAS_BOILER,
                HeatingSystem.HYBRID_HEAT_PUMP, HeatingSystem.ELECTRIC_HEAT_PUMP }) {
            HeatingSystemSpec h = hs.get(t);
            long fromGas = Economics.computeEAC(h, dwelling(HeatingSystem.NATURAL_GAS_BOILER), NO_INSULATION);
            long fromDH  = Economics.computeEAC(h, dwelling(HeatingSystem.DISTRICT_HEATING), NO_INSULATION);
            double oneOff = DistrictHeatingCosts.disconnectionFee() + Constants.DH_EXIT_BARRIER;
            assertEquals(oneOff / annuity(h), fromDH - fromGas, 1.0, t.name());
        }
    }

    @Test void exitBarrierDefault() {
        assertEquals(0, Constants.DH_EXIT_BARRIER, 1e-9);     // baseline: cost-driven, may leave
    }

    @Test void reviewTrigger() {
        assertFalse(Decision.hasDhReviewTrigger(0, 5));    // not in the year it connected
        assertFalse(Decision.hasDhReviewTrigger(4, 5));
        assertTrue(Decision.hasDhReviewTrigger(5, 5));
        assertFalse(Decision.hasDhReviewTrigger(7, 5));
        assertTrue(Decision.hasDhReviewTrigger(10, 5));
        assertTrue(Decision.hasDhReviewTrigger(31, 1));    // every year
        assertFalse(Decision.hasDhReviewTrigger(10, 0));   // 0 = never review
    }
}
