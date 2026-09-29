package heattransition;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.Map;

/** Heat pumps are priced by capacity (heat_pump_capex.csv): fixed + per kW, with kW from the heat
 *  demand and the full-load hours, clamped to the unit range. The gas boiler keeps its demand tiers. */
class HeatPumpCapexTest {

    private static final double TERRACED_MEDIAN = 9772;   // kWh, the dwelling Milieu Centraal prices

    @Test void anchorsHitMilieuCentraalPrices() {
        Map<HeatingSystem, HeatingSystemSpec> hs = HeatingSystemData.freshSpecs();
        // 8 kW electric = 12,000; 4 kW hybrid incl. new boiler = 8,300 (within rounding of 1,220 h)
        assertEquals(12000, Economics.investmentCostsSupply(hs.get(HeatingSystem.ELECTRIC_HEAT_PUMP), TERRACED_MEDIAN), 15);
        assertEquals(8300, Economics.investmentCostsSupply(hs.get(HeatingSystem.HYBRID_HEAT_PUMP), TERRACED_MEDIAN), 15);
    }

    @Test void costRisesWithDemandAndIsClamped() {
        Map<HeatingSystem, HeatingSystemSpec> hs = HeatingSystemData.freshSpecs();
        HeatingSystemSpec ehp = hs.get(HeatingSystem.ELECTRIC_HEAT_PUMP), hhp = hs.get(HeatingSystem.HYBRID_HEAT_PUMP);
        assertTrue(Economics.investmentCostsSupply(ehp, 15000) > Economics.investmentCostsSupply(ehp, 10000));
        // below the minimum unit size the price no longer falls
        assertEquals(Economics.investmentCostsSupply(ehp, 1000), Economics.investmentCostsSupply(ehp, 2000), 1e-9);
        assertEquals(ehp.investFixed + ehp.investPerKw * ehp.kwMin, Economics.investmentCostsSupply(ehp, 1000), 1e-9);
        // the hybrid stops at its largest unit (6 kW)
        assertEquals(hhp.investFixed + hhp.investPerKw * 6, Economics.investmentCostsSupply(hhp, 40000), 1e-9);
    }

    @Test void gasBoilerKeepsDemandTiers() {
        HeatingSystemSpec gas = HeatingSystemData.freshSpecs().get(HeatingSystem.NATURAL_GAS_BOILER);
        assertTrue(Double.isNaN(gas.investFixed));
        assertEquals(gas.investSmall, Economics.investmentCostsSupply(gas, 5000), 1e-9);
        assertEquals(gas.investMedium, Economics.investmentCostsSupply(gas, 8000), 1e-9);
        assertEquals(gas.investLarge, Economics.investmentCostsSupply(gas, 12000), 1e-9);
    }
}
