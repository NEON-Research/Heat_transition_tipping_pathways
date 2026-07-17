package heattransition;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.List;

/** JUnit 5 mirror of ../../tests/test_heat_model_spec.py against the Java engine. */
class DecisionTest {
    static final double T = 1e-9;

    @Test void attitude() {
        assertEquals(0.8, Decision.attitudeValue(0.7, 0.9), T);
        assertEquals(1.0, Decision.attitudeValue(0.42, 0.42), T);
        assertEquals(0.0, Decision.attitudeValue(0.0, 1.0), T);
    }
    @Test void effort() {
        assertEquals(0.2, Decision.effort(true, true, false), T);
        assertEquals(0.8, Decision.effort(false, true, false), T);
        assertEquals(0.5, Decision.effort(false, true, true), T);
        assertEquals(0.5, Decision.effort(false, false, false), T);
    }
    @Test void pbc() {
        assertEquals(0.475 / 0.7, Decision.pbc(0.25, 0.5), T);
        assertEquals(1.0, Decision.pbc(0.0, 0.0), T);
        assertEquals(0.0, Decision.pbc(1.0, 1.0), T);
    }
    @Test void subjectiveNorm() {
        assertEquals(0.6, Decision.subjectiveNorm(4, 10, 0.5), T);
        assertEquals(1.0, Decision.subjectiveNorm(8, 10, 1.0), T);
        assertEquals(0.3, Decision.subjectiveNorm(3, 10, 0.0), T);
    }
    @Test void intentionMediumSlf() {
        double p = 0.475 / 0.7;
        assertEquals((0.8 * 0.5 + 0.6 * 0.5 * 0.3 + p * 0.5) / 1.5,
                Decision.intention(0.8, 0.6, p, 1.0, 0.3), T);
    }
    @Test void intentionHighSlf() {
        assertEquals((0.8 * 0.5 + 0.6 * 1.0 * 0.3 + 0.5 * 0.5) / (0.5 + 1.0 + 0.5),
                Decision.intention(0.8, 0.6, 0.5, 2.0, 0.3), T);
    }
    @Test void perceivedUtility() { assertEquals(0.6, Decision.perceivedUtility(0.4, 0.8), T); }
    @Test void salienceFlat() {
        double novelty = 1 / (1 + Math.exp(10 * (0.2 - 0.3)));
        assertEquals(0.5 * novelty + 0.5 * 0.2, Decision.salienceFactor(0.2, 0.2), T);
    }
    @Test void learningCurve() {
        assertEquals(0.81, Decision.capexLearningFactor(4000, 1000, 0.1, 1.0), T);
        assertEquals(0.64, Decision.capexLearningFactor(4000, 1000, 0.1, 2.0), T);
        assertEquals(1.0, Decision.capexLearningFactor(1000, 1000, 0.1, 1.0), T);
    }
    @Test void opportunityTriggerCorrected() {
        assertFalse(Decision.hasOpportunityTrigger(11, 15, false));
        assertTrue(Decision.hasOpportunityTrigger(12, 15, false));
        assertTrue(Decision.hasOpportunityTrigger(14, 15, false));
    }
    @Test void opportunityTriggerLegacyBug() {
        assertFalse(Decision.hasOpportunityTrigger(14, 15, true)); // 93% but false
        assertTrue(Decision.hasOpportunityTrigger(15, 15, true));
    }
    @Test void endOfLife() {
        assertFalse(Decision.hasEndOfLifeTrigger(14, 15));
        assertTrue(Decision.hasEndOfLifeTrigger(15, 15));
    }
    @Test void eacPositive() {
        Dwelling d = new Dwelling(0, HeatingSystem.NATURAL_GAS_BOILER, 5, 9000, "d", 100, "HOUSE", false, false, 0.7);
        long eac = Economics.computeEAC(HeatingSystemData.freshSpecs().get(HeatingSystem.NATURAL_GAS_BOILER), d, (dd, l) -> 0);
        assertTrue(eac > 0);
    }
    @Test void endToEndConservesStock() {
        List<Dwelling> stock = SyntheticData.make(500, 7);
        List<YearResult> rows = new Simulation(2024, 2050, new Rng(1), new Scenario(), false, 10, stock).run();
        assertEquals(27, rows.size());
        int total = rows.get(rows.size() - 1).stock.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(500, total);
    }
    @Test void rngBetaMean() {
        Rng r = new Rng(1); double s = 0; int n = 50000;
        for (int i = 0; i < n; i++) s += r.beta(5, 2, 0, 1);
        assertEquals(5.0 / 7.0, s / n, 0.01);
    }
}
