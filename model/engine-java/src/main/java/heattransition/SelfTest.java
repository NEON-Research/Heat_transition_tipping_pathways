package heattransition;

import java.util.List;

/** Dependency-free assertions harness — verifies the core formulas WITHOUT needing JUnit
 *  (useful offline: `gradle selfTest` or `java heattransition/SelfTest.java` with a JDK).
 *  Mirrors the hand-verified values in ../../tests/test_heat_model_spec.py. */
public final class SelfTest {
    static int passed = 0, failed = 0;
    static void approx(double a, double b) { approx(a, b, 1e-9); }
    static void approx(double a, double b, double tol) {
        if (Math.abs(a - b) <= tol) passed++;
        else { failed++; System.out.println("  FAIL: expected " + b + " got " + a); }
    }
    static void ok(boolean c, String msg) { if (c) passed++; else { failed++; System.out.println("  FAIL: " + msg); } }

    public static void main(String[] args) {
        // attitude
        approx(Decision.attitudeValue(0.7, 0.9), 0.8);
        approx(Decision.attitudeValue(0.42, 0.42), 1.0);
        // effort
        approx(Decision.effort(true, true, false), 0.2);
        approx(Decision.effort(false, true, false), 0.8);
        approx(Decision.effort(false, false, false), 0.5);
        // normalize
        approx(Decision.normalizedValue(1500, 1000, 2000), 0.5);
        // PBC
        approx(Decision.pbc(0.25, 0.5), 0.475 / 0.7);
        approx(Decision.pbc(0.0, 0.0), 1.0);
        approx(Decision.pbc(1.0, 1.0), 0.0);
        // subjective norm
        approx(Decision.subjectiveNorm(4, 10, 0.5), 0.6);
        approx(Decision.subjectiveNorm(8, 10, 1.0), 1.0);
        // intention medium SLF
        double p = 0.475 / 0.7;
        approx(Decision.intention(0.8, 0.6, p, 1.0, 0.3), (0.8 * 0.5 + 0.6 * 0.5 * 0.3 + p * 0.5) / 1.5);
        // intention high SLF
        approx(Decision.intention(0.8, 0.6, 0.5, 2.0, 0.3),
               (0.8 * 0.5 + 0.6 * 1.0 * 0.3 + 0.5 * 0.5) / (0.5 + 1.0 + 0.5));
        // perceived utility
        approx(Decision.perceivedUtility(0.4, 0.8), 0.6);
        // salience flat = half blend
        double novelty = 1 / (1 + Math.exp(10 * (0.2 - 0.3)));
        approx(Decision.salienceFactor(0.2, 0.2), 0.5 * novelty + 0.5 * 0.2);
        ok(Decision.salienceFactor(0.9, 0.9) >= 0 && Decision.salienceFactor(0.9, 0.9) <= 1, "salience bounded");
        // learning curve
        approx(Decision.capexLearningFactor(4000, 1000, 0.1, 1.0), 0.81);
        approx(Decision.capexLearningFactor(4000, 1000, 0.1, 2.0), 0.64);
        approx(Decision.capexLearningFactor(1000, 1000, 0.1, 1.0), 1.0);
        // triggers — CORRECTED
        ok(!Decision.hasOpportunityTrigger(11, 15, false), "corrected: 11/15 no trigger");
        ok(Decision.hasOpportunityTrigger(12, 15, false), "corrected: 12/15 trigger");
        ok(Decision.hasOpportunityTrigger(14, 15, false), "corrected: 14/15 trigger");
        // triggers — LEGACY (int division bug)
        ok(!Decision.hasOpportunityTrigger(14, 15, true), "legacy: 14/15 no trigger (bug)");
        ok(Decision.hasOpportunityTrigger(15, 15, true), "legacy: 15/15 trigger");
        ok(!Decision.hasEndOfLifeTrigger(14, 15), "eol 14<15");
        ok(Decision.hasEndOfLifeTrigger(15, 15), "eol 15>=15");
        // RUM
        approx(Decision.rumScore(0.7, 0.0), 0.7);
        ok(Decision.rumScore(0.7, 1.0) > Decision.rumScore(0.6, 1.0), "rum ranking");
        // EAC finite/positive on real data
        Dwelling d = new Dwelling(0, HeatingSystem.NATURAL_GAS_BOILER, 5, 9000, "d", 100, "HOUSE", false, false, 0.7);
        long eac = Economics.computeEAC(HeatingSystemData.freshSpecs().get(HeatingSystem.NATURAL_GAS_BOILER), d, (dd, l) -> 0);
        ok(eac > 0, "EAC positive");
        // RNG beta(5,2) mean ~ 5/7
        Rng r = new Rng(1); double s = 0; int nn = 50000;
        for (int i = 0; i < nn; i++) s += r.beta(5, 2, 0, 1);
        approx(s / nn, 5.0 / 7.0, 0.01);
        // end-to-end smoke test (synthetic stock -> blocks/landlords/homeowners, stock conserved)
        try {
            java.util.List<Dwelling> ho = SyntheticData.make(300, 5);
            for (Dwelling dw : ho) dw.ownership = "PRIVATELY_OWNED";
            java.util.List<Dwelling> ll = SyntheticData.make(100, 6);
            for (Dwelling dw : ll) dw.ownership = "PRIVATELY_RENTED";
            HousingBlock blk = new HousingBlock("B1", 5);
            for (Dwelling dw : SyntheticData.make(100, 7)) { dw.ownership = "SOCIAL_HOUSING"; blk.households.add(dw); }
            Vesta vesta = Vesta.load("../data/reference/dwellings_demand_insulation.json");
            Simulation fs = new Simulation(2024, 2050, new Rng(1), new Scenario(), false, 10,
                    ho, ll, java.util.List.of(blk), java.util.List.of(), vesta);
            java.util.List<Simulation.YearRow> fullRows = fs.run();
            int tot = 0; for (int v : fullRows.get(fullRows.size() - 1).stock.values()) tot += v;
            ok(fullRows.size() == 27, "full-sim 27 years");
            ok(tot == 500, "full-sim stock conserved (=500)");
        } catch (Exception e) { failed++; System.out.println("  FAIL: full-sim threw " + e); }

        System.out.println("\n" + passed + " passed, " + failed + " failed, " + (passed + failed) + " total");
        if (failed > 0) System.exit(1);
    }
}
