package heattransition;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

/** StockLoader.assignOwnership: buurt totals are met exactly, KNOWN PC6s get their corporation
 *  count, and the rest of the social stock is clustered in whole UNKNOWN PC6s. */
class OwnershipTest {

    private static int nextId = 0;

    private static void add(StockLoader.Nbh g, String pc6, int count, double[] pc6Shares) {
        for (int i = 0; i < count; i++) {
            Dwelling d = new Dwelling(nextId++, HeatingSystem.NATURAL_GAS_BOILER, 0, 10000, "c", 80,
                    "TERRACED", false, false, 0.5);
            d.pc6 = pc6; d.archetype = "TERRACED";
            g.all.add(d); g.pc6.add(pc6Shares);
        }
    }

    /** 100 dwellings, buurt 30 % social / 20 % private rented / 50 % owned.
     *  K : 20 dwellings, corporation count known (half of them social).
     *  U1: 20 dwellings, 60 % rented but aantal_corp = 0 (unknown).
     *  U2: 20 dwellings, figures suppressed (unknown).
     *  Z : 40 dwellings, 0 % rented. */
    private static StockLoader.Nbh build(long seed) {
        StockLoader.Nbh g = new StockLoader.Nbh();
        g.buurt = "BU_TEST";
        add(g, "K", 20, new double[]{ 0.5, 0.3, 0.2 });
        add(g, "U1", 20, new double[]{ 0.0, 0.6, 0.4 });
        add(g, "U2", 20, null);
        add(g, "Z", 40, new double[]{ 0.0, 0.0, 1.0 });
        StockLoader.assignOwnership(new Rng(seed), g, new double[]{ 0.3, 0.2, 0.5 });
        return g;
    }

    private static Map<String, Integer> socialPerPc6(StockLoader.Nbh g) {
        Map<String, Integer> m = new HashMap<>();
        for (Dwelling d : g.sh) m.merge(d.pc6, 1, Integer::sum);
        return m;
    }

    @Test void buurtTotalsAreMet() {
        for (long seed = 1; seed <= 20; seed++) {
            StockLoader.Nbh g = build(seed);
            assertEquals(30, g.sh.size(), "seed " + seed);
            assertEquals(20, g.ll.size(), "seed " + seed);
            assertEquals(50, g.ho.size(), "seed " + seed);
            for (Dwelling d : g.sh) assertEquals("SOCIAL_HOUSING", d.ownership);
            for (Dwelling d : g.ll) assertEquals("PRIVATELY_RENTED", d.ownership);
            for (Dwelling d : g.ho) assertEquals("PRIVATELY_OWNED", d.ownership);
        }
    }

    @Test void knownCountPlacedAndRestClusteredInOneWholePc6() {
        for (long seed = 1; seed <= 20; seed++) {
            Map<String, Integer> s = socialPerPc6(build(seed));
            assertEquals(10, s.getOrDefault("K", 0), "seed " + seed);
            assertEquals(0, s.getOrDefault("Z", 0), "seed " + seed);
            int u1 = s.getOrDefault("U1", 0), u2 = s.getOrDefault("U2", 0);
            // the remaining 20 social dwellings fill ONE unknown PC6 entirely, not both halfway
            assertTrue((u1 == 20 && u2 == 0) || (u1 == 0 && u2 == 20),
                    "seed " + seed + ": U1=" + u1 + " U2=" + u2);
        }
    }

    @Test void zeroRentedPc6GetsNoRental() {
        for (long seed = 1; seed <= 20; seed++) {
            StockLoader.Nbh g = build(seed);
            for (Dwelling d : g.ll) assertNotEquals("Z", d.pc6, "seed " + seed);
        }
    }
}
