package heattransition;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** StockLoader.assignInitial: social housing forms one block per PC6, and the initial heating
 *  quotas of the neighbourhood are met with multi-family complexes served first. */
class SocialBlocksTest {

    private static int nextId = 0;

    private static Dwelling dwelling(String pc6, String archetype, String ownership) {
        Dwelling d = new Dwelling(nextId++, HeatingSystem.NATURAL_GAS_BOILER, 0, 10000, "c", 80,
                archetype, false, true, 0.5);
        d.pc6 = pc6; d.archetype = archetype; d.ownership = ownership; d.buurt = "BU_TEST";
        d.constructionYear = 1975;
        return d;
    }

    /** 100 dwellings: social complexes A (20 flats), B (15, one flat among terraced), C (5 terraced),
     *  plus 60 owner-occupied terraced houses. Quotas: 20 % DH, 15 % gas block, 65 % gas boiler. */
    private static StockLoader.Agents build(long seed) {
        StockLoader.Nbh nbh = new StockLoader.Nbh();
        nbh.buurt = "BU_TEST";
        for (int i = 0; i < 20; i++) nbh.sh.add(dwelling("A", "APARTMENT", "SOCIAL_HOUSING"));
        nbh.sh.add(dwelling("B", "APARTMENT", "SOCIAL_HOUSING"));
        for (int i = 0; i < 14; i++) nbh.sh.add(dwelling("B", "TERRACED", "SOCIAL_HOUSING"));
        for (int i = 0; i < 5; i++) nbh.sh.add(dwelling("C", "TERRACED", "SOCIAL_HOUSING"));
        for (int i = 0; i < 60; i++) nbh.ho.add(dwelling("D", "TERRACED", "PRIVATELY_OWNED"));
        StockLoader.Perc perc = new StockLoader.Perc();
        perc.dh = 0.20; perc.gasBlock = 0.15; perc.gasCV = 0.65; perc.grid = true;
        StockLoader.Agents out = new StockLoader.Agents();
        StockLoader.assignInitial(new Rng(seed), nbh, perc, out, 2024);
        return out;
    }

    @Test void oneSocialBlockPerPc6() {
        StockLoader.Agents out = build(1);
        assertEquals(3, out.socialBlocks.size());
        Set<String> seen = new HashSet<>();
        int n = 0;
        for (HousingBlock b : out.socialBlocks) {
            Set<String> pc6s = new HashSet<>();
            for (Dwelling d : b.households) pc6s.add(d.pc6);
            assertEquals(1, pc6s.size(), "a social block spans more than one PC6");
            assertTrue(seen.add(pc6s.iterator().next()), "two social blocks share a PC6");
            n += b.households.size();
        }
        assertEquals(40, n);
    }

    @Test void multiFamilyComplexesGetCollectiveSystemsFirst() {
        for (long seed = 1; seed <= 20; seed++) {
            Map<String, HeatingSystem> byPc6 = new java.util.HashMap<>();
            for (HousingBlock b : build(seed).socialBlocks) {
                for (Dwelling d : b.households) assertEquals(b.currentType, d.currentType);
                byPc6.put(b.households.get(0).pc6, b.currentType);
            }
            assertEquals(HeatingSystem.DISTRICT_HEATING, byPc6.get("A"), "seed " + seed);
            assertEquals(HeatingSystem.NATURAL_GAS_BLOCK, byPc6.get("B"), "seed " + seed);
            assertEquals(HeatingSystem.NATURAL_GAS_BOILER, byPc6.get("C"), "seed " + seed);
        }
    }

    @Test void initialAgesCountToTheStartYear() {
        for (int start : new int[]{ 2022, 2024 }) {
            StockLoader.Nbh nbh = new StockLoader.Nbh();
            nbh.buurt = "BU_TEST";
            Dwelling d = dwelling("D", "TERRACED", "PRIVATELY_OWNED");
            d.constructionYear = 2015;          // boiler (lifetime 12) installed at construction
            nbh.ho.add(d);
            StockLoader.Perc perc = new StockLoader.Perc();
            perc.gasCV = 1.0;
            StockLoader.Agents out = new StockLoader.Agents();
            StockLoader.assignInitial(new Rng(1), nbh, perc, out, start);
            assertEquals(start - 2015, out.homeowners.get(0).age, "start " + start);
        }
    }

    @Test void neighbourhoodQuotasAreMet() {
        for (long seed = 1; seed <= 20; seed++) {
            StockLoader.Agents out = build(seed);
            Map<HeatingSystem, Integer> n = new EnumMap<>(HeatingSystem.class);
            for (HousingBlock b : out.socialBlocks) for (Dwelling d : b.households) n.merge(d.currentType, 1, Integer::sum);
            for (HousingBlock b : out.hoaBlocks) for (Dwelling d : b.households) n.merge(d.currentType, 1, Integer::sum);
            for (Dwelling d : out.homeowners) n.merge(d.currentType, 1, Integer::sum);
            assertEquals(20, n.getOrDefault(HeatingSystem.DISTRICT_HEATING, 0), "seed " + seed);
            assertEquals(15, n.getOrDefault(HeatingSystem.NATURAL_GAS_BLOCK, 0), "seed " + seed);
            assertEquals(65, n.getOrDefault(HeatingSystem.NATURAL_GAS_BOILER, 0), "seed " + seed);
            assertEquals(100, out.total());
        }
    }
}
