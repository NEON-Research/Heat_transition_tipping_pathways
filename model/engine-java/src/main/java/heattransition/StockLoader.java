package heattransition;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** Full multi-owner Limburg stock with faithful per-neighbourhood INITIAL heating assignment
 *  (GZ_Neighborhood.f_setHouseholdDefaultHeatingSystem), which also forms the HOA blocks
 *  (f_addHouseholdToHOABlock). Mirrors engine/data/loadRealStockAll.js. */
public final class StockLoader {
    private StockLoader() {}

    public static final class Agents {
        public final List<Dwelling> homeowners = new ArrayList<>();
        public final List<Dwelling> landlords = new ArrayList<>();
        public final List<HousingBlock> socialBlocks = new ArrayList<>();
        public final List<HousingBlock> hoaBlocks = new ArrayList<>();
        public final List<Neighbourhood> neighbourhoods = new ArrayList<>();
        public Vesta vesta;
        public int total() { int n = homeowners.size() + landlords.size();
            for (HousingBlock b : socialBlocks) n += b.households.size();
            for (HousingBlock b : hoaBlocks) n += b.households.size(); return n; }
    }
    static final class Perc { double gasCV, gasBlock, ehp, hhp, dh; boolean grid; }
    private static double parseD(String v) {
        try { return Double.parseDouble(v); } catch (Exception e) { return 0; }
    }

    /** neighborhoods.csv from data-export/scripts/export_neighborhoods.py. Optional: without
     *  it DH expansion has no surface area and the grid stays static (pre-port behaviour). */
    private static java.util.Map<String, String[]> loadNeighbourhoodCsv(Path stockDir) {
        java.util.Map<String, String[]> out = new java.util.LinkedHashMap<>();
        java.io.File f = refFile(stockDir, "neighborhoods.csv").toFile();
        if (!f.exists()) return out;
        try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String header = r.readLine();
            if (header == null) return out;
            String[] h = header.replace("\uFEFF", "").split(",");
            java.util.Map<String, Integer> ix = new java.util.LinkedHashMap<>();
            for (int i = 0; i < h.length; i++) ix.put(h[i].trim(), i);
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] c = line.split(",");
                out.put(c[ix.get("buurtcode")], new String[] {
                    c[ix.get("a_lan_ha")], c[ix.get("a_hh")], c[ix.get("g_ele")], c[ix.get("a_pau")],
                    c[ix.get("policy_plan")], c[ix.get("policy_start_jaar")],
                    c[ix.get("policy_eind_jaar")], c[ix.get("policy_infra_w")] });
            }
        } catch (java.io.IOException e) { /* optional file */ }
        return out;
    }

    static final class Nbh {
        String buurt;
        List<Dwelling> ho = new ArrayList<>(), ll = new ArrayList<>(), sh = new ArrayList<>();
        List<Dwelling> all = new ArrayList<>();       // every loaded dwelling, before ownership
        List<double[]> pc6 = new ArrayList<>();       // parallel to all: PC6 ownership shares or null
    }

    private static boolean requiresLowTemp(HeatingSystem t) {
        return t == HeatingSystem.HYBRID_HEAT_PUMP || t == HeatingSystem.ELECTRIC_HEAT_PUMP;
    }
    private static int lifetimeOf(HeatingSystem t) {
        switch (t) { case NATURAL_GAS_BOILER: case NATURAL_GAS_BLOCK: return 12;
            case HYBRID_HEAT_PUMP: case ELECTRIC_HEAT_PUMP: return 15; case DISTRICT_HEATING: return 30; default: return 12; }
    }
    // J_Dwelling/J_HousingBlock.f_setInitialHeatingMethod: age = startYear - constructionYear; if >= lifetime, random(0,lifetime)
    private static int initialHsAge(Rng rng, int startYear, int constructionYear, HeatingSystem type) {
        int a = startYear - constructionYear; int lt = lifetimeOf(type);
        if (a >= lt) a = rng.nextInt(0, lt);
        return Math.max(0, a);
    }
    private static int blockYearLastRenovation(Rng rng, java.util.List<Dwelling> hh) {
        int sum = 0; for (Dwelling d : hh) sum += d.constructionYear; int avgCy = Math.round((float) sum / hh.size());
        return avgCy < 1990 ? 1990 + rng.nextInt(0, 35) : avgCy;   // J_HousingBlock.setDefaultYearLastRenovation
    }
    private static String blockAvgLabel(java.util.List<Dwelling> hh) {                  // setDefaultEnergyLabel
        int sum = 0; for (Dwelling d : hh) sum += Vesta.labelNum(d.energyLabel);
        return Vesta.numLabel((int) Math.round((double) sum / hh.size()));   // round to nearest (was AL int-division floor)
    }

    private static double truncNormal(Rng rng, double mean, double sd, double lo, double hi) {
        for (int i = 0; i < 100; i++) { double u = 0, v = 0;
            while (u == 0) u = rng.next(); while (v == 0) v = rng.next();
            double z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); double x = mean + sd * z;
            if (x >= lo && x <= hi) return x; } return mean;
    }
    /** Ownership shares {social, privately rented, owned} for one PC6, from the CBS PC6 key figures:
     *  perc_koop and perc_huur are PERCENTAGES (rounded to 10), aantal_corp is a COUNT of dwellings
     *  owned by housing corporations (rounded to 5). The social share is that count over the PC6's
     *  dwelling count in the stock, capped at the rented share; the rest of the rented share is
     *  private rental. koop + huur is rescaled to 100 (rounding, unknown remainder). A PC6 whose
     *  figures are suppressed (koop = huur = 0) returns null.
     *  (Replaces AL f_getHouseholdOwnership, which subtracted the count from the percentage.) */
    static double[] ownershipShares(double koop, double huur, double corp, int dwellingsInPc6) {
        double known = koop + huur;
        if (known <= 0) return null;
        double social = dwellingsInPc6 > 0 ? Math.min(corp / dwellingsInPc6, huur / 100) : 0;
        double rented = huur / 100 - social;
        double owned = koop / 100;
        double sum = known / 100;
        return new double[]{ social / sum, rented / sum, owned / sum };
    }
    /** Buurt ownership shares {social, privately rented, owned} (fractions summing to 1) from the
     *  own_corp / own_other / own_koop columns of neighborhoods.csv (CBS KWB p_wcorpw / p_ov_hw /
     *  p_koopw). Buurten CBS suppresses have blank cells and are left out. Missing columns throw. */
    private static Map<String, double[]> loadOwnership(Path csv) throws Exception {
        Map<String, double[]> out = new HashMap<>();
        for (Map<String, String> r : Csv.read(csv)) {
            if (!r.containsKey("own_corp") || !r.containsKey("own_other") || !r.containsKey("own_koop"))
                throw new IllegalStateException(csv + ": no own_corp/own_other/own_koop columns "
                        + "(rebuild with data-export/scripts/build_neighborhoods.py)");
            String corp = r.get("own_corp"), other = r.get("own_other"), koop = r.get("own_koop");
            if (corp == null || corp.isEmpty() || other == null || other.isEmpty() || koop == null || koop.isEmpty())
                continue;
            double s = Double.parseDouble(corp), p = Double.parseDouble(other), o = Double.parseDouble(koop);
            double sum = s + p + o;
            if (sum <= 0) continue;
            out.put(r.get("buurtcode"), new double[]{ s / sum, p / sum, o / sum });
        }
        return out;
    }

    /** Weighted random order (Efraimidis-Spirakis: key -ln(u)/w, smallest first). A zero weight
     *  becomes a tiny one, so such items come last but in random order. */
    private static <T> List<T> weightedOrder(Rng rng, List<T> items, java.util.function.ToDoubleFunction<T> w) {
        double[] key = new double[items.size()];
        Integer[] ix = new Integer[items.size()];
        for (int i = 0; i < ix.length; i++) {
            ix[i] = i;
            key[i] = -Math.log(1 - rng.next()) / Math.max(w.applyAsDouble(items.get(i)), 1e-6);
        }
        java.util.Arrays.sort(ix, (a, b) -> Double.compare(key[a], key[b]));
        List<T> out = new ArrayList<>(ix.length);
        for (int i : ix) out.add(items.get(i));
        return out;
    }

    /** Assign ownership inside one buurt. The buurt shares fix HOW MANY dwellings are social,
     *  privately rented and owned; the PC6 figures decide WHERE.
     *
     *  <p>Social housing is placed in clusters, because corporations own whole streets and
     *  complexes rather than scattered dwellings. Each PC6 in the buurt is one of:
     *  <ul>
     *    <li>KNOWN: aantal_corp &gt; 0. It gets that count (scaled to its dwellings in this buurt).</li>
     *    <li>UNKNOWN: figures suppressed, or rented dwellings with aantal_corp = 0. The source stores
     *        a missing count as 0, so such a zero is not evidence of no corporation stock.</li>
     *    <li>NONE: 0 % rented. No social housing unless nothing else is left.</li>
     *  </ul>
     *  The KNOWN counts are placed first. The rest of the buurt's social quota then fills UNKNOWN
     *  PC6s one WHOLE PC6 at a time (the last one partly), in a random order weighted by the PC6's
     *  rented share. So some PC6s become entirely social and the others get none. Any remainder
     *  tops up KNOWN PC6s, then NONE PC6s. Within a PC6, flats are taken before houses.
     *
     *  <p>Private rental is then drawn per dwelling from what is left, weighted by the PC6's
     *  private-rented share (the buurt's for a suppressed PC6). The rest is owner-occupied. */
    static void assignOwnership(Rng rng, Nbh g, double[] buurt) {
        int n = g.all.size();
        int nSocial = Math.min(n, (int) Math.round(buurt[0] * n));
        int nPrivate = Math.min(n - nSocial, (int) Math.round(buurt[1] * n));

        // group this buurt's dwellings by PC6; all dwellings of a PC6 share the same PC6 figures
        java.util.Map<String, List<Integer>> byPc6 = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) byPc6.computeIfAbsent(g.all.get(i).pc6, k -> new ArrayList<>()).add(i);
        List<List<Integer>> known = new ArrayList<>(), unknown = new ArrayList<>(), none = new ArrayList<>();
        for (List<Integer> ix : byPc6.values()) {
            double[] p = g.pc6.get(ix.get(0));
            if (p != null && p[0] > 0) known.add(ix);
            else if (p == null || p[1] > 0) unknown.add(ix);
            else none.add(ix);
        }
        // within a PC6: flats first, otherwise random
        for (List<List<Integer>> grp : List.of(known, unknown, none))
            for (List<Integer> ix : grp) {
                shuffle(rng, ix);
                ix.sort((a, b) -> Boolean.compare(multiFamily(g.all.get(b)), multiFamily(g.all.get(a))));
            }

        boolean[] social = new boolean[n];
        int[] left = { nSocial };
        java.util.function.BiConsumer<List<Integer>, Integer> take = (ix, want) -> {
            int t = 0;
            for (int i : ix) {
                if (t >= want || left[0] == 0) break;
                if (!social[i]) { social[i] = true; t++; left[0]--; }
            }
        };
        // 1. KNOWN PC6s get their reported corporation count
        for (List<Integer> ix : weightedOrder(rng, known, ix -> 1.0))
            take.accept(ix, (int) Math.round(g.pc6.get(ix.get(0))[0] * ix.size()));
        // 2. the rest fills UNKNOWN PC6s whole, weighted by their rented share
        for (List<Integer> ix : weightedOrder(rng, unknown, ix -> {
                double[] p = g.pc6.get(ix.get(0));
                return p != null ? p[1] : buurt[0] + buurt[1];
            }))
            take.accept(ix, ix.size());
        // 3. still short: top up KNOWN PC6s, then NONE PC6s
        for (List<Integer> ix : weightedOrder(rng, known, ix -> 1.0)) take.accept(ix, ix.size());
        for (List<Integer> ix : weightedOrder(rng, none, ix -> 1.0)) take.accept(ix, ix.size());

        // private rental per dwelling among the rest
        List<Integer> rest = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Dwelling d = g.all.get(i);
            if (social[i]) { d.ownership = "SOCIAL_HOUSING"; g.sh.add(d); }
            else rest.add(i);
        }
        List<Integer> ordered = weightedOrder(rng, rest, i -> {
            double[] p = g.pc6.get(i);
            return p != null ? p[1] : buurt[1];
        });
        for (int k = 0; k < ordered.size(); k++) {
            Dwelling d = g.all.get(ordered.get(k));
            if (k < nPrivate) { d.ownership = "PRIVATELY_RENTED"; g.ll.add(d); }
            else { d.ownership = "PRIVATELY_OWNED"; g.ho.add(d); }
        }
    }
    /** Dwellings per PC6 over the WHOLE stock file (before any everyNth / buurt filter), because
     *  aantal_corp counts the real PC6. */
    private static Map<String, Integer> countPc6(String csvPath) throws Exception {
        Map<String, Integer> n = new HashMap<>();
        try (BufferedReader br = Files.newBufferedReader(Path.of(csvPath))) {
            String[] header = br.readLine().replace("\r", "").split(",");
            int col = java.util.Arrays.asList(header).indexOf("pc6");
            if (col < 0) throw new IllegalStateException(csvPath + ": no 'pc6' column");
            String ln;
            while ((ln = br.readLine()) != null) {
                if (ln.isEmpty()) continue;
                n.merge(ln.split(",", col + 2)[col], 1, Integer::sum);
            }
        }
        return n;
    }
    private static <T> void shuffle(Rng rng, List<T> a) {
        for (int i = a.size() - 1; i > 0; i--) { int j = rng.nextInt(0, i + 1); T t = a.get(i); a.set(i, a.get(j)); a.set(j, t); }
    }

    /** Load the optional buurt subselection (-Dht.buurtFilter / HT_BUURTFILTER). Returns null when
     *  unset, i.e. simulate every neighbourhood in the stock. Path may be absolute or relative to
     *  the reference dir. */
    private static java.util.Set<String> loadBuurtFilter(Path stockDir) {
        String spec = System.getProperty("ht.buurtFilter", System.getenv("HT_BUURTFILTER"));
        if (spec == null || spec.isEmpty()) return null;
        Path p = Path.of(spec);
        if (!p.isAbsolute() && !java.nio.file.Files.exists(p)) p = refFile(stockDir, spec);
        java.util.Set<String> out = new HashSet<>();
        try {
            for (String line : java.nio.file.Files.readAllLines(p)) {
                String s = line.replace("﻿", "").trim();
                if (s.isEmpty() || s.startsWith("#")) continue;
                int comma = s.indexOf(',');
                if (comma >= 0) s = s.substring(0, comma).trim();
                if (s.equalsIgnoreCase("buurtcode")) continue;      // header
                out.add(s);
            }
        } catch (java.io.IOException e) {
            throw new RuntimeException("cannot read buurt filter " + p + ": " + e.getMessage(), e);
        }
        System.out.printf("[stock] buurt filter: %d neighbourhoods from %s%n", out.size(), p.getFileName());
        return out;
    }

    // nbh_heating.csv: buurtcode,gasCV,gasBlock,ehp,hhp,dh,hasDHgrid  (per-neighbourhood shares)
    /** Initial heating-method shares per neighbourhood, read from the combined neighborhoods.csv
     *  (columns gasCV_<year>, gasBlock_<year>, ehp_<year>, hhp_<year>, dh_<year>; built by
     *  data-export/scripts/build_neighborhoods.py). Year via -Dht.heatingYear (default 2023).
     *  Falls back to unsuffixed columns so a legacy nbh_heating-style file still loads.
     *
     *  MISSING DATA: a neighbourhood whose shares are absent/blank (CBS privacy suppression) is
     *  simply not put in the map -- the caller then applies the default Perc (gasCV = 1, i.e. all
     *  gas boiler), which is AL's behaviour. Shares are NOT silently zeroed, because an all-zero
     *  row would leave the neighbourhood with no heating system at all. */
    private static Map<String, Perc> loadNbh(String path) throws Exception {
        String yr = System.getProperty("ht.heatingYear", System.getenv().getOrDefault("HT_HEATINGYEAR", "2023"));
        Map<String, Perc> out = new HashMap<>();
        int skipped = 0;
        for (Map<String, String> r : Csv.read(Path.of(path))) {
            String buurt = r.get("buurtcode");
            if (buurt == null || buurt.isEmpty()) continue;
            // year-suffixed columns first, then unsuffixed (legacy nbh_heating.csv layout)
            String gc = pick(r, "gasCV", yr), gb = pick(r, "gasBlock", yr), eh = pick(r, "ehp", yr),
                   hh = pick(r, "hhp", yr), dh = pick(r, "dh", yr);
            if (gc == null || gc.isEmpty()) { skipped++; continue; }   // no data -> default (gas)
            Perc p = new Perc();
            p.gasCV = Csv.d(gc); p.gasBlock = Csv.d(gb); p.ehp = Csv.d(eh);
            p.hhp = Csv.d(hh); p.dh = Csv.d(dh);
            if (p.gasCV + p.gasBlock + p.ehp + p.hhp + p.dh <= 0) { skipped++; continue; }
            p.grid = p.dh > 0;                       // hasDHgrid == dh>0 (same rule as the export)
            out.put(buurt, p);
        }
        heatingYearUsed = yr;      // reported per-run below, once the stock's neighbourhoods are known
        return out;
    }
    private static String heatingYearUsed = "";

    /** column "<name>_<year>" if present, else the unsuffixed "<name>". */
    private static String pick(Map<String, String> r, String name, String year) {
        String v = r.get(name + "_" + year);
        return v != null ? v : r.get(name);
    }

    private static int rnd(double x) { return (int) Math.round(x); }

    private static boolean multiFamily(Dwelling d) {
        return d.archetype.equals("APARTMENT") || d.archetype.equals("HIGHRISE");
    }
    private static double multiFamilyShare(List<Dwelling> hh) {
        int m = 0; for (Dwelling d : hh) if (multiFamily(d)) m++;
        return hh.isEmpty() ? 0 : (double) m / hh.size();
    }

    // GZ_Neighborhood.f_setHouseholdDefaultHeatingSystem
    static void assignInitial(Rng rng, Nbh nbh, Perc perc, Agents out, int startYear) {
        List<Dwelling> nonSHB = new ArrayList<>(nbh.ho); nonSHB.addAll(nbh.ll);
        int households = nbh.ho.size() + nbh.ll.size() + nbh.sh.size();
        if (households == 0) return;
        int reqNGB = rnd(perc.gasCV * households), reqNGBl = rnd(perc.gasBlock * households);
        int reqEHP = rnd(perc.ehp * households), reqHHP = rnd(perc.hhp * households), reqDH = rnd(perc.dh * households);
        int total = reqNGB + reqNGBl + reqHHP + reqEHP + reqDH;
        while (total > households) { if (reqNGB > 0) reqNGB--; else if (reqNGBl > 0) reqNGBl--; else if (reqDH > 0) reqDH--; else if (reqHHP > 0) reqHHP--; else if (reqEHP > 0) reqEHP--; total--; }
        while (total < households) { reqNGB++; total++; }
        int setNGB = 0, setNGBl = 0, setEHP = 0, setHHP = 0, setDH = 0;
        // Social housing: one block per PC6 (a street / complex), not one per neighbourhood, so a
        // corporation's stock re-decides complex by complex and the initial collective systems can
        // land on part of it. Each complex takes one heating type from the neighbourhood quotas, as
        // AL did for its single block: DH if it still fits, else gas block, else a random chance of
        // a heat pump, else gas boiler. Multi-family complexes go first, because DH and block
        // heating sit mostly in flats; ties are in random order.
        java.util.Map<String, List<Dwelling>> byPc6 = new LinkedHashMap<>();
        for (Dwelling d : nbh.sh) byPc6.computeIfAbsent(d.pc6, k -> new ArrayList<>()).add(d);
        List<List<Dwelling>> complexes = new ArrayList<>(byPc6.values());
        shuffle(rng, complexes);
        complexes.sort((a, b) -> Double.compare(multiFamilyShare(b), multiFamilyShare(a)));   // stable
        for (List<Dwelling> cx : complexes) {
            int size = cx.size();
            HeatingSystem type;
            if (setDH + size <= reqDH) { type = HeatingSystem.DISTRICT_HEATING; setDH += size; }
            else if (setNGBl + size <= reqNGBl) { type = HeatingSystem.NATURAL_GAS_BLOCK; setNGBl += size; }
            else { double r = rng.next();
                if (r < 0.1 && setEHP + size <= reqEHP) { type = HeatingSystem.ELECTRIC_HEAT_PUMP; setEHP += size; }
                else if (r < 0.25 && setHHP + size <= reqHHP) { type = HeatingSystem.HYBRID_HEAT_PUMP; setHHP += size; }
                else { type = HeatingSystem.NATURAL_GAS_BOILER; setNGB += size; } }
            int ylrS = blockYearLastRenovation(rng, cx);
            HousingBlock b = new HousingBlock(nbh.buurt, initialHsAge(rng, startYear, ylrS, type)); b.currentType = type;
            b.yearLastRenovation = ylrS; b.energyLabel = blockAvgLabel(cx);
            for (Dwelling d : cx) { d.currentType = type; b.households.add(d); }
            out.socialBlocks.add(b);
        }

        shuffle(rng, nonSHB);
        List<Dwelling> hoaHH = new ArrayList<>(); Set<Dwelling> taken = new HashSet<>();
        // AL fills tier 1 with APARTMENT || HIGHRISE. Limburg has more highrise (62k) than
        // apartments (48k), so omitting it pushes the fill into the terraced/random tiers.
        for (Dwelling d : nonSHB) { if (setNGBl >= reqNGBl) break; if (d.archetype.equals("APARTMENT") || d.archetype.equals("HIGHRISE")) { d.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoaHH.add(d); taken.add(d); setNGBl++; } }
        for (Dwelling d : nonSHB) { if (setNGBl >= reqNGBl) break; if (!taken.contains(d) && d.archetype.equals("TERRACED")) { d.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoaHH.add(d); taken.add(d); setNGBl++; } }
        for (Dwelling d : nonSHB) { if (setNGBl >= reqNGBl) break; if (!taken.contains(d)) { d.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoaHH.add(d); taken.add(d); setNGBl++; } }
        if (!hoaHH.isEmpty()) { int ylrH = blockYearLastRenovation(rng, hoaHH);
            HousingBlock hoa = new HousingBlock(nbh.buurt, initialHsAge(rng, startYear, ylrH, HeatingSystem.NATURAL_GAS_BLOCK));
            hoa.blockType = "HOA";
            hoa.yearLastRenovation = ylrH; hoa.energyLabel = blockAvgLabel(hoaHH);
            hoa.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoa.households.addAll(hoaHH);
            for (Dwelling d : hoaHH) d.ownership = "HOME_OWNER_ASSOCIATION"; out.hoaBlocks.add(hoa); }

        List<Dwelling> rem = new ArrayList<>(); for (Dwelling d : nonSHB) if (!taken.contains(d)) rem.add(d);
        int i = 0, remDH = reqDH - setDH, remEHP = reqEHP - setEHP, remHHP = reqHHP - setHHP;
        while (remDH > 0 && i < rem.size()) { rem.get(i++).currentType = HeatingSystem.DISTRICT_HEATING; remDH--; }
        while (remEHP > 0 && i < rem.size()) { rem.get(i++).currentType = HeatingSystem.ELECTRIC_HEAT_PUMP; remEHP--; }
        while (remHHP > 0 && i < rem.size()) { rem.get(i++).currentType = HeatingSystem.HYBRID_HEAT_PUMP; remHHP--; }
        while (i < rem.size()) rem.get(i++).currentType = HeatingSystem.NATURAL_GAS_BOILER;

        for (Dwelling d : nbh.ho) d.hasLowTemp = requiresLowTemp(d.currentType);
        for (Dwelling d : nbh.ll) d.hasLowTemp = requiresLowTemp(d.currentType);
        for (Dwelling d : nbh.sh) d.hasLowTemp = requiresLowTemp(d.currentType);
        for (Dwelling d : nbh.ho) if (d.ownership.equals("PRIVATELY_OWNED")) { d.age = initialHsAge(rng, startYear, d.constructionYear, d.currentType); out.homeowners.add(d); }
        for (Dwelling d : nbh.ll) if (d.ownership.equals("PRIVATELY_RENTED")) { d.age = initialHsAge(rng, startYear, d.constructionYear, d.currentType); out.landlords.add(d); }
    }

    /** Resolve a shared reference lookup file. Layout: data/stock/<scope>.csv alongside
     *  data/reference/<name>. Falls back to the stock CSV's own dir (flat layout) for compat. */
    private static Path refFile(Path stockDir, String name) {
        if (stockDir.getParent() != null) {
            Path r = stockDir.getParent().resolve("reference").resolve(name);
            if (Files.exists(r)) return r;
        }
        return stockDir.resolve(name);
    }

    public static Agents load(String csvPath, Rng rng, int everyNth) throws Exception {
        return load(csvPath, rng, everyNth, Constants.DEFAULT_START_YEAR);
    }

    /** Load the stock for a run starting in startYear. Initial system ages are counted to that
     *  year, so a calibration run from 2022 does not start with systems two years too old. */
    public static Agents load(String csvPath, Rng rng, int everyNth, int startYear) throws Exception {
        Agents out = new Agents();
        Path dir = Path.of(csvPath).toAbsolutePath().getParent();
        out.vesta = Vesta.load(refFile(dir, "dwellings_demand_insulation.csv").toString());
        // heating-system + energy-source specs from the reference CSVs (single source of truth,
        // generated from the data/ spreadsheets by export_reference_tables.py).
        HeatingSystemData.loadFrom(refFile(dir, "heating_system_data.csv"),
                                   refFile(dir, "energy_source_data.csv"));
        // Year x arm price + fixed-cost table. Required: a missing or malformed file aborts here.
        EnergyPrices.loadFrom(refFile(dir, "energy_prices_by_year.csv"));
        // ACM district-heating tariffs (connection/disconnection fee, annual fixed costs). Required.
        DistrictHeatingCosts.loadFrom(refFile(dir, "district_heating_costs.csv"));
        // Initial heating shares now live in the combined neighborhoods.csv; pick the vintage with
        // -Dht.heatingYear=2022|2023|2024 (a calibration run starts from the observed 2022 state).
        // -Dht.nbhHeating=<file> can still point at a separate file if ever needed.
        String nbhFile = System.getProperty("ht.nbhHeating",
                System.getenv().getOrDefault("HT_NBHHEATING", "neighborhoods.csv"));
        Map<String, Perc> nbhData = loadNbh(refFile(dir, nbhFile).toString());
        Perc def = new Perc(); def.gasCV = 1;

        Map<String, Nbh> nbhs = new LinkedHashMap<>();
        Map<String, Integer> pc6Count = countPc6(csvPath);
        Map<String, double[]> buurtOwnership = loadOwnership(refFile(dir, "neighborhoods.csv"));
        try (BufferedReader br = Files.newBufferedReader(Path.of(csvPath))) {
            String[] header = br.readLine().replace("\r", "").split(",");
            Map<String, Integer> c = new HashMap<>();
            for (int i = 0; i < header.length; i++) c.put(header[i], i);
            // Optional buurt subselection: -Dht.buurtFilter=<file> (one buurtcode per line, or a
            // CSV whose first column is the code; '#' comments allowed). Restricts the simulation to
            // those neighbourhoods -- used by the calibration to simulate exactly the buurten the
            // observed CBS aggregate covers, without duplicating the (large) stock CSV.
            java.util.Set<String> buurtFilter = loadBuurtFilter(dir);
            String ln; int row = 0, id = 0;
            while ((ln = br.readLine()) != null) {
                if (ln.isEmpty()) continue;
                if (row++ % everyNth != 0) continue;
                String[] f = ln.replace("\r", "").split(",");
                if (buurtFilter != null && !buurtFilter.contains(f[c.get("buurtcode")])) continue;
                double koop = Double.parseDouble(f[c.get("perc_koop")]), huur = Double.parseDouble(f[c.get("perc_huur")]), corp = Double.parseDouble(f[c.get("aantal_corp")]);
                double[] pc6Shares = ownershipShares(koop, huur, corp, pc6Count.getOrDefault(f[c.get("pc6")], 0));
                String label = f[c.get("label")]; if (label == null || label.isEmpty()) label = "n";
                double space = Double.parseDouble(f[c.get("space_heat_kwh")]), dhwBase = Double.parseDouble(f[c.get("dhw_base_kwh")]);
                double fac = truncNormal(rng, 1.0, 0.2, 0.5, 1.5);
                String buurt = f[c.get("buurtcode")]; String dtype = f[c.get("dwelling_type")];
                Perc perc = nbhData.getOrDefault(buurt, def);
                Dwelling d = new Dwelling(id++, HeatingSystem.NATURAL_GAS_BOILER, rng.nextInt(0, 12),
                        fac * space + fac * fac * dhwBase, label, Double.parseDouble(f[c.get("area_m2")]),
                        dtype, false, perc.grid, rng.beta(5, 2, 0, 1));   // hasLowTemp set from initial heating system below
                // split off the hot-water part and keep the stochastic factor: insulation reduces the
                // SPACE-heat component only, so demand can be rebuilt when the label improves
                d.dhwKWh = fac * fac * dhwBase;
                d.demandFactor = fac;
                d.archetype = dtype; d.buurt = buurt;   // ownership is assigned per buurt below
                d.pc6 = f[c.get("pc6")];
                d.numid = f[c.get("numid")];
                d.constructionYear = (int) Double.parseDouble(f[c.get("construction_year")]);
                d.yearLastRenovation = d.constructionYear;
                Nbh g = nbhs.computeIfAbsent(buurt, k -> { Nbh x = new Nbh(); x.buurt = k; return x; });
                g.all.add(d); g.pc6.add(pc6Shares);
            }
        }
        // Ownership per buurt. A buurt CBS suppresses takes the dwelling-weighted average of this
        // run's buurten that have data.
        double[] fallback = new double[3]; int nWithData = 0, nDw = 0;
        for (Nbh g : nbhs.values()) {
            nDw += g.all.size();
            double[] s = buurtOwnership.get(g.buurt);
            if (s == null) continue;
            for (int k = 0; k < 3; k++) fallback[k] += s[k] * g.all.size();
            nWithData += g.all.size();
        }
        if (nWithData == 0) throw new IllegalStateException("no ownership data for any buurt in this run");
        for (int k = 0; k < 3; k++) fallback[k] /= nWithData;
        int pc6Suppressed = 0;
        for (Nbh g : nbhs.values()) {
            assignOwnership(rng, g, buurtOwnership.getOrDefault(g.buurt, fallback));
            for (double[] p : g.pc6) if (p == null) pc6Suppressed++;
        }
        int nSocial = 0, nRented = 0, nOwned = 0;
        for (Nbh g : nbhs.values()) { nSocial += g.sh.size(); nRented += g.ll.size(); nOwned += g.ho.size(); }
        System.out.printf(java.util.Locale.US, "[stock] ownership (buurt shares, PC6 placement): %d dwellings; "
                + "social %.1f%%, private rented %.1f%%, owned %.1f%%; %.1f%% in buurten without CBS data "
                + "(run average used), %.1f%% in suppressed PC6s (buurt ratio used for placement)%n",
                nDw, 100.0 * nSocial / nDw, 100.0 * nRented / nDw, 100.0 * nOwned / nDw,
                100.0 * (nDw - nWithData) / nDw, 100.0 * pc6Suppressed / nDw);
        // Report coverage for THIS RUN's neighbourhoods (not the NL-wide reference table): how many
        // of the neighbourhoods actually simulated have CBS heating data, and how many fall back to
        // the default all-gas mix.
        int nbWith = 0;
        for (String b : nbhs.keySet()) if (nbhData.containsKey(b)) nbWith++;
        System.out.printf("[stock] initial heating shares (year %s): %d of %d neighbourhoods in this "
                + "run have data%s%n", heatingYearUsed, nbWith, nbhs.size(),
                nbhs.size() - nbWith > 0
                        ? String.format("; %d default to gas boiler", nbhs.size() - nbWith) : "");

        java.util.Map<String, String[]> nbhCsv = loadNeighbourhoodCsv(dir);
        for (Nbh nbh : nbhs.values()) {
            assignInitial(rng, nbh, nbhData.getOrDefault(nbh.buurt, def), out, startYear);

            // Neighbourhood agent for DH expansion (and later the DSO).
            // dhDwellings excludes HOA blocks, matching f_DHExpansionPlanCosts.
            Neighbourhood n = new Neighbourhood(nbh.buurt);
            n.dhDwellings.addAll(nbh.ho); n.dhDwellings.addAll(nbh.ll); n.dhDwellings.addAll(nbh.sh);
            n.allDwellings.addAll(n.dhDwellings);
            for (HousingBlock b : out.hoaBlocks)
                if (b.buurt.equals(nbh.buurt)) n.allDwellings.addAll(b.households);
            n.hasDH = nbhData.getOrDefault(nbh.buurt, def).grid;

            String[] f = nbhCsv.get(nbh.buurt);
            if (f != null) {
                n.surfaceAreaLand = parseD(f[0]);
                n.households = (int) parseD(f[1]);
                n.avgElectricityKWh = parseD(f[2]);   // -99999 for every nbh -> 0: no baseload
                n.cars = (int) parseD(f[3]);
                n.policyPlan = f[4];
                n.policyStartJaar = (int) parseD(f[5]);
                n.policyEindJaar = (int) parseD(f[6]);
                n.policyInfraW = "1".equals(f[7]);
            }
            out.neighbourhoods.add(n);
        }
        return out;
    }
}
