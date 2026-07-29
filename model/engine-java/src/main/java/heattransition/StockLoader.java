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

    static final class Nbh { String buurt; List<Dwelling> ho = new ArrayList<>(), ll = new ArrayList<>(), sh = new ArrayList<>(); }

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
    private static String ownership(Rng rng, double koop, double huur, double corp) {
        double hba = (huur - corp) / 100, oll = (huur - hba) / 100; double r = rng.next();
        if (r < hba) return "SOCIAL_HOUSING"; if (r < hba + oll) return "PRIVATELY_RENTED"; return "PRIVATELY_OWNED";
    }
    private static <T> void shuffle(Rng rng, List<T> a) {
        for (int i = a.size() - 1; i > 0; i--) { int j = rng.nextInt(0, i + 1); T t = a.get(i); a.set(i, a.get(j)); a.set(j, t); }
    }

    // nbh_heating.csv: buurtcode,gasCV,gasBlock,ehp,hhp,dh,hasDHgrid  (per-neighbourhood shares)
    private static Map<String, Perc> loadNbh(String path) throws Exception {
        Map<String, Perc> out = new HashMap<>();
        for (Map<String, String> r : Csv.read(Path.of(path))) {
            Perc p = new Perc();
            p.gasCV = Csv.d(r.get("gasCV")); p.gasBlock = Csv.d(r.get("gasBlock"));
            p.ehp = Csv.d(r.get("ehp")); p.hhp = Csv.d(r.get("hhp")); p.dh = Csv.d(r.get("dh"));
            p.grid = p.dh > 0;                       // hasDHgrid == dh>0 (same rule as the export)
            out.put(r.get("buurtcode"), p);
        }
        return out;
    }

    private static int rnd(double x) { return (int) Math.round(x); }

    // GZ_Neighborhood.f_setHouseholdDefaultHeatingSystem
    private static void assignInitial(Rng rng, Nbh nbh, Perc perc, Agents out) {
        List<Dwelling> nonSHB = new ArrayList<>(nbh.ho); nonSHB.addAll(nbh.ll);
        int households = nbh.ho.size() + nbh.ll.size() + nbh.sh.size();
        if (households == 0) return;
        int reqNGB = rnd(perc.gasCV * households), reqNGBl = rnd(perc.gasBlock * households);
        int reqEHP = rnd(perc.ehp * households), reqHHP = rnd(perc.hhp * households), reqDH = rnd(perc.dh * households);
        int total = reqNGB + reqNGBl + reqHHP + reqEHP + reqDH;
        while (total > households) { if (reqNGB > 0) reqNGB--; else if (reqNGBl > 0) reqNGBl--; else if (reqDH > 0) reqDH--; else if (reqHHP > 0) reqHHP--; else if (reqEHP > 0) reqEHP--; total--; }
        while (total < households) { reqNGB++; total++; }
        int setNGB = 0, setNGBl = 0, setEHP = 0, setHHP = 0, setDH = 0;
        int socialSize = nbh.sh.size();

        if (socialSize > 0) {
            HeatingSystem type;
            if (setDH + socialSize <= reqDH) { type = HeatingSystem.DISTRICT_HEATING; setDH += socialSize; }
            else if (setNGBl + socialSize <= reqNGBl) { type = HeatingSystem.NATURAL_GAS_BLOCK; setNGBl += socialSize; }
            else { double r = rng.next();
                if (r < 0.1 && setEHP + socialSize <= reqEHP) { type = HeatingSystem.ELECTRIC_HEAT_PUMP; setEHP += socialSize; }
                else if (r < 0.25 && setHHP + socialSize <= reqHHP) { type = HeatingSystem.HYBRID_HEAT_PUMP; setHHP += socialSize; }
                else { type = HeatingSystem.NATURAL_GAS_BOILER; setNGB += socialSize; } }
            int ylrS = blockYearLastRenovation(rng, nbh.sh);
            HousingBlock b = new HousingBlock(nbh.buurt, initialHsAge(rng, 2024, ylrS, type)); b.currentType = type;
            b.yearLastRenovation = ylrS; b.energyLabel = blockAvgLabel(nbh.sh);
            for (Dwelling d : nbh.sh) { d.currentType = type; b.households.add(d); }
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
            HousingBlock hoa = new HousingBlock(nbh.buurt, initialHsAge(rng, 2024, ylrH, HeatingSystem.NATURAL_GAS_BLOCK));
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
        for (Dwelling d : nbh.ho) if (d.ownership.equals("PRIVATELY_OWNED")) { d.age = initialHsAge(rng, 2024, d.constructionYear, d.currentType); out.homeowners.add(d); }
        for (Dwelling d : nbh.ll) if (d.ownership.equals("PRIVATELY_RENTED")) { d.age = initialHsAge(rng, 2024, d.constructionYear, d.currentType); out.landlords.add(d); }
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
        Agents out = new Agents();
        Path dir = Path.of(csvPath).toAbsolutePath().getParent();
        out.vesta = Vesta.load(refFile(dir, "dwellings_demand_insulation.csv").toString());
        // heating-system + energy-source specs from the reference CSVs (single source of truth,
        // generated from the data/ spreadsheets by export_reference_tables.py).
        HeatingSystemData.loadFrom(refFile(dir, "heating_system_data.csv"),
                                   refFile(dir, "energy_source_data.csv"));
        // -Dht.nbhHeating=nbh_heating_2022.csv lets a calibration run start from an observed
        // historical state (see export_observed_heating.py) instead of the default 2023 shares.
        String nbhFile = System.getProperty("ht.nbhHeating",
                System.getenv().getOrDefault("HT_NBHHEATING", "nbh_heating.csv"));
        Map<String, Perc> nbhData = loadNbh(refFile(dir, nbhFile).toString());
        Perc def = new Perc(); def.gasCV = 1;

        Map<String, Nbh> nbhs = new LinkedHashMap<>();
        try (BufferedReader br = Files.newBufferedReader(Path.of(csvPath))) {
            String[] header = br.readLine().replace("\r", "").split(",");
            Map<String, Integer> c = new HashMap<>();
            for (int i = 0; i < header.length; i++) c.put(header[i], i);
            String ln; int row = 0, id = 0;
            while ((ln = br.readLine()) != null) {
                if (ln.isEmpty()) continue;
                if (row++ % everyNth != 0) continue;
                String[] f = ln.replace("\r", "").split(",");
                double koop = Double.parseDouble(f[c.get("perc_koop")]), huur = Double.parseDouble(f[c.get("perc_huur")]), corp = Double.parseDouble(f[c.get("aantal_corp")]);
                String own = ownership(rng, koop, huur, corp);
                String label = f[c.get("label")]; if (label == null || label.isEmpty()) label = "n";
                double space = Double.parseDouble(f[c.get("space_heat_kwh")]), dhwBase = Double.parseDouble(f[c.get("dhw_base_kwh")]);
                double fac = truncNormal(rng, 1.0, 0.2, 0.5, 1.5);
                String buurt = f[c.get("buurtcode")]; String dtype = f[c.get("dwelling_type")];
                Perc perc = nbhData.getOrDefault(buurt, def);
                Dwelling d = new Dwelling(id++, HeatingSystem.NATURAL_GAS_BOILER, rng.nextInt(0, 12),
                        fac * space + fac * fac * dhwBase, label, Double.parseDouble(f[c.get("area_m2")]),
                        dtype, false, perc.grid, rng.beta(5, 2, 0, 1));   // hasLowTemp set from initial heating system below
                d.ownership = own; d.archetype = dtype; d.buurt = buurt;
                d.numid = f[c.get("numid")];
                d.constructionYear = (int) Double.parseDouble(f[c.get("construction_year")]);
                d.yearLastRenovation = d.constructionYear;
                Nbh g = nbhs.computeIfAbsent(buurt, k -> { Nbh x = new Nbh(); x.buurt = k; return x; });
                if (own.equals("PRIVATELY_OWNED")) g.ho.add(d);
                else if (own.equals("PRIVATELY_RENTED")) g.ll.add(d);
                else g.sh.add(d);
            }
        }
        java.util.Map<String, String[]> nbhCsv = loadNeighbourhoodCsv(dir);
        for (Nbh nbh : nbhs.values()) {
            assignInitial(rng, nbh, nbhData.getOrDefault(nbh.buurt, def), out);

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
