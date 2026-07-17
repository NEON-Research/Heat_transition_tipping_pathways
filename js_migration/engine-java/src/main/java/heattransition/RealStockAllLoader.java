package heattransition;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** Full multi-owner Limburg stock with faithful per-neighbourhood INITIAL heating assignment
 *  (GZ_Neighborhood.f_setHouseholdDefaultHeatingSystem), which also forms the HOA blocks
 *  (f_addHouseholdToHOABlock). Mirrors engine/data/loadRealStockAll.js. */
public final class RealStockAllLoader {
    private RealStockAllLoader() {}

    public static final class Agents {
        public final List<Dwelling> homeowners = new ArrayList<>();
        public final List<Dwelling> landlords = new ArrayList<>();
        public final List<HousingBlock> socialBlocks = new ArrayList<>();
        public final List<HousingBlock> hoaBlocks = new ArrayList<>();
        public Vesta vesta;
        public int total() { int n = homeowners.size() + landlords.size();
            for (HousingBlock b : socialBlocks) n += b.households.size();
            for (HousingBlock b : hoaBlocks) n += b.households.size(); return n; }
    }
    static final class Perc { double gasCV, gasBlock, ehp, hhp, dh; boolean grid; }
    static final class Nbh { String buurt; List<Dwelling> ho = new ArrayList<>(), ll = new ArrayList<>(), sh = new ArrayList<>(); }

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
        int sum = 0; for (Dwelling d : hh) sum += Vesta.labelNum(d.energyLabel); return Vesta.numLabel(Math.round((float) sum / hh.size()));
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

    // minimal parser for nbh_heating.json: {"BUxxxx":{"gasCV":..,"gasBlock":..,"ehp":..,"hhp":..,"dh":..,"hasDHgrid":true},..}
    private static Map<String, Perc> loadNbh(String path) throws Exception {
        Map<String, Perc> out = new HashMap<>();
        String t = Files.readString(Path.of(path));
        int i = 1, n = t.length();
        while (i < n) {
            if (t.charAt(i) == '"') {
                int ks = i + 1, ke = t.indexOf('"', ks); String buurt = t.substring(ks, ke);
                int ob = t.indexOf('{', ke); int cb = t.indexOf('}', ob); String body = t.substring(ob, cb + 1);
                Perc p = new Perc();
                p.gasCV = num(body, "gasCV"); p.gasBlock = num(body, "gasBlock");
                p.ehp = num(body, "ehp"); p.hhp = num(body, "hhp"); p.dh = num(body, "dh");
                p.grid = body.contains("\"hasDHgrid\":true");
                out.put(buurt, p); i = cb + 1;
            } else i++;
        }
        return out;
    }
    private static double num(String body, String key) {
        int k = body.indexOf("\"" + key + "\""); if (k < 0) return 0;
        int c = body.indexOf(':', k) + 1; int e = c; while (e < body.length() && body.charAt(e) != ',' && body.charAt(e) != '}') e++;
        try { return Double.parseDouble(body.substring(c, e).trim()); } catch (Exception ex) { return 0; }
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
        for (Dwelling d : nonSHB) { if (setNGBl >= reqNGBl) break; if (d.archetype.equals("APARTMENT")) { d.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoaHH.add(d); taken.add(d); setNGBl++; } }
        for (Dwelling d : nonSHB) { if (setNGBl >= reqNGBl) break; if (!taken.contains(d) && d.archetype.equals("TERRACED")) { d.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoaHH.add(d); taken.add(d); setNGBl++; } }
        for (Dwelling d : nonSHB) { if (setNGBl >= reqNGBl) break; if (!taken.contains(d)) { d.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoaHH.add(d); taken.add(d); setNGBl++; } }
        if (!hoaHH.isEmpty()) { int ylrH = blockYearLastRenovation(rng, hoaHH);
            HousingBlock hoa = new HousingBlock(nbh.buurt, initialHsAge(rng, 2024, ylrH, HeatingSystem.NATURAL_GAS_BLOCK));
            hoa.yearLastRenovation = ylrH; hoa.energyLabel = blockAvgLabel(hoaHH);
            hoa.currentType = HeatingSystem.NATURAL_GAS_BLOCK; hoa.households.addAll(hoaHH);
            for (Dwelling d : hoaHH) d.ownership = "HOME_OWNER_ASSOCIATION"; out.hoaBlocks.add(hoa); }

        List<Dwelling> rem = new ArrayList<>(); for (Dwelling d : nonSHB) if (!taken.contains(d)) rem.add(d);
        int i = 0, remDH = reqDH - setDH, remEHP = reqEHP - setEHP, remHHP = reqHHP - setHHP;
        while (remDH > 0 && i < rem.size()) { rem.get(i++).currentType = HeatingSystem.DISTRICT_HEATING; remDH--; }
        while (remEHP > 0 && i < rem.size()) { rem.get(i++).currentType = HeatingSystem.ELECTRIC_HEAT_PUMP; remEHP--; }
        while (remHHP > 0 && i < rem.size()) { rem.get(i++).currentType = HeatingSystem.HYBRID_HEAT_PUMP; remHHP--; }
        while (i < rem.size()) rem.get(i++).currentType = HeatingSystem.NATURAL_GAS_BOILER;

        for (Dwelling d : nbh.ho) if (d.ownership.equals("PRIVATELY_OWNED")) { d.age = initialHsAge(rng, 2024, d.constructionYear, d.currentType); out.homeowners.add(d); }
        for (Dwelling d : nbh.ll) if (d.ownership.equals("PRIVATELY_RENTED")) { d.age = initialHsAge(rng, 2024, d.constructionYear, d.currentType); out.landlords.add(d); }
    }

    public static Agents load(String csvPath, Rng rng, int everyNth) throws Exception {
        Agents out = new Agents();
        Path dir = Path.of(csvPath).toAbsolutePath().getParent();
        out.vesta = Vesta.load(dir.resolve("dwellings_demand_insulation.json").toString());
        Map<String, Perc> nbhData = loadNbh(dir.resolve("nbh_heating.json").toString());
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
                        dtype, label.equals("a") || label.equals("b"), perc.grid, rng.beta(5, 2, 0, 1));
                d.ownership = own; d.archetype = dtype; d.buurt = buurt;
                d.constructionYear = (int) Double.parseDouble(f[c.get("construction_year")]);
                d.yearLastRenovation = d.constructionYear;
                Nbh g = nbhs.computeIfAbsent(buurt, k -> { Nbh x = new Nbh(); x.buurt = k; return x; });
                if (own.equals("PRIVATELY_OWNED")) g.ho.add(d);
                else if (own.equals("PRIVATELY_RENTED")) g.ll.add(d);
                else g.sh.add(d);
            }
        }
        for (Nbh nbh : nbhs.values()) assignInitial(rng, nbh, nbhData.getOrDefault(nbh.buurt, def), out);
        return out;
    }
}
