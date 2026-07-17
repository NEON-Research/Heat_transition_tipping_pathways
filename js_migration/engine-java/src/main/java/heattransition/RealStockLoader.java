package heattransition;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Load the REAL Limburg homeowner stock from data-export/out/limburg_dwellings.csv.
 *  Mirrors the JS loadRealStock.js and is faithful to J_Dwelling / f_createDwellings:
 *   - ownership via f_getDwellingOwnership (validated: reproduces golden 65/19/15 split)
 *   - heatDemand = f*space + f*f*dhwBase, f ~ truncated Normal(1, 0.2) in [0.5,1.5]
 *  Homeowner core -> keeps PRIVATELY_OWNED only. */
public final class RealStockLoader {
    private RealStockLoader() {}

    private static double truncNormal(Rng rng, double mean, double sd, double lo, double hi) {
        for (int i = 0; i < 100; i++) {
            double u = 0, v = 0;
            while (u == 0) u = rng.next();
            while (v == 0) v = rng.next();
            double z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
            double x = mean + sd * z;
            if (x >= lo && x <= hi) return x;
        }
        return mean;
    }

    private static String ownership(Rng rng, double koop, double huur, double corp) {
        double hba = (huur - corp) / 100;
        double oll = (huur - hba) / 100;
        double r = rng.next();
        if (r < hba) return "SOCIAL_HOUSING";
        if (r < hba + oll) return "PRIVATELY_RENTED";
        return "PRIVATELY_OWNED";
    }

    public static List<Dwelling> loadHomeowners(String csvPath, Rng rng, int everyNth) throws Exception {
        List<Dwelling> out = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Path.of(csvPath))) {
            String headerLine = br.readLine();
            String[] header = headerLine.replace("\r", "").split(",");
            Map<String, Integer> col = new HashMap<>();
            for (int i = 0; i < header.length; i++) col.put(header[i], i);
            String ln; int row = 0, id = 0;
            while ((ln = br.readLine()) != null) {
                if (ln.isEmpty()) continue;
                if (row++ % everyNth != 0) continue;
                String[] f = ln.replace("\r", "").split(",");
                double koop = Double.parseDouble(f[col.get("perc_koop")]);
                double huur = Double.parseDouble(f[col.get("perc_huur")]);
                double corp = Double.parseDouble(f[col.get("aantal_corp")]);
                if (!ownership(rng, koop, huur, corp).equals("PRIVATELY_OWNED")) continue;
                String dtype = f[col.get("dwelling_type")];
                double area = Double.parseDouble(f[col.get("area_m2")]);
                String label = f[col.get("label")];
                if (label == null || label.isEmpty()) label = "n";
                double space = Double.parseDouble(f[col.get("space_heat_kwh")]);
                double dhwBase = Double.parseDouble(f[col.get("dhw_base_kwh")]);
                double fac = truncNormal(rng, 1.0, 0.2, 0.5, 1.5);
                double heatDemand = fac * space + fac * fac * dhwBase;
                String typeCat = (dtype.equals("APARTMENT") || dtype.equals("HIGHRISE")) ? "APARTMENT" : "HOUSE";
                boolean lowTemp = label.equals("a") || label.equals("b");
                out.add(new Dwelling(id++, HeatingSystem.NATURAL_GAS_BOILER, rng.nextInt(0, 12),
                        heatDemand, label, area, typeCat, lowTemp, false, rng.beta(5, 2, 0, 1)));
            }
        }
        return out;
    }
}
