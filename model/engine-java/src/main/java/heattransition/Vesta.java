package heattransition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** On-the-fly insulation cost from the archetype table (dwellings_demand_insulation.csv),
 *  matching AL (J_Dwelling.f_getInsulationCosts + f_insulationLabelLetterToNumber). Heat demand
 *  Heat demand is a function of the CURRENT label (spaceHeatKWh), so insulation reduces demand and
 *  therefore future running costs -- which is what makes a better-insulated dwelling cheaper to heat
 *  with a heat pump. Flat CSV (numbers + a couple of string columns). */
public final class Vesta {
    private final Map<String, List<Map<String, Double>>> byType = new HashMap<>();
    // keep bouwjaar bounds + string type per row
    private final Map<String, List<double[]>> bands = new HashMap<>();

    public static int labelNum(String l) {
        if (l == null) return 3;
        switch (l.toLowerCase()) {
            case "a": return 1; case "b": return 2; case "c": return 3; case "d": return 4;
            case "e": return 5; case "f": return 6; case "g": return 7; default: return 3;
        }
    }
    public static String numLabel(int n) {
        if (n <= 1) return "a"; if (n == 2) return "b"; if (n == 3) return "c";
        if (n == 4) return "d"; if (n == 5) return "e"; if (n == 6) return "f"; return "g";
    }

    /** Load from the archetype CSV (flat table; key = column header). */
    public static Vesta load(String csvPath) throws IOException {
        Vesta v = new Vesta();
        for (Map<String, String> row : Csv.read(Path.of(csvPath))) {
            String type = row.get("type_ol");
            if (type == null) continue;
            Map<String, Double> nums = new HashMap<>();
            double bmin = 0, bmax = 0;
            for (Map.Entry<String, String> e : row.entrySet()) {
                String s = e.getValue();
                if (s == null || s.isEmpty()) continue;
                try {
                    double d = Double.parseDouble(s);
                    nums.put(e.getKey(), d);
                    if (e.getKey().equals("bouwjaar_min")) bmin = d;
                    if (e.getKey().equals("bouwjaar_max")) bmax = d;
                } catch (NumberFormatException ex) { /* string column (type_str etc.) */ }
            }
            v.byType.computeIfAbsent(type, k -> new ArrayList<>()).add(nums);
            v.bands.computeIfAbsent(type, k -> new ArrayList<>()).add(new double[]{ bmin, bmax });
        }
        return v;
    }

    private Map<String, Double> archRow(String typeOl, int year) {
        List<Map<String, Double>> rows = byType.get(typeOl);
        List<double[]> bs = bands.get(typeOl);
        if (rows == null) return null;
        for (int i = 0; i < rows.size(); i++) if (bs.get(i)[0] <= year && year <= bs.get(i)[1]) return rows.get(i);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** Annual space-heat demand (kWh) at a given energy label, from the VestaMAIS archetype table:
     *  (vrv_<label>_asl + vrv_<label>_opp * area) / 3.6 * 1000  [GJ -> kWh], the same formula the
     *  stock export uses. Returns -1 when the archetype/label is unknown so the caller can keep the
     *  existing value. */
    public double spaceHeatKWh(String archetype, int year, String label, double area) {
        Map<String, Double> r = archRow(archetype, year);
        if (r == null) return -1;
        String l = (label == null ? "n" : label.toLowerCase());
        Double asl = r.get("vrv_" + l + "_asl"), opp = r.get("vrv_" + l + "_opp");
        if (asl == null || opp == null) { asl = r.get("vrv_n_asl"); opp = r.get("vrv_n_opp"); }
        if (asl == null || opp == null) return -1;
        return (asl + opp * area) / 3.6 * 1000.0;
    }

    /** f_getInsulationCosts: 0 if no upgrade needed (labelNum(to) >= labelNum(from)). */
    public double insulationCost(String archetype, int year, String fromLabel, String toLabel, double area) {
        if (labelNum(toLabel) >= labelNum(fromLabel)) return 0;
        Map<String, Double> r = archRow(archetype, year);
        if (r == null) return 0;
        String fl = (fromLabel == null ? "n" : fromLabel.toLowerCase());
        String col = "ki_s" + fl + toLabel;
        Double a1 = r.get(col + "_min_asl"), o1 = r.get(col + "_min_opp");
        Double a2 = r.get(col + "_max_asl"), o2 = r.get(col + "_max_opp");
        if (a1 == null || a2 == null) return 0;
        double mn = a1 + (o1 == null ? 0 : o1) * area, mx = a2 + (o2 == null ? 0 : o2) * area;
        return (mn + mx) / 2;
    }
}
