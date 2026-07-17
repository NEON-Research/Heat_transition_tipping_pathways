package heattransition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** On-the-fly insulation cost from the archetype table (dwellings_demand_insulation.json),
 *  matching AL (J_Dwelling.f_getInsulationCosts + f_insulationLabelLetterToNumber). Heat demand
 *  stays frozen at the initial label; insulation cost uses the dwelling's CURRENT label so
 *  exogenous insulation flows through. Minimal JSON parsing (numbers/strings only). */
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

    /** Load from the archetype JSON (array of flat objects). */
    public static Vesta load(String jsonPath) throws IOException {
        String txt = Files.readString(Path.of(jsonPath));
        Vesta v = new Vesta();
        // split top-level objects: naive but the file is flat {..},{..}
        int i = 0, n = txt.length();
        while (i < n) {
            if (txt.charAt(i) == '{') {
                int depth = 0, start = i;
                while (i < n) { char c = txt.charAt(i); if (c == '{') depth++; else if (c == '}') { depth--; if (depth == 0) { i++; break; } } i++; }
                v.addRow(txt.substring(start, i));
            } else i++;
        }
        return v;
    }
    private void addRow(String obj) {
        Map<String, Double> nums = new HashMap<>();
        String type = null; double bmin = 0, bmax = 0;
        // parse "key":value pairs
        int i = 0, n = obj.length();
        while (i < n) {
            if (obj.charAt(i) == '"') {
                int ks = i + 1; int ke = obj.indexOf('"', ks); String key = obj.substring(ks, ke);
                int c = obj.indexOf(':', ke); int j = c + 1;
                while (j < n && Character.isWhitespace(obj.charAt(j))) j++;
                if (obj.charAt(j) == '"') { int ve = obj.indexOf('"', j + 1); String sval = obj.substring(j + 1, ve);
                    if (key.equals("type_ol")) type = sval; i = ve + 1; }
                else { int ve = j; while (ve < n && obj.charAt(ve) != ',' && obj.charAt(ve) != '}') ve++;
                    String sval = obj.substring(j, ve).trim();
                    try { double d = Double.parseDouble(sval); nums.put(key, d);
                        if (key.equals("bouwjaar_min")) bmin = d; if (key.equals("bouwjaar_max")) bmax = d; } catch (Exception e) {}
                    i = ve; }
            } else i++;
        }
        if (type != null) {
            byType.computeIfAbsent(type, k -> new ArrayList<>()).add(nums);
            bands.computeIfAbsent(type, k -> new ArrayList<>()).add(new double[]{ bmin, bmax });
        }
    }

    private Map<String, Double> archRow(String typeOl, int year) {
        List<Map<String, Double>> rows = byType.get(typeOl);
        List<double[]> bs = bands.get(typeOl);
        if (rows == null) return null;
        for (int i = 0; i < rows.size(); i++) if (bs.get(i)[0] <= year && year <= bs.get(i)[1]) return rows.get(i);
        return rows.isEmpty() ? null : rows.get(0);
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
