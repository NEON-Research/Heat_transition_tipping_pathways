package heattransition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal CSV reader for the reference tables: header row + flat rows, quoted fields with ""
 *  escapes (so values like "LT, HT" survive), BOM/CRLF tolerant. No embedded newlines (the
 *  reference data has none). Returns header-keyed rows. */
final class Csv {
    private Csv() {}

    static List<Map<String, String>> read(Path path) throws IOException {
        List<Map<String, String>> rows = new ArrayList<>();
        List<String> lines = Files.readAllLines(path);
        if (lines.isEmpty()) return rows;
        List<String> header = parseLine(lines.get(0).replace("\uFEFF", ""));
        for (int li = 1; li < lines.size(); li++) {
            String line = lines.get(li);
            if (line.isEmpty()) continue;
            List<String> vals = parseLine(line);
            Map<String, String> m = new LinkedHashMap<>();
            for (int i = 0; i < header.size(); i++) m.put(header.get(i), i < vals.size() ? vals.get(i) : "");
            rows.add(m);
        }
        return rows;
    }

    static List<String> parseLine(String s) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean q = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (q) {
                if (c == '"') {
                    if (i + 1 < s.length() && s.charAt(i + 1) == '"') { cur.append('"'); i++; }
                    else q = false;
                } else cur.append(c);
            } else {
                if (c == '"') q = true;
                else if (c == ',') { out.add(cur.toString()); cur.setLength(0); }
                else cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }

    /** Parse to double, treating null/empty/non-numeric as 0. */
    static double d(String v) {
        if (v == null || v.isEmpty()) return 0.0;
        try { return Double.parseDouble(v); } catch (NumberFormatException e) { return 0.0; }
    }
}
