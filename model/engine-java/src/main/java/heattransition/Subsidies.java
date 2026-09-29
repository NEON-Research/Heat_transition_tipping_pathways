package heattransition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Investment subsidies per year, technology and ownership group, from
 *  data/reference/subsidies.csv (ISDE for homeowners, see the file header for all sources).
 *
 *  <p>The amount of the simulated year applies; before the table's first year its first value,
 *  after its last year the last value (held). A pair not in the table gets 0. The scenario's
 *  subsidy_policy decides how the table is used:
 *  <ul>
 *    <li>CURRENT: the table as is;</li>
 *    <li>NONE: no subsidy at all;</li>
 *    <li>PHASE_OUT: the table until the technology's share of the housing stock reaches
 *        {@link Constants#SUBSIDY_PHASE_OUT_SHARE}, then 0 for the rest of the run.</li>
 *  </ul>
 *  State (year, phased-out technologies) is per simulation: {@link #start} resets it. The table
 *  is required; a missing or malformed file aborts the run. */
public final class Subsidies {
    private Subsidies() {}

    public static final Set<String> POLICIES = Set.of("CURRENT", "NONE", "PHASE_OUT");
    private static final Set<String> OWNERSHIPS = Set.of("PRIVATELY_OWNED", "PRIVATELY_RENTED",
            "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION");

    /** technology -> ownership -> year -> amount */
    private static Map<HeatingSystem, Map<String, TreeMap<Integer, Double>>> table;
    private static String policy = "CURRENT";
    private static int year = Constants.DEFAULT_START_YEAR;
    private static final Set<HeatingSystem> phasedOut = EnumSet.noneOf(HeatingSystem.class);

    public static synchronized void loadFrom(Path csv) {
        table = null;
        if (csv == null || !Files.exists(csv))
            throw new IllegalStateException("subsidy table not found: " + csv
                    + " (export it with data-export/scripts/export_reference_tables.py)");
        Map<HeatingSystem, Map<String, TreeMap<Integer, Double>>> t = new EnumMap<>(HeatingSystem.class);
        int line = 0;
        try {
            for (Map<String, String> r : Csv.read(csv)) {
                line++;
                HeatingSystem tech;
                try { tech = HeatingSystem.valueOf(r.get("technology").trim()); }
                catch (RuntimeException e) {
                    throw new IllegalStateException(csv + " row " + line + ": unknown technology '" + r.get("technology") + "'");
                }
                String own = r.get("ownership") == null ? "" : r.get("ownership").trim();
                if (!OWNERSHIPS.contains(own))
                    throw new IllegalStateException(csv + " row " + line + ": unknown ownership '" + own + "'");
                int y; double amount;
                try { y = Integer.parseInt(r.get("year").trim()); amount = Double.parseDouble(r.get("amount_eur").trim()); }
                catch (RuntimeException e) {
                    throw new IllegalStateException(csv + " row " + line + ": year and amount_eur must be numbers");
                }
                if (amount < 0) throw new IllegalStateException(csv + " row " + line + ": negative amount");
                if (t.computeIfAbsent(tech, k -> new HashMap<>()).computeIfAbsent(own, k -> new TreeMap<>())
                        .put(y, amount) != null)
                    throw new IllegalStateException(csv + " row " + line + ": duplicate " + tech + " " + own + " " + y);
            }
        } catch (IOException e) {
            throw new RuntimeException("cannot read " + csv + ": " + e.getMessage(), e);
        }
        table = t;
    }

    private static synchronized void ensureLoaded() {
        if (table != null) return;
        for (String p : new String[]{ "../data/reference/subsidies.csv", "model/data/reference/subsidies.csv",
                                      "data/reference/subsidies.csv" }) {
            if (Files.exists(Path.of(p))) { loadFrom(Path.of(p)); return; }
        }
        throw new IllegalStateException("subsidies.csv not loaded and not found on the default search path");
    }

    /** Start of a simulation: the scenario's policy, nothing phased out yet. */
    public static synchronized void start(String subsidyPolicy, int startYear) {
        if (!POLICIES.contains(subsidyPolicy))
            throw new IllegalArgumentException("unknown subsidy policy '" + subsidyPolicy + "'");
        policy = subsidyPolicy; year = startYear; phasedOut.clear();
    }

    /** Start of a simulated year. `stockShare` is each technology's share of the housing stock
     *  (0-1) at the start of the year; under PHASE_OUT a technology that has reached the threshold
     *  loses its subsidy for good. Returns the technologies phased out THIS year (for the log). */
    public static synchronized Set<HeatingSystem> setYear(int y, Map<HeatingSystem, Double> stockShare) {
        year = y;
        Set<HeatingSystem> now = EnumSet.noneOf(HeatingSystem.class);
        if ("PHASE_OUT".equals(policy))
            for (Map.Entry<HeatingSystem, Double> e : stockShare.entrySet())
                if (e.getValue() >= Constants.SUBSIDY_PHASE_OUT_SHARE && phasedOut.add(e.getKey())) now.add(e.getKey());
        return now;
    }

    /** Subsidy for installing `tech` in a dwelling of `ownership` in the current year, EUR. */
    public static synchronized double amount(HeatingSystem tech, String ownership) {
        if ("NONE".equals(policy) || phasedOut.contains(tech)) return 0.0;
        ensureLoaded();
        Map<String, TreeMap<Integer, Double>> byOwn = table.get(tech);
        if (byOwn == null) return 0.0;
        TreeMap<Integer, Double> byYear = byOwn.get(ownership);
        if (byYear == null || byYear.isEmpty()) return 0.0;
        Map.Entry<Integer, Double> e = byYear.floorEntry(year);       // held after the last year
        return (e != null ? e : byYear.firstEntry()).getValue();      // first value before the first year
    }
}
