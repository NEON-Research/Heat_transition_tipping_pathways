package heattransition;

import java.nio.file.Path;

/** Scenario switches, from the AnyLogic scenario matrix (= JS SCEN table). Learning factors and
 *  the DH/SHA strategy + obligation/congestion flags. Use {@link #byName(String)} to build one. */
public final class Scenario {
    public int scenId = 1;
    public String scenName = "baseline";
    public String socialLearningFactor = "MEDIUM";
    public String economicLearningFactor = "MEDIUM";
    public String gridReinforcementRate = "MEDIUM";
    /** Energy-price arm. NOT a price level: the arm moves the two fuels to OPPOSITE corners of
     *  their KEV bands, so what it varies is the spark spread that drives fuel switching.
     *    GAS_LOW_ELEC_HIGH  -> priceT 0    spread ~2.7, favours staying on gas
     *    CENTRAL            -> priceT 0.5  spread ~2.0
     *    GAS_HIGH_ELEC_LOW  -> priceT 1    spread ~1.5, favours heat pumps */
    public String energyPrices = "CENTRAL";
    public int dhConstructionTime = 5;
    public String dhExpansionStrategy = "COST_BASED";   // or POLICY_BASED
    public String shaStrategy = "COST_BASED";           // or POLICY_BASED (social-housing strategy)
    public boolean dhConnectionObligation = false;
    public boolean gridCongestionHpBan = false;

    /** The scenario's energy-price arm as the continuous knob EnergyPrices blends on:
     *  LOW = 0 (both fuels at the low arm), MEDIUM = 0.5 (central), HIGH = 1. */
    public double priceT() {
        switch (energyPrices) {
            case "GAS_LOW_ELEC_HIGH": return 0.0;
            case "GAS_HIGH_ELEC_LOW": return 1.0;
            default:                  return 0.5;   // CENTRAL
        }
    }

    public Scenario() {}
    public Scenario(int id, String name, String slf, String elf) {
        this.scenId = id; this.scenName = name; this.socialLearningFactor = slf; this.economicLearningFactor = elf;
    }

    // ---- Loaded from data/reference/scenario_settings.csv -------------------------------------
    // The matrix is DATA, not code: the CSV is the single source of truth and the engine refuses to
    // run without it. Parsing is strict on purpose -- an unknown column, an unknown enum value, a
    // duplicate name or a bad status is a hard error, because the failure this design exists to
    // prevent is a scenario silently differing from what the table says.

    private static final java.util.Set<String> LEVEL = java.util.Set.of("LOW", "MEDIUM", "HIGH");
    private static final java.util.Set<String> STRATEGY = java.util.Set.of("COST_BASED", "POLICY_BASED");
    private static final java.util.Set<String> PRICE_ARM =
            java.util.Set.of("GAS_LOW_ELEC_HIGH", "CENTRAL", "GAS_HIGH_ELEC_LOW");
    private static final String[] REQUIRED = {
        "scen_id", "scen_name", "social_learning_factor", "economic_learning_factor",
        "grid_reinforcement_rate", "energy_prices", "DH_construction_time", "DH_expansion_strategy",
        "SHA_strategy", "DH_connection_obligation", "Grid_congestion_HP_ban", "status" };

    private static java.util.LinkedHashMap<String, Scenario> TABLE;   // every row, by name
    private static String[] activeNames;                              // status == active, in file order
    private static String[] retiredNames;
    private static Path source;

    /** The scenario matrix `--scenario all` runs: the rows marked active, in file order. */
    public static String[] names() { ensureLoaded(); return activeNames.clone(); }
    /** Rows marked retired: still buildable by name so archived runs reproduce, never iterated. */
    public static String[] retired() { ensureLoaded(); return retiredNames.clone(); }
    /** Where the matrix was read from, for the run log. */
    public static String sourcePath() { ensureLoaded(); return String.valueOf(source); }

    /** Read the matrix. Call before byName() to control the path; otherwise the default is used. */
    public static synchronized void loadFrom(Path csv) {
        java.util.List<java.util.Map<String, String>> rows;
        try {
            rows = Csv.read(csv);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("cannot read the scenario matrix at " + csv
                + " -- the engine will not guess it: " + e.getMessage(), e);
        }
        if (rows.isEmpty()) throw new IllegalStateException("scenario matrix is empty: " + csv);
        for (String c : REQUIRED)
            if (!rows.get(0).containsKey(c))
                throw new IllegalStateException("scenario matrix " + csv + " is missing column '" + c + "'");

        java.util.LinkedHashMap<String, Scenario> t = new java.util.LinkedHashMap<>();
        java.util.List<String> act = new java.util.ArrayList<>(), ret = new java.util.ArrayList<>();
        int line = 1;
        for (java.util.Map<String, String> r : rows) {
            line++;
            String name = r.get("scen_name").trim();
            if (name.isEmpty()) continue;
            Scenario s = new Scenario();
            s.scenName = name;
            s.scenId = (int) num(csv, line, "scen_id", r.get("scen_id"));
            s.socialLearningFactor   = level(csv, line, "social_learning_factor", r);
            s.economicLearningFactor = level(csv, line, "economic_learning_factor", r);
            s.gridReinforcementRate  = level(csv, line, "grid_reinforcement_rate", r);
            s.energyPrices           = priceArm(csv, line, "energy_prices", r);
            s.dhConstructionTime = (int) num(csv, line, "DH_construction_time", r.get("DH_construction_time"));
            s.dhExpansionStrategy = strategy(csv, line, "DH_expansion_strategy", r);
            s.shaStrategy         = strategy(csv, line, "SHA_strategy", r);
            s.dhConnectionObligation = bool(csv, line, "DH_connection_obligation", r);
            s.gridCongestionHpBan    = bool(csv, line, "Grid_congestion_HP_ban", r);
            if (t.put(name, s) != null)
                throw new IllegalStateException(csv + " line " + line + ": duplicate scenario '" + name + "'");
            String st = r.get("status").trim().toLowerCase();
            if (st.equals("active")) act.add(name);
            else if (st.equals("retired")) ret.add(name);
            else throw new IllegalStateException(csv + " line " + line + ": status must be 'active' or "
                    + "'retired', got '" + r.get("status") + "'");
        }
        if (act.isEmpty()) throw new IllegalStateException("scenario matrix has no active rows: " + csv);
        TABLE = t; activeNames = act.toArray(new String[0]); retiredNames = ret.toArray(new String[0]);
        source = csv;
    }

    private static String level(Path f, int line, String col, java.util.Map<String, String> r) {
        String v = String.valueOf(r.get(col)).trim().toUpperCase();
        if (!LEVEL.contains(v)) throw new IllegalStateException(f + " line " + line + ": " + col
            + " must be LOW/MEDIUM/HIGH, got '" + r.get(col) + "'");
        return v;
    }
    private static String priceArm(Path f, int line, String col, java.util.Map<String, String> r) {
        String v = String.valueOf(r.get(col)).trim().toUpperCase();
        if (!PRICE_ARM.contains(v)) throw new IllegalStateException(f + " line " + line + ": " + col
            + " must be GAS_LOW_ELEC_HIGH / CENTRAL / GAS_HIGH_ELEC_LOW (it is a spark-spread arm, "
            + "not a price level), got '" + r.get(col) + "'");
        return v;
    }
    private static String strategy(Path f, int line, String col, java.util.Map<String, String> r) {
        String v = String.valueOf(r.get(col)).trim().toUpperCase();
        if (!STRATEGY.contains(v)) throw new IllegalStateException(f + " line " + line + ": " + col
            + " must be COST_BASED/POLICY_BASED, got '" + r.get(col) + "'");
        return v;
    }
    private static boolean bool(Path f, int line, String col, java.util.Map<String, String> r) {
        String v = String.valueOf(r.get(col)).trim().toUpperCase();
        if (v.equals("TRUE") || v.equals("1")) return true;
        if (v.equals("FALSE") || v.equals("0")) return false;
        throw new IllegalStateException(f + " line " + line + ": " + col
            + " must be TRUE/FALSE, got '" + r.get(col) + "'");
    }
    private static double num(Path f, int line, String col, String v) {
        try { return Double.parseDouble(String.valueOf(v).trim()); }
        catch (NumberFormatException e) {
            throw new IllegalStateException(f + " line " + line + ": " + col + " must be a number, got '" + v + "'");
        }
    }

    /** Default search path, for callers that did not loadFrom() explicitly (tests, SelfTest). */
    private static synchronized void ensureLoaded() {
        if (TABLE != null) return;
        for (String p : new String[]{ "../data/reference/scenario_settings.csv",
                                      "model/data/reference/scenario_settings.csv",
                                      "data/reference/scenario_settings.csv" }) {
            Path c = Path.of(p);
            if (java.nio.file.Files.exists(c)) { loadFrom(c); return; }
        }
        throw new IllegalStateException("scenario matrix not found; pass --scenarios <file> or run "
            + "from the engine directory (looked for data/reference/scenario_settings.csv)");
    }

    /** One line per active scenario, for the run log -- so a result CSV can be traced to its matrix. */
    public static String describeMatrix() {
        ensureLoaded();
        StringBuilder b = new StringBuilder("[scenarios] ").append(activeNames.length)
            .append(" active, ").append(retiredNames.length).append(" retired  <- ").append(source).append('\n');
        for (String n : activeNames) {
            Scenario s = TABLE.get(n);
            b.append(String.format("  %-3d %-38s slf=%-6s elf=%-6s price=%-17s dh=%-12s sha=%-12s obligation=%-5b hpBan=%b%n",
                s.scenId, s.scenName, s.socialLearningFactor, s.economicLearningFactor, s.energyPrices,
                s.dhExpansionStrategy, s.shaStrategy, s.dhConnectionObligation, s.gridCongestionHpBan));
        }
        return b.toString();
    }

    /** The active matrix as JSON -- the array the deprecated JS engine takes via --scenarios. */
    public static String toJson() {
        ensureLoaded();
        StringBuilder b = new StringBuilder("[\n");
        for (int i = 0; i < activeNames.length; i++) {
            Scenario s = TABLE.get(activeNames[i]);
            b.append(String.format("  {\"scenId\": %d, \"scenName\": \"%s\", \"socialLearningFactor\": \"%s\", "
                + "\"economicLearningFactor\": \"%s\", \"gridReinforcementRate\": \"%s\", "
                + "\"energyPrices\": \"%s\", "
                + "\"dhConstructionTime\": %d, \"dhExpansionStrategy\": \"%s\", \"shaStrategy\": \"%s\", "
                + "\"dhConnectionObligation\": %b, \"gridCongestionHpBan\": %b}%s%n",
                s.scenId, s.scenName, s.socialLearningFactor, s.economicLearningFactor,
                s.gridReinforcementRate, s.energyPrices, s.dhConstructionTime, s.dhExpansionStrategy, s.shaStrategy,
                s.dhConnectionObligation, s.gridCongestionHpBan, i < activeNames.length - 1 ? "," : ""));
        }
        return b.append("]").toString();
    }

    /** Build the scenario for a name. Returns a COPY, so a caller mutating it cannot corrupt the
     *  loaded table. Throws on an unknown name, listing what the matrix does contain. */
    public static Scenario byName(String name) {
        ensureLoaded();
        Scenario src = TABLE.get(name);
        if (src == null) throw new IllegalArgumentException("unknown scenario: " + name
            + " (active: " + String.join(", ", activeNames)
            + "; retired but still buildable: " + String.join(", ", retiredNames) + ")");
        Scenario s = new Scenario();
        s.scenId = src.scenId; s.scenName = src.scenName;
        s.socialLearningFactor = src.socialLearningFactor;
        s.economicLearningFactor = src.economicLearningFactor;
        s.gridReinforcementRate = src.gridReinforcementRate;
        s.energyPrices = src.energyPrices;
        s.dhConstructionTime = src.dhConstructionTime;
        s.dhExpansionStrategy = src.dhExpansionStrategy;
        s.shaStrategy = src.shaStrategy;
        s.dhConnectionObligation = src.dhConnectionObligation;
        s.gridCongestionHpBan = src.gridCongestionHpBan;
        return s;
    }
}
