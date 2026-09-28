package heattransition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Year x arm energy prices and fixed connection costs, in CONSTANT 2024 EUROS.
 *
 *  Loaded from data/reference/energy_prices_by_year.csv, which is built by
 *  results_analysis/build_energy_price_table.py from CBS 85592NED (observed 2021..now),
 *  KEV 2025 Table 1.5 (forward arms) and the ACM maximum heat tariffs.
 *
 *  <p>Two things this replaces, and why:
 *  <ul>
 *    <li><b>The single retail CAGR.</b> The old path scaled the whole retail price by
 *        (1+g)^(y-y0), which escalates the energy tax and the network charge at the commodity's
 *        rate. Here retail = commodity + tax, and only the commodity differs between arms; the
 *        tax is held at its last observed level. It also lets the path be a ramp to 2030 and flat
 *        afterwards, which a constant growth rate cannot express and which is what KEV supports.</li>
 *    <li><b>Missing fixed connection costs.</b> A dwelling that leaves gas stops paying the gas
 *        standing charge; a district-heating connection carries a much larger one. The electricity
 *        connection is kept whatever the dwelling heats with, so it cancels between technologies
 *        and is deliberately NOT charged here.</li>
 *  </ul>
 *
 *  <p>The arm is selected by a single continuous knob t in [0,1] (the scenario's energy_prices
 *  column, or {@code -Dht.priceT}): 0 = low, 0.5 = central, 1 = high, linearly interpolated in
 *  between, so a Latin-hypercube sample can sweep it. The arms are spark-spread corners, not price
 *  levels: "low" is gas low / electricity high, "high" is gas high / electricity low.
 *
 *  <p>The table is required. A missing file, a missing arm or year, or an unparseable value aborts
 *  the run rather than silently changing the prices. */
public final class EnergyPrices {
    private EnergyPrices() {}

    private static final String[] ARMS = { "low", "central", "high" };
    // The table also carries heat_fixed / heat_afleverset columns; the engine takes the district-
    // heating fixed costs from DistrictHeatingCosts (ACM 2025) instead, so they are not read here.
    private static final String[] COLUMNS = { "gas_retail_eur_kwh", "elec_retail_eur_kwh",
            "heat_variable_eur_kwh", "gas_fixed_eur_year" };

    private static final class Row {
        double gasRetail, elecRetail, heatVar, gasFixed;
    }

    /** arm -> year -> row */
    private static Map<String, Map<Integer, Row>> table;
    private static int minYear = Integer.MAX_VALUE, maxYear = Integer.MIN_VALUE;

    /** Load and validate the table. Every arm must cover the same contiguous run of years and
     *  every price column must hold a number; anything else throws. */
    public static synchronized void loadFrom(Path csv) {
        table = null; minYear = Integer.MAX_VALUE; maxYear = Integer.MIN_VALUE;
        if (csv == null || !Files.exists(csv))
            throw new IllegalStateException("energy price table not found: " + csv
                    + " (build it with results_analysis/build_energy_price_table.py)");
        List<Map<String, String>> rows;
        try {
            rows = Csv.read(csv);
        } catch (IOException e) {
            throw new RuntimeException("cannot read " + csv + ": " + e.getMessage(), e);
        }
        Map<String, Map<Integer, Row>> t = new HashMap<>();
        int line = 0;
        for (Map<String, String> r : rows) {
            line++;
            String arm = r.get("arm");
            if (arm == null || arm.trim().isEmpty())
                throw new IllegalStateException(csv + " data row " + line + ": empty 'arm'");
            arm = arm.trim();
            int y = (int) num(csv, line, "year", r.get("year"));
            Row row = new Row();
            row.gasRetail  = num(csv, line, COLUMNS[0], r.get(COLUMNS[0]));
            row.elecRetail = num(csv, line, COLUMNS[1], r.get(COLUMNS[1]));
            row.heatVar    = num(csv, line, COLUMNS[2], r.get(COLUMNS[2]));
            row.gasFixed   = num(csv, line, COLUMNS[3], r.get(COLUMNS[3]));
            if (t.computeIfAbsent(arm, k -> new HashMap<>()).put(y, row) != null)
                throw new IllegalStateException(csv + " data row " + line + ": duplicate " + arm + " " + y);
            minYear = Math.min(minYear, y); maxYear = Math.max(maxYear, y);
        }
        for (String arm : ARMS) {
            Map<Integer, Row> m = t.get(arm);
            if (m == null) throw new IllegalStateException(csv + ": no rows for arm '" + arm + "'");
            for (int y = minYear; y <= maxYear; y++)
                if (!m.containsKey(y))
                    throw new IllegalStateException(csv + ": arm '" + arm + "' has no year " + y);
        }
        table = t;
    }

    private static double num(Path f, int line, String col, String v) {
        if (v == null || v.trim().isEmpty())
            throw new IllegalStateException(f + " data row " + line + ": missing column '" + col + "'");
        try { return Double.parseDouble(v.trim()); }
        catch (NumberFormatException e) {
            throw new IllegalStateException(f + " data row " + line + ": " + col + " must be a number, got '" + v + "'");
        }
    }

    /** Default search path, for callers that did not loadFrom() explicitly (tests, SelfTest). */
    private static synchronized void ensureLoaded() {
        if (table != null) return;
        for (String p : new String[]{ "../data/reference/energy_prices_by_year.csv",
                                      "model/data/reference/energy_prices_by_year.csv",
                                      "data/reference/energy_prices_by_year.csv" }) {
            Path c = Path.of(p);
            if (Files.exists(c)) { loadFrom(c); return; }
        }
        throw new IllegalStateException("energy price table not loaded and energy_prices_by_year.csv "
                + "not found on the default search path");
    }

    private static Row row(String arm, int year) {
        ensureLoaded();
        // clamp outside the table: before the first year use the first, after the last use the last
        return table.get(arm).get(Math.max(minYear, Math.min(maxYear, year)));
    }

    /** Blend low/central/high on t in [0,1] (0 = low, 0.5 = central, 1 = high). */
    private static double blend(int year, double t, java.util.function.ToDoubleFunction<Row> f) {
        double tt = Math.max(0.0, Math.min(1.0, t));
        if (tt <= 0.5) {
            double u = tt / 0.5;
            return f.applyAsDouble(row("low", year)) * (1 - u) + f.applyAsDouble(row("central", year)) * u;
        }
        double u = (tt - 0.5) / 0.5;
        return f.applyAsDouble(row("central", year)) * (1 - u) + f.applyAsDouble(row("high", year)) * u;
    }

    /** Retail variable price in EUR/kWh for an energy_source_data source name. */
    public static double variable(String source, int year, double t) {
        if (source == null) return 0.0;
        switch (source) {
            case "NATURAL_GAS": return blend(year, t, r -> r.gasRetail);
            case "ELECTRICITY": return blend(year, t, r -> r.elecRetail);
            case "HEAT":        return blend(year, t, r -> r.heatVar);
            default:            return 0.0;
        }
    }

    /** Annual gas connection charge (transport + vastrecht), EUR/yr, constant 2024 EUR. */
    public static double gasFixed(int year, double t) { return blend(year, t, r -> r.gasFixed); }

}
