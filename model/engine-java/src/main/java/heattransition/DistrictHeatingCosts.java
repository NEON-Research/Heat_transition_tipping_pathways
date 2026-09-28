package heattransition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** District-heating tariffs from data/reference/district_heating_costs.csv (ACM maximum tariffs
 *  2025, EUR incl. VAT; the source is listed in the file's '#' header).
 *
 *  <p>Annual: fixed charge + metering + delivery-set rental, paid every year by a dwelling on
 *  district heating. One-off: the connection fee for a dwelling that newly connects, and the
 *  disconnection fee for a dwelling on district heating that permanently leaves for any other
 *  heating method. The variable heat price is NOT here; it comes from EnergyPrices.
 *
 *  <p>The table is required: a missing file or component aborts the run. */
public final class DistrictHeatingCosts {
    private DistrictHeatingCosts() {}

    private static final String[] COMPONENTS = { "connection_fee", "disconnection_fee",
            "fixed_charge", "metering", "delivery_set_rental" };
    private static Map<String, Double> table;

    public static synchronized void loadFrom(Path csv) {
        table = null;
        if (csv == null || !Files.exists(csv))
            throw new IllegalStateException("district heating costs not found: " + csv
                    + " (export it with data-export/scripts/export_reference_tables.py)");
        Map<String, Double> t = new HashMap<>();
        try {
            for (Map<String, String> r : Csv.read(csv)) {
                String c = r.get("component") == null ? "" : r.get("component").trim();
                String v = r.get("value") == null ? "" : r.get("value").trim();
                try {
                    if (t.put(c, Double.parseDouble(v)) != null)
                        throw new IllegalStateException(csv + ": duplicate component '" + c + "'");
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(csv + ": " + c + " must be a number, got '" + v + "'");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("cannot read " + csv + ": " + e.getMessage(), e);
        }
        for (String c : COMPONENTS)
            if (!t.containsKey(c)) throw new IllegalStateException(csv + ": missing component '" + c + "'");
        table = t;
    }

    /** Default search path, for callers that did not loadFrom() explicitly (tests, SelfTest). */
    private static synchronized double get(String component) {
        if (table == null) {
            for (String p : new String[]{ "../data/reference/district_heating_costs.csv",
                                          "model/data/reference/district_heating_costs.csv",
                                          "data/reference/district_heating_costs.csv" }) {
                if (Files.exists(Path.of(p))) { loadFrom(Path.of(p)); break; }
            }
            if (table == null) throw new IllegalStateException("district_heating_costs.csv not loaded "
                    + "and not found on the default search path");
        }
        return table.get(component);
    }

    /** One-off fee for a new connection, EUR. */
    public static double connectionFee()    { return get("connection_fee"); }
    /** One-off fee for permanently leaving district heating, EUR. */
    public static double disconnectionFee() { return get("disconnection_fee"); }
    /** Annual fixed cost of being on district heating: fixed charge + metering + delivery set, EUR/yr. */
    public static double annualFixed() {
        return get("fixed_charge") + get("metering") + get("delivery_set_rental");
    }
}
