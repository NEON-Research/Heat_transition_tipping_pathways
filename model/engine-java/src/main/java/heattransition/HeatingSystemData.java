package heattransition;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Heating-system + energy-source specs, loaded from the reference CSVs that are generated from the
 *  top-level data/ spreadsheets (heating_system_data.csv + energy_source_data.csv) by
 *  export_reference_tables.py -- the single source of truth. Values are NOT hardcoded here.
 *  Costs EUR, demand kWh. requiresLowTemp = (required_distribution_system == "LT"). */
public final class HeatingSystemData {
    private HeatingSystemData() {}

    // Loaded once (idempotent) before any Simulation is built; see StockLoader.load / ensureLoaded.
    private static List<Map<String, String>> hsRows;     // heating_system_data rows
    private static Map<String, Double> energyCost;       // energy_source -> costs_eur_per_k_wh

    /** Load the two reference tables from their CSV files (called with the run's reference dir). */
    public static synchronized void loadFrom(Path heatingSystemCsv, Path energySourceCsv) {
        try {
            hsRows = Csv.read(heatingSystemCsv);
            Map<String, Double> ec = new HashMap<>();
            for (Map<String, String> r : Csv.read(energySourceCsv))
                ec.put(r.get("energy_source"), Csv.d(r.get("costs_eur_per_k_wh")));
            energyCost = ec;
        } catch (IOException e) {
            throw new RuntimeException("cannot read heating/energy reference CSV: " + e.getMessage(), e);
        }
    }

    /** Fallback for callers that didn't loadFrom() explicitly (tests run from engine-java). */
    private static synchronized void ensureLoaded() {
        if (hsRows == null || energyCost == null)
            loadFrom(Path.of("../data/reference/heating_system_data.csv"),
                     Path.of("../data/reference/energy_source_data.csv"));
    }

    /** Fresh set of specs (mutable state per simulation, so build a new one per run). */
    public static Map<HeatingSystem, HeatingSystemSpec> freshSpecs() {
        ensureLoaded();
        Map<HeatingSystem, HeatingSystemSpec> m = new EnumMap<>(HeatingSystem.class);
        for (Map<String, String> r : hsRows) {
            HeatingSystem type;
            try { type = HeatingSystem.valueOf(r.get("type")); }
            catch (IllegalArgumentException | NullPointerException e) { continue; }   // skip non-enum rows
            String primary = r.get("energy_source_primary");
            String secondary = r.get("energy_source_secondary");
            double capexF = 1.0;                       // capex sensitivity multipliers
            if (type == HeatingSystem.HYBRID_HEAT_PUMP || type == HeatingSystem.ELECTRIC_HEAT_PUMP)
                capexF = Constants.CAPEX_MULT_HP;
            else if (type == HeatingSystem.DISTRICT_HEATING)
                capexF = Constants.CAPEX_MULT_DH;
            m.put(type, new HeatingSystemSpec(type,
                    capexF * Csv.d(r.get("investment_costs_eur_per_unit_small")),
                    capexF * Csv.d(r.get("investment_costs_eur_per_unit_medium")),
                    capexF * Csv.d(r.get("investment_costs_eur_per_unit_high")),
                    Csv.d(r.get("investment_costs_heat_distribution_system_eur")),
                    "LT".equals(r.get("required_distribution_system")),
                    r.get("required_energy_label"),
                    Csv.d(r.get("maintenance_costs_eur_per_year")),
                    (int) Csv.d(r.get("lifetime_years")),
                    Csv.d(r.get("discount_rate")),
                    Constants.LEARNING_RATE_MULT * Csv.d(r.get("economic_learning_rate_per_unit")),
                    primary, secondary,
                    Csv.d(r.get("efficiency_primary_source")),
                    Csv.d(r.get("efficiency_secondary_source")),
                    Csv.d(r.get("fraction_primary_energy_source")),
                    Csv.d(r.get("fraction_secondary_energy_source")),
                    energyCost.getOrDefault(primary, 0.0),
                    energyCost.getOrDefault(secondary, 0.0),
                    Csv.d(r.get("subsidy_eur")),
                    (int) Csv.d(r.get("sustainability_score")),
                    Csv.d(r.get("social_learning_rate"))));
        }
        // min-max sustainability normalization across all systems (AL f_setHeatingSystemOptions)
        int minS = Integer.MAX_VALUE, maxS = Integer.MIN_VALUE;
        for (HeatingSystemSpec sp : m.values()) { minS = Math.min(minS, sp.sustainabilityScore); maxS = Math.max(maxS, sp.sustainabilityScore); }
        for (HeatingSystemSpec sp : m.values()) sp.sustainabilityScoreNorm = (double) (sp.sustainabilityScore - minS) / (maxS - minS);
        return m;
    }
}
