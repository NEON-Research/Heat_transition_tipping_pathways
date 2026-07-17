package heattransition;

import java.util.List;

/** Aggregate YearResults into the EXACT simulation_results CSV schema the AnyLogic model
 *  exports, so tests/compare_to_golden.py works unchanged. Homeowner-only core ->
 *  ownership column is PRIVATELY_OWNED and TOTAL. */
public final class Results {
    private Results() {}

    public static final String HEADER = String.join(",",
        "scenario","scenario_name","iteration","year",
        "nbh_in_grid_congestion_perc","nbh_with_dh_perc","heating_system","ownership",
        "installed_current","installed_annually","removed_annually","installed_cumulative",
        "considered_annually","avg_att","avg_util","avg_sub_norm","avg_eac","avg_pbc",
        "SLF","ELF","GRR","DHCT","DHES","SHAES","DHCO","GCHPB");

    /** Append rows for one run (no header). */
    public static void appendRun(StringBuilder sb, List<YearResult> rows, Scenario s, int iteration) {
        for (YearResult r : rows) {
            for (HeatingSystem hs : HeatingSystem.values()) {
                for (String ownership : new String[]{"PRIVATELY_OWNED", "TOTAL"}) {
                    sb.append(s.scenId).append(',').append(s.scenName).append(',')
                      .append(iteration).append(',').append(r.year).append(',')
                      .append(0).append(',').append(0).append(',')
                      .append(hs).append(',').append(ownership).append(',')
                      .append(r.stock.get(hs)).append(',').append(r.installed.get(hs)).append(',')
                      .append(r.removed.get(hs)).append(',').append(r.cumInstalled.get(hs)).append(',')
                      .append(r.considered).append(',')
                      .append(0).append(',').append(0).append(',').append(0).append(',')
                      .append(0).append(',').append(0).append(',')
                      .append(s.socialLearningFactor).append(',').append(s.economicLearningFactor).append(',')
                      .append(s.gridReinforcementRate).append(',').append(s.dhConstructionTime).append(',')
                      .append(s.dhExpansionStrategy).append(',').append(s.shaStrategy).append(',')
                      .append(s.dhConnectionObligation).append(',').append(s.gridCongestionHpBan)
                      .append('\n');
                }
            }
        }
    }
}
