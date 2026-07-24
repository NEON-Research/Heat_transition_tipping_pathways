package heattransition;

/** The EXACT simulation_results CSV column schema the AnyLogic model exported, so downstream
 *  analysis (results_analysis/script_results.py, tests/compare_to_golden.py) works unchanged.
 *  Cli builds the rows itself (per-owner: PRIVATELY_OWNED / PRIVATELY_RENTED / SOCIAL_HOUSING /
 *  HOME_OWNER_ASSOCIATION / TOTAL); this class only owns the header line. */
public final class Results {
    private Results() {}

    public static final String HEADER = String.join(",",
        "scenario","scenario_name","iteration","year",
        "nbh_in_grid_congestion_perc","nbh_with_dh_perc","heating_system","ownership",
        "installed_current","installed_annually","removed_annually","installed_cumulative",
        "considered_annually","avg_att","avg_util","avg_sub_norm","avg_eac","avg_pbc",
        "SLF","ELF","GRR","DHCT","DHES","SHAES","DHCO","GCHPB");
}
