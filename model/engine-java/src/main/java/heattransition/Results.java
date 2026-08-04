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

    /** Per scenario x iteration x year x technology: the DECISION-TIME state of the two reinforcing
     *  loops (learned capex, salience, cumulative installs, energy price). Written to loop_state.csv
     *  alongside the main results so the feedback mechanisms can be plotted directly. */
    public static final String LOOP_HEADER = String.join(",",
        "scenario","scenario_name","iteration","year","heating_system",
        "learned_capex","salience","cumulative_installs","energy_price","installed_annually");

    /** Per scenario x iteration x year x segment x technology: who adopts what, and which driver
     *  carried the choice. `segment_type` is `rogers` (adopter category) or `dwelling`
     *  (archetype|area|era|label). Written to <out>_segments.csv. */
    public static final String SEG_HEADER = String.join(",",
        "scenario","scenario_name","iteration","year","segment_type","segment","heating_system",
        "stock","installed_annually","considered","avg_att","avg_sub_norm","avg_pbc","avg_util","avg_eac","avg_intention");
}
