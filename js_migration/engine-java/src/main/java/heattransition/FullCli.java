package heattransition;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Multi-owner headless runner (social blocks + landlords + homeowners).
 *  Usage: java heattransition.FullCli --real ..\data-export\out\limburg_dwellings.csv
 *         [--scenario baseline] [--every 1] [--iterations 1] [--out file.csv] */
public final class FullCli {
    static String arg(String[] a, String n, String d) {
        for (int i = 0; i < a.length; i++) if (a[i].equals("--" + n))
            return (i + 1 < a.length && !a[i + 1].startsWith("--")) ? a[i + 1] : "true";
        return d;
    }
    static final String[] OWN_OUT = { "PRIVATELY_OWNED", "PRIVATELY_RENTED", "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION", "TOTAL" };

    public static void main(String[] args) throws Exception {
        String csv = arg(args, "real", "../data-export/out/limburg_dwellings.csv");
        int everyNth = Integer.parseInt(arg(args, "every", "1"));
        int iterations = Integer.parseInt(arg(args, "iterations", "1"));
        String scName = arg(args, "scenario", "baseline");
        int startYear = Integer.parseInt(arg(args, "start", "2024"));
        int endYear = Integer.parseInt(arg(args, "end", "2050"));
        String out = arg(args, "out", null);
        if (out == null) out = "simulation_results_" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".csv";

        Scenario scen = new Scenario(1, "baseline", "MEDIUM", "MEDIUM");
        if (scName.equals("social_learning_factor_high")) scen = new Scenario(3, scName, "HIGH", "MEDIUM");
        else if (scName.equals("economic_learning_factor_high")) scen = new Scenario(5, scName, "MEDIUM", "HIGH");
        else scen.scenName = scName;

        long t0 = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(Results.HEADER).append('\n');
        int nAgents = 0, nBlocks = 0;
        for (int it = 1; it <= iterations; it++) {
            Rng rng = new Rng(1 + it - 1);
            RealStockAllLoader.Agents ag = RealStockAllLoader.load(csv, rng, everyNth);
            nAgents = ag.total(); nBlocks = ag.socialBlocks.size() + ag.hoaBlocks.size();
            FullSimulation sim = new FullSimulation(startYear, endYear, rng, scen, false, 10,
                    ag.homeowners, ag.landlords, ag.socialBlocks, ag.hoaBlocks, ag.vesta);
            List<FullSimulation.YearRow> rows = sim.run();
            for (FullSimulation.YearRow r : rows) {
                for (HeatingSystem hs : HeatingSystem.values()) {
                    for (String own : OWN_OUT) {
                        int cur = own.equals("TOTAL") ? r.stock.get(hs) : r.stockOwn.get(own).get(hs);
                        int inst = own.equals("TOTAL") ? r.installed.get(hs) : 0;
                        int rem = own.equals("TOTAL") ? r.removed.get(hs) : 0;
                        sb.append(scen.scenId).append(',').append(scen.scenName).append(',').append(it).append(',')
                          .append(r.year).append(",0,0,").append(hs).append(',').append(own).append(',')
                          .append(cur).append(',').append(inst).append(',').append(rem).append(',')
                          .append(r.cumInstalled.get(hs)).append(',').append(r.considered).append(",0,0,0,0,0,")
                          .append(scen.socialLearningFactor).append(',').append(scen.economicLearningFactor)
                          .append(",MEDIUM,5,COST_BASED,COST_BASED,false,false\n");
                    }
                }
            }
        }
        Files.writeString(Path.of(out), sb.toString());
        System.out.printf("Ran %d iters, %d agents (%d social blocks), %d-%d in %.1fs -> %s%n",
                iterations, nAgents, nBlocks, startYear, endYear, (System.currentTimeMillis() - t0) / 1000.0, out);
    }
}
