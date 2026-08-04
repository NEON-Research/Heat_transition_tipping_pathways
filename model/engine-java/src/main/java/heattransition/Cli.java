package heattransition;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Multi-owner headless runner (social blocks + landlords + homeowners).
 *  Usage: java heattransition.Cli --real ..\data-export\out\limburg_dwellings.csv
 *         [--scenario baseline] [--every 1] [--iterations 1] [--out file.csv] */
public final class Cli {
    static String arg(String[] a, String n, String d) {
        for (int i = 0; i < a.length; i++) if (a[i].equals("--" + n))
            return (i + 1 < a.length && !a[i + 1].startsWith("--")) ? a[i + 1] : "true";
        return d;
    }
    static final String[] OWN_OUT = { "PRIVATELY_OWNED", "PRIVATELY_RENTED", "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION", "TOTAL" };

    public static void main(String[] args) throws Exception {
        String csv = arg(args, "real", "../data/stock/limburg_dwellings.csv");
        int everyNth = Integer.parseInt(arg(args, "every", "1"));
        int iterations = Integer.parseInt(arg(args, "iterations", "1"));
        String scName = arg(args, "scenario", "baseline");
        int startYear = Integer.parseInt(arg(args, "start", "2024"));
        int endYear = Integer.parseInt(arg(args, "end", "2050"));
        String out = arg(args, "out", null);
        if (out == null) out = "simulation_results_" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".csv";

        // --scenario all runs the full 16-scenario matrix into one CSV (like the AL batch run).
        String[] scenarios = scName.equalsIgnoreCase("all") ? Scenario.NAMES : new String[]{ scName };

        long t0 = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(Results.HEADER).append('\n');
        StringBuilder lb = new StringBuilder(Results.LOOP_HEADER).append('\n');
        StringBuilder gb = new StringBuilder(Results.SEG_HEADER).append('\n');
        int nAgents = 0, nBlocks = 0;
        for (String sn : scenarios) {
            Scenario scen = Scenario.byName(sn);
            System.out.printf("scenario %s (id %d): %d iteration(s), %d-%d%n",
                    scen.scenName, scen.scenId, iterations, startYear, endYear);
            StockLoader.Agents ag = null;   // hoisted so the previous iteration's ~8.4M-dwelling
                                            // graph can be released BEFORE the next one is built
            for (int it = 1; it <= iterations; it++) {
                long itStart = System.currentTimeMillis();
                // Seed = iteration index (+ optional offset). The stochastic content -- peer
                // networks, attitudes, ownership draws, heat-demand factors -- is FULLY retained and
                // redrawn every iteration; fixing the seed sequence only makes the set of sampled
                // worlds REPRODUCIBLE, and lets two parameter sets be compared on the SAME worlds
                // (common random numbers). -Dht.seedOffset=1000 gives a fresh, independent set of
                // worlds, for out-of-sample validation of a calibrated parameter set.
                int seedOffset = Integer.parseInt(System.getProperty("ht.seedOffset",
                        System.getenv().getOrDefault("HT_SEEDOFFSET", "0")));
                Rng rng = new Rng(seedOffset + it);
                ag = null;   // drop the last iteration's graph now -> GC can reclaim it during load,
                             // keeping peak heap ~1x instead of ~2x (MODEL_TODOS C). Each iteration
                             // still rebuilds/redraws the stock -- that's the Monte-Carlo variance.
                ag = StockLoader.load(csv, rng, everyNth);
                nAgents = ag.total(); nBlocks = ag.socialBlocks.size() + ag.hoaBlocks.size();
                Simulation sim = new Simulation(startYear, endYear, rng, scen, false, 10,
                        ag.homeowners, ag.landlords, ag.socialBlocks, ag.hoaBlocks, ag.vesta,
                        ag.neighbourhoods);
                List<Simulation.YearRow> rows = sim.run();
                String tags = ',' + scen.socialLearningFactor + ',' + scen.economicLearningFactor + ','
                        + scen.gridReinforcementRate + ',' + scen.dhConstructionTime + ','
                        + scen.dhExpansionStrategy + ',' + scen.shaStrategy + ','
                        + scen.dhConnectionObligation + ',' + scen.gridCongestionHpBan + '\n';
                for (Simulation.YearRow r : rows) {
                    for (java.util.Map.Entry<String, double[]> e : r.seg.entrySet()) {
                        String[] k = e.getKey().split("\\|", 3); double[] v = e.getValue();
                        double n = v[8] > 0 ? v[8] : 1;
                        gb.append(scen.scenId).append(',').append(scen.scenName).append(',').append(it)
                          .append(',').append(r.year).append(',').append(k[0]).append(',').append(k[1])
                          .append(',').append(k[2]).append(',')
                          .append((long) v[0]).append(',').append((long) v[1]).append(',').append((long) v[2]).append(',')
                          .append(String.format(java.util.Locale.US,"%.4f",v[3]/n)).append(',')
                          .append(String.format(java.util.Locale.US,"%.4f",v[4]/n)).append(',')
                          .append(String.format(java.util.Locale.US,"%.4f",v[5]/n)).append(',')
                          .append(String.format(java.util.Locale.US,"%.4f",v[6]/n)).append(',')
                          .append(String.format(java.util.Locale.US,"%.1f",v[7]/n)).append(',')
                          .append(String.format(java.util.Locale.US,"%.4f",v[9]/n)).append('\n');
                    }
                    for (HeatingSystem t : HeatingSystem.values()) {   // loop-state row per technology
                        lb.append(scen.scenId).append(',').append(scen.scenName).append(',').append(it)
                          .append(',').append(r.year).append(',').append(t).append(',')
                          .append(String.format(java.util.Locale.US, "%.2f", r.learnedCapex.getOrDefault(t, 0.0))).append(',')
                          .append(String.format(java.util.Locale.US, "%.5f", r.salience.getOrDefault(t, 0.0))).append(',')
                          .append(r.cumInstalled.get(t)).append(',')
                          .append(String.format(java.util.Locale.US, "%.5f", r.energyPrice.getOrDefault(t, 0.0))).append(',')
                          .append(r.installed.get(t)).append('\n');
                    }
                    // nbh_*_perc are FRACTIONS (0-1), matching AL (e.g. 0.234 at 2050), not integers.
                    String dh = String.valueOf(r.nbhWithDhPerc), cong = String.valueOf(r.nbhCongestionPerc);
                    for (HeatingSystem hs : HeatingSystem.values()) {
                        int ord = hs.ordinal();
                        for (String own : OWN_OUT) {
                            int cur = own.equals("TOTAL") ? r.stock.get(hs) : r.stockOwn.get(own).get(hs);
                            int inst = own.equals("TOTAL") ? r.installed.get(hs) : 0;
                            int rem = own.equals("TOTAL") ? r.removed.get(hs) : 0;
                            // avg_* columns. Emit BLANK (not 0) when no decider of this ownership
                            // evaluated this type this year -> the cell reads as NaN downstream and is
                            // left OUT of cross-iteration averaging, instead of biasing the mean to 0.
                            // TPB terms are homeowner-only (PRIVATELY_OWNED / TOTAL); avg_eac is per
                            // ownership.
                            boolean ho = own.equals("PRIVATELY_OWNED") || own.equals("TOTAL");
                            boolean hoData = ho && r.hoN[ord] > 0;
                            String att = hoData ? String.valueOf(r.hoMean(r.hoAtt, ord)) : "";
                            String util = hoData ? String.valueOf(r.hoMean(r.hoUtil, ord)) : "";
                            String subNorm = hoData ? String.valueOf(r.hoMean(r.hoSn, ord)) : "";
                            String pbc = hoData ? String.valueOf(r.hoMean(r.hoPbc, ord)) : "";
                            String eac = r.eacCount(own, ord) > 0 ? String.valueOf(r.eacMean(own, ord)) : "";
                            sb.append(scen.scenId).append(',').append(scen.scenName).append(',').append(it).append(',')
                              .append(r.year).append(',').append(cong).append(',').append(dh).append(',')
                              .append(hs).append(',').append(own).append(',')
                              .append(cur).append(',').append(inst).append(',').append(rem).append(',')
                              .append(r.cumInstalled.get(hs)).append(',').append(r.considered).append(',')
                              .append(att).append(',').append(util).append(',').append(subNorm).append(',')
                              .append(eac).append(',').append(pbc)
                              .append(tags);
                        }
                    }
                }

                // progress line: time, running avg, ETA, and final-year gas share (early failure signal).
                Simulation.YearRow last = rows.get(rows.size() - 1);
                int total = 0, gas = last.stock.get(HeatingSystem.NATURAL_GAS_BOILER);
                for (HeatingSystem hs : HeatingSystem.values()) total += last.stock.get(hs);
                double itSecs = (System.currentTimeMillis() - itStart) / 1000.0;
                double avg = (System.currentTimeMillis() - t0) / 1000.0 / Math.max(1, it);
                System.out.printf("  %s iter %d/%d  %.1fs (avg %.1fs)  %d gas %.1f%% at %d%n",
                        scen.scenName, it, iterations, itSecs, avg,
                        total, total > 0 ? 100.0 * gas / total : 0.0, last.year);
                System.out.flush();
            }
        }
        Files.writeString(Path.of(out), sb.toString());
        String loopOut = out.replaceAll("\\.csv$", "") + "_loop_state.csv";
        Files.writeString(Path.of(loopOut), lb.toString());
        Files.writeString(Path.of(out.replaceAll("\\.csv$", "") + "_segments.csv"), gb.toString());
        System.out.printf("Ran %d scenario(s) x %d iters, %d agents (%d blocks), %d-%d in %.1fs -> %s%n",
                scenarios.length, iterations, nAgents, nBlocks, startYear, endYear,
                (System.currentTimeMillis() - t0) / 1000.0, out);
    }
}
