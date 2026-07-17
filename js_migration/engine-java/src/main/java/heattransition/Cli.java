package heattransition;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Headless CLI runner. Runs scenarios x iterations and writes a simulation_results CSV
 *  (same schema as the AnyLogic export).
 *
 *  Usage:
 *    gradle run --args="--dwellings 2000 --iterations 5 --out out.csv"
 *    gradle run --args="--scenario social_learning_factor_high"
 *    gradle run --args="--legacy-trigger"   # reproduce the original int-division bug
 */
public final class Cli {
    static String arg(String[] a, String name, String def) {
        for (int i = 0; i < a.length; i++) {
            if (a[i].equals("--" + name)) {
                if (i + 1 < a.length && !a[i + 1].startsWith("--")) return a[i + 1];
                return "true";
            }
        }
        return def;
    }

    static Map<String, Scenario> builtinScenarios() {
        Map<String, Scenario> m = new LinkedHashMap<>();
        m.put("baseline", new Scenario(1, "baseline", "MEDIUM", "MEDIUM"));
        m.put("social_learning_factor_low", new Scenario(2, "social_learning_factor_low", "LOW", "MEDIUM"));
        m.put("social_learning_factor_high", new Scenario(3, "social_learning_factor_high", "HIGH", "MEDIUM"));
        m.put("economic_learning_factor_high", new Scenario(5, "economic_learning_factor_high", "MEDIUM", "HIGH"));
        return m;
    }

    public static void main(String[] args) throws Exception {
        int nDwellings = Integer.parseInt(arg(args, "dwellings", "2000"));
        int iterations = Integer.parseInt(arg(args, "iterations", "3"));
        int startYear = Integer.parseInt(arg(args, "start", String.valueOf(Constants.DEFAULT_START_YEAR)));
        int endYear = Integer.parseInt(arg(args, "end", String.valueOf(Constants.DEFAULT_END_YEAR)));
        long baseSeed = Long.parseLong(arg(args, "seed", String.valueOf(Constants.DEFAULT_SEED)));
        boolean legacy = arg(args, "legacy-trigger", "false").equals("true");
        String out = arg(args, "out", null);
        if (out == null) {
            String ts = java.time.LocalDateTime.now()
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            out = "simulation_results_" + ts + ".csv";
        }
        String scName = arg(args, "scenario", null);
        String realCsv = arg(args, "real", null);
        int everyNth = Integer.parseInt(arg(args, "every", "1"));

        Map<String, Scenario> builtins = builtinScenarios();
        List<Scenario> scenarios = new ArrayList<>();
        if (scName != null) scenarios.add(builtins.getOrDefault(scName, builtins.get("baseline")));
        else scenarios.addAll(builtins.values());

        long t0 = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(Results.HEADER).append('\n');
        int runCount = 0;
        int actualN = nDwellings;
        for (Scenario scen : scenarios) {
            for (int it = 1; it <= iterations; it++) {
                long seed = baseSeed + it - 1;
                Rng rng = new Rng(seed);
                List<Dwelling> dwellings = (realCsv != null)
                        ? RealStockLoader.loadHomeowners(realCsv, new Rng(seed), everyNth)
                        : SyntheticData.make(nDwellings, 1000L + it);
                Simulation sim = new Simulation(startYear, endYear, rng, scen, legacy, 10, dwellings);
                Results.appendRun(sb, sim.run(), scen, it);
                actualN = dwellings.size();
                runCount++;
            }
        }
        Files.writeString(Path.of(out), sb.toString());
        double dt = (System.currentTimeMillis() - t0) / 1000.0;
        System.out.printf("Ran %d runs (%d scenarios x %d iters, %d %s dwellings, %d-%d%s) in %.2fs -> %s%n",
                runCount, scenarios.size(), iterations, actualN, (realCsv != null ? "real" : "synthetic"), startYear, endYear,
                legacy ? ", LEGACY trigger" : "", dt, out);
    }
}
