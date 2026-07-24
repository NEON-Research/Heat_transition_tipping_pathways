package heat_transition_tipping_pathways;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Locale;
import java.util.Map;

/**
 * HT_DIAG probe for AnyLogic — emits the SAME per-term TPB averages + EAC window bounds
 * as the JS engine ("JSDIAG ...") and the Java engine ("DIAG ...") so all three can be
 * diffed term-by-term with tests/compare_diag.py. AL lines are tagged "ALDIAG".
 *
 * Output goes to BOTH the AnyLogic console (via traceln) AND a log file, so you never have
 * to copy-paste the console. The file:
 *   - defaults to "al.log" in the run's working directory,
 *   - can be overridden with  -Dht.diag.file=C:\path\al.log  or env var HT_DIAG_FILE,
 *   - is truncated once at the first report and then appended to for every iteration in the
 *     same process, so one file collects all 20 iterations,
 *   - is closed by a shutdown hook.
 * The absolute path is printed once so you know exactly where it landed.
 *
 * Index 0 = NATURAL_GAS_BOILER, 1 = HYBRID_HEAT_PUMP (same two systems the engines probe).
 * Averages are over TRIGGERED homeowners for whom the option is possible. eacNorm is recomputed
 * exactly as Main.f_getNormalizedValue does: (EAC - min)/(max - min). Purely observational.
 */
public final class HTDiag {

    private static final int[] YEARS = { 2025, 2030, 2035 };

    private static boolean active = false;
    private static final long[]   n   = new long[2];
    private static final double[] att = new double[2];
    private static final double[] sn  = new double[2];
    private static final double[] pbc = new double[2];
    private static final double[] eacN= new double[2];
    private static final double[] sal = new double[2];

    // --- file output -------------------------------------------------------------------
    private static PrintWriter out = null;
    private static boolean fileInitTried = false;

    private static void ensureFile() {
        if (fileInitTried) return;
        fileInitTried = true;
        String path = System.getProperty("ht.diag.file");
        if (path == null) path = System.getenv("HT_DIAG_FILE");
        if (path == null) path = "al.log";
        try {
            out = new PrintWriter(new FileWriter(path, false), true);  // truncate once, autoflush
            System.out.println("HTDiag: writing ALDIAG lines to " + new File(path).getAbsolutePath());
            final PrintWriter w = out;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> { w.flush(); w.close(); }));
        } catch (IOException e) {
            System.out.println("HTDiag: could not open diag file (" + e.getMessage() + "); console only");
            out = null;
        }
    }

    private HTDiag() {}

    /** Call at the top of f_adoptionProces (before step 5). Enables accumulation for report years. */
    public static void reset(int year) {
        active = false;
        for (int y : YEARS) if (y == year) active = true;
        if (!active) return;
        for (int k = 0; k < 2; k++) { n[k] = 0; att[k] = 0; sn[k] = 0; pbc[k] = 0; eacN[k] = 0; sal[k] = 0; }
    }

    /** Call inside the step-7 loop AFTER ho.f_getHeatingSystemOptionsUtility() and BEFORE
     *  ho.f_adoptNewHeatingSystem() (so the option terms are set and not yet cleared). */
    public static void accumulate(J_HomeOwner ho, Main m) {
        if (!active) return;
        Map<OL_HeatingSystem, J_HeatingSystemOption> opts = ho.getHeatingSystemOptions();
        if (opts == null) return;
        acc(0, OL_HeatingSystem.NATURAL_GAS_BOILER, opts, m);
        acc(1, OL_HeatingSystem.HYBRID_HEAT_PUMP,   opts, m);
    }

    private static void acc(int k, OL_HeatingSystem type,
                            Map<OL_HeatingSystem, J_HeatingSystemOption> opts, Main m) {
        J_HeatingSystemOption o = opts.get(type);
        if (o == null || !o.isPossible()) return;                 // match the engines' "possible" filter
        J_HeatingSystemOptionsGlobal g = m.heatingSystemOptionsGlobalData.get(type);
        double min = g.getMinEAC(), max = g.getMaxEAC();
        double eacNorm = (max > min) ? (o.getEAC() - min) / (max - min) : 0.0;
        n[k]++;
        att[k]  += o.getAttitude();
        sn[k]   += o.getSubjectiveNorm();
        pbc[k]  += o.getPBC();
        eacN[k] += eacNorm;
        sal[k]  += g.getSalienceFactor();
    }

    /** Call once AFTER the step-7 loop. Prints one ALDIAG line per system, to console AND al.log. */
    public static void report(int year, Main m) {
        if (!active) return;
        ensureFile();
        String[] label = { "gas   ", "hybrid" };
        OL_HeatingSystem[] wt = { OL_HeatingSystem.NATURAL_GAS_BOILER, OL_HeatingSystem.HYBRID_HEAT_PUMP };
        for (int k = 0; k < 2; k++) {
            long cnt = (n[k] == 0) ? 1 : n[k];
            J_HeatingSystemOptionsGlobal g = m.heatingSystemOptionsGlobalData.get(wt[k]);
            String line = String.format(Locale.US,
                "ALDIAG %d %s n=%d att=%.3f sn=%.3f pbc=%.3f eacNorm=%.3f salience=%.3f minEAC=%d maxEAC=%d",
                year, label[k], n[k],
                att[k] / cnt, sn[k] / cnt, pbc[k] / cnt, eacN[k] / cnt, sal[k] / cnt,
                Math.round(g.getMinEAC()), Math.round(g.getMaxEAC()));
            m.traceln(line);                 // console
            if (out != null) out.println(line);  // al.log (autoflush)
        }
    }
}
