package heat_transition_tipping_pathways;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Locale;

/**
 * HT_DYN probe for AnyLogic -- emits the DECISION-TIME dynamic state (learned supply-system
 * investment + salience factor) per heating type, once per year, so the per-year dynamics can be
 * diffed 1:1 against the JS/Java engine's ENGDYN dump (tests/compare_dyn.py). AL lines are tagged
 * "ALDYN".
 *
 * WHERE TO CALL IT: the very FIRST line of Main.f_adoptionProces, BEFORE any decision step (before
 * step 5). At that point invest was last set by step 12 (f_updateGlobalCostsHeatingMethods) of the
 * PREVIOUS year and salience by step 13 -- i.e. exactly the values this year's decisions will use.
 * The engine's ENGDYN dump uses the same convention (start of stepYear, before _updateLearningCurve),
 * so ALDYN <year> == ENGDYN <year> when the dynamics agree. Year 1 (2025) is the base/initial state
 * (factor 1.0) in both.
 *
 *   void f_adoptionProces() {
 *       HTDyn.emit(v_year, this);          // <-- add this line
 *       int yearIndex = v_year - v_startYear;
 *       ...
 *   }
 *
 * Output goes to BOTH the console and a file (default "al_dyn.log"; override with
 * -Dht.dyn.file=... or env HT_DYN_FILE). Truncated once per process, appended thereafter, closed
 * by a shutdown hook. Purely observational: no RNG, no state change. It ALWAYS emits when called
 * (no env-var gate) -- the emit call site is the on/off switch. Use a BASELINE-ONLY AnyLogic run
 * for the dynamics comparison: al_dyn.log has no scenario tag, so a multi-scenario analysis mixes
 * every scenario into one file.
 */
public final class HTDyn {

    private static PrintWriter out = null;
    private static boolean fileInitTried = false;

    private HTDyn() {}

    private static void ensureFile() {
        if (fileInitTried) return;
        fileInitTried = true;
        String path = System.getProperty("ht.dyn.file");
        if (path == null) path = System.getenv("HT_DYN_FILE");
        if (path == null) path = "al_dyn.log";
        try {
            out = new PrintWriter(new FileWriter(path, false), true);  // truncate once, autoflush
            System.out.println("HTDyn: writing ALDYN lines to " + new File(path).getAbsolutePath());
            final PrintWriter w = out;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> { w.flush(); w.close(); }));
        } catch (IOException e) {
            System.out.println("HTDyn: could not open dyn file (" + e.getMessage() + "); console only");
            out = null;
        }
    }

    /** Emit one ALDYN line per heating type for the given year (call at start of f_adoptionProces).
     *  No env-var gate -- the presence of the HTDyn.emit(...) call site is the toggle (same as the
     *  HTDiag probe). Remove the call, or comment it out, to turn the probe off. */
    public static void emit(int year, Main m) {
        ensureFile();
        for (OL_HeatingSystem t : OL_HeatingSystem.values()) {
            J_HeatingSystemOptionsGlobal g = m.heatingSystemOptionsGlobalData.get(t);
            if (g == null) continue;
            String line = String.format(Locale.US,
                "ALDYN %d %s invest=%.0f salience=%.4f",
                year, t, g.getInvestmentCostsHeatSupplySystem_medium(), g.getSalienceFactor());
            m.traceln(line);
            if (out != null) out.println(line);
        }
    }
}
