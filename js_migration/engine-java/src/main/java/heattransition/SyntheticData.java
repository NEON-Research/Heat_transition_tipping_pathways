package heattransition;

import java.util.ArrayList;
import java.util.List;

/** Plausible homeowner stock so the engine runs end-to-end on REAL economics.
 *  NOT the real building stock: for golden parity, export DWELLINGS_DEMAND_INSULATION +
 *  the neighbourhood household tables and load those instead (same Dwelling shape). */
public final class SyntheticData {
    private SyntheticData() {}
    private static final String[] LABELS = {"a","b","c","d","e","f","g"};

    public static List<Dwelling> make(int n, long seed) {
        Rng rng = new Rng(seed);
        List<Dwelling> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double heatDemand = Math.round(3000 + Math.pow(rng.next(), 1.5) * 15000);
            String label = LABELS[Math.min(6, (int) Math.floor(Math.pow(rng.next(), 0.7) * 7))];
            double area = Math.round(60 + rng.next() * 140);
            String type = rng.next() < 0.35 ? "APARTMENT" : "HOUSE";
            boolean lowTemp = label.equals("a") || label.equals("b");
            out.add(new Dwelling(i, HeatingSystem.NATURAL_GAS_BOILER, rng.nextInt(0, 12),
                    heatDemand, label, area, type, lowTemp, false, rng.beta(5, 2, 0, 1)));
        }
        return out;
    }
}
