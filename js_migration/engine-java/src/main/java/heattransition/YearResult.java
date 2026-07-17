package heattransition;

import java.util.EnumMap;
import java.util.Map;

/** One simulated year's aggregate outcome. */
public final class YearResult {
    public final int year;
    public final Map<HeatingSystem, Integer> stock = new EnumMap<>(HeatingSystem.class);
    public final Map<HeatingSystem, Integer> installed = new EnumMap<>(HeatingSystem.class);
    public final Map<HeatingSystem, Integer> removed = new EnumMap<>(HeatingSystem.class);
    public final Map<HeatingSystem, Integer> cumInstalled = new EnumMap<>(HeatingSystem.class);
    public int considered = 0;

    public YearResult(int year) {
        this.year = year;
        for (HeatingSystem t : HeatingSystem.values()) {
            stock.put(t, 0); installed.put(t, 0); removed.put(t, 0); cumInstalled.put(t, 0);
        }
    }
}
