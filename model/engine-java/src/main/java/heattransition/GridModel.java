package heattransition;

import java.util.List;

/** Electricity-grid capacity, load and congestion per neighbourhood (the DSO mechanism).
 *
 *  Ported from the AnyLogic f_setGridCapacity / f_reinforceGrid logic, with one deliberate
 *  change: the neighbourhood baseload is derived from the ACTUAL electricity demand of the
 *  dwellings in that neighbourhood, NOT from the CBS `g_ele` column. `g_ele` is privacy-suppressed
 *  (-99999) in every row, which would zero the baseload and systematically under-detect congestion.
 *  Deriving it from the simulated stock is both available and internally consistent: the same
 *  dwellings whose heating choices drive the load also define the grid that serves them.
 *
 *  Peak load = baseload + electric-heating load + EV load, each with its own simultaneity factor.
 *  Initial capacity is sized from the t0 peak load times a design margin, so grids start adequate
 *  and congestion emerges from the transition itself (heat pumps + EVs), not from a data artefact. */
public final class GridModel {
    private GridModel() {}

    /** Annual household electricity use NOT related to heating (appliances, lighting, cooking). */
    public static double applianceKWh() { return Constants.BASELOAD_KWH_PER_DWELLING; }

    /** Electric power drawn by a dwelling's heating system at design conditions (kW), 0 for gas/DH. */
    public static double heatingPeakKW(Dwelling d, java.util.Map<HeatingSystem, HeatingSystemSpec> hs) {
        HeatingSystemSpec h = hs.get(d.currentType);
        double frac = 0;
        if ("ELECTRICITY".equals(h.primarySource)) frac += h.fractionPrimary;
        if ("ELECTRICITY".equals(h.secondarySource)) frac += h.fractionSecondary;
        if (frac <= 0) return 0;
        // hybrids run the heat pump for only part of the demand -> scale the connected power by it
        return Constants.PMAX_ELECTRIC_HEAT_PUMP * frac;
    }

    /** Diversified peak load of a neighbourhood (kW) for the current stock + EV count. */
    public static double peakLoadKW(Neighbourhood nb, int evs,
                                    java.util.Map<HeatingSystem, HeatingSystemSpec> hs) {
        List<Dwelling> dw = nb.allDwellings;
        int n = dw.size();
        if (n == 0) return 0;
        // 1. baseload from the dwellings themselves (annual kWh -> average kW -> peak via factor)
        double baseAvgKW = n * applianceKWh() / 8760.0;
        double baseline = baseAvgKW * Constants.BASELOAD_PEAK_FACTOR * simultaneity(n);
        // 2. electric heating, diversified
        double heat = 0;
        for (Dwelling d : dw) heat += heatingPeakKW(d, hs);
        heat *= Constants.SIMULTANEITY_HEAT_PUMP;
        // 3. EV charging, diversified
        double ev = evs * Constants.PMAX_EV * Constants.SIMULTANEITY_EV;
        return baseline + heat + ev;
    }

    /** Dimension a grid for a given load: the larger of (load x design margin) and the per-dwelling
     *  standard allowance, rounded UP to a discrete transformer size. */
    public static double sizeCapacityKW(double loadKW, int dwellings) {
        double byLoad = loadKW * Constants.GRID_DESIGN_MARGIN;
        double byDwellings = dwellings * Constants.CAPACITY_PER_DWELLING_KW;
        double need = Math.max(byLoad, byDwellings);
        double step = Constants.CAPACITY_STEP_KW;
        return step > 0 ? Math.ceil(need / step) * step : need;
    }

    /** Rusck/Velander-style diversity: the more households, the lower the per-household coincidence. */
    public static double simultaneity(int households) {
        if (households <= 1) return 1.0;
        return Constants.SIMULTANEITY_BASE
             + (1 - Constants.SIMULTANEITY_BASE) / Math.sqrt(households);
    }

    /** EV count for a neighbourhood in `year`: logistic share of its car fleet (AL's S-curve). */
    public static int evs(Neighbourhood nb, int year) {
        double t = year - Constants.EV_MIDPOINT_YEAR;
        double share = Constants.EV_SHARE_2050 / (1 + Math.exp(-Constants.EV_STEEPNESS * t));
        return (int) Math.round(nb.cars * Math.max(0, Math.min(1, share)));
    }
}
