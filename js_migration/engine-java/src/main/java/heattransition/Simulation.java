package heattransition;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** The ABM: dwellings + homeowners, annual adoption loop, salience + learning-curve feedback.
 *  Faithful to Main.f_adoptionProces / J_HomeOwner, with the corrected opportunity trigger.
 *  Scope: homeowner adoption core (the tipping mechanics). Renters/landlords/SHA/HOA blocks
 *  and energy/grid/DH modules are separate (TODO) and don't affect this S-curve. */
public final class Simulation {
    private final int startYear, endYear, networkSize;
    private final Rng rng;
    private final Scenario scenario;
    private final boolean legacyTrigger;
    private final double slf, elf;
    private final List<Dwelling> dwellings;
    private final Map<HeatingSystem, HeatingSystemSpec> hs;
    private final Map<HeatingSystem, Integer> cumInstalled = new EnumMap<>(HeatingSystem.class);
    private final Map<HeatingSystem, Double> prevShare = new EnumMap<>(HeatingSystem.class);

    public Simulation(int startYear, int endYear, Rng rng, Scenario scenario,
                      boolean legacyTrigger, int networkSize, List<Dwelling> dwellings) {
        this.startYear = startYear; this.endYear = endYear; this.rng = rng; this.scenario = scenario;
        this.legacyTrigger = legacyTrigger; this.networkSize = networkSize; this.dwellings = dwellings;
        this.slf = Constants.LEARNING_MULTIPLIER.getOrDefault(scenario.socialLearningFactor, 1.0);
        this.elf = Constants.LEARNING_MULTIPLIER.getOrDefault(scenario.economicLearningFactor, 1.0);
        this.hs = HeatingSystemData.freshSpecs();
        for (HeatingSystem t : HeatingSystem.values()) { cumInstalled.put(t, 0); prevShare.put(t, 0.0); }
        buildNetwork();
        initInstalledBase();
    }

    // Placeholder insulation cost (real values live in DWELLINGS_DEMAND_INSULATION; export for parity).
    private static double insulationCost(Dwelling d, String toLabel) {
        int gap = Constants.labelToNumber(toLabel) - Constants.labelToNumber(d.energyLabel);
        return Math.max(0, gap) * 40 * d.livingAreaM2; // ~EUR40/m2 per label step (placeholder)
    }

    private void buildNetwork() {
        int n = dwellings.size();
        for (Dwelling d : dwellings) d.network = new ArrayList<>();
        for (Dwelling d : dwellings) {
            for (int i = 0; i < networkSize; i++) {
                Dwelling j = dwellings.get(rng.nextInt(0, n));
                if (j != d) d.network.add(j);
            }
        }
        for (Dwelling d : dwellings) {
            d.peerCounts = new EnumMap<>(HeatingSystem.class);
            for (HeatingSystem t : HeatingSystem.values()) d.peerCounts.put(t, 0);
            for (Dwelling p : d.network) d.peerCounts.merge(p.currentType, 1, Integer::sum);
        }
    }

    private void initInstalledBase() {
        for (Dwelling d : dwellings) cumInstalled.merge(d.currentType, 1, Integer::sum);
        for (HeatingSystem t : HeatingSystem.values()) hs.get(t).initialUnits = Math.max(1, cumInstalled.get(t));
        updateSalience();
    }

    private void updateSalience() {
        int n = dwellings.size();
        for (HeatingSystem t : HeatingSystem.values()) {
            double cur = (double) cumInstalled.get(t) / n;
            hs.get(t).salienceFactor = Decision.salienceFactor(cur, prevShare.get(t));
        }
    }

    private void updateLearningCurve() {
        for (HeatingSystem t : HeatingSystem.values()) {
            HeatingSystemSpec h = hs.get(t);
            double f = Decision.capexLearningFactor(cumInstalled.get(t), h.initialUnits, h.economicLearningRate, elf);
            h.investSmall = h.initialInvestSmall * f;
            h.investMedium = h.initialInvestMedium * f;
            h.investLarge = h.initialInvestLarge * f;
        }
    }

    private void resetMinMax() {
        for (HeatingSystem t : HeatingSystem.values()) {
            hs.get(t).minEAC = Double.POSITIVE_INFINITY; hs.get(t).maxEAC = Double.NEGATIVE_INFINITY;
        }
    }

    private boolean possible(HeatingSystem type, Dwelling d) {
        if (type == HeatingSystem.DISTRICT_HEATING) return d.hasDistrictHeatingGrid;
        if (type == HeatingSystem.NATURAL_GAS_BLOCK) return d.dwellingType.equals("APARTMENT");
        return true;
    }

    private Map<HeatingSystem, long[]> computeEacs(Dwelling d) { // value: [eac, possible(0/1)]
        Map<HeatingSystem, long[]> out = new EnumMap<>(HeatingSystem.class);
        for (HeatingSystem t : HeatingSystem.values()) {
            HeatingSystemSpec h = hs.get(t);
            long eac = Economics.computeEAC(h, d, Simulation::insulationCost);
            out.put(t, new long[]{ eac, possible(t, d) ? 1 : 0 });
            if (eac < h.minEAC) h.minEAC = eac;
            if (eac > h.maxEAC) h.maxEAC = eac;
        }
        return out;
    }

    private HeatingSystem chooseType(Dwelling d, Map<HeatingSystem, long[]> options) {
        Map<HeatingSystem, Double> util = new EnumMap<>(HeatingSystem.class);
        for (HeatingSystem t : HeatingSystem.values()) {
            if (options.get(t)[1] == 0) continue;
            HeatingSystemSpec h = hs.get(t);
            double att = Decision.attitudeValue(d.attitude, h.sustainabilityScoreNorm);
            double eff = Decision.effort(t == d.currentType, h.requiresLowTemp, d.hasLowTemp);
            double eacNorm = h.maxEAC > h.minEAC
                    ? Decision.normalizedValue(options.get(t)[0], h.minEAC, h.maxEAC) : 0;
            double pbc = Decision.pbc(eacNorm, eff);
            int peers = d.peerCounts.getOrDefault(t, 0);
            double sn = Decision.subjectiveNorm(peers, Math.max(1, d.network.size()), h.salienceFactor);
            double intent = Decision.intention(att, sn, pbc, slf, h.socialLearningRate);
            util.put(t, Decision.perceivedUtility(intent, pbc));
        }
        HeatingSystem best = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (Map.Entry<HeatingSystem, Double> e : util.entrySet()) {
            double score = Decision.rumScore(e.getValue(), rng.gumbel());
            if (score > bestScore) { bestScore = score; best = e.getKey(); }
        }
        return best;
    }

    private void notifyPeers(Dwelling d, HeatingSystem oldType, HeatingSystem newType) {
        for (Dwelling p : d.network) {
            p.peerCounts.merge(oldType, -1, Integer::sum);
            p.peerCounts.merge(newType, 1, Integer::sum);
        }
    }

    private YearResult stepYear(int year) {
        resetMinMax();
        for (Dwelling d : dwellings) d.age++;
        int n = dwellings.size();
        for (HeatingSystem t : HeatingSystem.values()) prevShare.put(t, (double) cumInstalled.get(t) / n);

        YearResult r = new YearResult(year);
        for (Dwelling d : dwellings) {
            int lifetime = hs.get(d.currentType).lifetime;
            boolean eol = Decision.hasEndOfLifeTrigger(d.age, lifetime);
            boolean opp = Decision.hasOpportunityTrigger(d.age, lifetime, legacyTrigger);
            if (!eol && !opp) continue;
            r.considered++;
            Map<HeatingSystem, long[]> options = computeEacs(d);
            HeatingSystem chosen = chooseType(d, options);
            if (chosen == null) continue;
            if (eol || chosen != d.currentType) {
                HeatingSystem oldType = d.currentType;
                notifyPeers(d, oldType, chosen);
                r.removed.merge(oldType, 1, Integer::sum);
                r.installed.merge(chosen, 1, Integer::sum);
                cumInstalled.merge(chosen, 1, Integer::sum);
                d.currentType = chosen;
                d.age = 0;
            }
        }
        updateSalience();
        updateLearningCurve();
        for (Dwelling d : dwellings) r.stock.merge(d.currentType, 1, Integer::sum);
        for (HeatingSystem t : HeatingSystem.values()) r.cumInstalled.put(t, cumInstalled.get(t));
        return r;
    }

    public List<YearResult> run() {
        List<YearResult> rows = new ArrayList<>();
        YearResult r0 = new YearResult(startYear);
        for (Dwelling d : dwellings) r0.stock.merge(d.currentType, 1, Integer::sum);
        for (HeatingSystem t : HeatingSystem.values()) r0.cumInstalled.put(t, cumInstalled.get(t));
        rows.add(r0);
        for (int y = startYear + 1; y <= endYear; y++) rows.add(stepYear(y));
        return rows;
    }
}
