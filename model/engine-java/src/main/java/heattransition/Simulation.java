package heattransition;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Multi-owner ABM: social-housing blocks + landlords + homeowners, all feeding the SHARED
 *  cumulative installs that drive the learning curve + salience. Faithful port of
 *  engine/src/modelFull.js (itself faithful to Main.f_adoptionProces). Order per year:
 *  age -> SHA blocks (whole block, EAC) -> landlords (individual, EAC)
 *       -> homeowners (TPB utility) -> update salience + learning curve. */
public final class Simulation {
    public static final class YearRow {
        public final int year;
        public final Map<HeatingSystem, Integer> stock = new EnumMap<>(HeatingSystem.class);
        public final Map<String, Map<HeatingSystem, Integer>> stockOwn = new java.util.HashMap<>();
        public final Map<HeatingSystem, Integer> installed = new EnumMap<>(HeatingSystem.class);
        public final Map<HeatingSystem, Integer> removed = new EnumMap<>(HeatingSystem.class);
        public final Map<HeatingSystem, Integer> cumInstalled = new EnumMap<>(HeatingSystem.class);
        public int considered = 0;
        public double nbhWithDhPerc = 0;      // % of neighbourhoods with a DH grid this year
        public double nbhCongestionPerc = 0;  // % of neighbourhoods in grid congestion this year
        YearRow(int y) {
            year = y;
            for (HeatingSystem t : HeatingSystem.values()) {
                stock.put(t, 0); installed.put(t, 0); removed.put(t, 0); cumInstalled.put(t, 0);
            }
            for (String o : OWN) { Map<HeatingSystem, Integer> m = new EnumMap<>(HeatingSystem.class);
                for (HeatingSystem t : HeatingSystem.values()) m.put(t, 0); stockOwn.put(o, m); }
        }
    }
    static final String[] OWN = { "PRIVATELY_OWNED", "PRIVATELY_RENTED", "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION" };

    private final int startYear, endYear;
    private final Rng rng;
    private final boolean legacyTrigger;
    private final double slf, elf;
    private final List<Dwelling> homeowners, landlords, all;
    private final Scenario scen;
    private final List<Neighbourhood> neighbourhoods;
    private final java.util.Map<String, String> nbhPolicyPlan = new java.util.HashMap<>();
    private final int nbhsWithoutDHStartYear;
    public double nbhWithDHPerc = 0;
    public double nbhCongestionPerc = 0;
    // --- DIAGNOSTIC state (HT_DIAG); index 0 = gas, 1 = hybrid ---
    private static final boolean DIAG = System.getenv("HT_DIAG") != null;
    // Accumulate ONLY during a report year, else the counters sum across all years between
    // reports (2026-2030 etc.) and n/sn/salience come out ~5x too high. Set in stepYear.
    private boolean diagActive = false;
    private final long[] diagN = new long[2];
    private final double[] diagAtt = new double[2], diagSn = new double[2],
                           diagPbc = new double[2], diagEacN = new double[2], diagSal = new double[2];
    private void diagReport(int year) {
        if (!DIAG || diagN[0] == 0) return;
        HeatingSystem[] wt = { HeatingSystem.NATURAL_GAS_BOILER, HeatingSystem.HYBRID_HEAT_PUMP };
        for (int k = 0; k < 2; k++) {
            HeatingSystemSpec h = hs.get(wt[k]);
            System.out.printf("DIAG %d %-6s n=%d att=%.3f sn=%.3f pbc=%.3f eacNorm=%.3f salience=%.3f minEAC=%d maxEAC=%d%n",
                year, k == 0 ? "gas" : "hybrid", diagN[k], diagAtt[k]/diagN[k], diagSn[k]/diagN[k],
                diagPbc[k]/diagN[k], diagEacN[k]/diagN[k], diagSal[k]/diagN[k],
                Math.round(h.minEAC), Math.round(h.maxEAC));
        }
        java.util.Arrays.fill(diagN, 0); java.util.Arrays.fill(diagAtt, 0); java.util.Arrays.fill(diagSn, 0);
        java.util.Arrays.fill(diagPbc, 0); java.util.Arrays.fill(diagEacN, 0); java.util.Arrays.fill(diagSal, 0);
        System.out.flush();
    }
    private final List<HousingBlock> blocks;
    private final Map<HeatingSystem, HeatingSystemSpec> hs;
    private final Vesta vesta;
    private final Map<HeatingSystem, Integer> cumInstalled = new EnumMap<>(HeatingSystem.class);  // starting stock + all new installs
    private final Map<HeatingSystem, Double> prevShare = new EnumMap<>(HeatingSystem.class);

    public Simulation(int startYear, int endYear, Rng rng, Scenario scen, boolean legacyTrigger,
                          int networkSize, List<Dwelling> homeowners, List<Dwelling> landlords,
                          List<HousingBlock> socialBlocks, List<HousingBlock> hoaBlocks, Vesta vesta) {
        this(startYear, endYear, rng, scen, legacyTrigger, networkSize, homeowners, landlords,
             socialBlocks, hoaBlocks, vesta, new ArrayList<Neighbourhood>());
    }

    public Simulation(int startYear, int endYear, Rng rng, Scenario scen, boolean legacyTrigger,
                          int networkSize, List<Dwelling> homeowners, List<Dwelling> landlords,
                          List<HousingBlock> socialBlocks, List<HousingBlock> hoaBlocks, Vesta vesta,
                          List<Neighbourhood> neighbourhoods) {
        this.startYear = startYear; this.endYear = endYear; this.rng = rng;
        this.legacyTrigger = legacyTrigger;
        this.slf = Constants.LEARNING_MULTIPLIER.getOrDefault(scen.socialLearningFactor, 1.0);
        this.elf = Constants.LEARNING_MULTIPLIER.getOrDefault(scen.economicLearningFactor, 1.0);
        this.homeowners = homeowners; this.landlords = landlords; this.vesta = vesta;
        this.blocks = new ArrayList<>(socialBlocks); this.blocks.addAll(hoaBlocks);
        // DistrictHeatingCompany state. Policy plan years are drawn ONCE per run, and the
        // "no start year" count feeds chanceOfDHBeingInstalled in the POLICY_BASED branch.
        this.scen = scen;
        this.neighbourhoods = neighbourhoods;
        for (Neighbourhood nb : neighbourhoods) this.nbhPolicyPlan.put(nb.buurt, nb.policyPlan);
        this.nbhsWithoutDHStartYear = DistrictHeating.drawPolicyPlanYears(neighbourhoods, rng, startYear);
        this.all = new ArrayList<>(homeowners); this.all.addAll(landlords);
        for (HousingBlock b : blocks) this.all.addAll(b.households);
        this.hs = HeatingSystemData.freshSpecs();
        for (HeatingSystem t : HeatingSystem.values()) { cumInstalled.put(t, 0); prevShare.put(t, 0.0); }
        buildNetwork(networkSize);

        // --- DIAGNOSTIC (remove once Java/JS parity is resolved) -------------------------
        // JS reference at t0: 911 neighbourhoods, 67 with DH, 911 with surfaceArea>0,
        // avg homeowner network ~10, zero-network homeowners 0.
        if (System.getenv("HT_DIAG") != null) {
            int dhNbh = 0, areaOk = 0, planDH = 0;
            for (Neighbourhood nb : neighbourhoods) {
                if (nb.hasDH) dhNbh++;
                if (nb.surfaceAreaLand > 0) areaOk++;
                if ("DISTRICT_HEATING".equals(nb.policyPlan)) planDH++;
            }
            int dhDw = 0;
            for (Dwelling d : all) if (d.hasDistrictHeatingGrid) dhDw++;
            double netAvg = 0; int netZero = 0;
            for (Dwelling d : homeowners) { netAvg += d.network.size(); if (d.network.isEmpty()) netZero++; }
            netAvg /= Math.max(1, homeowners.size());
            System.out.printf("DIAG t0  nbh=%d dh=%d area>0=%d planDH=%d | dwellings=%d dhFlag=%d "
                            + "| homeowners=%d landlords=%d blocks=%d | avgNet=%.2f zeroNet=%d%n",
                    neighbourhoods.size(), dhNbh, areaOk, planDH, all.size(), dhDw,
                    homeowners.size(), landlords.size(), blocks.size(), netAvg, netZero);
            System.out.flush();
        }
        // --- end diagnostic ---------------------------------------------------------------
        for (Dwelling d : all) cumInstalled.merge(d.currentType, 1, Integer::sum);   // count the starting stock
        for (HeatingSystem t : HeatingSystem.values()) hs.get(t).initialUnits = Math.max(1, cumInstalled.get(t));
        int n0 = Math.max(1, all.size());
        for (HeatingSystem t : HeatingSystem.values())            // f_setInitialSalienceFactor: initial STOCK share
            hs.get(t).salienceFactor = Decision.salienceFactor((double) cumInstalled.get(t) / n0, (double) cumInstalled.get(t) / n0);
    }

    private double insulationCost(Dwelling d, String toLabel) {
        return vesta.insulationCost(d.archetype, d.constructionYear, d.energyLabel, toLabel, d.livingAreaM2);
    }

    private void buildNetwork(int k) {
        // Faithful port of Main.f_setNetworkSimilar (SMALL_WORLD_SIMILAR, the AL default).
        // networkSizeMean 25, shareLocal 0.8, shareSimilar 0.8; 6 attitude categories.
        final int NB_CAT = 6, MEAN_SIZE = 25; final double SHARE_LOCAL = 0.8, SHARE_SIMILAR = 0.8;
        int n = Math.max(1, homeowners.size());
        double sum = 0; for (Dwelling h : homeowners) sum += h.attitude;
        double mean = sum / n;
        double varr = 0; for (Dwelling h : homeowners) varr += (h.attitude - mean) * (h.attitude - mean);
        double sd = Math.sqrt(varr / n);
        double[] pv = { 0, mean + 2*sd, mean + sd, mean, mean - sd, mean - 2*sd, 0 };
        java.util.Map<Integer, java.util.List<Dwelling>> globalCat = new java.util.LinkedHashMap<>();
        for (int i = 1; i <= NB_CAT; i++) globalCat.put(i, new ArrayList<>());
        java.util.Map<String, java.util.List<Dwelling>> munMembers = new java.util.LinkedHashMap<>();
        java.util.Map<String, java.util.Map<Integer, java.util.List<Dwelling>>> munCat = new java.util.LinkedHashMap<>();
        for (Dwelling h : homeowners) {
            int cat = 1; while (cat < NB_CAT && h.attitude <= pv[cat]) cat++;
            h.category = cat; h.network = new ArrayList<>();
            String m = municipality(h.buurt);
            munMembers.computeIfAbsent(m, x -> new ArrayList<>()).add(h);
            munCat.computeIfAbsent(m, x -> { java.util.Map<Integer, java.util.List<Dwelling>> c = new java.util.LinkedHashMap<>();
                for (int i = 1; i <= NB_CAT; i++) c.put(i, new ArrayList<>()); return c; }).get(cat).add(h);
            globalCat.get(cat).add(h);
        }
        for (Dwelling h : homeowners) h.networkSize = Math.max(0, (int) Math.round(truncNormal(0, 1, 0.5, 0.05) * MEAN_SIZE));
        for (java.util.Map.Entry<String, java.util.List<Dwelling>> e : munMembers.entrySet()) {
            java.util.List<Dwelling> members = e.getValue();
            java.util.Map<Integer, java.util.List<Dwelling>> cats = munCat.get(e.getKey());
            for (Dwelling x : members) {
                for (int i = 0; i < x.networkSize; i++) {
                    double r = rng.next(), r2 = rng.next(); Dwelling conn;
                    if (r < SHARE_LOCAL) { java.util.List<Dwelling> ls = cats.get(x.category);
                        conn = (r2 < SHARE_SIMILAR && !ls.isEmpty()) ? ls.get(rng.nextInt(0, ls.size())) : members.get(rng.nextInt(0, members.size())); }
                    else { java.util.List<Dwelling> gs = globalCat.get(x.category);
                        conn = (r2 < SHARE_SIMILAR && !gs.isEmpty()) ? gs.get(rng.nextInt(0, gs.size())) : homeowners.get(rng.nextInt(0, homeowners.size())); }
                    x.network.add(conn); conn.network.add(x);
                }
            }
        }
        for (Dwelling h : homeowners) {
            h.peerCounts = new EnumMap<>(HeatingSystem.class);
            for (HeatingSystem t : HeatingSystem.values()) h.peerCounts.put(t, 0);
            for (Dwelling pp : h.network) h.peerCounts.merge(pp.currentType, 1, Integer::sum);
        }
    }
    private static String municipality(String buurt) {
        if (buurt == null || buurt.length() <= 4) return "NA";
        return buurt.substring(0, buurt.length() - 4);
    }
    private double truncNormal(double mn, double mx, double m, double sdv) {
        for (int i = 0; i < 100; i++) {
            double u = 0, v = 0; while (u == 0) u = rng.next(); while (v == 0) v = rng.next();
            double z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); double x = m + sdv * z;
            if (x >= mn && x <= mx) return x;
        }
        return m;
    }
    private void updateSalience() {
        int n = all.size();
        for (HeatingSystem t : HeatingSystem.values())
            hs.get(t).salienceFactor = Decision.salienceFactor((double) cumInstalled.get(t) / n, prevShare.get(t));
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
        for (HeatingSystem t : HeatingSystem.values()) { hs.get(t).minEAC = Double.POSITIVE_INFINITY; hs.get(t).maxEAC = Double.NEGATIVE_INFINITY; }
    }
    // J_Dwelling.f_getHeatingMethodPossibility -- ORDER MATTERS (early returns). Mirrors JS _possible.
    private boolean possible(HeatingSystem t, Dwelling d) {
        // 1. obligation to connect to DH overrides everything where a grid exists
        if (scen.dhConnectionObligation && d.hasDistrictHeatingGrid) return t == HeatingSystem.DISTRICT_HEATING;
        // 2. DH only where a grid exists
        if (t == HeatingSystem.DISTRICT_HEATING) return d.hasDistrictHeatingGrid;
        // 3. electric HP blocked under grid congestion (congestion state set by the DSO model)
        if (t == HeatingSystem.ELECTRIC_HEAT_PUMP && scen.gridCongestionHpBan && d.hasGridCongestion) return false;
        // 4. gas block only for apartments
        if (t == HeatingSystem.NATURAL_GAS_BLOCK) return d.archetype.equals("APARTMENT");
        return true;
    }
    private Map<HeatingSystem, long[]> eac(Dwelling d) {
        Map<HeatingSystem, long[]> out = new EnumMap<>(HeatingSystem.class);
        for (HeatingSystem t : HeatingSystem.values()) {
            long e = Economics.computeEAC(hs.get(t), d, this::insulationCost);
            out.put(t, new long[]{ e, possible(t, d) ? 1 : 0 });
            if (e < hs.get(t).minEAC) hs.get(t).minEAC = e;
            if (e > hs.get(t).maxEAC) hs.get(t).maxEAC = e;
        }
        return out;
    }
    private HeatingSystem chooseByEAC(Map<HeatingSystem, double[]> eacByType) { // [eac, possible]
        HeatingSystem best = null; double bestScore = Double.POSITIVE_INFINITY;
        for (HeatingSystem t : HeatingSystem.values()) {
            if (eacByType.get(t)[1] == 0) continue;
            double score = eacByType.get(t)[0] + Constants.GUMBEL_SCALE_EAC * rng.gumbel();
            if (score < bestScore) { bestScore = score; best = t; }
        }
        return best;
    }
    private HeatingSystem chooseByUtility(Dwelling d, Map<HeatingSystem, long[]> opts) {
        Map<HeatingSystem, Double> util = new EnumMap<>(HeatingSystem.class);
        for (HeatingSystem t : HeatingSystem.values()) {
            if (opts.get(t)[1] == 0) continue;
            HeatingSystemSpec h = hs.get(t);
            double att = Decision.attitudeValue(d.attitude, h.sustainabilityScoreNorm);
            // --- DIAGNOSTIC: accumulate TPB terms for gas vs hybrid (report years only) ---
            if (diagActive && (t == HeatingSystem.NATURAL_GAS_BOILER || t == HeatingSystem.HYBRID_HEAT_PUMP)) {
                int k = (t == HeatingSystem.NATURAL_GAS_BOILER) ? 0 : 1;
                diagN[k]++; diagAtt[k] += att;
            }
            double eff = Decision.effort(t == d.currentType, h.requiresLowTemp, d.hasLowTemp);
            double eacNorm = h.maxEAC > h.minEAC ? Decision.normalizedValue(opts.get(t)[0], h.minEAC, h.maxEAC) : 0;
            double pbc = Decision.pbc(eacNorm, eff);
            int peers = d.peerCounts.getOrDefault(t, 0);
            double sn = Decision.subjectiveNorm(peers, Math.max(1, d.network.size()), h.salienceFactor);
            if (diagActive && (t == HeatingSystem.NATURAL_GAS_BOILER || t == HeatingSystem.HYBRID_HEAT_PUMP)) {
                int k = (t == HeatingSystem.NATURAL_GAS_BOILER) ? 0 : 1;
                diagSn[k] += sn; diagPbc[k] += pbc; diagEacN[k] += eacNorm; diagSal[k] += h.salienceFactor;
            }
            double intent = Decision.intention(att, sn, pbc, slf, h.socialLearningRate);
            util.put(t, Decision.perceivedUtility(intent, pbc));
        }
        HeatingSystem best = null; double bestScore = Double.NEGATIVE_INFINITY;
        for (Map.Entry<HeatingSystem, Double> e : util.entrySet()) {
            double s = Decision.rumScore(e.getValue(), rng.gumbel());
            if (s > bestScore) { bestScore = s; best = e.getKey(); }
        }
        return best;
    }
    /** A triggered homeowner whose EACs were computed in pass 1, carried into pass 2. */
    private static final class PendingHomeowner {
        final Dwelling dwelling; final boolean endOfLife; final Map<HeatingSystem, long[]> opts;
        PendingHomeowner(Dwelling d, boolean eol, Map<HeatingSystem, long[]> opts) {
            this.dwelling = d; this.endOfLife = eol; this.opts = opts;
        }
    }
    private void notifyPeers(Dwelling d, HeatingSystem oldT, HeatingSystem newT) {
        for (Dwelling p : d.network) { p.peerCounts.merge(oldT, -1, Integer::sum); p.peerCounts.merge(newT, 1, Integer::sum); }
    }

    private void exogInsulOne(Dwelling d, int year) {
        int cur = Vesta.labelNum(d.energyLabel);
        if (cur > 1 && (year - d.yearLastRenovation) > 15 && rng.next() < 0.05) {
            int newNum = Math.max(1, cur - 2);
            d.energyLabel = Vesta.numLabel(newNum);
            d.yearLastRenovation = year;
            if (newNum <= 2) d.hasLowTemp = true;
        }
    }
    private void exogInsulBlock(HousingBlock b, int year) {   // J_HousingBlock.updateExogeneousInsulation (block-level)
        // NOTE the propagation loop sits INSIDE the >15-year condition but OUTSIDE the
        // rand<0.05 branch, so every dwelling is overwritten with the block label EVERY year
        // the block is overdue -- not only when a renovation fires. This continuously flattens
        // intra-block label heterogeneity. Do not "optimise" it into a change-guarded branch.
        int cur = Vesta.labelNum(b.energyLabel);
        if (cur > 1 && (year - b.yearLastRenovation) > 15) {
            if (rng.next() < 0.05) {                       // draw happens inside the outer if
                int newNum = Math.max(1, cur - 2);
                b.energyLabel = Vesta.numLabel(newNum);
                b.yearLastRenovation = year;
                if (newNum <= 2) b.hasLowTemp = true;
            }
            for (Dwelling d : b.households) {              // unconditional propagation
                d.energyLabel = b.energyLabel;
                d.hasLowTemp = b.hasLowTemp;
                d.yearLastRenovation = b.yearLastRenovation;
            }
        }
    }

    private static final boolean DYN = System.getenv("HT_DYN") != null;
    private static final boolean BLK = System.getenv("HT_BLOCK") != null;

    private YearRow stepYear(int year) {
        // HT_DYN probe: decision-time dynamic state (learned invest + salience + cumulative) per
        // heating type at the START of the year -- the values this year's decisions will use.
        // Compare 1:1 with AL's ALDYN probe (tests/compare_dyn.py). Prefix ENGDYN (same as JS).
        if (DYN) {
            for (HeatingSystem t : HeatingSystem.values()) {
                HeatingSystemSpec h = hs.get(t);
                System.out.printf(java.util.Locale.US,
                    "ENGDYN %d %s invest=%.0f salience=%.4f cumulative=%d initial=%.0f%n",
                    year, t, h.investMedium, h.salienceFactor, cumInstalled.get(t), h.initialUnits);
            }
        }
        diagActive = DIAG && (year == 2025 || year == 2030 || year == 2035);
        resetMinMax();
        for (Dwelling d : all) d.age++;
        for (HousingBlock b : blocks) b.age++;
        int n = all.size();
        for (HeatingSystem t : HeatingSystem.values()) prevShare.put(t, (double) cumInstalled.get(t) / n);

        // exogenous insulation (J_Dwelling.updateExogeneousInsulation)
        for (Dwelling d : homeowners) exogInsulOne(d, year);
        for (Dwelling d : landlords) exogInsulOne(d, year);
        for (HousingBlock b : blocks) { if (!b.households.isEmpty()) exogInsulBlock(b, year); }

        // District-heating company expands the grid (Main.f_adoptionProces step 2).
        // COST_BASED runs in the baseline too -- this is not policy-scenario-only.
        nbhWithDHPerc = DistrictHeating.expand(neighbourhoods, year, rng,
            scen.dhExpansionStrategy, scen.dhConstructionTime, nbhsWithoutDHStartYear);

        YearRow r = new YearRow(year);

        // HT_BLOCK probe: per-year, per-block-type counts of what triggered blocks CHOOSE, plus how
        // often the Gumbel pick differs from the deterministic cheapest (mirrors AL's
        // shbStochasticChoiceIsNotBestEAC). Emit ENGBLOCK lines after the loop. See compare_block.py.
        java.util.Map<String, long[]> blkChosen = null;   // key blockType -> count per HeatingSystem ordinal
        java.util.Map<String, long[]> blkStat = null;      // key blockType -> {triggered, households, notTopEAC, hybLtGas}
        java.util.Map<String, double[]> blkEac = null;     // key blockType -> {sumGasEAC, sumHybEAC}
        if (BLK) {
            blkChosen = new java.util.LinkedHashMap<>(); blkStat = new java.util.LinkedHashMap<>();
            blkEac = new java.util.LinkedHashMap<>();
            for (String bt : new String[]{"SOCIAL", "HOA"}) {
                blkChosen.put(bt, new long[HeatingSystem.values().length]);
                blkStat.put(bt, new long[4]);
                blkEac.put(bt, new double[2]);
            }
        }

        // 1. Social-housing + HOA blocks (whole block, avg EAC, end-of-life)
        for (HousingBlock b : blocks) {
            int lifetime = hs.get(b.currentType).lifetime;
            if (b.age < lifetime) continue;
            Map<HeatingSystem, Double> avg = new EnumMap<>(HeatingSystem.class);
            Map<HeatingSystem, Boolean> poss = new EnumMap<>(HeatingSystem.class);
            for (HeatingSystem t : HeatingSystem.values()) { avg.put(t, 0.0); poss.put(t, true); }
            for (Dwelling d : b.households) {
                Map<HeatingSystem, long[]> e = eac(d);
                for (HeatingSystem t : HeatingSystem.values()) {
                    avg.merge(t, (double) e.get(t)[0], Double::sum);
                    if (e.get(t)[1] == 0) poss.put(t, false);
                }
            }
            Map<HeatingSystem, double[]> eacByType = new EnumMap<>(HeatingSystem.class);
            for (HeatingSystem t : HeatingSystem.values())
                eacByType.put(t, new double[]{ avg.get(t) / b.households.size(), poss.get(t) ? 1 : 0 });
            r.considered += b.households.size();
            HeatingSystem chosen = chooseByEAC(eacByType);
            // SHA POLICY_BASED: a triggered social block follows its neighbourhood's TVW policy plan
            // instead of the cost choice (AL J_SocialHousingBlock.f_adoptHeatingMethodSHA). HOA blocks
            // are unaffected (they use f_adoptHeatingMethodHOA, always cost-based).
            if ("POLICY_BASED".equals(scen.shaStrategy) && "SOCIAL".equals(b.blockType)) {
                String pp = nbhPolicyPlan.getOrDefault(b.buurt, "NONE");
                if (!"NONE".equals(pp)) {
                    try { chosen = HeatingSystem.valueOf(pp); } catch (IllegalArgumentException ignore) {}
                }
            }
            if (BLK && chosen != null) {
                String bt = b.blockType;
                long[] st = blkStat.get(bt); st[0]++; st[1] += b.households.size();
                blkChosen.get(bt)[chosen.ordinal()]++;
                // deterministic cheapest among possible (no Gumbel):
                HeatingSystem top = null; double best = Double.POSITIVE_INFINITY;
                for (HeatingSystem t : HeatingSystem.values())
                    if (eacByType.get(t)[1] == 1 && eacByType.get(t)[0] < best) { best = eacByType.get(t)[0]; top = t; }
                if (chosen != top) st[2]++;
                double gEac = eacByType.get(HeatingSystem.NATURAL_GAS_BOILER)[0];
                double hEac = eacByType.get(HeatingSystem.HYBRID_HEAT_PUMP)[0];
                double[] ea = blkEac.get(bt); ea[0] += gEac; ea[1] += hEac;
                if (hEac < gEac) st[3]++;
            }
            if (chosen != null) {
                b.hasLowTemp = hs.get(chosen).requiresLowTemp;   // installNewHeatingMethodInHouseholds
                for (Dwelling d : b.households) {
                    d.hasLowTemp = b.hasLowTemp;
                    r.removed.merge(d.currentType, 1, Integer::sum);
                    r.installed.merge(chosen, 1, Integer::sum);
                    cumInstalled.merge(chosen, 1, Integer::sum);
                    d.currentType = chosen; d.age = 0;
                }
                b.currentType = chosen; b.age = 0;
            }
        }
        if (BLK) {
            for (String bt : new String[]{"SOCIAL", "HOA"}) {
                long[] st = blkStat.get(bt); long[] ch = blkChosen.get(bt); double[] ea = blkEac.get(bt);
                long nb = Math.max(1, st[0]);
                StringBuilder sb = new StringBuilder();
                for (HeatingSystem t : HeatingSystem.values()) sb.append(' ').append(t).append('=').append(ch[t.ordinal()]);
                System.out.printf(java.util.Locale.US,
                    "ENGBLOCK %d %s triggered=%d households=%d notTopEAC=%d hybLtGas=%d gasEAC=%.0f hybEAC=%.0f chosen:%s%n",
                    year, bt, st[0], st[1], st[2], st[3], ea[0]/nb, ea[1]/nb, sb.toString());
            }
        }

        // 2. Landlords (individual, end-of-life, cost)
        for (Dwelling d : landlords) {
            int lifetime = hs.get(d.currentType).lifetime;
            if (!Decision.hasEndOfLifeTrigger(d.age, lifetime)) continue;
            r.considered++;
            Map<HeatingSystem, long[]> e = eac(d);
            Map<HeatingSystem, double[]> eacByType = new EnumMap<>(HeatingSystem.class);
            for (HeatingSystem t : HeatingSystem.values()) eacByType.put(t, new double[]{ e.get(t)[0], e.get(t)[1] });
            HeatingSystem chosen = chooseByEAC(eacByType);
            if (chosen != null) { r.removed.merge(d.currentType, 1, Integer::sum); r.installed.merge(chosen, 1, Integer::sum);
                cumInstalled.merge(chosen, 1, Integer::sum);
                if (!d.hasLowTemp) d.hasLowTemp = hs.get(chosen).requiresLowTemp;
                d.currentType = chosen; d.age = 0; }
        }

        // 3. Homeowners (TPB utility; end-of-life or 75%-of-life opportunity)
        // TWO PASSES, faithful to Main.f_adoptionProces step 5. AL computes the EACs of EVERY
        // triggered homeowner first, so minEAC/maxEAC is COMPLETE before any utility is
        // evaluated ("executed in seperate step to ensure valid normalization"). Interleaving
        // makes eacNorm depend on how many agents have contributed to the window so far, which
        // silently biases every homeowner decision. Do NOT merge these loops, and do NOT call
        // eac() again in pass 2 -- that would double-count into the min/max window.
        List<PendingHomeowner> hoPending = new ArrayList<>();
        for (Dwelling d : homeowners) {
            int lifetime = hs.get(d.currentType).lifetime;
            boolean eol = Decision.hasEndOfLifeTrigger(d.age, lifetime);
            boolean opp = Decision.hasOpportunityTrigger(d.age, lifetime, legacyTrigger);
            if (!eol && !opp) continue;
            hoPending.add(new PendingHomeowner(d, eol, eac(d)));  // PASS 1: populate window only
        }
        for (PendingHomeowner p : hoPending) {                    // PASS 2: utility + decide
            Dwelling d = p.dwelling;
            boolean eol = p.endOfLife;
            Map<HeatingSystem, long[]> opts = p.opts;
            r.considered++;
            HeatingSystem chosen = chooseByUtility(d, opts);
            if (chosen == null) continue;
            if (eol || chosen != d.currentType) {
                notifyPeers(d, d.currentType, chosen);
                r.removed.merge(d.currentType, 1, Integer::sum);
                r.installed.merge(chosen, 1, Integer::sum);
                cumInstalled.merge(chosen, 1, Integer::sum);
                if (!d.hasLowTemp) d.hasLowTemp = hs.get(chosen).requiresLowTemp;
                d.currentType = chosen; d.age = 0;
            }
        }

        updateSalience();

        if (year == 2025 || year == 2030 || year == 2035) diagReport(year);
        updateLearningCurve();
        for (Dwelling d : all) { r.stock.merge(d.currentType, 1, Integer::sum);
            r.stockOwn.get(d.ownership).merge(d.currentType, 1, Integer::sum); }
        for (HeatingSystem t : HeatingSystem.values()) r.cumInstalled.put(t, cumInstalled.get(t));
        r.nbhWithDhPerc = nbhWithDHPerc;
        r.nbhCongestionPerc = nbhCongestionPerc;
        return r;
    }

    public List<YearRow> run() {
        List<YearRow> rows = new ArrayList<>();
        YearRow r0 = new YearRow(startYear);
        for (Dwelling d : all) { r0.stock.merge(d.currentType, 1, Integer::sum);
            r0.stockOwn.get(d.ownership).merge(d.currentType, 1, Integer::sum); }
        for (HeatingSystem t : HeatingSystem.values()) r0.cumInstalled.put(t, cumInstalled.get(t));
        rows.add(r0);
        for (int y = startYear + 1; y <= endYear; y++) rows.add(stepYear(y));
        return rows;
    }
}
