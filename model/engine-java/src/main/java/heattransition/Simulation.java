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
        // Loop-state at DECISION TIME (start of the year): the intermediates of the two reinforcing
        // loops, so the feedback can be plotted rather than inferred. Emitted to loop_state.csv.
        public final Map<HeatingSystem, Double> learnedCapex = new EnumMap<>(HeatingSystem.class);
        public final Map<HeatingSystem, Double> salience = new EnumMap<>(HeatingSystem.class);
        public final Map<HeatingSystem, Double> energyPrice = new EnumMap<>(HeatingSystem.class);
        /** Per-segment tracing (Q6): key = "segType|segment|technology" ->
         *  {stock, installed, considered, sumAtt, sumSn, sumPbc, sumUtil, sumEac, nChosen, sumIntent}. */
        public final java.util.Map<String, double[]> seg = new java.util.LinkedHashMap<>();
        public double[] segSlot(String type, String name, HeatingSystem t) {
            return seg.computeIfAbsent(type + "|" + name + "|" + t, k -> new double[10]);
        }
        public int considered = 0;
        public double nbhWithDhPerc = 0;      // % of neighbourhoods with a DH grid this year
        public double nbhCongestionPerc = 0;  // % of neighbourhoods in grid congestion this year
        // avg_* decision columns (means emitted to the CSV), accumulated per heating type this year.
        // TPB terms (att/util/sub_norm/pbc) are meaningful only for homeowners -> reported for
        // PRIVATELY_OWNED and TOTAL, 0 elsewhere. avg_eac is tracked per ownership so every
        // category (incl. landlords/social/HOA) gets a real EAC to plot.
        static final int NT = HeatingSystem.values().length;
        public final double[] hoAtt = new double[NT], hoUtil = new double[NT], hoSn = new double[NT], hoPbc = new double[NT];
        public final int[] hoN = new int[NT];
        public final Map<String, double[]> eacSum = new java.util.HashMap<>();
        public final Map<String, int[]> eacN = new java.util.HashMap<>();
        YearRow(int y) {
            year = y;
            for (HeatingSystem t : HeatingSystem.values()) {
                stock.put(t, 0); installed.put(t, 0); removed.put(t, 0); cumInstalled.put(t, 0);
            }
            for (String o : OWN) { Map<HeatingSystem, Integer> m = new EnumMap<>(HeatingSystem.class);
                for (HeatingSystem t : HeatingSystem.values()) m.put(t, 0); stockOwn.put(o, m);
                eacSum.put(o, new double[NT]); eacN.put(o, new int[NT]); }
        }
        /** Mean of a homeowner TPB accumulator for a heating type (0 if none evaluated it). */
        public double hoMean(double[] sum, int ord) { return hoN[ord] > 0 ? sum[ord] / hoN[ord] : 0; }
        /** Mean EAC for an ownership + heating type; own="TOTAL" aggregates across all categories. */
        public double eacMean(String own, int ord) {
            if ("TOTAL".equals(own)) {
                double s = 0; int n = 0;
                for (String o : OWN) { s += eacSum.get(o)[ord]; n += eacN.get(o)[ord]; }
                return n > 0 ? s / n : 0;
            }
            int n = eacN.get(own)[ord];
            return n > 0 ? eacSum.get(own)[ord] / n : 0;
        }
        /** How many deciders of this ownership evaluated this type this year (0 -> emit blank, not 0,
         *  so a non-triggered year is excluded from cross-iteration averaging instead of biasing it). */
        public int eacCount(String own, int ord) {
            if ("TOTAL".equals(own)) { int n = 0; for (String o : OWN) n += eacN.get(o)[ord]; return n; }
            return eacN.get(own)[ord];
        }
    }
    static final String[] OWN = { "PRIVATELY_OWNED", "PRIVATELY_RENTED", "SOCIAL_HOUSING", "HOME_OWNER_ASSOCIATION" };

    private final int startYear, endYear;
    private final Rng rng;
    private final boolean legacyTrigger;
    /** DH review interval of this run: the -Dht.dhReviewYears override, else the scenario's. */
    private final int dhReviewYears;
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
    private final Map<HeatingSystem, double[]> lastTerms = new EnumMap<>(HeatingSystem.class);  // per-option TPB terms of the last homeowner evaluated

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
        this.dhReviewYears = Constants.DH_REVIEW_YEARS >= 0 ? Constants.DH_REVIEW_YEARS : scen.dhReviewYears;
        this.slf = Constants.SOCIAL_LEARNING_MULT >= 0 ? Constants.SOCIAL_LEARNING_MULT
                : Constants.LEARNING_MULTIPLIER.getOrDefault(scen.socialLearningFactor, 1.0);
        this.elf = Constants.LEARNING_MULTIPLIER.getOrDefault(scen.economicLearningFactor, 1.0);
        this.homeowners = homeowners; this.landlords = landlords; this.vesta = vesta;
        this.blocks = new ArrayList<>(socialBlocks); this.blocks.addAll(hoaBlocks);
        Subsidies.start(scen.subsidyPolicy, startYear);   // policy of this run, nothing phased out yet
        // DistrictHeatingCompany state. Policy plan years are drawn ONCE per run, and the
        // "no start year" count feeds chanceOfDHBeingInstalled in the POLICY_BASED branch.
        this.scen = scen;
        this.neighbourhoods = neighbourhoods;
        for (Neighbourhood nb : neighbourhoods) this.nbhPolicyPlan.put(nb.buurt, nb.policyPlan);
        this.nbhsWithoutDHStartYear = DistrictHeating.drawPolicyPlanYears(neighbourhoods, rng, startYear);
        this.all = new ArrayList<>(homeowners); this.all.addAll(landlords);
        for (HousingBlock b : blocks) this.all.addAll(b.households);
        this.hs = HeatingSystemData.freshSpecs();
        // stochastic equipment lifetime: draw each agent's end-of-life age for its CURRENT system
        for (Dwelling d : all) d.lifeDraw = drawLife(d.currentType);
        for (HousingBlock b : blocks) b.lifeDraw = drawLife(b.currentType);
        for (HeatingSystem t : HeatingSystem.values()) { cumInstalled.put(t, 0); prevShare.put(t, 0.0); }
        buildNetwork(networkSize);
        // Adopter + dwelling segmentation (Q6): tag every dwelling so adoption can be traced per
        // group, and homeowners additionally by Rogers category from their propensity index.
        for (Dwelling d : all) { d.segDwelling = Segments.dwellingSegment(d);
                                 d.segContext = Segments.contextSegment(d); }
        for (Dwelling d : homeowners) {
            double net = 0; int n = 0;
            for (Dwelling p : d.network) { net += p.attitude; n++; }
            d.propensity = Segments.propensity(d, n > 0 ? net / n : d.attitude);
        }
        Segments.assignRogers(homeowners);

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
        // DSO: size each neighbourhood's grid from its ACTUAL t0 dwelling demand (never g_ele), so
        // grids start adequate and congestion emerges from the transition (heat pumps + EVs) itself.
        for (Neighbourhood nb : neighbourhoods) {
            double l0 = GridModel.peakLoadKW(nb, GridModel.evs(nb, startYear), hs);
            nb.gridLoadKW = l0;
            nb.gridCapacityKW = GridModel.sizeCapacityKW(l0, nb.allDwellings.size());
            nb.hasGridCongestion = false;
        }
        for (HeatingSystem t : HeatingSystem.values())            // f_setInitialSalienceFactor: initial STOCK share
            hs.get(t).salienceFactor = Decision.salienceFactor((double) cumInstalled.get(t) / n0, (double) cumInstalled.get(t) / n0);
    }

    /** Re-derive a dwelling's heat demand from its CURRENT label. Insulation therefore lowers future
     *  running costs, which is what makes an insulated dwelling cheaper to heat with a heat pump and
     *  feeds back into every later adoption decision. Hot water is unaffected by insulation. */
    /** A heating system with a required label includes that insulation upgrade in its EAC, so on
     *  adoption the dwelling's label must actually improve -- otherwise the household pays for
     *  insulation and never receives the demand reduction. */
    private void applyRequiredInsulation(Dwelling d, HeatingSystem chosen) {
        String req = hs.get(chosen).requiredLabel;
        if (req == null || req.equals("no")) return;
        if (Vesta.labelNum(req) < Vesta.labelNum(d.energyLabel)) {   // lower number = better label
            d.energyLabel = req;
        }
        refreshHeatDemand(d);
    }

    private void refreshHeatDemand(Dwelling d) {
        double space = vesta.spaceHeatKWh(d.archetype, d.constructionYear, d.energyLabel, d.livingAreaM2);
        if (space < 0) return;                       // unknown archetype/label: keep the current value
        d.heatDemandKWh = d.demandFactor * space + d.dhwKWh;
    }

    private double insulationCost(Dwelling d, String toLabel) {
        return vesta.insulationCost(d.archetype, d.constructionYear, d.energyLabel, toLabel, d.livingAreaM2);
    }

    private void buildNetwork(int k) {
        // Faithful port of Main.f_setNetworkSimilar (SMALL_WORLD_SIMILAR, the AL default).
        // networkSizeMean 25, shareLocal 0.8, shareSimilar 0.8; 6 attitude categories.
        // mean size and share-similar are sensitivity parameters (-Dht.networkSize / networkShareSimilar)
        final int NB_CAT = 6, MEAN_SIZE = Constants.NETWORK_SIZE;
        final double SHARE_LOCAL = 0.8, SHARE_SIMILAR = Constants.NETWORK_SHARE_SIMILAR;
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
        if (Constants.SALIENCE_FREEZE) return;   // knock-out: social-learning loop disabled
        int n = all.size();
        for (HeatingSystem t : HeatingSystem.values())
            hs.get(t).salienceFactor = Decision.salienceFactor((double) cumInstalled.get(t) / n, prevShare.get(t));
    }
    /** Set this year's energy prices and fixed connection charges on every technology spec, from
     *  data/reference/energy_prices_by_year.csv (see {@link EnergyPrices}): an absolute retail price
     *  per year per arm, plus the annual gas and heat connection charges. */
    private void updateEnergyPrices(int year) {
        // The arm comes from the scenario's energy_prices column; -Dht.priceT overrides it, which is
        // what the LHS uses to sweep the arm continuously.
        double t = Constants.PRICE_T >= 0 ? Constants.PRICE_T : scen.priceT();
        for (HeatingSystem type : HeatingSystem.values()) {
            HeatingSystemSpec h = hs.get(type);
            h.primaryCostPerKWh   = EnergyPrices.variable(h.primarySource,   year, t);
            h.secondaryCostPerKWh = EnergyPrices.variable(h.secondarySource, year, t);
            // Fixed connection charges. Only the GAS and HEAT connections differ between options;
            // the electricity connection is common to all of them and so is not charged. The heat
            // connection costs the ACM fixed charge + metering + delivery-set rental.
            double fixed = 0;
            if (!Constants.FIXED_COSTS_OFF) {
                if (h.gasConnection)  fixed += EnergyPrices.gasFixed(year, t);
                if (h.heatConnection) fixed += DistrictHeatingCosts.annualFixed();
            }
            h.fixedAnnualCost = fixed;
        }
    }
    /** DSO step: recompute each neighbourhood's peak load, flag congestion, then let the grid
     *  operator reinforce a share of congested neighbourhoods (scenario rate GRR). Congestion is
     *  propagated to the dwellings so possible() rule 3 can block electric heat pumps. */
    private void updateGrid(int year) {
        if (Constants.CONGESTION_OFF || neighbourhoods.isEmpty()) return;
        java.util.List<Neighbourhood> congested = new ArrayList<>();
        for (Neighbourhood nb : neighbourhoods) {
            nb.gridLoadKW = GridModel.peakLoadKW(nb, GridModel.evs(nb, year), hs);
            nb.hasGridCongestion = nb.gridLoadKW > nb.gridCapacityKW;
            if (nb.hasGridCongestion) congested.add(nb);
        }
        // reinforcement: capacity is raised to serve the current load with the design margin
        double rate = Constants.GRID_REINFORCE_RATE.getOrDefault(scen.gridReinforcementRate, 0.15);
        int budget = (int) Math.ceil(rate * congested.size());
        for (int i = 0; i < Math.min(budget, congested.size()); i++) {
            Neighbourhood nb = congested.get(i);
            nb.gridCapacityKW = Math.max(nb.gridCapacityKW,
                    GridModel.sizeCapacityKW(nb.gridLoadKW, nb.allDwellings.size()));
            nb.hasGridCongestion = false;
        }
        int stillCongested = 0;
        for (Neighbourhood nb : neighbourhoods) {
            for (Dwelling d : nb.allDwellings) d.hasGridCongestion = nb.hasGridCongestion;
            if (nb.hasGridCongestion) stillCongested++;
        }
        nbhCongestionPerc = (double) stillCongested / neighbourhoods.size();
    }

    private void updateLearningCurve() {
        for (HeatingSystem t : HeatingSystem.values()) {
            HeatingSystemSpec h = hs.get(t);
            double f = Decision.capexLearningFactor(cumInstalled.get(t), h.initialUnits, h.economicLearningRate, elf);
            h.investSmall = h.initialInvestSmall * f;
            h.investMedium = h.initialInvestMedium * f;
            h.investLarge = h.initialInvestLarge * f;
            h.investFixed = h.initialInvestFixed * f;
            h.investPerKw = h.initialInvestPerKw * f;
        }
    }
    /** ONE cost scale shared by all technologies and all dwellings in the year (see eacNorm below). */
    private double globalMinEAC = Double.POSITIVE_INFINITY, globalMaxEAC = Double.NEGATIVE_INFINITY;

    private void resetMinMax() {
        globalMinEAC = Double.POSITIVE_INFINITY; globalMaxEAC = Double.NEGATIVE_INFINITY;
        for (HeatingSystem t : HeatingSystem.values()) { hs.get(t).minEAC = Double.POSITIVE_INFINITY; hs.get(t).maxEAC = Double.NEGATIVE_INFINITY; }
    }
    // J_Dwelling.f_getHeatingMethodPossibility -- ORDER MATTERS (early returns). Mirrors JS _possible.
    /** Draw a jittered end-of-life age for a newly (re)installed system of type t. */
    private int drawLife(HeatingSystem t) {
        return rng.jitteredLifetime(hs.get(t).lifetime, Constants.LIFETIME_JITTER_SD, Constants.LIFETIME_JITTER_MAX);
    }
    private boolean possible(HeatingSystem t, Dwelling d) {
        // 1. obligation to connect to DH overrides everything where a grid exists
        if (scen.dhConnectionObligation && d.hasDistrictHeatingGrid) return t == HeatingSystem.DISTRICT_HEATING;
        // 2. DH only where a grid exists
        if (t == HeatingSystem.DISTRICT_HEATING) return d.hasDistrictHeatingGrid;
        // 3. electric HP blocked under grid congestion (congestion state set by the DSO model)
        if (t == HeatingSystem.ELECTRIC_HEAT_PUMP && scen.gridCongestionHpBan && d.hasGridCongestion) return false;
        // 4. gas block is a COLLECTIVE system: it may only be kept/replaced by a dwelling that is
        //    ALREADY on block heating. An individual boiler never becomes block heating. Combined
        //    with rule 5, gas block is a closed category that can only shrink or upgrade to HP/DH --
        //    never grow from boilers. (Fixes the privately-rented boiler->block ratchet that inflated
        //    gas block; MODEL_TODOS A.) No archetype condition: the loader also puts terraced and
        //    other dwellings in HOA blocks, and a block can only re-install gas block if every one of
        //    its households may, so an archetype test locked mixed blocks out of gas altogether.
        if (t == HeatingSystem.NATURAL_GAS_BLOCK)
            return d.currentType == HeatingSystem.NATURAL_GAS_BLOCK;
        // 5. conversely, a dwelling on collective block heating cannot switch to an INDIVIDUAL gas
        //    boiler (a block can't fragment into per-dwelling boilers; the two gas rows are
        //    byte-identical in the AL data apart from the DB key, so the swap has no cost basis). It
        //    re-installs block heating or upgrades to a heat pump / district heating instead.
        if (t == HeatingSystem.NATURAL_GAS_BOILER && d.currentType == HeatingSystem.NATURAL_GAS_BLOCK) return false;
        return true;
    }
    private Map<HeatingSystem, long[]> eac(Dwelling d) {
        Map<HeatingSystem, long[]> out = new EnumMap<>(HeatingSystem.class);
        for (HeatingSystem t : HeatingSystem.values()) {
            long e = Economics.computeEAC(hs.get(t), d, this::insulationCost);
            out.put(t, new long[]{ e, possible(t, d) ? 1 : 0 });
            if (e < hs.get(t).minEAC) hs.get(t).minEAC = e;      // per-type, retained for HT_DIAG only
            if (e > hs.get(t).maxEAC) hs.get(t).maxEAC = e;
            if (possible(t, d)) {                                  // global window over the options
                if (e < globalMinEAC) globalMinEAC = e;            // a household could actually choose
                if (e > globalMaxEAC) globalMaxEAC = e;
                if (EACPROBE) probeAll.add((double) e);
            }
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
    private HeatingSystem chooseByUtility(Dwelling d, Map<HeatingSystem, long[]> opts, YearRow r) {
        lastTerms.clear();
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
            // Affordability is normalised on ONE cost scale shared by all technologies and dwellings
            // (population-wide, across technologies), so it carries the real cost DIFFERENCE between a
            // household's options: options that are close together score similarly (cost barely
            // discriminates for that household), options far apart score far apart (cost dominates).
            // Per-technology normalisation was rejected -- it rescales each technology by its own
            // population spread, which erases the level difference and can even invert it (an
            // expensive technology with a wide spread scored as MORE affordable).
            // The scale is LOGARITHMIC (Decision.normalizedLog). A linear global min/max let dwelling
            // SIZE dominate: the window was stretched by a few very large/expensive dwellings (p99
            // was less than half the max), so a household's own option spread occupied only ~6-13% of
            // the scale while the size signal spanned ~40%. log(EAC) makes the mapping PROPORTIONAL --
            // people weigh cost differences in %, not euros -- so a +40% gap reads similarly for a
            // small and a large dwelling, and the expensive tail is compressed without clamping.
            double eacNorm = globalMaxEAC > globalMinEAC
                    ? Decision.normalizedLog(opts.get(t)[0], globalMinEAC, globalMaxEAC) : 0;
            double pbc = Decision.pbc(eacNorm, eff);
            int peers = d.peerCounts.getOrDefault(t, 0);
            double sn = Decision.subjectiveNorm(peers, Math.max(1, d.network.size()), h.salienceFactor);
            if (diagActive && (t == HeatingSystem.NATURAL_GAS_BOILER || t == HeatingSystem.HYBRID_HEAT_PUMP)) {
                int k = (t == HeatingSystem.NATURAL_GAS_BOILER) ? 0 : 1;
                diagSn[k] += sn; diagPbc[k] += pbc; diagEacN[k] += eacNorm; diagSal[k] += h.salienceFactor;
            }
            double intent = Decision.intention(att, sn, pbc, slf, h.socialLearningRate);
            double pu = Decision.perceivedUtility(intent, pbc);
            util.put(t, pu);
            lastTerms.put(t, new double[]{ att, sn, pbc, pu, opts.get(t)[0], intent });   // for segment tracing
            // avg_* accumulation (homeowners -> PRIVATELY_OWNED): TPB terms + raw EAC per type.
            int o = t.ordinal();
            r.hoAtt[o] += att; r.hoUtil[o] += pu; r.hoSn[o] += sn; r.hoPbc[o] += pbc; r.hoN[o]++;
            r.eacSum.get("PRIVATELY_OWNED")[o] += opts.get(t)[0]; r.eacN.get("PRIVATELY_OWNED")[o]++;
        }
        HeatingSystem best = null; double bestScore = Double.NEGATIVE_INFINITY;
        for (Map.Entry<HeatingSystem, Double> e : util.entrySet()) {
            double s = Decision.rumScore(e.getValue(), rng.gumbel());
            if (s > bestScore) { bestScore = s; best = e.getKey(); }
        }
        if (NONMAX) {   // diagnostic: did the random term overrule the highest modelled utility?
            HeatingSystem top = null; double topU = Double.NEGATIVE_INFINITY;
            for (Map.Entry<HeatingSystem, Double> e : util.entrySet())
                if (e.getValue() > topU) { topU = e.getValue(); top = e.getKey(); }
            long[] c = NONMAX_COUNT.computeIfAbsent(r.year, y -> new long[2]);
            synchronized (c) { c[0]++; if (best != top) c[1]++; }
        }
        return best;
    }
    // HT_NONMAX=1: per year, the share of homeowner decisions in which the chosen option is NOT the one
    // with the highest modelled utility (the random term decided). Printed at exit; draws no random
    // numbers, so results are identical with and without it.
    private static final boolean NONMAX = System.getenv("HT_NONMAX") != null;
    private static final Map<Integer, long[]> NONMAX_COUNT = new java.util.concurrent.ConcurrentSkipListMap<>();
    static {
        if (NONMAX) Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Map.Entry<Integer, long[]> e : NONMAX_COUNT.entrySet())
                System.out.printf(java.util.Locale.US, "NONMAX %d decisions=%d nonmax=%d share=%.4f%n",
                        e.getKey(), e.getValue()[0], e.getValue()[1], (double) e.getValue()[1] / Math.max(1, e.getValue()[0]));
        }));
    }
    // HT_TRIGGERS=1: per year, how many decisions each trigger produced and how many of them changed
    // technology, per agent type. Slots: homeowner eol/eolSwitch/opp/oppSwitch/dhReview/dhLeave,
    // landlord eol/eolSwitch/dhReview/dhLeave, block households eol/eolSwitch/dhReview/dhLeave.
    // Printed at exit; draws no random numbers, so results are identical with and without it.
    // Counts accumulate over every scenario and iteration in the JVM: run one at a time.
    private static final boolean TRIG = System.getenv("HT_TRIGGERS") != null;
    private static final Map<Integer, long[]> TRIG_COUNT = new java.util.concurrent.ConcurrentSkipListMap<>();
    private static final String[] TRIG_NAMES = {"hoEol", "hoEolSwitch", "hoOpp", "hoOppSwitch", "hoDhReview",
            "hoDhLeave", "llEol", "llEolSwitch", "llDhReview", "llDhLeave", "blkEol", "blkEolSwitch",
            "blkDhReview", "blkDhLeave", "blkShaConnectOnArrival"};
    private static void trig(int year, int slot, long n) {
        if (!TRIG) return;
        long[] c = TRIG_COUNT.computeIfAbsent(year, y -> new long[TRIG_NAMES.length]);
        synchronized (c) { c[slot] += n; }
    }
    static {
        if (TRIG) Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Map.Entry<Integer, long[]> e : TRIG_COUNT.entrySet()) {
                StringBuilder sb = new StringBuilder("TRIGGERS ").append(e.getKey());
                for (int i = 0; i < TRIG_NAMES.length; i++) sb.append(' ').append(TRIG_NAMES[i]).append('=').append(e.getValue()[i]);
                System.out.println(sb);
            }
        }));
    }
    /** Install {@code chosen} in every household of a block (installNewHeatingMethodInHouseholds). */
    private void switchBlock(HousingBlock b, HeatingSystem chosen, YearRow r) {
        b.hasLowTemp = hs.get(chosen).requiresLowTemp;
        for (Dwelling d : b.households) {
            d.hasLowTemp = b.hasLowTemp;
            applyRequiredInsulation(d, chosen);
            r.removed.merge(d.currentType, 1, Integer::sum);
            r.installed.merge(chosen, 1, Integer::sum);
            cumInstalled.merge(chosen, 1, Integer::sum);
            d.currentType = chosen; d.age = 0; d.lifeDraw = drawLife(chosen);
        }
        b.currentType = chosen; b.age = 0; b.lifeDraw = drawLife(chosen);
    }
    /** A triggered homeowner whose EACs were computed in pass 1, carried into pass 2. */
    private static final class PendingHomeowner {
        final Dwelling dwelling; final boolean endOfLife; final Map<HeatingSystem, long[]> opts;
        PendingHomeowner(Dwelling d, boolean eol, Map<HeatingSystem, long[]> opts) {
            this.dwelling = d; this.endOfLife = eol; this.opts = opts;
        }
    }
    private void notifyPeers(Dwelling d, HeatingSystem oldT, HeatingSystem newT) {
        if (Constants.PEER_FREEZE) return;   // knock-out: peer composition frozen at t0
        for (Dwelling p : d.network) { p.peerCounts.merge(oldT, -1, Integer::sum); p.peerCounts.merge(newT, 1, Integer::sum); }
    }

    private void exogInsulOne(Dwelling d, int year) {
        int cur = Vesta.labelNum(d.energyLabel);
        if (cur > 1 && (year - d.yearLastRenovation) > 15 && rng.next() < 0.05) {
            int newNum = Math.max(1, cur - 2);
            d.energyLabel = Vesta.numLabel(newNum); refreshHeatDemand(d);
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
                d.energyLabel = b.energyLabel; refreshHeatDemand(d);
                d.hasLowTemp = b.hasLowTemp;
                d.yearLastRenovation = b.yearLastRenovation;
            }
        }
    }

    // HT_EACPROBE: dump the global EAC window, its distribution, and a few example dwellings, so the
    // width of the shared cost scale can be judged (is a household's own spread visible on it?).
    private static final boolean EACPROBE = System.getenv("HT_EACPROBE") != null;

    // ---- cost-optimality probe -------------------------------------------------------------
    // HT_COSTPROBE=<path.csv> records, per year, the cross-tabulation of the option a homeowner
    // WOULD have taken on equivalent annual cost alone against the option actually chosen through
    // the full behavioural function, plus the annual cost premium accepted when they differ.
    // Off unless the variable is set; nothing else in the run changes.
    private static final String COSTPROBE = System.getenv("HT_COSTPROBE");
    private boolean costProbeHeader = false;

    /** Cheapest POSSIBLE option on EAC alone (no random-utility term, no behavioural weights). */
    private static HeatingSystem cheapestPossible(Map<HeatingSystem, long[]> opts) {
        HeatingSystem best = null; double bestEac = Double.POSITIVE_INFINITY;
        for (HeatingSystem t : HeatingSystem.values()) {
            long[] o = opts.get(t);
            if (o == null || o[1] == 0) continue;          // not available for this dwelling
            if (o[0] < bestEac) { bestEac = o[0]; best = t; }
        }
        return best;
    }

    private void writeCostProbe(int year, java.util.Map<String, long[]> counts,
                                java.util.Map<String, double[]> premium) {
        try (java.io.PrintWriter w = new java.io.PrintWriter(new java.io.FileWriter(COSTPROBE, true))) {
            if (!costProbeHeader) {
                java.io.File f = new java.io.File(COSTPROBE);
                if (f.length() == 0) w.println("year,cheapest_on_cost,chosen,households,mean_eac_premium_eur");
                costProbeHeader = true;
            }
            for (java.util.Map.Entry<String, long[]> e : counts.entrySet()) {
                String[] kv = e.getKey().split("\\|");
                long n = e.getValue()[0];
                double prem = n > 0 ? premium.get(e.getKey())[0] / n : 0.0;
                w.printf(java.util.Locale.ROOT, "%d,%s,%s,%d,%.1f%n", year, kv[0], kv[1], n, prem);
            }
        } catch (java.io.IOException ex) {
            System.err.println("[costprobe] " + ex.getMessage());
        }
    }
    private final java.util.List<Double> probeAll = new ArrayList<>();

    private void eacProbe(int year, List<PendingHomeowner> pending) {
        if (!EACPROBE || pending.isEmpty()) return;
        double[] v = probeAll.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        java.util.function.DoubleUnaryOperator pct = q -> v[(int) Math.min(v.length - 1, Math.max(0, Math.round(q * (v.length - 1))))];
        System.out.printf("%nEACPROBE %d  global LOG window [%.0f, %.0f]  log-width %.3f  (n=%d options)%n",
                year, globalMinEAC, globalMaxEAC, Math.log(globalMaxEAC) - Math.log(globalMinEAC), v.length);
        System.out.printf("  EAC percentiles: p1=%.0f p5=%.0f p25=%.0f p50=%.0f p75=%.0f p95=%.0f p99=%.0f%n",
                pct.applyAsDouble(.01), pct.applyAsDouble(.05), pct.applyAsDouble(.25), pct.applyAsDouble(.50),
                pct.applyAsDouble(.75), pct.applyAsDouble(.95), pct.applyAsDouble(.99));
        // three example dwellings: smallest / median / largest floor area among the triggered ones
        List<PendingHomeowner> byArea = new ArrayList<>(pending);
        byArea.sort((a, b) -> Double.compare(a.dwelling.livingAreaM2, b.dwelling.livingAreaM2));
        int[] pick = { 0, byArea.size() / 2, byArea.size() - 1 };
        String[] lbl = { "small", "median", "large" };
        for (int i = 0; i < 3; i++) {
            PendingHomeowner ph = byArea.get(pick[i]);
            Dwelling d = ph.dwelling;
            StringBuilder eacs = new StringBuilder(), norms = new StringBuilder();
            double lo = Double.MAX_VALUE, hi = -Double.MAX_VALUE;
            for (HeatingSystem t : HeatingSystem.values()) {
                if (ph.opts.get(t)[1] == 0) continue;
                double e = ph.opts.get(t)[0];
                double n = Decision.normalizedLog(e, globalMinEAC, globalMaxEAC);
                lo = Math.min(lo, e); hi = Math.max(hi, e);
                eacs.append(String.format(" %s=%.0f", t.toString().substring(0, 4), e));
                norms.append(String.format(" %s=%.3f", t.toString().substring(0, 4), 1 - n));
            }
            System.out.printf("  [%s] area=%.0fm2 label=%s%n    EAC:%s   (own log-spread %.3f = %.1f%% of global log-width)%n    affordability (1-eacNorm):%s%n",
                    lbl[i], d.livingAreaM2, d.energyLabel, eacs, Math.log(hi) - Math.log(lo),
                    100 * (Math.log(hi) - Math.log(lo)) / (Math.log(globalMaxEAC) - Math.log(globalMinEAC)), norms);
        }
        probeAll.clear();
        System.out.flush();
    }

    private static final boolean DYN = System.getenv("HT_DYN") != null;
    private static final boolean BLK = System.getenv("HT_BLOCK") != null;

    private YearRow stepYear(int year) {
        updateEnergyPrices(year);   // apply the fuel price path for THIS year before any EAC is computed
        {   // subsidies of THIS year; under PHASE_OUT a technology whose stock share reached the threshold loses it
            Map<HeatingSystem, Double> share = new EnumMap<>(HeatingSystem.class);
            for (HeatingSystem t : HeatingSystem.values()) share.put(t, 0.0);
            for (Dwelling d : all) share.merge(d.currentType, 1.0, Double::sum);
            for (HeatingSystem t : HeatingSystem.values()) share.put(t, share.get(t) / Math.max(1, all.size()));
            for (HeatingSystem t : Subsidies.setYear(year, share))
                System.out.printf(java.util.Locale.US, "[subsidy] %d: %s subsidy phased out (stock share %.1f %%)%n",
                        year, t, 100 * share.get(t));
        }
        YearRow loopRow = null;     // filled at the end; capture decision-time loop state now
        Map<HeatingSystem, Double> capex0 = new EnumMap<>(HeatingSystem.class),
                                   sal0   = new EnumMap<>(HeatingSystem.class),
                                   price0 = new EnumMap<>(HeatingSystem.class);
        for (HeatingSystem t : HeatingSystem.values()) {
            HeatingSystemSpec h = hs.get(t);
            capex0.put(t, Economics.investmentCostsSupply(h, 10000)); sal0.put(t, h.salienceFactor); price0.put(t, h.primaryCostPerKWh);
        }
        // HT_DYN probe: decision-time dynamic state (learned invest + salience + cumulative) per
        // heating type at the START of the year -- the values this year's decisions will use.
        // Compare 1:1 with AL's ALDYN probe (tests/compare_dyn.py). Prefix ENGDYN (same as JS).
        if (DYN) {
            for (HeatingSystem t : HeatingSystem.values()) {
                HeatingSystemSpec h = hs.get(t);
                System.out.printf(java.util.Locale.US,
                    "ENGDYN %d %s invest=%.0f salience=%.4f cumulative=%d initial=%.0f%n",
                    year, t, Economics.investmentCostsSupply(h, 10000), h.salienceFactor, cumInstalled.get(t), h.initialUnits);
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
        boolean shaPolicy = "POLICY_BASED".equals(scen.shaStrategy);
        java.util.Set<String> gridBefore = new java.util.HashSet<>();
        if (shaPolicy) for (Neighbourhood nb : neighbourhoods) if (nb.hasDH) gridBefore.add(nb.buurt);
        nbhWithDHPerc = DistrictHeating.expand(neighbourhoods, year, rng,
            scen.dhExpansionStrategy, Constants.DH_CONSTRUCTION_YEARS >= 0
                ? (int) Constants.DH_CONSTRUCTION_YEARS : scen.dhConstructionTime,
            nbhsWithoutDHStartYear);
        // neighbourhoods whose heat network became operational this year (SHA POLICY_BASED only)
        java.util.Set<String> gridNew = new java.util.HashSet<>();
        if (shaPolicy) for (Neighbourhood nb : neighbourhoods) if (nb.hasDH && !gridBefore.contains(nb.buurt)) gridNew.add(nb.buurt);

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

        // 1. Social-housing + HOA blocks (whole block, avg EAC, end-of-life; a block on district
        //    heating instead reviews every DH_REVIEW_YEARS and stays unless another option wins)
        for (HousingBlock b : blocks) {
            boolean dhReview = b.currentType == HeatingSystem.DISTRICT_HEATING;
            // SHA POLICY_BASED, plan = district heating: the housing association connects the block
            // in the year the heat network arrives in its neighbourhood, whatever the age of its
            // current system (network and connection go hand in hand).
            boolean shaPlanDH = shaPolicy && "SOCIAL".equals(b.blockType) && !b.households.isEmpty()
                    && "DISTRICT_HEATING".equals(nbhPolicyPlan.get(b.buurt));
            if (shaPlanDH && !dhReview && gridNew.contains(b.buurt)) {
                if (TRIG) { trig(year, 14, b.households.size()); }
                switchBlock(b, HeatingSystem.DISTRICT_HEATING, r);
                continue;
            }
            if (dhReview ? !Decision.hasDhReviewTrigger(b.age, dhReviewYears)
                         : !Decision.hasEndOfLifeTrigger(b.age, b.lifeDraw)) continue;
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
            // avg_eac accumulation for blocks, household-weighted, keyed by block ownership.
            String eacOwnKey = "HOA".equals(b.blockType) ? "HOME_OWNER_ASSOCIATION" : "SOCIAL_HOUSING";
            int nh = b.households.size();
            for (HeatingSystem t : HeatingSystem.values()) if (eacByType.get(t)[1] == 1) {
                r.eacSum.get(eacOwnKey)[t.ordinal()] += eacByType.get(t)[0] * nh; r.eacN.get(eacOwnKey)[t.ordinal()] += nh; }
            HeatingSystem chosen = chooseByEAC(eacByType);
            // SHA POLICY_BASED: a triggered social block follows its neighbourhood's TVW policy plan
            // instead of the cost choice (AL J_SocialHousingBlock.f_adoptHeatingMethodSHA). HOA blocks
            // are unaffected (they use f_adoptHeatingMethodHOA, always cost-based). Unlike AL, a
            // district-heating plan is only followed where the network is already there; before it
            // arrives the block makes the cost choice, and connects once it does (see above).
            if (shaPolicy && "SOCIAL".equals(b.blockType)) {
                String pp = nbhPolicyPlan.getOrDefault(b.buurt, "NONE");
                if (!"NONE".equals(pp)) {
                    try {
                        HeatingSystem plan = HeatingSystem.valueOf(pp);
                        if (plan != HeatingSystem.DISTRICT_HEATING || b.households.get(0).hasDistrictHeatingGrid)
                            chosen = plan;
                    } catch (IllegalArgumentException ignore) {}
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
            if (dhReview && chosen == HeatingSystem.DISTRICT_HEATING) chosen = null;   // stays connected
            if (TRIG) {
                int nh0 = b.households.size();
                trig(year, dhReview ? 12 : 10, nh0);
                if (chosen != null && chosen != b.currentType) trig(year, dhReview ? 13 : 11, nh0);
            }
            if (chosen != null) switchBlock(b, chosen, r);
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

        // 2. Landlords (individual, end-of-life, cost; district heating: periodic review)
        for (Dwelling d : landlords) {
            boolean dhReview = d.currentType == HeatingSystem.DISTRICT_HEATING;
            if (dhReview ? !Decision.hasDhReviewTrigger(d.age, dhReviewYears)
                         : !Decision.hasEndOfLifeTrigger(d.age, d.lifeDraw)) continue;
            r.considered++;
            Map<HeatingSystem, long[]> e = eac(d);
            Map<HeatingSystem, double[]> eacByType = new EnumMap<>(HeatingSystem.class);
            for (HeatingSystem t : HeatingSystem.values()) eacByType.put(t, new double[]{ e.get(t)[0], e.get(t)[1] });
            // avg_eac accumulation for landlords (PRIVATELY_RENTED), possible types only.
            for (HeatingSystem t : HeatingSystem.values()) if (e.get(t)[1] != 0) {
                r.eacSum.get("PRIVATELY_RENTED")[t.ordinal()] += e.get(t)[0]; r.eacN.get("PRIVATELY_RENTED")[t.ordinal()]++; }
            HeatingSystem chosen = chooseByEAC(eacByType);
            if (dhReview && chosen == HeatingSystem.DISTRICT_HEATING) chosen = null;   // stays connected
            if (TRIG) {
                trig(year, dhReview ? 8 : 6, 1);
                if (chosen != null && chosen != d.currentType) trig(year, dhReview ? 9 : 7, 1);
            }
            if (chosen != null) { r.removed.merge(d.currentType, 1, Integer::sum); r.installed.merge(chosen, 1, Integer::sum);
                cumInstalled.merge(chosen, 1, Integer::sum);
                if (!d.hasLowTemp) d.hasLowTemp = hs.get(chosen).requiresLowTemp;
                applyRequiredInsulation(d, chosen);
                d.currentType = chosen; d.age = 0; d.lifeDraw = drawLife(chosen); }
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
            int lifetime = d.lifeDraw;
            // On district heating there is no end of life: a periodic review acts as an
            // opportunity trigger, so the dwelling only changes if another option wins.
            boolean onDH = d.currentType == HeatingSystem.DISTRICT_HEATING;
            boolean eol = !onDH && Decision.hasEndOfLifeTrigger(d.age, lifetime);
            boolean opp = onDH ? Decision.hasDhReviewTrigger(d.age, dhReviewYears)
                               : Decision.hasOpportunityTrigger(d.age, lifetime, legacyTrigger);
            if (!eol && !opp) continue;
            hoPending.add(new PendingHomeowner(d, eol, eac(d)));  // PASS 1: populate window only
        }
        if (EACPROBE && (year == 2025 || year == 2030)) eacProbe(year, hoPending);
        java.util.Map<String, long[]> cpCount = COSTPROBE != null ? new java.util.LinkedHashMap<>() : null;
        java.util.Map<String, double[]> cpPrem  = COSTPROBE != null ? new java.util.LinkedHashMap<>() : null;
        for (PendingHomeowner p : hoPending) {                    // PASS 2: utility + decide
            Dwelling d = p.dwelling;
            boolean eol = p.endOfLife;
            Map<HeatingSystem, long[]> opts = p.opts;
            r.considered++;
            HeatingSystem chosen = chooseByUtility(d, opts, r);
            if (TRIG) {
                boolean onDH = d.currentType == HeatingSystem.DISTRICT_HEATING;
                int base = onDH ? 4 : eol ? 0 : 2;
                trig(year, base, 1);
                if (chosen != null && chosen != d.currentType) trig(year, base + 1, 1);
            }
            if (chosen == null) continue;
            if (COSTPROBE != null) {
                HeatingSystem cheap = cheapestPossible(opts);
                if (cheap != null) {
                    String key = cheap.name() + "|" + chosen.name();
                    cpCount.computeIfAbsent(key, k -> new long[1])[0]++;
                    cpPrem.computeIfAbsent(key, k -> new double[1])[0] +=
                            (double) opts.get(chosen)[0] - (double) opts.get(cheap)[0];
                }
            }
            // per-segment tracing: record the DRIVER TERMS OF THE OPTION ACTUALLY CHOSEN, so each
            // segment's adoption can be attributed to attitude / social norm / affordability.
            double[] tm = lastTerms.get(chosen);
            if (tm != null) {
                for (String[] sg : new String[][]{{"rogers", d.segRogers}, {"dwelling", d.segDwelling},
                                                  {"context", d.segContext}}) {
                    double[] a = r.segSlot(sg[0], sg[1], chosen);
                    a[2] += 1;                       // considered/triggered
                    a[3] += tm[0]; a[4] += tm[1]; a[5] += tm[2]; a[6] += tm[3]; a[7] += tm[4];
                    a[9] += tm[5];                   // intention of the chosen option
                    a[8] += 1;                       // n for the means
                }
            }
            if (eol || chosen != d.currentType) {
                notifyPeers(d, d.currentType, chosen);
                r.removed.merge(d.currentType, 1, Integer::sum);
                r.installed.merge(chosen, 1, Integer::sum);
                cumInstalled.merge(chosen, 1, Integer::sum);
                if (!d.hasLowTemp) d.hasLowTemp = hs.get(chosen).requiresLowTemp;
                applyRequiredInsulation(d, chosen);
                r.segSlot("rogers", d.segRogers, chosen)[1]++;      // installed this year
                r.segSlot("dwelling", d.segDwelling, chosen)[1]++;
                r.segSlot("context", d.segContext, chosen)[1]++;
                d.currentType = chosen; d.age = 0; d.lifeDraw = drawLife(chosen);
            }
        }

        if (COSTPROBE != null) writeCostProbe(year, cpCount, cpPrem);

        updateGrid(year);      // DSO reacts to this year's adoption before the next year's decisions
        updateSalience();

        if (year == 2025 || year == 2030 || year == 2035) diagReport(year);
        updateLearningCurve();
        for (Dwelling d : all) { r.stock.merge(d.currentType, 1, Integer::sum);
            r.stockOwn.get(d.ownership).merge(d.currentType, 1, Integer::sum); }
        for (HeatingSystem t : HeatingSystem.values()) r.cumInstalled.put(t, cumInstalled.get(t));
        r.nbhWithDhPerc = nbhWithDHPerc;
        r.nbhCongestionPerc = nbhCongestionPerc;
        r.learnedCapex.putAll(capex0); r.salience.putAll(sal0); r.energyPrice.putAll(price0);
        for (Dwelling d : all) {                       // per-segment stock snapshot
            r.segSlot("dwelling", d.segDwelling, d.currentType)[0]++;
            r.segSlot("context", d.segContext, d.currentType)[0]++;
            if (!"NA".equals(d.segRogers)) r.segSlot("rogers", d.segRogers, d.currentType)[0]++;
        }
        return r;
    }

    public List<YearRow> run() {
        List<YearRow> rows = new ArrayList<>();
        YearRow r0 = new YearRow(startYear);
        for (Dwelling d : all) { r0.stock.merge(d.currentType, 1, Integer::sum);
            r0.stockOwn.get(d.ownership).merge(d.currentType, 1, Integer::sum); }
        for (HeatingSystem t : HeatingSystem.values()) r0.cumInstalled.put(t, cumInstalled.get(t));
        rows.add(r0);
        java.util.Set<String> dh0 = new java.util.HashSet<>();
        for (Neighbourhood nb : neighbourhoods) if (nb.hasDH) dh0.add(nb.buurt);
        for (int y = startYear + 1; y <= endYear; y++) rows.add(stepYear(y));
        if (System.getenv("HT_DHNET") != null) {
            // HT_DHNET=1: one line per neighbourhood, heat-network state at the end of the run.
            for (Neighbourhood nb : neighbourhoods)
                System.out.printf(java.util.Locale.US, "DHNET %s start=%b end=%b costYear=%d plan=%s planYear=%d "
                        + "planStart=%d planEnd=%d dwellings=%d social=%d%n", nb.buurt, dh0.contains(nb.buurt), nb.hasDH,
                        nb.dhYearOfOperation, nb.policyPlan, nb.policyPlanYear, nb.policyStartJaar, nb.policyEindJaar,
                        nb.allDwellings.size(), nb.allDwellings.stream().filter(d -> "SOCIAL_HOUSING".equals(d.ownership)).count());
        }
        return rows;
    }
}
