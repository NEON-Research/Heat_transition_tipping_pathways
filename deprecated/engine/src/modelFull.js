// modelFull.js — multi-owner ABM: social-housing blocks + landlords + homeowners, all
// feeding the SHARED cumulative-installs that drive the learning curve + salience.
// Faithful to Main.f_adoptionProces order:
//   age -> SHA blocks (whole block, EAC) -> landlords (individual, EAC)
//   -> homeowners (TPB utility) -> update salience + learning curve.
// Blocks/landlords decide on cost (getStochasticChoice on EAC + gumbelScaleEAC*gumbel).
import { HEATING_SYSTEMS, LEARNING_MULTIPLIER, GUMBEL_SCALE_EAC } from './constants.js';
import { HEATING_SYSTEM_DATA, MAX_SUSTAINABILITY_SCORE } from '../data/heating_systems.js';
import * as D from './decision.js';
import { dhExpansion, drawPolicyPlanYears } from './districtHeating.js';
import { computeEAC } from './economics.js';
import { archRow, insulationCost as vestaInsul, labelNum, numLabel } from './vesta.js';


export class FullSimulation {
  constructor(cfg, agents) {
    this.cfg = cfg; this.rng = cfg.rng; this.archIndex = agents.archIndex;
    this.homeowners = agents.homeowners;
    this.landlords = agents.landlords;
    this.blocks = [...agents.socialBlocks, ...(agents.hoaBlocks || [])];
    this.blockTrace = cfg.blockTrace || null;   // optional decision-trace sink
    // DistrictHeatingCompany state. Policy plan years are drawn ONCE per run (AL does this
    // during setup), and the "no start year" count drives chanceOfDHBeingInstalled.
    this.neighbourhoods = agents.neighbourhoods || [];
    this.nbhsWithoutDHStartYear = drawPolicyPlanYears(this.neighbourhoods, this.rng, cfg.startYear);
    this.nbhWithDHPerc = 0;
    this.all = [...this.homeowners, ...this.landlords, ...this.blocks.flatMap(b => b.households)];
    this.slf = LEARNING_MULTIPLIER[cfg.scenario.social_learning_factor ?? 'MEDIUM'];
    this.elf = LEARNING_MULTIPLIER[cfg.scenario.economic_learning_factor ?? 'MEDIUM'];
    this.legacyTrigger = !!cfg.legacyTrigger;

    this.hs = {};
    const _scores = HEATING_SYSTEMS.map(t => HEATING_SYSTEM_DATA[t].sustainabilityScore);
    const _minS = Math.min(..._scores), _maxS = Math.max(..._scores);   // AL: min-max normalization
    for (const t of HEATING_SYSTEMS) {
      const base = HEATING_SYSTEM_DATA[t];
      this.hs[t] = { ...base, sustainabilityScoreNorm: (base.sustainabilityScore - _minS) / (_maxS - _minS),
        salienceFactor: 0, _iS: base.investSmall, _iM: base.investMedium, _iL: base.investLarge,
        minEAC: Infinity, maxEAC: -Infinity };
    }
    // AL (corrected): installedCumulative = starting stock + all new installs (never decremented);
    // learning base = initial STOCK (f_setInitialUnitsInstalledPerHeatingMethod). doublings = log2((stock+new)/stock).
    this.cumInstalled = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));  // = stock, then += each install
    this.prevShare = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    this._buildNetwork(cfg.networkSize ?? 10);
    for (const d of this.all) this.cumInstalled[d.currentType]++;             // count the starting stock
    for (const t of HEATING_SYSTEMS) this.hs[t]._initialUnits = Math.max(1, this.cumInstalled[t]);
    const n0 = this.all.length || 1;                                          // f_setInitialSalienceFactor: stock share
    for (const t of HEATING_SYSTEMS)
      this.hs[t].salienceFactor = D.salienceFactor(this.cumInstalled[t] / n0, this.cumInstalled[t] / n0);
  }

  _buildNetwork(k) {
    // Faithful port of Main.f_setNetworkSimilar (SMALL_WORLD_SIMILAR, the AL default).
    // Homophily: connect preferentially to same attitude-category peers, biased to same
    // municipality. Constants from f_setNetworkAssumptions: networkSizeMean 25, shareLocal 0.8,
    // shareSimilar 0.8; 6 attitude categories on mean +/- 2sd,1sd bands.
    const ho = this.homeowners, n = ho.length || 1;
    const NB_CAT = 6, MEAN_SIZE = 25, SHARE_LOCAL = 0.8, SHARE_SIMILAR = 0.8;
    let sum = 0; for (const h of ho) sum += h.attitude;
    const mean = sum / n;
    let varr = 0; for (const h of ho) varr += (h.attitude - mean) ** 2;
    const sd = Math.sqrt(varr / n);
    const pv = [0, mean + 2*sd, mean + sd, mean, mean - sd, mean - 2*sd, 0]; // pv[1..5] used
    const catOf = att => { let c = 1; while (c < NB_CAT && att <= pv[c]) c++; return c; };
    const munOf = d => (d.buurt || 'NA').slice(0, -4) || 'NA';
    const truncN = (mn, mx, m, s) => { for (let i = 0; i < 100; i++) {
      let u = 0, v = 0; while (u === 0) u = this.rng.next(); while (v === 0) v = this.rng.next();
      const z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); const x = m + s * z;
      if (x >= mn && x <= mx) return x; } return m; };

    const globalCat = {}; for (let i = 1; i <= NB_CAT; i++) globalCat[i] = [];
    const munMembers = new Map(), munCat = new Map();
    for (const h of ho) {
      h.category = catOf(h.attitude); h.network = [];
      const m = munOf(h);
      if (!munMembers.has(m)) { munMembers.set(m, []);
        const c = {}; for (let i = 1; i <= NB_CAT; i++) c[i] = []; munCat.set(m, c); }
      munMembers.get(m).push(h); munCat.get(m)[h.category].push(h); globalCat[h.category].push(h);
    }
    for (const h of ho) h.networkSize = Math.max(0, Math.round(truncN(0, 1, 0.5, 0.05) * MEAN_SIZE));
    const randFrom = arr => arr[this.rng.int(0, arr.length)];
    for (const [m, members] of munMembers) {
      const cats = munCat.get(m);
      for (const x of members) {
        for (let i = 0; i < x.networkSize; i++) {
          const r = this.rng.next(), r2 = this.rng.next();
          let conn;
          if (r < SHARE_LOCAL) { const ls = cats[x.category];
            conn = (r2 < SHARE_SIMILAR && ls.length) ? randFrom(ls) : randFrom(members); }
          else { const gs = globalCat[x.category];
            conn = (r2 < SHARE_SIMILAR && gs.length) ? randFrom(gs) : randFrom(ho); }
          x.network.push(conn); conn.network.push(x);
        }
      }
    }
    for (const h of ho) {
      h.peerCounts = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
      for (const p of h.network) h.peerCounts[p.currentType]++;
    }
  }
  _updateSalience() {
    const n = this.all.length;
    for (const t of HEATING_SYSTEMS)
      this.hs[t].salienceFactor = D.salienceFactor(this.cumInstalled[t] / n, this.prevShare[t]);
  }
  _updateLearningCurve() {
    for (const t of HEATING_SYSTEMS) {
      const h = this.hs[t];
      const fct = D.capexLearningFactor(this.cumInstalled[t], h._initialUnits, h.economicLearningRate, this.elf);
      h.investSmall = h._iS * fct; h.investMedium = h._iM * fct; h.investLarge = h._iL * fct;
    }
  }
  _insul(d, toLabel) {
    return vestaInsul(archRow(this.archIndex, d.archetype, d.constructionYear), d.energyLabel, toLabel, d.livingAreaM2);
  }
  _resetMinMax() { for (const t of HEATING_SYSTEMS) { this.hs[t].minEAC = Infinity; this.hs[t].maxEAC = -Infinity; } }
  // J_Dwelling.f_getHeatingMethodPossibility -- ORDER MATTERS, it early-returns.
  _possible(t, d) {
    const sc = this.cfg.scenario || {};
    // 1. obligation to connect to DH overrides everything, where a grid exists
    if (sc.DH_connection_obligation && d.hasDistrictHeatingGrid) return t === 'DISTRICT_HEATING';
    // 2. DH only where a grid exists
    if (t === 'DISTRICT_HEATING') return d.hasDistrictHeatingGrid === true;
    // 3. electric HP blocked under grid congestion
    if (t === 'ELECTRIC_HEAT_PUMP' && sc.congestion_block && d.hasGridCongestion) return false;
    // 4. gas block only for apartments
    if (t === 'NATURAL_GAS_BLOCK') return d.archetype === 'APARTMENT';
    return true;
  }
  _eac(d) {
    const out = {};
    for (const t of HEATING_SYSTEMS) {
      const e = computeEAC(this.hs[t], d, (dd, lab) => this._insul(dd, lab));
      out[t] = { eac: e, possible: this._possible(t, d) };
      if (e < this.hs[t].minEAC) this.hs[t].minEAC = e;
      if (e > this.hs[t].maxEAC) this.hs[t].maxEAC = e;
      // HT_DIAG: capture the dwelling that sets the hybrid maximum, with its attributes
      if (this._maxProbe && t === 'HYBRID_HEAT_PUMP' && e > this._maxProbe.eac) {
        const ins = this._insul(d, 'c');
        this._maxProbe = { eac: e, phase: this._phase, own: d.ownership, label: d.energyLabel,
          area: +d.livingAreaM2, demand: Math.round(d.heatDemandKWh), lowTemp: d.hasLowTemp, insul: Math.round(ins) };
      }
    }
    return out;
  }
  // lowest-EAC stochastic choice (landlords + blocks)
  _chooseByEAC(eacByType) {
    let best = null, bestScore = Infinity;
    for (const t of HEATING_SYSTEMS) {
      if (!eacByType[t].possible) continue;
      const score = eacByType[t].eac + GUMBEL_SCALE_EAC * this.rng.gumbel();
      if (score < bestScore) { bestScore = score; best = t; }
    }
    return best;
  }
  // TPB utility choice (homeowners)
  _chooseByUtility(d, opts) {
    const util = {};
    for (const t of HEATING_SYSTEMS) {
      if (!opts[t].possible) continue;
      const h = this.hs[t];
      const att = D.attitudeValue(d.attitude, h.sustainabilityScoreNorm);
      const eff = D.effort(t === d.currentType, h.requiresLowTemp, d.hasLowTemp);
      const eacNorm = h.maxEAC > h.minEAC ? D.normalizedValue(opts[t].eac, h.minEAC, h.maxEAC) : 0;
      const pbc = D.pbc(eacNorm, eff);
      const sn = D.subjectiveNorm(d.peerCounts[t], d.network.length || 1, h.salienceFactor);
      const intent = D.intention(att, sn, pbc, this.slf, h.socialLearningRate);
      util[t] = D.perceivedUtility(intent, pbc);
      // --- DIAGNOSTIC: accumulate TPB terms for gas vs hybrid (HT_DIAG only) ---
      if (this._diag && (t === 'NATURAL_GAS_BOILER' || t === 'HYBRID_HEAT_PUMP')) {
        const g = this._diag[t === 'NATURAL_GAS_BOILER' ? 0 : 1];
        g.n++; g.att += att; g.sn += sn; g.pbc += pbc; g.eacN += eacNorm; g.sal += h.salienceFactor;
      }
    }
    let best = null, bestScore = -Infinity;
    for (const t of Object.keys(util)) {
      const s = D.rumScore(util[t], this.rng.gumbel());
      if (s > bestScore) { bestScore = s; best = t; }
    }
    return best;
  }
  _notifyPeers(d, oldT, newT) { for (const p of d.network) { p.peerCounts[oldT]--; p.peerCounts[newT]++; } }

  _exogInsulOne(d, year) {
    const cur = labelNum(d.energyLabel);
    if (cur > 1 && (year - d.yearLastRenovation) > 15 && this.rng.next() < 0.05) {
      const newNum = Math.max(1, cur - 2);
      d.energyLabel = numLabel(newNum);
      d.yearLastRenovation = year;
      if (newNum <= 2) d.hasLowTemp = true;
    }
  }
  _exogenousInsulation(list, year) { for (const d of list) this._exogInsulOne(d, year); }

  stepYear(year) {
    // HT_DYN probe: emit the DECISION-TIME dynamic state (learned invest + salience + cumulative)
    // per heating type at the START of the year, i.e. exactly the values this year's block/
    // landlord/homeowner decisions will use (investMedium set by last year's _updateLearningCurve,
    // salienceFactor by last year's _updateSalience). Compare 1:1 with AL's ALDYN probe to see the
    // first year the dynamics diverge. One line per type per year; prefix ENGDYN.
    if (process.env.HT_DYN) {
      for (const t of HEATING_SYSTEMS) {
        const h = this.hs[t];
        console.error(`ENGDYN ${year} ${t} invest=${Math.round(h.investMedium)} `
          + `salience=${h.salienceFactor.toFixed(4)} cumulative=${this.cumInstalled[t]} `
          + `initial=${h._initialUnits}`);
      }
    }
    // HT_DIAG probe: accumulate homeowner TPB terms for gas/hybrid, print at 2025/2030/2035.
    this._diag = (process.env.HT_DIAG && (year === 2025 || year === 2030 || year === 2035))
      ? [{ n: 0, att: 0, sn: 0, pbc: 0, eacN: 0, sal: 0 }, { n: 0, att: 0, sn: 0, pbc: 0, eacN: 0, sal: 0 }]
      : null;
    this._maxProbe = this._diag ? { eac: -Infinity } : null;
    this._phase = null;
    this._resetMinMax();
    for (const d of this.all) d.age++;
    for (const b of this.blocks) b.age++;
    const n = this.all.length;
    for (const t of HEATING_SYSTEMS) this.prevShare[t] = this.cumInstalled[t] / n;

    // exogenous insulation (J_Dwelling.updateExogeneousInsulation): non-block households + blocks
    this._exogenousInsulation(this.homeowners, year);
    this._exogenousInsulation(this.landlords, year);
    // J_HousingBlock.updateExogeneousInsulation -- faithful port. NOTE the propagation loop
    // sits INSIDE the outer >15-year condition but OUTSIDE the rand<0.05 branch, so every
    // dwelling in the block has its individual label OVERWRITTEN by the block label EVERY
    // year the block is overdue for renovation -- not only when a renovation actually fires.
    // This continuously flattens intra-block label heterogeneity, which keeps heat-pump
    // retrofit costs uniformly high. Do not "optimise" this into a change-guarded branch.
    for (const b of this.blocks) {
      const cur = labelNum(b.energyLabel);
      if (cur > 1 && (year - b.yearLastRenovation) > 15) {
        if (this.rng.next() < 0.05) {                     // draw happens inside the outer if
          const nn = Math.max(1, cur - 2);
          b.energyLabel = numLabel(nn);
          b.yearLastRenovation = year;
          if (nn <= 2) b.hasLowTemp = true;
        }
        for (const d of b.households) {                   // unconditional propagation
          d.energyLabel = b.energyLabel;
          d.hasLowTemp = b.hasLowTemp;
          d.yearLastRenovation = b.yearLastRenovation;
        }
      }
    }

    // 0. District-heating company expands the grid (Main.f_adoptionProces step 2).
    //    COST_BASED runs in the baseline too -- this is not policy-scenario-only.
    this.nbhWithDHPerc = dhExpansion(this.neighbourhoods, year, this.rng, {
      strategy: (this.cfg.scenario && this.cfg.scenario.DH_strategy) || 'COST_BASED',
      nbhsWithoutDHStartYear: this.nbhsWithoutDHStartYear,
    });

    const inst = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    const rem = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    let considered = 0;

    // 1. Social-housing blocks (end-of-life; whole block switches on avg EAC)
    this._phase = 'block';
    for (const b of this.blocks) {
      const lifetime = this.hs[b.currentType].lifetime;
      // AL J_HousingBlock.hasTrigger() is END-OF-LIFE ONLY -- verified in the .alp:
      //     if( heatingSystem.getAge() >= lifetime ) { setAverageHeatingSystemOptionsEAC(); return true; }
      //     else return false;
      // NOTE this differs from J_Household.hasTrigger(), which is EOL || opportunity.
      // Blocks get NO opportunity trigger. Do not "unify" these two.
      if (b.age < lifetime) continue;
      // average EAC across households
      const avg = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
      const poss = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, true]));
      const nImp = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
      for (const d of b.households) {
        const e = this._eac(d);
        for (const t of HEATING_SYSTEMS) {
          avg[t] += e[t].eac;
          if (!e[t].possible) { poss[t] = false; nImp[t]++; }   // AL ANDs isPossible over ALL households
        }
      }
      const eacByType = Object.fromEntries(HEATING_SYSTEMS.map(t =>
        [t, { eac: avg[t] / b.households.length, possible: poss[t] }]));
      considered += b.households.length;
      const chosen = this._chooseByEAC(eacByType);
      if (this.blockTrace) {
        const trigger = b.age >= lifetime ? 'end_of_life' : 'opportunity';
        for (const t of HEATING_SYSTEMS) {
          this.blockTrace.push({
            year: year, block_type: b.blockType || 'SOCIAL', buurtcode: b.buurt || 'NA',
            block_key: b.blockKey, n_households: b.households.length,
            current_hs: b.currentType, hs_age: b.age, lifetime,
            trigger, option: t,
            avg_eac: Math.round(eacByType[t].eac * 100) / 100,
            is_possible: eacByType[t].possible ? 1 : 0,
            n_impossible: nImp[t], chosen: t === chosen ? 1 : 0,
          });
        }
      }
      if (chosen) {
        b.hasLowTemp = this.hs[chosen].requiresLowTemp;      // installNewHeatingMethodInHouseholds
        for (const d of b.households) {
          d.hasLowTemp = b.hasLowTemp;
          if (d.currentType !== chosen) { rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++; }
          else { rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++; } // end-of-life always reinstalls
          d.currentType = chosen; d.age = 0;
        }
        b.currentType = chosen; b.age = 0;
      }
    }

    // 2. Landlords (individual, end-of-life, cost)
    this._phase = 'landlord';
    for (const d of this.landlords) {
      const lifetime = this.hs[d.currentType].lifetime;
      if (!D.hasEndOfLifeTrigger(d.age, lifetime)) continue;
      considered++;
      const chosen = this._chooseByEAC(this._eac(d));
      if (chosen) { rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++;
        if (!d.hasLowTemp) d.hasLowTemp = this.hs[chosen].requiresLowTemp;
        d.currentType = chosen; d.age = 0; }
    }

    // 3. Homeowners (TPB utility; end-of-life or opportunity)
    // TWO PASSES, faithful to Main.f_adoptionProces step 5. AL computes the EACs of EVERY
    // triggered homeowner first, so minEAC/maxEAC is COMPLETE before any utility is evaluated
    // ("executed in seperate step to ensure valid normalization"). Interleaving the two makes
    // eacNorm depend on how many agents have contributed to the window so far, which silently
    // biases every homeowner decision. Do NOT merge these loops back together, and do NOT
    // recompute _eac() in pass 2 -- that would double-count into the min/max window.
    this._phase = 'homeowner';
    const hoPending = [];
    for (const d of this.homeowners) {
      const lifetime = this.hs[d.currentType].lifetime;
      const eol = D.hasEndOfLifeTrigger(d.age, lifetime);
      const opp = D.hasOpportunityTrigger(d.age, lifetime, this.legacyTrigger);
      if (!eol && !opp) continue;
      hoPending.push({ d, eol, opts: this._eac(d) });   // PASS 1: populate the window only
    }
    for (const { d, eol, opts } of hoPending) {         // PASS 2: utility + decide
      considered++;
      const chosen = this._chooseByUtility(d, opts);
      if (!chosen) continue;
      if (eol || chosen !== d.currentType) {
        this._notifyPeers(d, d.currentType, chosen);
        rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++;
        if (!d.hasLowTemp) d.hasLowTemp = this.hs[chosen].requiresLowTemp;
        d.currentType = chosen; d.age = 0;
      }
    }

    if (this._diag) {
      const lbl = ['gas   ', 'hybrid'];
      const wt = ['NATURAL_GAS_BOILER', 'HYBRID_HEAT_PUMP'];
      for (let k = 0; k < 2; k++) {
        const g = this._diag[k]; const n = g.n || 1; const h = this.hs[wt[k]];
        console.error(`JSDIAG ${year} ${lbl[k]} n=${g.n} att=${(g.att/n).toFixed(3)} `
          + `sn=${(g.sn/n).toFixed(3)} pbc=${(g.pbc/n).toFixed(3)} `
          + `eacNorm=${(g.eacN/n).toFixed(3)} salience=${(g.sal/n).toFixed(3)} `
          + `minEAC=${Math.round(h.minEAC)} maxEAC=${Math.round(h.maxEAC)}`);
      }
      const p = this._maxProbe;
      if (p && p.eac > -Infinity) console.error(`JSDIAG ${year} HYBRID-MAX eac=${Math.round(p.eac)} `
        + `phase=${p.phase} own=${p.own} label=${p.label} area=${p.area} demand=${p.demand} lowTemp=${p.lowTemp} insul=${p.insul}`);
      this._diag = null; this._maxProbe = null;
    }

    this._updateSalience();
    this._updateLearningCurve();

    // stock per ownership
    const stock = {}, stockOwn = {};
    for (const t of HEATING_SYSTEMS) stock[t] = 0;
    for (const own of ['PRIVATELY_OWNED','PRIVATELY_RENTED','SOCIAL_HOUSING','HOME_OWNER_ASSOCIATION'])
      stockOwn[own] = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    for (const d of this.all) { stock[d.currentType]++; stockOwn[d.ownership][d.currentType]++; }
    return { year, stock, stockOwn, yearInstalled: inst, yearRemoved: rem,
             cumInstalled: { ...this.cumInstalled }, considered };
  }

  run() {
    const rows = [];
    const stock0 = {}, stockOwn0 = {};
    for (const t of HEATING_SYSTEMS) stock0[t] = 0;
    for (const own of ['PRIVATELY_OWNED','PRIVATELY_RENTED','SOCIAL_HOUSING','HOME_OWNER_ASSOCIATION'])
      stockOwn0[own] = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    for (const d of this.all) { stock0[d.currentType]++; stockOwn0[d.ownership][d.currentType]++; }
    rows.push({ year: this.cfg.startYear, stock: stock0, stockOwn: stockOwn0,
      yearInstalled: Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0])),
      yearRemoved: Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0])),
      cumInstalled: { ...this.cumInstalled }, considered: 0 });
    for (let y = this.cfg.startYear + 1; y <= this.cfg.endYear; y++) rows.push(this.stepYear(y));
    return rows;
  }
}
