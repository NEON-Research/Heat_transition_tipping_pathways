// modelFull.js — multi-owner ABM: social-housing blocks + landlords + homeowners, all
// feeding the SHARED cumulative-installs that drive the learning curve + salience.
// Faithful to Main.f_adoptionProces order:
//   age -> SHA blocks (whole block, EAC) -> landlords (individual, EAC)
//   -> homeowners (TPB utility) -> update salience + learning curve.
// Blocks/landlords decide on cost (getStochasticChoice on EAC + gumbelScaleEAC*gumbel).
import { HEATING_SYSTEMS, LEARNING_MULTIPLIER, GUMBEL_SCALE_EAC } from './constants.js';
import { HEATING_SYSTEM_DATA, MAX_SUSTAINABILITY_SCORE } from '../data/heating_systems.js';
import * as D from './decision.js';
import { computeEAC } from './economics.js';
import { archRow, insulationCost as vestaInsul, labelNum, numLabel } from './vesta.js';


export class FullSimulation {
  constructor(cfg, agents) {
    this.cfg = cfg; this.rng = cfg.rng; this.archIndex = agents.archIndex;
    this.homeowners = agents.homeowners;
    this.landlords = agents.landlords;
    this.blocks = [...agents.socialBlocks, ...(agents.hoaBlocks || [])];
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
  _possible(t, d) {
    if (t === 'DISTRICT_HEATING') return d.hasDistrictHeatingGrid === true;
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
    this._resetMinMax();
    for (const d of this.all) d.age++;
    for (const b of this.blocks) b.age++;
    const n = this.all.length;
    for (const t of HEATING_SYSTEMS) this.prevShare[t] = this.cumInstalled[t] / n;

    // exogenous insulation (J_Dwelling.updateExogeneousInsulation): non-block households + blocks
    this._exogenousInsulation(this.homeowners, year);
    this._exogenousInsulation(this.landlords, year);
    for (const b of this.blocks) {                       // J_HousingBlock.updateExogeneousInsulation (block-level)
      const before = b.energyLabel; this._exogInsulOne(b, year);
      if (b.energyLabel !== before) for (const d of b.households) {
        d.energyLabel = b.energyLabel; d.hasLowTemp = b.hasLowTemp; d.yearLastRenovation = b.yearLastRenovation; }
    }

    const inst = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    const rem = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    let considered = 0;

    // 1. Social-housing blocks (end-of-life; whole block switches on avg EAC)
    for (const b of this.blocks) {
      const lifetime = this.hs[b.currentType].lifetime;
      if (b.age < lifetime) continue;
      // average EAC across households
      const avg = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
      const poss = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, true]));
      for (const d of b.households) {
        const e = this._eac(d);
        for (const t of HEATING_SYSTEMS) { avg[t] += e[t].eac; if (!e[t].possible) poss[t] = false; }
      }
      const eacByType = Object.fromEntries(HEATING_SYSTEMS.map(t =>
        [t, { eac: avg[t] / b.households.length, possible: poss[t] }]));
      considered += b.households.length;
      const chosen = this._chooseByEAC(eacByType);
      if (chosen) {
        for (const d of b.households) {
          if (d.currentType !== chosen) { rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++; }
          else { rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++; } // end-of-life always reinstalls
          d.currentType = chosen; d.age = 0;
        }
        b.currentType = chosen; b.age = 0;
      }
    }

    // 2. Landlords (individual, end-of-life, cost)
    for (const d of this.landlords) {
      const lifetime = this.hs[d.currentType].lifetime;
      if (!D.hasEndOfLifeTrigger(d.age, lifetime)) continue;
      considered++;
      const chosen = this._chooseByEAC(this._eac(d));
      if (chosen) { rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++; d.currentType = chosen; d.age = 0; }
    }

    // 3. Homeowners (TPB utility; end-of-life or opportunity)
    for (const d of this.homeowners) {
      const lifetime = this.hs[d.currentType].lifetime;
      const eol = D.hasEndOfLifeTrigger(d.age, lifetime);
      const opp = D.hasOpportunityTrigger(d.age, lifetime, this.legacyTrigger);
      if (!eol && !opp) continue;
      considered++;
      const chosen = this._chooseByUtility(d, this._eac(d));
      if (!chosen) continue;
      if (eol || chosen !== d.currentType) {
        this._notifyPeers(d, d.currentType, chosen);
        rem[d.currentType]++; inst[chosen]++; this.cumInstalled[chosen]++;
        d.currentType = chosen; d.age = 0;
      }
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
