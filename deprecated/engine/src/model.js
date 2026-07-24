// model.js — the ABM: dwellings + homeowners, the annual adoption loop, and the
// two feedback loops (social salience, cost learning curve). Faithful to
// Main.f_adoptionProces / J_HomeOwner / J_HeatingSystemOptionsGlobal, with the
// corrected opportunity trigger (see decision.js).
//
// Scope note: this core covers the HOMEOWNER adoption dynamics that drive the
// tipping behaviour. Renters/landlords/social-housing/HOA blocks, energy/grid
// calculations and DH expansion are separate modules (TODO) and do not affect
// the homeowner S-curve mechanics reproduced here.

import { HEATING_SYSTEMS, LEARNING_MULTIPLIER, GUMBEL_SCALE_UTIL } from './constants.js';
import { HEATING_SYSTEM_DATA, MAX_SUSTAINABILITY_SCORE } from '../data/heating_systems.js';
import * as D from './decision.js';
import { computeEAC } from './economics.js';

// Placeholder insulation cost (real values live in DWELLINGS_DEMAND_INSULATION;
// export them for parity). Simple per-label-step estimate so the engine runs.
function insulationCost(dwelling, toLabel) {
  const step = { a: 7, b: 6, c: 5, d: 4, e: 3, f: 2, g: 1 };
  const gap = (step[toLabel] ?? 5) - (step[String(dwelling.energyLabel).toLowerCase()] ?? 5);
  return Math.max(0, gap) * 40 * dwelling.livingAreaM2; // ~EUR40/m2 per label step (placeholder)
}

export class Simulation {
  /**
   * @param {object} cfg  { startYear, endYear, seed, scenario, legacyTrigger }
   * @param {Array}  dwellings  homeowner dwellings (see data/synthetic.js)
   */
  constructor(cfg, dwellings) {
    this.cfg = cfg;
    this.dwellings = dwellings;
    this.rng = cfg.rng;
    this.slf = LEARNING_MULTIPLIER[cfg.scenario.social_learning_factor ?? 'MEDIUM'];
    this.elf = LEARNING_MULTIPLIER[cfg.scenario.economic_learning_factor ?? 'MEDIUM'];
    this.legacyTrigger = !!cfg.legacyTrigger;

    // per-technology mutable global state (capex mutated by learning curve; salience)
    this.hs = {};
    for (const t of HEATING_SYSTEMS) {
      const base = HEATING_SYSTEM_DATA[t];
      this.hs[t] = {
        ...base,
        sustainabilityScoreNorm: base.sustainabilityScore / MAX_SUSTAINABILITY_SCORE,
        salienceFactor: 0,
        _initialInvestSmall: base.investSmall,
        _initialInvestMedium: base.investMedium,
        _initialInvestLarge: base.investLarge,
        minEAC: Infinity, maxEAC: -Infinity,
      };
    }
    // cumulative installs by type
    this.cumInstalled = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    this.prevShare = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));

    // build the peer network (simple random k-regular) and peer counts
    this._buildNetwork(cfg.networkSize ?? 10);
    this._initInstalledBase();
  }

  _buildNetwork(k) {
    const n = this.dwellings.length;
    for (const d of this.dwellings) d.network = [];
    for (const d of this.dwellings) {
      for (let i = 0; i < k; i++) {
        const j = this.rng.int(0, n);
        if (this.dwellings[j] !== d) d.network.push(this.dwellings[j]);
      }
    }
    // peer counts per type (index by ordinal)
    for (const d of this.dwellings) {
      d.peerCounts = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
      for (const p of d.network) d.peerCounts[p.currentType]++;
    }
  }

  _initInstalledBase() {
    // initial cumulative installs = current stock (feeds salience/learning at t0)
    for (const d of this.dwellings) this.cumInstalled[d.currentType]++;
    for (const t of HEATING_SYSTEMS) {
      this.hs[t]._initialUnits = this.cumInstalled[t] || 1;
    }
    this._updateSalience(true);
  }

  _updateSalience() {
    const n = this.dwellings.length;
    for (const t of HEATING_SYSTEMS) {
      const cur = this.cumInstalled[t] / n;
      this.hs[t].salienceFactor = D.salienceFactor(cur, this.prevShare[t]);
    }
  }

  _updateLearningCurve() {
    for (const t of HEATING_SYSTEMS) {
      const h = this.hs[t];
      const f = D.capexLearningFactor(this.cumInstalled[t], h._initialUnits,
                                      h.economicLearningRate, this.elf);
      h.investSmall = h._initialInvestSmall * f;
      h.investMedium = h._initialInvestMedium * f;
      h.investLarge = h._initialInvestLarge * f;
    }
  }

  // compute EACs for one dwelling's options and update global min/max
  _computeEACs(d) {
    const out = {};
    for (const t of HEATING_SYSTEMS) {
      const eac = computeEAC(this.hs[t], d, insulationCost);
      out[t] = { eac, possible: this._possible(t, d) };
      if (eac < this.hs[t].minEAC) this.hs[t].minEAC = eac;
      if (eac > this.hs[t].maxEAC) this.hs[t].maxEAC = eac;
    }
    return out;
  }

  _possible(type, d) {
    // homeowner subset of J_Dwelling.f_getHeatingMethodPossibility
    if (type === 'DISTRICT_HEATING') return d.hasDistrictHeatingGrid === true;
    if (type === 'NATURAL_GAS_BLOCK') return d.dwellingType === 'APARTMENT';
    return true;
  }

  _resetMinMax() {
    for (const t of HEATING_SYSTEMS) { this.hs[t].minEAC = Infinity; this.hs[t].maxEAC = -Infinity; }
  }

  // one homeowner's utility-based stochastic choice among feasible options
  _chooseType(d, options) {
    // utility per option
    const util = {};
    for (const t of HEATING_SYSTEMS) {
      if (!options[t].possible) continue;
      const h = this.hs[t];
      const att = D.attitudeValue(d.attitude, h.sustainabilityScoreNorm);
      const eff = D.effort(t === d.currentType, h.requiresLowTemp, d.hasLowTemp);
      const eacNorm = h.maxEAC > h.minEAC ? D.normalizedValue(options[t].eac, h.minEAC, h.maxEAC) : 0;
      const pbcV = D.pbc(eacNorm, eff);
      const sn = D.subjectiveNorm(d.peerCounts[t], d.network.length || 1, h.salienceFactor);
      const intV = D.intention(att, sn, pbcV, this.slf, h.socialLearningRate);
      util[t] = D.perceivedUtility(intV, pbcV);
    }
    // RUM / Gumbel argmax
    let best = null, bestScore = -Infinity;
    for (const t of Object.keys(util)) {
      const score = D.rumScore(util[t], this.rng.gumbel());
      if (score > bestScore) { bestScore = score; best = t; }
    }
    return best;
  }

  _notifyPeers(d, oldType, newType) {
    for (const p of d.network) { p.peerCounts[oldType]--; p.peerCounts[newType]++; }
  }

  stepYear(year) {
    this._resetMinMax();
    // age heating systems
    for (const d of this.dwellings) d.age++;

    // record previous shares (for salience momentum)
    const n = this.dwellings.length;
    for (const t of HEATING_SYSTEMS) this.prevShare[t] = this.cumInstalled[t] / n;

    const yearInstalled = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    const yearRemoved = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    let considered = 0;

    // decide per triggered homeowner
    for (const d of this.dwellings) {
      const lifetime = this.hs[d.currentType].lifetime;
      const eol = D.hasEndOfLifeTrigger(d.age, lifetime);
      const opp = D.hasOpportunityTrigger(d.age, lifetime, this.legacyTrigger);
      if (!eol && !opp) continue;
      considered++;
      const options = this._computeEACs(d);
      const chosen = this._chooseType(d, options);
      if (!chosen) continue;
      if (eol || chosen !== d.currentType) {
        const oldType = d.currentType;
        this._notifyPeers(d, oldType, chosen);
        yearRemoved[oldType]++;
        yearInstalled[chosen]++;
        this.cumInstalled[chosen]++;   // cumulative installs (never decremented)
        d.currentType = chosen;
        d.age = 0;
      }
    }

    // feedback loops for next year
    this._updateSalience();
    this._updateLearningCurve();

    // current stock snapshot
    const stock = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    for (const d of this.dwellings) stock[d.currentType]++;

    return { year, stock, yearInstalled, yearRemoved,
             cumInstalled: { ...this.cumInstalled }, considered };
  }

  run() {
    const rows = [];
    // year 0 snapshot (no decisions)
    const stock0 = Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0]));
    for (const d of this.dwellings) stock0[d.currentType]++;
    rows.push({ year: this.cfg.startYear, stock: stock0,
                yearInstalled: Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0])),
                yearRemoved: Object.fromEntries(HEATING_SYSTEMS.map(t => [t, 0])),
                cumInstalled: { ...this.cumInstalled }, considered: 0 });
    for (let y = this.cfg.startYear + 1; y <= this.cfg.endYear; y++) {
      rows.push(this.stepYear(y));
    }
    return rows;
  }
}
