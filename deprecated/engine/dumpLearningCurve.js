// dumpLearningCurve.js — per-year, per-technology learning-curve state.
//
// Mirrors the AnyLogic exporter in AL_LEARNING_CURVE_EXPORT.md so the two CSVs drop
// straight into tests/compare_learning_curve.py.
//
//   node dumpLearningCurve.js [--out learning_curve.csv]
//
// The decisive column is `capex_factor`. If AL holds at exactly 1.000 for the first
// several years while this engine is already below 1, the two models disagree about
// WHEN learning starts -- which is the leading explanation for the engine tipping
// early-and-shallow where AL tips late-and-steep.

import fs from 'node:fs';
import { RNG } from './src/rng.js';
import { loadRealAll } from './data/loadRealStockAll.js';
import { FullSimulation } from './src/modelFull.js';
import { DEFAULT_SEED, HEATING_SYSTEMS } from './src/constants.js';
import * as D from './src/decision.js';

const arg = (n, d) => {
  const i = process.argv.indexOf('--' + n);
  if (i === -1) return d;
  const v = process.argv[i + 1];
  return v && !v.startsWith('--') ? v : true;
};

const out = arg('out', 'learning_curve.csv');
const rng = new RNG(+arg('seed', DEFAULT_SEED));
const agents = loadRealAll(arg('real', '../data-export/out/limburg_dwellings.csv'),
                           { rng, everyNth: +arg('every', 1) });

const rows = [];
const sim = new FullSimulation({
  startYear: +arg('startYear', 2024), endYear: +arg('endYear', 2050), rng,
  scenario: { social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM' },
  legacyTrigger: false, networkSize: 10,
}, agents);

// snapshot after each year's capex update, matching AL's f_recordLearningCurve(yearIndex)
const origStep = sim.stepYear.bind(sim);
sim.stepYear = (year) => {
  const r = origStep(year);
  for (const t of HEATING_SYSTEMS) {
    const h = sim.hs[t];
    const cum = sim.cumInstalled[t];
    const factor = D.capexLearningFactor(cum, h._initialUnits, h.economicLearningRate, sim.elf);
    rows.push([year, t,
      sim.all.reduce((a, d) => a + (d.currentType === t ? 1 : 0), 0),  // installed_current
      cum,                                                             // installed_cumulative
      h._initialUnits,                                                 // initial_units
      factor.toFixed(6),
      h.investMedium.toFixed(2),                                       // capex_medium (already learning-adjusted)
    ].join(','));
  }
  return r;
};

sim.run();

fs.writeFileSync(out,
  'year,heating_system,installed_current,installed_cumulative,initial_units,capex_factor,capex_medium\n'
  + rows.join('\n') + '\n');
console.log(`${rows.length} learning-curve rows -> ${out}`);
