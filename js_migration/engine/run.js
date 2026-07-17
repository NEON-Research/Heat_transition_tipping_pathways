// run.js — headless CLI runner. Runs one or more scenarios x iterations and writes
// a simulation_results CSV (same schema as the AnyLogic export).
//
// Usage:
//   node run.js --dwellings 2000 --iterations 5 --out out.csv
//   node run.js --scenario baseline --start 2024 --end 2050 --seed 1
//   node run.js --legacy-trigger        # reproduce the original int-division bug
//
// Scenarios default to the model's _scenario_settings.csv rows if --scenarios <file>
// is given (JSON array); otherwise a built-in baseline is used.

import fs from 'node:fs';
import { Simulation } from './src/model.js';
import { rowsToCsv } from './src/results.js';
import { makeSyntheticDwellings } from './data/synthetic.js';
import { loadRealHomeowners } from './data/loadRealStock.js';
import { RNG } from './src/rng.js';
import { DEFAULT_START_YEAR, DEFAULT_END_YEAR, DEFAULT_SEED } from './src/constants.js';

function arg(name, def) {
  const i = process.argv.indexOf('--' + name);
  if (i === -1) return def;
  const v = process.argv[i + 1];
  return v && !v.startsWith('--') ? v : true;
}

const BUILTIN_SCENARIOS = {
  baseline: { scen_id: 1, scen_name: 'baseline', social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM' },
  social_learning_factor_high: { scen_id: 3, scen_name: 'social_learning_factor_high', social_learning_factor: 'HIGH', economic_learning_factor: 'MEDIUM' },
  social_learning_factor_low: { scen_id: 2, scen_name: 'social_learning_factor_low', social_learning_factor: 'LOW', economic_learning_factor: 'MEDIUM' },
  economic_learning_factor_high: { scen_id: 5, scen_name: 'economic_learning_factor_high', social_learning_factor: 'MEDIUM', economic_learning_factor: 'HIGH' },
};

const nDwellings = +arg('dwellings', 2000);
const iterations = +arg('iterations', 3);
const startYear = +arg('start', DEFAULT_START_YEAR);
const endYear = +arg('end', DEFAULT_END_YEAR);
const baseSeed = +arg('seed', DEFAULT_SEED);
const legacyTrigger = arg('legacy-trigger', false) === true;
let out = arg('out', null);
if (!out) {
  const ts = new Date().toISOString().slice(0,19).replace(/[-:T]/g, m => m === 'T' ? '_' : '').replace('_','_');
  const pad = n => String(n).padStart(2,'0');
  const d = new Date();
  out = `simulation_results_${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}.csv`;
}
const realCsv = arg('real', null);
const realEveryNth = +arg('every', 1);

let scenarios;
const scFile = arg('scenarios', null);
const scName = arg('scenario', null);
if (scFile) scenarios = JSON.parse(fs.readFileSync(scFile, 'utf8'));
else if (scName) scenarios = [BUILTIN_SCENARIOS[scName] ?? BUILTIN_SCENARIOS.baseline];
else scenarios = Object.values(BUILTIN_SCENARIOS);

const t0 = Date.now();
let csvParts = [];
let first = true;
let runCount = 0;
let lastN = nDwellings;

for (const scen of scenarios) {
  for (let it = 1; it <= iterations; it++) {
    const seed = baseSeed + it - 1;
    const rng = new RNG(seed);
    // regenerate the SAME synthetic stock per iteration seed for reproducibility
    const dwellings = realCsv
      ? loadRealHomeowners(realCsv, { rng: new RNG(seed), everyNth: realEveryNth })
      : makeSyntheticDwellings(nDwellings, 1000 + it);
    const sim = new Simulation(
      { startYear, endYear, seed, scenario: scen, rng, legacyTrigger, networkSize: 10 },
      dwellings);
    const rows = sim.run();
    lastN = dwellings.length;
    const csv = rowsToCsv(rows, { scenario: scen, scenarioId: scen.scen_id, iteration: it });
    csvParts.push(first ? csv : csv.split('\n').slice(1).join('\n'));
    first = false;
    runCount++;
  }
}

fs.writeFileSync(out, csvParts.join(''));
const dt = ((Date.now() - t0) / 1000).toFixed(2);
console.log(`Ran ${runCount} runs (${scenarios.length} scenarios x ${iterations} iters, `
  + `${realCsv ? lastN+' real' : nDwellings} dwellings, ${startYear}-${endYear}${legacyTrigger ? ', LEGACY trigger' : ''}) `
  + `in ${dt}s -> ${out}`);
