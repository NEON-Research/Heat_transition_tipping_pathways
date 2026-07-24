// dumpBlocks.js — per-block state at YEAR 0, to compare 1:1 against AnyLogic's block dump.
// Keyed on buurtcode (one social block + one HOA block per neighbourhood).
// Usage: node dumpBlocks.js [--real path] [--every N] [--out file.csv]
import fs from 'node:fs';
import { RNG } from './src/rng.js';
import { loadRealAll } from './data/loadRealStockAll.js';
import { FullSimulation } from './src/modelFull.js';
import { computeEAC } from './src/economics.js';
import { archRow, insulationCost as vestaInsul } from './src/vesta.js';
import { HEATING_SYSTEMS, DEFAULT_SEED } from './src/constants.js';

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); if (i === -1) return d;
  const v = process.argv[i + 1]; return v && !v.startsWith('--') ? v : true; };
const csv = arg('real', '../data-export/out/limburg_dwellings.csv');
const rng = new RNG(+arg('seed', DEFAULT_SEED));
const ag = loadRealAll(csv, { rng, everyNth: +arg('every', 1) });
const sim = new FullSimulation({ startYear: 2024, endYear: 2050, rng,
  scenario: { social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM' },
  legacyTrigger: false, networkSize: 10 }, ag);

const insul = (d, l) => vestaInsul(archRow(sim.archIndex, d.archetype, d.constructionYear), d.energyLabel, l, d.livingAreaM2);
const possible = (t, d) => t === 'DISTRICT_HEATING' ? d.hasDistrictHeatingGrid === true
  : t === 'NATURAL_GAS_BLOCK' ? d.archetype === 'APARTMENT' : true;

function blockRow(kind, b) {
  // block-average EAC per option + possibility (AL: setAverageHeatingSystemOptionsEAC)
  const sum = {}, poss = {};
  for (const t of HEATING_SYSTEMS) { sum[t] = 0; poss[t] = true; }
  let demand = 0;
  for (const d of b.households) {
    demand += d.heatDemandKWh;
    for (const t of HEATING_SYSTEMS) {
      sum[t] += computeEAC(sim.hs[t], d, insul);
      if (!possible(t, d)) poss[t] = false;
    }
  }
  const n = b.households.length || 1;
  const cells = [kind, b.buurt, b.households.length, b.currentType, b.age,
    b.energyLabel, b.yearLastRenovation, !!b.hasLowTemp, Math.round(demand / n)];
  for (const t of HEATING_SYSTEMS) cells.push(Math.round(sum[t] / n), poss[t]);
  return cells.join(',');
}

const head = ['block_type','buurtcode','n_households','heating_system','hs_age','energy_label',
  'year_last_renovation','has_low_temp','avg_heatDemand_kWh'];
for (const t of HEATING_SYSTEMS) head.push('EAC_' + t, 'possible_' + t);
const lines = [head.join(',')];
for (const b of ag.socialBlocks) lines.push(blockRow('SOCIAL', b));
for (const b of ag.hoaBlocks) lines.push(blockRow('HOA', b));
const out = arg('out', 'block_dump.csv');
fs.writeFileSync(out, lines.join('\n') + '\n');
console.log(`dumped ${ag.socialBlocks.length} social + ${ag.hoaBlocks.length} HOA blocks -> ${out}`);
