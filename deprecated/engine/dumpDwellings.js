// dumpDwellings.js — dump per-dwelling state at YEAR 0 (no learning applied) so it can be
// compared 1:1 against an equivalent AnyLogic dump, keyed on numid (stable households.db id).
// Usage: node dumpDwellings.js [--real path] [--every N] [--limit N] [--out file.csv]
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
const everyNth = +arg('every', 200);
const limit = +arg('limit', 500);
const out = arg('out', 'dwelling_dump.csv');
const buurt = arg('buurt', null);   // match AL's dump neighbourhood

const rng = new RNG(+arg('seed', DEFAULT_SEED));
const ag = loadRealAll(csv, { rng, everyNth });
// build the sim only to get the initial (unlearned) heating-system specs + salience
const sim = new FullSimulation({ startYear: 2024, endYear: 2050, rng,
  scenario: { social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM' },
  legacyTrigger: false, networkSize: 10 }, ag);

const all = [...ag.homeowners, ...ag.landlords,
  ...ag.socialBlocks.flatMap(b => b.households), ...ag.hoaBlocks.flatMap(b => b.households)];
const insul = (d, l) => vestaInsul(archRow(sim.archIndex, d.archetype, d.constructionYear), d.energyLabel, l, d.livingAreaM2);

const head = ['numid','ownership','archetype','label','construction_year','area_m2','hs_age',
  'heatDemand_kWh','hasLowTemp','initial_hs', ...HEATING_SYSTEMS.map(t => 'EAC_' + t)];
const lines = [head.join(',')];
const sel = buurt ? all.filter(d => d.buurt === buurt) : all.slice(0, limit);
for (const d of sel) {
  const eacs = HEATING_SYSTEMS.map(t => computeEAC(sim.hs[t], d, insul));
  lines.push([d.numid, d.ownership, d.archetype, d.energyLabel, d.constructionYear,
    d.livingAreaM2, d.age, Math.round(d.heatDemandKWh), d.hasLowTemp, d.currentType, ...eacs].join(','));
}
fs.writeFileSync(out, lines.join('\n') + '\n');
console.log(`dumped ${sel.length} dwellings${buurt ? ' in ' + buurt : ''} -> ${out}`);
console.log('columns:', head.join(' '));
