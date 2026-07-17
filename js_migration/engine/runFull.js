// runFull.js — multi-owner headless runner (social blocks + landlords + homeowners).
import fs from 'node:fs';
import { FullSimulation } from './src/modelFull.js';
import { loadRealAll } from './data/loadRealStockAll.js';
import { RNG } from './src/rng.js';
import { HEATING_SYSTEMS, DEFAULT_START_YEAR, DEFAULT_END_YEAR, DEFAULT_SEED } from './src/constants.js';

function arg(n, d) { const i = process.argv.indexOf('--' + n); if (i === -1) return d;
  const v = process.argv[i + 1]; return v && !v.startsWith('--') ? v : true; }

const SCEN = {
  baseline: { scen_id: 1, scen_name: 'baseline', social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM' },
  social_learning_factor_high: { scen_id: 3, scen_name: 'social_learning_factor_high', social_learning_factor: 'HIGH', economic_learning_factor: 'MEDIUM' },
  economic_learning_factor_high: { scen_id: 5, scen_name: 'economic_learning_factor_high', social_learning_factor: 'MEDIUM', economic_learning_factor: 'HIGH' },
};
const HEADER = ['scenario','scenario_name','iteration','year','nbh_in_grid_congestion_perc','nbh_with_dh_perc',
  'heating_system','ownership','installed_current','installed_annually','removed_annually','installed_cumulative',
  'considered_annually','avg_att','avg_util','avg_sub_norm','avg_eac','avg_pbc','SLF','ELF','GRR','DHCT','DHES','SHAES','DHCO','GCHPB'];

const csv = arg('real', '../data-export/out/limburg_dwellings.csv');
const everyNth = +arg('every', 1);
const iterations = +arg('iterations', 1);
const scName = arg('scenario', 'baseline');
const legacyTrigger = arg('legacy-trigger', false) === true;
const startYear = +arg('start', DEFAULT_START_YEAR), endYear = +arg('end', DEFAULT_END_YEAR);
let out = arg('out', null);
if (!out) { const d = new Date(), p = n => String(n).padStart(2, '0');
  out = `simulation_results_${d.getFullYear()}${p(d.getMonth()+1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}.csv`; }
const scen = SCEN[scName] ?? SCEN.baseline;

const t0 = Date.now();
const lines = [HEADER.join(',')];
let nAgents = 0;
for (let it = 1; it <= iterations; it++) {
  const rng = new RNG(DEFAULT_SEED + it - 1);
  const agents = loadRealAll(csv, { rng, everyNth });
  nAgents = agents.homeowners.length + agents.landlords.length + agents.socialBlocks.reduce((s, b) => s + b.households.length, 0);
  const sim = new FullSimulation({ startYear, endYear, rng, scenario: scen, legacyTrigger, networkSize: 10 }, agents);
  const rows = sim.run();
  for (const r of rows) {
    for (const hs of HEATING_SYSTEMS) {
      const perOwn = {
        PRIVATELY_OWNED: r.stockOwn.PRIVATELY_OWNED[hs],
        PRIVATELY_RENTED: r.stockOwn.PRIVATELY_RENTED[hs],
        SOCIAL_HOUSING: r.stockOwn.SOCIAL_HOUSING[hs],
        HOME_OWNER_ASSOCIATION: r.stockOwn.HOME_OWNER_ASSOCIATION[hs],
        TOTAL: r.stock[hs],
      };
      for (const own of Object.keys(perOwn)) {
        lines.push([scen.scen_id, scen.scen_name, it, r.year, 0, 0, hs, own,
          perOwn[own], own === 'TOTAL' ? r.yearInstalled[hs] : 0, own === 'TOTAL' ? r.yearRemoved[hs] : 0,
          r.cumInstalled[hs], r.considered, 0, 0, 0, 0, 0,
          scen.social_learning_factor, scen.economic_learning_factor, 'MEDIUM', 5, 'COST_BASED', 'COST_BASED', false, false].join(','));
      }
    }
  }
}
fs.writeFileSync(out, lines.join('\n') + '\n');
console.log(`Ran ${iterations} iters, ${nAgents} agents (multi-owner), ${startYear}-${endYear} in ${((Date.now()-t0)/1000).toFixed(1)}s -> ${out}`);
