// runFull.js — multi-owner headless runner (social blocks + landlords + homeowners).
import fs from 'node:fs';
import { FullSimulation } from './src/modelFull.js';
import { loadRealAll } from './data/loadRealStockAll.js';
import { RNG } from './src/rng.js';
import { HEATING_SYSTEMS, DEFAULT_START_YEAR, DEFAULT_END_YEAR, DEFAULT_SEED } from './src/constants.js';

function arg(n, d) { const i = process.argv.indexOf('--' + n); if (i === -1) return d;
  const v = process.argv[i + 1]; return v && !v.startsWith('--') ? v : true; }

// Scenario matrix, transcribed from the AnyLogic results columns (SLF, ELF, GRR, DHCT,
// DHES, SHAES, DHCO, GCHPB) -- i.e. from what the model actually ran, not from the .alp.
// GRR (MEDIUM) and DHCT (5) never vary, so they are omitted.
//   DHES  = district-heating expansion strategy   -> requires neighbourhood policy plans
//   SHAES = social-housing strategy               -> requires neighbourhood policy plans
//   DHCO  = DH connection obligation              -> implemented (needs only hasDHgrid)
//   GCHPB = grid-congestion electric-HP ban       -> requires DSO congestion state
const SCEN = {
  baseline:                                 { scen_id: 1,  social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM' },
  social_learning_factor_low:               { scen_id: 2,  social_learning_factor: 'LOW',    economic_learning_factor: 'MEDIUM' },
  social_learning_factor_high:              { scen_id: 3,  social_learning_factor: 'HIGH',   economic_learning_factor: 'MEDIUM' },
  economic_learning_factor_low:             { scen_id: 4,  social_learning_factor: 'MEDIUM', economic_learning_factor: 'LOW' },
  economic_learning_factor_high:            { scen_id: 5,  social_learning_factor: 'MEDIUM', economic_learning_factor: 'HIGH' },
  policy_driven_dh_strategy:                { scen_id: 6,  social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM', DH_strategy: 'POLICY_BASED' },
  policy_driven_sha_strategy:               { scen_id: 7,  social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM', SHA_strategy: 'POLICY_BASED' },
  actor_allignment_strategy:                { scen_id: 8,  social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM', DH_strategy: 'POLICY_BASED', SHA_strategy: 'POLICY_BASED' },
  grid_congestion_HP_ban:                   { scen_id: 9,  social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM', congestion_block: true },
  dh_policy_based_connection_obligation:    { scen_id: 10, social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM', DH_strategy: 'POLICY_BASED', DH_connection_obligation: true },
  individual_technologies:                  { scen_id: 12, social_learning_factor: 'HIGH',   economic_learning_factor: 'HIGH' },
  collective_technologies:                  { scen_id: 13, social_learning_factor: 'LOW',    economic_learning_factor: 'LOW',  DH_strategy: 'POLICY_BASED', SHA_strategy: 'POLICY_BASED' },
  individual_tech_grid_congestion_ban:      { scen_id: 14, social_learning_factor: 'HIGH',   economic_learning_factor: 'HIGH', congestion_block: true },
  collective_tech_grid_congestion_ban:      { scen_id: 15, social_learning_factor: 'LOW',    economic_learning_factor: 'LOW',  DH_strategy: 'POLICY_BASED', SHA_strategy: 'POLICY_BASED', congestion_block: true },
  individual_tech_dh_connection_obligation: { scen_id: 16, social_learning_factor: 'HIGH',   economic_learning_factor: 'HIGH', DH_connection_obligation: true },
  collective_tech_dh_connection_obligation: { scen_id: 17, social_learning_factor: 'LOW',    economic_learning_factor: 'LOW',  DH_strategy: 'POLICY_BASED', SHA_strategy: 'POLICY_BASED', DH_connection_obligation: true },
};
for (const [k, v] of Object.entries(SCEN)) v.scen_name = k;
export const SCENARIOS = SCEN;

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
  const rng = new RNG(+arg('seed', DEFAULT_SEED) + it - 1);   // --seed shifts the whole MC block
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
