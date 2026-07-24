// dumpBlockDecisions.js — per-block, per-year, per-option decision trace.
//
// The block analogue of the sample-dwelling dump. Emits ONE ROW PER
// (block, year, heating-system option) at every year a block actually reaches a
// decision, so we can see not just WHAT a block chose but WHY the alternatives lost.
//
// Schema is identical to the AnyLogic exporter in AL_BLOCK_DECISION_TRACE.md, so the
// two CSVs drop straight into tests/compare_block_decisions.py.
//
//   node dumpBlockDecisions.js [--out trace.csv] [--scenario baseline] [--seed 42]
//
// The decisive columns are `avg_eac` (does the option cost what AL thinks it costs?)
// and `is_possible` / `n_impossible` (is the option even on the table?). AL ANDs
// isPossible across EVERY household in the block, so a single ineligible dwelling
// removes the option for the whole block — n_impossible tells you how many did that.

import fs from 'node:fs';
import { RNG } from './src/rng.js';
import { loadRealAll } from './data/loadRealStockAll.js';
import { FullSimulation } from './src/modelFull.js';
import { DEFAULT_SEED, HEATING_SYSTEMS } from './src/constants.js';

function arg(n, d) {
  const i = process.argv.indexOf('--' + n);
  if (i === -1) return d;
  const v = process.argv[i + 1];
  return v && !v.startsWith('--') ? v : true;
}

const out = arg('out', 'block_decision_trace.csv');
const rng = new RNG(+arg('seed', DEFAULT_SEED));
const agents = loadRealAll(arg('real', '../data-export/out/limburg_dwellings.csv'),
                           { rng, everyNth: +arg('every', 1) });

const blockTrace = [];
const sim = new FullSimulation({
  startYear: +arg('startYear', 2024), endYear: +arg('endYear', 2050), rng,
  scenario: { social_learning_factor: 'MEDIUM', economic_learning_factor: 'MEDIUM' },
  legacyTrigger: false, networkSize: 10, blockTrace,
}, agents);
sim.run();

const COLS = ['year', 'block_type', 'buurtcode', 'block_key', 'n_households',
  'current_hs', 'hs_age', 'lifetime', 'trigger', 'option',
  'avg_eac', 'is_possible', 'n_impossible', 'chosen'];

const lines = [COLS.join(',')];
for (const r of blockTrace) lines.push(COLS.map(c => r[c]).join(','));
fs.writeFileSync(out, lines.join('\n') + '\n');

const decisions = blockTrace.length / HEATING_SYSTEMS.length;
const blocks = new Set(blockTrace.map(r => r.block_key)).size;
console.log(`${decisions} block decisions across ${blocks} blocks -> ${out}`);
