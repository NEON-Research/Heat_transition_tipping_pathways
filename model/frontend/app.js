// app.js — runs the multi-agent heat-transition engine live in the browser and charts it.
// Imports the SAME engine modules the Java/Node engines mirror (browser-safe: no node deps).
import { FullSimulation } from '../../deprecated/engine/src/modelFull.js';
import { RNG } from '../../deprecated/engine/src/rng.js';
import { HEATING_SYSTEMS } from '../../deprecated/engine/src/constants.js';
import { buildAgents as buildAgentsShared } from '../../deprecated/engine/src/stockBuilder.js';

const COLORS = {
  NATURAL_GAS_BOILER: '#7a7a7a', NATURAL_GAS_BLOCK: '#b0b0b0',
  HYBRID_HEAT_PUMP: '#f4a259', ELECTRIC_HEAT_PUMP: '#2a9d8f', DISTRICT_HEATING: '#e76f51',
};
const LABELS = {
  NATURAL_GAS_BOILER: 'Gas boiler', NATURAL_GAS_BLOCK: 'Gas block',
  HYBRID_HEAT_PUMP: 'Hybrid HP', ELECTRIC_HEAT_PUMP: 'Electric HP', DISTRICT_HEATING: 'District heat',
};

let sample = null, archetypeRows = null, nbhData = null;
const $ = id => document.getElementById(id);

async function loadSample() {
  if (sample) return sample;
  [sample, archetypeRows, nbhData] = await Promise.all([
    fetch('./limburg_sample.json').then(r => r.json()),
    fetch('./dwellings_demand_insulation.json').then(r => r.json()),
    fetch('./nbh_heating.json').then(r => r.json()),
  ]);
  return sample;
}

// build agents via the shared faithful builder (ownership + per-neighbourhood initial mix + HOA)
function buildAgents(data, rng) {
  const F = Object.fromEntries(data.fields.map((f, i) => [f, i]));
  return buildAgentsShared(data.rows, F, { rng, archetypeRows, nbhData });
}

let charts = {};
function drawStacked(rows) {
  const years = rows.map(r => r.year);
  const totals = rows.map(r => Object.values(r.stock).reduce((a, b) => a + b, 0));
  const datasets = HEATING_SYSTEMS.map(hs => ({
    label: LABELS[hs], backgroundColor: COLORS[hs], borderColor: COLORS[hs], fill: true,
    data: rows.map((r, i) => 100 * r.stock[hs] / totals[i]), pointRadius: 0, tension: 0.2,
  }));
  charts.stacked?.destroy();
  charts.stacked = new Chart($('chartStacked'), {
    type: 'line',
    data: { labels: years, datasets },
    options: { responsive: true, animation: false,
      scales: { y: { stacked: true, min: 0, max: 100, title: { display: true, text: '% of dwellings' } },
                x: { title: { display: true, text: 'Year' } } },
      plugins: { title: { display: true, text: 'Heating-system mix over time (all owners)' }, legend: { position: 'bottom' } } },
  });
}
function drawOwnership(rows) {
  const last = rows[rows.length - 1];
  const owns = ['PRIVATELY_OWNED', 'PRIVATELY_RENTED', 'SOCIAL_HOUSING'];
  const datasets = HEATING_SYSTEMS.map(hs => ({ label: LABELS[hs], backgroundColor: COLORS[hs],
    data: owns.map(o => { const t = Object.values(last.stockOwn[o]).reduce((a, b) => a + b, 0) || 1;
      return 100 * last.stockOwn[o][hs] / t; }) }));
  charts.own?.destroy();
  charts.own = new Chart($('chartOwn'), {
    type: 'bar', data: { labels: owns.map(o => o.replace('PRIVATELY_', '').replace('_', ' ')), datasets },
    options: { responsive: true, animation: false,
      scales: { x: { stacked: true }, y: { stacked: true, min: 0, max: 100, title: { display: true, text: `% (final year ${last.year})` } } },
      plugins: { title: { display: true, text: 'Final-year mix by owner type' }, legend: { position: 'bottom' } } },
  });
}

function run() {
  const btn = $('run'); btn.disabled = true; $('status').textContent = 'running…';
  setTimeout(() => {
    const scenario = { social_learning_factor: $('slf').value, economic_learning_factor: $('elf').value };
    const seed = +$('seed').value || 1;
    const rng = new RNG(seed);
    const agents = buildAgents(sample, rng);
    const t0 = performance.now();
    const sim = new FullSimulation({ startYear: 2024, endYear: 2050, rng, scenario,
      legacyTrigger: false, networkSize: 10 }, agents);   // trigger fixed at 75% of life
    const rows = sim.run();
    const dt = ((performance.now() - t0) / 1000).toFixed(2);
    drawStacked(rows); drawOwnership(rows);
    const last = rows[rows.length - 1], tot = Object.values(last.stock).reduce((a, b) => a + b, 0);
    const hp = 100 * (last.stock.ELECTRIC_HEAT_PUMP + last.stock.HYBRID_HEAT_PUMP) / tot;
    const nAgents = agents.homeowners.length + agents.landlords.length +
      agents.socialBlocks.reduce((s, b) => s + b.households.length, 0);
    $('status').textContent = `${nAgents.toLocaleString()} dwellings · ${agents.socialBlocks.length} social blocks · ${dt}s · ${hp.toFixed(0)}% heat pumps by 2050`;
    btn.disabled = false;
  }, 30);
}

window.addEventListener('DOMContentLoaded', async () => {
  $('status').textContent = 'loading data…';
  await loadSample();
  $('status').textContent = `ready — ${sample.rows.length.toLocaleString()} sample dwellings loaded`;
  $('run').addEventListener('click', run);
  run();
});
