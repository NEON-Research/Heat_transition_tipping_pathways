// loadRealStockAll.js — Node wrapper: read files, parse CSV, delegate to the shared builder.
import fs from 'node:fs';
import { buildAgents } from '../src/stockBuilder.js';

export function loadRealAll(csvPath, opts = {}) {
  const { rng, everyNth = 1 } = opts;
  const archetypeRows = JSON.parse(fs.readFileSync(new URL('./dwellings_demand_insulation.json', import.meta.url), 'utf8'));
  const nbhData = JSON.parse(fs.readFileSync(new URL('./nbh_heating.json', import.meta.url), 'utf8'));
  const lines = fs.readFileSync(csvPath, 'utf8').split(/\r?\n/);
  const header = lines[0].replace('\r', '').split(',');
  const col = Object.fromEntries(header.map((h, i) => [h, i]));
  // map CSV columns -> builder field names
  const F = { buurt: col.buurtcode, archetype: col.dwelling_type, label: col.label, area: col.area_m2,
    year: col.construction_year, space: col.space_heat_kwh, dhw: col.dhw_base_kwh,
    koop: col.perc_koop, huur: col.perc_huur, corp: col.aantal_corp };
  const rows = [];
  for (let i = 1; i < lines.length; i++) { if (lines[i]) rows.push(lines[i].split(',')); }
  return buildAgents(rows, F, { rng, archetypeRows, nbhData, everyNth });
}
