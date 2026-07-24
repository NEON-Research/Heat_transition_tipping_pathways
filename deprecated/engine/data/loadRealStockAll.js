// loadRealStockAll.js — Node wrapper: read files, parse CSV, delegate to the shared builder.
import fs from 'node:fs';
import { buildAgents } from '../src/stockBuilder.js';

export function loadRealAll(csvPath, opts = {}) {
  const { rng, everyNth = 1 } = opts;
  const archetypeRows = JSON.parse(fs.readFileSync(new URL('./dwellings_demand_insulation.json', import.meta.url), 'utf8'));
  const nbhData = JSON.parse(fs.readFileSync(new URL('./nbh_heating.json', import.meta.url), 'utf8'));

  // Merge the neighbourhood dataset (surface area, policy plan, EV share) onto nbhData.
  // Produced by data-export/scripts/export_neighborhoods.py. Optional so older setups
  // still run -- without it DH expansion simply has no surface area and stays static.
  try {
    const nl = fs.readFileSync(new URL('./neighborhoods.csv', import.meta.url), 'utf8').split(/\r?\n/);
    const nh = nl[0].replace('\r', '').split(',');
    const ni = Object.fromEntries(nh.map((h, i) => [h, i]));
    for (let i = 1; i < nl.length; i++) {
      if (!nl[i]) continue;
      const f = nl[i].split(',');
      const code = f[ni.buurtcode];
      const t = (nbhData[code] ||= {});
      t.surfaceAreaLand = +f[ni.a_lan_ha] || 0;
      t.households = +f[ni.a_hh] || 0;
      t.avgElectricityKWh = +f[ni.g_ele] || 0;   // NOTE: -99999 for every nbh -> 0 -> no baseload
      t.cars = +f[ni.a_pau] || 0;
      t.postalCode = +f[ni.pst_mvp] || 0;
      t.bevShare = +f[ni.bev_share] || 0;
      t.policyPlan = f[ni.policy_plan];
      t.policyStartJaar = +f[ni.policy_start_jaar] || 0;
      t.policyEindJaar = +f[ni.policy_eind_jaar] || 0;
      t.policyInfraW = f[ni.policy_infra_w] === '1';
    }
  } catch (e) {
    if (e.code !== 'ENOENT') throw e;
  }
  const lines = fs.readFileSync(csvPath, 'utf8').split(/\r?\n/);
  const header = lines[0].replace('\r', '').split(',');
  const col = Object.fromEntries(header.map((h, i) => [h, i]));
  // map CSV columns -> builder field names
  const F = { numid: col.numid, buurt: col.buurtcode, archetype: col.dwelling_type, label: col.label, area: col.area_m2,
    year: col.construction_year, space: col.space_heat_kwh, dhw: col.dhw_base_kwh,
    koop: col.perc_koop, huur: col.perc_huur, corp: col.aantal_corp };
  const rows = [];
  for (let i = 1; i < lines.length; i++) { if (lines[i]) rows.push(lines[i].split(',')); }
  return buildAgents(rows, F, { rng, archetypeRows, nbhData, everyNth });
}
