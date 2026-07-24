// loadRealStock.js — build the REAL Limburg homeowner dwelling stock from the exported
// data-export/out/limburg_dwellings.csv. Faithful to J_Dwelling / f_createDwellings:
//   - ownership via f_getDwellingOwnership (validated: reproduces the golden 65/19/15 split)
//   - heat demand = f*space + f*f*dhwBase, f ~ truncated Normal(mean 1, sd 0.2) in [0.5,1.5]
//     (J_Dwelling.f_setAnnualEnergyDemandFromVestaMAIS; space & dhwBase precomputed in export)
// Homeowner core -> keep PRIVATELY_OWNED dwellings only.

import fs from 'node:fs';

function truncNormal(rng, mean, sd, lo, hi) {
  for (let i = 0; i < 100; i++) {
    // Box-Muller via two uniforms
    let u = 0, v = 0;
    while (u === 0) u = rng.next();
    while (v === 0) v = rng.next();
    const z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
    const x = mean + sd * z;
    if (x >= lo && x <= hi) return x;
  }
  return mean;
}

// faithful ownership assignment (f_getDwellingOwnership)
function ownership(rng, koop, huur, corp) {
  const hba = (huur - corp) / 100;
  const oll = (huur - hba) / 100;
  const r = rng.next();
  if (r < hba) return 'SOCIAL_HOUSING';
  if (r < hba + oll) return 'PRIVATELY_RENTED';
  return 'PRIVATELY_OWNED';
}

/** Load homeowner dwellings. opts: { rng, limit, everyNth } */
export function loadRealHomeowners(csvPath, opts = {}) {
  const { rng, limit = Infinity, everyNth = 1 } = opts;
  const text = fs.readFileSync(csvPath, 'utf8');
  const lines = text.split(/\r?\n/);
  const header = lines[0].split(',');
  const col = Object.fromEntries(header.map((h, i) => [h, i]));
  const out = [];
  let id = 0;
  for (let i = 1; i < lines.length && out.length < limit; i++) {
    const ln = lines[i];
    if (!ln) continue;
    if ((i - 1) % everyNth !== 0) continue;
    const f = ln.split(',');
    const koop = +f[col.perc_koop], huur = +f[col.perc_huur], corp = +f[col.aantal_corp];
    const own = ownership(rng, koop, huur, corp);
    if (own !== 'PRIVATELY_OWNED') continue;               // homeowner core
    const dtype = f[col.dwelling_type];
    if (dtype === undefined) continue;
    const area = +f[col.area_m2];
    const label = f[col.label] || 'n';
    const space = +f[col.space_heat_kwh], dhwBase = +f[col.dhw_base_kwh];
    const fac = truncNormal(rng, 1.0, 0.2, 0.5, 1.5);
    const heatDemandKWh = fac * space + fac * fac * dhwBase;
    const dwellingTypeCat = (dtype === 'APARTMENT' || dtype === 'HIGHRISE') ? 'APARTMENT' : 'HOUSE';
    out.push({
      id: id++,
      currentType: 'NATURAL_GAS_BOILER',
      age: rng.int(0, 12),
      heatDemandKWh,
      energyLabel: label,
      livingAreaM2: area,
      dwellingType: dwellingTypeCat,
      hasLowTemp: label === 'a' || label === 'b',
      hasDistrictHeatingGrid: false,
      attitude: rng.beta(5, 2, 0, 1),
      network: [], peerCounts: null,
      insulToB: +f[col.insul_to_b] || 0, insulToC: +f[col.insul_to_c] || 0,
    });
  }
  return out;
}
