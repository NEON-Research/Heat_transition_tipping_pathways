// browser-safe: `process` is undefined in the frontend, so guard with typeof
export const LEGACY_LABEL_INT_DIVISION =
  typeof process !== 'undefined' && process.env && process.env.LEGACY_LABEL === '1';
// stockBuilder.js — browser-safe agent builder shared by the Node loader and the frontend.
// Faithful to f_getDwellingOwnership + GZ_Neighborhood.f_setHouseholdDefaultHeatingSystem
// (+ f_addHouseholdToHOABlock). Takes already-parsed rows + a field->index map + lookups.
import { buildArchetypeIndex, labelNum, numLabel } from './vesta.js';

// initial heating-system age (J_Dwelling / J_HousingBlock.f_setInitialHeatingMethod):
// age = startYear - yearLastRenovation; if >= lifetime, random(0, lifetime). yearLastRenovation = constructionYear.
const REQUIRES_LOWTEMP = { NATURAL_GAS_BOILER: false, NATURAL_GAS_BLOCK: false, HYBRID_HEAT_PUMP: true, ELECTRIC_HEAT_PUMP: true, DISTRICT_HEATING: false };
const LIFETIME = { NATURAL_GAS_BOILER: 12, NATURAL_GAS_BLOCK: 12, HYBRID_HEAT_PUMP: 15, ELECTRIC_HEAT_PUMP: 15, DISTRICT_HEATING: 30 };
function blockYearLastRenovation(rng, hh) {
  const avgCy = Math.round(hh.reduce((a, d) => a + d.constructionYear, 0) / hh.length);
  return avgCy < 1990 ? 1990 + rng.int(0, 35) : avgCy;   // J_HousingBlock.setDefaultYearLastRenovation
}
// Stable cross-engine block identity: the smallest BAG numid among its members.
// Deterministic and independent of iteration order, so AL and the JS/Java engines
// agree on it whenever they agree on block membership.
function blockKeyOf(hh) {
  let k = null;
  for (const d of hh) { const n = String(d.numid || ''); if (n && (k === null || n < k)) k = n; }
  return k === null ? 'NA' : k;
}
function blockAvgLabel(hh, legacyIntDivision = LEGACY_LABEL_INT_DIVISION) {                               // J_HousingBlock.setDefaultEnergyLabel
  // AL J_SocialBlock.setDefaultEnergyLabel:
  //   int avgLabelNb = roundToInt(sumLabelNumber / households.size());
  // sumLabelNumber and size() are BOTH int -> integer division (truncation) happens
  // BEFORE roundToInt, which is then a no-op. Labels run a=1..g=7, so truncation
  // systematically awards a BETTER label than the true mean.
  // legacyIntDivision=true  -> bit-faithful to the AnyLogic model as it stands
  // legacyIntDivision=false -> corrected true rounding
  const sum = hh.reduce((a, d) => a + labelNum(d.energyLabel), 0);
  return numLabel(legacyIntDivision ? Math.floor(sum / hh.length)
                                    : Math.round(sum / hh.length));
}
function initialHsAge(rng, startYear, constructionYear, type) {
  let a = startYear - constructionYear;
  const lifetime = LIFETIME[type] || 12;
  if (a >= lifetime) a = rng.int(0, lifetime);
  return Math.max(0, a);
}

function truncNormal(rng, mean, sd, lo, hi) {
  for (let i = 0; i < 100; i++) { let u = 0, v = 0;
    while (u === 0) u = rng.next(); while (v === 0) v = rng.next();
    const z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); const x = mean + sd * z;
    if (x >= lo && x <= hi) return x; } return mean;
}
function ownership(rng, koop, huur, corp) {
  const hba = (huur - corp) / 100, oll = (huur - hba) / 100; const r = rng.next();
  if (r < hba) return 'SOCIAL_HOUSING'; if (r < hba + oll) return 'PRIVATELY_RENTED'; return 'PRIVATELY_OWNED';
}
function shuffle(rng, a) { for (let i = a.length - 1; i > 0; i--) { const j = rng.int(0, i + 1); [a[i], a[j]] = [a[j], a[i]]; } }
const roundToInt = x => Math.round(x);

function assignInitialHeating(rng, nbh, perc) {
  const nonSHB = [...nbh.homeowners, ...nbh.landlords];
  const households = nbh.homeowners.length + nbh.landlords.length + nbh.socialHH.length;
  if (households === 0) return;
  let reqNGB = roundToInt(perc.gasCV * households), reqNGBl = roundToInt(perc.gasBlock * households);
  let reqEHP = roundToInt(perc.ehp * households), reqHHP = roundToInt(perc.hhp * households), reqDH = roundToInt(perc.dh * households);
  let total = reqNGB + reqNGBl + reqHHP + reqEHP + reqDH;
  while (total > households) { if (reqNGB > 0) reqNGB--; else if (reqNGBl > 0) reqNGBl--; else if (reqDH > 0) reqDH--; else if (reqHHP > 0) reqHHP--; else if (reqEHP > 0) reqEHP--; total--; }
  while (total < households) { reqNGB++; total++; }
  let setNGB = 0, setNGBl = 0, setEHP = 0, setHHP = 0, setDH = 0;
  const socialSize = nbh.socialHH.length;
  if (socialSize > 0) {
    let type;
    if (setDH + socialSize <= reqDH) { type = 'DISTRICT_HEATING'; setDH += socialSize; }
    else if (setNGBl + socialSize <= reqNGBl) { type = 'NATURAL_GAS_BLOCK'; setNGBl += socialSize; }
    else { const r = rng.next();
      if (r < 0.1 && setEHP + socialSize <= reqEHP) { type = 'ELECTRIC_HEAT_PUMP'; setEHP += socialSize; }
      else if (r < 0.25 && setHHP + socialSize <= reqHHP) { type = 'HYBRID_HEAT_PUMP'; setHHP += socialSize; }
      else { type = 'NATURAL_GAS_BOILER'; setNGB += socialSize; } }
    for (const d of nbh.socialHH) d.currentType = type;
    const _ylrS = blockYearLastRenovation(rng, nbh.socialHH);
    nbh.socialBlock = { buurt: nbh.buurt, blockType: 'SOCIAL', blockKey: blockKeyOf(nbh.socialHH), households: nbh.socialHH, currentType: type,
      age: initialHsAge(rng, 2024, _ylrS, type), yearLastRenovation: _ylrS, energyLabel: blockAvgLabel(nbh.socialHH), hasLowTemp: false };
  }
  shuffle(rng, nonSHB);
  const hoaHH = [], taken = new Set();
  const put = d => { d.currentType = 'NATURAL_GAS_BLOCK'; hoaHH.push(d); taken.add(d); setNGBl++; };
  // AL: `if (dwellingType == APARTMENT || dwellingType == HIGHRISE)` -- HIGHRISE counts in
  // tier 1. Limburg has MORE highrise (62k) than apartments (48k), so omitting it pushes the
  // HOA fill down into the terraced/random tiers and changes the block's dwelling mix.
  for (const d of nonSHB) { if (setNGBl >= reqNGBl) break; if (d.archetype === 'APARTMENT' || d.archetype === 'HIGHRISE') put(d); }
  for (const d of nonSHB) { if (setNGBl >= reqNGBl) break; if (!taken.has(d) && d.archetype === 'TERRACED') put(d); }
  for (const d of nonSHB) { if (setNGBl >= reqNGBl) break; if (!taken.has(d)) put(d); }
  if (hoaHH.length) { const _ylrH = blockYearLastRenovation(rng, hoaHH);
    nbh.hoaBlock = { buurt: nbh.buurt, blockType: 'HOA', blockKey: blockKeyOf(hoaHH), households: hoaHH, currentType: 'NATURAL_GAS_BLOCK',
      age: initialHsAge(rng, 2024, _ylrH, 'NATURAL_GAS_BLOCK'), yearLastRenovation: _ylrH, energyLabel: blockAvgLabel(hoaHH), hasLowTemp: false };
    for (const d of hoaHH) d.ownership = 'HOME_OWNER_ASSOCIATION'; }
  const rem = nonSHB.filter(d => !taken.has(d)); let i = 0;
  let remDH = reqDH - setDH, remEHP = reqEHP - setEHP, remHHP = reqHHP - setHHP;
  while (remDH > 0 && i < rem.length) { rem[i++].currentType = 'DISTRICT_HEATING'; remDH--; }
  while (remEHP > 0 && i < rem.length) { rem[i++].currentType = 'ELECTRIC_HEAT_PUMP'; remEHP--; }
  while (remHHP > 0 && i < rem.length) { rem[i++].currentType = 'HYBRID_HEAT_PUMP'; remHHP--; }
  while (i < rem.length) rem[i++].currentType = 'NATURAL_GAS_BOILER';
}

/**
 * @param rows  array of raw field arrays
 * @param F     field-name -> index map for `rows`
 * @param get   accessor helpers: { rng, archetypeRows, nbhData, everyNth }
 */
export function buildAgents(rows, F, { rng, archetypeRows, nbhData, everyNth = 1 }) {
  const archIndex = buildArchetypeIndex(archetypeRows);
  const nbhs = new Map(); let id = 0, row = 0;
  for (const f of rows) {
    if (row++ % everyNth !== 0) continue;
    const koop = +f[F.koop], huur = +f[F.huur], corp = +f[F.corp];
    const own = ownership(rng, koop, huur, corp);
    const label = f[F.label] || 'n';
    const fac = truncNormal(rng, 1.0, 0.2, 0.5, 1.5);
    const buurt = f[F.buurt] || 'NA';
    const dtype = f[F.archetype];
    const nb = nbhData[buurt];
    const d = { id: id++, numid: f[F.numid], ownership: own, currentType: 'NATURAL_GAS_BOILER', age: rng.int(0, 12),
      heatDemandKWh: fac * f[F.space] + fac * fac * f[F.dhw], energyLabel: label,
      livingAreaM2: f[F.area], archetype: dtype, dwellingType: dtype,
      constructionYear: +f[F.year], yearLastRenovation: +f[F.year],
      hasLowTemp: false,   // AL: set from the initial heating system's requiresLowTemp (below), not the label
      hasDistrictHeatingGrid: !!(nb && nb.hasDHgrid),
      attitude: rng.beta(5, 2, 0, 1), network: [], peerCounts: null, buurt, category: 0, networkSize: 0 };
    if (!nbhs.has(buurt)) nbhs.set(buurt, { buurt, homeowners: [], landlords: [], socialHH: [] });
    const g = nbhs.get(buurt);
    if (own === 'PRIVATELY_OWNED') g.homeowners.push(d);
    else if (own === 'PRIVATELY_RENTED') g.landlords.push(d);
    else g.socialHH.push(d);
  }
  const homeowners = [], landlords = [], socialBlocks = [], hoaBlocks = [];
  const def = { gasCV: 1, gasBlock: 0, ehp: 0, hhp: 0, dh: 0, hasDHgrid: false };
  const neighbourhoods = [];
  for (const nbh of nbhs.values()) {
    assignInitialHeating(rng, nbh, nbhData[nbh.buurt] || def);
    for (const d of [...nbh.homeowners, ...nbh.landlords, ...nbh.socialHH]) d.hasLowTemp = REQUIRES_LOWTEMP[d.currentType] || false;
    for (const d of nbh.homeowners) d.age = initialHsAge(rng, 2024, d.constructionYear, d.currentType);
    for (const d of nbh.landlords) d.age = initialHsAge(rng, 2024, d.constructionYear, d.currentType);
    for (const d of nbh.homeowners) if (d.ownership === 'PRIVATELY_OWNED') homeowners.push(d);
    for (const d of nbh.landlords) if (d.ownership === 'PRIVATELY_RENTED') landlords.push(d);
    if (nbh.socialBlock) socialBlocks.push(nbh.socialBlock);
    if (nbh.hoaBlock) hoaBlocks.push(nbh.hoaBlock);

    // Neighbourhood agent for DH expansion (DistrictHeatingCompany) and grid congestion.
    // dhDwellings = homeowners + landlord renters + social block members. AL's
    // f_DHExpansionPlanCosts EXCLUDES HOA blocks from the heat-demand sum -- keep it that way.
    const nb = nbhData[nbh.buurt] || def;
    nbh.dhDwellings = [...nbh.homeowners, ...nbh.landlords, ...nbh.socialHH];
    nbh.allDwellings = [...nbh.dhDwellings, ...(nbh.hoaBlock ? nbh.hoaBlock.households : [])];
    nbh.hasDH = !!(nb && nb.hasDHgrid);
    nbh.dhYearOfOperation = 0;            // scheduling latch, 0 = not yet planned
    nbh.surfaceAreaLand = nb && nb.surfaceAreaLand != null ? nb.surfaceAreaLand : 0;
    nbh.policyPlan = (nb && nb.policyPlan) || 'NONE';
    nbh.policyStartJaar = (nb && nb.policyStartJaar) || 0;
    nbh.policyEindJaar = (nb && nb.policyEindJaar) || 0;
    nbh.policyInfraW = !!(nb && nb.policyInfraW);
    neighbourhoods.push(nbh);
  }
  return { homeowners, landlords, socialBlocks, hoaBlocks, neighbourhoods, archIndex };
}
