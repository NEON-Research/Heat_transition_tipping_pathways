// vesta.js — on-the-fly insulation cost from the archetype table (dwellings_demand_insulation),
// matching AL exactly (J_Dwelling.f_getInsulationCosts + f_insulationLabelLetterToNumber).
// Heat demand stays frozen at the initial label (AL does NOT recompute it after renovation),
// but insulation cost uses the dwelling's CURRENT label, so exogenous insulation flows through.

// f_insulationLabelLetterToNumber: a=1 (best) .. g=7 (worst); unknown -> 3 (default).
export function labelNum(letter) {
  switch ((letter || 'n').toLowerCase()) {
    case 'a': return 1; case 'b': return 2; case 'c': return 3; case 'd': return 4;
    case 'e': return 5; case 'f': return 6; case 'g': return 7; default: return 3;
  }
}
// f_insulationLabelNumberToLetter
export function numLabel(n) {
  if (n <= 1) return 'a'; if (n === 2) return 'b'; if (n === 3) return 'c';
  if (n === 4) return 'd'; if (n === 5) return 'e'; if (n === 6) return 'f'; return 'g';
}

// index archetype rows by type_ol -> sorted bands
export function buildArchetypeIndex(rows) {
  const byType = {};
  for (const r of rows) (byType[r.type_ol] ??= []).push(r);
  for (const t in byType) byType[t].sort((a, b) => a.bouwjaar_min - b.bouwjaar_min);
  return byType;
}
export function archRow(index, typeOl, year) {
  const rows = index[typeOl] || [];
  for (const r of rows) if (r.bouwjaar_min <= year && year <= r.bouwjaar_max) return r;
  return rows[0] || null;
}

// f_getInsulationCosts: (minTotal+maxTotal)/2, each = asl + opp*area. 0 if no upgrade needed.
// Upgrade needed when required label is better than current: labelNum(to) < labelNum(from).
export function insulationCost(row, fromLabel, toLabel, area) {
  if (!row) return 0;
  if (labelNum(toLabel) >= labelNum(fromLabel)) return 0;   // already good enough
  const fl = (fromLabel || 'n').toLowerCase();
  const col = `ki_s${fl}${toLabel}`;
  const a1 = row[`${col}_min_asl`], o1 = row[`${col}_min_opp`];
  const a2 = row[`${col}_max_asl`], o2 = row[`${col}_max_opp`];
  if (a1 == null || a2 == null) return 0;
  const mn = a1 + (o1 || 0) * area, mx = a2 + (o2 || 0) * area;
  return (mn + mx) / 2;
}
