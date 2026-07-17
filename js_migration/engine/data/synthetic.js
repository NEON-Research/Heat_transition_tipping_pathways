// synthetic.js — a plausible homeowner dwelling stock so the engine runs end-to-end
// TODAY on REAL economics. This is NOT the real building stock: for golden parity,
// export DWELLINGS_DEMAND_INSULATION + the neighbourhood household tables and load
// those instead (same dwelling shape). Marked clearly as synthetic.

import { RNG } from '../src/rng.js';

const LABELS = ['a', 'b', 'c', 'd', 'e', 'f', 'g'];

export function makeSyntheticDwellings(n, seed = 42) {
  const rng = new RNG(seed);
  const dwellings = [];
  for (let i = 0; i < n; i++) {
    // heat demand: log-normalish spread 3,000..18,000 kWh
    const heatDemandKWh = Math.round(3000 + Math.pow(rng.next(), 1.5) * 15000);
    // energy label skewed to poorer labels (typical older stock)
    const label = LABELS[Math.min(6, Math.floor(Math.pow(rng.next(), 0.7) * 7))];
    const livingAreaM2 = Math.round(60 + rng.next() * 140);
    const dwellingType = rng.next() < 0.35 ? 'APARTMENT' : 'HOUSE';
    dwellings.push({
      id: i,
      currentType: 'NATURAL_GAS_BOILER',           // vast majority start on gas
      age: rng.int(0, 12),                          // random age within boiler lifetime
      heatDemandKWh,
      energyLabel: label,
      livingAreaM2,
      dwellingType,
      hasLowTemp: label === 'a' || label === 'b',   // best labels already low-temp ready
      hasDistrictHeatingGrid: false,                // homeowner neighbourhood w/o DH
      attitude: rng.beta(5, 2, 0, 1),               // sustainability attitude (model dist.)
      network: [], peerCounts: null,
    });
  }
  return dwellings;
}
