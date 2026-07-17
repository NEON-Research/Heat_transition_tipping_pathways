// results.js — aggregate simulation rows into the EXACT simulation_results CSV
// schema the AnyLogic model exports, so tests/compare_to_golden.py works unchanged.
// Homeowner-only core -> ownership column is PRIVATELY_OWNED and TOTAL.

import { HEATING_SYSTEMS } from './constants.js';

const HEADER = ['scenario','scenario_name','iteration','year',
  'nbh_in_grid_congestion_perc','nbh_with_dh_perc','heating_system','ownership',
  'installed_current','installed_annually','removed_annually','installed_cumulative',
  'considered_annually','avg_att','avg_util','avg_sub_norm','avg_eac','avg_pbc',
  'SLF','ELF','GRR','DHCT','DHES','SHAES','DHCO','GCHPB'];

export function rowsToCsv(runRows, meta) {
  const s = meta.scenario;
  const lines = [HEADER.join(',')];
  for (const r of runRows) {
    for (const hs of HEATING_SYSTEMS) {
      for (const ownership of ['PRIVATELY_OWNED', 'TOTAL']) {
        lines.push([
          meta.scenarioId, s.scen_name, meta.iteration, r.year,
          0, 0, hs, ownership,
          r.stock[hs], r.yearInstalled[hs], r.yearRemoved[hs], r.cumInstalled[hs],
          r.considered, 0, 0, 0, 0, 0,
          s.social_learning_factor, s.economic_learning_factor, s.grid_reinforcement_rate ?? 'MEDIUM',
          s.DH_construction_time ?? 5, s.DH_expansion_strategy ?? 'COST_BASED',
          s.SHA_strategy ?? 'COST_BASED', s.DH_connection_obligation ?? false,
          s.Grid_congestion_HP_ban ?? false,
        ].join(','));
      }
    }
  }
  return lines.join('\n') + '\n';
}
