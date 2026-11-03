# Where the numbers in Sections 6–7 come from

Checked 3 Nov 2026 against local `main.tex`. Line numbers refer to that file.
All paths are relative to the repository root. "✓" = recomputed and matches;
"⚠" = see the note.

## Section 6 — Calibration (lines 731–879), Appendix B

Source: `results/calib1000_v3/log_calibration_report.txt`
(regenerate: `python results_analysis/calibration_report.py results/calib1000_v3 --jobs 3`)

| Line | Number in text | Report section |
|---|---|---|
| 775 | admitted deviation ≈ 1.7 pp | B (1.71–1.74) ✓ |
| 780–783 | 1000 sampled, 124 retained, MAD 0.22–0.86, median 1.27, plateau ≈ 2.9 | A ✓ |
| 796 | SN no set above 0.64, attitude none above 0.72 | D ✓ |
| 804–808 | line cost ≈ 0.79 − 2.0·att, residual 0.07, 82–98 %, 21 % | D ✓ |
| 835–842 | table `tab:ensemble` (ranges, 92/82/98/98/97/21 %) | D ✓ |
| 858–860 | 1–16 % (median 5.5 %) of decisions not on max utility | E ✓ |
| 867–869 | 25 of 124 sets, quantile matching | H ✓ |
| 870–878, App. B | Morris sets retained[0, 110, 76, 88]; LHS set retained[0] = best fit = central | H ✓ |

## Section 7.1 — Baseline (lines 883–910)

Source: `results/nl/calib/set*/simulation_results.csv` (25 sets × 20 scenarios,
run 2–6 Oct). Plain median and 5–95 % over the 25 sets, gas block merged into gas.
Figure data: `results/nl/figures/ensemble_band_baseline.csv`
(`python results_analysis/ensemble_band.py --scope nl`).

| Line | Number | Recomputed |
|---|---|---|
| 897 | gas 86 % (2024) → 15 % (7–27) | 86.3 → 15.3 (6.7–26.9) ✓ |
| 898 | electric 57 % (47–70) | 56.9 (47.1–69.5) ✓ |
| 899 | hybrid 16 % (7–26) | 16.2 (7.0–25.9) ✓ |
| 899–900 | district heating 11 % (7–19) | 10.9 (7.3–19.3) ✓ |

## Section 7.2 — Sensitivity (lines 915–990)

Morris: `results/calib1000_v3/structural_morris_retained{0,110,76,88}.json`
(13 Oct; figure via `results_analysis/paper_figures.py`). Ranking (mean total μ*):
social learning 74.0, attitude 45.8, hybrid sustainability 36.7, learning rate 27.3,
capex HP 26.1, opportunity 23.2, network size 19.8, energy price 18.9, homophily 13.6,
salience 13.0/10.1/4.0, lifetime spread 2.0, EAC noise 1.4, capex DH 0.9.

LHS: `results/calib1000_v3/log_lhs_report.txt`
(`python results_analysis/lhs_report.py results/calib1000_v3`), raw data
`structural_lhs_retained0.csv` (2 Nov, 80 × 8 iterations, 7 factors). Figure:
`python results_analysis/lhs_uncertainty.py --lhs results/calib1000_v3/structural_lhs_retained0.csv`.
All numbers in lines 959–980 ✓ (independently recomputed: percentiles, SRC,
linear change, leave-one-out R²).

## Section 7.3 — Feedback loops (lines 991–1050)

Source: `results/nl/figures/scenario_mix_grid.csv`
(`python results_analysis/scenario_mix_grid.py --scope nl`, log `results/nl/log_mix_grid.txt`).
⚠ These are **per-technology medians over the 25 sets, renormalized to 100**
(the same numbers the stacked figures plot), not plain medians. The difference
is up to ~2 pp (e.g. strong social learning gas 26.3 vs plain median 24.8).

| Line | Number | Grid value (2050) |
|---|---|---|
| 1011–1013 | gas 26 (strong SL) / 10 (weak) / 15 (baseline) | 26.3 / 9.8 / 15.4 ✓ |
| 1015–1016 | hybrid 8 vs 21, DH 8 vs 12 | 8.2 / 21.1, 8.3 / 12.3 ✓ |
| 1018–1021 | electric up to 8 pp behind ~2036; gas +11 pp in 2050 | −8.1 (2036); +10.9 ⚠ |
| 1028–1029 | economic learning 8 vs 19 | 8.2 / 18.7 ✓ |
| 1030–1031 | prices 21 vs 7 | 21.0 / 6.5 ✓ |
| 1033–1034 | transition slow 41, fast 2 | 40.9 / 2.2 ✓ |
| 1037–1038 | econ low + social high 31; opposite 5 | 30.5 / 5.4 ✓ |
| 1041–1042 | DH between 8 and 12 % | 8.1–12.3 ✓ |

Lever swings: `results/nl/log_loop_effects.txt` (`loop_effects.py --scope nl`).

⚠ Line 1021: "+11" is the gap between the two median pathways. The median of the
per-set differences is 7 pp in 2050 (peak ~10 pp around 2040).

## Section 7.4 — Segments (lines 1052–1145)

Segments (Fig. `fig:segments_2050`): best-fitting set only,
`results/nl/calib/set00_mad0.22/simulation_results_segments.csv`
(`python results_analysis/segment_analysis.py --scope nl --segments <that file>`; prints all tables).

| Line | Number | Recomputed |
|---|---|---|
| 1062 | owner-occupiers 34 % hybrid, 3 % gas | 33.6 / 3.2 ✓ |
| 1063–1064 | electric 60 (social) / 58 (private rented) vs 51 | 60.3 / 58.2 / 51.2 ✓ |
| 1066 | gas 12 / 10 % | 11.8 / 9.9 ✓ |
| 1069 | DH 12–13 % vs 8 % | 12.0, 12.6 / 8.0 ✓ |
| 1075–1078 | 83–92 % electric; late majority 60 / 24; laggards 67 / 1, gas 17 | ✓ |
| 1085–1086 | hybrid 36 vs 28; electric 48–53 % | ✓ |
| 1087–1089 | label A–B 65 %, E–G 37 %, gas 18 % | 64.6 / 36.9 / 18.2 ✓ |

S-curves (lines 1106–1112): `results/nl/mechanism/mechanism_report.txt` plus the
`rogers` rows of the same segment file. ~90 % by 2040, late majority electric ≈ 25 % around
2045, DH 4 → 15 % ✓. ⚠ "still increasing steeply in 2050": the late majority adds only 2.4
hybrid points in 2048–50 (laggards 3.8).

Decision mix (lines 1125–1131): `results/nl/decisions_band/figures/dec_mix_all.csv`
(`decision_optimality.py --ensemble ... --installed-only`). 2030: cost-optimal heat pumps
75.9 % ("three quarters") ✓; gas highest utility 55.3 % ("more than half") ✓.

## Section 7.5–7.6 — Policy, connection costs, split PBC (lines 1147–1258)

Source: `results/nl/figures/scenario_mix_grid.csv` (same renormalized medians).

| Line | Number | Grid value (2050) |
|---|---|---|
| 1154–1155 | HP ban: electric roughly halves, hybrids almost triple | 57.4 → 28.3; 16.3 → 45.8 ✓ |
| 1157 | "congestion becomes significant after 2035" | ⚠ see below |
| 1174–1176 | voluntary: ≈ baseline DH; obligation: ~4 in 10 | 9.8; 39.6 ✓ |
| 1204–1206 | no connection costs: electric 57 → 17, hybrid 16 → 33, DH 11 → 40 | 16.5 / 32.6 / 40.5 ✓ |
| 1239–1240 | split PBC: electric 50 vs 57, DH 18 vs 11 | ⚠ 50.6 (→ 51) / 18.1 ✓ |

Connection-cost run log: `results/nl/log_conn_costs_off.txt`.

⚠ Line 1157: `nbh_in_grid_congestion_perc` (a fraction, despite the name) rises from
~7 % of neighborhoods in 2030 to ~50 % in 2033–35 and peaks at ~60 % in 2036–37
(sets 00 and 12). Congestion becomes widespread in the early 2030s, not after 2035.

## Not checked here

Sections 1–5 (data and input values), Appendix A tables, Discussion line 1330.
