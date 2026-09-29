# Next steps: from calibration to scenarios

Run everything from the repo root with the project Python (`.venv\Scripts\python`). On Windows set
`$env:PYTHONIOENCODING="utf-8"` first, otherwise printing "€" can crash a script at the end.

The new calibration writes to `results\calib1000\`. The previous one is kept in
`results\calib1000_before_2026-09-28\` for comparison (`results\` is git-ignored: that copy exists
only on this disk).

> **Watch the paths.** Several analysis scripts read a fixed folder, and they disagree:
> `results\calib\` (analyze_all, glue_weighting, extract_band_baseline, run.py `--weights-file`,
> social_sweep) holds an OLD calibration from 2026-08-04. Always pass the new file explicitly
> (`--search` / `--weights-file` / `--calib`) as below, or, once step 2 is done, back up
> `results\calib\` and copy the new `calibration_search.json` into it.

---

## Update 2026-09-29 (afternoon): the calibration must be rerun first

Three model changes since the calibration of 2026-09-28 (commits 7d134f3, af8d374 and the
behavioural-screen commit): heating-system costs from CE Delft (2024 EUR incl. VAT; lifetimes
15 years; SCOP 3.8; gas efficiency 0.94 on the upper heating value), time-dependent subsidies
(ISDE etc., `data/subsidies.csv`, scenario column `subsidy_policy`), and six behavioural
parameters in the Morris screen (now 19 factors: ~160 runs per screen set, ~5.7 h for the four).
`results\calib1000\` therefore describes the OLD model. Before step a-d below:
```powershell
.venv\Scripts\python results_analysis\calibrate_weights.py search --samples 1000 --outdir results\calib1000_v2
.venv\Scripts\python results_analysis\select_subsample.py select results\calib1000_v2 --n 25
.venv\Scripts\python results_analysis\ensemble_structure.py results\calib1000_v2
```
then run a-d with `results\calib1000_v2` in place of `results\calib1000` (a fresh folder keeps the
old calibration for comparison). Later the same day the heat pumps were repriced by capacity at
the Milieu Centraal level (`data/heating_system_capex.csv`: electric 12,000 at 8 kW, hybrid incl. boiler
8,300 at 4 kW, Vesta MAIS cost per kW); at the default weights the fit is then MAD 0.25 pp
(2024: electric 8.5 % vs 8.2 %, hybrid 3.1 % vs 2.9 %). The gas boiler and block then moved to
Milieu Centraal's 2,100 (one price; boilers are sized on hot water). Calibration takes ~5.5 h.

## Status and next phase (2026-09-29)

Done: calibration (161 of 1000 retained), ensemble figures, `ensemble_structure.py`, subsample
`select` (25 sets + the screen sets, in `results\calib1000\calibration_subsample.json`). Old
screen outputs moved to `results\calib1000\_old_screen_2026-08-28\` so they do not mix with the
new ones. Running: the quick Noord-Brabant pathway run (step 3b).

One weight set is used for every single-set experiment: the **central set** (retained:146 in
`calibration_search.json`, retained:17 in `calibration_subsample.json`: the same set). Morris uses
four screen sets. Every script resolves `central` / `morris` the same way
(`select_subsample.screen_indices`), and it is now the default everywhere.

After reviewing the quick run (step 3b), in this order:
```powershell
$env:PYTHONIOENCODING="utf-8"
# a. price arms + knock-outs of the quick run again at the central set (~15 min; the running quick
#    run used the old synthetic-median default for these)
.venv\Scripts\python results_analysis\pathway_batch.py --scope province:Noord-Brabant --search results\calib1000\calibration_search.json --no-band --price-scenario baseline
# b. check that the 25 stand in for all 161 (~1.5 h)
.venv\Scripts\python results_analysis\select_subsample.py validate results\calib1000 --scope province:Noord-Brabant
# c. Morris at the 4 screen sets + LHS at the central set + figures (~10 h)
.venv\Scripts\python results_analysis
un_overnight.py --scope province:Noord-Brabant --outdir results\calib1000 --skip-search
# d. national scenarios on the 25 (long)
.venv\Scripts\python results_analysis\pathway_batch.py --scope nl --search results\calib1000\calibration_subsample.json
```
b and c can run back to back overnight; d after checking b and c. Details per step below.

## 1. Calibration (running)

```powershell
.venv\Scripts\python results_analysis\calibrate_weights.py search --samples 1000 --outdir results\calib1000
```

Started 2026-09-28 17:47. About 25 s per weight set, so ~7 h: expect it to finish around 01:00.
What it does differently from the previous run: district heating is scored, DH dwellings stay
connected (`dhReviewYears=0`), sd_obs is derived from the data, and the retention table is written.

## 2. Check the calibration and decide on the retained sets

Output: `results\calib1000\calibration_search.json` and `calibration_retention_sensitivity.csv`.

Check, in the console summary at the end of the run:
- [ ] **Start-year gate** passed (every technology within 2 pp of observed 2022).
- [ ] **sd_obs** printed as ~0.28-0.29 pp; the admitted deviation ~1.7 pp.
- [ ] **Retained at I < 3**: a workable number (roughly 10-100 of 1000). Zero means the model cannot
      reproduce 2023-24 with any weights: stop and investigate before continuing.
- [ ] **Separation**: retained sets clearly better than the bulk (the old run: retained MAD
      0.19-0.88 pp against a bulk around 5 pp).
- [ ] **District heating** in the retained sets close to observed (6.1 %) in 2023 and 2024.
- [ ] **Retention table**: how strongly the count depends on sd_disc versus the cutoff.

Compare with the previous calibration (old model: no DH scoring, fixed sd_obs 0.25):
```powershell
.venv\Scripts\python results_analysis\make_ensemble_figures.py results\calib1000
# compare results\calib1000\fig_ensemble_*.png with results\calib1000_before_2026-09-28\
.venv\Scripts\python results_analysis\calibrate_weights.py reanalyse --json results\calib1000\calibration_search.json
```
What the observations constrain (the cost-attitude trade-off, Figure `fig:ensemble_structure`):
```powershell
.venv\Scripts\python results_analysis\ensemble_structure.py results\calib1000 --copy-to heat_transition_tipping_dynamics\figures
```
Look for: did the retained weight ranges (fig_ensemble_weights) shift, and did the count change
much? Large shifts are expected mainly in the affordability weight, since the cost model changed
(DH costs, ownership, prices).

**Decide** the retention rule. Default: I < 3 with sd_disc = 0.5 pp (already applied in the JSON).
If you choose a different sd_disc or cutoff, the JSON's `retained` list must be rewritten for it
(not yet a script option: ask for a `--rewrite` mode, or rerun `search` with `--sd-disc` /
`--implaus-cut` on a copy).

Then update the paper (`heat_transition_tipping_dynamics\main.tex`):
- [ ] fill Table `tab:retention_sensitivity` from `calibration_retention_sensitivity.csv`
- [ ] the numbers marked `% TODO` in the results (sets retained, MAD range, default MAD)
- [ ] the replication sentence (0.028 pp) if `converge` gives a different value

## 3. Parameter screen: Morris, then LHS on the survivors (~9-10 h)

```powershell
.venv\Scripts\python results_analysis\run_overnight.py --scope province:Noord-Brabant --outdir results\calib1000 --skip-search
```
This reuses the new sweep and runs: Morris at the four screen sets from `calibration_subsample.json`
(run step 3c `select` first; without it they are picked from all 161)
(13 factors, now including `dhReviewYears` and `dhExitBarrier`), LHS on the top factors at the
central set, then the ensemble and paper figures. Everything is logged in `results\calib1000\`.
`--dry-run` shows the commands first.

Check: the Morris ranking is similar across the four screen sets; where the two DH factors rank;
`structural_lhs_retained146.csv` exists (the LHS file is named after the central set). Then:
```powershell
.venv\Scripts\python results_analysis\lhs_uncertainty.py --lhs results\calib1000\structural_lhs_retained146.csv
```

## 3b. Quick scenario check on Noord-Brabant (~1.5 h), then compare with before

The old Noord-Brabant results (Aug/Sep, old model and old calibration) were moved to
`results\noord-brabant_before_2026-09-29\`, so the new run starts in a clean `results\noord-brabant\`.
Three representative weight sets x all scenarios x 3 iterations, price arms on the baseline only,
knock-outs (~32 s per scenario-iteration):
```powershell
.venv\Scripts\python results_analysis\pathway_batch.py --scope province:Noord-Brabant --search results\calib1000\calibration_search.json --ensemble-weights representative --price-scenario baseline
.venv\Scripts\python results_analysis\compare_runs.py results\noord-brabant_before_2026-09-29 results\noord-brabant --years 2030,2050 --by-ownership
```
`compare_runs.py` prints old > new per scenario and technology (median over weight sets), marks
shifts of 2 pp or more, lists the largest ones and writes `results\noord-brabant\compare_to_*.csv`.
Scenarios that were renamed or added since August appear as "only in old/new". Note the old band
is the median of 21 sets, the quick new one of 3.

## 3c. Subsample of the retained sets for the scenarios

161 sets are too many for every scenario at national scale. Pick 25 by a space-filling pick along
the cost-attitude line and the social-norm weight (no model runs, seconds), then check on
Noord-Brabant that they stand in for all 161 (baseline, 1 iteration per set, ~1.5 h, resumable,
outputs in `results\calib1000\subsample_validation\`; run it when nothing else is running):
```powershell
.venv\Scripts\python results_analysis\select_subsample.py select results\calib1000 --n 25
.venv\Scripts\python results_analysis\select_subsample.py validate results\calib1000 --scope province:Noord-Brabant
```
`select` writes `calibration_subsample.json` (same format, 'retained' = the 25, equal weight) with
the screen sets for step 3 (Morris: best fit, both ends of the cost-attitude line, highest
effective social norm; LHS: the central set), and `fig_subsample.png`. Run `select` BEFORE step 3:
`run_overnight.py` takes its screen sets from this file. `validate` prints PASS/FAIL per technology and year and writes
`subsample_validation.csv`; then remove the `% TODO` after the subsample paragraph in `main.tex`.
On FAIL: rerun `select` with a larger `--n`.

## 4. Scenarios (the pathways)

Preference band (every retained set x every scenario), the price arms and the knock-outs:
```powershell
.venv\Scripts\python results_analysis\pathway_batch.py --scope nl --search results\calib1000\calibration_subsample.json
```
(`--scope province:Noord-Brabant` first for a faster check.) The baseline now shows the cost-driven
DH decline; `dh_connection_obligation` counters it. Then the figures:
```powershell
.venv\Scripts\python results_analysis\scenario_mix_grid.py --scope nl --search results\calib1000\calibration_subsample.json
.venv\Scripts\python results_analysis\ensemble_band.py --scope nl --search results\calib1000\calibration_subsample.json
.venv\Scripts\python results_analysis\paper_figures.py --scope nl --calib results\calib1000
.venv\Scripts\python results_analysis\energy_cost_figures.py
```
`analyze_all.py` and `glue_weighting.py` read `results\calib\` only (see the note at the top).

## 5. Wrap up

- [ ] copy the final figures into `heat_transition_tipping_dynamics\figures\` and check the captions
- [ ] results text: the DH decline in the baseline and which policy scenarios counter it
- [ ] commit (code and paper; `results\` stays out of git)
