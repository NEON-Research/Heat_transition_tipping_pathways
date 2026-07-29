# Model TODOs

**Open** analysis/modelling tasks are up top; a brief **Done** log is at the bottom. Plain bug fixes
are applied in code and *not* logged here (they belong in git history, not this list).

---

# Open — analysis & modelling tasks

## Q3 + Q4. TPB-weight sensitivity + calibration — **tooling built, ready to run**

**Data decision (2026-07-28).** The only usable observed series is CBS maatwerk
*Hoofdverwarmingsinstallaties woningen 2022–2024* (`data/Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx`).
CBS changed method before 2022, so **only the 2022–2024 window is internally consistent**.
*Is it still worth using?* **Yes — but only as a broad plausibility range, not a fit target.** Without
it the weights are unconstrained on [0,1]; with it we can at least reject weight sets that get the
*direction and magnitude* of 2022–24 change badly wrong. The signal is real and buurt-level:

| | 2022 mean | change 2022→2024 |
|---|---|---|
| individual gas CV | 83.5 % | **−5.05 %pts** |
| electric (EHP) | 4.1 % | **+3.15** |
| hybrid (HHP) | 1.5 % | **+1.75** |
| district heating | 4.2 % | +0.17 |
| gas block | 4.2 % | −0.17 |

(9,318 buurten with complete data in both years; NL-level gas CV 82→77 %, electric 4→8 %.)

**Calibration region: `province:Noord-Brabant`** — best combined match to NL on *both* criteria
(heating-mix L1 5.9 and dwelling-type L1 12.1 vs NL; Utrecht matches stock better but has ~2× the NL
district-heating share, Limburg is DH-poor). ~1.36 M dwellings.

**Setup:** start the engine in **2022 from the observed 2022 state** (`nbh_heating_2022.csv`), run to
2024, score the simulated 2024 mix against the observed 2024 mix. Same source both ends ⇒ consistent.

**Tooling (built 2026-07-28):**
- `model/data-export/scripts/export_observed_heating.py` → `observed_heating_by_year.csv` (buurt ×
  2022/23/24, mapped to the model's 5 systems, CBS suppression handled) + `nbh_heating_2022.csv`.
- Engine weights are now **runtime-configurable**: `-Dht.<name>=<value>` (or `HT_<NAME>`) for
  `wAttitudeToIntention, wSocialnormToIntention, wPbcToIntention, wAffordabilityToPbc, wEffortToPbc,
  wIntentionToBehavior` (+ gumbel/salience) — see `Constants.p`. No recompile per sweep.
  `-Dht.nbhHeating=nbh_heating_2022.csv` selects the historical initial state.
- `results_analysis/calibrate_weights.py` with three modes:
  `evaluate` (one weight set vs observed), `search` (LHS ensemble → retain sets within a MAD
  tolerance = history matching, **not** a point fit), `morris` (elementary effects: µ*, µ, σ).

**Run order (next actions):**
1. Provision the region: `python run.py --scope province:Noord-Brabant --scenario baseline --iterations 1`
2. `python results_analysis/calibrate_weights.py evaluate --scope province:Noord-Brabant`
   → how far the AL defaults are from observed 2022–24.
3. `... search --samples 60 --tolerance 2 --outdir <dir>` → behaviourally-plausible weight ensemble.
4. `... morris --trajectories 8 --outdir <dir>` → rank which weights actually drive the outcome.
   Use the retained/default weights as the **baseline** for the Morris screen, per Naud.

**Verified 2026-07-28 (smoke-tested on Limburg, JDK 17):** weight overrides demonstrably move results
(gas at 2024: 89.2 % with `wAttitudeToIntention=0.2` vs 76.8 % with 0.9); `evaluate` ran
(AL defaults vs observed 2024 → MAD 1.38 %pts, biggest gap = electric HP under-predicted by 2.6 pts);
`morris` ran (1 trajectory / 7 runs, ranking `wSocialnormToIntention` > `wAttitudeToIntention` >
`wIntentionToBehavior`). Numbers are illustrative only — Limburg, 1 MC iteration, 1 trajectory.

**Caveats to carry into the write-up:** 3 years is short and subsidy-driven; weights remain
non-identifiable (many sets fit) — so report a *retained ensemble band*, not a single calibrated set.

### Background: method choice and feasibility
> *Q3: sensitivity tests for the TPB decision weights; what it means for interpretation and how to
> validate. Q4: calibrate to 2020–2026 adoption? — but weights are non-unique (equifinality) and
> calibration may overweight that window. Goal (Naud): NOT the "true" weights, but to test tipping
> points / pathways under a set of plausible weights.*

The goal is exploratory, so Q3 and Q4 are one workflow: **don't calibrate to a point — bound, sample,
filter, explore** ("exploratory modelling / history-matching (GLUE) under deep uncertainty"; Kwakkel,
Lempert, Beven).

**SA methods (cheapest → most rigorous):**
1. **OAT tornado** — vary each weight ±25/50 %, one at a time (~2×k runs). First screen; local only.
2. **Morris elementary effects** — global screening (~r×(k+1) runs). Best "which weights matter" rank.
3. **LHS + PRCC / metamodel** — sample the space, regress outcomes on weights (~200 runs).
4. **Variance-based Sobol** — first/total-order indices; gold standard but thousands of runs.
5. **Scenario discovery (PRIM/CART on an LHS ensemble)** — find the *weight regions* that produce each
   pathway. **Fits the goal best** ("under which weights do we get which tipping pathway?").

**Feasibility (subset):** Limburg ≈ 8 s/iter, ~3–5 MC iters/sample ⇒ ~30–40 s/sample. Morris (k≈6,
r=10) ≈ 40 min; LHS-200 ≈ 2 h; Sobol-500 ≈ 40 h (too much). Downsample with `--every` or use one
municipality for heavy sweeps. **Prereq:** make the weights runtime-configurable so sweeps don't
recompile.

**Recommended:** Morris (rank weights) → literature-bounded LHS ensemble → scenario discovery mapping
weight-regions → pathways. Report **robustness** ("which tipping conclusions hold across the plausible
weight space"), not a single fitted set.

**On calibration (Q4):** a point fit to 2020–26 overweights a short, subsidy-driven window and is
non-identifiable. Instead use 2020–26 as a **plausibility filter, not a fit target**: run the LHS
ensemble, keep the weight sets whose simulated 2020–26 adoption is within tolerance of observed (CBS /
RVO / Netbeheer / PBL KEV), discard the rest (history matching / GLUE), and explore pathways across
the retained ensemble. **Default weight-setting:** AL defaults as the central case, literature ranges
as bounds, history-match to prune, present results as an ensemble band.

## Q5. Energy-price sensitivity (gas + electricity, high/low)  *(scenarios + a source pull)*
> *Add high/low price scenarios; suggest good bounds with sources.*

Keep it simple — **only LOW and HIGH** per fuel (no central/extra scenarios):
1. **Where price enters:** `energy_source_data` feeds EAC via annual energy cost. Add a per-source
   real annual growth-rate parameter (gas, electricity), runtime-configurable, with LOW/HIGH settings.
2. **Source the bounds (separate step, in order):** PBL **KEV** first (official NL household price
   projections → the low/high bracket); then **TNO / CE Delft** NL heating studies to sanity-check.
   *(Placeholder until pulled: gas ±~3–5 %/yr real, electricity ±~2–4 %/yr real.)*
3. **Report the spark spread** (gas:electricity ratio) — it, not absolute levels, drives HP-vs-gas EAC.
4. Deliverable: LOW + HIGH runs over the matrix + a note on which outcomes are price-robust.
   **Next action: the KEV/TNO/CE Delft source pull, then wire the two paths.**

## Q6. Homeowner segmentation by Rogers' diffusion categories  *(engine tagging + accumulation)*
> *Segment homeowners (e.g. 'laggards': low climate-concern attitude + network + poorly-insulated),
> compute their annual decisions and average stats.*

1. **Propensity index** per homeowner from attitude + network climate-concern + insulation label; bin
   into Rogers' five segments (innovators 2.5 / early adopters 13.5 / early majority 34 / late majority
   34 / laggards 16 %) by percentile.
2. **Tag + accumulate** each homeowner's segment at stock build; add per-segment `YearRow` stats
   (mirrors the avg_* machinery): decisions, chosen-tech mix, avg attitude/EAC/label, switch timing.
3. Deliverable: per-segment adoption curves + a "laggard profile". Validate the index reproduces the
   S-curve ordering. Effort: moderate (engine change).

## Q7. Segmentation by dwelling characteristics  *(prefer post-hoc join)*
> *Differences by dwelling type × floor area × construction year × initial heating, across ownership.*

1. Segments = dwelling_type × floor-area bin × construction-year bin × initial-heating × ownership
   (bin coarsely).
2. **Cheapest route:** dump a per-dwelling final-state row `{id, segment keys, year-of-switch, final
   tech}` once, join to the stock characteristics, pivot in pandas. Avoids heavy engine changes.
3. Deliverable: adoption-rate + tech-mix tables/heatmaps by segment; flag hard-to-decarbonise segments.

## Q8. Demonstrate the grid-congestion feedback loops  *(depends on the grid-congestion port below)*
> *Show how/when feedback loops that account for grid congestion get triggered and accelerate DH /
> renewables; optionally add subsidy/other scenarios to trigger loops or overcome bottlenecks.*

A **tipping point** = when a *reinforcing* loop becomes self-sustaining and uptake accelerates on its
own. The model has two loops; Q8 is to show it can exhibit + locate them, and that congestion reroutes
which fires:
- **Learning-curve loop:** more installs → cumulative volume ↑ → learning lowers capex → EAC ↓ → wins
  more decisions → more installs. The tipping point is where damped flips to self-sustaining (S-curve).
- **Congestion-redirection loop:** HP uptake → local load ↑ → congestion → EHP blocked → DH/hybrid
  becomes viable → DH uptake → DH learning kicks in → more DH. Congestion redirects the tipping from
  all-electric toward DH.

**How to prove it (not just assert):**
1. Plot loop state variables (per-tech cumulative installs, learned capex, salience, congestion events,
   DH-grid count) vs the **adoption rate** — a tipping point shows as an inflection when the driver
   crosses a threshold.
2. **Knock-out test (causal proof):** re-run with the loop disabled (freeze learning / turn congestion
   off), same seeds — the with/without gap quantifies the loop's contribution.
3. **Locate the trigger:** sweep a driver (cost gap, subsidy, congestion severity) and find the flip
   threshold (outcome-vs-driver step).
4. **Policy levers (optional):** subsidy / carbon or gas price (Q5) / faster DH rollout / faster grid
   reinforcement — show they move the tipping point or convert a non-tipping run to a tipping one.

Deliverable: annotated time-series + with/without-loop comparison + a small trigger scenario set.

## Grid congestion + DSO port  *(engine; the linchpin for Q8)*
Congestion is currently **never detected** — `Dwelling.hasGridCongestion` is never set true, so the
three `*_grid_congestion_ban` scenarios ≡ their baselines and Q8's congestion loop can't be shown.
Porting the DSO / grid-capacity mechanism is the open engine work. **When porting, handle the
baseload:** `_neighborhoods_data_2023.csv` has `g_ele = -99999` (CBS privacy suppression) in all
14,421 rows → household baseload = 0, so a naïve port sizes grid *capacity* with a baseload but the
*load* without it (congestion systematically under-detected). Source per-household electricity
elsewhere (e.g. `p6_kwh_2023` from the households DB, or a municipal average) so the comparison is
like-for-like. Regenerate the three ban scenarios afterwards.

---

# Done  *(brief log)*

- **S0 — scenario-difference statistics (done).** `results_analysis/scenario_spread.py` computes the
  cross-scenario spread of the 2050 tech mix (min/max/range/stdev per technology + which scenario is
  the min/max) and writes a strip figure. On Limburg: DH range 41 %pts, hybrid 34, all-electric 23,
  gas boiler 21, gas block ~0 — so the scenarios diverge most on DH and hybrid. (The per-scenario mix
  table and the differentiating-scenario figure already exist in the scenario-comparison plots.)
- **Reference data single-sourced → CSV.** All engine reference inputs (`heating_system_data`,
  `energy_source_data`, `dwellings_demand_insulation`, `nbh_heating`, `neighborhoods`) are now
  generated from the top-level `data/` spreadsheets and read as **CSV**; heating/energy specs are no
  longer hardcoded in `HeatingSystemData.java`; the deprecated AL-dump dependency is gone. Verified
  faithful (baseline identical).
- **Q1 — resolved (documented).** Scenarios aren't actually converged; the apparent similarity was a
  plot-selection artifact (the DH-comparison figure shows 3 near-identical policy scenarios) + weak
  POLICY_BASED DH expansion + congestion never firing. Real spread is large. Follow-up = the open
  **S0** task above.
- **Q2 — done.** Split the figure: homeowner-only `detail_values` + a new 5-panel `eac_by_ownership`
  figure. Also fixed the block-EAC no-trigger years (engine emits **blank**, so they're gaps and don't
  bias the cross-iteration mean — no smoothing).
- **A — done.** `NATURAL_GAS_BLOCK` is a *closed* category (`possible()` rules 4/5): only a current
  gas-block apartment keeps it, and a block can't drop to an individual boiler. Verified: gas block = 0
  for private owners (the earlier "gain" was a flip ratchet), HOA → small residual floor, SOCIAL fully
  converts.
- **B — done.** `avg_att/util/sub_norm/pbc` (homeowners) + per-ownership `avg_eac` now emitted (were
  hardcoded zeros); spot-checked against the AL golden.
- **C — done (re-measure on NL).** `Cli` nulls the agent graph before each reload (peak ~1× not ~2×).
  Confirm the per-iteration time flattened on an NL run with `-Xlog:gc*`.
- **Block average energy label — fixed.** `StockLoader.blockAvgLabel` now rounds to nearest instead of
  integer-division floor. Changes HOA results → regenerate reference runs.

> Note: cross-platform floating-point (`Math.*` not bit-identical) gives small run-to-run differences
> between Windows and Linux for the same seed. Parked per Naud (fine); `StrictMath.*` would make it
> bit-reproducible if ever needed.
