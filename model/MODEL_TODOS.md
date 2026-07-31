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

> **Full write-up: `results_analysis/CALIBRATION_AND_VALIDATION.md`** — data justification, method
> (history matching, not a point fit), region choice incl. the Utrecht-vs-Noord-Brabant test, how to
> run, findings and caveats. **Region decided: `province:Noord-Brabant`.** Utrecht was tested and
> rejected: lower MAD (1.49 vs 1.97) but a +4 %pt district-heating over-assignment contaminates its
> gas residual, whereas Noord-Brabant reproduces the non-calibrated channels (DH +0.1, gas block +0.3)
> and leaves a clean heat-pump residual. A **start-year initialisation gate** now runs in every mode
> (warns if the simulated start-year mix deviates > `--start-tol`, default 2 %pts).

**Verified 2026-07-28 (smoke-tested on Limburg, JDK 17):** weight overrides demonstrably move results
(gas at 2024: 89.2 % with `wAttitudeToIntention=0.2` vs 76.8 % with 0.9); `evaluate` ran
(AL defaults vs observed 2024 → MAD 1.38 %pts, biggest gap = electric HP under-predicted by 2.6 pts);
`morris` ran (1 trajectory / 7 runs, ranking `wSocialnormToIntention` > `wAttitudeToIntention` >
`wIntentionToBehavior`). Numbers are illustrative only — Limburg, 1 MC iteration, 1 trajectory.

**Open follow-up — sharpen the objective before the retained-ensemble step.** The score is currently
a province-level aggregate (3 systems × 2 years = 6 numbers), which is robust but weakly identifying:
many weight sets reproduce the same provincial total. Fine for the Morris *ranking*, not enough to
select a plausible ensemble. Recommended upgrade: **stratified aggregation** (group buurten by
urbanity / dominant dwelling type / ownership mix, score per stratum) — more spatial signal than one
aggregate, far less noise than per-buurt (CBS buurt shares are rounded to whole %, and per-buurt MC
variance is large). Per-buurt scoring would also need per-buurt engine output. A multinomial
likelihood would be the rigorous alternative to MAD, but needs a defensible noise model. See
`results_analysis/CALIBRATION_AND_VALIDATION.md` §2.2–2.3.

**RESOLVED 2026-07-29 — affordability now uses ONE global EAC scale.** `eacNorm` was normalised per
technology against that technology's own population min/max, which erased the cost *level* difference
and empirically **inverted** it (hybrid, the more expensive option, scored as more affordable because
its range was stretched by outliers). Now normalised on a single global scale across all technologies
and dwellings, so the real cost gap is preserved and homeowner behaviour is consistent with the
block/landlord raw-EAC choice. **Changes results — everything must be re-run:** Limburg baseline 2050
gas 8.4 % → 14.9 %; calibration objective at defaults 1.18 → 1.99 %pts; the earlier Morris screen is
void. Details in `results_analysis/CALIBRATION_AND_VALIDATION.md`.

**ADDED 2026-07-30 — stochastic equipment lifetime (all technologies).** End-of-life age is drawn `~ N(lifetime, sd=1)` clamped to `lifetime ± 3`, redrawn on each (re)install (`Rng.jitteredLifetime`, used in every trigger + install site). Deterministic lifetimes made whole cohorts re-decide in lockstep — most visibly the HOA end-of-life *echo*, where the initial gas-block stock (12-yr) all converted to hybrid HP (15-yr) and left a 3-year trigger gap (2037-2039). Jitter smears this: the 2039 HOA dead zone is filled, and Limburg/NB gas 2050 shifts modestly (NB baseline 12.2%%→11.0%%). `-Dht.lifetimeJitterSd=0` recovers AnyLogic-faithful deterministic lifetimes. **Changes results — re-run calibration + scenarios.**

**REFINED 2026-07-30 — the global scale is now LOGARITHMIC.** The linear global window was set by a
handful of very large/expensive dwellings (p99 ≈ €5.8k vs max €13.2k), so a household's own option
spread was only ~6–13 % of the scale and affordability mostly encoded dwelling *size* rather than
*which option is cheaper*. `eacNorm` now normalises `log(EAC)` on the same global window
(`Decision.normalizedLog`): the mapping is proportional (people weigh cost in %, matching TPB's felt
control), the expensive tail is compressed without clamping, and cost level is retained (unlike a
per-m² transform). Own-spread share for a small dwelling rises 5.9 % → 18.8 %; the size gradient
flattens. Ordering still correct (gas cheaper ⇒ more affordable). **Re-run everything again** — the
affordability distribution recentres (median dwelling ≈ 0.46 vs ~0.85 before), which recalibration
will re-weight.

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

## Finding (2026-07-30): TPB **driver signatures** — each technology is carried by a different term

Measured from the calibrated best_fit run (Noord-Brabant, PRIVATELY_OWNED; the `avg_att / avg_sub_norm
/ avg_pbc` columns are the appeal of each option averaged over all evaluators each year). This is the
quantitative backbone for Q6 below — every cell is a script-producible output column, not a hand
narrative; the *interpretation* under the table is the qualitative layer.

| technology | attitude | subjective norm (2026 → 2050) | affordability `pbc` (2026 → 2050) | **carried by** |
|---|---|---|---|---|
| gas boiler | 0.30 (low) | **0.89 → 0.05** (collapses) | 0.66 → 0.49 | **subjective norm** (incumbency), then nothing |
| hybrid HP | **0.79** (high) | 0.07 → 0.49 (builds) | 0.36 → **0.57** (learning) | **attitude + rising affordability & social proof** |
| electric HP | 0.70 | 0.18 → 0.50 | 0.34 → 0.55 | **attitude**, but affordability/effort-penalised |
| district heating | 0.77 | 0.28 (grid-gated) | 0.63 (high) | attractive **where the grid exists** |

Three insights, in decreasing obviousness:

1. **Only subjective norm migrates.** Attitude and affordability are ~flat over time; subjective norm
   is the single driver that moves (gas 0.89→0.05, hybrid 0.07→0.49). **The transition IS a handover of
   social norm from gas to heat pumps** — this is the tipping mechanism, and its sharpness/timing is
   governed by the salience parameters (see the structural-sensitivity table in
   CALIBRATION_AND_VALIDATION.md), not by cost or attitude. Gas has no intrinsic pull (attitude 0.30);
   it lives on incumbency and dies when social proof flips.
2. **Non-monotonic attitude: hybrid beats full-electric on *attitude*, not only on cost.** attitude =
   `1 − |householdAttitude − sustainabilityScoreNorm|`; norms are gas 0 / hybrid 0.5 / electric 1.0, and
   the mean household is Beta(5,2) ≈ 0.71, which sits *closer to hybrid's 0.5 than to electric's 1.0*.
   So the median household is more pro-hybrid (0.79) than pro-electric (0.71) on conviction alone;
   full-electric only wins attitude in the top attitude quartile. Hybrids therefore dominate for **two
   independent structural reasons** — cheaper **and** a better attitude match for the median — so full
   electrification stalls unless the median attitude rises OR electric HP's `pbc` (cost + retrofit
   effort) improves. The model says the binding lever is electric HP's `pbc`.
3. **Driver → Rogers adopter group falls straight out** (motivates Q6): attitude drives the front of
   the S-curve (innovators/early adopters — the only group where electric HP's attitude edge wins, they
   move before cost/norm favour it); affordability + rising social proof drive the bulk (early/late
   majority → hybrid); social-norm inertia + forced end-of-life define the tail (laggards on gas).

*Caveat:* the table's `avg_*` are over **evaluators**, so they measure each option's *appeal per
dimension* — good for signatures, but not a per-adopter attribution. Q6 adds the adopter-side
decomposition.

## Q6 (merged with old Q7). Adopter-segment **adoption pathways** + driver signatures  *(engine change — traces annual statistics)*
> *Trace how each adopter segment moves through the transition year by year — the S-curve per segment,
> which technology they pick, and which TPB term drove that pick — and cross this with dwelling
> characteristics. Annual tracing is the core research output, so this is done in-engine (not a
> one-shot post-hoc dump).*

**Two segmentation axes, same accumulation machinery:**
- **A. Rogers behavioural segment** (old Q6): a per-homeowner **propensity index** = weighted blend of
  attitude + network climate-concern (mean attitude of the peer network) + insulation label, binned by
  percentile into innovators 2.5 / early adopters 13.5 / early majority 34 / late majority 34 /
  laggards 16 %. Tag on the `Dwelling` at stock build.
- **B. Dwelling-characteristic segment** (old Q7): dwelling_type × floor-area bin × construction-year
  bin × initial-heating (coarse bins). Also tagged on the `Dwelling`. Kept as a *second key* so the
  same annual stats can be sliced either way (and cross-tabbed, e.g. "laggards in poorly-insulated
  terraced houses").

**Engine changes (fine to store more + rerun):**
1. Add `segmentRogers` and `segmentDwelling` fields to `Dwelling`; compute + assign at load
   (propensity index needs the network, so after `buildNetwork`).
2. New per-`(year × segment × heating_system)` accumulator, mirroring the `avg_*` machinery but keyed
   by segment. Each year record, per segment: **stock** (current holders), **installed** (chose this
   tech this year → the adoption curve), **considered/triggered** (deciders), the **driver
   decomposition of the CHOSEN option** (mean att / sn / pbc / intention / util of what each adopter
   actually picked → the per-adopter driver signature), plus avg EAC, avg label, and mean switch age.
3. Emit a **separate** `segment_stats.csv` alongside `simulation_results.csv` (rows: scenario,
   iteration, year, segment_type, segment, heating_system, + the stats above). Keeps the main schema
   clean; one extra file to analyse. Do it for both segmentation axes (segment_type ∈ {rogers,
   dwelling}).

**Analysis scripts (results_analysis/):**
- **Per-segment adoption curves** — stacked/line tech share over 2024–2050 for each Rogers segment
  (and each dwelling segment). Validate the index reproduces the **S-curve ordering** (innovators lead,
  laggards trail).
- **Driver-signature-over-time** — per segment × chosen tech, the mean att/sn/pbc contribution, so the
  handover (gas→HP subjective norm) and the attitude-vs-cost split across segments are explicit and
  data-based, not asserted.
- **"Laggard profile"** and **hard-to-decarbonise segments** (cross Rogers × dwelling): who is last,
  on what, and why.

**Deliverables:** per-segment annual adoption curves; driver-signature tables/plots by segment and
year; laggard/hard-to-decarbonise profiles. **Effort:** moderate engine change + ~2 analysis scripts.
This is the primary mechanism-level research output, so prioritise the annual per-segment tracing.

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
