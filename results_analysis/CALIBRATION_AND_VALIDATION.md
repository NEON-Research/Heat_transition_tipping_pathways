# Calibration & validation of the TPB decision weights

How the behavioural weights are constrained against observed data, why the approach is a
*plausibility filter* rather than a point fit, which region is used and why, and how to run it.

Companion docs: `ANALYSIS.md` (results & scenarios), `model/MODEL_TODOS.md` (open work).

---

## 1. The data, and whether it is usable

**Source:** CBS maatwerk *Hoofdverwarmingsinstallaties woningen 2022–2024*
(`data/Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx`, sheet `Tabel 2`, buurt level).
<https://www.cbs.nl/nl-nl/maatwerk/2025/34/hoofdverwarmingsinstallaties-woningen-2022-2024>

**The catch:** CBS used a different method before 2022, so only **2022–2024** is an internally
consistent series — a 3-year window.

**Is it still worth using? Yes, but only as a broad plausibility range.** Without any observed
anchor the weights are unconstrained on [0,1]; with it we can at least reject weight sets that get
the *direction and magnitude* of recent change badly wrong. It is **not** enough to identify a
unique weight vector, and it should never be presented as a fitted calibration.

**The signal is real** (9,318 buurten with complete data in both years):

| system | 2022 mean | change 2022→2024 |
|---|---|---|
| individual gas CV | 83.5 % | **−5.05 %pts** |
| electric (EHP) | 4.1 % | **+3.15** |
| hybrid (HHP) | 1.5 % | **+1.75** |
| district heating | 4.2 % | +0.17 |
| gas block | 4.2 % | −0.17 |

(NL totals: gas CV 82→77 %, electric 4→8 %.)

**Mapping to the model's 5 systems** (same rule as the AL model / `export_nbh_heating.py`):
`Individuele CV → NATURAL_GAS_BOILER`, `Blok-verwarming → NATURAL_GAS_BLOCK`,
`Stadsverwarming (hoog+laag+zonder) → DISTRICT_HEATING`,
`elektrisch met hoog gasverbruik → HYBRID_HEAT_PUMP`,
`elektrisch (laag + zonder gas) → ELECTRIC_HEAT_PUMP`.
CBS suppression (`.`) in the stadsverwarming columns means *no district heating there*, so it is read
as 0 and does not make a row incomplete; the individually-modelled columns must all be present for a
buurt-year to count as `complete`.

---

## 2. Method: history matching, not a point fit

Three years of subsidy-driven data cannot identify six weights (many combinations fit equally well —
equifinality). So the workflow is **bound → sample → filter → explore**, i.e. history matching /
GLUE-style exploratory modelling, consistent with the project goal of mapping *tipping pathways under
plausible weights* rather than finding "the true" weights.

1. **Bound** each weight with a plausible range (currently ±around the AnyLogic defaults).
2. **Sample** the weight space (Latin hypercube).
3. **Filter**: retain the weight sets whose simulated 2022→24 change lands within a tolerance of
   observed (default MAD ≤ 2 %pts). Discard the wildly-off ones.
4. **Explore**: run scenarios across the *retained ensemble* and report a band, not a single line.

### 2.1 What exactly is scored

The objective is currently:

> **mean, over the scored years {2023, 2024}, of the mean absolute deviation (in %-points) across
> three heating systems, computed on the *province-level* dwelling-weighted share.**

i.e. one simulated number and one observed number per (system, year) for all of Noord-Brabant —
5 systems × 2 years, of which 3 systems enter the score. 2022 is the initialisation year and is never
scored (it is identity-checked instead, §3).

**Which systems.** Only gas boiler, electric HP and hybrid HP. District heating is grid-gated and
obligation-driven rather than a TPB choice, and gas block is a closed, decaying category — including
either would score mechanisms the weights don't control.
*Note the systems are weighted **equally**, so a 1 %-point error on electric HP (≈6 % of the stock)
counts as much as 1 %-point on gas boiler (≈85 %). That is deliberate — the small, policy-relevant
heat-pump categories are the ones we care about — but it is a choice, not a neutral default.*

### 2.2 Why MAD, and what the alternatives are

| metric | behaviour | verdict |
|---|---|---|
| **MAD / MAE** (chosen) | mean of \|sim − obs\| in %-points | **used** — directly interpretable ("off by 1.2 %-points on average"), robust to one bad system, and it makes the retention tolerance meaningful (`--tolerance 2` = "within 2 %-points") |
| RMSE / MSE | squares errors, so large misses dominate | rejected: with only 3 systems there is no outlier problem to solve, and squaring amplifies Monte-Carlo noise, which would make the Morris ranking jumpier |
| relative / percentage error | error ÷ observed share | rejected: would let district heating (1.6 %) or hybrid (2.6 %) dominate purely because their denominators are small |
| multinomial log-likelihood / χ² | treats shares as counts over known dwelling totals | **the rigorous upgrade** — it would give a proper statistical acceptance criterion and enable Bayesian/ABC calibration. Deferred: it needs a defensible noise model (CBS shares are rounded to whole %, and MC iterations add their own variance) |

Since the goal is a *plausibility filter*, not an optimum, an interpretable distance in %-points is
worth more than statistical sharpness — the number has to support a defensible statement like
"weight sets more than 2 %-points off the observed trajectory were discarded".

### 2.3 Spatial aggregation: why province-level, and the alternatives

This is the more consequential choice, so the options are worth stating:

| level | information used | pros / cons |
|---|---|---|
| **Province total per year** (current) | 3 systems × 2 years = 6 numbers | Robust and nearly noise-free (aggregating ~1.3 M dwellings averages out MC variance). **But weakly identifying**: many different weight sets reproduce the same provincial aggregate, and it is blind to *where* adoption happens. |
| Per-neighbourhood (buurt) | ~1,450 buurten × 3 systems × 2 years | Far more information, and it tests the *spatial distribution* — whether heat pumps appear in the right kind of neighbourhood, not just in the right quantity. **Costs:** the engine would need per-buurt output (it currently emits only TOTAL rows); per-buurt MC noise is large (small populations); and CBS buurt shares are rounded to whole percent, a ±0.5 %-point quantisation floor. Real risk of fitting noise. |
| Stratified groups | e.g. by urbanity, dominant dwelling type, ownership mix | **The sensible middle ground and the recommended next step**: enough spatial signal to distinguish weight sets, while each stratum still aggregates enough dwellings to suppress noise. |
| Change (Δ) instead of level | Δ2022→2024 per system | Emphasises dynamics over starting state. Here it is nearly equivalent to the level-based score, because the run is initialised from the observed 2022 state — so levels already *are* changes. |

**Why province-level for now:** it is the level at which the data is trustworthy (buurt values are
rounded and noisy), it costs no extra engine plumbing, and it is adequate for the immediate purpose —
a Morris *ranking* of which weights matter. Before the retained-ensemble step (which does need
identification power) the stratified option should be revisited; it is logged as a follow-up.

**Experimental setup.** Initialise the engine in **2022 from the observed 2022 state**
(`-Dht.heatingYear=2022`, reading the `gasCV_2022 … dh_2022` columns of the combined
`neighborhoods.csv`), run to 2024, and score the simulated **2023 and 2024** mixes against observed.
Same source throughout, so method differences cancel.

**The CBS-complete filter is calibration-only.** `-Dht.buurtFilter` is passed *only* by
`calibrate_weights.py`; `model/run.py` never sets it, so all scenario/analysis runs simulate **every**
neighbourhood, with data-less buurten falling back to the default gas-boiler mix. Dropping buurten is
acceptable for calibration (it makes simulated and observed cover the same population) but would bias
the headline results, so it must not leak into the main model.

---

## 3. Calibration region: Noord-Brabant

The region must be representative of NL in **dwelling stock** (drives EAC / insulation cost) and
**current heating mix**, so weights fitted there transfer. Scored by L1 distance to NL:

| province | dwellings | stock L1 | heating L1 | note |
|---|---|---|---|---|
| **Noord-Brabant** | 1.36 M | 12.1 | **5.9** | best heating match, good overall |
| Utrecht | 0.74 M | **6.5** | 12.4 | best stock match, but ~2× NL district heating |
| Gelderland | 1.13 M | 14.5 | 7.0 | |
| Limburg | 0.62 M | 16.3 | — | district-heating-poor (1.6 % vs NL 7.0 %) |

Owner-occupancy (the TPB deciders) — NL 47.7 %, Utrecht 49.7 %, Noord-Brabant 51.4 %.

**Utrecht was tested and rejected.** It is half the size (~14 min vs ~26 min for a Morris screen) and
matches the stock better, so it looked attractive. But running `evaluate` on both showed *why the
error composition matters more than its size*:

| | MAD (gas/EHP/HHP) | DH error | gas error | EHP error |
|---|---|---|---|---|
| Utrecht | **1.49** | **+4.0** | −2.5 | −1.7 |
| Noord-Brabant | 1.97 | **+0.1** | +2.6 | −3.2 |

Utrecht's lower MAD is misleading: the model puts **+4 %pts too many dwellings in district heating**
there, and those dwellings are removed from the gas pool — which is very likely why its gas error is
*negative* while Noord-Brabant's is positive. In other words Utrecht's residual is contaminated by an
initialisation/DH artefact rather than being behaviour. Noord-Brabant reproduces the channels we are
*not* calibrating almost exactly (DH +0.1, gas block +0.3), leaving a clean residual concentrated in
the heat-pump channel we *are* calibrating. **Noord-Brabant is therefore the calibration region.**

**Guard, not a special mode.** Rather than a separate check, every mode runs a **start-year
initialisation gate**: it compares the simulated 2022 mix with the observed 2022 mix and warns if any
system deviates by more than `--start-tol` (default 2 %pts). This is exactly the "make sure district
heating in the first year isn't much higher than the data" check; disable with `--no-start-check`.

### The start year should be (and now is) essentially identical

The run is initialised from the same 2022 CBS data it is scored against, so the start-year mix should
match almost exactly. Initially it didn't (deviations of 1–2 %pts, and +4 %pts DH in Utrecht). That
turned out to be **three flaws in the comparison, not in the model**:

1. **Unweighted vs dwelling-weighted aggregation.** `observed_mix()` took a plain mean of buurt
   shares, while the engine's TOTAL is dwelling-weighted — so small buurten counted equally in the
   observation but barely in the simulation.
   → **Fixed:** the observed aggregate is now weighted by the model's per-buurt dwelling counts.
2. **Different buurt sets.** CBS suppresses cells in ~30 % of buurten (4,348 of 14,317 in 2022).
   Those were *simulated* (falling back to gas) but *excluded* from the observed average.
   → **Fixed in the engine:** `-Dht.buurtFilter=<file>` (see `StockLoader.loadBuurtFilter`) restricts
   the simulation to a list of buurtcodes. `calibrate_weights.py` generates
   `calibration_buurten.txt` (buurten complete in **both** 2022 and 2024) and passes it, so the engine
   simulates exactly the buurten being scored. Done engine-side rather than by writing a filtered
   stock copy, because the NL stock CSV is ~790 MB. The flag is general-purpose — any run can be
   restricted to a neighbourhood subset this way.
3. **Shares not summing to 100 %.** For *complete* buurten the five modelled shares sum to
   **97.4 % mean / 99.0 % median**; adding `Type installaties onbekend` (mean 2.4 %) gives **99.8 %**
   — i.e. the data is essentially complete, the remainder is the unknown category.
   (`Totaal zonder gasverbruik (ex. onbekend)` is a **subtotal** and is correctly never read.)
   → **Fixed:** the 2022 initial state renormalises the five shares to sum to 1, distributing the
   unknown category proportionally, matching how the observed score is normalised. Without this the
   engine dumped the shortfall onto the gas boiler (`while (total < households) reqNGB++`).

   > An earlier note in this doc claimed the shares "sum to 0.68 on average". That statistic averaged
   > in the suppressed buurten and was misleading — for usable buurten the data is ~100 % complete.

**Result (Limburg, after the fixes):** every system matches within **0.1 %pt** at the start year:

| system | observed 2022 | simulated 2022 | diff |
|---|---|---|---|
| gas boiler | 89.0 | 88.9 | −0.1 |
| gas block | 5.8 | 5.9 | +0.1 |
| hybrid HP | 1.2 | 1.2 | −0.0 |
| electric HP | 2.6 | 2.6 | +0.0 |
| district heating | 1.4 | 1.4 | −0.0 |

Residual sources (now negligible): integer rounding per neighbourhood, and block lumpiness (whole
social/HOA blocks take a single system). The gate therefore now behaves as a genuine identity check —
any deviation beyond ~0.5 %pt points at a real initialisation problem.

### Why buurten drop out, and why `Indelingswijziging` is *not* the fix

`Tabel 2` carries an `Indelingswijziging` column (codes 1 / 2 / 3) flagging boundary or coding
changes — municipal mergers and re-coded buurten. It is a plausible explanation for buurten missing a
year, so it was tested. It does **not** work as a discriminator:

| | count | codes present |
|---|---|---|
| buurten missing ≥1 year | 1,809 | `{1}` 772 · `{1,2}` 545 · `{3}` 311 · `{1,3}` 179 |
| buurten in all 3 years | 13,541 | `{1}` 11,909 · `{1,3}` 1,501 · `{1,2}` 105 |

772 of the *missing* buurten carry only code `1` (the same as the overwhelmingly stable majority),
while 1,606 buurten that are present in all three years carry a `2` or `3`. So the code cannot cleanly
identify reclassification, and buurtcode-remapping across years would be guesswork.

**More importantly it is not the dominant cause.** Of the 13,541 buurten present in all three years,
**4,224 still have suppressed (privacy) cells** in at least one year, leaving **9,317 fully usable**.
So suppression (4,224) outweighs reclassification (1,809): even perfect `Indelingswijziging` handling
would recover at most ~1,800 buurten and the completeness filter would still be required.

**Decision:** don't chase it. 9,317 fully usable buurten nationally is an ample calibration sample,
the filter is unbiased with respect to the *behaviour* being calibrated (suppression tracks small
buurten, not heat-pump propensity), and the start-year identity check confirms the resulting setup is
consistent. Worth revisiting only if a much larger sample is ever needed.

---

## 4. How to run

```powershell
# 0. one-off: extract the observed series + the 2022 initial state
python model\data-export\scripts\export_observed_heating.py

# 1. one-off: provision the region's stock
python model\run.py --scope province:Noord-Brabant --scenario baseline --iterations 1

# 2. how far are the AL defaults from observed? (also runs the start-year gate)
python results_analysis\calibrate_weights.py evaluate --scope province:Noord-Brabant

# 3. plausible-weight ensemble (LHS + retain within tolerance)
python results_analysis\calibrate_weights.py search --scope province:Noord-Brabant `
    --samples 60 --iterations 2 --tolerance 2 --outdir results\calib

# 4. Morris screening: which weights actually matter
python results_analysis\calibrate_weights.py morris --scope province:Noord-Brabant `
    --trajectories 8 --levels 4 --iterations 2 --outdir results\calib
```

```powershell
# 5. run the 2024-2050 scenarios across the retained weight sets (the "explore" step) --
#    representative bracket, or `all` for the full ensemble (needs many --iterations; size with
#    results_analysis/pathway_convergence.py):
python model\run.py --scope province:Noord-Brabant --scenario all --iterations 10 --analyze --weights representative

# 6. compare the weight sets as a BAND (per scenario: summary CSV + min-max band figure across sets):
python results_analysis\compare_weight_sets.py --scope province:Noord-Brabant --scenario baseline
```

`compare_weight_sets.py` only *reads* the runs `run.py --weights` already wrote under
`results/<scope>/calib/<label>/` — it does not re-simulate, so run it any time after the scenario
runs finish. It globs every `calib/*/` set, so the same command covers 2 representative sets or the
full 20-set ensemble. Options: `--scenario`, `--year` (summary year, default 2050), `--ownership`,
`--dir` (point at a calib folder directly).

Weights are passed to the engine at runtime (`-Dht.<name>=<value>`, see `Constants.p`) so **no
recompile per sample**. `-Dht.nbhHeating=nbh_heating_2022.csv` selects the historical initial state.
Weights screened: `wAttitudeToIntention`, `wSocialnormToIntention`, `wPbcToIntention`,
`wAffordabilityToPbc`, `wEffortToPbc`, `wIntentionToBehavior`.

### What `--trajectories` means (Morris sampling)

Morris is a **one-at-a-time screening design repeated from many random starting points**. One
*trajectory* is:

1. pick a random starting point on a discretised grid of the weight box (`--levels`, default 4
   levels per weight);
2. step **one weight at a time**, in random order, each by a fixed amount Δ, until all *k* weights
   have been moved once.

Each step yields one **elementary effect** for that weight: EE = (change in model error) / (change in
that weight). One trajectory therefore costs **k + 1 runs** (here 6 weights + 1 = **7 runs**) and
gives one EE per weight. Repeating over `r` trajectories gives *r* elementary effects per weight,
sampled from different regions of the space, which are summarised as:

- **µ\*** — mean of |EE| = overall **influence** (the ranking you care about);
- **µ** — mean of EE = direction of effect;
- **σ** — spread of EE = **interaction / non-linearity** (a weight whose effect depends on where you
  are in the space has large σ).

**Why 8?** Total cost is `r × (k + 1)` runs, so r trades precision against runtime. The screening
literature typically uses **r = 4–10** (SALib's default is in the same range); below ~4 the ranking
is unstable, above ~10 you pay a lot for little extra ranking accuracy — and Morris is only meant to
*rank* factors, not quantify them precisely (that is Sobol's job). **r = 8** sits at the reliable end
of that range: 8 × 7 = **56 model runs** ≈ 26 min on Noord-Brabant. Use `--trajectories 4` for a
quick look, and if the top of the µ\* ranking is stable between r = 4 and r = 8 you can trust it.


### What is varied: normalised SHARES, not raw weights (2026-07-29)

**Is the normalisation in the TPB implementation defensible?** Partly. Ajzen's TPB is usually a plain
linear combination; dividing by the sum of weights is AL's choice, and it buys a real advantage —
intention, PBC and perceived utility all stay in [0, 1], directly comparable with their inputs. What
it costs is the *overall strength* of the TPB predictor: you cannot make deliberation matter more in
absolute terms by scaling all weights up. **That lever isn't lost, it moved**: it now lives in
`GUMBEL_SCALE_UTIL` (`rumScore = utility + scale × Gumbel`), which sets how deterministic the choice
is versus idiosyncratic noise. If "how much does deliberate reasoning drive the decision" is a
question of interest, that is the parameter to screen — not the weight magnitudes.

Given the normalisation, the honest reporting unit is the **share** ("attitude 25 %, norm 50 %,
control 25 %"), which is also how TPB weights are conventionally read. So the sampler works in shares
too — the sampled parameter, the engine input and the reported number are then the same quantity.

**Why this beats sampling raw weights.** The first screen (6 raw weights) returned σ > µ\* for every
factor with µ ≈ 0. That is not noise — the engine seeds deterministically per iteration
(`new Rng(1 + it - 1)`), so the objective is a *deterministic* function of the weights. Two causes:

1. **Redundancy.** Scaling a whole group changes nothing, so part of the sampled space is a null
   direction — the intention group has 3 weights but 2 degrees of freedom, the PBC group 2 but 1.
2. **Inconsistent step size** (the more damaging one). A Morris step of Δ = 0.3 is a large
   compositional change when the group's weights sum to 0.6 and a tiny one when they sum to 3.0 — so
   identical steps produced wildly different behavioural changes, inflating σ and cancelling µ.

Sampling shares fixes both: a step of 0.05 always means the same reallocation, and there is no null
direction left. Because the engine divides by the group sum, **shares are passed straight through as
weights** and the normalisation becomes a no-op.

| group | sampled | residual |
|---|---|---|
| intention | `shareAttitude`, `shareSocialnorm` | `sharePbc = 1 − the two` |
| PBC | `shareAffordability` | `shareEffort = 1 − it` |
| behaviour | `shareIntention` | `sharePbcBeh = 1 − it` |

**4 free parameters.** Defaults reproduce the AnyLogic weights exactly (1/3 · 1/3 · 1/3;
0.714/0.286; 0.5/0.5) — verified: the objective at defaults is identical (1.18 %pts on Limburg)
before and after the re-parameterisation. A minimum residual of 0.10 keeps every implied share
positive. `HT_RAW_WEIGHTS=1` restores raw-weight sampling for comparison.

**Read the effects as relative.** Within a group the shares are codependent by construction: raising
attitude's share necessarily lowers the others. An elementary effect therefore reads *"reallocate
weight toward X, away from the residual"* — the meaningful behavioural question, but it means the
within-group effects are linearly dependent and should be reported as a **ranking**, not as
independent contributions.

### Structural parameters held fixed — candidates for a second-stage sensitivity analysis

The calibration above varies only the **4 TPB decision-weight shares**. Several *structural* constants
that shape the tipping dynamics are currently held at their AnyLogic defaults but are legitimate
sensitivity-analysis inputs — some were flagged by inspecting result graphs (e.g. the sharp year-1→2
kink in gas subjective norm, and the strength with which social norm holds gas). All are already
runtime-overridable via `-Dht.<name>` (see `Constants.p`), so they can be screened with the same
Morris/LHS machinery without a recompile:

| parameter | `-Dht` name | default | what it controls | why it matters |
|---|---|---|---|---|
| Salience novelty midpoint | `salienceThreshold` | 0.30 | share below which a technology is treated as "novel" and gains social salience | sets **when an incumbent's social-norm lock-in releases** — gas subjective norm collapses once its share falls through this threshold |
| Salience novelty sharpness | `salienceK` | 10.0 | steepness of the novelty logistic in *share* | how abruptly the novelty boost switches on/off around the threshold |
| Salience momentum sharpness | `salienceSteepness` | 30.0 | steepness of the momentum logistic in *Δshare* | **directly responsible for the non-physical year-1→2 kink** in subjective norm: at 30 the momentum term flips almost like a step around Δshare≈0, producing a sharp drop→flat→re-accelerate shape. A softer value smooths the transition |
| Choice noise (blocks/landlords) | `gumbelScaleEac` | 20 | € scale of the Gumbel noise added to each option's EAC before the cheapest is chosen | how much collective/landlord choices scatter off the pure cost optimum; too small and near-identical cohorts all pick the same technology (contributes to the HOA end-of-life "cohort echo") |
| Choice noise (homeowners) | `gumbelScaleUtil` | 0.02 | scale of the Gumbel noise on homeowner utility | homeowner choice dispersion |
| Lifetime jitter | `lifetimeJitterSd` / `lifetimeJitterMax` | 1.0 / 3 | spread of the stochastic end-of-life age `N(lifetime, sd)` clamped to `±max`, all technologies | **now on by default** (removes the synchronised replacement cohorts / HOA echo); `sd=0` recovers deterministic AnyLogic lifetimes. Its magnitude is a sensitivity input |
| Social-learning rate | scenario `SLF` (LOW/MED/HIGH) | MEDIUM | how fast salience feeds back into adoption | already a scenario axis; note Morris/LHS hold it at MEDIUM |

These are **structural**, not behavioural-weight, parameters, so they belong in a *separate* screening
(ideally after the weight ensemble is fixed) rather than being folded into the 4-parameter weight
calibration — varying them together would confound "which weights fit" with "how sharp is the
salience curve". A sensible second-stage design: Morris over `{salienceThreshold, salienceK,
salienceSteepness, gumbelScaleEac}` at the retained-ensemble median weights, scored on both the
2022–24 fit and the 2050 pathway spread. The salience-curve constants in particular are the lever
behind two graph artefacts already observed (the subjective-norm kink and the strength of gas
social-norm persistence), so they are the highest-priority structural factors to screen.

### Morris results (8 trajectories, 40 runs, Noord-Brabant, init 2022, scored 2023+2024)

**What the three numbers are.** An *elementary effect* is `Δ(objective) / Δ(sampled coordinate)`,
where the objective is the calibration MAD **in %-points** and the coordinate is normalised to [0, 1]
over that factor's sampled range. So:

- **µ\*** = mean \|EE\| = **how many %-points the MAD moves if the share is swept across its whole
  sampled range** — the influence ranking.
- **µ** = mean signed EE = the *direction* (positive ⇒ increasing that share makes the fit worse).
- **σ** = spread of the EEs = interaction / non-linearity (how much the effect depends on where the
  other shares are).

**Translating to the codependent shares.** Because µ\* is *per full range* and the ranges differ
(0.45 vs 0.60 wide), the raw µ\* partly reflects range width. Converting to a common unit —
**%-points of MAD per 0.1 of share** — is the fair comparison:

| factor (share ↑, at the expense of its residual) | sampled range | µ\* (per range) | **per 0.1 share** | direction |
|---|---|---|---|---|
| **shareAffordability** (residual = shareEffort) | [0.30, 0.90] | 3.29 | **0.549** | + worse |
| **shareSocialnorm** (residual = sharePbc) | [0.15, 0.60] | 1.86 | **0.412** | **− better** |
| shareIntention (residual = sharePbcBeh) | [0.20, 0.80] | 2.39 | 0.399 | + worse |
| shareAttitude (residual = sharePbc) | [0.15, 0.60] | 1.31 | 0.291 | + worse |

Note the ranking **changes** once normalised: `shareSocialnorm` and `shareIntention` swap, because the
former was sampled over a narrower range. Use the per-0.1-share column for interpretation.

**How to read a row.** "shareAffordability, per 0.1 share, +0.549" means: *shifting 10 percentage
points of PBC weight from effort onto affordability (cost) worsens the fit by about 0.55 %-points of
MAD, on average across the sampled space.* Each effect is a **reallocation** between a component and
its residual partner — never an isolated change, since the shares in a group sum to 1.

**Substantive reading.**
- **Cost-weighting in PBC is the strongest single lever, and it hurts.** Heat pumps have high EAC, so
  a cost-heavy PBC suppresses precisely the adoption the model already under-predicts.
- **Social norm is the only lever that improves the fit** (µ < 0): more peer influence → more
  heat-pump uptake → closes the standing gap.
- Together these say the same thing from two directions: to match 2022–24, the model needs *less*
  cost-dominance and *more* social influence than the AnyLogic defaults provide.
- **The weights matter a lot relative to the target.** Sweeping one share moves the MAD by 1.3–3.3
  %-points against a baseline objective of ~1.18 — the data can genuinely discriminate between weight
  sets, which is encouraging for the history-matching step.

### The ranking is a property of the IMPLEMENTATION, not of the TPB constructs

A Morris µ\* measures the leverage of a *weight*, and that leverage is roughly

> (conceptual importance of the construct) × (**how much the underlying variable actually varies
> across the alternatives being compared**).

A weight on a variable that barely differs between heating systems has little leverage no matter how
important the construct is in theory. The four TPB inputs are implemented on very different scales:

| input | implementation | cross-alternative contrast |
|---|---|---|
| **attitude** | `1 − \|householdAttitude − sustainabilityScoreNorm\|`; sustainability 1/1/3/5/3 → normalised 0 / 0 / 0.5 / 1.0 / 0.5 | moderate (~0.5 spread) and **non-monotonic**: for a mean household (Beta(5,2) ≈ 0.71) hybrid (0.79) scores above all-electric (0.71) |
| **subjective norm** | `min(1, peerShare × (1 + salience))` | **largest**: gas ≈0.85 vs heat pumps ≈0.03 early, and it *moves* as adoption grows — this is the tipping feedback |
| **affordability** | `1 − eacNorm` on a **single global LOG cost scale** (see below) | proportional to the *relative* (%) cost gap between a household's options |
| **effort** | discrete `0.2` (keep current) / `0.8` (needs low-temp retrofit) / `0.5` | large, systematic **status-quo bias** (0.8 incumbent vs 0.2 heat-pump-with-retrofit) |

#### EAC normalisation: fixed 2026-07-29 (per-technology → single global scale)

**The problem found.** `eacNorm` was normalised against each technology's **own** population min/max
(`hs[t].minEAC/maxEAC`). That rescales every technology by its own spread, which erases the cost
*level* difference — and empirically it **inverted** it. Measured on Limburg 2025 (`HT_DIAG`):

| | min–max EAC | mean `eacNorm` | affordability `1−eacNorm` |
|---|---|---|---|
| gas boiler | 329 – 8,333 | 0.208 | 0.792 |
| hybrid HP | 301 – **12,629** | 0.150 | **0.850** ← *scored as more affordable* |

Hybrid is *more expensive* on average (~2,150 vs ~1,950 for homeowners) yet scored as **more
affordable**, purely because its range is stretched wider by a few expensive outliers.

**The fix (implemented, no flag — this is now the model).** Affordability is normalised on **one
global scale** shared by all technologies and all dwellings in the year, built from the options a
household could actually choose (`possible()`), reset annually. This is what the model always
intended: it preserves the *magnitude* of the cost difference, so a household whose options are
close together sees little cost discrimination, while a household whose options diverge sees cost
dominate. It also makes homeowner cost behaviour consistent with blocks and landlords, which choose
on raw EAC.

*After the fix* (same probe): gas `eacNorm` 0.131 → affordability **0.869**, hybrid 0.144 → **0.856**.
The ordering is now correct (gas cheaper ⇒ more affordable) and the gap is small *because the real
cost gap is small relative to the population range* — exactly the intended behaviour.

#### EAC scale made logarithmic: 2026-07-30 (linear global → log global)

**The remaining problem.** The single global scale (above) fixed the inversion, but a probe of the
window (`HT_EACPROBE`, Limburg 2025) showed the *linear* range was still dominated by dwelling
**size**, not by which option is cheaper:

```
EACPROBE 2025  global window [301, 13205]  width 12904   (linear)
  EAC percentiles: p1=761  p5=1014  p25=1522  p50=2041  p75=2728  p95=4210  p99=5805
```

p99 (5,805) is under half the max (13,205): a handful of very large/expensive dwellings set the whole
scale. Consequently a household's own choice-relevant spread — *which of my options is cheaper* —
occupied only **~6–13 %** of the scale, while the dwelling-size signal (small→large) spanned ~**40 %**.
So "affordability" mostly encoded *how big is my house*, not *which option should I pick*. Small
dwellings, whose euro spread is small but whose **proportional** spread is large, were flattened to
near-zero cost discrimination — precisely the local-vs-global imbalance flagged in review.

**The fix (implemented, no flag — this is now the model).** `eacNorm` is normalised on
`log(EAC)` between `log(globalMin)` and `log(globalMax)` (`Decision.normalizedLog`), same global
window, reset annually. Three reasons:

- **Proportional, not absolute.** People judge cost differences in **percent**, not euros (the
  behavioural-economics standard; TPB's *perceived behavioural control* is about felt, relative
  burden). A +40 % gap now reads similarly whether the dwelling is small or large.
- **Removes size dominance without discarding level.** The expensive right tail is compressed by the
  logarithm, so extreme dwellings no longer set the scale — yet absolute level is still present
  (a €6,800 option is still high on the scale), unlike a per-m² transform which would erase it.
- **No clamping.** Unlike a robust p1–p99 window, log needs no percentile cut-off, so the largest
  dwellings keep — rather than lose — their cost discrimination.

*After the fix* (same probe, now log-scale), own spread as a share of the scale rises sharply for the
small dwelling and the size gradient flattens:

| dwelling | area | EACs (gas / hybrid / electric) | own spread, **linear** | own spread, **log** |
|---|---|---|---|---|
| small | 20 m² | 957 / 732 / 1488 | 5.9 % | **18.8 %** |
| median | 134 m² | 2326 / 2263 / 3243 | 7.6 % | **9.5 %** |
| large | 500 m² | 5127 / 6800 / 5990 | 13.0 % | **7.5 %** |

The inversion check still holds — lower cost ⇒ higher affordability (small dwelling: hybrid €732 is
cheapest and scores highest, 0.765). Because the window's geometric mean (~€1,994) sits at the median
EAC (p50 ≈ 2,041), a median dwelling now centres near affordability ≈ 0.46 instead of being crammed to
the cheap end (~0.85) — a more even use of the scale that recalibration will re-weight.

**Consequences — everything downstream must be re-run.** Homeowner PBC for gas rose 0.786 → 0.840
(hybrid 0.677 → 0.682), widening the gas-vs-hybrid PBC gap from 0.109 to 0.158. On Limburg:
- baseline 2050 gas share **8.4 % → 14.9 %**;
- the calibration objective at AL defaults **1.18 → 1.99 %-points**, with electric HP now
  under-predicted by 3.4 %-points (was 1.7).

So the model is now *further* from observed 2022–24 at the default weights, which sharpens rather
than weakens the case for the calibration: the cost channel is now real and the defaults over-weight
it. **The Morris ranking below predates this change and must be re-run.**

**Caveats.** σ/µ\* remains > 1 for all factors (1.10 for the strongest, 1.83 for the weakest), so real
interaction/non-linearity is present and the ranking is *indicative*, not additive. At 8 trajectories
the middle two (0.412 vs 0.399 per 0.1 share) are **not** separated — treat them as tied. Confirm with
`--trajectories 16` or a second `--seed` before relying on that ordering.

> An earlier screen using raw (unnormalised) weights was run before the share re-parameterisation. Its
> elementary effects cancelled (\|µ\|/µ\* ≈ 0.1) because identical steps meant different things at
> different points in the space, so **no conclusions are drawn from it**; it is superseded by the run
> above and retained only as the motivation for switching to shares.

**Runtime** (calibration runs span only 3 years, so they are cheap): ≈14 s per run per MC iteration
for Noord-Brabant ⇒ Morris with 8 trajectories ≈ 26 min, LHS-60 ≈ 28 min.

---

## 4b. The LHS / history-matching step (Noord-Brabant)

**Why Morris is not enough.** Morris is a *screening* method: it perturbs one factor at a time along
a few trajectories and averages the resulting derivatives. That yields a **ranking and a direction**
— which levers matter, and which way they push — but never a *fitted value*, because it never
searches the space for points that reproduce the data. Locating plausible values is the LHS + filter
step's job.

**Why do it rather than jump to high/low scenarios around the defaults.** The AnyLogic defaults are
**known to be biased** (electric HP under-predicted by 1.7–3.2 %-points), and Morris says the two
strongest levers — cost-weighting in PBC, and social norm — both push in the direction that would
close that gap. Anchoring the whole pathway analysis on a base we know is off, then varying around
it, would bake the bias into every scenario. The filter is what establishes whether a *plausible*
weight region removes it. It is also cheap: calibration runs span only 3 years.

**The workflow (implemented in `search` mode):**

```powershell
python results_analysis\calibrate_weights.py search --scope province:Noord-Brabant `
    --samples 100 --iterations 2 --tolerance 2 --outdir results\calib
```

1. **Latin-hypercube sample** the 4 shares over their ranges (`sample_lhs`).
2. **Score** each set on 2023+2024 (same objective as Morris).
3. **Retain** the sets with `MAD <= --tolerance` — history matching, *not* a point fit. The AL
   default's own MAD is printed alongside, so "did we beat the default, and by how much" is explicit.
4. **Report the retained ranges** per share (min / median / max vs the default) — this is the
   defensible "plausible weights" statement for the write-up.
5. **Pick representative sets** — actual evaluated points, not synthetic medians: the **best fit**
   plus the retained **extremes of the dominant axis** (`--axis`, default `shareAffordability`, i.e.
   Morris' top lever). Printed complete with ready-to-paste `-Dht.…` flags and saved to
   `calibration_search.json`.

**Then run the 2050 pathways under those representative sets only** — the band costs 3 full scenario
runs instead of 100, which is what makes the ensemble approach affordable at 2024–2050 × 16 scenarios.

### Choosing the three settings (they are NOT arbitrary defaults — decide them explicitly)

**1. What "the default" is.** It is the AnyLogic weight set, expressed as shares. It is *not* "all
weights equal":

| group | AL weights | as shares | equal? |
|---|---|---|---|
| intention (att / norm / PBC) | 0.5 / 0.5 / 0.5 | 0.333 / 0.333 / 0.333 | yes |
| PBC (affordability / effort) | 0.5 / 0.2 | **0.714 / 0.286** | **no — cost weighted 2.5×** |
| behaviour (intention / PBC) | 0.5 / 0.5 | 0.500 / 0.500 | yes |

So AL is neutral within intention and behaviour, but deliberately **cost-dominant in PBC** — which is
precisely the lever Morris flags as the strongest and as pushing the fit the wrong way. `search`
prints the default's own MAD next to the best sampled MAD so the comparison is explicit.

**2. Retention tolerance — derive it, don't assume it.** `--tolerance 2` was a placeholder. Three
defensible bases, best first:

- **Relative to the achievable distribution** (standard GLUE practice): run the LHS, look at the MAD
  histogram, and retain the best *X* % (e.g. 10–20 %). This adapts to how well the model can actually
  do and guarantees a non-empty, non-trivial ensemble.
- **At least as good as the default**: retain everything with `MAD <= default MAD`. Directly
  defensible in writing ("no worse than the published parameterisation") but it can retain a lot.
- **Tied to observational error**: CBS publishes whole percentages, so there is a ≈0.5 %-point
  quantisation floor per buurt, plus the ~0.1–0.2 %-point residual visible in the start-year identity
  check. A tolerance below ~0.5 %-points would be fitting noise.

Recommended: run once, inspect the distribution, then set the threshold — and **state which basis was
used** in the write-up.

**3. Monte-Carlo replications — measured, not guessed** (`converge` mode).

*First, a clarification: the model is NOT deterministic in the sense of having no randomness.* Peer
networks, household attitudes, ownership draws and heat-demand factors are all **redrawn every
iteration** — each iteration is a different random world, exactly as intended. What is fixed is the
**seed sequence**: iteration *it* uses seed *it*. So a set of N iterations samples N different worlds,
and re-running the same configuration samples *those same* N worlds again.

That has two deliberate benefits and one risk:

- **Reproducibility.** Re-running a configuration reproduces it exactly, which is what makes
  debugging, regression checks and this document's verified numbers possible.
- **Common random numbers (CRN).** Two weight sets are compared on the *same* worlds, so the world-to
  -world variation cancels in the *difference* between them. This is a standard variance-reduction
  technique in simulation and it makes the Morris/LHS comparisons far more efficient than they would
  be with independent seeds — the ranking is not polluted by which worlds happened to be drawn.
- **The risk you correctly identified:** with a small N, a "calibrated" weight set could be tuned to
  those particular N worlds. The fix is not to unfix the seeds (that would only add noise) but to
  **validate on fresh worlds**: `-Dht.seedOffset=1000` (exposed as `--seed-offset`) shifts the seed
  sequence to an independent set. Recalculate the retained set's fit there — the stochastic analogue
  of an out-of-sample test. *Verified:* offset 0 reproduces exactly (gas 83.4 % twice), offset 1000
  gives a different world (83.6 %).

**The replication analysis itself** is the textbook procedure for stochastic simulation (Law &
Kelton, *Simulation Modeling and Analysis*; for ABMs specifically Lorscheid, Heine & Meyer 2012, and
Secchi & Seri 2017): run *n* independent replications, estimate the between-replication standard
deviation *s*, and choose *n* so the confidence-interval half-width on the mean meets the precision
the analysis needs:

> half-width = t(0.975, n−1) · s / √n  ⟹  **n_required ≈ (1.96 · s / target)²**

`converge` reports the per-world objective, the running mean, SD, standard error and 95 % CI
half-width, then inverts the formula for several target precisions:

```powershell
python results_analysis\calibrate_weights.py converge --scope province:Noord-Brabant --iter-list 1,2,3,5,8,12
```

*Illustrative output (Limburg, 3 worlds — too few to trust, shown for shape):* per-world objectives
1.184 / 1.095 / 0.843, between-world **SD ≈ 0.18 %-points**, implying n ≈ 2 for ±0.25 %-points and
**n ≈ 13 for ±0.1 %-points**. Choose the precision from what the analysis must resolve: it should sit
well below the retention `--tolerance` *and* below the differences between the weight sets you intend
to rank.

**The calibration and the 2050 runs need different iteration counts.** The calibration objective is a
3-year run aggregated over a whole province — hundreds of thousands of decisions, so it is
comparatively stable. A 2024–2050 pathway is path-dependent: the learning curve feeds cumulative
installs back into costs, so small early differences compound and tipping timing can diverge between
draws. **The scenario runs need more iterations than the calibration**, and that should be checked
separately on the spread of the 2050 outcome (not on the calibration objective).

**Runtime:** ~14 s per sample on Noord-Brabant (3-year runs, 2 MC iterations) ⇒ **LHS-100 ≈ 25 min**.

**Sample size.** With only 4 free parameters, 100 LHS points is comfortable coverage (~3 per
dimension per decile). Raise `--samples` if the retained set comes back very small; widen
`--tolerance` (or the share ranges) if nothing is retained at all.

---

## 5. Findings so far

- **The AL default weights under-produce heat-pump adoption.** Electric HP is under-predicted in
  *both* test regions (−1.7 %pts Utrecht, −3.2 Noord-Brabant). Two independent regions agreeing means
  this is a weights/model property, not a regional quirk — and it is exactly the gap calibration
  should close. Hybrid is close (+0.2/+0.3), so the shortfall is specific to all-electric.
- **Non-calibrated channels are reproduced well in Noord-Brabant** (DH +0.1, gas block +0.3), which is
  what makes it a clean calibration target.
- **District heating is over-assigned in DH-rich regions** (Utrecht +4.0 %pts). Suspected cause: the
  initial assignment hands whole social-housing blocks to DH preferentially, which is lumpy and can
  overshoot where DH is common. Not pursued further (Noord-Brabant is unaffected), but worth
  remembering if a DH-heavy region is ever used.
- **A first full Morris screen was run on Noord-Brabant (8 trajectories, 56 runs) but PRE-FIX** —
  before the aggregation/buurt-set/normalisation corrections above. It gave a flat, weakly-separated
  ranking (µ\* 2.29 → 0.98: `wEffortToPbc`, `wIntentionToBehavior`, `wAttitudeToIntention`,
  `wSocialnormToIntention`, `wAffordabilityToPbc`, `wPbcToIntention`) with σ ≥ µ\* for every weight —
  i.e. dominated by interaction/noise, which is what a contaminated objective looks like.
  **It must be re-run now that the start year matches**; treat the old ranking as void.
- Note µ ≈ −µ\* for `wAttitudeToIntention` (a consistently negative effect: raising it lowers the
  error), which is the kind of clean directional signal worth re-checking after the re-run.

---

## 6. Caveats for the write-up

- **3 years, subsidy-driven.** The window coincides with strong NL heat-pump subsidies and the post-
  2022 gas-price spike; weights fitted to it may absorb those transient drivers.
- **Non-identifiable.** Report a *retained ensemble band*, never a single "calibrated" weight vector,
  and state the identifiability limits explicitly.
- **Regional transfer.** Weights are constrained on one province; representativeness was checked on
  stock, heating mix and ownership, but not on income or attitudes (not in the model).
- **Not a validation of outcomes.** Matching 2022–24 does not validate 2050 projections; it only
  filters out behaviourally implausible parameterisations.

---

## 6. Comprehensive sensitivity analysis — parameter inventory, categories, and plan

The weight calibration (sections 4–4b) covers only **one kind of uncertainty**: how much each decision
criterion matters. The model contains many other tunable parameters that represent *different kinds*
of uncertainty, and lumping them into one Morris/LHS would confound them — e.g. mixing `salienceSteepness`
(how sharply the social process unfolds) into the weight sampling would blur "which preferences fit the
data" with "how fast does diffusion tip." This section inventories every parameter, sorts them by the
**type of uncertainty** they carry, and gives a staged plan that keeps the types separate.

### 6.1 Why separate by uncertainty type

A sensitivity analysis is only interpretable if each factor answers a well-posed question. Four
distinct questions are tangled in this model:

1. *What do households value?* — the TPB decision weights (preference/value uncertainty).
2. *How does the social diffusion process play out?* — salience curve, choice noise, network, social
   learning (behavioural-**dynamics**/mechanism uncertainty).
3. *What do the technologies cost and deliver?* — capex, learning rates, COP, discount, subsidy,
   lifetime (techno-**economic** uncertainty, estimable from engineering/literature).
4. *What macro futures unfold?* — energy prices, policy levers (exogenous **scenario** uncertainty).

These need different treatments (ensemble filter vs screening vs literature bounds vs discrete
scenarios) and, crucially, different **reporting**: a preference band, a process band, and a set of
price/policy scenarios are three separate statements, not one number.

### 6.2 Full parameter inventory

| parameter(s) | default | where | category | uncertainty type | proposed treatment |
|---|---|---|---|---|---|
| `shareAttitude, shareSocialnorm, shareAffordability, shareIntention` (4 weights) | AL: ⅓·⅓·⅓ / 0.71 / 0.5 | `Constants` | **A. Decision weights** | preference / value | **done** — LHS + history-matching ensemble (§4b) |
| `salienceThreshold` (0.30), `salienceK` (10), `salienceSteepness` (30) | — | `Constants` | **B. Behavioural dynamics** | mechanism / process | Morris screen at fixed weights → quantify top |
| `gumbelScaleEac` (20), `gumbelScaleUtil` (0.02) | — | `Constants` | **B. Behavioural dynamics** | mechanism (choice stochasticity) | Morris screen |
| `social_learning_rate` (per tech, 0.5–1.0) | data | `heating_system_data.csv` | **B. Behavioural dynamics** | mechanism | Morris screen (or fold into `SLF`) |
| network: `MEAN_SIZE` (25), `SHARE_LOCAL` (0.8), `SHARE_SIMILAR` (0.8), `NB_CAT` (6) | — | `Simulation.buildNetwork` | **B. Behavioural dynamics** | mechanism (network structure) | Morris screen; currently hard-coded → expose as `-Dht.*` first |
| attitude distribution `Beta(5,2)` | — | `StockLoader` | **B. Behavioural dynamics** | mechanism (population heterogeneity) | OAT on the shape (mean/variance); drives the Rogers front |
| `lifetimeJitterSd` (1.0), `lifetimeJitterMax` (3) | — | `Constants` | **B/C. Dynamics × economics** | mechanism (cohort smearing) | already exposed; include in Morris |
| investment costs S/M/H (per tech) | data | `heating_system_data.csv` | **C. Techno-economic** | engineering estimate | literature bounds (TNO/CE Delft) → Morris/OAT |
| `economic_learning_rate_per_unit` (0.01–0.10) | data | `heating_system_data.csv` | **C. Techno-economic** | estimate (drives capex fall) | **high priority** — the strongest scenario lever seen (ELF); bounds + Morris |
| `efficiency_primary` (COP 3 for HPs) | data | `heating_system_data.csv` | **C. Techno-economic** | engineering estimate | OAT (COP 2.5–4.5) |
| `discount_rate` (0.02 / 0.03 DH) | data | `heating_system_data.csv` | **C. Techno-economic** | estimate (household discounting) | OAT (1–7%); interacts with lifetime |
| `subsidy_eur` (HP 4000, DH 3775) | data | `heating_system_data.csv` | **C. Techno-economic / policy** | policy lever | **scenario** (ISDE schedule LOW/BASE/HIGH), not random |
| `lifetime_years` (12/15/30) | data | `heating_system_data.csv` | **C. Techno-economic** | estimate | OAT ±cohort; jitter already added |
| **gas price** (0.14 €/kWh) + real growth | static | `energy_source_data.csv` | **D. Energy prices** | exogenous market | **scenario LOW/HIGH path** (§6.5) |
| **electricity price** (0.32 €/kWh) + real growth | static | `energy_source_data.csv` | **D. Energy prices** | exogenous market | **scenario LOW/HIGH path** (§6.5) |
| `SLF, ELF, GRR, DHCT, DHES, SHAES, DHCO, GCHPB` | scenario | `Scenario` | **E. Policy / scenario levers** | decision, not uncertainty | the 16-scenario matrix (existing) |
| CBS-suppressed neighbourhoods → default gas (≈1.8% of stock), stock draw, `heatingYear` | — | `StockLoader` | **F. Initial-condition / data** | data uncertainty | robustness checks, not sampled |

### 6.3 The categories and their methods

- **A — Decision weights.** *Done.* History-matching ensemble; reported as a band. Preference
  uncertainty. Nothing to add.
- **B — Behavioural dynamics.** The largest *new* block and the one the calibration cannot touch (it
  is orthogonal to "which weights fit"). Strongly interacting and non-additive (Morris σ ≫ µ\* expected),
  so **screen with Morris, do not fold into the weight LHS**. Two of these (`salienceSteepness`,
  `gumbelScaleEac`) are already implicated in graph artefacts (the subjective-norm kink; the cohort echo).
  The network parameters are currently hard-coded and must be exposed as `-Dht.*` before they can be
  screened.
- **C — Techno-economic.** Estimable from engineering/literature, so they get **bounds, not priors**.
  `economic_learning_rate` is the priority — the ELF scenario already showed it swings 2050 gas from
  ~5% to ~14%, so its *continuous* uncertainty deserves quantification, not just LOW/HIGH. Subsidy is a
  **policy lever** → treat as a scenario (ISDE), not a random factor.
- **D — Energy prices.** Exogenous futures → discrete **scenario paths**, not continuous sampling
  (§6.5). Report which conclusions are price-robust.
- **E — Policy/scenario levers.** Already the 16-scenario matrix; these are *choices*, reported as
  scenarios, never mixed into uncertainty sampling.
- **F — Initial-condition/data.** Handled by targeted robustness checks (e.g. the 1.8% gas-default
  sensitivity, a `heatingYear` swap, multi-seed stock draws), not by parameter sampling.

### 6.4 Staged plan (order matters, to avoid confounding)

**Scope decisions (2026-07-30).** Held **fixed** (good assumptions or low interest): `efficiency`
(COP), `discount_rate`, `lifetime` (the ±3 jitter already covers spread), `subsidy` (fixed policy
assumption), and the network parameters (`MEAN_SIZE`, `SHARE_LOCAL/SIMILAR`) — for now. **Energy price
is one *coupled* factor** (gas and electricity move together, since the power price is gas-linked at the
margin): a single axis from both-fall (LOW) to both-rise (HIGH), not a 2×2 grid. Screening is run in
the **baseline scenario only** to keep it attainable (the policy scenarios A–E are a separate axis).

**Retained structural factor set (9):** `salienceSteepness`, `salienceThreshold`, `salienceK`,
`gumbelScaleEac`, `gumbelScaleUtil`, `lifetimeJitterSd` (category B), `capexMultHp`, `learningRateMult`
(category C), and the coupled `energyPrice` (D). All are `-Dht`-overridable (capex and learning-rate
multipliers were exposed for this).

**Method — Morris first, THEN LHS on the survivors (not straight to a presumed 5).** Screening which
factors matter is an empirical question, so do not presuppose the key five. Same two-stage logic as the
weight calibration:

1. **Fix the preference band (A).** Hold the decision weights at the **retained-ensemble median**
   (`structural_sensitivity.py` does this automatically), so B/C/D are screened on a fixed, plausible
   preference setting rather than entangled with weight sampling. Optionally re-run at 2 edge weight
   vectors to confirm the ranking is weight-stable.
2. **Structural Morris screen over all 9 factors** at the median weights, baseline scenario, scored on
   the full **2050 modal split** — the script reports a factor × technology µ\* matrix (gas / hybrid /
   electric / DH) plus a TOTAL = Σ|µ\*| over the mix, so a factor that swaps hybrid↔electric without
   touching gas still registers. (The 2050 outcome, not the 2022–24 fit, is the right score: a factor
   can be fit-irrelevant yet pathway-decisive.)
   `python structural_sensitivity.py morris --scope province:Noord-Brabant --trajectories 8 --iterations 1`
   → µ\* ranking. Drop the inert factors.
3. **LHS/Sobol on the ~5 survivors** to quantify their share of 2050 variance (standardized regression
   coefficients now; Sobol from the same CSV later):
   `python structural_sensitivity.py lhs --factors <survivors> --samples 80 --iterations 5`
4. **Report in tiers, never merged:** (i) preference band from A; (ii) process/economic band from the
   structural survivors (B/C); (iii) coupled energy-price scenario (D) and the policy matrix (E). The
   headline is *which tipping conclusions survive all three*.

**Iterations — the 1-vs-more point clarified.** `converge` found 1 iteration suffices for the
*2022–24 calibration* objective (between-world SD 0.028 %pts). The structural screen scores the *2050
pathway*, which is ~15× noisier (SD ≈0.4 %pts, `pathway_convergence.py`). But for **Morris ranking**
that noise is still tiny next to the effects (capex/price move shares 10–25 %pts), so **`--iterations 1`
is fine for the screen** and keeps it cheap: a 9-factor Morris at r=8 is (9+1)·8 = 80 runs ≈ 25 min at
NB. **LHS quantification needs `5–10`** — SRC/Sobol estimate variance, which 0.4 %pt noise attenuates
(the script warns if lhs is run below 5). Outputs: `structural_morris.json`, `structural_lhs.csv`.

**Why baseline only (and when to cross-check).** Baseline is the *right* isolation: it pins every
policy switch at neutral, so the structural factors are screened cleanly. Several of the 16 scenarios
would **confound** the screen because they themselves move structural parameters — `individual_/
collective_technologies` and the `economic/social_learning_factor_*` scenarios set ELF/SLF, i.e. the
same learning/salience channels the factors vary. So do **not** screen across those. If you want to
confirm the *ranking is policy-stable*, re-run the Morris on one or two **pure-policy** scenarios that
change only flags, not B/C params — e.g. `dh_policy_based_connection_obligation` or
`grid_congestion_HP_ban` (`--scenario ...`) — as a robustness check, not the main screen.

### 6.5 Energy prices — implementation + test (was Q5)

**Current state (verified).** Prices are **static**: `Economics.computeEAC` uses a fixed
`primaryCostPerKWh` (gas 0.14, electricity 0.32 €/kWh) for every year and every point in the EAC
horizon; the `cost_trend` column in `energy_source_data.csv` is read nowhere. So today the model has no
gas-vs-electricity price dynamics at all — a material simplification given the spark spread drives
HP-vs-gas economics.

**Implemented (2026-07-30).** A per-fuel real annual growth rate is applied to the **retail** price:
`price(y) = p0·(1+g)^(y−startYear)`, set each simulation year before any EAC is computed (myopic: that
year's price is held flat over the equipment horizon). `Constants.GAS_PRICE_GROWTH` /
`ELEC_PRICE_GROWTH` (`-Dht.gasPriceGrowth` / `-Dht.elecPriceGrowth`), default **0 = static** (parity).
HEAT (district heating) follows the gas growth ("niet meer dan anders"); only gas and electricity are
varied. Base prices are the frozen `initial*` on each `HeatingSystemSpec`.

**Bounds — KEV 2025 Table 10** (wholesale, constant-2024 prices; 2030 bandwidth → LOW/HIGH, trend
extrapolated to 2050). The model prices are **retail**, so the wholesale 2030 bandwidth is passed
through **additively on the commodity component** (energy tax + network + VAT held fixed), then
converted to an effective retail CAGR (gas 1 m³ = 9.769 kWh; retail base gas 0.14, electricity 0.32
€/kWh):

| fuel | KEV wholesale 2024 → 2030 band | retail 2030 (LOW/HIGH) | effective retail CAGR |
|---|---|---|---|
| natural gas | 0.23 €/m³ → **0.15 / 0.46** | 0.132 / 0.164 €/kWh | **−1.00 % / +2.62 %/yr** |
| electricity | 77 €/MWh → **48 / 105** | 0.291 / 0.348 €/kWh | **−1.57 % / +1.41 %/yr** |

*(Applying the wholesale CAGR directly to retail would be wrong — retail gas would rise ~20× by 2050 —
so the additive-commodity passthrough is the defensible choice. ETS2 from 2027 is the main upside-gas
risk and sits inside the HIGH path.)*

**Run the 2×2 corner grid** (via `run.py --prop`, which injects the `-D` flags; combine with
`--weights` to sweep the ensemble):
```powershell
# max HP advantage (gas dear, power cheap):
python run.py --scope province:Noord-Brabant --scenario all --iterations 10 --analyze `
    --prop ht.gasPriceGrowth=0.0262 --prop ht.elecPriceGrowth=-0.0157 --out results
oord-brabant\price_gasHI_elLO\simulation_results.csv
# min HP advantage (gas cheap, power dear):
python run.py --scope province:Noord-Brabant --scenario all --iterations 10 --analyze `
    --prop ht.gasPriceGrowth=-0.0100 --prop ht.elecPriceGrowth=0.0141 --out results
oord-brabant\price_gasLO_elHI\simulation_results.csv
```

**Demonstrated effect (NB baseline, 1 iter, 2050 TOTAL mix)** — the spark spread is decisive and
*reorders* the low-carbon winner, not just the gas residual:

| price path | gas | hybrid HP | electric HP | DH |
|---|---|---|---|---|
| static (current model) | 11.0 % | 60.6 % | 23.0 % | 5.3 % |
| gas HIGH + elec LOW | **0.4 %** | 43.8 % | **54.0 %** | 1.9 % |
| gas LOW + elec HIGH | **36.3 %** | 35.9 % | 17.1 % | 10.7 % |

Cheap power + dear gas drives **full electrification** (electric HP 23→54 %, gas ≈ 0); dear power + cheap
gas keeps a **third of dwellings on gas** and lifts DH. So energy price is at least as decisive as the
weight uncertainty, and it flips the hybrid-vs-electric question — confirming it was a major missing
driver. Report the **spark spread** (gas:electricity, and per-useful-kWh after COP) as the summary
variable and state which tipping conclusions survive the price grid.

**Priority.** Wiring done; the outstanding work is running the price grid across the retained weight
ensemble and folding the result into the tiered report (preference band × price scenarios).
### 6.6 Why the economic factors dominate — a structural reading (preliminary)

The first structural Morris (median weights, baseline, 80 runs) puts three techno-economic factors —
`energyPrice`, `learningRateMult`, `capexMultHp` — in a clear top tier (µ\* ≈ 24–26), roughly double any
behavioural-dynamics factor. That is not an artefact; it follows from four features of the decision
structure. (Numbers refer to `Decision.java`.)

**1. Cost enters the utility *twice* — it is structurally over-represented.** The RUM chain is
`pbc = ((1−eacNorm)·wEac + (1−effort)·wEffort)/(…)`, then `intention = (att·wAtt + sn·wSn + pbc·wPbcToInt)/(…)`,
then `perceivedUtility = (intention·wInt + pbc·wPbc)/(…)`. Affordability (`1−eacNorm`) sits inside `pbc`,
and **`pbc` reaches the final utility through two paths** — via `intention` *and* directly in
`perceivedUtility`. Attitude and subjective norm reach utility through the single `intention` path only.
So the cost term has a built-in leverage advantage over the other TPB constructs, independent of the
weights.

**2. All three economic factors converge on the *same* most-leveraged variable, `eacNorm`.** Energy
price moves the operating cost (`energyPerYear`), capex moves the investment term, and the learning rate
moves the capex *trajectory* — but all three feed `EAC → eacNorm`, which is the doubly-weighted input
from point 1. They are three levers on the one variable the decision is most sensitive to, so their
effects stack on the same channel rather than dispersing.

**3. The learning rate is the *gain on a reinforcing feedback loop*, so it compounds over 26 years.**
`capexLearningFactor = (1−rate)^doublings` with `doublings = log₂(cumInstalled/initialUnits)`: more heat-pump
installs → more doublings → lower capex → lower EAC → higher `pbc`/utility → more installs. `learningRateMult`
scales `rate`, i.e. the loop gain — and a small gain change is amplified by the whole 2024–2050 trajectory
of the loop. This is why it is not just top-tier but specifically the lever on the **hybrid-vs-electric
endpoint** (µ\* hybrid 10, electric 12, gas 3): the loop runs hardest for whichever heat pump is winning,
so faster learning tips the system toward *full* electrification. Salience is also a feedback (social
proof → subjective norm), but its gain is the social-norm weight, which the calibration set to a moderate
~0.28 — a weaker loop than the learning loop acting on the doubly-weighted cost channel.

**4. The economic factors move their input by large, *compounding*, always-on amounts.** The price path
compounds annually (gas +2.62 %/yr ≈ ×2 by 2050); capex and learning act every year and through the loop;
and because affordability is normalised on the **log-EAC scale** (§6.5/§log-fix), proportional cost shifts
map directly onto `eacNorm`. Since the hybrid-vs-gas EAC is close for many dwellings, a modest shift in
price or capex flips the cheaper option for a large sub-population → big share swings. The behavioural-shape
parameters (salience threshold/K/steepness, lifetime jitter) reshape *how* the response curves but within
bounded, largely one-off effects; the choice-noise scales (`gumbelScale*`) add dispersion around the
cost-optimal choice (hence mid-tier, not top-tier) rather than moving the mean trajectory.

**The subtle point — sensitivity is not the same as preference weight.** The calibration actually
*down-weighted* affordability (best_fit puts cost at ~39 % of PBC, below the AnyLogic 0.71), i.e.
households care about cost *less* than AL assumed. Yet the cost *drivers* are the most influential on the
2050 outcome, because sensitivity ≈ (weight) × (how far the factor moves the input) × (feedback
amplification), and the economic factors dominate the last two terms even at a modest weight. So "economics
gate the pathway" is a statement about the **forcing and the feedback**, not about households being
cost-obsessed.

**Caveats.** This is one weight vector (the median) and the baseline scenario. Whether the ranking holds
is being checked at the `high_shareAffordability`/`best_fit` vectors; the salience factors in particular
should rise where the social-norm weight is higher (the weight×structural interaction). Magnitudes will be
quantified by the LHS/Sobol step. Read this as the *mechanism* behind the preliminary ranking, to be
confirmed by the full run.
