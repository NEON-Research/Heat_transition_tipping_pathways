# Calibration, validation and sensitivity analysis

How the model's uncertain parameters are constrained, what was found, and how to reproduce it.

The model contains two distinct kinds of uncertainty and they are treated separately throughout:

| | uncertainty | constrained by | reported as |
|---|---|---|---|
| **A. Preferences** | the four TPB decision-weight shares | observed 2022–24 adoption (history matching) | an ensemble **band** |
| **B/C. Structure** | salience curve, choice noise, HP capex, learning rate, grid | *not* identifiable from 3 years of data | a **sensitivity ranking** at fixed weights |
| **D/E. Exogenous** | energy prices, policy levers | scenario assumptions (KEV, policy design) | discrete **scenarios** |

Merging these would conflate "what households value" with "how fast the technology learns" and "what
the gas price does". Sections 2–4 cover A; section 5 covers B/C; section 6 covers D.

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
---

## 2. Method: history matching, not a point fit

Three years of subsidy-driven data cannot identify four weights — many combinations fit equally well
(equifinality). Rather than force a point fit, the workflow is **bound → sample → filter → explore**
(history matching / GLUE-style exploratory modelling), which matches the project goal of mapping
*tipping pathways under plausible weights* rather than finding "the true" weights:

1. **Bound** each weight with a plausible range around the AnyLogic defaults.
2. **Sample** the space with a Latin hypercube.
3. **Filter**: retain the weight sets whose simulated 2022→24 change lands within a tolerance of
   observed (default MAD ≤ 2 %pts); discard the rest.
4. **Explore**: run 2024–2050 scenarios across the *retained ensemble* and report a band.

### 2.1 What is scored

The engine is initialised in **2022 from the observed 2022 state** (`-Dht.heatingYear=2022`), run to
2024, and each weight set is scored by the **mean absolute deviation** of its simulated 2023 *and*
2024 provincial technology mixes from observed. MAD is preferred over RMSE because with five
technologies there is no outlier problem to solve, and squaring would amplify Monte-Carlo noise and
make the Morris ranking jumpier. Scoring is at **province level**: that is where the CBS data is
trustworthy (buurt values are rounded and privacy-suppressed), and it is adequate for ranking weights.
A **start-year gate** runs in every mode and warns if the initialised mix deviates from observed by
more than `--start-tol` (default 2 %pts); it currently passes within 0.1 %pts on every technology.

### 2.2 Monte-Carlo replications — measured, not guessed

`converge` runs the replication analysis (Law & Kelton; for ABMs, Lorscheid et al.): between-world SD
of the objective is **0.028 %pts**, so even ±0.1 %pts precision needs only **n = 1** for the 3-year
calibration objective — the province-wide aggregate over ~1.19 M dwellings averages out per-agent
stochasticity. This does **not** transfer to the 2024–2050 pathways, which are path-dependent and
~15× noisier (SD ≈ 0.4 %pts); `pathway_convergence.py` sizes those separately (≈9–10 iterations for
±0.25 %pts, driven by the hybrid heat pump).

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

### 3.1 Start-year initialisation matches observed

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

### 3.2 Why buurten drop out

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
---

## 4. Calibrating the decision weights

### 4.1 What is varied: normalised shares, not raw weights

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

### 4.2 What `--trajectories` means (Morris sampling)

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

### 4.3 The LHS / history-matching step

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

### 4.4 Choosing the three settings (decide them explicitly, they are not defaults)

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
### 4.5 Result: the retained ensemble

100 LHS samples, 2022–24, Noord-Brabant:

- AL default weights score **MAD 2.21 %pts** — *above* the 2.0 tolerance, so the defaults themselves
  are not retained. The best sampled set scores **0.21**, a ~10× better fit well inside the plausible
  box.
- **20 of 100 sets retained** — a healthy history-matching yield: not empty (which would mean the
  ranges are wrong) and not everything (which would mean the data cannot discriminate).
- The ensemble pulls **affordability down** (default 0.714 → median 0.621) and **effort up**
  (0.286 → 0.430): cost matters *less* to households than the AnyLogic defaults assume.
- **Every retained range is wide** — equifinality confirmed. The deliverable is a band, never a single
  "calibrated" vector.

### 4.6 Which weights matter (Morris)

8 trajectories, 40 runs, at the retained-ensemble settings:

| weight | µ* (influence) | µ (direction) | σ (interaction) |
|---|---|---|---|
| shareAffordability | **2.74** | +2.47 | 3.43 |
| shareSocialnorm | 2.29 | −1.83 | 3.39 |
| shareIntention | 2.12 | −0.48 | 2.86 |
| shareAttitude | 1.49 | +1.25 | 3.64 |

Affordability is the strongest lever and pushes the fit the *wrong* way (µ > 0: raising the cost
weight increases error), confirming the defaults over-weight cost. Social norm is the strongest
*improving* lever. σ > µ* for every factor, so the response surface is non-additive — the weights
cannot be tuned one at a time, which is precisely why the LHS-and-filter approach is used. At 8
trajectories the middle two are not separated; treat their order as tied.

### 4.7 Why the cost channel is so influential

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
and because affordability is normalised on the **log-EAC scale** (§4.1), proportional cost shifts
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
was confirmed at the `high_shareAffordability` and `best_fit` vectors (§5.3): the ranking holds, and the
salience factors do rise where the social-norm weight is higher — the weight×structural interaction,
reported as a finding rather than a caveat. Magnitudes are quantified by the LHS in §5.3.
---

## 5. Structural sensitivity (process and techno-economic parameters)

The weight calibration covers preference uncertainty only. The parameters governing *how the dynamics
play out* are screened **separately**, holding the weights at the retained-ensemble median so that
"which weights fit" is never entangled with "how sharp is the salience curve".

### 5.1 Full parameter inventory

| parameter(s) | default | where | category | uncertainty type | proposed treatment |
|---|---|---|---|---|---|
| `shareAttitude, shareSocialnorm, shareAffordability, shareIntention` (4 weights) | AL: ⅓·⅓·⅓ / 0.71 / 0.5 | `Constants` | **A. Decision weights** | preference / value | **done** — LHS + history-matching ensemble (§4) |
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
| **gas price** (0.14 €/kWh) + real growth | static | `energy_source_data.csv` | **D. Energy prices** | exogenous market | **scenario LOW/HIGH path** (§6) |
| **electricity price** (0.32 €/kWh) + real growth | static | `energy_source_data.csv` | **D. Energy prices** | exogenous market | **scenario LOW/HIGH path** (§6) |
| `SLF, ELF, GRR, DHCT, DHES, SHAES, DHCO, GCHPB` | scenario | `Scenario` | **E. Policy / scenario levers** | decision, not uncertainty | the 16-scenario matrix (existing) |
| CBS-suppressed neighbourhoods → default gas (≈1.8% of stock), stock draw, `heatingYear` | — | `StockLoader` | **F. Initial-condition / data** | data uncertainty | robustness checks, not sampled |
### 5.2 Design

Held **fixed** (good assumptions or low interest): efficiency (COP), discount rate, lifetime (the ±3
jitter already covers its spread), subsidy (a policy assumption), and network structure. **Energy
price is one *coupled* factor** — gas and electricity co-move through the marginal power price — so it
is a single axis from both-fall to both-rise, not a 2×2 grid. Screening runs on the **baseline
scenario**: it pins every policy switch at neutral, and several scenarios would *confound* the screen
because they themselves move structural parameters (`individual_/collective_technologies` and the
learning-factor scenarios set SLF/ELF).

Method is **Morris first, then LHS on the survivors** — which factors matter is an empirical question,
so the expensive variance-based step is spent only on those that survive screening. Morris ranking is
robust at 1 iteration (effects of 10–25 %pts dwarf the ~0.4 %pt pathway noise); LHS quantification
needs 5–10.

The objective is the full **2050 modal split**: the screen reports a factor × technology µ* matrix plus
a TOTAL = Σ|µ*|, so a factor that swaps hybrid↔electric without touching gas still registers.

### 5.3 Result: economics dominate, and the ranking is weight-stable

µ* TOTAL, at three weight vectors spanning the retained ensemble:

| factor | median | high-affordability | best-fit |
|---|---|---|---|
| energyPrice | 25.7 | 23.1 | 25.7 |
| learningRateMult | 25.4 | 12.2 | 25.8 |
| capexMultHp | 24.4 | **33.0** | 23.2 |
| gumbelScaleUtil | 13.7 | 8.7 | 15.1 |
| gumbelScaleEac | 9.2 | 5.4 | 9.2 |
| salienceThreshold | 3.3 | 3.1 | 6.6 |
| salienceSteepness | 1.5 | **4.1** | 2.3 |
| lifetimeJitterSd | 1.5 | 2.8 | 2.2 |
| salienceK | 1.4 | 1.2 | 2.5 |

Three techno-economic factors dominate in **every** weight vector, each roughly twice as influential
as any behavioural parameter. The LHS (80 samples, 8 iterations) gives directions and variance shares:

| factor | gas | hybrid | electric | DH |
|---|---|---|---|---|
| energyPrice | −0.54 | −0.30 | **+0.52** | −0.47 |
| capexMultHp | +0.48 | +0.18 | −0.40 | +0.53 |
| learningRateMult | −0.27 | **−0.50** | **+0.54** | −0.16 |
| gumbelScaleUtil | −0.27 | +0.37 | −0.14 | −0.44 |
| gumbelScaleEac | ~0 | −0.18 | +0.13 | +0.09 |
| **model R²** | 0.76 | **0.63** | **0.91** | 0.81 |

Higher prices and faster learning both push gas *and hybrid* toward full electrification; higher HP
capex holds gas and DH. The 2050 electric-HP share is almost entirely explained by these economic
factors (R² 0.91), whereas hybrid is the least predictable (0.63) — it is the swing technology.

**The salience parameters matter only where the social-norm weight is high** (steepness µ* 1.5 at the
median vs 4.1 at the high-affordability vector, whose intention group carries more social norm). This
is a genuine weight × structural interaction and a reportable finding, not a confound: the tipping
parameters bite exactly to the extent that calibrated preferences weight the social channel.

### 5.4 Note on the grid parameters

Congestion is deliberately **policy-gated** (`possible()` rule 3 is conditional on
`gridCongestionHpBan`), reflecting that a DSO cannot stop a homeowner installing a heat pump within
their existing connection capacity. Verified: in baseline, adoption outcomes are **bit-identical**
with congestion active or disabled — only the reported congestion percentage differs. Consequently the
grid parameters return µ* = 0 on a baseline screen and are **not** part of the table above; they can
only be screened on a scenario where the ban is active. The baseline structural results are therefore
unaffected by the congestion port.

---

## 6. Energy prices (exogenous scenario axis)

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
---

## 7. How to run

```powershell
# 0. one-off: extract the observed series + the 2022 initial state
python model\data-export\scripts\export_observed_heating.py
# 1. one-off: provision the region's stock
python model\run.py --scope province:Noord-Brabant --scenario baseline --iterations 1

# 2. how far are the AL defaults from observed? (also runs the start-year gate)
python results_analysis\calibrate_weights.py evaluate --scope province:Noord-Brabant
# 3. how many MC iterations does the 3-year objective need?
python results_analysis\calibrate_weights.py converge --scope province:Noord-Brabant
# 4. plausible-weight ensemble (LHS + retain within tolerance)
python results_analysis\calibrate_weights.py search  --scope province:Noord-Brabant --samples 100 --iterations 1 --tolerance 2 --outdir results\calib
# 5. structural sensitivity at fixed calibrated weights (Morris -> LHS on the survivors)
python results_analysis\structural_batch.py --scope province:Noord-Brabant

# 6. explore: scenarios across the ensemble + price bracket + loop knock-outs + all figures
python results_analysis\pathway_batch.py --scope province:Noord-Brabant --iterations 5
```

`pathway_batch` writes the band to `results/<scope>/calib/`, the price runs to `price_low|price_high/`,
the knock-out runs and mechanism report to `knockout_*/` and `mechanism/`, and all comparison figures
to `figures/` (per-scenario bands in `figures/scenarios/`). To regenerate only the figures from
existing runs, use `analyze_all.py --scope <scope>`.

Weights are passed to the engine at runtime (`-Dht.<name>=<value>`), so **no recompile per sample**.
Each run also emits `<out>_loop_state.csv` (decision-time loop intermediates) and `<out>_segments.csv`
(per adopter/dwelling segment), which `mechanism_analysis.py` combines.

---

## 8. Verification

- **Unit tests** on the decision functions (`SelfTest`, 38 assertions) plus a JUnit suite.
- **Cross-implementation parity** between the Java research engine and the original AnyLogic model on
  hand-verified cases.
- **Start-year gate** in every calibration mode (see §2.1), currently passing within 0.1 %pts.
- **Knock-out tests** for causal claims about the reinforcing loops: disabling economic learning
  (`-Dht.learningRateMult=0`) leaves 31 % of dwellings on gas in 2050 versus 13 % intact; freezing
  salience (`-Dht.salienceFreeze=1`) accounts for a further ~4 %pts. Both loops are causally necessary
  for the gas phase-out, and the economic loop is much the stronger.
- **Segment validation**: the adopter-propensity index reproduces the expected S-curve ordering
  (innovators lead, laggards trail) — see §8.

---

## 9. Findings

- **Decarbonisation is robust; the route is not.** Across all 20 retained weight sets the 2050 gas
  share lands between 7 % and 24 %, but hybrid spans 35–81 %, electric 11–42 % and DH 1–16 %. Report
  the band; the qualitative outcome is identified, the specific mix is not.
- **Electric heat pumps are an innovator/early-adopter technology.** 2050 electric-HP share *within*
  each Rogers segment: innovators 99.7 %, early adopters 77 %, early majority 28 %, late majority 1.8 %,
  laggards 0 %. This explains the aggregate ~35 % electric-HP plateau as **segment saturation**, not a
  cost ceiling. Dwelling segments confirm the technical channel: large, ground-access, better-insulated
  homes electrify; apartments and high-rise do not.
- **Economics gate the pathway** (§5.2), and the HP learning rate specifically decides hybrid versus
  full electrification.
- **Energy prices can exceed the preference band**: under the high-price path the 2050 electric-HP
  share reaches 47.5 %, above anything any weight set produces at static prices, while gas falls to
  2.4 %. Price uncertainty is not a subset of preference uncertainty.
- **The AL default weights under-produce heat-pump adoption** — electric HP is under-predicted in two
  independent regions (−1.7 %pts Utrecht, −3.2 Noord-Brabant), so this is a model property rather than
  a regional quirk, and it is exactly the gap calibration closes. Hybrid is close (+0.2/+0.3).
- **District heating is over-assigned in DH-rich regions** (Utrecht +4.0 %pts), suspected to be the
  lumpy initial assignment of whole social-housing blocks. Noord-Brabant is unaffected; worth
  remembering if a DH-heavy region is ever used.

---

## 10. Caveats for the write-up

- **3 years, subsidy-driven.** The window coincides with strong NL heat-pump subsidies and the
  post-2022 gas-price spike; weights fitted to it may absorb those transient drivers.
- **Non-identifiable.** Report a *retained ensemble band*, never a single "calibrated" weight vector,
  and state the identifiability limits explicitly.
- **Regional transfer.** Weights are constrained on one province; representativeness was checked on
  stock, heating mix and ownership, but not on income or attitudes (not in the model).
- **Not a validation of outcomes.** Matching 2022–24 filters out behaviourally implausible
  parameterisations; it does **not** validate the 2050 projections, which are interpreted for the
  mechanisms they reveal rather than as forecasts.
- **Sensitivity ≠ preference weight.** Calibration *down-weights* affordability, yet the cost *drivers*
  dominate the 2050 outcome, because influence ≈ weight × input swing × feedback amplification.
