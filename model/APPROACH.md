# Heat Transition Tipping Pathways — Migration Approach

**Goal.** Take the AnyLogic agent-based model (ABM) of the Dutch heat transition and
produce a version you can (a) run headlessly and *fast* for Monte Carlo and sensitivity
analysis, and (b) drive and inspect from a JavaScript/HTML front end — while keeping an
ABM structure you can trace, test, and trust against the current results.

**Decision taken (final).** The research engine stays in **Java** — the science is the
priority, and Java is the stronger fit for a correctness-critical, compute-heavy ABM you'll
maintain deeply (explicit numeric types, mature testing/debugging, true parallelism for
Monte Carlo, and it's already Java so fidelity risk is lowest). The **front end is
JavaScript** (a thin presentation layer). The Java engine lives in
`model/engine-java/` (see its README). During exploration a **JavaScript engine** was
also built (`model/engine/`); it is retained as a *reference oracle* — a runnable
second implementation to cross-check the Java against, number-for-number on the deterministic
parts and statistically on the stochastic ones. Both engines fix the trigger bug and run on
the real heating-system economics.

---

## 1. What the current model actually is

The `.alp` file is a 30k-line XML wrapper. Almost all real logic lives in **18 plain Java
classes** (prefixed `J_`) embedded in it. The AnyLogic-specific parts are thin: five agent
populations, a handful of UI charts/maps, a simulation experiment, and database/Excel
loaders.

### Agents and objects
- **AnyLogic agents** (population containers, light logic): `GZ_Neighborhood`, `DSO`,
  `DistrictHeatingCompany`, `SocialHousingAssociation`, `Landlord`, plus a `Startup_agent`
  that orchestrates runs.
- **Domain objects** (the substance, plain Java): `J_Household` / `J_HomeOwner` / `J_Renter`,
  `J_Dwelling`, `J_HeatingSystem`, `J_HeatingSystemOption(sGlobal/Local)`, `J_HousingBlock`
  / `J_SocialHousingBlock`, `J_Neighborhood`, `J_Municipality`, `J_PostalCode6`, and results
  aggregators `J_ResultsData` / `J_ResultsPerOwnerType` / `J_ResultsPerHeatingMethod` /
  `J_AnnualValues` / `J_AdoptionData` / `J_ScenarioConfig`.

Households are **not** AnyLogic agents — they are Java objects created from the database.
That is good news: the core simulation is essentially a standalone Java program already.

### The simulation loop (annual, 2024→2050)
`Startup_agent.f_runSimulation()` steps year by year and calls, per year:
1. `Main.f_adoptionProces()` — the heart of the model (see §2).
2. `GZ_Neighborhood.f_energyCalculations()` — hourly energy demand/supply per household.
3. `DSO.f_reinforceNeighborhoods()` — grid reinforcement.

### Decision core — Theory of Planned Behaviour + RUM
Each homeowner, when *triggered*, scores every feasible heating system:

```
attitude       = 1 - |householdAttitude - technologySustainabilityScore|
effort         ∈ {0.2 same system, 0.5 different, 0.8 needs low-temp retrofit}
EAC_norm       = minmax(levelised cost, minEAC, maxEAC)     # affordability
PBC            = weighted mean of (1-EAC_norm) and (1-effort)
subjectiveNorm = min(1, peerShare × (1 + salienceFactor))   # social contagion
intention      = weighted mean of attitude, subjectiveNorm×SLF×tech-rate, PBC
utility        = mean of intention and PBC
choice         = argmax over options of (utility + gumbelScale × Gumbel noise)  # RUM
```
Triggers: **end-of-life** (age ≥ lifetime) or **opportunity** (see the integer-division
note in §5 — it effectively also means age ≥ lifetime).

### The two feedback loops that create *tipping*
1. **Social salience** (`J_HeatingSystemOptionsGlobal.setSalienceFactor`): a novelty
   S-curve × momentum blend that amplifies the subjective norm for technologies whose
   share is low but *rising* — this is the social-contagion accelerator.
2. **Learning curve** (`f_updateCapexFromLearningCurve`): capex falls as
   `(1 − rate)^doublings` of cumulative installs, lowering EAC and reinforcing adoption.

Both are keyed off cumulative installs, so adoption feeds cost/desirability which feeds
more adoption — the mechanism the paper's "tipping pathways" name refers to.

### Data and outputs
- **Inputs:** an HSQLDB database (`database/db.script`, tables `DWELLINGS_DEMAND_INSULATION`,
  `HEATING_SYSTEM_DATA`, `ENERGY_SOURCE_DATA`, `NEIGHBORHOOD_*`, `MUNICIPALITIES`, …) plus
  `_*.xlsx` source spreadsheets and `geodata/` shapefiles.
- **Outputs:** `results/simulation_results_*.csv` (per scenario × iteration × year ×
  heating_system × ownership) and `results/example_dwellings_*.csv` (per-decision trace).
- **Determinism:** fixed RNG seed = 1; stochasticity from `beta(5,2)` attitudes and Gumbel
  choice. Aggregate run-to-run variation is small (CV ≈ 1% on major technologies).

### Calibration constants (extracted, current defaults)
| Constant | Value | Source |
|---|---|---|
| weight affordability→PBC | 0.5 | `f_setDefaultWeights` |
| weight effort→PBC | 0.2 | `f_setDefaultWeights` |
| weight attitude→intention | 0.5 | `f_setDefaultWeights` |
| weight PBC→intention | 0.5 | `f_setDefaultWeights` |
| weight socialNorm→intention | 0.5 | `f_setDefaultWeights` |
| weight intention→behaviour | 0.5 | `f_setDefaultWeights` |
| weight PBC→behaviour | 0.5 | `f_setDefaultWeights` |
| gumbelScaleUtil | 0.02 | `f_initializeMain` |
| gumbelScaleEAC | 20 | `f_initializeMain` |
| salience k / threshold / steepness | 10 / 0.3 / 30 | `J_HeatingSystemOptionsGlobal` |
| learning multiplier LOW/MED/HIGH | 0.5 / 1 / 2 | `f_learningFactorToMultiplier` |
| attitude distribution | beta(5,2,0,1) | `f_setAttitude` |
| horizon / seed | 2024–2050 / 1 | Simulation experiment |

These are all locked as an executable spec in `tests/heat_model_ref.py`.

---

## 2. Target architecture

Three decoupled layers, each independently testable:

```
┌─────────────────────────────────────────────────────────────┐
│  FRONT END  (JavaScript / HTML)                              │
│  scenario builder · run button · charts · map · MC dashboard │
└───────────────▲──────────────────────────────┬──────────────┘
                │ JSON (scenario config)        │ JSON/CSV (results)
                │                               ▼
┌───────────────┴──────────────────────────────────────────────┐
│  RUNNER  (thin Java HTTP server OR CLI)                        │
│  POST /run {scenario} → run engine → return results           │
│  CLI: engine run --config scenarios.json --out results.csv    │
└───────────────▲──────────────────────────────┬───────────────┘
                │                               ▼
┌───────────────┴──────────────────────────────────────────────┐
│  ENGINE  (pure Java, no AnyLogic)                             │
│  the J_ classes, decoupled: RNG · data loader · sim loop      │
│  emits results in the EXISTING simulation_results CSV schema  │
└──────────────────────────────────────────────────────────────┘
```

**Why a thin server rather than porting to JS?** The engine is CPU-bound integer/float
work over ~2,000–8,000 dwellings × 27 years × many iterations. On the JVM this is fast and
already written. The front end just needs to *start runs* and *read results*. A localhost
HTTP endpoint (or, for pure offline use, the CLI writing a CSV the page then loads) keeps
the maths in one place and the UI free to be pure JS/HTML.

If you later want the model to run fully in-browser (no server), the same decoupled engine
can be compiled to WebAssembly (TeaVM/CheerpJ) — but that is an optimisation, not a
prerequisite, and is out of scope for the first milestone.

### Data handling
Export the AnyLogic HSQLDB tables and the `_*.xlsx` inputs **once** into static
`data/*.json` (or `.csv`) bundled with the engine. This removes the HSQLDB/AnyLogic
dependency, makes runs reproducible and portable, and lets the front end read the same
inputs for display. A one-off `tools/export_db.py` (HSQLDB → JSON) plus an Excel→JSON step
covers it. Keep the raw DB/Excel in the repo as the provenance of record.

---

## 3. Keeping the ABM inspectable, traceable, and testable

This is the part that protects you as the model evolves.

**Separation of concerns.** The engine has no UI and no I/O framework in its core. Agents
and objects are plain classes with explicit state; the scheduler is an explicit annual loop
you can read top-to-bottom. No hidden AnyLogic lifecycle, no presentation code entangled
with behaviour.

**One source of truth for the maths.** `tests/heat_model_ref.py` transcribes every core
formula with a citation back to the Java method. Any change to behaviour changes this file
and its tests first — the spec leads the code.

**Deterministic mode.** Provide a `--seed` and a `--deterministic` switch (Gumbel scale → 0,
fixed attitudes) so a run is exactly reproducible. Deterministic runs make regressions
obvious and debugging tractable; stochastic runs are for the actual analysis.

**Structured trace, not `traceln`.** Replace the scattered `traceln`/`System.out` debug
lines with a single levelled tracer that can emit, per decision:
`year, dwelling_id, ownership, trigger, options[{type, EAC, PBC, SN, attitude, utility}],
chosen`. This is exactly the `example_dwellings_*.csv` schema you already export — keep it
as the canonical per-decision audit trail. You can then trace any single household's path
through the run.

**Invariants / assertions.** Add cheap runtime checks that encode things that must always
hold: shares ∈ [0,1], each dwelling has exactly one heating system, stock conserved
(installed − removed balances), utilities finite. Fail fast in deterministic/test mode.

**Golden-master safety net.** The comparison harness (§4) means any refactor — extracting a
class, changing a data source, optimising a loop — is checked against the current model's
emergent behaviour automatically.

**Suggested engine layout**
```
engine/
  agents/        GZ_Neighborhood, DSO, DistrictHeatingCompany, SHA, Landlord
  domain/        J_Household, J_HomeOwner, J_Dwelling, J_HeatingSystem, ...
  decision/      attitude, pbc, intention, utility, salience, learning-curve  (pure fns)
  data/          loaders + exported json/csv
  sim/           annual loop, scheduler, RNG, trace, invariants
  io/            results CSV writer (existing schema), scenario config reader
  cli/ or http/  runner
tests/           spec tests (JUnit, mirroring tests/heat_model_ref.py) + golden master
```

---

## 4. Testing strategy (what's in `tests/` now)

Matching bit-for-bit across RNG implementations is impossible and not the goal. We test on
**two levels**, both runnable today.

**A. Formula spec — exact.** `tests/test_heat_model_spec.py` (+ `heat_model_ref.py`) pins
every deterministic formula (attitude, PBC, subjective norm, intention, utility, salience,
learning curve, triggers, RUM) with hand-computed values. 21 tests, all passing. Mirror
these as JUnit tests directly against the engine's `decision/` functions so the Java is held
to the identical numbers. *This is the executable specification of the model core.*

**B. Golden master — statistical.** `tests/compare_to_golden.py` compares any engine's
`simulation_results` CSV against reference metrics distilled from the current AnyLogic runs
(`tests/extract_reference_metrics.py` → `tests/golden/baseline_metrics.json`). It checks, per
scenario:
- final-year installed shares per heating system × ownership,
- a mid-horizon checkpoint (catches trajectory/timing drift),
- tipping years (first year a technology crosses 25% / 50% of stock),

with tolerance `|new − golden| ≤ k·std + rel·mean + floor` (defaults k=4, rel=5%,
floor=500). Validated: it passes 100% against its own golden and correctly flags a genuinely
different model version. Tune `k`/`rel` down as the engine matures.

**How you'll use it during the port**
1. Freeze today's canonical run as golden: `python extract_reference_metrics.py <your
   current results.csv> -o golden/baseline_metrics.json`.
2. Build the engine; make it emit the same CSV schema (ideally ≥10 iterations/scenario).
3. `python compare_to_golden.py engine_out.csv` → aim for green, investigate every red.
4. Keep JUnit spec tests green on every commit.

**Recommended additions as the engine lands**
- JUnit port of the spec tests (same numbers, against real engine functions).
- A tiny fixture neighbourhood (10–20 dwellings) with a fully hand-worked expected first
  year, for a truly exact end-to-end deterministic test.
- A characterization test on `example_dwellings` (per-decision) for one seeded dwelling.

---

## 5. Fidelity traps to reproduce deliberately

Porting/refactoring will "fix" these unless you pin them. They change results.

- **Opportunity trigger integer division — FIXED.** Java `age / lifetime > 0.75` with both
  `int` ⇒ integer division ⇒ fired only when `age ≥ lifetime`, *not* at 75% of life. This was
  a bug: the engine now uses real division (`hasOpportunityTrigger`), so homeowners reconsider
  once past 75% of life as intended (~3x more replacement decisions per run). A
  `legacyIntDivision` flag reproduces the old behaviour so you can still validate a faithful
  port against the pre-fix golden metrics, then switch to the corrected trigger. Because the
  golden was generated by the buggy model, expect tipping years to shift *earlier* after the
  fix — that shift is the intended effect, not a regression.
- **Subjective-norm weight in the denominator.** `weight_SN = socialLearningFactor ×
  weight_socialNormToIntention` appears in the intention denominator; the per-technology
  `socialLearningRate` multiplies only the numerator term. Easy to conflate.
- **`f_getNormalizedValue` has no min==max guard** — replicate or guard consistently.
- **Min/max EAC reset each year** before options are scored (normalisation baseline moves
  annually). Order of operations in `f_adoptionProces` matters — keep it.
- **Gumbel uses `Math.random()`**, independent of the seeded instance used elsewhere, so
  the stochastic choice stream is *not* controlled by seed=1. If you want reproducible
  stochastic runs, route it through the seeded RNG in the engine (a deliberate improvement —
  note it, since it will change exact numbers but not distributions).

---

## 6. Milestones

1. **Engine core — DONE.** Java: `model/engine-java/` (seeded RNG, pure decision
   functions with the bug fixed, faithful EAC/TCO on real heating-system data, annual loop
   with salience + learning-curve feedback, exact CSV schema, CLI, JUnit + a dependency-free
   `SelfTest`; syntax-checked here, `gradle test` to confirm green locally). A JavaScript
   twin (`model/engine/`, tested green here) serves as the reference oracle.
2. **Real data — DONE (export + loaders).** `model/data-export/` extracts the real
   Limburg stock from `households.db` (SQLite, 9.7M NL dwellings) with faithful heat-demand
   computation, plus the archetype/heating/energy reference tables from `db.script`. Both
   engines load it via `--real`. Validation: the ownership split reproduces the golden
   65.6/19.4/15.0 almost exactly, and heat demand is realistic (~10 MWh median). See
   `data-export/README.md`.
   **Still open for parity — full agents.** On the real stock the homeowner-only core reaches
   ~11% heat pumps by 2050 vs the golden ~96%. The gap is structural: the tipping feedback
   (learning curve + salience) is driven by cumulative installs across *all* owner types, so
   the transition can't ignite from homeowners alone. Add social-housing blocks + landlords
   (their installs feed the shared learning-curve/salience state) and set the initial heating
   mix from `NBH_HOUSEHOLD_HEATING_METHOD2023`. Then validate with `compare_to_golden.py` in
   `--legacy-trigger` mode, then switch to the corrected trigger.
3. **Monte Carlo / sensitivity harness.** Extend the CLI to sweep seeds and parameters
   (weights, learning factors, salience constants, scenario switches), parallelise across
   worker threads, write tidy long-format CSV/Parquet. Reuse the `f_varyWeights` grid design.
4. **JS front end.** HTML page: scenario builder → run in-browser (or load a CSV the CLI
   wrote) → charts (adoption curves, stacked shares, tipping markers) + neighbourhood map.
   Mirror the AnyLogic dashboard.
5. **(Optional) parallel batch runner** (Node worker_threads) for large sweeps, and a
   Java/JVM engine if you ever want it — the module structure ports 1:1.

---

## 7. Open decisions for you

- **Runner transport:** localhost HTTP server (interactive, live runs from the page) vs.
  CLI-writes-CSV-page-reads (fully offline, simpler). Recommendation: start with CLI+CSV,
  add HTTP when you want live runs.
- **Golden source of record:** which existing results file is "the current model"? The
  30-iteration `simulation_results_non_stochastic_limburg.csv` is used as the default golden
  here because it characterises variance well — confirm it's the canonical configuration, or
  point the extractor at the right file.
- **MC output format:** CSV (simple, matches now) vs. Parquet (compact for large sweeps).

See `model/MODEL_REFERENCE.md` for the per-class/formula reference and
`model/tests/README.md` for how to run the tests.
