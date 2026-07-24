# Heat Transition Engine (JavaScript)

A dependency-free, ES-module port of the AnyLogic model's **homeowner adoption core**.
Runs in Node (headless Monte Carlo) and in the browser (the HTML tool) with no build step.
The heavy decision logic is transcribed faithfully from the Java `J_` classes; the
opportunity-trigger integer-division bug is **fixed** (see below).

## Layout
```
engine/
  src/constants.js    calibration constants + enums (from f_setDefaultWeights etc.)
  src/rng.js          seeded RNG: uniform, beta(5,2), gumbel  (reproducible)
  src/decision.js     pure decision fns: attitude, PBC, subjectiveNorm, intention,
                      utility, salience, learning curve, triggers, RUM
  src/economics.js    EAC/TCO (faithful to J_Household.f_getEAC)
  src/model.js        Simulation: dwellings + homeowners, annual loop, feedback loops
  src/results.js      aggregation -> exact simulation_results CSV schema
  data/heating_systems.js  REAL values from HEATING_SYSTEM_DATA / ENERGY_SOURCE_DATA
  data/synthetic.js   plausible synthetic dwelling stock (placeholder — see parity)
  run.js              CLI runner
  test/decision.test.mjs   node --test suite mirroring tests/heat_model_ref.py
```

## Run
```bash
node run.js --dwellings 2000 --iterations 3 --out out.csv        # baseline + a few scenarios
node run.js --scenario social_learning_factor_high --iterations 5
node run.js --legacy-trigger                                     # reproduce the original bug
node --test                                                      # unit tests (17)
```

Speed: ~12 runs of 2,000 dwellings x 27 years in ~0.5s — comfortably fast for Monte Carlo.

## The trigger fix
`decision.js: hasOpportunityTrigger()` uses real division (`age/lifetime > 0.75`), so a
homeowner reconsiders once their system passes 75% of its life — the intended behaviour.
The original AnyLogic code used Java integer division, which only ever fired at
`age >= lifetime`. Pass `legacyIntDivision=true` (or `--legacy-trigger`) to reproduce the
old behaviour for validating a faithful port against the pre-fix golden metrics.
Effect: the fix produces ~3x more replacement *decisions* per run (homeowners look earlier
and often keep their system) — realistic churn the buggy version could not generate.

## Reaching golden parity (next steps)
This core intentionally covers homeowner adoption only, on a **synthetic** building stock.
To match the AnyLogic golden results you need to:
1. Export the real stock (`DWELLINGS_DEMAND_INSULATION` + neighbourhood household tables)
   to JSON and load it instead of `data/synthetic.js` (same dwelling shape).
2. Export the real insulation-cost columns and replace the placeholder in `model.js`.
3. Add the other decision agents (renters/landlords, social-housing & HOA blocks) and the
   DH-expansion / grid modules, mirroring `Main.f_adoptionProces`.
4. Validate with `../tests/compare_to_golden.py` in `--legacy-trigger` mode first (to match
   the pre-fix golden), then switch to the corrected trigger and observe the intended shift.
