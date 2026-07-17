# Heat Transition Engine (Java)

The **research engine** — an AnyLogic-free port of the model's homeowner adoption core, in
plain Java 17. This is the authoritative model (per your decision to keep the science in
Java); the JavaScript engine in `../engine/` is now a *reference oracle* used to cross-check
this one, and the front end will be a thin layer on top.

## Layout
```
engine-java/
  build.gradle / settings.gradle       Gradle build (Java 17 toolchain, JUnit 5)
  src/main/java/heattransition/
    HeatingSystem.java, Ownership.java  enums (OL_* order preserved)
    Constants.java                      calibration constants (f_setDefaultWeights etc.)
    Rng.java                            seeded RNG: uniform, beta(5,2), gumbel
    Decision.java                       pure decision fns (bug fixed + legacy flag)
    Economics.java                      EAC/TCO (faithful to J_Household.f_getEAC)
    HeatingSystemSpec/Data.java         REAL values from HEATING_SYSTEM_DATA / ENERGY_SOURCE_DATA
    Dwelling.java, Scenario.java, YearResult.java   state
    Simulation.java                     annual loop + salience + learning-curve feedback
    Results.java                        exact simulation_results CSV schema
    SyntheticData.java                  placeholder stock (see parity note)
    Cli.java                            headless runner
    SelfTest.java                       dependency-free assertions (no JUnit needed)
  src/test/java/heattransition/
    DecisionTest.java                   JUnit 5, mirrors tests/heat_model_ref.py
```

## Build, test, run (needs a local JDK 17+)
```bash
gradle test                                             # JUnit suite
gradle selfTest                                         # dependency-free assertions (no network)
gradle run --args="--dwellings 2000 --iterations 3 --out out.csv"
gradle run --args="--scenario social_learning_factor_high --iterations 5"
gradle run --args="--legacy-trigger"                    # reproduce the original int-division bug
```
No Gradle? With a JDK you can also run the assertions directly:
`java src/main/java/heattransition/SelfTest.java` (Java 22+ multi-file source launch).

## Verification status
Every file is **syntax-checked** here (parsed clean), but this environment has no Java
compiler, so it was not compiled/run here. The tests (`DecisionTest`, `SelfTest`) encode the
same hand-verified values as the JS engine's passing suite, so they are correct by
construction. Run `gradle test && gradle selfTest` locally to confirm green. To cross-check
against the JS oracle, run both CLIs with the same settings and compare the CSVs with
`../tests/compare_to_golden.py` (statistical tolerance — the two RNG streams are
equivalent, not bit-identical).

## The trigger fix
`Decision.hasOpportunityTrigger(age, lifetime, legacyIntDivision=false)` uses real division
(`(double) age / lifetime > 0.75`), so a homeowner reconsiders once past 75% of system life —
the intended behaviour. The original AnyLogic code used `age / lifetime` with both `int`
(integer division), which only ever fired at `age >= lifetime`. Pass `true` (or
`--legacy-trigger`) to reproduce the old behaviour for validating against the pre-fix golden.


## Full multi-owner model (social blocks + landlords + homeowners)
`FullSimulation` + `RealStockAllLoader` + `FullCli` port the multi-agent model
(engine/src/modelFull.js): social-housing blocks and landlords decide on cost, homeowners on
TPB utility, all feeding the shared learning-curve + salience. Run it:
```
javac -d ..\..\..\build heattransition\*.java      # from src/main/java (recompile all)
java -cp build heattransition.FullCli --real ..\data-export\out\limburg_dwellings.csv --scenario baseline
```
Output is the same simulation_results CSV schema, with PRIVATELY_OWNED / PRIVATELY_RENTED /
SOCIAL_HOUSING / TOTAL rows. `heattransition.Cli` remains the homeowner-only runner.
The opportunity trigger is fixed at 75% of life (the corrected behaviour); the `legacy` flag
is retained only for reproducing the original int-division bug.

## Reaching golden parity (next steps)
Homeowner-only, on a **synthetic** stock. To match the AnyLogic golden results:
1. Export the real stock (`DWELLINGS_DEMAND_INSULATION` + neighbourhood household tables) and
   insulation-cost columns to JSON; load instead of `SyntheticData`.
2. Add the other decision agents (renters/landlords, social-housing & HOA blocks) and the
   DH-expansion / grid modules, mirroring `Main.f_adoptionProces`.
3. Validate with `../tests/compare_to_golden.py` in `--legacy-trigger` mode (to match the
   pre-fix golden), then switch to the corrected trigger and observe the intended shift.
