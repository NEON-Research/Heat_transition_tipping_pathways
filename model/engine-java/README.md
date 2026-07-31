# Heat Transition Engine (Java)

The **research engine** — an AnyLogic-free port of the model's multi-agent adoption core, in
plain Java 17. This is the authoritative model; the JavaScript engine in `../../deprecated/engine/`
is a retired reference oracle used to cross-check it.

The whole model lives here: homeowners (TPB/RUM utility), landlords, social-housing & HOA blocks,
and neighbourhood district-heating/grid state all decide year by year (2024→2050) and feed a shared
learning curve + salience. There is no separate "full" vs "core" build — `Simulation` **is** the
model and `Cli` is its single entry point.

## Quick start

Needs a JDK 17+. From `model/engine-java` (the Gradle wrapper self-manages Gradle):
```bash
gradlew build          # compile + run tests (JUnit DecisionTest + SelfTest)
gradlew selfTest       # dependency-free formula assertions only
gradlew run  --args="--real ../data/stock/limburg_dwellings.csv --scenario baseline --iterations 5"
gradlew cli  --args="--real ../data/stock/limburg_dwellings.csv --scenario all --iterations 20"   # + heap & HT_DIAG/HT_DYN probes
```
In practice you drive runs through **`../run.py`** (scope resolution, data provisioning, build,
and optional plotting) rather than calling `Cli` directly — see [`../README.md`](../README.md) for the full command reference.

Output is the `simulation_results` CSV schema (rows per scenario × iteration × year × heating
system × ownership: PRIVATELY_OWNED / PRIVATELY_RENTED / SOCIAL_HOUSING / HOME_OWNER_ASSOCIATION /
TOTAL). The opportunity trigger is fixed at 75% of system life (the corrected behaviour); a `legacy`
flag is retained only to reproduce the original int-division bug for golden comparison.

## More

Full internals — execution model, class reference, diagnostics, and conventions — are in
**[ARCHITECTURE.md](ARCHITECTURE.md)**. Parity findings and the AL cross-check live in
`../PARITY_TESTS.md`.
