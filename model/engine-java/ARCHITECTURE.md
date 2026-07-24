# Heat-transition engine (Java) — architecture & usage

An AnyLogic-free port of the model's adoption core: ~545k dwellings in Limburg deciding, year by
year (2024→2050), whether to keep gas or switch to a heat pump / district heating, driven by cost
(EAC), social influence, and a learning curve. This is the **authoritative research engine**; the
JavaScript engine in `../../deprecated/engine/` is now a retired reference oracle.

Everything here is a faithful transcription of the generated AnyLogic Java in
`../../deprecated/Heat transition tipping pathways_BUILD/src.generated/` (the `.alp` is NOT authoritative — the
generated source is). Where a line looks odd, it usually mirrors an AnyLogic quirk on purpose; such
spots carry a comment saying so.

---

## 1. Quick start

Needs a JDK 17+ (you have 21). The Gradle **wrapper** (`gradlew.bat`) is the only tool you run —
generate + commit it once, then it self-manages Gradle. First time, from `model\engine-java`:
```powershell
powershell -ExecutionPolicy Bypass -File .\gradle-bootstrap.ps1
```
That finds a Gradle (on PATH, or one it downloaded into `.tooling\`), creates `gradlew.bat`, and
builds. **Commit `gradlew`, `gradlew.bat`, and `gradle\wrapper\`** — after that nobody needs Gradle
installed. The whole day-to-day workflow is then just the wrapper:
```powershell
.\gradlew.bat selfTest            # dependency-free formula assertions (fast sanity check)
.\gradlew.bat test                # JUnit suite (DecisionTest)
.\gradlew.bat build               # compile + test, jar in build\libs\
.\gradlew.bat cli --args="--real ..\data\stock\limburg_dwellings.csv --scenario baseline --iterations 20 --out java_baseline.csv"
```

**Scenarios.** `--scenario <name>` runs one of the 16; `--scenario all` runs the full matrix into
one CSV (the AL batch equivalent). Names are in `Scenario.NAMES`. 13 are fully functional (learning
factors, DH/SHA POLICY_BASED strategies, DH connection obligation, collective/individual tech). The
3 `*_grid_congestion_ban` scenarios run but are inert — grid congestion needs data the export lacks
(per-nbh electricity `g_ele` is 0, and grid capacity isn't exported); they currently equal their
non-congestion twins. See `../HANDOVER.md` §4.
```powershell
.\gradlew.bat cli --args="--real ..\data\stock\limburg_dwellings.csv --scenario all --iterations 20 --out results\all_scenarios.csv"
```

**Diagnostic probes** (toggle with `-P` flags so you don't touch env vars):
```powershell
.\gradlew.bat --quiet cli -Pdiag --args="... --iterations 20 --out java_baseline.csv" > java.log     # HT_DIAG term probe
.\gradlew.bat --quiet cli -Pdyn  --args="... --scenario baseline --iterations 20 --out java_baseline.csv" > eng_dyn.log  # HT_DYN per-year dynamics
```

**In the Cowork Linux sandbox** (no Gradle reachable there): use raw `javac` instead —
`bash ../setup-java.sh` for the JDK, then
`javac -d build $(find src/main/java -name '*.java')` and `java -cp build heattransition.Cli ...`.

---

## 2. Project layout

```
model/
  run.py                        # scope-aware entry: provision (if needed) + run — see below
  households.db                 # raw NL source, 2.7GB SQLite, git-ignored (must exist locally)
  data/
    reference/                  # committed NL-wide lookups the engine reads
      dwellings_demand_insulation.json   nbh_heating.json   neighborhoods.csv
      heating_system_data.json           energy_source_data.json
    stock/                      # generated per-scope stock CSVs (git-ignored, large)
      limburg_dwellings.csv  nl_dwellings.csv  gemeente_maastricht_dwellings.csv ...
  data-export/scripts/          # Python DB->CSV pipeline (export_limburg_stock.py + others)
  engine-java/                  # the Java model (this folder)
    build.gradle  settings.gradle  gradlew(.bat)  gradle/wrapper/   # standard Gradle
    src/main/java/heattransition/*.java              # the engine (see §4)
    src/test/java/heattransition/DecisionTest.java   # JUnit mirror of the reference formulas
    build/  .tooling/                                # git-ignored
```

**Data flow.** The 2.7GB `households.db` is the raw source. `data-export/scripts/` derives per-scope
stock CSVs from it (heat demand via the VestaMAIS archetype tables, etc.) into `data/stock/`. The
engine reads a stock CSV (`--real`) plus the shared NL-wide lookups in `data/reference/`, which it
resolves *relative to the stock CSV* — no hardcoded paths. The engine never touches the DB.

**Running by scope (`run.py`).** One command handles the whole loop: **build + test** the engine
(`gradlew build`, so a run never uses stale classes — `--skip-build` to skip), check whether the
scope's data exists, provision it from the DB if not, then run:
```powershell
python run.py --scope province:Limburg --scenario baseline --iterations 20 --out results\limburg.csv
python run.py --scope gemeente:Maastricht --scenario all --iterations 5
python run.py --scope nl --scenario baseline --iterations 5 --xmx 32g   # whole country (memory-heavy)
```
It resolves the scope to `data/stock/<tag>_dwellings.csv`, provisions it from the DB if missing,
then runs `Cli` with any extra engine args forwarded. Scopes: `nl`, `province:<Name>`,
`gemeente:<Name>` (RES regions need a gemeente->RES mapping — not built yet). NL-wide is the eventual
target (adoption in one region influences another via the shared learning curve), so provinces are
subsets for testing, not independent runs.

Why Gradle: it compiles incrementally, manages the JUnit dependency, provides the toolchain (right
JDK), and the **wrapper** pins the exact Gradle version so every machine/CI builds identically with
no manual install. The `application` plugin + custom `JavaExec` tasks give named run commands
(`selfTest`, `cli`) instead of long raw `java -cp` lines.

---

## 3. How a run works (execution model)

**Setup** — `StockLoader.load(csv, rng, everyNth)`:
1. Read `limburg_dwellings.csv` row by row; for each dwelling draw an **ownership**
   (`PRIVATELY_OWNED` / `PRIVATELY_RENTED` / `SOCIAL_HOUSING`) from the neighbourhood's koop/huur/corp
   percentages, and a heat demand `fac*space + fac²*dhw` (fac ~ truncNormal).
2. Group by neighbourhood; assign each neighbourhood's **initial heating mix** from `nbh_heating.json`
   (gasCV/gasBlock/ehp/hhp/dh shares); build one **social-housing block** and one **HOA block** per
   neighbourhood from the social dwellings.
3. Build peer **networks** (homophily: same attitude band, biased local), seed `cumInstalled`
   (= starting stock) and each technology's learning base `initialUnits` and initial `salienceFactor`.

**Annual loop** — `Simulation.run()` calls `stepYear(year)` for 2025..2050. Each `stepYear`:
1. *(HT_DYN probe: emit decision-time invest+salience per type.)*
2. reset the per-technology min/max EAC window; age everything by one year; snapshot `prevShare`.
3. exogenous insulation (households + blocks slowly improve labels).
4. district-heating company expands the grid.
5. **decisions, in AnyLogic order:**
   - **Social/HOA blocks** — only at end-of-life; the whole block computes an average EAC per option
     and picks the cheapest (+ Gumbel noise); all its dwellings switch together.
   - **Landlords** — individual, end-of-life only, pick cheapest EAC (+ Gumbel).
   - **Homeowners** — end-of-life OR 75%-of-life opportunity; **two passes**: pass 1 computes every
     triggered homeowner's EACs (completing the min/max window), pass 2 evaluates TPB utility and
     makes a random-utility (Gumbel) choice.
6. update salience (per-technology social visibility from rising adoption).
7. *(HT_DIAG probe: at 2025/2030/2035 print averaged TPB terms.)*
8. update the learning curve (cheaper capex as cumulative installs grow) — feeds next year.
9. record the year's aggregate `YearResult`.

The two feedback loops — **learning curve** (more installs → cheaper) and **salience** (more
adoption → stronger social norm) — are why small early differences compound; see `../PARITY_TESTS.md`.

---

## 4. Class reference

### Entry points
| class | role |
|---|---|
| **Cli** | `main` for the model. Parses `--real --scenario --iterations --every --out`, runs, writes the `simulation_results` CSV. Launched by `gradlew run` or `gradlew cli` (the latter adds heap + probe toggles); normally driven via `../run.py`. |
| **SelfTest** | Dependency-free `main` that asserts the core formulas against hand-verified values. No JUnit needed. `gradlew selfTest`. |

### Simulation core
| class | role |
|---|---|
| **Simulation** | The ABM. Holds all agents (homeowners, landlords, social/HOA blocks, neighbourhoods), the shared `cumInstalled`/learning/salience state, and `stepYear`; exposes the inner `YearRow` per-year aggregate. Contains the `HT_DIAG` (term) and `HT_DYN` (dynamics) probes. **This is the file you edit for model behaviour.** |

### Agents & state
| class | role |
|---|---|
| **Dwelling** | One dwelling: `currentType`, `age`, ownership, archetype, label, area, heat demand, `hasLowTemp`, peer network, attitude, per-type `peerCounts`. |
| **HousingBlock** | A social-housing or HOA block: the households sharing one heating system, deciding together at end-of-life. |
| **Neighbourhood** | Neighbourhood aggregate for district-heating expansion + grid state. |
| **HeatingSystem** | Enum of the 5 technologies (order matches the AL enum / DB; ordinal is used for peer-count arrays). |
| **Ownership** | Enum of ownership types (`TOTAL` is an output aggregate, not stored). |
| **HeatingSystemSpec** | Per-technology **mutable** economics + behaviour: `investSmall/Medium/Large` (updated by the learning curve), `salienceFactor`, `minEAC/maxEAC` window, `initialUnits`, rates, sources, subsidy, required label. |
| **Simulation.YearRow** | One year's aggregate outcome (installed/removed/cumulative per type, per ownership, plus DH/congestion %). Inner class of `Simulation`. |

### Economics & decision (pure functions — the science)
| class | role |
|---|---|
| **Economics** | `computeEAC` — Equivalent Annual Cost = annualised discounted TCO (invest − subsidy + Σ discounted maintenance+energy). Faithful to `J_Household.f_getEAC`. Insulation cost added unconditionally, Vesta returns 0 when no upgrade needed (see fix #1 in PARITY_TESTS). |
| **Decision** | Pure TPB/RUM functions: `attitudeValue`, `effort`, `pbc`, `subjectiveNorm`, `intention`, `perceivedUtility`, `normalizedValue`, `rumScore`, and the trigger predicates (`hasEndOfLifeTrigger`, `hasOpportunityTrigger`). |
| **Vesta** | On-the-fly insulation cost from `dwellings_demand_insulation.json` (per archetype/label/area). Uses the a=1..g=7 label scale. |
| **Constants** | Calibration constants transcribed verbatim from AL (weights, Gumbel scale, learning multipliers). ⚠ `labelToNumber` here is the REVERSED scale (a=7..g=1) — a historical AL quirk; do NOT reuse it in `Economics` (that was bug #1, now fixed). |
| **HeatingSystemData** | The REAL per-technology values (invest, lifetime, rates, energy sources) exported from the AL `HEATING_SYSTEM_DATA` / `ENERGY_SOURCE_DATA` tables. `freshSpecs()` builds a fresh mutable `HeatingSystemSpec` map. |

### Data loading & config
| class | role |
|---|---|
| **StockLoader** | Builds the full multi-owner stock from the CSV + `nbh_heating.json` + `dwellings_demand_insulation.json`: ownership assignment, initial heating, blocks, networks. **The setup you edit for stock/ownership.** |
| **SyntheticData** | Plausible synthetic stock so `SelfTest`/`DecisionTest` can run the model without the real CSV. |
| **Scenario** | Scenario switches (learning factors, DH strategy, congestion block, connection obligation) — the 16 named scenarios. |
| **Results** | Owns the exact `simulation_results` CSV column header (`Results.HEADER`); `Cli` writes the rows to match it. |

### Infrastructure
| class | role |
|---|---|
| **Rng** | Deterministic seeded RNG (uniform, beta(5,2), Gumbel, truncNormal) — mirrors the JS engine so streams are comparable. |
| **DistrictHeating** | DH-company grid expansion logic (COST_BASED / POLICY_BASED). |

---

## 5. Diagnostics (parity probes)

Both are env-gated, zero cost when off. See `../PARITY_TESTS.md` for the full methodology.

- **HT_DIAG** (`-Pdiag` / `HT_DIAG=1`) — at 2025/2030/2035, prints averaged homeowner TPB terms
  (`att sn pbc eacNorm salience minEAC maxEAC`) for gas & hybrid. Tag `DIAG` on stdout. Compare
  3-way (JS/Java/AL) with `../tests/compare_diag.py`. The AL side is `../../deprecated/al-parity/ALDIAG_HTDiag.java`.
- **HT_DYN** (`-Pdyn` / `HT_DYN=1`) — at the start of every year, prints decision-time
  `invest`/`salience`/`cumulative` per technology. Tag `ENGDYN` on stdout. Compare vs AL with
  `../tests/compare_dyn.py`. The AL side is `../../deprecated/al-parity/ALDYN_HTDyn.java`.

Both AL probe classes are pasted into the AnyLogic model (see their headers for the one-line call
site) and write their own `al.log` / `al_dyn.log`. Use a **baseline-only** AL run for the dynamics
comparison (the log has no scenario tag).

---

## 6. Conventions & gotchas

- **Generated source is truth**, not the `.alp`. Confirm behaviour at the live call site — AL keeps
  dead overloads and commented blocks.
- **Two label scales exist**: `Vesta.labelNum` a=1..g=7 (worse=higher) and `Constants.labelToNumber`
  a=7..g=1 (better=higher). Mixing them caused the biggest bug this cycle. `Economics`/`Vesta` use
  the a=1..g=7 scale.
- **`cumInstalled` is never decremented** (starting stock + all installs, incl. reinstalls) — matches
  AL's `installedCumulative`; it drives the learning curve.
- **Silent defaults bite**: a failed string match → `false`, a bad map lookup → a fallback constant,
  a missing file → empty collection. None throw. Prefer printing STATE (the `HT_DIAG`/`HT_DYN`
  probes) over guessing.
- The **open residual** is PRIVATELY_RENTED gas ~1.5 pp low vs AL — real, ELF-invariant, all t0
  states match; see `../PARITY_TESTS.md` §4–5 for the trail and the next instrumentation step.
