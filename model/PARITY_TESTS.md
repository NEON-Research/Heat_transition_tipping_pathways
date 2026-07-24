# Parity tests & checks — JS / Java engines vs AnyLogic

Traceability log of the verification work: what was tested, how, the result, and the exact
commands, so any step can be re-run and the trail continued in a later session. Companion to
`HANDOVER.md` (which holds the narrative + open items); this file is the reproducible how-to.

Reference points as of 2026-07-22. Paths are relative to `model/` unless noted.

---

## 0. Environment / running

**Sandbox JDK** (Cowork Linux box has no Java by default): `bash setup-java.sh`, then
`export JAVA_HOME=$(cat /tmp/jdkroot/.javahome) && export PATH="$JAVA_HOME/bin:$PATH"`. Idempotent,
~1 min, re-run each session. See `setup-java.sh`.

**Compile the Java engine (from ANY dir — avoids sweeping the AL source):**
```
javac -d "$REPO/engine-java/build" $(find "$REPO/engine-java/src/main/java" -name '*.java')
```
On Windows: `javac -d engine-java\build (Get-ChildItem -Recurse -Filter *.java -Path engine-java\src\main\java | % FullName)`.
**Never** run a recursive `javac` from `model/` root — it pulls in `Heat transition tipping
pathways_BUILD/src.generated` (needs AnyLogic libs) and the standalone `ALDIAG_*.java` reference
files, producing thousands of errors.

**Run engines (20 iters, baseline):**
```
# JS  (JSDIAG -> stderr)
cd engine && HT_DIAG=1 node runFull.js --scenario baseline --iterations 20 --out js_baseline.csv 2> js.log
# Java (DIAG -> stdout)
cd engine-java && HT_DIAG=1 java -Xmx4g -cp build heattransition.Cli --real ../data/stock/limburg_dwellings.csv --scenario baseline --iterations 20 --out java_baseline.csv > java.log
```
PowerShell `>`/`2>` write **UTF-16**; the compare scripts handle that, but be aware.

---

## 1. Bugs found & fixed this cycle

| # | Bug | Where | Effect | Fix |
|---|---|---|---|---|
| 1 | Inverted insulation guard | `engine-java/.../Economics.java` | Java skipped hybrid/electric insulation cost for every dwelling needing it → hybrid EAC window collapsed (maxEAC 6764 vs 12748) → homeowners stranded on gas (21% vs 11% at 2050) | Removed the `Constants.labelToNumber` guard (reversed scale a=7..g=1 vs Vesta a=1..g=7); call insulation unconditionally, let Vesta return 0. Matches JS + AL `f_getEAC`. |
| 2 | Homeowner phase interleaved normalization | both engines | AL computes ALL triggered homeowners' EACs first (complete min/max window) then decides; engines did it in one pass | Split into two passes (`hoPending`). Faithful to AL, but with 84k agents the window is ~complete anyway so it did NOT move results (bug #1 was the real cause). |
| 3 | Java HT_DIAG counter not reset per year | `engine-java/.../Simulation.java` | 2030/2035 `n` reported 5-year SUM (~350k vs ~65k) → looked like over-triggering | Gate accumulate on `diagActive` (report years only), matching JS/AL probes. Was a PROBE artifact, model was fine. |

All three verified against AnyLogic at the term level (see §3).

---

## 2. What was checked and CONFIRMED to match AL (do not re-litigate)

- **Ownership assignment** `(huur-corp)/100` split — byte-identical to AL `f_getDwellingOwnership`
  (Main.java ~3699).
- **Landlord decision propensity** — gas is cheapest for 83.6% (engine) vs 84.1% (AL) of landlords
  at t0. `dumpDwellings.js` vs AL `dwelling_dump_*.csv`.
- **Block t0 state** — `dumpBlocks.js` vs AL `block_dump_*.csv`: 920 vs 926 blocks, mean EAC gas
  1652/1650, hybrid 1899/1902, ~97.5% gas-cheapest in both.
- **Base supply-system invest costs** — engine `heating_systems.js` (hybrid 6000, electric 9000,
  gas 2250) match the authoritative AL DB `HEATING_SYSTEM_DATA` (db.script rows 3230/3232/3233).
  AL's runtime `5871.63` in `learning_curve_*.csv` is a post-update RECORDING (capex_medium logged
  after step-12 while the factor column is recomputed from start-of-year cumulative → looks like
  factor 1.0 but isn't). At DECISION time, 2025 hybrid invest = 6000 in both (see ENGDYN §5).
- **Insulation cost table + parser** — Java's hand-rolled JSON parser replicated in Python: 0
  mismatches vs `json.load` on the `ki_s*` columns. Formula `(min+max)/2` identical.
- **Learning base** (`initial_units`) — 11396 (engine) vs 11398 (AL) hybrid; match.
- **Area cap** [20,500] — baked into the CSV export (`export_limburg_stock.py`), identical for both.
- **Heat-demand formula** `fac*space + fac²*dhw`, truncNormal(1,0.2,0.5,1.5) — identical.
- **Gumbel / RUM randomness** — identical. Block+landlord EAC choice `score = EAC + 20·gumbel`,
  homeowner utility `score = util + 0.02·gumbel`, `gumbel = -log(-log(u))`, argmin/argmax. Scales
  confirmed from AL `Startup_agent.java`: `gumbelScaleEAC = 20`, `gumbelScaleUtil = 0.02` (the
  `0.0` defaults in `_..._DefaultValue_xjal()` are a codegen artifact overridden at startup; only
  `SensitivityAnalysis` uses 0). Engine `GUMBEL_SCALE_EAC=20`, `GUMBEL_SCALE_UTIL=0.02` match. So
  Gumbel is symmetric+identical → cannot be the source of a *directional* bias.
- **Block construction** — `dumpBlocks.js` vs AL `block_dump`, split by type: SOCIAL 674 blocks,
  avg size 157, gas/hyb EAC 1805/2048 (engine) vs 1807/2048 (AL), ~3.7% vs 4.0% hybrid-cheapest;
  HOA 246 vs 247, size 104, identical EACs. So which-households-compose-a-block matches AL.

---

## 3. Term-level probe (HT_DIAG) — 3-way, `tests/compare_diag.py`

Each engine + AL emit, at 2025/2030/2035, per system (gas/hybrid): `n att sn pbc eacNorm salience
minEAC maxEAC`. Tags: `JSDIAG` (JS, stderr), `DIAG` (Java, stdout), `ALDIAG` (AnyLogic, via the
`HTDiag` class — paste `deprecated/al-parity/ALDIAG_HTDiag.java` into the model + 3 calls in `Main.f_adoptionProces`;
writes `al.log` itself).

```
python tests/compare_diag.py engine/js.log engine-java/java.log al.log
```

Result (20 iters): **all three agree at 2025 on every TPB term** — hybrid eacNorm 0.149/0.149/0.150,
pbc 0.678/0.676/0.674, maxEAC 12704/12432/12605. Insulation fix validated against AL itself. The
`compare_diag.py` script: reads UTF-8/UTF-16, warns on empty/missing files, and FLAGS n-count
outliers as probe artifacts (the guard that would have caught bug #3 instantly).

Aggregate (`tests/compare_engines.py`, within-owner heating shares vs AL): all owner types within
noise EXCEPT PRIVATELY_RENTED gas (see §4).

---

## 4. Open residual: PRIVATELY_RENTED gas ~1.5 pp low vs AL

**Real, not noise.** 2050 landlord gas, 20 iters: AL 32.10±0.24, JS 30.59±0.23, Java 30.65±0.23.
Welch t≈20 (≈20 standard errors). 20 iterations is far more than enough. (Blocks have sd 2.7–5.4
so THEIR differences are within noise — but they seed the loop.)
```
# significance + per-year trajectory:
python3 -c "..."   # see git history / re-derive: per-iteration share of PRIVATELY_RENTED gas @2050
```

**Trajectory:** gap is 0 at 2024 (all 88.4%), opens to ~2.7 pp at 2035-40, back to 1.5 pp at 2050.
A cumulative RATE effect — engines leave gas ~0.2-0.3 pp/yr faster.

**ELF sensitivity (confirming experiment).** AL landlord gas @2050: ELF-low 38.75, base 32.07,
ELF-high 18.60 (from `simulation_results_20260721_155228.csv`, all 16 scenarios × 20 iters).
Engine (1 iter each): 37.28 / 30.59 / 17.35. **The gap is ~constant (~1.4 pp) across ELF, NOT
scaling** → it is NOT learning-rate amplification; it's an EARLY adoption head-start baked in while
the factor is still ~1.0, then carried in parallel.

**Localization so far:** everything at t0 matches AL (§2). At 2025 the decision-time invest (6000),
salience (0.482), and homeowner TPB terms all match — yet the engine installs ~2× the hybrids AL
does in year 1 (cumulative 18856 vs 15267). So the seed is NOT cost/learning/salience.

**Which owner types diverge (significance, 20 iters, `20260723_085053` baseline vs Java):**
| owner | AL gas | Java gas | diff | Welch t |
|---|---|---|---|---|
| SOCIAL_HOUSING | 23.12±2.34 | 17.14±2.31 | +5.99 | **7.9 (systematic)** |
| HOME_OWNER_ASSOCIATION | 44.24±4.69 | 43.50±5.16 | +0.74 | **0.5 (noise)** |
| PRIVATELY_RENTED | 32.12±0.22 | 30.65±0.22 | +1.47 | 20.3 |
| PRIVATELY_OWNED | 2.37±0.14 | 2.19±0.18 | +0.17 | 3.3 |

Key: **HOA is within noise** (block-composition randomness averages out over 20 iters); **SOCIAL is
a real systematic bias** (t≈8) despite similar per-iteration SD. So the excess is specific to
social blocks — and since block construction matches AL (§2), it's a social-block DECISION dynamic,
not composition, not Gumbel, not randomness.

**SEED LOCALIZED (2026-07-23, `compare_block.py` vs AL `block_decision_trace`).** Per-iteration
social-block hybrid-choice rate, engine vs AL: 2025 22% vs 3%, 2030 39% vs 4%, 2035 48% vs 5%,
converging by 2040. `notTopEAC` (Gumbel deviation) matches (~4-8% both) → DETERMINISTIC, not
Gumbel. `NATURAL_GAS_BLOCK` ruled out (0% possible/chosen for social in both). The cause is the
block-level EAC: 2025 triggered social blocks have hybrid−gas gap **174 (engine) vs 251 (AL)** —
engine hybrid 2060 vs 2096, gas 1886 vs 1845. Social blocks sit right at the gas≈hybrid crossover
(marginal), so this ~77 EUR (~3%) difference flips ~7× as many to hybrid early, which the learning
feedback amplifies into the landlord/total gaps.

**Final component to pin (open):** which piece of the block-household hybrid EAC is ~77 EUR off —
candidates: low-temp distribution cost (`hasLowTemp` state of block households), insulation-to-C
cost (block household labels), heat demand of the specific TRIGGERED blocks, or a systematic
difference in block-age initialisation (which blocks trigger when). Next: dump block-household EAC
components (invest / low-temp / insulation / energy) for gas & hybrid, engine vs an AL traceln, on
the 2025 triggered social blocks. Engine probe: extend `HT_BLOCK` (already prints gasEAC/hybEAC per
year). NOTE: because social blocks are marginal, part of this may be irreducible sensitivity rather
than a discrete bug — quantify how much of the 6 pp closes if the block hybrid EAC is nudged by 77.

---

## 5. Per-year dynamics probe (HT_DYN) — `tests/compare_dyn.py`  [NEW, for continuing the chase]

Emits, at the START of each year (decision-time), per heating type: learned `invest` (medium) +
`salience` (+ cumulative/initial on the engine side). Same convention both sides, so `ENGDYN <year>`
== `ALDYN <year>` when dynamics agree; year 1 = base state.

- **Java engine (primary; JS retired):** `HT_DYN=1` env var → `ENGDYN` lines on **stdout**
  (`Simulation.java` top of `stepYear`). (JS `modelFull.js` also has it on stderr, kept only as
  the reference oracle.)
- **AnyLogic:** paste `deprecated/al-parity/ALDYN_HTDyn.java` into the model, add `HTDyn.emit(v_year, this);` as the
  FIRST line of `Main.f_adoptionProces`. Writes `al_dyn.log` (override `-Dht.dyn.file=`), gated on
  the `HT_DYN` env var (must be set in the run environment, else nothing emits — look for the
  console line `HTDyn: writing ALDYN lines to ...` to confirm it's on).

**IMPORTANT: `al_dyn.log` has NO scenario tag.** Use a BASELINE-only run for the comparison — a
multi-scenario analysis mixes all scenarios into one file and `compare_dyn.py` would average across
them. The file truncates once per process, so a fresh baseline run overwrites any earlier mixed log.

```
cd engine-java && HT_DYN=1 java -Xmx4g -cp build heattransition.Cli \
    --real ../data/stock/limburg_dwellings.csv --scenario baseline --iterations 20 \
    --out java_baseline.csv > eng_dyn.log        # ENGDYN on stdout
# run AL baseline with HT_DYN=1 -> al_dyn.log
python tests/compare_dyn.py engine-java/eng_dyn.log al_dyn.log --type HYBRID_HEAT_PUMP
```

**Preliminary (engine ENGDYN vs AL learning_curve as an invest proxy):** decision-time hybrid
invest agrees at 2025 (6000), then the engine's factor drops slightly faster (engine ~85 EUR
cheaper by 2030). Confirms the feedback carries the seed. **Next step:** run the REAL `ALDYN` probe
(includes salience) to check whether salience or invest diverge year-by-year, and if both track,
move to instrumenting the per-agent CHOICE (how many homeowners/landlords/blocks pick hybrid each
year, engine vs an AL traceln) to find the year-1 ~2× hybrid seed.

---

## 6. Artifacts / scripts index

| file | purpose |
|---|---|
| `setup-java.sh` | install JDK 17 in the sandbox without root |
| `deprecated/al-parity/ALDIAG_HTDiag.java` | AL term-level probe (paste into model); writes `al.log` |
| `deprecated/al-parity/ALDYN_HTDyn.java` | AL per-year dynamics probe (paste into model); writes `al_dyn.log` |
| `tests/compare_diag.py` | 3-way term comparison (JSDIAG/DIAG/ALDIAG); UTF-16-safe, artifact guard |
| `tests/compare_dyn.py` | per-year invest+salience diff (ENGDYN/ALDYN) |
| `tests/compare_block.py` | per-year block-choice diff (engine `HT_BLOCK=1` ENGBLOCK vs AL `block_decision_trace_*.csv`) |
| `tests/compare_engines.py` | aggregate within-owner heating shares vs AL |
| `engine/dumpDwellings.js` `dumpBlocks.js` `dumpLearningCurve.js` | t0 / learning-curve dumps to match AL's `*_dump_*.csv` / `learning_curve_*.csv` |

Engine probes are env-gated (`HT_DIAG=1`, `HT_DYN=1`, `HT_BLOCK=1`), zero cost otherwise.

**HT_BLOCK (block-choice probe) — next step for the social-block seed.**
- Engine: `HT_BLOCK=1` → `ENGBLOCK <year> <SOCIAL|HOA> triggered=.. households=.. notTopEAC=..
  chosen: TYPE=count ...` on stdout (`Simulation` block loop).
- AnyLogic: the trace already exists (`f_recordBlockDecision`); it just isn't written. Add one
  line `f_exportBlockDecisionTrace();` next to `f_exportLearningCurve();` (Main.java ~3829), run
  **baseline 1 iteration** (the trace is a static list that accumulates across iterations, so use
  1 iter for a clean file), → `results/block_decision_trace_*.csv`.
- Compare: `python tests/compare_block.py eng_block.log results/block_decision_trace_*.csv --type SOCIAL`.
  Shows, per year, how many social blocks trigger, how many pick hybrid, and the `notTopEAC`
  (Gumbel-deviation) count — engine vs AL. If the per-year hybrid-choice counts diverge with
  matching triggers, the social-block decision is the seed; if `notTopEAC` fractions match, Gumbel
  is confirmed equal in practice (as the scales already are).
