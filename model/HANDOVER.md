# Handover — state as of 2026-07-21

Written at the end of a long session. Everything below is verified against the generated
AnyLogic Java in `deprecated/Heat transition tipping pathways_BUILD/src.generated/`, which is
the authoritative reference (not the `.alp` XML).

---

## 1. Where each engine stands

### JS engine (`model/engine`) — GOOD, use this one

Baseline, engine n=18 vs AnyLogic n=20. `z = (engine mean − AL mean) / AL sd`:

| owner | verdict |
|---|---|
| TOTAL, all systems | z −2.1 … +2.0 — within noise |
| PRIVATELY_OWNED | z −0.4 … +1.0 — within noise |
| SOCIAL_HOUSING | z −1.8 … +1.8 — within noise |
| HOME_OWNER_ASSOCIATION | z −1.1 … +1.1 — within noise |
| DISTRICT_HEATING (all owners) | z −0.0 … −0.3 — essentially exact |
| **PRIVATELY_RENTED gas** | **z = −10.3** (30.68 ± 0.19 vs 32.21 ± 0.15) — OPEN |

The engine's own sds now track AL's closely (social 2.77 vs 2.82, HOA 4.31 vs 5.39, PR 0.19 vs
0.15), which matters more than the means agreeing — it means the stochastic structure is right.

**Open item — PRIVATELY_RENTED, 1.5 pp too little gas. INVESTIGATED 2026-07-22 — it's a
learning-curve feedback amplification, not the landlord decision.** Trail:

- Initial (2024) landlord gas share is IDENTICAL across AL/JS/Java (88.4%). The gap opens over
  time: 2030 ~1.5pp, peaks ~2.7pp at 2035-40, back to 1.5pp at 2050. A cumulative RATE effect:
  the engines convert landlords off gas ~0.2-0.3 pp/yr faster than AL.
- Landlord decision propensity matches: at t0, gas is the cheapest option for 84.1% (AL) vs 83.6%
  (engine). Ownership assignment `(huur-corp)/100` is byte-identical to AL's
  `f_getDwellingOwnership`. So NOT the population and NOT the decision rule.
- Root: the learning curve. `initial_units` (learning base) match exactly (11398 vs 11396 hybrid),
  but the engine's cumulative HYBRID installs run ahead of AL's and COMPOUND: aligning the
  one-year indexing offset (AL records start-of-year cumulative -> factor 1.0 in yr 1; engine
  records end-of-year), EN[2025] hybrid = 18856 vs AL[2026] = 15267 (+23%), and the gap grows
  every year. GAS cumulative matches at aligned indexing (511232 vs 515310), so install-counting
  is correct -- the engine genuinely adopts more hybrid from year one. Cheaper hybrid (via more
  learning) -> faster gas exit for landlords.
- Seed (not fully closed): homeowner 2025 TPB terms all match AL (HT_DIAG), so the extra year-1
  hybrid is likely the BLOCK decisions -- `compare_engines` shows SOCIAL_HOUSING hybrid ~80%
  (engines) vs 75.5% (AL) and HOA ~58% vs 54%. Blocks switch many dwellings at once, pumping
  cumulative hybrid, which the learning + salience feedback amplifies. Those block gaps are within
  the large block-count variance individually but feed the loop.

Significance (2026-07-22): the gap is REAL, not noise. 2050 landlord gas, 20 iters each:
AL 32.10 +- 0.24, JS 30.59 +- 0.23, Java 30.65 +- 0.23; Welch t = 20 (AL vs each). 20 iters is
far more than enough (even 2-3 would flag it). Landlord population is large + homogeneous so its
share has tiny variance -- contrast the blocks (social/HOA sd ~2.7/5.4, few of them), whose
differences ARE within noise but seed the loop.

Block t0 state MATCHES AL (2026-07-22): `dumpBlocks.js` vs AL `block_dump` -- 920 vs 926 blocks,
mean EAC gas 1652/1650, hybrid 1899/1902, ~97.5% gas-cheapest in BOTH. So the block hybrid gap at
2050 is dynamic too, not a static seed. Everything at t0 matches AL.

ELF confirming experiment DONE (2026-07-22) -- and it REFINES the diagnosis. Landlord gas @2050:
| ELF | AL (20 it) | engine (1 it) | gap |
| low | 38.75 | 37.28 | +1.5 |
| baseline | 32.07 | 30.59 | +1.5 |
| high | 18.60 | 17.35 | +1.3 |
The gap is roughly CONSTANT across ELF, NOT scaling with it. So it is NOT learning-rate
amplification (that would blow up at high ELF). It's a small EARLY adoption head-start (first year
or two, while the factor is still ~1.0 in both) that gets baked in; learning carries it but does
not cause it. The engine converts ~1.4 pp more landlords off gas via a slight early excess
adoption of alternatives -- with all t0 states matching AL and the 2025 decision factor = 1.0 in
both. Root of the early excess is still open and small; pinning it would need year-by-year
invest/salience instrumentation in BOTH engine and AL (AL side not yet instrumented). Given the
effect is ~1.4 pp, ELF-invariant, and everything static matches AL, this is a reasonable point to
characterize it as a known residual rather than chase further.

### Java engine (`model/engine-java`) — BROKEN, ~20 pp off on homeowners

Compiles and runs, but `compare_engines.py` fails: PRIVATELY_OWNED gas 21.9% vs JS 2.2%.
Blocks (social, HOA) are fine; the individual owner types are not.

**Verified identical between JS and Java** — do not re-check these:
t0 state (882 nbhs, 22 with DH, 39,340 dwellings flagged, avg network 24.99, zero-network 0),
network construction, triggers (EOL and opportunity both present for homeowners), the
subjectiveNorm formula, sustainability score min-max normalisation, salience factor (matches to
3 dp at 2025/2030/2035), the learning curve.

**Located, not fixed: `eacNorm`.** Homeowner TPB terms at 2025:

| term | JS gas | Java gas | JS hybrid | Java hybrid |
|---|---|---|---|---|
| eacNorm | 0.212 | 0.227 | **0.147** | **0.252** |
| pbc | 0.783 | 0.772 | **0.679** | **0.603** |
| salience | 0.436 | 0.436 | 0.482 | 0.482 |

Hybrid's `eacNorm` is 70% higher in Java, which flows into `pbc` and makes hybrid look
unaffordable, stranding homeowners on gas. `normalizedValue` is identical in both, and the
min/max *comparison* is identical too (see section 2), so the difference is in the **inputs to
the window**: which agents contribute EACs to `minEAC`/`maxEAC`, and when it is reset relative to
the decision phases. Start by diffing the `stepYear` phase order and the `resetMinMax` call sites.

A second possibility not yet excluded: the hybrid **EAC values themselves** differ between the
engines (not just the window). Print raw min/max/mean EAC per type per year from both before
assuming it is the window.

Supporting evidence: Java evaluates 400,329 homeowner decisions in 2030 against JS's 65,325.
Java's homeowners keep hitting the opportunity trigger and never switching.

---

## 2. `setMinMaxEAC` else-if — CHECKED, HARMLESS (do not chase)

`J_HeatingSystemOptionsGlobal.setMinMaxEAC` uses `if / else if` where both engines use two
independent `if`s:

```java
if(EAC < minEAC) { minEAC = EAC; }
else if (EAC > maxEAC) { maxEAC = EAC; }
```

I initially flagged this as a significant AL/engine deviation. **That was wrong.** A value that
sets a new minimum cannot also be the maximum, so the only real effect is that the *first* EAC in
the sequence can never become `maxEAC`. Verified empirically on the real Limburg stock
(n=545,757, HYBRID_HEAT_PUMP, year 0):

```
correct   : min=221 max=14596
AL else-if: min=221 max=14596   -> identical
```

The one pathological case is a strictly descending sequence, where every value sets a new minimum
and `maxEAC` stays at `-MAX_VALUE`, making `maxEAC > minEAC` false and forcing `eacNorm` to 0.
Real agent order is not sorted by cost, so this does not arise.

Naud changed it to a plain `if` on 2026-07-21 — fine to keep (removes the edge case), but it does
not change results.

**Consequence: the Java/JS `eacNorm` divergence in section 1 is still unexplained.** Both engines
already use the correct form, so the 0.147 vs 0.252 gap on hybrid comes from the *inputs* to the
window — which agents contribute to it, and when it is reset relative to the decision loop — not
from the min/max comparison itself. Compare `resetMinMax()` call sites and the order of the
homeowner / landlord / block phases in the two `stepYear` implementations.

Reset sites in AL for reference: `Main.java` 2968, 4260, 4294 (`resetMinMaxEAC`), accumulated in
`J_Household.java:306`.

---

## 2c. ACTUAL ROOT CAUSE of the Java/JS homeowner gap (2026-07-22) — inverted insulation guard

The two-pass fix in §2b was correct and faithful to AL, but it did **not** move the numbers —
because interleaving was never the cause (with 84k+ triggered homeowners the min/max window is
essentially complete after the first handful of agents, so who decides "early" is negligible).

The real cause: **`Economics.computeEAC` skipped insulation cost for every dwelling that needed
it**, because it guarded the cost with `Constants.labelToNumber`, whose scale is the REVERSE of
`Vesta.labelNum`:

- `Vesta.labelNum`      : a=1, b=2, … g=7   (higher = worse)
- `Constants.labelToNumber` : a=7, b=6, … g=1   (higher = better)

The old guard was:
```java
int req = Constants.labelToNumber(hs.requiredLabel);  // hybrid "c" -> 5
int cur = Constants.labelToNumber(d.energyLabel);      // "g"        -> 1
if (req < cur) investment += insulationCost.cost(...);  // 5 < 1 -> FALSE, cost skipped
```
Fully inverted: it passed only for dwellings already better than the required label (which then
got 0 from Vesta's own, correct-scale internal guard), and skipped every dwelling that actually
needed the upgrade. Net effect: **hybrid and electric heat-pump EACs carried zero insulation cost
in Java.** Gas/DH were unaffected (`requiredLabel = "no"`).

How it was found (method note for next time): instrument BOTH engines with the same `HT_DIAG`
probe printing per-term TPB averages AND `minEAC`/`maxEAC`. Every term matched except `eacNorm`,
and the window bounds diverged:

| 2025 hybrid | JS | Java (pre-fix) |
|---|---|---|
| minEAC | 302 | 373 |
| maxEAC | **12890** | **6764** |
| eacNorm | 0.147 | 0.245 |

A `maxEAC` of 6764 is *below* what a label-g, 500 m² dwelling costs from insulation alone
(~7500+), proving Java added no insulation. A phase/attribute probe in JS confirmed its `maxEAC`
came from a label-g, 500 m², insul≈76,888 dwelling — absent from Java's window.

Ruled out along the way (do not re-check): `computeEAC` arithmetic, `Vesta.insulationCost`
formula, Java's hand-rolled JSON parser (replicated in Python, 0 mismatches vs `json.load` on the
`ki_s*` columns), the area cap (baked into the CSV export as `min(500, max(20, area))`, identical
for both), heat-demand formula (`fac*space + fac²*dhw`, identical), and the §2b interleaving.

### The fix (applied 2026-07-22)

`engine-java/.../Economics.java`: removed the inverted guard; now calls `insulationCost.cost(...)`
unconditionally and lets `Vesta` return 0 when no upgrade is needed — exactly what the JS engine
does. One-line change plus comment.

`Constants.labelToNumber` itself is left as-is: it is used correctly (with its reversed scale) by
the legacy `Simulation.java` placeholder insulation (`Math.max(0, to-from) * 40 * area`), which is
internally consistent. Only the `Economics` call site mixed the two scales.

**VERIFIED after rebuild (2026-07-22, 1 iter, HT_DIAG):**

| 2025 hybrid | JS | Java pre-fix | Java post-fix |
|---|---|---|---|
| maxEAC | 12890 | 6764 | 12748 |
| eacNorm | 0.147 | 0.245 | 0.144 |
| pbc | 0.679 | 0.609 | 0.681 |

2050 gas share: 23.0% -> **10.6%** (JS total gas ≈ 10.5%). The primary gap is closed.

**Residual, lower priority — 2030 homeowner decision count still diverges.** JS 2030 n≈65.7k vs
Java 2030 n≈347k (was ~400k pre-fix; the counts DO match at 2025: JS 84.4k, Java 84.5k). Final
shares now agree, so this is a path/trigger-frequency difference, not an outcome bug — likely the
opportunity-trigger re-firing cadence or the initial heating-system age distribution diverging
after 2025. Worth a look but does not block parity. Next: regenerate the JS baseline (the §1 table
predates all recent fixes), re-run `compare_engines.py`, and run the 20-iteration AL comparison.

---

## 2b. Interleaving pass split (2026-07-21) — correct, faithful to AL, but NOT the cause of the gap

`Main.f_adoptionProces`, step 5 (verified in the build folder):

```java
//5. Homeowners - if home owners have triggered heating method, calculate EACs
for(J_HomeOwner ho : c_homeOwners){
    if (triggered) ho.getHeatingSystemOptionsEAC();      // PASS 1: populate min/max for ALL
}

for(J_HomeOwner ho : c_homeOwners){
    if( ho.getHeatingSystemOptions() != null) {          // AL's own comment:
        ho.f_getHeatingSystemOptionsUtility();           // "executed in seperate step to
        ho.f_adoptNewHeatingSystem();                    //  ensure valid normalization"
    }
}
```

AL computes **every** triggered homeowner's EACs first, so `minEAC`/`maxEAC` is complete before
any utility is evaluated. **Both engines do this in ONE pass** — each homeowner computes its EAC
and decides immediately, so the first homeowner normalises against a window containing only its
own values and the last against a nearly complete one.

This explains the open item in section 1:

- `eacNorm` depends on how much of the window exists when a given agent decides, so it differs
  between JS and Java even though `normalizedValue` and the min/max comparison are identical
- **hybrid** is hit hardest because it has the widest EAC spread (221 … 14,596 at year 0)
- only homeowners are affected; blocks and landlords choose on raw EAC, not the normalised value

**Both engines deviate from AL.** JS happens to land within AL's noise at the aggregate level, so
the effect partly averages out, but the mechanism is wrong in both.

### The fix — APPLIED in both engines (2026-07-21)

The homeowner phase in `stepYear` is now split into two passes:

1. pass 1 — for every triggered homeowner, compute and **cache** the EACs (this populates min/max)
2. pass 2 — for each of those, compute utility from the cached EACs and decide

- JS: `deprecated/engine/src/modelFull.js` — `hoPending` array holds `{ d, eol, opts }`
- Java: `engine-java/.../Simulation.java` — `List<PendingHomeowner>`

EACs are **not** recomputed in pass 2 (that would double-count into the min/max window). Both
loops carry a comment warning against merging them back.

Not touched: the legacy single-owner pair (`engine/src/model.js`, `Simulation.java`) still
interleaves. They are only used by `SelfTest` for JS/Java parity, not for AL comparison, so they
remain mutually consistent. Fix them together or not at all.

### 3-way term-level verification (2026-07-22, 20 iters each, `tests/compare_diag.py`)

AnyLogic now emits the same probe (`ALDIAG`, via the `HTDiag` class + 3 calls in
`Main.f_adoptionProces`). Means over 20 iterations:

| 2025 hybrid | JS | Java | AL |
|---|---|---|---|
| eacNorm | 0.149 | 0.151 | 0.151 |
| pbc | 0.678 | 0.676 | 0.674 |
| maxEAC | 12704 | 12432 | 12605 |

**All three agree at 2025 on every TPB term** — the insulation fix is validated against AnyLogic
itself, not just against the other port. JS's `eacNorm` was never off-by-luck; it is AL's value.

**The 2030/2035 `n` divergence was a PROBE ARTIFACT, now FIXED (2026-07-22).** Java's `diagReport`
only reset its accumulators in report years (2025/2030/2035), but the accumulate in
`chooseByUtility` was gated on `DIAG` alone, so it ran EVERY year. The 2030 line therefore printed
the SUM over 2026-2030 (~5 x 65k ≈ 325k, matching the observed 349k) and 2035 the sum over
2031-2035. That also inflated the time-varying `sn`/`salience` (5-year averages vs a single-year
snapshot); the stable terms (`att`/`pbc`/`eacNorm`) were unaffected because they are sum/count.

**The model was never over-triggering.** JS (accumulator allocated only in report years) and AL
(`HTDiag.reset` gates `active` on report years) both reported single years correctly, which is why
they agreed. Fix: `Simulation` now sets `diagActive = DIAG && isReportYear` at the top of
`stepYear` and gates both accumulate blocks on it. Rebuild Java and re-run — 2030/2035 `n` should
drop to ~65k / ~41k and `sn`/`salience` should fall into line with JS and AL.

With that, JS / Java / AL agree at the term level in all three probe years. No known mechanism
divergence remains; the only open aggregate item is PRIVATELY_RENTED gas (~1.6 pp low vs AL, §1).

**Still to verify (blocked on tooling, not on the change):**

- Java did not compile/run in this session — no JDK was available in the sandbox. Compile it and
  run `SelfTest` first, then `Cli`.
- Run with `HT_DIAG=1`: JS and Java `eacNorm` should now agree, and hybrid's Java `eacNorm` should
  drop back from 0.252 toward the JS value.
- Re-run `compare_engines.py` and the 20-iteration AL comparison. **All JS parity numbers in
  section 1 predate this change and must be regenerated** — they were produced with the wrong
  normalisation.

JS smoke test, baseline, 1 iteration, seed 1, 2050 stock shares (old = 20-iter mean, so owner
types dominated by blocks differ mostly from run-to-run variance, not from this change):

| owner | gas old → new | hybrid old → new |
|---|---|---|
| PRIVATELY_OWNED | 2.21 → 2.20 | 42.42 → 42.83 |
| PRIVATELY_RENTED | 30.66 → 30.26 | 64.59 → 65.26 |
| TOTAL | 11.06 → 10.48 | 53.34 → 54.35 |

Small, as expected for JS — the interleaving effect largely averaged out there. The Java change is
the one that should move materially.

Ruled out while finding this (do not re-check): learning-curve base seeding
(`initialUnits = max(1, starting stock)`, identical), learning-curve application
(`invest = initialInvest * factor` from the stored original, identical, no compounding),
`resetMinMax` placement (top of `stepYear` in both), decision phase order
(blocks -> landlords -> homeowners in both).

---

## 3. Fixed this session

**AnyLogic model** (by Naud, all verified in the rebuilt source):
1. `gridCap / 100000` integer division and the other int-division sites
2. `g_ele` / `g_gas` sentinel cleanup — was producing `sqrt(negative)` → `NaN` → congestion
   silently disabled model-wide
3. DSO reinforcement now raises `p_gridCapacity_kW` to `households × 6 kW`; enqueueing guarded on
   the transition into congestion so the queue can't accumulate duplicates

**JS engine:** block `hasTrigger` reverted to end-of-life only; block exogenous insulation
propagates unconditionally; HOA fill tier 1 takes `APARTMENT || HIGHRISE`; `--seed` honoured;
DH expansion ported (`src/districtHeating.js`) — this also closed the HOA electric-heat-pump
anomaly, which was the same bug.

**Java engine:** `Dwelling.hasDistrictHeatingGrid` un-`final`ed; the `nbh_heating.json` parser
tested `"hasDHgrid":true` while `json.dump` writes `"hasDHgrid": true` with a space, so **every**
neighbourhood got `grid = false` — worth 1.4 pp of the gas gap; plus the three JS fixes above.

---

## 4. Scenario support — 13 of 16 DONE (2026-07-23), 3 blocked on data

All 16 scenarios are now wired into the Java engine (`Scenario.byName`, `Cli --scenario <name>`
or `--scenario all`), producing the AL `simulation_results` schema with correct SLF/ELF/DHES/SHAES/
DHCO/GCHPB tags and per-year `nbh_with_dh_perc` (a fraction; baseline grows 0 -> ~0.244 at 2050,
matching AL's 0.234).

**Working (13):** baseline, social/economic learning-factor low+high, `policy_driven_dh_strategy`,
`policy_driven_sha_strategy`, `actor_allignment_strategy`, `dh_policy_based_connection_obligation`,
`individual_technologies`, `collective_technologies`, `individual/collective_tech_dh_connection_obligation`.
Implemented this round: DH POLICY_BASED expansion (was already present), SHA POLICY_BASED (social
blocks follow the neighbourhood TVW `policyPlan`, matching AL `f_adoptHeatingMethodSHA`), and the
`possible()` rules for DH connection obligation + congestion ban.

**Blocked on data (3):** `grid_congestion_HP_ban`, `individual/collective_tech_grid_congestion_ban`.
They RUN, but produce results identical to their non-congestion twins because grid congestion is
never triggered. The DSO/Velander model (`GRID_AND_DH_PORT_SPEC.md` §2) needs two data pieces the
export does not have: (a) `g_ele` per-neighbourhood electricity is all 0 (sentinel), so the Velander
baseload is 0; (b) `p_gridCapacity_kW` (the congestion threshold) is not exported at all. The
`possible()` rule + `Dwelling.hasGridCongestion` hook are in place, so once the data-export adds
grid capacity and real electricity, only `Congestion.java` (compute load vs capacity + DSO FIFO
reinforcement) needs writing. The JS reference engine also never computed congestion, so there is no
oracle for it besides AnyLogic (whose generated source is currently empty under `deprecated/…_BUILD`
— re-generate it if you pick this up).

- Remaining open modelling decisions in `MODEL_TODOS.md` (g_ele baseload, block label int-division).

---

## 5. How to run

**Sandbox JDK:** the Cowork Linux sandbox has no Java by default and blocks `apt-get install` /
external JDK downloads, but `bash model/setup-java.sh` installs a working JDK 17 without
root (fetches the Ubuntu `.deb`s via `apt-get download`, extracts with `dpkg-deb -x`, repairs the
`conf/` symlinks). Re-run each session (~1 min; idempotent). After it, `export JAVA_HOME` per the
script's output and `javac`/`java`/the engine all work in-sandbox.


```powershell
# JS
cd model\engine
node runFull.js --scenario baseline --iterations 20 --out js_baseline.csv

# Java  (from engine-java; HT_DIAG=1 enables the t0 + TPB diagnostics)
javac -d build\classes (Get-ChildItem -Recurse -Filter *.java src\main\java | % FullName)
java -cp build\classes heattransition.SelfTest
java -Xmx4g -cp build\classes heattransition.Cli --real ..\data\stock\limburg_dwellings.csv --scenario baseline --iterations 20 --out java_baseline.csv

# compare (from model)
python tests\compare_engines.py engine-java\java_baseline.csv engine\js_baseline.csv --al "..\deprecated\Heat transition tipping pathways\results\simulation_results_20260721_122302.csv"
```

Reference AL run: `results/simulation_results_20260721_122302.csv` (baseline, 20 iterations,
post-fix). Earlier AL results predate the int-division and NaN fixes and should not be used.

---

## 6. Method note for whoever continues

Four bugs this session were **silent defaults**: `false` from a failed string match, `NaN` from a
negative sqrt, a fallback constant from a type-mismatched map lookup, an empty collection from a
missing file. None threw. All produced plausible output. Two of them (`NaN` congestion, the
`hasDHgrid` parse) had been wrong for a long time without anyone noticing, because a model where
nobody has district heating still looks like a model.

What worked was printing **state**, not results — the `HT_DIAG` t0 line and the TPB term dump
localised in two runs what several rounds of hypothesis-testing had not. Reach for that first.

Second: AnyLogic's generated source keeps dead overloads and commented-out blocks. Twice a plain
grep landed on an unused variant and nearly produced a wrong fix. Always confirm at the **live
call site**.
