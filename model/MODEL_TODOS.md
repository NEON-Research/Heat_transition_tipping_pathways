# Model TODOs

Two kinds of item live here: **engineering / pipeline TODOs** (bugs + performance in the tooling,
safe to fix — top section), and **open modelling questions** (faithful ports of AL behaviour where
the question is whether that behaviour is what you want — fixing them changes results and
invalidates reference runs — numbered sections below).

---

# Engineering / pipeline TODOs — next week (added 2026-07-24)

## A. HOA collective natural-gas → boiler resets too quickly  *(conceptual — Naud to think through)*

**Not** an empty-figure / plotting problem. The HOA figure surfaces a real behaviour: HOA blocks on
**collective natural gas** flip back to an individual **gas boiler** too readily/quickly. Fixing it
needs a conceptual model adaptation (how a block's collective-gas state is represented and under
what conditions it may revert vs. switch to a low-carbon collective option), not a code tweak.

Parked deliberately until Naud has settled the concept. When ready, the likely touch points are the
`HousingBlock` decision path in `Simulation.stepYear` (the block loop) and the reversion/eligibility
rules there. Capture the intended semantics first, then change the rule, then regenerate references.

## B. `detail_values_*` figures are empty — **likely root cause found**

The engine writes the five TPB/economics columns as **hardcoded zeros**: in `Cli.java` the row
append ends with `...append(r.considered).append(",0,0,0,0,0")` — i.e. `avg_att, avg_util,
avg_sub_norm, avg_eac, avg_pbc` are all 0. The `detail_values` plots those very columns, so they
have nothing to draw. The old AL model populated them.

**Fix:** have `Simulation.YearRow` accumulate per-owner means of attitude / perceived utility /
subjective norm / EAC / PBC during `stepYear` (the `HT_DIAG` probe already computes attitude &
subjective-norm sums per group — reuse that accumulation), and emit them from `Cli` instead of the
`,0,0,0,0,0` literal. Then the detail figures populate. (Cross-check the magnitudes against the AL
golden while you're there.)

## C. Per-iteration sim time grows over a run (~125s → ~197s over 16 iters)

Iterations run in one JVM, so this is almost certainly **heap filling → GC pressure**, not real
work growing.

**Design constraint (Naud):** the per-iteration rebuild is *intentional* — peer networks and
household attributes are drawn stochastically each iteration, and that between-iteration variation is
the whole point of the Monte-Carlo run. So "load once and reuse the networks" is **rejected**: it
would collapse the variance we're trying to sample. Each iteration must redraw. The goal is only to
make that redraw not *accumulate* memory.

**Key premise (confirmed):** once a year's aggregate values are written into the results buffer, the
iteration's agent graph is genuinely dead — nothing needs to persist across iterations except the
small results `StringBuilder`. So if heap keeps climbing, something is either transiently doubling or
leaking; both are fixable without touching the stochastics.

Hypotheses, most likely first:

1. **Transient 2× peak during rebuild.** `Cli` does `ag = StockLoader.load(...)` at the top of the
   loop. The new ~8.4M-dwelling graph is fully built *before* the assignment drops the old
   reference, so for a moment **both** graphs are live → peak ≈ 2× graph. As that peak nears
   `-Xmx8g`, GC runs more often/longer → the creeping slowdown and the late spikes (159/178/197s).
   - **What "nulling" does:** set `ag = null` (and clear any sim references to the old agents)
     *before* calling `StockLoader.load(...)`. That makes the previous graph unreachable, so the GC
     can reclaim it *while* the new one is being built — peak stays ≈ 1× instead of 2×. It doesn't
     change any results (that iteration is already done and its numbers are stored); it just frees
     memory sooner. Low-risk, likely the whole fix.
2. **A genuine retention leak** — if heap still grows after nulling, something is holding old
   iterations alive. Check for any `static` cache or shared collection in `StockLoader` / `Vesta` /
   `Rng`, a field on `Simulation`/`Cli` not cleared between iterations, or listener/back-references
   in the peer network that keep the graph rooted. Find it with `-verbose:gc` (does *post-GC* live
   heap climb each iteration? → leak) or a heap dump on iter ~15.
3. If 1 & 2 are clean, it's just GC cost near the heap ceiling — raise `-Xmx` or tune G1; but that
   treats the symptom.

**First action (the hint):** run once with `-Xlog:gc*` (e.g. add it to the `java` args, or the `cli`
task's JVM args) and read the *post-GC* live-heap size each iteration:
- **flat** post-GC heap → it's only the transient 2× peak; `ag = null` before the reload fixes it;
- **rising** post-GC heap → a real leak, hunt the retained reference per #2 (heap-dump iter ~15).
This one measurement tells you which fix to apply before changing any code.

**Evidence (full NL, baseline, 20 iters, 2026-07-24):** per-iteration wall-time rises monotonically
131s → 215s (iters 17/19 at 215/211s), ~65% growth over the run — consistent with heap filling.
Meanwhile determinism is fine: agent count constant (8,467,974) and gas share stable ~14% across all
20 iterations, so results aren't drifting — only wall-time is. Whole run: 3094s (~51 min).

---

# Open modelling questions

Things found while porting that are **not** porting bugs. The engine reproduces AnyLogic's
current behaviour faithfully in each case; these are decisions about whether that behaviour is
what you want. Parked deliberately — fixing any of them changes results and invalidates the
reference runs.

---

## 1. `g_ele` / `g_gas` are suppressed for every neighbourhood — grid baseload is zero

**Status:** open, no action taken. Engine mirrors AL (baseload = 0).

`_neighborhoods_data_2023.csv` has `g_ele = -99999` and `g_gas = -99999` in **all 14,421 rows**
— CBS privacy suppression at buurt level, not a data-loading fault. After the 2026-07-21 cleanup
fix these become 0, so:

```java
JaarverbruikAansluitingen = p_households * 0;                    // = 0
PmaxHouseholds = 0.23e-3 * 0 + 0.016 * sqrt(0);                  // = 0
```

The Velander household baseload — normally the largest single component of a distribution-grid
load — contributes **nothing**. `f_getGridImpact` therefore returns heat pumps + EVs only.

### Why it matters

`f_setGridCapacity` *does* include a household baseload:

```java
PmaxHouseholds = PmaxPerHousehold * households * simultaneityBaseload * pow(1+growth, yearsInstalled);
```

So **capacity is sized for baseload + heat pumps + EVs, while load counts only heat pumps + EVs.**
The comparison `v_maxGridLoad_kW > p_gridCapacity_kW` is not like-for-like, and congestion is
systematically under-detected. In the 2026-07-21 baseline trace, capacity ~1490 kW against an
opening load of ~185 kW — most of that headroom is the missing baseload.

This directly affects the three `grid_congestion_ban` scenarios and any future calibration of
`GridReinforcementPace` (which does work: LOW/MEDIUM/HIGH → 50/100/150 nbh/year via
`f_setScenarios`, it is simply MEDIUM in all 16 scenarios).

### Options

1. **Source electricity consumption elsewhere** — a municipality- or district-level average
   scaled by household count, or a national per-household figure. Restores the intended
   Velander term.
2. **Drop the baseload from capacity too**, so both sides count heat pumps + EVs only. Internally
   consistent and needs no new data, but capacity then means something different from the
   phasetophase reference it cites.
3. **Leave it.** Congestion stays a heat-pump/EV-driven phenomenon. Defensible if congestion is
   meant to represent *incremental* electrification load, but then the capacity formula should
   say so.

Option 1 is the only one that preserves the model as documented. Whichever you pick, the three
ban scenarios need regenerating afterwards.

> `p_averageGasConsumptionTotal` is read nowhere outside generated boilerplate — dead data.
> No action needed, but don't spend time sourcing it.

---

## 2. Block average energy label uses integer division

**Status:** open. Engine supports both via `LEGACY_LABEL=1` (default = mirror AL).

```java
int avgLabelNb = roundToInt(sumLabelNumber / households.size());   // int / int -> floor
```

Both operands are `int`, so the division truncates and `roundToInt` is a no-op. Labels run
a=1…g=7, so flooring systematically awards blocks a **better** label than the true mean, which
lowers their heat-pump retrofit costs.

Correcting it (`Math.round((double) sum / size)`) moved HOA noticeably in testing. Genuine bug,
but it changes results — decide, then regenerate.

---

## 3. `f_setGridCapacity` — unused variable and a placeholder TODO

**Status:** cosmetic, flagged for completeness.

`double technicalLifetime = 30;` is declared and never used, and there's a
`//TODO: Check if we need steps for first Eerstvolgende normtransformator: Snom = 250 kVA`.
Suggests the function was mid-revision. If the intent was to round capacity up to standard
transformer sizes, that logic is still missing.

---

## 4. DSO reinforcement semantics (resolved 2026-07-21 — recorded for provenance)

Previously the DSO cleared `v_hasGridCongestion` without raising `p_gridCapacity_kW`, so
neighbourhoods re-congested the next year and the waiting list accumulated duplicates. Fixed:
capacity now rises to `p_households × 6 kW` on reinforcement, and enqueueing is guarded on the
transition into congestion. Congestion is now temporary (~2 years) rather than permanent.

Consequence worth remembering: the EHP ban now only bites during that ~2-year window, so the
three ban scenarios will differ from their baselines far less than a blanket prohibition would.
