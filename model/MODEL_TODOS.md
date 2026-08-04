# Model TODOs

A short, current overview: what is still **open**, and a one-line **done** log. Detailed method and
results descriptions live in `results_analysis/CALIBRATION_AND_VALIDATION.md` and
`model/engine-java/ARCHITECTURE.md`; plain bug fixes belong in git history, not here.

Last updated: 2026-08-03.

---

# Open

## 1. Restructure the paper around tipping mechanisms  *(Q9 — in progress)*

Organise scenarios **and** results by mechanism instead of by instrument, because the
`social_learning_factor_*` / `economic_learning_factor_*` "scenarios" are not policies — they are
discrete points on the same axes the sensitivity analysis varies, so presenting both duplicates the
evidence. Mirror the four mechanism families of the theory section:

| # | Mechanism | Varied |
|---|---|---|
| 0 | General exploration | Weight ensemble + full structural screen |
| 1 | Economic learning / increasing returns | HP learning rate, HP capex, **energy prices**, ELF |
| 2 | Social learning and thresholds | Salience curve, choice noise, SLF |
| 3 | Rules, regulations, infrastructure | DH obligation, congestion HP ban, DH/SHA strategy, GRR |
| 4 | Co-evolution / coordination | Actor alignment, individual vs collective technology sets |

- [x] Section 4 "Scenario analysis" rewritten as a mechanism table (`scenario_section_draft.tex`).
- [ ] **Restructure the Results section** into: general → economic learning → social learning →
      rules/infrastructure → coordination → **policy synthesis** (keep a policy-facing conclusion; a
      mechanism-first paper must not become policy-mute).
- [ ] Add the **adopter-segment layer** to each mechanism section (who leads, who lags, and why).
- [ ] Keep baseline as the reference pathway in every mechanism section.

## FIXED (2026-08-03): heat demand now responds to insulation

`Dwelling.heatDemandKWh` was **final** — frozen at the initial label — so a dwelling paid for
insulation but never received the energy saving. Fixed in three places:

1. `Vesta.spaceHeatKWh(archetype, year, label, area)` derives demand from the VestaMAIS table for any
   label, using the same formula as the stock export
   (`(vrv_<label>_asl + vrv_<label>_opp x area)/3.6x1000`). No data regeneration needed.
2. `Simulation.refreshHeatDemand()` re-derives demand from the dwelling's **current** label; the
   hot-water component and the per-dwelling stochastic factor are preserved, so only the space-heat
   part responds to insulation. Called at every label change: **autonomous/exogenous insulation**
   (individual and block, including the unconditional block propagation).
3. `Simulation.applyRequiredInsulation()` — a second, related bug: a system with a `requiredLabel`
   charges the insulation in its EAC, but the dwelling's label was never upgraded on adoption. The
   label is now improved on install (homeowners, landlords and blocks) and demand refreshed, so the
   upgrade that was paid for is actually delivered.

**Verified** (Maastricht, 1 iteration): mean energy label improves 3.09 -> 1.86 over the run and mean
heat demand falls 9,210 -> 8,572 kWh in step; gas-boiler EAC (which involves no insulation) falls
1,763 -> 1,647 at 2050, i.e. pure demand reduction. Electric-HP EAC at 2050 falls 1,735 -> 1,534
(-12 %), and the 2050 mix shifts gas 13.5 -> 11.1 %, hybrid 70.3 -> 73.9 %, electric 10.3 -> 11.0 %.

**Consequences:** heat pumps are now cheaper where insulation is required, the cost structure is no
longer near-linear in demand (the insulation feedback is the main non-linearity), and heat demand
should now differentiate the technology choice rather than scaling all options together.
**Re-run calibration and all scenarios** — the previous fit partly compensated for the missing saving
through the weights.

## 3. Production runs still to do

- [ ] **Full weight ensemble, Noord-Brabant** at `--iterations 5` (20 sets × 16 scenarios, ≈11 h).
- [ ] **NL headline run**: `pathway_batch.py --scope nl --iterations 3 --ensemble-weights
      representative --price-scenario baseline --xmx 48g` (≈4 h). NL is 8.47 M dwellings (7.1× NB);
      the **full** ensemble on NL is infeasible (~126 h) and unnecessary — NB carries the uncertainty
      analysis, NL carries the policy-relevant headline. NL needs *fewer* MC iterations (aggregate
      noise scales ~1/√N), so 3 is enough. **Memory, not time, is the constraint** — smoke-test heap
      first with a single scenario.
- [ ] Re-check the three congestion scenarios wherever they are reported — before the port they were
      no-ops, so any earlier numbers for them are void.

## 4. Sharpen the calibration objective *(optional, before the final ensemble)*

The objective is an unweighted MAD over technologies. Consider weighting by stock share or scoring
the *change* rather than the level, so a large stable category cannot dominate the fit. Not blocking —
the current ensemble is defensible — but worth a sensitivity check on the retained set.

## 5. Deferred — only if the spatial tipping story goes in this paper

- [ ] **Neighbourhood-level output** (`neighbourhood_state.csv`, sampled ~200 stratified
      neighbourhoods): per-year DH state, congestion, heat-demand density vs the ~600 GJ/ha business
      case. The DH and congestion thresholds are *defined* per neighbourhood, but only province-wide
      percentages are currently emitted, so "threshold crossed → uptake accelerates" cannot yet be
      shown spatially. Stratify by DH presence, demand-density band (the *near-threshold* stratum is
      where tipping happens), social-housing share and congestion status.
- [ ] Validate the grid power/simultaneity constants against Netbeheer Nederland figures. *Not
      required for the current paper* — the congestion scenario is illustrative and the combined
      values are data-validated.

---

# Done

**Calibration & validation** — full description in `results_analysis/CALIBRATION_AND_VALIDATION.md`.
- **Q3/Q4 — TPB-weight calibration.** History matching (GLUE) on Noord-Brabant 2022–24: LHS over the
  4 share-parameters, 20/100 sets retained within 2 %pts MAD, reported as an ensemble band. Morris
  screen ranks the weights; `converge` sets the replication count (1 suffices for the 3-year objective).
- **Structural sensitivity.** Separate Morris + LHS screen of process and techno-economic parameters
  at fixed calibrated weights, over the 2050 modal split, repeated at three weight vectors to confirm
  the ranking is weight-stable. Energy price, HP learning rate and HP capex dominate.
- **Q5 — energy prices.** Real annual growth per fuel wired into the EAC (`-Dht.gasPriceGrowth`,
  `elecPriceGrowth`; default 0 = static). Coupled low/high bracket derived from **KEV 2025 Table 10**
  2030 bandwidths, passed through to retail on the commodity component.
- **Replication analysis.** `pathway_convergence.py` sizes MC iterations for the 2024–2050 pathways
  (the 3-year calibration figure does not transfer — the pathway is ~15× noisier).

**Engine**
- **Affordability normalisation.** Per-technology → one **global logarithmic** EAC scale across all
  technologies and dwellings; removes an outright inversion and the dwelling-size dominance.
- **Stochastic equipment lifetime.** End-of-life ~ N(lifetime, 1) clamped ±3, redrawn per install;
  removes the synchronised replacement cohorts (the HOA "echo" and its blank years).
- **Grid congestion + DSO (ported).** `GridModel` — peak load = baseload + electric heating + EV, each
  diversified; congestion = load > capacity; DSO reinforces at the scenario rate. **`g_ele` is not
  used**: baseload comes from the actual dwellings and initial capacity is sized from t0 load, so
  congestion emerges from the transition rather than from a data artefact. The block is **policy-gated
  by design** (`gridCongestionHpBan`) — a DSO cannot stop a homeowner installing within their existing
  connection capacity. Verified: baseline outcomes are bit-identical with congestion on/off; the three
  ban scenarios are no longer no-ops.
- **Q8 — loop state + knock-outs.** `<out>_loop_state.csv` emits the decision-time loop intermediates
  (learned capex, salience, cumulative installs, price). Knock-out switches `learningRateMult=0`,
  `salienceFreeze`, `congestionOff` give causal evidence per loop; wired into `pathway_batch`.
- **Q6 (merged with old Q7) — per-segment tracing.** `Segments.java` + `<out>_segments.csv`: Rogers
  adopter categories from a propensity index (attitude + network + dwelling readiness) and dwelling
  segments (archetype-area-era-label), with the driver terms of the option **actually chosen**.
  Validated: reproduces the S-curve ordering.
- Reference data single-sourced to CSV; gas block closed as a category; per-ownership `avg_*` emitted;
  block-EAC no-trigger years emitted blank; agent graph released between iterations.

**Analysis tooling** (`results_analysis/`)
`calibrate_weights.py` (evaluate/search/morris/converge) · `structural_sensitivity.py` +
`structural_batch.py` · `pathway_batch.py` (ensemble + prices + knock-outs + analysis) ·
`analyze_all.py` (all scenario bands + global figures) · `compare_weight_sets.py` ·
`pathway_convergence.py` · `mechanism_analysis.py` (loop state × segments × knock-outs) ·
`paper_figures.py`.

**Key findings so far**
- Gas is phased out under **every** plausible parameterisation (2050: 7–24 %); the *route* — hybrid vs
  full electrification vs DH — is not identified. Report the band.
- **Electric heat pumps are an innovator/early-adopter technology**: 2050 share within segment runs
  99.7 % (innovators) → 0.0 % (laggards). This explains the aggregate ~35 % electric-HP plateau as
  **segment saturation**, not a cost ceiling.
- **Economics gate the pathway**: price, HP learning rate and HP capex dominate the structural screen;
  the learning-rate lever specifically decides hybrid vs electric.
- **Knock-out evidence**: disabling economic learning leaves 31 % on gas versus 13 % intact; the social
  loop contributes ~4 %pts. Both loops are causally necessary.
- Salience parameters matter **only** where the calibrated social-norm weight is high — a genuine
  weight × structural interaction, not a confound.
