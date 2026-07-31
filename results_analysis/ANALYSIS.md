# Results & scenario analysis

How the simulation results are produced, how to (re)generate every figure, what each output shows,
and the first-pass interpretation of the current Limburg runs. These are **illustrative** runs — the
behavioural weights are not yet calibrated (see `model/MODEL_TODOS.md` Q3/Q4), so read the numbers as
mechanism-level, not forecasts.

---

## 1. How results are produced

The pipeline is `data → export → Java engine → results CSV → Python analysis`, orchestrated by
`model/run.py`:

```
python run.py --scope province:Limburg --scenario all --iterations 20 --analyze
```

- **build + test** the engine, **provision** the stock CSV for the scope if missing, **simulate** all
  16 scenarios, write `results/<scope>/simulation_results.csv`, and (with `--analyze`) plot into
  `results/<scope>/plots/`.
- `--timestamp` archives a run under `results/<scope>/<yyyymmdd_hhmmss>/` instead of overwriting.
- The results CSV has **one row per** `scenario × iteration × year × heating_system × ownership`
  (ownership ∈ PRIVATELY_OWNED / PRIVATELY_RENTED / SOCIAL_HOUSING / HOME_OWNER_ASSOCIATION / TOTAL),
  carrying stock counts, annual installs/removals, `nbh_with_dh_perc`, `nbh_in_grid_congestion_perc`,
  and the decision means (`avg_att/util/sub_norm/eac/pbc`).

---

## 2. How to run the analyses

All analysis reads an existing `simulation_results.csv`, so **plot-only changes need no re-simulation**
— only engine changes do. Use the analysis venv and **absolute paths** (the scripts resolve relative
`--input` against their own folder). From the repo root, PowerShell:

```powershell
# 1. main figure set (stacked mixes, comparisons, detail/EAC, etc.)
.\results_analysis\.venv\Scripts\python.exe .\results_analysis\script_results.py `
  --input "$PWD\results\limburg\simulation_results.csv" --outdir "$PWD\results\limburg\plots"

# 2. district-heating deep-dive (grid supply vs. connections)
.\results_analysis\.venv\Scripts\python.exe .\results_analysis\dh_analysis.py `
  --input "$PWD\results\limburg\simulation_results.csv" --outdir "$PWD\results\limburg\plots"

# 3. cross-scenario spread of the 2050 mix (also prints a table)
.\results_analysis\.venv\Scripts\python.exe .\results_analysis\scenario_spread.py `
  --input "$PWD\results\limburg\simulation_results.csv" --outdir "$PWD\results\limburg\plots"
```

`run.py --analyze` runs script_results.py automatically after a simulation; the other two are run by
hand. `scenario_spread.py` and `dh_analysis.py` accept `--year` / `--ownership` to slice differently.

---

## 3. The 16 scenarios

Every scenario shares the same stock; they differ only in the switches below (id 11 is intentionally
skipped, mirroring AL). Learning factors scale how fast a technology's cost falls with cumulative
installs; DH/SHA strategy toggles whether district-heating expansion and social-housing choices are
COST_BASED or follow the municipal (TVW) POLICY plan; the obligation/ban are hard constraints.

| id | scenario | learning (SLF/ELF) | DH expansion | SHA | extra |
|---|---|---|---|---|---|
| 1 | baseline | MED / MED | cost | cost | — |
| 2/3 | social_learning_factor_low / _high | LOW/HIGH · MED | cost | cost | — |
| 4/5 | economic_learning_factor_low / _high | MED · LOW/HIGH | cost | cost | — |
| 6 | policy_driven_dh_strategy | MED/MED | **policy** | cost | — |
| 7 | policy_driven_sha_strategy | MED/MED | cost | **policy** | — |
| 8 | actor_allignment_strategy | MED/MED | **policy** | **policy** | — |
| 9 | grid_congestion_HP_ban | MED/MED | cost | cost | EHP banned under congestion |
| 10 | dh_policy_based_connection_obligation | MED/MED | **policy** | cost | **DH connection obligation** |
| 12 | individual_technologies | HIGH/HIGH | cost | cost | — |
| 13 | collective_technologies | LOW/LOW | **policy** | **policy** | — |
| 14/15 | individual/collective_tech_grid_congestion_ban | as 12/13 | | | + EHP congestion ban |
| 16/17 | individual/collective_tech_dh_connection_obligation | as 12/13 | | | + DH connection obligation |

---

## 4. Figures produced

In `results/<scope>/plots/`:

- **`stacked_percentage_plots/stacked_plot_<scenario>_mean_TOTAL.png`** — per scenario, the % mix of
  the 5 heating systems over 2024–2050 (stacked area). The headline "what happens" per scenario.
- **`combined_ownership_plot_<scenario>.png`** — per scenario, the mix split by ownership type.
- **`scenario_comparison_TOTAL.png`** — a **10-scenario** side-by-side of the TOTAL mix (baseline,
  learning-factor and policy/actor scenarios, individual/collective). The main cross-scenario view.
- **`scenario_comparison_district_heating.png`** — the **4 DH/policy** scenarios only (policy DH, policy
  SHA, actor-alignment, DH connection obligation). ⚠ three of these coincide — see §5.
- **`detail_values_<scenario>_PRIVATELY_OWNED.png`** — homeowners' average decision metrics
  (attitude, perceived utility, subjective norm, PBC, raw EAC) per heating system over time. Homeowner
  TPB only. *(Older per-ownership `detail_values_*` PNGs in the folder are stale leftovers; the current
  script writes homeowners only.)*
- **`eac_by_ownership_<scenario>.png`** — one figure, 5 panels: average EAC per heating system over
  time for each ownership type + TOTAL. Homeowner/renter panels are smooth; block panels are sparse.
- **`installed_current_all.png`**, **`considered_annually_by_ownership.png`** — installed stock and how
  many dwellings reconsidered per year.
- **`dh_analysis.png`** — DH deep-dive, 3 panels (all scenarios as lines): % neighbourhoods with a DH
  grid, % dwellings connected, and absolute dwellings connected.
- **`scenario_spread.png`** — cross-scenario spread of the 2050 mix (one dot per scenario per
  technology; grey bar = min–max range; red = mean).

---

## 5. Initial interpretations (Limburg)

**Scenarios diverge most on district heating and hybrid — they have *not* collapsed.** The
cross-scenario spread of the 2050 mix (from `scenario_spread.py`, TOTAL, % of dwellings):

| technology | min | max | range | mean |
|---|---|---|---|---|
| district heating | 0.6 | 41.3 | **40.7** | 6.0 |
| hybrid HP | 25.2 | 58.9 | **33.6** | 48.5 |
| electric HP | 22.9 | 45.6 | 22.7 | 33.9 |
| gas boiler | 4.4 | 25.3 | 20.9 | 11.6 |
| gas block | 0.0 | 0.1 | 0.1 | 0.0 |

The impression that "all scenarios look alike" comes from the
`scenario_comparison_district_heating.png` figure, which happens to plot three near-identical policy
scenarios (they barely move DH). Look at `scenario_comparison_TOTAL.png` and `scenario_spread.png` for
the real, wide spread.

**District heating is throttled by *connection*, not grid supply** (from `dh_analysis.py`). Grids
expand fine — to ~24% of neighbourhoods under cost-based expansion, ~12% under policy-based — but
voluntary uptake stays ~1% of all dwellings (~3% of dwellings *where a grid exists*). The same 24%
grid coverage yields 6,700 connections in baseline vs 225,000 under a **connection obligation**. So DH
only takes off when connection is mandated; otherwise heat pumps win the household choice. (Cost-based
expansion paradoxically builds *more* grids than the "DH strategy" policy scenarios, because it builds
wherever heat density justifies it rather than only where the TVW plan designates DH.)

**Learning factors are strong levers.** economic-learning-high drives the cheapest-tech outcome
(electric HP up to ~46%); social-learning-high leaves more gas (~25%) because peer conformity can lock
in the incumbent — a genuine tipping dynamic, not a bug.

**Gas block heating decays to ~0.** It is a closed category (a block can keep block heating or upgrade
to HP/DH, but can't fragment into individual boilers), and with heat pumps/DH eventually cheaper it
shrinks to a small residual by 2050.

**Grid-congestion scenarios currently equal their baselines.** Congestion is never detected yet (the
DSO/grid mechanism isn't ported — see MODEL_TODOS), so `*_grid_congestion_ban` ≡ baseline. This also
means Q8's congestion feedback loop can't be demonstrated until that port lands.

---

## 5b. Validation: the NL starting state against CBS national totals

The full-NL run's starting stock was checked against the CBS maatwerk table
(`Hoofdverwarmingsinstallaties_woningen_2022_2024.xlsx`, `Tabel 1`, `Soort regio = Land`), converted
to dwellings using the model's own stock size (8,467,974).

**Watch the vintage.** The engine initialises from the heating shares selected by
`-Dht.heatingYear` (**default 2023**), so the model's *first simulated year* (2024) carries the
**2023** mix. Comparing it against the CBS **2024** row therefore compares different years and
manufactures a ~2 %-point error. Against the correct (2023) vintage:

| system | CBS 2023 (renorm.) | model 2024 start | diff |
|---|---|---|---|
| natural gas boiler | 80.65 % | 80.85 % | **+0.20** |
| natural gas block | 5.60 % | 5.56 % | −0.04 |
| hybrid heat pump | 2.04 % | 1.93 % | −0.11 |
| electric heat pump | 4.79 % | 4.73 % | −0.06 |
| district heating | 6.92 % | 6.94 % | +0.01 |

**Max deviation 0.20 %-points** — the national starting state is reproduced essentially exactly. The
residual is rounding: CBS publishes buurt shares to whole percent, and the engine converts shares to
whole dwellings per neighbourhood (plus whole social/HOA blocks taking a single system).

Two notes on method:
- **Renormalisation.** CBS shares sum to 98.3 %, the remainder being `Type installaties onbekend`
  (1.7 %). The model has no "unknown" category, so the CBS shares are renormalised to 100 % before
  comparing — the same treatment used in the calibration.
- **The gas-boiler fallback does not distort the national total.** Neighbourhoods without CBS data
  default to all-gas, and 4,362 of 14,421 buurten lack data — but those buurten contain **zero
  dwellings** in the stock (they are water, industry, etc.). Every one of the 8,467,974 simulated
  dwellings sits in a neighbourhood with real CBS data, so the fallback affects nothing here.

*If the comparison is ever made against 2024, initialise the run with `-Dht.heatingYear=2024`.*

## 6. Caveats

- **Limburg is district-heating-poor.** Grid expansion keys off heat density, and Limburg is far less
  dense than the Randstad, so DH supply is low here; NL-wide or urban runs would show more DH grids and
  a higher DH starting point. The *connection* bottleneck, however, is behavioural and region-independent.
- **Grid congestion is not yet ported** — treat the three ban scenarios as placeholders.
- **Weights are uncalibrated.** All decision weights are the AL defaults; the sensitivity/robustness
  work (MODEL_TODOS Q3/Q4) is still open, so absolute shares are indicative.
- **Cross-platform reproducibility:** `Math.*` isn't bit-identical across OSes, so the same seed gives
  small differences (~1 %pt) between Windows and Linux runs. Not a modelling issue.
- **Block-EAC sparsity:** in the EAC-by-ownership figure the SHA/HOA panels are noisy because only a
  handful of blocks decide per year; years with no block decision are left as gaps. Fills in at NL scale.
