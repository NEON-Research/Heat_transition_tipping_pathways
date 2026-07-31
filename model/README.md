# Running the model

The research engine is the Java model in `engine-java/` (`heattransition.Cli`). You almost never call
it directly — **`run.py`** is the driver: it resolves a scope to a stock file, provisions data if
missing, **rebuilds + tests the engine so a run never uses stale classes**, runs the simulation, and
(optionally) plots the results.

```bash
python run.py --scope <spec> [run.py flags] [engine args]
```

Run it from `model/` (paths are resolved relative to `run.py`, so any working dir also works).

## Quick start

```bash
# Province, both a fresh CSV and fresh plots (the common case):
python run.py --scope province:Limburg --scenario all --iterations 20 --analyze

# Whole Netherlands (~9.7M dwellings — memory heavy, give it heap):
python run.py --scope nl --scenario all --iterations 5 --analyze --xmx 12g

# A single municipality, baseline only:
python run.py --scope gemeente:Maastricht --scenario baseline --iterations 10 --analyze
```

> **Always pass `--analyze` if you want the plots refreshed.** Without it, only the results CSV is
> written — the PNGs under `results/<scope>/plots/` are left untouched (a stale plot is the #1 cause
> of "why didn't my results change?"). And **don't add a custom `--out`** unless you know why: it
> writes the CSV somewhere other than `results/<scope>/simulation_results.csv`, which is the path the
> plotting and downstream analysis read from.

## `--scope` (required)

| spec | meaning |
|---|---|
| `nl` | whole Netherlands |
| `province:<Name>` | e.g. `province:Limburg`, `province:Noord-Brabant` |
| `gemeente:<Name>` | a municipality, e.g. `gemeente:Maastricht` |

If the matching `data/stock/<tag>_dwellings.csv` is missing it is provisioned from `households.db`
(git-ignored — must exist locally). Force a re-export with `--force-export`.

## `run.py` flags

| flag | default | what it does |
|---|---|---|
| `--scope <spec>` | *(required)* | region to run (see table above) |
| `--scenario <name>` | `baseline` | `baseline`, `all` (the full 16-scenario matrix), or one scenario name |
| `--iterations <n>` | `1` | Monte-Carlo iterations (stock, networks, attitudes redrawn each iter) |
| `--analyze` | off | after a clean run, plot the CSV into `results/<scope>/plots/` |
| `--timestamp` | off | archive to `results/<scope>/<yyyymmdd_hhmmss>/` instead of overwriting; writes a `LATEST` pointer |
| `--out <path>` | `results/<scope>/simulation_results.csv` | exact CSV path (overrides the default; plots then go next to it) |
| `--xmx <heap>` | `8g` | JVM max heap (nl-scale needs 12g+) |
| `--skip-build` | off | skip the `gradlew build`+test step (fast dev iteration; risks stale classes) |
| `--force-export` | off | re-provision the stock CSV even if it already exists |
| `--weights <name>` | off | run a calibrated weight set from `calibration_search.json` — a representative key (`best_fit`, `low_shareAffordability`, `high_shareAffordability`), `representative` (all three, deduped), `retained:<i>`, or `all` to sweep every retained set. Injects that set's `-Dht.*` into the engine. |
| `--weights-file <path>` | `results/calib/calibration_search.json` | which search JSON `--weights` reads |

## Engine args (passed straight through to `Cli`)

Anything `run.py` doesn't recognise is forwarded to the Java engine:

| arg | default | what it does |
|---|---|---|
| `--scenario <name\|all>` | `baseline` | as above (run.py forwards it) |
| `--iterations <n>` | `1` | as above |
| `--start <year>` | `2024` | first simulated year (end is 2050) |
| `--every <n>` | `1` | subsample the stock — keep every *n*-th dwelling (quick smoke tests) |

Diagnostics are environment-gated (zero cost when off), e.g.
`HT_DIAG=1` (per-year TPB terms for gas vs hybrid), `HT_EACPROBE=1` (the global EAC cost-scale window
and example dwellings), `HT_DYN=1`, `HT_BLOCK=1`. `HT_SEEDOFFSET=1000` (or `-Dht.seedOffset=1000`)
draws a fresh, independent Monte-Carlo set.

## Plotting a run you already have (running `--analyze` after the fact)

`--analyze` just calls the plotting script on the run's CSV. If you forgot the flag, you don't need to
re-simulate — point the script at the CSV yourself. It writes the same figures into `--outdir`:

```bash
# from model/ , using the analysis venv (has pandas + matplotlib):
../results_analysis/.venv/Scripts/python.exe ../results_analysis/script_results.py \
    --input ../results/<scope>/simulation_results.csv \
    --outdir ../results/<scope>/plots
```

On Windows PowerShell the equivalent for the NL run is:

```powershell
..\results_analysis\.venv\Scripts\python.exe ..\results_analysis\script_results.py `
    --input ..\results\nl\simulation_results.csv `
    --outdir ..\results\nl\plots
```

`--outdir` is exactly what `--analyze` uses (`<dir of the CSV>/plots`), so the result is identical to
having passed `--analyze` in the first place.

## Running calibrated weight sets (the "explore" step)

After `calibrate_weights.py search` writes `results/calib/calibration_search.json`, run the
2024–2050 scenarios with those weights instead of the AnyLogic defaults — no manual `-Dht.*` or
environment variables needed:

```bash
# the single best-fitting set:
python run.py --scope province:Noord-Brabant --scenario all --iterations 20 --analyze --weights best_fit

# the 3 representative sets that bracket the band (deduped -> the quick band):
python run.py --scope province:Noord-Brabant --scenario all --iterations 10 --analyze --weights representative

# the whole retained ensemble (one run per set -> the full pathway BAND):
python run.py --scope province:Noord-Brabant --scenario all --iterations 10 --analyze --weights all
```

`--weights` reads the set(s) from the search JSON and passes each weight to the engine as `-Dht.<name>`.
Outputs are organised per set under `results/<scope>/calib/<label>/` (e.g. `.../calib/best_fit/`,
`.../calib/set00_mad0.21/`), each with its own `plots/` when `--analyze` is passed. `--weights all`
runs every retained set in turn, so you can plot the ensemble as a band rather than a single line;
`--out` is rejected with `--weights all` since each set needs its own file. Point at a different
search file with `--weights-file`. Compare the sets afterwards (no re-simulation needed) with
`results_analysis/compare_weight_sets.py --scope <scope> --scenario <name>`, which writes a per-scenario
summary CSV and a min-max band figure across all `calib/*` sets. Use many `--iterations` here (the 2024–2050 pathways are
path-dependent); 1 iteration is only enough for the 3-year calibration objective.

## Outputs

Inputs = `data/`, code = `model/`, outputs = `results/` (git-ignored). A default run writes/overwrites:

```
results/<scope>/simulation_results.csv     # rows: scenario × iteration × year × heating system × ownership
results/<scope>/plots/*.png                # only when --analyze was passed
```

Ownership rows are `PRIVATELY_OWNED / PRIVATELY_RENTED / SOCIAL_HOUSING / HOME_OWNER_ASSOCIATION /
TOTAL`. Engine internals (execution model, class reference, the affordability/EAC cost scale) are in
[`engine-java/ARCHITECTURE.md`](engine-java/ARCHITECTURE.md); calibration in
[`../results_analysis/CALIBRATION_AND_VALIDATION.md`](../results_analysis/CALIBRATION_AND_VALIDATION.md).
