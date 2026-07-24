# Pipeline & orchestration — design + rationale

The end-to-end flow is **build → provision → simulate → analyse**, orchestrated by `run.py`
(Python) around the Java simulation kernel. This doc explains the design choices behind that,
answering three questions.

```
 data/households.db ─(export,Py)─> data/stock/<scope>.csv ─(Cli,Java)─> results/<scope>/simulation_results.csv ─(script_results.py,Py)─> results/<scope>/plots/
        │                                 ▲                                                                                    ▲
    raw input                   data/reference/ lookups                                                            results_analysis/ (analysis code)
```

---

## 1. Building — when does it rebuild, and does `--skip-build` make sense?

**Gradle already tracks changes; you don't.** `gradlew build` is *incremental and content-aware*:
it hashes the source files and task outputs, and if nothing changed it reports every task
`UP-TO-DATE` and does essentially nothing (~1 s: daemon + up-to-date checks). So you never manually
track "did the code change" — Gradle recompiles **only** what changed, runs tests **only** if their
inputs changed, and is a near-no-op otherwise.

**Monte-Carlo iterations do NOT rebuild.** The `--iterations N` loop runs *inside one JVM* (one
`Cli` invocation loops N times). So a run builds **once**, then does all N iterations in that
single process — there is no build between iterations by construction. The build cost (and JVM
startup) is amortised across the whole batch, exactly as you want: "first-start build is fine, no
rebuild between iterations."

**So what is `--skip-build` for?** Only to skip even the ~1 s up-to-date check + daemon startup.
Useful for:
- a **parameter sweep** that calls `run.py` many times in a loop and you *know* the Java is
  unchanged — build once, then `--skip-build` for the rest; or better,
- push the sweep *into Java* (as `--scenario all` already does) so it's one JVM, one build, many
  runs — the preferred pattern.

**Anti-pattern to avoid:** never spawn a JVM (or a build) per simulation iteration. Keep "one
process, many iterations/scenarios." Recommendation: leave the default as build-first (cheap +
safe), use `--skip-build` only for tight iteration or in-Python sweeps.

---

## 2. Triggering a Java model from Python — does it make sense?

**Yes — this is the standard "orchestrator + compute kernel" pattern, and it fits here well.**
- Data prep (SQLite → derived CSV) is naturally **Python** (sqlite3, the VestaMAIS derivations).
- Analysis + plots are naturally **Python** (pandas, matplotlib — `script_results.py`).
- The simulation is **Java** (performance; it's the AnyLogic port and the source of truth).

Python bookends the Java compute: **provision data in → Java computes → analyse out.** Calling a
compiled kernel from a scripting orchestrator is common in scientific/ML pipelines.

**The interface is file-based (CSV) + exit codes** — language-agnostic, inspectable between stages,
reproducible, and trivially debuggable (you can look at the stock CSV or the results CSV directly).
That loose coupling is a feature, not a compromise.

Trade-offs, and how they're handled:
- *JVM startup per invocation* — amortised over N iterations, so negligible; just don't spawn per
  iteration.
- *Two languages* — acceptable, since each stage uses the language it's best in.
- *When to reconsider* — if the pipeline grows to many cached, parallel stages, a dedicated
  workflow tool (Snakemake / Make) may beat a hand-rolled `run.py`. Not needed yet.

Verdict: keep Python as the thin orchestration/glue layer around the Java kernel, with a clean
CSV/exit-code interface. Do **not** invert it (Java calling Python for data + plots would be worse).

---

## 3. Coupling `run.py` → `script_results.py` for immediate graphs — **IMPLEMENTED**

The natural last stage of the pipeline, wired in *optionally* and *loosely*:

- **`--analyze` flag on `run.py`.** After a **successful** engine run it invokes
  `results_analysis/script_results.py` on the just-written results CSV.
- **Failure isolation:** if plotting fails (bad backend, missing dep, odd data) the run *does not*
  fail — it warns and exits with the engine's return code. The simulation CSV is already written and
  valid; a multi-hour NL run is never sunk by a matplotlib hiccup.
- **Per-run output grouping:** plots land in `results/<scope>/plots/`, right next to that run's
  `simulation_results.csv`, so each run's CSV + figures stay together.
- **Right interpreter, automatically:** `run.py` prefers `results_analysis/.venv`
  (`Scripts/python.exe` on Windows, `bin/python` elsewhere) so pandas/matplotlib resolve; it falls
  back to the interpreter running `run.py` if that venv is absent.

**The two prerequisites are done:** `script_results.py` no longer hardcodes paths — it takes
`--input <results.csv>` and `--outdir <dir>` (argparse), and `generate_all()` runs each figure in an
isolated `step()` that *skips* plots whose scenario/ownership isn't present in this run. So a
single-scenario run produces what it can instead of crashing on the all-scenario figures.

**Shape:**
```
python run.py --scope province:Limburg --scenario all --iterations 20 --analyze
   -> build+test -> (provision) -> simulate -> results/limburg/simulation_results.csv
                                            -> results/limburg/plots/*.png
```

Stages stay independently runnable: `python results_analysis/script_results.py --input <csv>
--outdir <dir>` still works by hand on any results CSV.

## Folder layout (why outputs live outside the code)

Three separated concerns, orchestrated by `run.py`:
`data/` (raw inputs) → `model/` (pipeline code: export + Java engine) → `results/` (per-run CSVs +
plots, git-ignored) — with `results_analysis/` holding the analysis code (its own venv + historical
paper figures). `run.py` reaches across these folders; they are not merged, so exploratory/paper
analysis stays decoupled from the production engine and run outputs never pollute the source tree.
