# results/ — simulation outputs

Generated outputs of the model. **Everything here except this README is git-ignored** — it is
all reproducible from `data/` + `model/`.

Layout, one subfolder per run scope (created automatically by `model/run.py`):

```
results/
  <scope>/                     e.g. limburg/, nl/, gemeente_maastricht/
    simulation_results.csv     the Java engine output (same schema as the old AL model)
    plots/                     figures from results_analysis/script_results.py (with --analyze)
```

Produce a run + its figures in one command:

```
cd model
python run.py --scope province:Limburg --scenario all --iterations 20 --analyze
```

`--out <path>` overrides the CSV location; without `--analyze` only the CSV is written.
The three top-level concerns are kept separate: **inputs = `data/`**, **code = `model/`**,
**outputs = `results/`** (analysis code lives in `results_analysis/`).
