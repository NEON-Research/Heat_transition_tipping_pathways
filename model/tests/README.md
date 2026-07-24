# Tests — how to run and use

Two levels of test, both runnable now with only Python + pandas.

```
tests/
  heat_model_ref.py           # executable spec of the core formulas (source of truth)
  test_heat_model_spec.py     # exact formula tests (hand-verified values)
  extract_reference_metrics.py# results CSV -> compact golden metrics JSON
  compare_to_golden.py        # engine CSV vs golden, statistical tolerance
  golden/baseline_metrics.json# reference metrics (regenerate from your canonical run)
```

## 1. Formula spec (exact)

```bash
python test_heat_model_spec.py          # standalone, no deps
# or
pytest test_heat_model_spec.py
```
21 tests locking attitude, effort, EAC normalisation, PBC, subjective norm, intention,
perceived utility, salience factor, learning curve, triggers (incl. the Java integer-division
opportunity trigger), and the Gumbel/RUM choice. When you build the Java engine, mirror these
as JUnit tests against the engine's decision functions — same numbers.

## 2. Golden master (statistical)

Freeze your canonical AnyLogic run as the reference:
```bash
python extract_reference_metrics.py \
    "../deprecated/Heat transition tipping pathways/results/<your_canonical>.csv" \
    -o golden/baseline_metrics.json
```

Compare a new engine run (same CSV schema, ideally ≥10 iterations/scenario):
```bash
python compare_to_golden.py engine_out.csv --golden golden/baseline_metrics.json
# add -v to see passing rows; --scenario baseline to focus; exit 0 = pass, 1 = fail
```

Tolerance: `|new − golden| ≤ k·std + rel·mean + floor` (defaults `k=4`, `rel=0.05`,
`floor=500`). Tighten with `--k 2 --rel 0.03` as the engine matures.

### Requirements
```bash
pip install pandas
```

### Notes
- The golden shipped here was built from `simulation_results_non_stochastic_limburg.csv`
  (30 iterations). Confirm that is your canonical configuration, or regenerate from the file
  that is.
- Self-checks performed: comparator passes 100% against its own golden, and correctly flags a
  genuinely different model version — so it detects real regressions, not just noise.
