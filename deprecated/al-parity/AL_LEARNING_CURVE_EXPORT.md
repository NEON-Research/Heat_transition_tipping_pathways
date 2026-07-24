# AnyLogic learning-curve export — code to paste

## Why

Year-0 block economics are **identical** between AnyLogic and the JS/Java engine (every option
within 0.1%, gas cheaper than hybrid in 97% of blocks in both). The models diverge over time:

| year | AL hybrid share | engine hybrid share |
|---|---|---|
| 2031 | 8.7% | 13.6% |
| 2034 | 14.7% | **23.1%** |
| 2037 | 23.2% | 33.7% |
| 2050 | 52.3% | 49.1% |

Same endpoint, but the engine gets there ~50% faster mid-run. Since cumulative installs drive
capex through the learning curve, and capex drives block EAC, this timing difference is the most
likely cause of the social-housing gap (AL 21% gas vs engine 43%).

This export makes the learning curve directly observable so we stop inferring it from adoption.

## The specific hypothesis to test

`f_updateGlobalCostsHeatingMethods` passes **cumulative installs**:

```java
int installedCumulative = resultsData.getHeatingMethod(system).getInstalledCumulativeIndex(yearIndex);
heatingSystemOptionsGlobalData.get(system).f_updateCapexFromLearningCurve(installedCumulative, p_economicLearningFactor);
```

but the base is the **year-0 stock**:

```java
initialUnitsInstalled = resultsData.getHeatingMethod(hs).getInstalledCurrentIndex(0);
```

and learning only happens when:

```java
if(unitsInstalled > initialUnitsInstalled) { ... }
```

Those are two different quantities. If `installedCumulative` starts near zero and counts install
*events*, then for hybrid heat pumps it must first climb past the entire year-0 hybrid stock
before **any** learning occurs. That is a threshold: capex holds flat, then drops and compounds —
which matches AL's late-but-steep tipping.

The engine currently seeds its cumulative counter *with* the year-0 stock, so the condition is
true from year 1 and capex declines immediately and gradually — matching the engine's
early-but-shallow tipping.

**If that is the difference, the export below will show AL's `capex_factor` sitting at exactly
1.000 for the first several years while the engine's is already below 1.**

---

## 1. Export function on Main

The commented-out `traceln` inside `f_updateCapexFromLearningCurve` already logs these fields —
this just writes them to CSV instead. Add to Main:

```java
// Collected each year by f_recordLearningCurve(); written once at end of run.
public java.util.List<String> learningCurveRows = new java.util.ArrayList<>();

public void f_recordLearningCurve(int yearIndex) {
    for (OL_HeatingSystem system : OL_HeatingSystem.values()) {
        J_HeatingSystemOptionsGlobal g = heatingSystemOptionsGlobalData.get(system);
        double cumulative = resultsData.getHeatingMethod(system).getInstalledCumulativeIndex(yearIndex);
        double current    = resultsData.getHeatingMethod(system).getInstalledCurrentIndex(yearIndex);
        double initial    = g.getInitialUnitsInstalled();
        // recompute the factor rather than storing it, so we see exactly what the formula yields
        double factor = 1.0;
        if (initial > 0 && cumulative > initial) {
            double rate = g.getEconomicLearningRate() * p_economicLearningFactor;
            factor = Math.pow(1 - rate, Math.log(cumulative / initial) / Math.log(2));
        }
        learningCurveRows.add(v_year + "," + system + ","
            + String.format(java.util.Locale.US, "%.0f", current) + ","
            + String.format(java.util.Locale.US, "%.0f", cumulative) + ","
            + String.format(java.util.Locale.US, "%.0f", initial) + ","
            + String.format(java.util.Locale.US, "%.6f", factor) + ","
            + String.format(java.util.Locale.US, "%.2f", g.getInvestmentCostsHeatSupplySystem_medium()));
    }
}
```

You may need small getters on `J_HeatingSystemOptionsGlobal` if these fields are private:

```java
public double getInitialUnitsInstalled() { return initialUnitsInstalled; }
public double getEconomicLearningRate()  { return economicLearningRate; }
public double getInvestmentCostsHeatSupplySystem_medium() { return investmentCostsHeatSupplySystem_medium; }
```

---

## 2. Call it once per year

In `f_runSimulation`, right **after** `f_updateGlobalCostsHeatingMethods` has run for the year so
the capex reflects this year's update:

```java
for(int i = 0; i < simulationYears; i++){
    v_year++;
    f_adoptionProces();
    ...
    f_recordLearningCurve(i);        // <-- add, at the end of the year loop
}
```

If `f_updateGlobalCostsHeatingMethods(yearIndex)` is called from inside `f_adoptionProces`, keep
the same `yearIndex` you pass there so the rows line up.

---

## 3. Write the file

Next to your other exports at the end of the run:

```java
public void f_exportLearningCurve() {
    String ts = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
    String path = "results/learning_curve_" + ts + ".csv";
    try (java.io.BufferedWriter w = new java.io.BufferedWriter(new java.io.FileWriter(path))) {
        w.write("year,heating_system,installed_current,installed_cumulative,initial_units,capex_factor,capex_medium");
        w.newLine();
        for (String r : learningCurveRows) { w.write(r); w.newLine(); }
        traceln("Wrote " + learningCurveRows.size() + " learning-curve rows -> " + path);
    } catch (java.io.IOException e) {
        traceln("learning curve export failed: " + e.getMessage());
    }
}
```

Single iteration, baseline. `learningCurveRows` accumulates across iterations, so clear it at the
start of each run if you do more.

---

## 4. Compare

```bash
node js_migration/engine/dumpLearningCurve.js --out /tmp/mylearning.csv
python js_migration/tests/compare_learning_curve.py \
    "Heat transition tipping pathways/results/learning_curve_<ts>.csv" /tmp/mylearning.csv
```

### Reading the result

- **`initial_units` differs** → we disagree on the base. Straightforward fix, and it changes every
  downstream capex.
- **`installed_cumulative` differs in level but not shape** → we disagree on what the counter
  counts: install *events* versus stock-plus-installs. This is the leading hypothesis.
- **`capex_factor` is 1.000 in AL for the early years while the engine is already below 1** →
  confirms the threshold effect. The fix is to seed the engine's cumulative counter at zero (or
  at whatever AL actually starts from) rather than at the year-0 stock.
- **Everything matches** → the learning curve is exonerated and the timing difference is in the
  homeowner decision loop instead (salience factor or subjective norm strength), which is the
  next thing to instrument.

One caution: `installed_cumulative` and `initial_units` are only comparable if both models have
the same year-0 stock per technology. We verified that earlier from the dwelling dump (gas
3482 / block 637 / electric 85 / hybrid 42 in both), so any difference here is genuinely in the
counter, not in the stock.
