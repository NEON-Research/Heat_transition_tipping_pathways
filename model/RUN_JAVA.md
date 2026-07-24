# Building and verifying the Java engine

The Java engine was synced with the JS engine on 2026-07-21 (DH expansion, HOA HIGHRISE fill,
block insulation propagation). It **has not been compiled** — the sandbox I work in has no
`javac`, so type errors and unresolved symbols are still possible. This is the first real test.

Run everything from `model/engine-java`.

---

## 1. Compile

```powershell
cd model\engine-java
if (Test-Path build\classes) { Remove-Item -Recurse -Force build\classes }
New-Item -ItemType Directory -Force build\classes | Out-Null
javac -d build\classes (Get-ChildItem -Recurse -Filter *.java src\main\java | % FullName)
```

Expect **zero output**. If it fails, the likely culprits are the three files I changed without
compiling:

| file | what changed | if it errors |
|---|---|---|
| `DistrictHeating.java` | new | check `Rng.nextInt(int,int)` exists and is public |
| `Neighbourhood.java` | new | should be self-contained |
| `StockLoader.java` | `Agents.neighbourhoods`, `loadNeighbourhoodCsv()`, `parseD()` | check `Perc.grid` is the field name for the DH flag, and `HousingBlock.buurt` is public |
| `Simulation.java` | new 12-arg constructor, DH call in `stepYear` | the old 11-arg constructor is kept as an overload, so `SelfTest` should still build |
| `Cli.java` | passes `ag.neighbourhoods` | — |

Paste any compiler output back and I'll fix it.

## 2. Self-test

```powershell
java -cp build\classes heattransition.SelfTest
```

Checks the formula spec (attitude, effort, PBC, subjective norm, intention, utility, salience,
learning curve, triggers, EAC) against hand-computed values. All must pass before the run
numbers mean anything.

## 3. Confirm the neighbourhood data is visible

`StockLoader.loadNeighbourhoodCsv()` looks for `../data/reference/neighborhoods.csv` then
`data/neighborhoods.csv`, and **silently returns empty if neither exists** — in which case DH
expansion has no surface area and the grid stays static, i.e. the pre-port behaviour. So verify
it is actually being read:

```powershell
Test-Path ..\engine\data\neighborhoods.csv     # must be True
```

If it is missing, regenerate:

```powershell
cd ..\data-export\scripts
python export_neighborhoods.py
copy ..\out\neighborhoods.csv ..\..\engine\data\neighborhoods.csv
```

## 4. Baseline run

```powershell
cd model\engine-java
java -Xmx4g -cp build\classes heattransition.Cli `
    --real ..\data\stock\limburg_dwellings.csv `
    --scenario baseline --iterations 20 --out java_baseline.csv
```

~20 s per iteration, so allow ~7 minutes for 20.

## 5. Check it against the JS engine

The two engines should agree to well inside AL's own run-to-run noise — they are the same
algorithm, so any real difference is a porting slip in one of them.

```powershell
cd model
python tests\compare_engines.py engine-java\java_baseline.csv <js_baseline.csv>
```

### What to expect

Baseline 2050 shares from the JS engine (n=8), for reference:

| owner | gas | hybrid | electric | DH |
|---|---|---|---|---|
| TOTAL | 10.9 | 53.5 | 34.4 | 0.76 |
| PRIVATELY_OWNED | 2.2 | 42.4 | 54.3 | 1.02 |
| PRIVATELY_RENTED | 30.7 | 64.6 | 1.2 | 0.63 |
| SOCIAL_HOUSING | 18.9 | 80.4 | 0.6 | — |
| HOME_OWNER_ASSOCIATION | 38.3 | 59.9 | 1.4 | — |

Java should land within roughly ±1 sd of these. Social housing and HOA are genuinely noisy
(sd ≈ 2 and ≈ 6), so don't read much into a few points there; TOTAL, PRIVATELY_OWNED and
PRIVATELY_RENTED are tight (sd ≈ 0.2–0.4) and any deviation beyond ~1 pp is a real difference
worth chasing.

**If DH comes out near 0.4% instead of 0.76%**, the neighbourhood CSV was not found — go back
to step 3.
