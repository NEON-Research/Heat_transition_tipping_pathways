# AnyLogic block decision trace — code to paste

The block analogue of the sample-dwelling export. Where `block_dump` showed the *state* of
every block at year 0, this shows the *decision*: at each year a block reaches a trigger, what
did each of the five options cost, which ones were even possible, and which won.

It deliberately mirrors the pattern already in the model (`validationHousehold` +
`J_AdoptionData` + `adoptionDataList`) rather than inventing a new mechanism.

Four edits, then run. All imports are written fully-qualified so nothing needs adding to the
import block.

---

## 1. New Java class: `J_BlockDecisionData`

*Projects panel → right-click the model → New → Java Class → name it `J_BlockDecisionData`.*

```java
import java.util.EnumMap;
import java.util.Map;

/** One block's decision in one year, with the full option set that produced it.
 *  Mirrors J_AdoptionData, but for J_HousingBlock / J_SocialHousingBlock. */
public class J_BlockDecisionData {
    public final int year;
    public final String blockType;      // "SOCIAL" | "HOA"
    public final String buurtcode;
    public final String blockKey;       // smallest member numid -> stable across engines
    public final int nHouseholds;
    public final OL_HeatingSystem currentHS;
    public final int hsAge;
    public final int lifetime;
    public final String trigger;        // "end_of_life" | "opportunity"
    public final OL_HeatingSystem chosen;

    public final Map<OL_HeatingSystem, Double>  avgEAC      = new EnumMap<>(OL_HeatingSystem.class);
    public final Map<OL_HeatingSystem, Boolean> isPossible  = new EnumMap<>(OL_HeatingSystem.class);
    /** how many households in the block individually blocked this option (AL ANDs isPossible) */
    public final Map<OL_HeatingSystem, Integer> nImpossible = new EnumMap<>(OL_HeatingSystem.class);

    public J_BlockDecisionData(int year, String blockType, String buurtcode, String blockKey,
                               int nHouseholds, OL_HeatingSystem currentHS, int hsAge, int lifetime,
                               String trigger, OL_HeatingSystem chosen) {
        this.year = year; this.blockType = blockType; this.buurtcode = buurtcode;
        this.blockKey = blockKey; this.nHouseholds = nHouseholds; this.currentHS = currentHS;
        this.hsAge = hsAge; this.lifetime = lifetime; this.trigger = trigger; this.chosen = chosen;
    }
}
```

---

## 2. Fields + block key on `J_HousingBlock`

Add to the `J_HousingBlock` class body (the trace list is **static** so both block subclasses collect into one
place and Main can write it out without extra plumbing):

```java
public static java.util.List<J_BlockDecisionData> blockDecisionTrace = new java.util.ArrayList<>();

/** how many households blocked each option in the most recent EAC averaging pass */
private transient Map<OL_HeatingSystem, Integer> lastNImpossible = new EnumMap<>(OL_HeatingSystem.class);

/** Stable cross-engine identity: smallest BAG numid among members. Independent of
 *  iteration order, so AL and the JS/Java engines agree whenever membership agrees. */
public String getBlockKey() {
    String k = null;
    for (J_Household hh : households) {
        String n = String.valueOf(hh.getDwelling().getNumid());
        if (n != null && !n.isEmpty() && (k == null || n.compareTo(k) < 0)) k = n;
    }
    return k == null ? "NA" : k;
}
```

> If your `J_Dwelling` getter is named differently, swap `getNumid()` for whatever you used when
> you added numid for the dwelling dump.

---

## 3. Count the blockers in `setAverageHeatingSystemOptionsEAC`

This is the diagnostic that matters most. Your existing loop already does:

```java
if( !hsol.isPossible() ) { isPossible = false; }
```

`isPossible` is ANDed across **every** household, so one ineligible dwelling removes the option
for the entire block. Add a counter so we can see how often that happens and how many households
are responsible. Inside the per-type loop, alongside the existing `boolean isPossible = true;`:

```java
int nImp = 0;                                    // <-- add
for(J_Household hh : households) {
    J_HeatingSystemOption hsol = hh.getHeatingSystemOptions().get(type);
    EAC += hsol.getEAC();
    if( !hsol.isPossible() ) {
        isPossible = false;
        nImp++;                                  // <-- add
    }
}
lastNImpossible.put(type, nImp);                 // <-- add
double averageEAC = EAC / households.size();
heatingSystemOptions.put(type, new J_HeatingSystemOption(averageEAC, isPossible));
```

---

## 4. Record the decision

In **`f_adoptHeatingMethodSHA`** and **`f_adoptHeatingMethodHOA`** (both on `J_HousingBlock`), capture the option set *before*
`installNewHeatingMethodInHouseholds` — that call ends with `setHeatingSystemOptionsNull()`, so
afterwards the evidence is gone:

```java
public void f_adoptHeatingMethodHOA() {
    if( hasTrigger()) {
        setAverageHeatingSystemOptionsEAC();               // ensure options exist
        OL_HeatingSystem heatingMethodToInstall = getStochasticChoice();
        f_recordBlockDecision("HOA", heatingMethodToInstall);   // <-- add, BEFORE install
        installNewHeatingMethodInHouseholds(heatingMethodToInstall, "end_of_life");
    }
}
```

Do the same in `f_adoptHeatingMethodSHA` with `"SOCIAL"` — note it has a POLICY_BASED
branch, so record `heatingMethodToInstall` after that if/else, not inside the cost-based arm. Then add the recorder:

```java
private void f_recordBlockDecision(String blockType, OL_HeatingSystem chosen) {
    int lifetime = main.heatingSystemOptionsGlobalData.get(heatingSystem.getType()).getLifetime();
    int age = heatingSystem.getAge();
    // hasTrigger() is EOL || opportunity; report which one actually fired
    String trigger = (age >= lifetime) ? "end_of_life" : "opportunity";

    J_BlockDecisionData rec = new J_BlockDecisionData(
        main.v_year, blockType, getNeighborhood().getBuurtcode(), getBlockKey(),
        households.size(), heatingSystem.getType(), age, lifetime, trigger, chosen);

    for (OL_HeatingSystem type : OL_HeatingSystem.values()) {
        J_HeatingSystemOption o = heatingSystemOptions.get(type);
        rec.avgEAC.put(type, o == null ? Double.NaN : o.getEAC());
        rec.isPossible.put(type, o != null && o.isPossible());
        rec.nImpossible.put(type, lastNImpossible.getOrDefault(type, 0));
    }
    blockDecisionTrace.add(rec);
}
```

> Swap `getNeighborhood().getBuurtcode()` for however the block reaches its buurtcode — if the
> block has no neighbourhood reference, `households.get(0).getDwelling().getBuurtcode()` works.

---

## 5. Export function on Main

Call this at the end of the run, next to wherever you write `block_dump`:

```java
public void f_exportBlockDecisionTrace() {
    String ts = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
    String path = "results/block_decision_trace_" + ts + ".csv";
    try (java.io.BufferedWriter w = new java.io.BufferedWriter(new java.io.FileWriter(path))) {
        w.write("year,block_type,buurtcode,block_key,n_households,current_hs,hs_age,lifetime,"
              + "trigger,option,avg_eac,is_possible,n_impossible,chosen");
        w.newLine();
        for (J_BlockDecisionData r : J_HousingBlock.blockDecisionTrace) {
            for (OL_HeatingSystem type : OL_HeatingSystem.values()) {
                w.write(r.year + "," + r.blockType + "," + r.buurtcode + "," + r.blockKey + ","
                      + r.nHouseholds + "," + r.currentHS + "," + r.hsAge + "," + r.lifetime + ","
                      + r.trigger + "," + type + ","
                      + String.format(java.util.Locale.US, "%.2f", r.avgEAC.get(type)) + ","
                      + (r.isPossible.get(type) ? 1 : 0) + ","
                      + r.nImpossible.get(type) + ","
                      + (type == r.chosen ? 1 : 0));
                w.newLine();
            }
        }
        traceln("Wrote " + J_HousingBlock.blockDecisionTrace.size() + " block decisions -> " + path);
    } catch (java.io.IOException e) {
        traceln("block decision trace failed: " + e.getMessage());
    }
}
```

**One run only.** The static list accumulates across iterations, so either run a single
iteration or clear it at the start of each: `J_HousingBlock.blockDecisionTrace.clear();`

---

## Then compare

```bash
node js_migration/engine/dumpBlockDecisions.js --out /tmp/mytrace.csv
python js_migration/tests/compare_block_decisions.py \
    "Heat transition tipping pathways/results/block_decision_trace_<ts>.csv" /tmp/mytrace.csv
```

### What the answer will look like

The comparator splits the gap three ways, and each points somewhere different:

- **`n_impossible` differs** → the two engines disagree about *eligibility*, not economics.
  Most likely `f_getHeatingMethodPossibility` — DH grid coverage or the APARTMENT test for
  NATURAL_GAS_BLOCK. This is my leading suspicion, because a single blocking dwelling vetoes the
  whole block, which is exactly the kind of mechanism that produces a stuck *subset* of blocks
  rather than a uniform drift.
- **`avg_eac` differs on the heat-pump rows** → economics. Probably the insulation retrofit
  being charged to blocks that AL considers already insulated.
- **both match but `chosen` differs** → nothing is wrong. That's the Gumbel draw, and the gap is
  RNG rather than structure.

The `trigger` column is worth a glance too: if AL's decisions are overwhelmingly `opportunity`
and mine are `end_of_life`, the two engines are meeting blocks at different points in their
lifecycle.

---

## Correction (verified against the .alp)

An earlier draft of this file referenced `getAverageHeatingSystemOptionsEAC` and a bare
`f_adoptHeatingMethod`. Those belong to the **deprecated per-postal-code path**
(`f_setPolicyHeatingMethod` -> `shaPerPostalCode` / `c_SHAHouseholdsPerPostalCode`), which is
not on the live route. The live members of `J_HousingBlock` are:

| live | deprecated |
|---|---|
| `setAverageHeatingSystemOptionsEAC()` | `getAverageHeatingSystemOptionsEAC()` |
| `f_adoptHeatingMethodSHA()` / `f_adoptHeatingMethodHOA()` | `f_adoptHeatingMethod()` |

Note `setAverageHeatingSystemOptionsEAC` is guarded by `if(heatingSystemOptions == null)`, so
calling it before recording is safe and idempotent — it will not recompute if the options are
already populated for this year.

Also verified: block-level eligibility comes from the **dwelling's**
`J_Dwelling.f_getHeatingMethodPossibility` (via `hh.getHeatingSystemOptionsEAC()`), not from
`J_HousingBlock.f_getHeatingMethodPossibility`. The dwelling version is the one with the
`NATURAL_GAS_BLOCK && dwellingType != APARTMENT` rule; the block's own copy lacks it and is
effectively dead code on this path.
