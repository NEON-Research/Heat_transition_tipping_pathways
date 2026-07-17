# Frontend — live in-browser model

A single-page dashboard that runs the **multi-agent engine** (social-housing blocks +
landlords + homeowners) live in your browser and charts the heat-transition over 2024–2050.
It imports the exact same engine modules the Java model mirrors — no build step, no server-side
code. This is the "open it and run a model from there" tool.

## Run it
ES modules + `fetch` need to be served over http:// (not opened as a `file://`). From the
`js_migration` folder:

```
py -m http.server 8000
```
then open **http://localhost:8000/frontend/** in your browser.

## What it does
- Rebuilds the dwelling population from `limburg_sample.json` (a 1-in-100 sample of the real
  Limburg stock: heat demand, energy label, ownership inputs, insulation cost).
- Assigns ownership + samples the demand factor with a seeded RNG (reproducible per seed).
- Runs `FullSimulation` (../engine/src/modelFull.js) and draws:
  - heating-system mix over time (all owners),
  - final-year mix by owner type.
- Controls: social/economic learning factor, opportunity-trigger mode (corrected vs legacy),
  and seed. ~6,200 dwellings run in well under a second.

## Regenerating the sample
```
# from repo root, after exporting the full stock:
py - <<'PY'
import csv, json
rows=[]
for i,r in enumerate(csv.DictReader(open("js_migration/data-export/out/limburg_dwellings.csv"))):
    if i%100: continue
    rows.append([r['buurtcode'],'APARTMENT' if r['dwelling_type'] in ('APARTMENT','HIGHRISE') else 'HOUSE',
        r['label'] or 'n', round(float(r['area_m2'])), round(float(r['space_heat_kwh'])),
        round(float(r['dhw_base_kwh'])), round(float(r['insul_to_b'])), round(float(r['insul_to_c'])),
        int(r['perc_koop'] or 0), int(r['perc_huur'] or 0), int(r['aantal_corp'] or 0)])
json.dump({"fields":["buurt","dtype","label","area","space","dhw","insulB","insulC","koop","huur","corp"],
           "rows":rows}, open("js_migration/frontend/limburg_sample.json","w"))
PY
```

## Status
Homeowner adoption is still being calibrated (heat-pump share sits below the full AnyLogic
model — see APPROACH.md). The social-housing/landlord transition and the overall structure
are in place. Next: calibrate the homeowner social-contagion snowball, then port the
multi-agent model (`modelFull.js`) to the Java engine (currently Java has the homeowner core).
