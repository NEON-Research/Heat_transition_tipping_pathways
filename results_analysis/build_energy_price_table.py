#!/usr/bin/env python3
"""Build the model's year x arm energy price and fixed-cost table.

Everything is in CONSTANT 2024 EUROS.

Structure
---------
A retail variable tariff is decomposed into two parts that behave differently:

    retail(fuel, year, arm) = commodity(fuel, year, arm) + tax(fuel, year)

`commodity` is the market-driven part (CBS "variabel leveringstarief, contractprijs") and is the
ONLY part that differs between arms. `tax` is the energiebelasting, which is policy: it is taken
from observation up to the last observed year and held constant in real terms thereafter.

History (2021 .. last observed year) is identical in all three arms -- it happened.
From the last observed year the arms ramp linearly to their 2030 endpoint and are flat to 2050,
matching the horizon of KEV 2025 (its projection ends in 2030).

Arm endpoints -- DECORRELATED
-----------------------------
The arms move the two fuels in OPPOSITE directions, because what drives fuel switching is the
spark spread (electricity price / gas price), not the price level. KEV publishes an independent
band for each commodity, so taking the corners is legitimate:

    HIGH   = gas at its upper bound, electricity at its lower  -> spread ~1.5, favours heat pumps
    MEDIUM = both central                                      -> spread ~2.0
    LOW    = gas at its lower bound, electricity at its upper  -> spread ~2.7, favours gas

An earlier version moved both fuels the same way. That arm was nearly inert -- 0.2 pp on the 2050
gas share across the whole range -- and pointed the wrong way: because electricity varies
proportionally more than gas, "both high" gave the WORST spread for heat pumps. Coupled arms
answer "what if energy gets expensive"; this model's question is "what if gas gets expensive
relative to electricity", which is the corner pair.

Arm endpoints
-------------
KEV 2025 Table 1.5 gives WHOLESALE levels for 2024 and 2030 (central + bandwidth). The KEV
2024->2030 trajectory is interpolated to the last observed year, and the remaining KEV change
from there to 2030 is applied ADDITIVELY to the observed commodity price. Anchoring on the last
observation rather than on 2024 matters: two further years of data exist, and for electricity
they point the other way from the 2024 anchor.

All six endpoints (central, low, high, for each fuel) are taken from KEV 2025 alone. An earlier
version widened the arms to the union of the KEV bound and the min/max of the observed commodity
over the post-crisis window; that let a single month of observation set an arm, and made the two
fuels' bands rest on different evidence. The observed post-crisis range is still written to the
provenance file as a CHECK -- it should sit inside the KEV band, and it does.

Fixed annual costs
------------------
Electricity: every dwelling keeps its electricity connection whatever it heats with, so the
electricity fixed block is carried for completeness but CANCELS in any EAC comparison.
Gas: a dwelling that switches to an electric heat pump or district heating drops the gas
connection and stops paying it. This is the term that matters.
Heat: ACM maximum tariffs (vastrecht heating+hot water, plus meettarief). The afleverset charge
is carried in a separate column because the model's district-heating capex may already cover it.

    python results_analysis/build_energy_price_table.py
"""
import csv, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC  = ROOT / "data" / "sources" / "cbs_85592NED_consumer_energy_tariffs.csv"
OUT  = ROOT / "model" / "data" / "reference" / "energy_prices_by_year.csv"
PROV = ROOT / "model" / "data" / "reference" / "energy_price_arms_provenance.csv"

KWH_PER_M3  = 9.769
KWH_PER_GJ  = 1000.0 / 3.6
BASE_YEAR   = 2024
KNEE        = 2030          # end of the KEV projection horizon
END         = 2050
CPI = {2021: 2.7, 2022: 10.0, 2023: 3.8, 2024: 3.3, 2025: 3.3, 2026: 3.3}   # CBS, % per year

# The 2022-2024 variable tariffs are replaced by this year's level; 0 uses the raw observed series.
#
# CBS 85592NED reports the CONTRACT price for NEW contracts. During the crisis that was far above
# what the average household actually paid: the 2023 prijsplafond capped gas at 1.45 EUR/m3 and
# electricity at 0.40 EUR/kWh for consumption up to 1200 m3 / 2900 kWh, most households were on
# fixed contracts signed before the spike, and the compensation scheme absorbed more on top. Left
# raw, the series peaks at 0.174 EUR/kWh for gas and 0.449 for electricity in 2023 -- and the
# calibration window is 2022-2024, so the model would be fitting household decisions to a price
# almost nobody was charged. Holding those three years at the 2025 level is the conservative
# reading: post-crisis, pre-projection, and actually paid.
CRISIS_BASELINE = 2025

# KEV 2025 Table 1.5, basispad, prijspeil 2024 -- WHOLESALE
KEV = {
    "gas":  dict(w2024=0.33 / KWH_PER_M3, w2030=0.23 / KWH_PER_M3,
                 band=(0.15 / KWH_PER_M3, 0.46 / KWH_PER_M3)),
    "elec": dict(w2024=77.0 / 1000.0,     w2030=73.0 / 1000.0,
                 band=(48.0 / 1000.0,     105.0 / 1000.0)),
}

# ACM maximum heat tariffs, incl. btw. consument.acm.nl/stadsverwarming-en-blokverwarming/warmtetarieven
# variable EUR/GJ; vastrecht = heating + hot water EUR/yr; meettarief EUR/yr; afleverset EUR/yr.
# 2023 uses the price-cap rate (47.38) that applied to the first 37 GJ, not the headline 90.91.
# 2022 variable is the mean of the Jan-Jun (53.95) and Jul-Dec (48.60) rates.
ACM = {
    2022: dict(var_gj=51.275, vastrecht=470.06, meet=26.11, afleverset=None),
    2023: dict(var_gj=47.380, vastrecht=549.58, meet=30.75, afleverset=None),
    2024: dict(var_gj=46.690, vastrecht=582.82, meet=31.68, afleverset=None),
    2025: dict(var_gj=43.790, vastrecht=577.48, meet=32.80, afleverset=None),
    2026: dict(var_gj=40.970, vastrecht=615.66, meet=33.73, afleverset=178.52),
}
# CBS fixed components, EUR/yr incl. btw (85592NED, Btw=A048944)
FIXED = {
    2021: dict(gas_transport=187.85, gas_vast=69.00, el_transport=257.35, el_vast=69.02, el_korting=-558.56),
    2022: dict(gas_transport=181.84, gas_vast=68.86, el_transport=253.88, el_vast=68.89, el_korting=-783.88),
    2023: dict(gas_transport=217.69, gas_vast=74.24, el_transport=342.16, el_vast=75.52, el_korting=-596.86),
    2024: dict(gas_transport=232.39, gas_vast=78.73, el_transport=415.67, el_vast=95.90, el_korting=-631.35),
    2025: dict(gas_transport=247.55, gas_vast=89.82, el_transport=467.17, el_vast=107.99, el_korting=-635.19),
    2026: dict(gas_transport=268.37, gas_vast=92.95, el_transport=476.87, el_vast=114.63, el_korting=-628.96),
}


def deflator(year, month=6.5):
    """Multiply nominal euros of (year, month) by this to get BASE_YEAR euros.

    Beyond the last year with a CPI figure there is nothing left to deflate: projected values are
    constructed in constant BASE_YEAR euros already, so the factor is 1."""
    if year > max(CPI):
        return 1.0
    f = 1.0
    if year > BASE_YEAR:
        for y in range(BASE_YEAR + 1, year):
            f *= 1 + CPI[y] / 100
        f *= (1 + CPI[year] / 100) ** ((month - 6.5) / 12 + 0.5)
    elif year < BASE_YEAR:
        for y in range(year + 1, BASE_YEAR + 1):
            f /= 1 + CPI[y] / 100
        f /= (1 + CPI[year] / 100) ** ((month - 6.5) / 12)
    return 1.0 / f


def read_cbs():
    """-> (annual {year: dict}, monthly [dict]) of the VARIABLE components, constant 2024 EUR/kWh."""
    rows = [l for l in open(SRC, encoding="utf-8") if not l.startswith("#")]
    annual, monthly = {}, []
    for r in csv.DictReader(rows):
        p = r["period"]; y = int(p[:4])
        gc = float(r["gas_levering_eur_m3"]) / KWH_PER_M3
        gt = float(r["gas_belasting_eur_m3"]) / KWH_PER_M3
        ec = float(r["elec_levering_eur_kwh"])
        et = float(r["elec_belasting_eur_kwh"])
        if p[4:6] == "JJ":
            d = deflator(y)
            annual[y] = dict(gas_c=gc * d, gas_t=gt * d, elec_c=ec * d, elec_t=et * d)
        else:
            m = int(p[6:]); d = deflator(y, m)
            monthly.append(dict(year=y, month=m, gas_c=gc * d, gas_t=gt * d,
                                elec_c=ec * d, elec_t=et * d))
    # the latest (incomplete) year has no JJ00 row: use the mean of its observed months
    last = max(r["year"] for r in monthly)
    if last not in annual:
        ms = [r for r in monthly if r["year"] == last]
        annual[last] = {k: sum(r[k] for r in ms) / len(ms) for k in ("gas_c", "gas_t", "elec_c", "elec_t")}
    return annual, monthly, last


def arm_endpoints(annual, monthly, last):
    """2030 commodity endpoint per fuel per arm, constant 2024 EUR/kWh, plus provenance.

    All six endpoints come from KEV 2025 Table 1.5 -- there is no blending with observed prices.
    An earlier version took the union of the KEV band and the observed post-crisis min/max, which
    left one endpoint (the electricity upper bound) set by January 2024, a month whose reported
    contract price is exactly what CRISIS_BASELINE excludes elsewhere. Keeping one rule for all six
    is both simpler to state and internally consistent.

    The observed range is still computed and written to the provenance file, but only as a CHECK:
    it says whether the projected band is consistent with what has actually been charged since the
    crisis, without being allowed to move the band."""
    out, prov = {}, []
    check_from = (CRISIS_BASELINE or 2024, 1)
    for fuel, key in (("gas", "gas_c"), ("elec", "elec_c")):
        k = KEV[fuel]
        obs_last = annual[last][key]
        # KEV's wholesale trajectory interpolated to the last OBSERVED year, so the two further
        # years of data since KEV's 2024 base are respected; only the residual change is applied.
        w_last = k["w2024"] + (k["w2030"] - k["w2024"]) * (last - BASE_YEAR) / (KNEE - BASE_YEAR)
        ends = {"low":     obs_last + k["band"][0] - w_last,
                "central": obs_last + k["w2030"]   - w_last,
                "high":    obs_last + k["band"][1] - w_last}
        out[fuel] = ends
        window = [r[key] for r in monthly if (r["year"], r["month"]) >= check_from]
        obs_lo, obs_hi = (min(window), max(window)) if window else ("", "")
        for arm in ("low", "central", "high"):
            v = ends[arm]
            inside = "" if obs_lo == "" else ("yes" if obs_lo <= v <= obs_hi else "no")
            prov.append(dict(fuel=fuel, arm=arm, endpoint_2030=round(v, 6),
                             source="KEV 2025 Table 1.5 "
                                    + ("basispad" if arm == "central" else "bandwidth"),
                             observed_min=("" if obs_lo == "" else round(obs_lo, 6)),
                             observed_max=("" if obs_hi == "" else round(obs_hi, 6)),
                             endpoint_within_observed_range=inside))
    return out, prov


def ramp(y, y0, v0, y1, v1):
    if y <= y0: return v0
    if y >= y1: return v1
    return v0 + (v1 - v0) * (y - y0) / (y1 - y0)


def main():
    annual, monthly, last = read_cbs()
    ends, prov = arm_endpoints(annual, monthly, last)
    first = min(annual)

    tax_gas, tax_elec = annual[last]["gas_t"], annual[last]["elec_t"]
    f_last = FIXED[last]
    gas_fixed_last  = f_last["gas_transport"] + f_last["gas_vast"]
    elec_fixed_last = f_last["el_transport"] + f_last["el_vast"] + f_last["el_korting"]
    acm_last = ACM[last]

    base_y = CRISIS_BASELINE
    if base_y and base_y not in annual:
        raise SystemExit(f"--crisis-baseline {base_y} is not an observed year "
                         f"({min(annual)}-{max(annual)})")

    rows = []
    for arm in ("low", "central", "high"):
        for y in range(first, END + 1):
            observed = y in annual
            d = deflator(y)
            if observed:
                # Crisis years take the baseline year's VARIABLE tariff (see --crisis-baseline).
                # Fixed charges stay observed: transport and vastrecht rose steadily and were not
                # distorted by the cap, and the electricity block cancels between technologies.
                src_y = base_y if (base_y and 2022 <= y < base_y) else y
                av = annual[src_y]
                gas_c, gas_t = av["gas_c"], av["gas_t"]
                elec_c, elec_t = av["elec_c"], av["elec_t"]
                # keep what CBS actually reported for this year, so the substitution is visible
                raw_gas  = annual[y]["gas_c"] + annual[y]["gas_t"]
                raw_elec = annual[y]["elec_c"] + annual[y]["elec_t"]
                f = FIXED[y]
                gas_fixed  = (f["gas_transport"] + f["gas_vast"]) * d
                elec_fixed = (f["el_transport"] + f["el_vast"] + f["el_korting"]) * d
                acm = ACM.get(y)
                if acm:
                    # Heat follows the same crisis-year rule as gas and electricity: the variable
                    # tariff of 2022..CRISIS_BASELINE-1 is held at the baseline year's level, and the
                    # observed ACM tariff is kept in heat_variable_raw_eur_kwh.
                    acm_v = ACM.get(src_y, acm)
                    heat_var   = acm_v["var_gj"] / KWH_PER_GJ * deflator(src_y if src_y in ACM else y)
                    raw_heat   = acm["var_gj"] / KWH_PER_GJ * d
                    heat_fixed = (acm["vastrecht"] + acm["meet"]) * d
                    heat_set   = (acm["afleverset"] or 0.0) * d
                    basis = ("observed" if src_y == y else
                             "variable tariff held at the %d level (crisis years)" % src_y)
                else:   # before the first ACM year: hold the earliest known value in real terms
                    a0 = ACM[min(ACM)]
                    heat_var   = a0["var_gj"] / KWH_PER_GJ * deflator(min(ACM))
                    heat_fixed = (a0["vastrecht"] + a0["meet"]) * deflator(min(ACM))
                    heat_set   = 0.0
                    raw_heat   = ""          # no ACM observation this early
                    basis = "observed; heat backcast from %d" % min(ACM)
            else:
                # decorrelated: the electricity arm is the OPPOSITE corner of the gas arm, so the
                # spark spread -- not the price level -- is what the arm varies
                elec_arm = {"low": "high", "high": "low"}.get(arm, "central")
                gas_c  = ramp(y, last, annual[last]["gas_c"],  KNEE, ends["gas"][arm])
                elec_c = ramp(y, last, annual[last]["elec_c"], KNEE, ends["elec"][elec_arm])
                gas_t, elec_t = tax_gas, tax_elec
                gas_fixed  = gas_fixed_last  * deflator(last)
                elec_fixed = elec_fixed_last * deflator(last)
                # heat: NMDA linkage -- the ACM maximum tracks the gas retail price
                g_ref = annual[last]["gas_c"] + tax_gas
                heat_ref = acm_last["var_gj"] / KWH_PER_GJ * deflator(last)
                heat_var   = heat_ref * (gas_c + gas_t) / g_ref
                heat_fixed = (acm_last["vastrecht"] + acm_last["meet"]) * deflator(last)
                heat_set   = (acm_last["afleverset"] or 0.0) * deflator(last)
                basis = "projected"
                raw_gas = raw_elec = raw_heat = ""   # nothing observed to compare a projection against
            rows.append(dict(
                year=y, arm=arm,
                gas_commodity_eur_kwh=round(gas_c, 6),
                gas_tax_eur_kwh=round(gas_t, 6),
                gas_retail_eur_kwh=round(gas_c + gas_t, 6),
                elec_commodity_eur_kwh=round(elec_c, 6),
                elec_tax_eur_kwh=round(elec_t, 6),
                elec_retail_eur_kwh=round(elec_c + elec_t, 6),
                heat_variable_eur_kwh=round(heat_var, 6),
                # what CBS actually reported, before the crisis-year substitution; blank when projected
                gas_retail_raw_eur_kwh=(round(raw_gas, 6) if raw_gas != "" else ""),
                elec_retail_raw_eur_kwh=(round(raw_elec, 6) if raw_elec != "" else ""),
                heat_variable_raw_eur_kwh=(round(raw_heat, 6) if raw_heat != "" else ""),
                gas_fixed_eur_year=round(gas_fixed, 2),
                elec_fixed_eur_year=round(elec_fixed, 2),
                heat_fixed_eur_year=round(heat_fixed, 2),
                heat_afleverset_eur_year=round(heat_set, 2),
                basis=basis))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    hdr = list(rows[0])
    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        fh.write("# Energy prices and fixed connection costs, CONSTANT 2024 EUR. "
                 "Built by results_analysis/build_energy_price_table.py\n")
        fh.write("# retail = commodity + tax. Only `commodity` differs between arms; tax and fixed "
                 "costs are policy, held at their last observed level.\n")
        fh.write("# elec_fixed_eur_year is carried for completeness and CANCELS between technologies "
                 "-- every dwelling keeps its electricity connection.\n")
        fh.write("# gas_fixed_eur_year is paid only by a dwelling that still has a gas connection "
                 "(gas boiler, gas block, hybrid heat pump).\n")
        fh.write("# heat_afleverset_eur_year is listed separately: the model's district-heating capex "
                 "may already cover the delivery set.\n")
        fh.write("# *_raw_eur_kwh is what CBS (gas, electricity) and ACM (heat) reported before the "
                 "crisis-year substitution, which holds the 2022 onwards variable tariffs at the "
                 f"CRISIS_BASELINE = {CRISIS_BASELINE} level; blank for projected years.\n")
        w = csv.DictWriter(fh, fieldnames=hdr); w.writeheader(); w.writerows(rows)

    with open(PROV, "w", newline="", encoding="utf-8") as fh:
        fh.write("# 2030 commodity endpoints per arm. All six come from KEV 2025 Table 1.5; the\n"
                 f"# observed columns are a CHECK against monthly CBS data from {CRISIS_BASELINE or 2024} "
                 "onwards and do not move the band. Constant 2024 EUR/kWh.\n")
        w = csv.DictWriter(fh, fieldnames=list(prov[0])); w.writeheader(); w.writerows(prov)

    print(f"observed {first}-{last}, projected {last+1}-{END}, knee {KNEE}\n")
    print("2030 commodity endpoints, constant 2024 EUR/kWh -- all six from KEV 2025 Table 1.5;\n"
          "the observed range is a consistency check only and does not move the band:")
    for p_ in prov:
        ok = p_["endpoint_within_observed_range"]
        print(f"  {p_['fuel']:5s} {p_['arm']:8s} {p_['endpoint_2030']:.4f}   "
              f"observed {p_['observed_min']:.4f}-{p_['observed_max']:.4f}   "
              f"{'inside the observed range' if ok == 'yes' else 'outside (a wider projection)'}")
    print("\nretail price at 2050 (constant 2024 EUR/kWh); the arms are DECORRELATED, so the\n"
          "spark spread -- not the level -- is what each arm moves:")
    for arm in ("low", "central", "high"):
        r = [x for x in rows if x["arm"] == arm and x["year"] == END][0]
        print(f"  {arm:8s} gas {r['gas_retail_eur_kwh']:.4f}   elec {r['elec_retail_eur_kwh']:.4f}"
              f"   heat {r['heat_variable_eur_kwh']:.4f}"
              f"   spark {r['elec_retail_eur_kwh']/r['gas_retail_eur_kwh']:.2f}")
    r = [x for x in rows if x["arm"] == "central" and x["year"] == END][0]
    print(f"\nfixed, constant 2024 EUR/yr (from {last} on): gas {r['gas_fixed_eur_year']:.2f}   "
          f"heat {r['heat_fixed_eur_year']:.2f} (+afleverset {r['heat_afleverset_eur_year']:.2f})   "
          f"elec {r['elec_fixed_eur_year']:.2f} (cancels)")
    print(f"\n-> {OUT.relative_to(ROOT)}\n-> {PROV.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
