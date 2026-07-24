"""
export_neighborhoods.py
-----------------------
Build the neighbourhood dataset the engine needs for DH expansion, grid congestion
and the POLICY_BASED scenarios.

Joins three files that AnyLogic reads separately:
  _neighborhoods_data_2023.csv   -> the 26 active `neighborhood_data_selection` columns
  _nbh_policy_plan_2023.csv      -> p_policyPlan / p_policyPlanYear (TVW strategy)
  _EVsPerPC.csv                  -> PC4 -> BEV share, for the EV adoption S-curve

Faithfulness notes (all traced to the generated Java, see SOURCE_AUDIT.md):

  * -99999 sentinels are cleaned to 0, matching Main.f_returnZeroIfDoubleIsSmallerThanZero.
    AnyLogic applies this to a SUBSET of fields; we apply it to all numerics because every
    field we export is one AL cleans (a_hh, a_pau, a_lan_ha, pst_mvp) or one AL cleans as of
    the 2026-07-21 fix (g_ele, g_gas). Leaving g_ele dirty produced sqrt(negative) -> NaN,
    which silently disabled congestion for the entire model.

  * bev_share uses the 0.084 Dutch-2023 fallback when the PC4 is absent. NOTE AnyLogic's
    lookup currently ALWAYS falls back (Integer-keyed map queried with a double), so to
    reproduce AL exactly the engine should ignore bev_share and use 0.084 everywhere until
    that cast is fixed. Both values are exported so the engine can switch cheaply.

  * policy_plan_year is drawn per-run in AL (uniform between start_jaar and eind_jaar), so
    we export the WINDOW, not a drawn year. The engine must draw it, or the two models
    cannot agree on run-to-run variance.

Usage:
    python export_neighborhoods.py [--province Limburg] [-o ../out/neighborhoods.csv]
"""
import argparse
import csv
import os

# Main.f_setMunicipalityCodes
PROVINCE_MUNICIPALITIES = {
    "Limburg": ["GM0882", "GM0888", "GM0889", "GM0893", "GM0899",
                "GM0907", "GM0917", "GM0928", "GM0935", "GM0938",
                "GM0944", "GM0946", "GM0957", "GM0965", "GM0971",
                "GM0981", "GM0983", "GM0984", "GM0986", "GM0988",
                "GM0994", "GM1507", "GM1640", "GM1641", "GM1669",
                "GM1711", "GM1729", "GM1883", "GM1894", "GM1903",
                "GM1954"],
    "Groningen": ["GM0014", "GM0037", "GM0047", "GM0765", "GM1895",
                  "GM1950", "GM1952", "GM1966", "GM1969", "GM1979"],
}

# active columns from neighborhood_data_selection (commented-out assignments excluded)
NBH_COLUMNS = [
    "a_hh",                                            # p_households
    "g_ele", "g_ele_ap", "g_ele_tw", "g_ele_hw", "g_ele_2w", "g_ele_vw",
    "g_gas", "g_gas_ap", "g_tw", "g_gas_hw", "g_gas_2w", "g_gas_vw",
    "p_stadsv",                                        # p_percHouseholdsWithDistrictHeating
    "a_bedv", "a_bed_a", "a_bed_bf", "a_bed_gi", "a_bed_hj",
    "a_bed_kl", "a_bed_mn", "a_bed_oq", "a_bed_ru",    # company locations
    "a_pau",                                           # p_cars
    "a_lan_ha",                                        # p_surfaceAreaLand
    "pst_mvp",                                         # p_postalCode
]

BEV_FALLBACK = 0.084     # Dutch 2023 average, per Main


def clean(v):
    """f_returnZeroIfDoubleIsSmallerThanZero: negatives (incl. -99999 sentinels) -> 0."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return f if f > 0 else 0.0


def municipality_of(buurtcode):
    """BU0014 0000 -> GM0014 (Main derives the district code the same way)."""
    return "GM" + buurtcode[2:6] if len(buurtcode) >= 6 else ""


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    # raw source files (_neighborhoods_data_2023.csv, _nbh_policy_plan_2023.csv) now live in top-level data/
    src = os.path.abspath(os.path.join(here, "..", "..", "..", "data"))

    ap = argparse.ArgumentParser()
    ap.add_argument("--province", default="Limburg")
    ap.add_argument("--src", default=src)
    ap.add_argument("-o", "--out", default=os.path.join(here, "..", "..", "data", "reference", "neighborhoods.csv"))
    args = ap.parse_args()

    codes = set(PROVINCE_MUNICIPALITIES.get(args.province, []))
    if not codes:
        raise SystemExit(f"unknown province {args.province!r}")

    # ---- 1. EV shares by PC4 ------------------------------------------------
    bev = {}
    with open(os.path.join(args.src, "_EVsPerPC.csv"), encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            try:
                bev[int(row["PC4"])] = float(row["perc_HPEV_BEV"])
            except (TypeError, ValueError):
                continue

    # ---- 2. policy plans by buurtcode --------------------------------------
    # installatie -> heating system, per f_setNeighborhoodPolicyPlan's substring tests
    plans = {}
    with open(os.path.join(args.src, "_nbh_policy_plan_2023.csv"), encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            inst = (row.get("installatie") or "")
            if "Wnet" in inst:
                plan = "DISTRICT_HEATING"
            elif "eHP" in inst:
                plan = "ELECTRIC_HEAT_PUMP"
            elif "hHP" in inst:
                plan = "HYBRID_HEAT_PUMP"
            else:
                plan = "NONE"

            def as_int(v):
                try:
                    return int(float(v))
                except (TypeError, ValueError):
                    return 0

            plans[row["buurtcode"]] = {
                "policy_plan": plan,
                # AL: startYear = (start_jaar == 0) ? currentYear : start_jaar
                #     endYear   = (eind_jaar  == 0) ? 2050        : eind_jaar
                # the YEAR is drawn uniformly in that window per run -> export the window
                "policy_start_jaar": as_int(row.get("start_jaar")),
                "policy_eind_jaar": as_int(row.get("eind_jaar")),
                # infra containing "W" feeds v_nbhsWithoutDHStartYear when start_jaar == 0
                "policy_infra_w": 1 if "W" in (row.get("infra") or "") else 0,
            }

    # ---- 3. neighbourhoods --------------------------------------------------
    out_cols = (["buurtcode", "municipality"] + NBH_COLUMNS
                + ["bev_share", "bev_from_data",
                   "policy_plan", "policy_start_jaar", "policy_eind_jaar", "policy_infra_w"])

    rows, matched_bev, matched_plan = [], 0, 0
    with open(os.path.join(args.src, "_neighborhoods_data_2023.csv"),
              encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = row.get("gwb_code_10", "")
            gm = municipality_of(code)
            if gm not in codes:
                continue

            rec = {"buurtcode": code, "municipality": gm}
            for c in NBH_COLUMNS:
                rec[c] = clean(row.get(c))

            pc4 = int(rec["pst_mvp"])
            if pc4 in bev:
                rec["bev_share"], rec["bev_from_data"] = bev[pc4], 1
                matched_bev += 1
            else:
                rec["bev_share"], rec["bev_from_data"] = BEV_FALLBACK, 0

            p = plans.get(code)
            if p:
                rec.update(p)
                matched_plan += 1
            else:
                rec.update({"policy_plan": "NONE", "policy_start_jaar": 0,
                            "policy_eind_jaar": 0, "policy_infra_w": 0})
            rows.append(rec)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_cols)
        w.writeheader()
        w.writerows(rows)

    n = len(rows)
    print(f"wrote {n} neighbourhoods ({args.province}) -> {args.out}")
    print(f"  policy plan matched : {matched_plan}/{n} ({100*matched_plan/n:.0f}%)")
    print(f"  BEV share matched   : {matched_bev}/{n} ({100*matched_bev/n:.0f}%)"
          f"  [rest use the {BEV_FALLBACK} fallback]")
    zero_land = sum(1 for r in rows if r["a_lan_ha"] == 0)
    zero_hh = sum(1 for r in rows if r["a_hh"] == 0)
    zero_ele = sum(1 for r in rows if r["g_ele"] == 0)
    print(f"  a_lan_ha == 0       : {zero_land}  (DH density would divide by zero — guard in engine)")
    print(f"  a_hh == 0           : {zero_hh}")
    print(f"  g_ele == 0          : {zero_ele}  (was -99999 before cleaning)")


if __name__ == "__main__":
    main()
