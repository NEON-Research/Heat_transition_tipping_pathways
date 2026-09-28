#!/usr/bin/env python3
"""Retail energy-price paths from KEV 2025 Table 1.5: ramp 2024->2030, constant thereafter.

KEV gives a 2024 level, a 2030 central estimate and a 2030 bandwidth, in constant 2024 euros, for
WHOLESALE gas and electricity. This builds the three consumer-price paths the model needs:

  1. linear interpolation of the wholesale price from its 2024 level to each 2030 value
     (central, low, high), so all three paths start together and fan out;
  2. flat in real terms after 2030 -- KEV's projection horizon ends there and extrapolating a
     six-year trend across a further twenty years is not supported by the source;
  3. wholesale -> retail by ADDITIVE passthrough on the commodity component only: energy tax,
     network tariff and VAT are held at their 2024 levels, so only the commodity part moves.
     Multiplying the retail price by the wholesale change would be wrong -- retail gas would rise
     several-fold by 2050, because taxes and network charges are most of the bill.

Writes a tidy CSV of the paths and a figure.

    python kev_price_paths.py
"""
import argparse, csv, os
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
KWH_PER_M3 = 9.769                      # Dutch gas, higher heating value

# --- Observed retail prices (CBS 85592NED), used to override a KEV arm reality has left behind ---
# Definition matched to the model's 2024 anchors: VARIABLE consumer tariff incl. btw = variabel
# leveringstarief (contractprijs) + energiebelasting. Fixed transport/vastrecht excluded. Deflated
# to constant 2024 euros with the CBS CPI. See results_analysis/observed_price_history.py.
#   gas   2024 avg 0.1360 -> 2026M08 0.1339 EUR/kWh  (-1.5%)  -- inside the KEV band, no override
#   elec  2024 avg 0.3026 -> 2026M08 0.2445 EUR/kWh (-19.2%)  -- BELOW the KEV low arm of 0.291
# Retailer comparison sites quote ~1.67 EUR/m3 for gas, but that amortises the fixed transport and
# standing charges over annual consumption; it is not the same quantity as the model's 0.14 anchor.
OBSERVED = {
    "date": "2026-08",
    "gas":  {"eur_per_m3": 1.3794, "eur_per_kwh": 0.13390,
             "source": "CBS 85592NED, variable consumer tariff incl. btw, constant 2024 EUR"},
    "elec": {"eur_per_kwh": 0.24450,
             "source": "CBS 85592NED, variable consumer tariff incl. btw, constant 2024 EUR"},
}
# arms replaced by an observation rather than derived from KEV: fuel -> (year reached, EUR/kWh)
# Set LOW_OVERRIDE["elec"] to pull the electricity low arm down to what is already observed.
HIGH_OVERRIDE = {}
LOW_OVERRIDE  = {}

# --- KEV 2025 Table 1.5, "basispad", prijspeil 2024 -------------------------------------------
KEV = {
    "gas":  {"unit": "EUR/m3",  "y2024": 0.33,  "y2030": 0.23,  "band2030": (0.15, 0.46),
             "to_kwh": lambda v: v / KWH_PER_M3, "retail2024": 0.14,  "label": "Natural gas"},
    "elec": {"unit": "EUR/MWh", "y2024": 77.0,  "y2030": 73.0,  "band2030": (48.0, 105.0),
             "to_kwh": lambda v: v / 1000.0,     "retail2024": 0.32,  "label": "Electricity"},
}
COLOR = {"gas": "#999999", "elec": "#0072B2"}
# the growth rates the model used before this file existed, for comparison
OLD = {"gas": (-0.0100, 0.0000, 0.0262), "elec": (-0.0157, -0.00209, 0.0141)}


def paths(fuel, start=2024, end=2050, knee=2030):
    """{arm: {year: retail EUR/kWh}} — linear wholesale ramp to `knee`, flat after."""
    k = KEV[fuel]
    w24 = k["to_kwh"](k["y2024"])
    ends = {"low": k["band2030"][0], "central": k["y2030"], "high": k["band2030"][1]}
    yrs = np.arange(start, end + 1)
    out = {}
    for arm, w30 in ends.items():
        ovr = (HIGH_OVERRIDE if arm == "high" else LOW_OVERRIDE if arm == "low" else {}).get(fuel)
        if ovr is not None:
            # observed level, reached in the year it was observed and held flat thereafter
            yr, target = ovr
            r = np.where(yrs <= yr,
                         k["retail2024"] + (target - k["retail2024"]) * (yrs - start) / (yr - start),
                         target)
            out[arm] = dict(zip(yrs, r))
            continue
        w30 = k["to_kwh"](w30)
        w = np.where(yrs <= knee,
                     w24 + (w30 - w24) * (yrs - start) / (knee - start),
                     w30)
        out[arm] = dict(zip(yrs, k["retail2024"] + (w - w24)))   # additive commodity passthrough
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=int, default=2024)
    ap.add_argument("--knee", type=int, default=2030)
    ap.add_argument("--end", type=int, default=2050)
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "nl", "figures", "fig_kev_price_paths.png"))
    ap.add_argument("--csv", default=os.path.join(ROOT, "model", "data", "reference", "energy_price_paths.csv"))
    ap.add_argument("--copy-to", default=os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures"))
    a = ap.parse_args()

    P = {f: paths(f, a.start, a.end, a.knee) for f in KEV}
    os.makedirs(os.path.dirname(a.csv), exist_ok=True)
    with open(a.csv, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["fuel", "arm", "year", "retail_eur_per_kwh"])
        for f in KEV:
            for arm, d in P[f].items():
                for y, v in d.items(): w.writerow([f, arm, y, f"{v:.6f}"])

    print("KEV 2025 Table 1.5 -> retail paths (constant 2024 EUR/kWh)\n")
    for f in KEV:
        k = KEV[f]
        print(f"{k['label']}  (wholesale {k['y2024']} -> {k['y2030']} {k['unit']}, band "
              f"{k['band2030'][0]}-{k['band2030'][1]}; retail base {k['retail2024']})")
        print(f"   {'arm':9s}{'2030 & after':>14s}{'change vs 2024':>17s}{'implied 24-30 CAGR':>21s}")
        for arm in ("low", "central", "high"):
            v30 = P[f][arm][a.end]
            cagr = (v30 / k["retail2024"]) ** (1 / (a.knee - a.start)) - 1
            ovr = (HIGH_OVERRIDE if arm == "high" else LOW_OVERRIDE if arm == "low" else {})
            tag = "  <- observed, not KEV" if f in ovr else ""
            print(f"   {arm:9s}{v30:14.4f}{100*(v30/k['retail2024']-1):16.1f} %{100*cagr:20.2f} %/yr{tag}")
        print()

    fig, axes = plt.subplots(1, 2, figsize=(12.4, 4.6), sharex=True)
    for ax, f in zip(axes, ("gas", "elec")):
        k = KEV[f]; c = COLOR[f]; yrs = sorted(P[f]["central"])
        lo = [P[f]["low"][y] for y in yrs]; hi = [P[f]["high"][y] for y in yrs]
        ovr = [n for n, d in (("low", LOW_OVERRIDE), ("high", HIGH_OVERRIDE)) if f in d]
        band_lbl = ("KEV 2030 bandwidth" if not ovr else
                    "KEV 2030 band, %s arm = observed %s held constant" % ("/".join(ovr), OBSERVED["date"]))
        ax.fill_between(yrs, lo, hi, color=c, alpha=.16, lw=0, label=band_lbl)
        for arm, ls, lw in (("low", "--", 1.0), ("high", "--", 1.0), ("central", "-", 2.4)):
            v = [P[f][arm][y] for y in yrs]
            ax.plot(yrs, v, color=c, ls=ls, lw=lw, label="KEV central" if arm == "central" else None)
            ax.annotate(f"{v[-1]:.3f}", (yrs[-1], v[-1]), xytext=(4, 0), textcoords="offset points",
                        fontsize=8.5, color=c, va="center")
        n = np.array(yrs) - a.start
        for r, ls in zip(OLD[f], (":", ":", ":")):
            ax.plot(yrs, k["retail2024"] * (1 + r) ** n, color="#b2182b", lw=0.9, ls=ls, alpha=.7)
        ax.plot([], [], color="#b2182b", lw=0.9, ls=":", label="previous constant-CAGR paths")
        ax.axvline(a.knee, color="#888", lw=0.8, ls="-", alpha=.5)
        ax.annotate("end of KEV\nprojection", (a.knee, ax.get_ylim()[1]), xytext=(4, -14),
                    textcoords="offset points", fontsize=8, color="#666", va="top")
        ax.set_title(f"{k['label']} — retail", fontsize=11, loc="left")
        ax.set_xlabel("year"); ax.set_xlim(a.start, a.end + 2); ax.set_ylim(bottom=0)
        ax.grid(alpha=.25); ax.legend(fontsize=8.5, frameon=False, loc="upper left")
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    axes[0].set_ylabel("retail price (€/kWh, constant 2024 €)")
    fig.suptitle("Retail energy-price paths from KEV 2025: wholesale ramp to 2030, constant thereafter, "
                 "additive commodity passthrough", fontsize=11.5)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    fig.savefig(a.out, dpi=150, bbox_inches="tight"); plt.close(fig)
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil; shutil.copy2(a.out, a.copy_to)
    print(f"csv    -> {os.path.relpath(a.csv, ROOT)}")
    print(f"figure -> {os.path.relpath(a.out, ROOT)}")


if __name__ == "__main__":
    main()
