"""
Observed Dutch consumer energy tariffs 2021-2026 (CBS 85592NED) versus the
KEV-derived price paths the model uses.

CBS 85592NED gives the VARIABLE consumer tariff, incl. btw:
    gas  = variabel leveringstarief (contractprijs) + energiebelasting   [EUR/m3]
    elec = variabel leveringstarief (contractprijs) + energiebelasting   [EUR/kWh]
Fixed transport / standing charges are excluded, which matches the definition
behind the model's 2024 retail anchors (gas 0.14, elec 0.32 EUR/kWh).

Nominal euros are deflated to constant 2024 euros with the CBS CPI annual
mutation (2021 2.7, 2022 10.0, 2023 3.8, 2024 3.3, 2025 3.3, 2026 3.3 %).

Usage:  python results_analysis/observed_price_history.py
"""
import csv, argparse
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT   = Path(__file__).resolve().parents[1]
SRC    = ROOT / "data" / "sources" / "cbs_85592NED_consumer_energy_tariffs.csv"
PATHS  = ROOT / "model" / "data" / "reference" / "energy_price_paths.csv"
OUTCSV = ROOT / "model" / "data" / "reference" / "observed_energy_prices.csv"
FIG    = ROOT / "results" / "nl" / "figures" / "fig_observed_vs_paths.png"

KWH_PER_M3 = 9.769
CPI = {2021: 2.7, 2022: 10.0, 2023: 3.8, 2024: 3.3, 2025: 3.3, 2026: 3.3}  # CBS, % per year
BASE_YEAR = 2024
COLOR = {"gas": "#999999", "elec": "#0072B2"}
LABEL = {"gas": "Natural gas", "elec": "Electricity"}


def deflator(year, month):
    """Index that converts nominal euros of (year, month) into BASE_YEAR euros."""
    f = 1.0
    if year > BASE_YEAR:
        for y in range(BASE_YEAR + 1, year):
            f *= 1 + CPI[y] / 100
        # part-year: months elapsed from the annual-average midpoint (mid-June)
        f *= (1 + CPI[year] / 100) ** ((month - 6.5) / 12 + 0.5)
    elif year < BASE_YEAR:
        for y in range(year + 1, BASE_YEAR + 1):
            f /= 1 + CPI[y] / 100
        f /= (1 + CPI[year] / 100) ** ((month - 6.5) / 12)
    return 1.0 / f


def load_observed():
    rows = []
    with open(SRC, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            rows.append(line)
    rd = csv.DictReader(rows)
    monthly, annual = [], {}
    for r in rd:
        p = r["period"]
        year = int(p[:4])
        gas_m3 = float(r["gas_levering_eur_m3"]) + float(r["gas_belasting_eur_m3"])
        elec   = float(r["elec_levering_eur_kwh"]) + float(r["elec_belasting_eur_kwh"])
        gas    = gas_m3 / KWH_PER_M3
        if p[4:6] == "JJ":
            annual[year] = dict(gas_nom=gas, elec_nom=elec, gas_m3=gas_m3,
                                gas_real=gas * deflator(year, 6.5),
                                elec_real=elec * deflator(year, 6.5))
        else:
            m = int(p[6:])
            d = deflator(year, m)
            monthly.append(dict(year=year, month=m, t=year + (m - 0.5) / 12,
                                gas_nom=gas, elec_nom=elec, gas_m3=gas_m3,
                                gas_real=gas * d, elec_real=elec * d))
    return monthly, annual


def load_paths():
    if not PATHS.exists():
        return {}
    out = {}
    with open(PATHS, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            out.setdefault(r["fuel"], {}).setdefault(r["arm"], {})[int(r["year"])] = float(r["retail_eur_per_kwh"])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", type=int, default=2032, help="right edge of the x-axis")
    a = ap.parse_args()

    monthly, annual = load_observed()
    P = load_paths()

    with open(OUTCSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["year", "month", "gas_eur_per_kwh_nominal", "gas_eur_per_kwh_2024",
                    "elec_eur_per_kwh_nominal", "elec_eur_per_kwh_2024"])
        for r in monthly:
            w.writerow([r["year"], r["month"], f"{r['gas_nom']:.5f}", f"{r['gas_real']:.5f}",
                        f"{r['elec_nom']:.5f}", f"{r['elec_real']:.5f}"])

    last = monthly[-1]
    base = annual[BASE_YEAR]
    print(f"CBS 85592NED variable consumer tariff, incl. btw   (constant {BASE_YEAR} EUR/kWh)\n")
    tag = "%dM%02d" % (last["year"], last["month"])
    print(f"{'':14s}{'2024 avg':>10s}{'2025 avg':>10s}{tag:>10s}{'vs 2024':>10s}")
    for f in ("gas", "elec"):
        v24, v25, vnow = base[f + "_real"], annual[2025][f + "_real"], last[f + "_real"]
        print(f"{LABEL[f]:14s}{v24:10.4f}{v25:10.4f}{vnow:10.4f}{100*(vnow/v24-1):9.1f} %")
    print(f"\nspark spread (elec/gas):  2024 {base['elec_real']/base['gas_real']:.2f}"
          f"   now {last['elec_real']/last['gas_real']:.2f}")
    if P:
        print("\nmodel arms at 2050 (constant 2024 EUR/kWh) and where the latest observation sits:")
        for f in ("gas", "elec"):
            arms = {k: P[f][k][2050] for k in ("low", "central", "high")}
            vnow = last[f + "_real"]
            inside = arms["low"] <= vnow <= arms["high"]
            print(f"  {LABEL[f]:12s} low {arms['low']:.4f}  central {arms['central']:.4f}  "
                  f"high {arms['high']:.4f}   observed {vnow:.4f}  "
                  f"{'inside' if inside else 'OUTSIDE'} the band")

    fig, axes = plt.subplots(1, 2, figsize=(12.4, 4.6))
    for ax, f in zip(axes, ("gas", "elec")):
        c = COLOR[f]
        t = [r["t"] for r in monthly]
        ax.plot(t, [r[f + "_nom"] for r in monthly], color=c, lw=1.0, alpha=.35,
                label="observed, nominal")
        ax.plot(t, [r[f + "_real"] for r in monthly], color=c, lw=1.8,
                label=f"observed, constant {BASE_YEAR} €")
        if f in P:
            yrs = sorted(P[f]["central"])
            yrs = [y for y in yrs if y <= a.end]
            lo = [P[f]["low"][y] for y in yrs]; hi = [P[f]["high"][y] for y in yrs]
            ax.fill_between(yrs, lo, hi, color="#b2182b", alpha=.10, lw=0,
                            label="model low–high arms")
            ax.plot(yrs, [P[f]["central"][y] for y in yrs], color="#b2182b", lw=1.6, ls="--",
                    label="model central")
        ax.annotate(f"{monthly[-1][f + '_real']:.3f}", (monthly[-1]["t"], monthly[-1][f + "_real"]),
                    xytext=(4, 6), textcoords="offset points", fontsize=8.5, color=c)
        ax.axvline(BASE_YEAR, color="#888", lw=0.8, alpha=.5)
        ax.set_title(f"{LABEL[f]} — variable consumer tariff", fontsize=11, loc="left")
        ax.set_xlabel("year"); ax.set_xlim(2021, a.end); ax.set_ylim(bottom=0)
        ax.grid(alpha=.25); ax.legend(fontsize=8.5, frameon=False, loc="upper right")
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    axes[0].set_ylabel("€/kWh")
    fig.suptitle("Observed consumer energy tariffs (CBS 85592NED) against the model's KEV-derived paths",
                 fontsize=11.5)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    FIG.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG, dpi=170)
    print(f"\ncsv    -> {OUTCSV.relative_to(ROOT)}")
    print(f"figure -> {FIG.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
