#!/usr/bin/env python3
"""What a household actually pays, from model/data/reference/energy_prices_by_year.csv.

  a. fig_energy_price_components.png
     The per-kWh retail price broken into its parts (commodity + energy tax, both incl. btw),
     per year, per fuel, for the three price arms. Shows both the composition and how little of
     the bill the arm can actually move: only the commodity part differs between arms, because
     the energy tax is policy and is held at its last observed level.

  b. fig_heating_cost_comparison.png
     Annual cost of delivering the same heat with a gas boiler versus an electric heat pump.
     Gas carries the standing charge, because a dwelling that leaves gas stops paying it.
     Electricity does not: every dwelling keeps an electricity connection whatever it heats with,
     so that charge is identical in both options and cancels. Efficiencies come from
     heating_system_data.csv, so the figure cannot drift from what the model runs.

    python results_analysis/energy_cost_figures.py [--demand 10552]
"""
import argparse, csv, os
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
REF  = os.path.join(ROOT, "model", "data", "reference")

# Arm names in the price table -> the scenario vocabulary, so the two cannot be confused.
ARMS = [("low",     "GAS_LOW_ELEC_HIGH",  "gas cheap, electricity dear"),
        ("central", "CENTRAL",            "both central"),
        ("high",    "GAS_HIGH_ELEC_LOW",  "gas dear, electricity cheap")]

# Okabe-Ito. Two steps within each fuel for the stacked parts: the darker step is the commodity
# (the part the arm moves), the lighter one the energy tax (policy, flat across arms).
C = {"gas_c": "#595959", "gas_t": "#BFBFBF", "elec_c": "#0072B2", "elec_t": "#9ECAE8"}
INK, MUTED = "#222222", "#666666"


def read_prices():
    p = os.path.join(REF, "energy_prices_by_year.csv")
    rows = [ln for ln in open(p, encoding="utf-8") if not ln.lstrip().startswith("#")]
    d = defaultdict(dict)
    for r in csv.DictReader(rows):
        # the raw observed columns are blank for projected years, so parse leniently
        def num(k, v):
            if k == "basis" or v == "":
                return v
            try: return float(v)
            except ValueError: return v
        d[r["arm"]][int(r["year"])] = {k: num(k, v) for k, v in r.items()
                                       if k not in ("year", "arm")}
    return d


def latest_month():
    """The most recent CBS monthly observation, constant 2024 EUR/kWh.

    The table stores 2026 as the Jan-Aug MEAN, which understates where the series actually sits at
    the end of the observed window -- gas rose through 2026. Marking the latest month says plainly
    where 'now' is relative to the projected band."""
    src = os.path.join(ROOT, "data", "sources", "cbs_85592NED_consumer_energy_tariffs.csv")
    if not os.path.exists(src):
        return None
    KWH, CPI, BASE = 9.769, {2021: 2.7, 2022: 10.0, 2023: 3.8, 2024: 3.3, 2025: 3.3, 2026: 3.3}, 2024
    def defl(y, m):
        f = 1.0
        if y > BASE:
            for yy in range(BASE + 1, y): f *= 1 + CPI[yy] / 100
            f *= (1 + CPI[y] / 100) ** ((m - 6.5) / 12 + 0.5)
        return 1 / f
    rows = [l for l in open(src, encoding="utf-8") if not l.startswith("#")]
    mon = [r for r in csv.DictReader(rows) if "MM" in r["period"]]
    if not mon:
        return None
    r = mon[-1]
    y, m = int(r["period"][:4]), int(r["period"][6:])
    d = defl(y, m)
    return dict(year=y, month=m, t=y + (m - 0.5) / 12,
                gas=(float(r["gas_levering_eur_m3"]) + float(r["gas_belasting_eur_m3"])) / KWH * d,
                elec=(float(r["elec_levering_eur_kwh"]) + float(r["elec_belasting_eur_kwh"])) * d)


def read_systems():
    """Efficiency and connection flags straight from the model's own table."""
    p = os.path.join(REF, "heating_system_data.csv")
    out = {}
    for r in csv.DictReader(open(p, encoding="utf-8")):
        out[r["type"]] = r
    return out


def last_observed(P):
    """Final year whose `basis` says observed -- before this the three arms are identical."""
    return max(y for y, v in P["central"].items() if str(v.get("basis", "")).startswith("observed"))


def fig_components(P, out, start, end):
    fig, axes = plt.subplots(2, 3, figsize=(12.6, 6.0), sharex=True)
    obs = last_observed(P)
    top = {f: 1.12 * max(P[a][y][f"{f}_commodity_eur_kwh"] + P[a][y][f"{f}_tax_eur_kwh"]
                         for a, _, _ in ARMS for y in P[a] if start <= y <= end)
           for f in ("gas", "elec")}
    for j, (arm, vocab, blurb) in enumerate(ARMS):
        yrs = [y for y in sorted(P[arm]) if start <= y <= end]
        for i, (fuel, cc, tc, unit) in enumerate(
                [("gas", C["gas_c"], C["gas_t"], "Natural gas"),
                 ("elec", C["elec_c"], C["elec_t"], "Electricity")]):
            ax = axes[i, j]
            com = [P[arm][y][f"{fuel}_commodity_eur_kwh"] for y in yrs]
            tax = [P[arm][y][f"{fuel}_tax_eur_kwh"] for y in yrs]
            ax.stackplot(yrs, [com, tax], colors=[cc, tc], linewidth=0)
            tot = [c + t for c, t in zip(com, tax)]
            ax.plot(yrs, tot, color=INK, lw=1.2)
            ax.annotate(f"{tot[-1]:.3f}", (yrs[-1], tot[-1]), xytext=(4, 0),
                        textcoords="offset points", fontsize=8.5, color=INK, va="center")
            ax.set_xlim(start, end + 3); ax.set_ylim(0, top[fuel])
            ax.axvline(obs, color=MUTED, lw=0.9, ls="--", alpha=.8)
            if i == 0 and j == 0:
                ax.annotate("observed \u2192 projected", (obs, top[fuel]), xytext=(3, -8),
                            textcoords="offset points", fontsize=8, color=MUTED, va="top")
            ax.grid(alpha=.2); ax.set_axisbelow(True)
            for sp in ("top", "right"): ax.spines[sp].set_visible(False)
            if j == 0: ax.set_ylabel(f"{unit}\n€/kWh", fontsize=9.5)
            if i == 0: ax.set_title(f"{vocab}\n{blurb}", fontsize=9.5, color=INK)
            if i == 1: ax.set_xlabel("year", fontsize=9.5)
    h = [plt.Rectangle((0, 0), 1, 1, color=C["gas_c"]), plt.Rectangle((0, 0), 1, 1, color=C["gas_t"]),
         plt.Rectangle((0, 0), 1, 1, color=C["elec_c"]), plt.Rectangle((0, 0), 1, 1, color=C["elec_t"])]
    fig.legend(h, ["gas: commodity", "gas: energy tax", "electricity: commodity",
                   "electricity: energy tax"],
               ncol=4, loc="lower center", frameon=False, fontsize=9.5, bbox_to_anchor=(0.5, -0.03))
    fig.suptitle("Retail energy price per kWh by component, constant 2024 \u20ac. Observed to "
                 f"{obs}, identical in all three panels, then the arms diverge \u2014 only the "
                 "commodity part differs.", fontsize=11)
    fig.tight_layout(rect=(0, 0.05, 1, 0.94))
    fig.savefig(out, dpi=170, bbox_inches="tight"); plt.close(fig)
    return out


def fig_bands(P, out, start, end):
    """Total retail price per kWh for gas, electricity and district heat, central path plus the
    low-high band.

    One panel, no numbers inside it: the figure is for a document where the caption carries the
    values. The arms are decorrelated, so each fuel's envelope comes from opposite arms -- gas is
    dearest on GAS_HIGH_ELEC_LOW, electricity on GAS_LOW_ELEC_HIGH -- which is why the two bands
    must not be read as independent uncertainties. District heat is per kWh DELIVERED (ACM
    variable tariff) and moves by the same percentage as the gas retail price in every arm (the
    'niet meer dan anders' linkage), so its band is the gas band scaled."""
    obs = last_observed(P)
    yrs = [y for y in sorted(P["central"]) if start <= y <= end]

    def col(fuel, raw=False):
        base = "heat_variable" if fuel == "heat" else f"{fuel}_retail"
        return f"{base}_raw_eur_kwh" if raw else f"{base}_eur_kwh"

    def band(fuel):
        c = col(fuel)
        return ([min(P[a][y][c] for a, _, _ in ARMS) for y in yrs],
                [P["central"][y][c] for y in yrs],
                [max(P[a][y][c] for a, _, _ in ARMS) for y in yrs])

    FUELS = (("gas", "#595959", "Natural gas"),
             ("heat", "#D55E00", "District heat (per kWh delivered)"),
             ("elec", "#0072B2", "Electricity"))
    fig, ax = plt.subplots(figsize=(8.4, 4.6))
    for fuel, colour, name in FUELS:
        lo, ce, hi = band(fuel)
        ax.fill_between(yrs, lo, hi, color=colour, alpha=.15, lw=0)
        ax.plot(yrs, ce, color=colour, lw=2.4)
        for v in (lo, hi):
            ax.plot(yrs, v, color=colour, lw=1.0, ls="--", alpha=.8)
        # what CBS (gas, electricity) or ACM (heat) reported before the crisis-year substitution,
        # so the assumption is visible
        rc = col(fuel, raw=True)
        ry = [y for y in yrs if P["central"][y].get(rc) not in (None, "")]
        if ry:
            ax.plot(ry, [float(P["central"][y][rc]) for y in ry],
                    color=colour, lw=1.4, ls=":", alpha=.95)

    lm = latest_month()      # reported in the console summary, not drawn
    # No observed/projected divider: every solid and dashed line is an ASSUMPTION (in the crisis
    # years too), and only the dotted lines are observed, so a divider would mislabel the solid
    # line left of it as observed.

    peak = max([float(P["central"][y][col(f, raw=True)])
                for f in ("gas", "heat", "elec") for y in yrs
                if P["central"][y].get(col(f, raw=True)) not in (None, "")] or [0])
    ax.set_xlim(start, end); ax.set_ylim(0, max(peak, 0.31) * 1.08)
    ax.set_xticks([y for y in range(start + 3, end + 1, 5)] + [end])
    ax.set_xlabel("year", fontsize=10)
    ax.set_ylabel("retail price (\u20ac/kWh, constant 2024 \u20ac)", fontsize=10)
    ax.grid(alpha=.2); ax.set_axisbelow(True)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    # legend: one row for the fuels (colour), one for the line styles (what each line means)
    fuels = [plt.Rectangle((0, 0), 1, 1, color=c, alpha=.85, label=n) for _, c, n in FUELS]
    styles = [Line2D([], [], color=INK, lw=2.4, label="assumed, central path"),
              Line2D([], [], color=INK, lw=1.0, ls="--", label="assumed, low / high arm"),
              Line2D([], [], color=INK, lw=1.4, ls=":", label="observed prices (CBS, ACM)")]
    # matplotlib fills legend columns top-down, so interleave to get fuels on row 1, styles on row 2
    h = [x for pair in zip(fuels, styles) for x in pair]
    ax.legend(handles=h, ncol=3, frameon=False, fontsize=9, loc="upper center",
              bbox_to_anchor=(0.5, -0.13))
    ax.set_title("Assumed retail energy prices per kWh", fontsize=12, loc="left")
    fig.tight_layout()
    fig.savefig(out, dpi=170, bbox_inches="tight"); plt.close(fig)

    glo, gce, ghi = band("gas"); elo, ece, ehi = band("elec"); hlo, hce, hhi = band("heat")
    print(f"\n{end}: gas {glo[-1]:.4f}\u2013{ghi[-1]:.4f} (central {gce[-1]:.4f})   "
          f"electricity {elo[-1]:.4f}\u2013{ehi[-1]:.4f} (central {ece[-1]:.4f})   "
          f"district heat {hlo[-1]:.4f}\u2013{hhi[-1]:.4f} (central {hce[-1]:.4f})")
    print(f"      spark spread {elo[-1]/ghi[-1]:.2f}\u2013{ehi[-1]/glo[-1]:.2f} "
          f"(central {ece[-1]/gce[-1]:.2f})")
    if lm:
        print(f"latest month {lm['year']}-{lm['month']:02d}: gas {lm['gas']:.4f}   "
              f"electricity {lm['elec']:.4f}   spread {lm['elec']/lm['gas']:.2f}")
    return out


def fig_heating_cost(P, S, out, start, end, demand):
    eta_gas = float(S["NATURAL_GAS_BOILER"]["efficiency_primary_source"])
    scop    = float(S["ELECTRIC_HEAT_PUMP"]["efficiency_primary_source"])
    gas_kwh, elec_kwh = demand / eta_gas, demand / scop
    fig, axes = plt.subplots(1, 3, figsize=(12.6, 4.2), sharey=True)
    # sharey with a per-panel autoscale sized the axis on the first arm and clipped the dearest gas
    # line in the last one; size it once on the global maximum instead.
    ymax = max(P[a][y]["gas_retail_eur_kwh"] * gas_kwh + P[a][y]["gas_fixed_eur_year"]
               for a, _, _ in ARMS for y in P[a] if start <= y <= end)
    obs = last_observed(P)
    summary = []
    for j, (arm, vocab, blurb) in enumerate(ARMS):
        ax = axes[j]
        yrs = [y for y in sorted(P[arm]) if start <= y <= end]
        gv = np.array([P[arm][y]["gas_retail_eur_kwh"] * gas_kwh for y in yrs])
        gf = np.array([P[arm][y]["gas_fixed_eur_year"] for y in yrs])
        ev = np.array([P[arm][y]["elec_retail_eur_kwh"] * elec_kwh for y in yrs])
        gt = gv + gf
        ax.fill_between(yrs, ev, gt, where=(gt >= ev), color="#0072B2", alpha=.10, lw=0)
        ax.fill_between(yrs, ev, gt, where=(gt < ev), color="#7F7F7F", alpha=.14, lw=0)
        ax.plot(yrs, gt, color="#595959", lw=2.4)
        ax.plot(yrs, gv, color="#595959", lw=1.1, ls=":")
        ax.plot(yrs, ev, color="#0072B2", lw=2.4)
        for v, c in ((gt[-1], "#595959"), (ev[-1], "#0072B2")):
            ax.annotate(f"€{v:,.0f}", (yrs[-1], v), xytext=(5, 0), textcoords="offset points",
                        fontsize=9, color=c, va="center")
        d = gt[-1] - ev[-1]
        ax.set_title(f"{vocab}\n{blurb}", fontsize=9.5, color=INK)
        ax.annotate(f"heat pump {'cheaper' if d > 0 else 'dearer'} by €{abs(d):,.0f}/yr in {yrs[-1]}",
                    (0.5, 0.04), xycoords="axes fraction", ha="center", fontsize=9,
                    color="#0072B2" if d > 0 else "#595959")
        ax.set_xlim(start, end + 4); ax.set_ylim(0, ymax * 1.10)
        ax.axvline(obs, color=MUTED, lw=0.9, ls="--", alpha=.8)
        if j == 0:
            ax.annotate("observed \u2192 projected", (obs, ymax * 1.10), xytext=(3, -8),
                        textcoords="offset points", fontsize=8, color=MUTED, va="top")
        ax.set_xlabel("year", fontsize=9.5); ax.grid(alpha=.2); ax.set_axisbelow(True)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
        summary.append((vocab, gv[-1], gf[-1], gt[-1], ev[-1], d))
    axes[0].set_ylabel("annual energy cost (€, constant 2024 €)", fontsize=9.5)
    h = [Line2D([], [], color="#595959", lw=2.4, label="gas boiler: energy + standing charge"),
         Line2D([], [], color="#595959", lw=1.1, ls=":", label="gas boiler: energy only"),
         Line2D([], [], color="#0072B2", lw=2.4, label=f"electric heat pump (SCOP {scop:g}), energy only")]
    fig.legend(handles=h, ncol=3, loc="lower center", frameon=False, fontsize=9.5,
               bbox_to_anchor=(0.5, -0.06))
    fig.suptitle(f"Annual cost of {demand:,.0f} kWh of heat: gas boiler (η {eta_gas:g}) versus "
                 f"electric heat pump (SCOP {scop:g})", fontsize=11.5)
    fig.tight_layout(rect=(0, 0.07, 1, 0.92))
    fig.savefig(out, dpi=170, bbox_inches="tight"); plt.close(fig)

    print(f"\n{demand:,.0f} kWh heat -> {gas_kwh:,.0f} kWh gas (η {eta_gas:g}) / "
          f"{elec_kwh:,.0f} kWh electricity (SCOP {scop:g})")
    print(f"\n{end} annual cost, constant 2024 €")
    print(f"{'arm':20s}{'gas energy':>12s}{'gas fixed':>11s}{'gas total':>11s}"
          f"{'heat pump':>11s}{'HP saves':>10s}")
    for v, a, b, c, d, e in summary:
        print(f"{v:20s}{a:12,.0f}{b:11,.0f}{c:11,.0f}{d:11,.0f}{e:10,.0f}")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--demand", type=float, default=10552, help="annual heat demand, kWh")
    ap.add_argument("--start", type=int, default=2022,
                    help="2022 by default: the observed years put the projected arms in context")
    ap.add_argument("--end", type=int, default=2050)
    ap.add_argument("--outdir", default=os.path.join(ROOT, "results", "nl", "figures"))
    ap.add_argument("--copy-to", default=os.path.join(ROOT, "heat_transition_tipping_dynamics", "figures"))
    a = ap.parse_args()
    P, S = read_prices(), read_systems()
    os.makedirs(a.outdir, exist_ok=True)
    w = [fig_bands(P, os.path.join(a.outdir, "fig_energy_price_bands.png"), a.start, a.end),
         fig_components(P, os.path.join(a.outdir, "fig_energy_price_components.png"), a.start, a.end),
         fig_heating_cost(P, S, os.path.join(a.outdir, "fig_heating_cost_comparison.png"),
                          a.start, a.end, a.demand)]
    if a.copy_to and os.path.isdir(a.copy_to):
        import shutil
        for f in w: shutil.copy2(f, a.copy_to)
    print("\nwrote:"); [print("  ", f) for f in w]


if __name__ == "__main__":
    main()
