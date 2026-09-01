#!/usr/bin/env python3
"""Three figures for the ensemble result section.

  fig_band_baseline.png       2024-2050 technology-share band across the retained weight
                              ensemble, BASELINE only, coloured by the affordability weight,
                              on a shared y-axis.
  fig_ensemble_fit.png        (a) objective of all sampled sets against the retention
                              tolerance; (b) retention rate by tercile of each weight.
  fig_ensemble_weights.png    sampled vs retained values of each of the four decision weights.

Reads  results/calib/calibration_search.json
       results/calib/band_baseline_tidy.csv   (built by extract_band_baseline.py)
"""
import json, os, sys
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import Normalize

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.normpath(os.path.join(HERE, ".."))
CAL  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "results", "calib")
if not os.path.isabs(CAL) and not os.path.exists(CAL):
    CAL = os.path.join(ROOT, CAL)
OUT  = os.environ.get("HT_FIGOUT", CAL)

TECHS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP", "DISTRICT_HEATING"]
NICE  = {"NATURAL_GAS_BOILER": "Gas boiler", "HYBRID_HEAT_PUMP": "Hybrid heat pump",
         "ELECTRIC_HEAT_PUMP": "Electric heat pump", "DISTRICT_HEATING": "District heating"}
W     = ["shareAttitude", "shareSocialnorm", "shareAffordability", "shareIntention"]
WNICE = {"shareAttitude": "attitude", "shareSocialnorm": "social norm",
         "shareAffordability": "affordability", "shareIntention": "intention"}
INK, MUTED = "#222222", "#666666"

cal = json.load(open(os.path.join(CAL, "calibration_search.json")))
_bandcsv = os.path.join(CAL, "band_baseline_tidy.csv")
band = pd.read_csv(_bandcsv) if os.path.exists(_bandcsv) else None
tol, dflt = cal["tolerance"], cal["default_mad"]

if band is None:
    print("[skip] no band_baseline_tidy.csv in", CAL, "-- run extract_band_baseline.py for the band figure")
else:
    # ---------------------------------------------------------------- Figure 1: band
    aff = band.groupby("set")["shareAffordability"].first()
    norm = Normalize(aff.min(), aff.max()); cmap = cm.viridis
    ymax = np.ceil(band[band.year >= 2024].share.max() / 10.0) * 10
    
    fig, axes = plt.subplots(2, 2, figsize=(10.5, 7.0), sharex=True, sharey=True)
    for ax, tech in zip(axes.flat, TECHS):
        g = band[band.tech == tech]
        for s, gg in g.groupby("set"):
            gg = gg.sort_values("year")
            ax.plot(gg.year, gg.share, color=cmap(norm(aff[s])), lw=1.1, alpha=0.9)
        ax.set_title(NICE[tech], fontsize=11, color=INK, loc="left")
        ax.grid(alpha=0.25, lw=0.6)
        ax.set_ylim(0, ymax)
        ax.tick_params(labelsize=9, colors=MUTED)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
        for sp in ("left", "bottom"): ax.spines[sp].set_color("#bbbbbb")
    for ax in axes[:, 0]: ax.set_ylabel("% of dwellings", fontsize=9.5, color=INK)
    for ax in axes[1, :]: ax.set_xlabel("year", fontsize=9.5, color=INK)
    sm = cm.ScalarMappable(norm=norm, cmap=cmap); sm.set_array([])
    cb = fig.colorbar(sm, ax=axes, shrink=0.85, pad=0.02)
    cb.set_label("affordability weight (share of PBC)", fontsize=9.5, color=INK)
    cb.ax.tick_params(labelsize=8, colors=MUTED)
    fig.suptitle(f"2024–2050 technology share across the {aff.size} retained weight sets, baseline scenario",
                 fontsize=12, color=INK, x=0.06, ha="left")
    f1 = os.path.join(OUT, "fig_band_baseline.png")
    fig.savefig(f1, dpi=190, bbox_inches="tight"); plt.close(fig); print("wrote", f1)


# ---------------------------------------------------------------- Figure 2: fit
# Works with either retention score. `implausibility_max` is a single number per set (the worst
# output), so it sorts and thresholds exactly like the MAD does -- and the per-output values can be
# shown behind it, which the MAD cannot do.
S = pd.DataFrame([{**s["sampled"], "mad": s["mad"],
                   "imax": s.get("implausibility_max", np.nan)} for s in cal["samples"]])
HAS_I = S["imax"].notna().all()
if HAS_I:
    SCORE, CUT = "imax", cal.get("implaus_cut", 3.0)
    YLAB = "implausibility  $I$  (worst technology and year)"
    CUTLAB = f"cutoff $I$ = {CUT:g}"
else:
    SCORE, CUT = "mad", tol
    YLAB = "mean absolute deviation, 2023–2024 (%pt)"
    CUTLAB = f"retention tolerance {CUT:g} %pt"
S["kept"] = S[SCORE] <= CUT
KEEP, DROP = "#2c7fb8", "#cccccc"

fig, axA = plt.subplots(1, 1, figsize=(6.6, 4.2))
s_ = S.sort_values(SCORE).reset_index(drop=True)

axA.bar(s_.index, s_[SCORE], width=1.0, zorder=2,
        color=[KEEP if k else DROP for k in s_.kept], linewidth=0)
axA.axhline(CUT, color="#b2182b", lw=1.4, ls="--", zorder=3)
axA.text(len(s_) - 1, CUT - 0.03 * axA.get_ylim()[1], CUTLAB,
         ha="right", va="top", fontsize=8.5, color="#b2182b")
if not HAS_I:
    axA.axhline(dflt, color=MUTED, lw=1.2, ls=":")
    axA.text(len(s_) - 1, dflt + 0.03 * axA.get_ylim()[1],
             f"uncalibrated default {dflt:.2f}", ha="right", va="bottom",
             fontsize=8.5, color=MUTED)
axA.set_xlabel("sampled weight set, ranked by fit", fontsize=9.5, color=INK)
axA.set_ylabel(YLAB, fontsize=9.5, color=INK)
axA.set_title(f"{s_.kept.sum()} of {len(s_)} sets are not ruled out",
              fontsize=11, color=INK, loc="left")
axA.set_xlim(-1, len(s_)); axA.grid(axis="y", alpha=0.25, lw=0.6)
axA.tick_params(labelsize=9, colors=MUTED)
for sp in ("top", "right"): axA.spines[sp].set_visible(False)
for sp in ("left", "bottom"): axA.spines[sp].set_color("#bbbbbb")

fig.tight_layout()
f2 = os.path.join(OUT, "fig_ensemble_fit.png")
fig.savefig(f2, dpi=190, bbox_inches="tight"); plt.close(fig); print("wrote", f2)

# ---------------------------------------------------------------- Figure 3: weight distributions
import matplotlib.patches as mp
WNICE2 = WNICE
BOUNDS = {"shareAttitude": (0.15, 0.60), "shareSocialnorm": (0.15, 0.60),
          "shareAffordability": (0.30, 0.90), "shareIntention": (0.20, 0.80)}
ALL = "#d9d9d9"
fig,axes=plt.subplots(1,4,figsize=(12.4,3.4))
for ax,w in zip(axes,W):
    lo,hi=0.0,1.0; bins=np.round(np.arange(0.0,1.0001,0.1),2)   # common axis for all four weights
    allv=S[w].values; kept=S[S.kept][w].values
    ax.hist(allv,bins=bins,color=ALL,edgecolor="white",linewidth=0.7,
            label=f"sampled ({len(allv)})",zorder=1)
    ax.hist(kept,bins=bins,color=KEEP,edgecolor="white",linewidth=0.7,
            label=f"retained ({len(kept)})",zorder=2)
    ax.set_title(WNICE[w],fontsize=10.5,color=INK,loc="left")
    ax.set_xlim(0,1); ax.set_xticks(np.arange(0,1.01,0.2))
    ax.set_xlabel("share within its group",fontsize=8.5,color=MUTED)
    ax.text(0.99,0.965,f"retained {kept.min():.2f}–{kept.max():.2f}",
            transform=ax.transAxes,ha="right",va="top",fontsize=8.5,color=MUTED)
    ax.tick_params(labelsize=8.5,colors=MUTED)
    for sp in ("top","right"): ax.spines[sp].set_visible(False)
    for sp in ("left","bottom"): ax.spines[sp].set_color("#bbbbbb")
    ax.grid(axis="y",alpha=0.22,lw=0.6)
_ymax=max(np.histogram(S[w].values,bins=np.round(np.arange(0.0,1.0001,0.1),2))[0].max() for w in W)
for ax in axes: ax.set_ylim(0, 1.15*_ymax)
axes[0].set_ylabel("number of weight sets",fontsize=9.5,color=INK)
h,l=axes[0].get_legend_handles_labels()
fig.legend(h,l,fontsize=8.5,frameon=False,ncol=2,loc="lower center",bbox_to_anchor=(0.5,-0.04))
fig.suptitle("Sampled and retained values of each decision weight",fontsize=11.5,color=INK)
fig.tight_layout(rect=[0,0.06,1,0.94])
f3 = os.path.join(OUT, "fig_ensemble_weights.png")
fig.savefig(f3, dpi=190, bbox_inches="tight"); plt.close(fig); print("wrote", f3)

# ---------------------------------------------------------------- Figure 4: simulated vs observed
# Only possible once the search stores each set's simulated mix. Shows what the retention rule
# actually bought: the ensemble against the record it was matched to, with the uncalibrated
# parameterisation for reference.
if "observed" in cal and all("mix" in r for r in cal["retained"]):
    obs = cal["observed"]; years = sorted(int(y) for y in obs)
    FOCUS = ["NATURAL_GAS_BOILER", "HYBRID_HEAT_PUMP", "ELECTRIC_HEAT_PUMP"]
    NICE3 = {"NATURAL_GAS_BOILER": "Gas boiler", "HYBRID_HEAT_PUMP": "Hybrid heat pump",
             "ELECTRIC_HEAT_PUMP": "Electric heat pump"}
    dflt_mix = cal.get("default_mix")
    fig, axes = plt.subplots(1, 3, figsize=(11.2, 3.6))
    for ax, t in zip(axes, FOCUS):
        lo = [min(r["mix"][str(y)][t] for r in cal["retained"]) for y in years]
        hi = [max(r["mix"][str(y)][t] for r in cal["retained"]) for y in years]
        md = [float(np.median([r["mix"][str(y)][t] for r in cal["retained"]])) for y in years]
        ax.fill_between(years, lo, hi, color="#2c7fb8", alpha=0.22, lw=0, label="retained ensemble")
        ax.plot(years, md, color="#2c7fb8", lw=1.8, label="ensemble median")
        ax.plot(years, [obs[str(y)][t] for y in years], "o-", color="#1a1a1a", lw=1.8, ms=5,
                label="observed (CBS)")
        if dflt_mix:
            ax.plot(years, [dflt_mix[str(y)][t] for y in years], "--", color="#999999", lw=1.4,
                    label="uncalibrated default")
        ax.set_title(NICE3[t], fontsize=10.5, color=INK, loc="left")
        ax.set_xticks(years); ax.set_xlabel("year", fontsize=9, color=MUTED)
        ax.grid(alpha=0.25, lw=0.6); ax.tick_params(labelsize=9, colors=MUTED)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
        for sp in ("left", "bottom"): ax.spines[sp].set_color("#bbbbbb")
    axes[0].set_ylabel("% of dwellings", fontsize=9.5, color=INK)
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, fontsize=8.5, frameon=False, ncol=4, loc="lower center", bbox_to_anchor=(0.5, -0.06))
    fig.suptitle("Simulated against observed, over the window the ensemble was matched to",
                 fontsize=11.5, color=INK)
    fig.tight_layout(rect=[0, 0.05, 1, 0.94])
    f4 = os.path.join(OUT, "fig_ensemble_observed.png")
    fig.savefig(f4, dpi=190, bbox_inches="tight"); plt.close(fig); print("wrote", f4)
