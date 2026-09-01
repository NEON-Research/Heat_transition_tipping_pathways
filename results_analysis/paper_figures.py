#!/usr/bin/env python3
"""Publication figures for the three uncertainty tiers. Reads the batch outputs; writes PNGs.
    python paper_figures.py --scope province:Noord-Brabant
"""
import argparse, csv, glob, json, os, re
from collections import defaultdict
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import Normalize
from structural_sensitivity import FACTOR_LABEL

HERE=os.path.dirname(os.path.abspath(__file__))
ROOT=os.path.normpath(os.path.join(HERE,".."))

TECHS=["NATURAL_GAS_BOILER","HYBRID_HEAT_PUMP","ELECTRIC_HEAT_PUMP","DISTRICT_HEATING"]
NICE={"NATURAL_GAS_BOILER":"Gas boiler","HYBRID_HEAT_PUMP":"Hybrid heat pump",
      "ELECTRIC_HEAT_PUMP":"Electric heat pump","DISTRICT_HEATING":"District heating"}

def tag(scope):
    k,_,n=scope.partition(":")
    if k.lower() in ("nl","netherlands"): return "nl"
    return n.strip().lower().replace(" ","-") if k=="province" else "gemeente_"+n.strip().lower()

def traj(path, tech, scen="baseline"):
    by=defaultdict(lambda: defaultdict(float))
    for r in csv.DictReader(open(path)):
        if r["scenario_name"]!=scen or r["ownership"]!="TOTAL": continue
        by[int(float(r["year"]))][r["heating_system"]]+=float(r["installed_current"])
    return {y:100*d.get(tech,0)/(sum(d.values()) or 1) for y,d in sorted(by.items())}

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--scope",default="province:Noord-Brabant")
    ap.add_argument("--calib",default=os.path.join(ROOT,"results","calib"),
                    help="directory holding calibration_search.json and structural_morris_*.json")
    ap.add_argument("--only",default="all",choices=["all","band","price","morris"],
                    help="'morris' draws the parameter-sensitivity heatmap alone; it needs only the\n"
                         "structural_morris_*.json files, not the 2024-2050 pathway runs")
    a=ap.parse_args(); t=tag(a.scope); base=os.path.join(ROOT,"results",t)
    CALDIR=os.path.abspath(a.calib)
    outdir=os.path.join(base,"figures"); os.makedirs(outdir,exist_ok=True)
    retained=json.load(open(os.path.join(CALDIR,"calibration_search.json")))["retained"]
    _written=[]

    # ---- Figure 1: preference band, coloured by affordability weight, price runs overlaid ----
    sets=sorted(glob.glob(os.path.join(base,"calib","set*","simulation_results.csv")))
    if not sets:   # scopes run with --ensemble-weights representative have no set* folders
        sets=sorted(glob.glob(os.path.join(base,"calib","*","simulation_results.csv")))
    RUNFIGS = bool(sets) and a.only in ("all","band","price")
    if not sets and a.only in ("all","band","price"):
        # the Morris heatmap reads only the screen JSONs, so a missing pathway run must skip the
        # run-based figures rather than abandon the whole script (it used to `return` here).
        print(f"[paper_figures] no runs under {base}/calib — skipping the band and price figures")
    if RUNFIGS:
        def idx(p): return int(re.search(r"set(\d+)_",p).group(1))
        reps=json.load(open(os.path.join(CALDIR,"calibration_search.json"))).get("representative",{})
        def afford(p):
            lab=os.path.basename(os.path.dirname(p)); m=re.search(r"set(\d+)_",lab)
            if m: return retained[int(m.group(1))]["sampled"]["shareAffordability"]
            return reps[lab]["sampled"]["shareAffordability"] if lab in reps else 0.5
        aff=[afford(p) for p in sets]
        norm=Normalize(min(aff),max(aff)); cmap=cm.viridis
        price={}
        for lvl in ("low","high"):
            pth=f"{base}/price_{lvl}/simulation_results.csv"
            if os.path.exists(pth): price[lvl]=pth
        fig,axes=plt.subplots(2,2,figsize=(12,8),sharex=True)
        for ax,tech in zip(axes.flat,TECHS):
            for p,av in zip(sets,aff):
                s=traj(p,tech); ax.plot(list(s),list(s.values()),color=cmap(norm(av)),lw=1,alpha=.85)
            for lvl,c in (("low","#1f78ff"),("high","#e31a1c")):
                if lvl in price:
                    s=traj(price[lvl],tech); ax.plot(list(s),list(s.values()),color=c,lw=2.6,
                        label=f"energy price {lvl.upper()}",zorder=5)
            ax.set_title(NICE[tech]); ax.set_ylabel("% of dwellings"); ax.grid(alpha=.3)
        axes[0,0].legend(fontsize=8,loc="upper left")
        sm=cm.ScalarMappable(norm=norm,cmap=cmap); sm.set_array([])
        cb=fig.colorbar(sm,ax=axes,shrink=.8,pad=.02); cb.set_label("affordability weight (share of PBC)")
        fig.suptitle("2024–2050 technology share: preference-uncertainty band (20 weight sets)\n"
                     "coloured by affordability weight; energy-price scenarios overlaid",fontsize=12)
        f1=os.path.join(outdir,"fig_band_affordability.png"); fig.savefig(f1,dpi=130,bbox_inches="tight"); plt.close(fig); _written.append(f1)

        # (the 2050 price triptych lived here; superseded by scenario_mix_grid.py, which shows
        # the full 2024-2050 stacked mix for every scenario including the price bracket)

    if a.only not in ("all","morris"):
        print("wrote:"); [print("  ",f) for f in _written]; return

    # ---- Figure 3: structural Morris heatmap (mean mu* factor x tech) + TOTAL bar ----
    _mjs=sorted(glob.glob(os.path.join(CALDIR,"structural_morris_*.json")))
    mor={os.path.basename(p_)[len("structural_morris_"):-5]:json.load(open(p_)) for p_ in _mjs}
    # sort by the SAME quantity that is plotted -- the mean over the three weight vectors.
    # sorting by the median vector alone left the TOTAL bars non-monotonic.
    if not mor:
        print(f"[paper_figures] no structural_morris_*.json in {CALDIR} — run structural_batch.py "
              f"first; skipping the parameter-sensitivity figure")
        print("wrote:"); [print("  ", f) for f in _written]; return
    _meantot={f_:np.mean([mor[v]["total"][f_] for v in mor]) for f_ in next(iter(mor.values()))["total"]}
    factors=sorted(_meantot,key=lambda f_:-_meantot[f_])
    short={"NATURAL_GAS_BOILER":"gas","HYBRID_HEAT_PUMP":"hybrid","ELECTRIC_HEAT_PUMP":"electric","DISTRICT_HEATING":"dh"}
    M=np.array([[np.mean([mor[v]["mu_star"][f][te] for v in mor]) for te in TECHS] for f in factors])
    fig,(ax,axb)=plt.subplots(1,2,figsize=(11,5),gridspec_kw={"width_ratios":[3,1]})
    # single-hue sequential ramp, light -> dark; annotation ink flips on cell luminance so the
    # numbers stay readable at both ends (magma with white text was illegible on its bright end).
    im=ax.imshow(M,cmap="Blues",aspect="auto",vmin=0)
    ax.set_xticks(range(4)); ax.set_xticklabels([short[te] for te in TECHS])
    ax.set_yticks(range(len(factors)))
    ax.set_yticklabels([FACTOR_LABEL.get(f_,f_) for f_ in factors],fontsize=9)
    _hi=M.max() or 1.0
    for i in range(len(factors)):
        for j in range(4):
            ax.text(j,i,f"{M[i,j]:.1f}",ha="center",va="center",fontsize=8,
                    color=("white" if M[i,j] > 0.6*_hi else "#1a1a1a"))
    ax.set_title("Parameter sensitivity, $\\mu^*$ per technology — Noord-Brabant screen\n(mean over 3 weight vectors)")
    fig.colorbar(im,ax=ax,shrink=.8,label="$\\mu^*$ (%pt per full-range move)")
    tot=[np.mean([mor[v]["total"][f] for v in mor]) for f in factors]
    axb.barh(range(len(factors)),tot,color="#555"); axb.set_yticks([]); axb.invert_yaxis()
    axb.set_title("$\\mu^*$ total\n(sum over technologies)",fontsize=10); axb.grid(alpha=.3,axis="x")
    f3=os.path.join(CALDIR,"fig_structural_morris.png"); fig.savefig(f3,dpi=130,bbox_inches="tight"); plt.close(fig)

    _written.append(f3)
    print("wrote:")
    for f in _written: print("  ",f)

if __name__=="__main__": main()
