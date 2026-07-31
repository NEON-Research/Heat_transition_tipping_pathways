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

TECHS=["NATURAL_GAS_BOILER","HYBRID_HEAT_PUMP","ELECTRIC_HEAT_PUMP","DISTRICT_HEATING"]
NICE={"NATURAL_GAS_BOILER":"Gas boiler","HYBRID_HEAT_PUMP":"Hybrid heat pump",
      "ELECTRIC_HEAT_PUMP":"Electric heat pump","DISTRICT_HEATING":"District heating"}

def tag(scope):
    k,n=scope.split(":"); return n.strip().lower().replace(" ","-") if k=="province" else scope

def traj(path, tech, scen="baseline"):
    by=defaultdict(lambda: defaultdict(float))
    for r in csv.DictReader(open(path)):
        if r["scenario_name"]!=scen or r["ownership"]!="TOTAL": continue
        by[int(float(r["year"]))][r["heating_system"]]+=float(r["installed_current"])
    return {y:100*d.get(tech,0)/(sum(d.values()) or 1) for y,d in sorted(by.items())}

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--scope",default="province:Noord-Brabant")
    a=ap.parse_args(); t=tag(a.scope); base=f"../results/{t}"
    outdir=os.path.join(base,"figures"); os.makedirs(outdir,exist_ok=True)
    retained=json.load(open("../results/calib/calibration_search.json"))["retained"]

    # ---- Figure 1: preference band, coloured by affordability weight, price runs overlaid ----
    sets=sorted(glob.glob(f"{base}/calib/set*/simulation_results.csv"))
    def idx(p): return int(re.search(r"set(\d+)_",p).group(1))
    aff=[retained[idx(p)]["sampled"]["shareAffordability"] for p in sets]
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
    f1=os.path.join(outdir,"fig_band_affordability.png"); fig.savefig(f1,dpi=130,bbox_inches="tight"); plt.close(fig)

    # ---- Figure 2: price triptych, 2050 mix ----
    def mix2050(path):
        by=defaultdict(lambda: defaultdict(float))
        for r in csv.DictReader(open(path)):
            if r["scenario_name"]=="baseline" and r["ownership"]=="TOTAL" and int(float(r["year"]))==2050:
                by[r["iteration"]][r["heating_system"]]+=float(r["installed_current"])
        out={te:np.mean([100*d.get(te,0)/(sum(d.values()) or 1) for d in by.values()]) for te in TECHS}
        return out
    static=f"{base}/calib/set00_mad0.21/simulation_results.csv"
    cols=[("price LOW",price.get("low")),("static",static),("price HIGH",price.get("high"))]
    cols=[(n,p) for n,p in cols if p and os.path.exists(p)]
    data=np.array([[mix2050(p)[te] for te in TECHS] for _,p in cols])
    fig,ax=plt.subplots(figsize=(7,4.5)); x=np.arange(len(cols)); bottom=np.zeros(len(cols))
    colors=["#7f7f7f","#2ca02c","#1f77b4","#ff7f0e"]
    for j,te in enumerate(TECHS):
        ax.bar(x,data[:,j],bottom=bottom,label=NICE[te],color=colors[j]); bottom+=data[:,j]
    ax.set_xticks(x); ax.set_xticklabels([n for n,_ in cols]); ax.set_ylabel("2050 share (%)")
    ax.set_title("2050 heating mix under the coupled energy-price bracket (best-fit weights)")
    ax.legend(fontsize=8,ncol=2,loc="lower center"); f2=os.path.join(outdir,"fig_price_triptych.png")
    fig.savefig(f2,dpi=130,bbox_inches="tight"); plt.close(fig)

    # ---- Figure 3: structural Morris heatmap (mean mu* factor x tech) + TOTAL bar ----
    mor={v:json.load(open(f"../results/calib/structural_morris_{v}.json"))
         for v in ("median","high_shareAffordability","best_fit")}
    factors=sorted(mor["median"]["total"],key=lambda f:-mor["median"]["total"][f])
    short={"NATURAL_GAS_BOILER":"gas","HYBRID_HEAT_PUMP":"hybrid","ELECTRIC_HEAT_PUMP":"electric","DISTRICT_HEATING":"dh"}
    M=np.array([[np.mean([mor[v]["mu_star"][f][te] for v in mor]) for te in TECHS] for f in factors])
    fig,(ax,axb)=plt.subplots(1,2,figsize=(11,5),gridspec_kw={"width_ratios":[3,1]})
    im=ax.imshow(M,cmap="magma",aspect="auto")
    ax.set_xticks(range(4)); ax.set_xticklabels([short[te] for te in TECHS])
    ax.set_yticks(range(len(factors))); ax.set_yticklabels(factors)
    for i in range(len(factors)):
        for j in range(4): ax.text(j,i,f"{M[i,j]:.1f}",ha="center",va="center",color="w",fontsize=8)
    ax.set_title("Morris $\\mu^*$ (mean over 3 weight vectors)"); fig.colorbar(im,ax=ax,shrink=.8)
    tot=[np.mean([mor[v]["total"][f] for v in mor]) for f in factors]
    axb.barh(range(len(factors)),tot,color="#555"); axb.set_yticks([]); axb.invert_yaxis()
    axb.set_title("$\\mu^*$ TOTAL"); axb.grid(alpha=.3,axis="x")
    f3=os.path.join(outdir,"fig_structural_morris.png"); fig.savefig(f3,dpi=130,bbox_inches="tight"); plt.close(fig)

    print("wrote:")
    for f in (f1,f2,f3): print("  ",f)

if __name__=="__main__": main()
