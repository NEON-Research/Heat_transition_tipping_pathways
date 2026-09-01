#!/usr/bin/env python3
"""One-step post-run analysis: regenerate every cross-set figure after a pathway_batch run.

  * per-scenario preference-band figures (all scenarios), coloured by affordability weight,
    with the energy-price runs overlaid, + a 2050 summary CSV per scenario;
  * global figures: structural Morris heatmap, energy-price triptych, EHP band-vs-price.

Each results CSV is read once. Usage:
    python analyze_all.py --scope province:Noord-Brabant
"""
import argparse, csv, glob, json, os, re
from collections import defaultdict
import numpy as np
from structural_sensitivity import FACTOR_LABEL, matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from matplotlib import cm; from matplotlib.colors import Normalize

HERE=os.path.dirname(os.path.abspath(__file__))
ROOT=os.path.normpath(os.path.join(HERE,".."))     # repo root: paths must not depend on the CWD

TECHS=["NATURAL_GAS_BOILER","HYBRID_HEAT_PUMP","ELECTRIC_HEAT_PUMP","DISTRICT_HEATING"]
NICE={"NATURAL_GAS_BOILER":"Gas boiler","HYBRID_HEAT_PUMP":"Hybrid HP",
      "ELECTRIC_HEAT_PUMP":"Electric HP","DISTRICT_HEATING":"District heating"}
SHORT={"NATURAL_GAS_BOILER":"gas","HYBRID_HEAT_PUMP":"hybrid","ELECTRIC_HEAT_PUMP":"electric","DISTRICT_HEATING":"dh"}

def tag(scope):
    k,_,n=scope.partition(":")
    return "nl" if k.lower()=="nl" else (n.strip().lower().replace(" ","-") if k=="province" else "gemeente_"+n.strip().lower())

def load(path):
    """-> {scenario: {tech: {year: mean-share%}}}, reading the CSV once (mean over iterations)."""
    # (scen,year,iter) -> {tech: installed}
    cells=defaultdict(lambda: defaultdict(float))
    for r in csv.DictReader(open(path)):
        if r["ownership"]!="TOTAL": continue
        cells[(r["scenario_name"],int(float(r["year"])),r["iteration"])][r["heating_system"]]+=float(r["installed_current"])
    grp=defaultdict(list)
    for (scn,yr,it),d in cells.items(): grp[(scn,yr)].append(d)
    out=defaultdict(lambda: defaultdict(dict))
    for (scn,yr),lst in grp.items():
        for te in TECHS:
            out[scn][te][yr]=float(np.mean([100*d.get(te,0)/(sum(d.values()) or 1) for d in lst]))
    return out

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--scope",default="province:Noord-Brabant"); a=ap.parse_args()
    t=tag(a.scope); base=os.path.join(ROOT,"results",t)
    figdir=os.path.join(base,"figures"); scndir=os.path.join(figdir,"scenarios"); os.makedirs(scndir,exist_ok=True)
    retained=json.load(open(os.path.join(ROOT,"results","calib","calibration_search.json")))["retained"]
    # every run folder under calib/, not just set* -- `--weights representative` writes best_fit/ etc.
    setpaths=sorted(glob.glob(os.path.join(base,"calib","*","simulation_results.csv")))
    reps=json.load(open(os.path.join(ROOT,"results","calib","calibration_search.json"))).get("representative",{})
    def afford(p):
        lab=os.path.basename(os.path.dirname(p))
        m=re.search(r"set(\d+)_",lab)
        if m: return retained[int(m.group(1))]["sampled"]["shareAffordability"]
        if lab in reps: return reps[lab]["sampled"]["shareAffordability"]
        return float("nan")
    # warn about runs produced by an older engine (no per-segment output) -- mixing them into one
    # band would compare different model versions
    stale=[p for p in setpaths if not os.path.exists(p.replace(".csv","_segments.csv"))]
    if stale and len(stale)!=len(setpaths):
        print(f"[warn] {len(stale)} of {len(setpaths)} runs under calib/ predate the current engine "
              f"(no _segments.csv). Mixing engine versions in one band is unsafe -- consider deleting "
              f"or re-running them. Stale: {', '.join(os.path.basename(os.path.dirname(p)) for p in stale[:4])}"
              + (" ..." if len(stale)>4 else ""), flush=True)
    print(f"loading {len(setpaths)} weight-set run(s) ...", flush=True)
    setdata=[load(p) for p in setpaths]
    aff=[afford(p) for p in setpaths]
    import math
    if all(math.isnan(x) for x in aff): aff=[0.5]*len(aff)
    else:
        m=[x for x in aff if not math.isnan(x)]; aff=[(sum(m)/len(m) if math.isnan(x) else x) for x in aff]
    norm=Normalize(min(aff),max(aff)); cmap=cm.viridis
    price={lvl:load(f"{base}/price_{lvl}/simulation_results.csv") for lvl in("low","high")
           if os.path.exists(f"{base}/price_{lvl}/simulation_results.csv")}
    scens=sorted(setdata[0].keys()); written=[]
    print(f"{len(scens)} scenarios; generating bands ...", flush=True)

    for scen in scens:
        fig,axes=plt.subplots(2,2,figsize=(12,8),sharex=True)
        for ax,tech in zip(axes.flat,TECHS):
            for sd,av in zip(setdata,aff):
                s=sd[scen][tech]; ax.plot(sorted(s),[s[y] for y in sorted(s)],color=cmap(norm(av)),lw=1,alpha=.85)
            for lvl,c in (("low","#1f78ff"),("high","#e31a1c")):
                if lvl in price:
                    s=price[lvl][scen][tech]
                    if s: ax.plot(sorted(s),[s[y] for y in sorted(s)],color=c,lw=2.4,label=f"price {lvl.upper()}",zorder=5)
            ax.set_title(NICE[tech]); ax.set_ylabel("% dwellings"); ax.grid(alpha=.3)
        axes[0,0].legend(fontsize=8,loc="upper left")
        sm=cm.ScalarMappable(norm=norm,cmap=cmap); sm.set_array([])
        cb=fig.colorbar(sm,ax=axes,shrink=.8,pad=.02); cb.set_label("affordability weight")
        fig.suptitle(f"{scen}: share band across weight sets (colour = affordability); price runs overlaid")
        fp=os.path.join(scndir,f"band_{scen}.png"); fig.savefig(fp,dpi=110,bbox_inches="tight"); plt.close(fig); written.append(fp)
        with open(os.path.join(scndir,f"summary2050_{scen}.csv"),"w",newline="") as fh:
            w=csv.writer(fh); w.writerow(["technology","min","median","max"])
            for te in TECHS:
                v=[sd[scen][te].get(2050,0) for sd in setdata]; w.writerow([te,f"{min(v):.2f}",f"{np.median(v):.2f}",f"{max(v):.2f}"])

    # global: Morris heatmap
    try:
        _mjs=sorted(glob.glob(os.path.join(ROOT,"results","calib","structural_morris_*.json")))
        mor={os.path.basename(p_)[len("structural_morris_"):-5]:json.load(open(p_)) for p_ in _mjs}
        # sort by the SAME quantity that is plotted -- the mean over the three weight vectors.
        # sorting by the median vector alone left the TOTAL bars non-monotonic.
        _meantot={f_:np.mean([mor[v]["total"][f_] for v in mor]) for f_ in next(iter(mor.values()))["total"]}
        factors=sorted(_meantot,key=lambda f_:-_meantot[f_])
        if not mor: raise RuntimeError("no structural_morris_*.json found")
        M=np.array([[np.mean([mor[v]["mu_star"][f][te] for v in mor]) for te in TECHS] for f in factors])
        fig,ax=plt.subplots(figsize=(8.6,5)); im=ax.imshow(M,cmap="Blues",aspect="auto",vmin=0)
        ax.set_xticks(range(4)); ax.set_xticklabels([SHORT[te] for te in TECHS]); ax.set_yticks(range(len(factors)))
        ax.set_yticklabels([FACTOR_LABEL.get(f_,f_) for f_ in factors],fontsize=9)
        _hi=M.max() or 1.0
        for i in range(len(factors)):
            for j in range(4): ax.text(j,i,f"{M[i,j]:.1f}",ha="center",va="center",fontsize=8,
                                       color=("white" if M[i,j] > 0.6*_hi else "#1a1a1a"))
        ax.set_title("Structural Morris $\\mu^*$ — Noord-Brabant screen\n(mean over 3 weight vectors)"); fig.colorbar(im,ax=ax,shrink=.8)
        fp=os.path.join(ROOT,"results","calib","fig_structural_morris.png"); fig.savefig(fp,dpi=130,bbox_inches="tight"); plt.close(fig); written.append(fp)
    except Exception as e: print("[skip morris]",e)

    # global: price triptych + EHP band-vs-price (baseline)
    if price and "baseline" in scens:
        static=setdata[0]   # reference run at static prices
        def m2050(sd): return {te:sd["baseline"][te].get(2050,0) for te in TECHS}
        cols=[("LOW",price.get("low")),("static",static),("HIGH",price.get("high"))]; cols=[(n,d) for n,d in cols if d]
        data=np.array([[m2050(d)[te] for te in TECHS] for _,d in cols]); x=np.arange(len(cols)); bottom=np.zeros(len(cols))
        fig,ax=plt.subplots(figsize=(7,4.5)); colors=["#7f7f7f","#2ca02c","#1f77b4","#ff7f0e"]
        for j,te in enumerate(TECHS): ax.bar(x,data[:,j],bottom=bottom,label=NICE[te],color=colors[j]); bottom+=data[:,j]
        ax.set_xticks(x); ax.set_xticklabels([n for n,_ in cols]); ax.set_ylabel("2050 share (%)")
        ax.set_title("2050 mix under energy-price bracket (best-fit, baseline)"); ax.legend(fontsize=8,ncol=2,loc="lower center")
        fp=os.path.join(figdir,"fig_price_triptych.png"); fig.savefig(fp,dpi=130,bbox_inches="tight"); plt.close(fig); written.append(fp)
        yrs=sorted(setdata[0]["baseline"]["ELECTRIC_HEAT_PUMP"])
        fig,ax=plt.subplots(figsize=(8,5))
        E=[sd["baseline"]["ELECTRIC_HEAT_PUMP"] for sd in setdata]
        ax.fill_between(yrs,[min(s[y] for s in E) for y in yrs],[max(s[y] for s in E) for y in yrs],color="#bbb",alpha=.5,label="preference band (static)")
        for lvl,c in (("low","#1f78ff"),("high","#e31a1c")):
            if lvl in price:
                s=price[lvl]["baseline"]["ELECTRIC_HEAT_PUMP"]; ax.plot(yrs,[s[y] for y in yrs],color=c,lw=2.5,label=f"price {lvl.upper()}")
        ax.set_ylabel("Electric HP (% dwellings)"); ax.set_xlabel("year"); ax.grid(alpha=.3); ax.legend(fontsize=8,loc="upper left")
        ax.set_title("Electric HP: energy-price scenarios exit the preference band")
        fp=os.path.join(figdir,"fig_ehp_band_vs_price.png"); fig.savefig(fp,dpi=130,bbox_inches="tight"); plt.close(fig); written.append(fp)

    print(f"done: {len(written)} figures. Per-scenario bands + summary2050_*.csv in {scndir}; global figures in {figdir}")

if __name__=="__main__": main()
