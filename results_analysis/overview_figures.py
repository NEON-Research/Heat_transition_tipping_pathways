#!/usr/bin/env python3
"""Two overview figures for the results section, from the ensemble median.

  A. stacked technology mix 2024-2050 for a selection of key scenarios (one panel each)
  B. stacked technology mix for the baseline, split by ownership type
  C. the same stack for ALL scenarios, grouped by the tipping mechanism each one perturbs
     (one row per mechanism, weak variant left / strong variant right) -- the ensemble-median
     counterpart of the per-run scenario_comparison_TOTAL grid, for the appendix

Shares are the median across the retained weight sets, so each panel is the central pathway of the
ensemble rather than a single run.

    python overview_figures.py --scope nl
    python overview_figures.py --scope nl --scenarios baseline,economic_learning_factor_high,...
    python overview_figures.py --scope nl --skip-all      # A and B only (faster)
"""
import argparse, csv, glob, os
from collections import defaultdict
import numpy as np, matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
import glue

HERE=os.path.dirname(os.path.abspath(__file__)); ROOT=os.path.normpath(os.path.join(HERE,".."))
ORDER=["NATURAL_GAS_BOILER","NATURAL_GAS_BLOCK","HYBRID_HEAT_PUMP","ELECTRIC_HEAT_PUMP","DISTRICT_HEATING"]
NICE={"NATURAL_GAS_BOILER":"Gas boiler","NATURAL_GAS_BLOCK":"Gas block","HYBRID_HEAT_PUMP":"Hybrid heat pump",
      "ELECTRIC_HEAT_PUMP":"Electric heat pump","DISTRICT_HEATING":"District heating"}
# same palette as HEATING_SYSTEM_COLORS in script_results.py, so the ensemble figures and the
# per-run diagnostics are directly comparable
COL={"NATURAL_GAS_BOILER":"#C0C0C0",   # silver
     "NATURAL_GAS_BLOCK":"gray",
     "HYBRID_HEAT_PUMP":"#D02090",     # violet red
     "ELECTRIC_HEAT_PUMP":"#1E90FF",   # dodger blue
     "DISTRICT_HEATING":"#CD853F"}     # peru
TITLE={"baseline":"Baseline","economic_learning_factor_high":"Fast economic learning",
       "social_learning_factor_high":"Strong social learning",
       "dh_policy_based_connection_obligation":"District-heating obligation",
       "grid_congestion_HP_ban":"Heat-pump ban under congestion",
       "actor_allignment_strategy":"Actor alignment"}
OWN=["PRIVATELY_OWNED","PRIVATELY_RENTED","SOCIAL_HOUSING","HOME_OWNER_ASSOCIATION"]
OWNNICE={"PRIVATELY_OWNED":"Owner-occupied","PRIVATELY_RENTED":"Privately rented",
         "SOCIAL_HOUSING":"Social housing","HOME_OWNER_ASSOCIATION":"Owner association"}

# All 16 scenarios, grouped by the tipping mechanism they perturb. Left column = the weak /
# permissive variant, right column = the strong / binding one, so each row reads as one
# mechanism dialled up. Keep in sync with SCENARIO_PAIRS in script_results.py.
MECHANISMS=[
 ("Reference",              ("baseline","Baseline"),
                            ("actor_allignment_strategy","Actor alignment")),
 ("Economic learning",      ("economic_learning_factor_low","Slow learning"),
                            ("economic_learning_factor_high","Fast learning")),
 ("Social learning",        ("social_learning_factor_low","Weak peer effect"),
                            ("social_learning_factor_high","Strong peer effect")),
 ("Rules & infrastructure", ("dh_policy_based_connection_obligation","DH connection obligation"),
                            ("grid_congestion_HP_ban","HP ban under congestion")),
 ("Coordination",           ("individual_technologies","Individual only"),
                            ("collective_technologies","Collective allowed")),
 ("Policy strategy",        ("policy_driven_dh_strategy","DH-led"),
                            ("policy_driven_sha_strategy","Insulation-led")),
 ("Individual + rules",     ("individual_tech_dh_connection_obligation","Individual + DH obligation"),
                            ("individual_tech_grid_congestion_ban","Individual + congestion ban")),
 ("Collective + rules",     ("collective_tech_dh_connection_obligation","Collective + DH obligation"),
                            ("collective_tech_grid_congestion_ban","Collective + congestion ban")),
]

def tag(scope):
    k,_,n=scope.partition(":")
    if k.lower() in ("nl","netherlands"): return "nl"
    return n.strip().lower().replace(" ","-") if k=="province" else "gemeente_"+n.strip().lower()

def load(path, ownership):
    """-> {scenario: {year: {tech: share%}}} for one run."""
    cells=defaultdict(lambda: defaultdict(float))
    for r in csv.DictReader(open(path)):
        if r["ownership"]!=ownership: continue
        cells[(r["scenario_name"],int(float(r["year"])),r["iteration"])][r["heating_system"]]+=float(r["installed_current"])
    grp=defaultdict(list)
    for (scn,yr,_),d in cells.items(): grp[(scn,yr)].append(d)
    out=defaultdict(dict)
    for (scn,yr),lst in grp.items():
        out[scn][yr]={t:float(np.mean([100*d.get(t,0)/(sum(d.values()) or 1) for d in lst])) for t in ORDER}
    return out

def combine_sets(paths, ownership, stat="mean", w=None):
    """Ensemble-average share vector across weight sets.

    Default is the MEAN, because a stacked area chart needs a genuine composition. Taking the
    median of each technology independently does not give one: the five medians need not add to
    100, and here they miss by up to ~15pp in the scenarios where the ensemble disagrees about
    which low-carbon route wins. That disagreement is a result in its own right (the gas exit is
    robust, the replacement route is not), so it should be stated in the text and shown by the
    per-technology bands -- not papered over by rescaling a median back onto the simplex.

    stat="median" is kept for comparison; it renormalises and prints the size of the gap.
    """
    runs=[load(p,ownership) for p in paths]
    if w is None: w=np.ones(len(runs))/len(runs)
    w=np.asarray(w,float)
    scen=set.intersection(*[set(r) for r in runs])
    out={}; worst=0.0
    for s in scen:
        yrs=sorted(runs[0][s]); out[s]={}
        for y in yrs:
            vals={t:[r[s][y][t] for r in runs] for t in ORDER}
            med={t:float(glue.wquantile(np.asarray(v),w,0.5)) for t,v in vals.items()}
            worst=max(worst,abs(sum(med.values())-100.0))
            if stat=="median":
                tot=sum(med.values())
                out[s][y]={t:(100.0*v/tot if tot else 0.0) for t,v in med.items()}
            else:
                out[s][y]={t:glue.wmean(np.asarray(v),w) for t,v in vals.items()}
    note="renormalised" if stat=="median" else "mean used"
    print(f"    {ownership}: {stat} ({note}); component-wise medians would miss 100% by up to {worst:.1f}pp",
          flush=True)
    return out

def spread_over_sets(paths, ownership, w=None):
    """-> {scenario: {year: {tech: percentiles}}}, GLUE-weighted across weight sets.

    `w` are the per-set likelihood weights from glue.set_weights (equal weights if None). The
    weighting acts WITHIN each scenario -- every scenario is evaluated over the same 21 sets with
    the same weights -- so cross-scenario comparison stays valid. Scenarios are never ranked
    against each other by fit; they are constructed policy alternatives, not rival hypotheses.
    """
    runs=[load(p,ownership) for p in paths]
    if w is None: w=np.ones(len(runs))/len(runs)
    w=np.asarray(w,float)
    scen=set.intersection(*[set(r) for r in runs])
    out={}
    for sc in scen:
        out[sc]={}
        for y in sorted(runs[0][sc]):
            out[sc][y]={}
            for t in ORDER:
                v=np.array([r[sc][y][t] for r in runs])
                p5,p10,p25,med,p75,p90,p95=[glue.wquantile(v,w,q)
                                            for q in (.05,.10,.25,.5,.75,.90,.95)]
                out[sc][y][t]=dict(p5=p5,p10=p10,p25=p25,med=med,p75=p75,p90=p90,p95=p95,
                                   mean=glue.wmean(v,w),lo=float(v.min()),hi=float(v.max()))
    return out

def _dark(c,f=0.62):
    """Darkened variant of a palette colour, for median lines drawn on white."""
    import matplotlib.colors as mc
    r,g,b=mc.to_rgb(c); return (r*f,g*f,b*f)


def lines(ax,data,title,band=True,outer=80):
    """Median trajectory per technology with an interquartile and an outer ribbon.

    `outer` is the width of the outer ribbon in percent: 80 -> 10-90%, 90 -> 5-95%.

    Default is 80 deliberately. With 21 weight sets and an effective sample size of ~15 after GLUE
    weighting, the outer 5% of the distribution is worth roughly one set, so a 5-95% edge is
    decided by whichever single run happens to rank second from the top. Where one set is an
    outlier -- e.g. set15 reaches 85% hybrid under the congestion ban while the next-highest sets
    sit near 71% -- rank swaps among near-tied runs move the p95 by several pp between adjacent
    years, producing kinks that are artefacts of the order statistic and not model behaviour.
    The p90 edge is stable across the same years. Use outer=90 only to reproduce older figures.
    """
    lo_q,hi_q = ((0.05,0.95) if outer==90 else (0.10,0.90))
    lo_k,hi_k = (("p5","p95") if outer==90 else ("p10","p90"))
    yrs=sorted(data)
    for t in ORDER:
        med=np.array([data[y][t]["med"] for y in yrs])
        if med.max()<0.5: continue                      # skip technologies that never appear
        if band:
            ax.fill_between(yrs,[data[y][t][lo_k] for y in yrs],[data[y][t][hi_k] for y in yrs],
                            color=COL[t],alpha=0.15,lw=0)
            ax.fill_between(yrs,[data[y][t]["p25"] for y in yrs],[data[y][t]["p75"] for y in yrs],
                            color=COL[t],alpha=0.30,lw=0)
        # bands keep the fill palette; the line is darkened so silver/grey stay legible
        ax.plot(yrs,med,color=_dark(COL[t]),lw=1.9,label=NICE[t],zorder=3)
    ax.set_xlim(min(yrs),max(yrs)); ax.set_ylim(0,100)
    ax.grid(alpha=0.3,ls="--",lw=0.6)
    ax.set_title(title,fontsize=10); ax.tick_params(labelsize=8)


def band_legend_handles(outer=80):
    """Grey proxy artists that spell out what the three shading layers mean."""
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    lo,hi = ((5,95) if outer==90 else (10,90))
    return ([Line2D([0],[0],color="#444",lw=1.9),
             Patch(facecolor="#444",alpha=0.30,lw=0),
             Patch(facecolor="#444",alpha=0.15,lw=0)],
            ["ensemble median (50th percentile)",
             "interquartile range (25th\u201375th)",
             f"outer range ({lo}th\u2013{hi}th)"])


def stack(ax,data,title):
    """Stacked composition. Needs a genuine share vector, so it takes combine_sets output
    (weighted mean by default), not the percentile dict from spread_over_sets."""
    yrs=sorted(data); bottom=np.zeros(len(yrs))
    for t in ORDER:
        v=np.array([data[y][t] for y in yrs])
        ax.fill_between(yrs,bottom,bottom+v,color=COL[t],label=NICE[t],lw=0)
        bottom+=v
    ax.set_xlim(min(yrs),max(yrs)); ax.set_ylim(0,100)
    ax.set_title(title,fontsize=10); ax.tick_params(labelsize=8)


def main():
    ap=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope",default="nl")
    ap.add_argument("--outer",type=int,default=80,choices=[80,90],
                    help="outer ribbon width %% (80 = 10-90th, default; 90 = 5-95th)")
    ap.add_argument("--glue-n",type=float,default=glue.DEFAULT_N,
                    help="GLUE likelihood exponent for the bands (0 = equal weights)")
    ap.add_argument("--stat",choices=["mean","median"],default="mean",
                    help="ensemble statistic for the stacks (default mean; see combine_sets)")
    ap.add_argument("--skip-all",action="store_true",help="skip figure C (all-scenario grid)")
    ap.add_argument("--scenarios",default="baseline,economic_learning_factor_high,social_learning_factor_high,"
                                          "dh_policy_based_connection_obligation,grid_congestion_HP_ban,"
                                          "actor_allignment_strategy")
    a=ap.parse_args(); t=tag(a.scope); base=os.path.join(ROOT,"results",t)
    figdir=os.path.join(base,"figures"); os.makedirs(figdir,exist_ok=True)
    paths=sorted(glob.glob(os.path.join(base,"calib","set*","simulation_results.csv")))
    if not paths: paths=sorted(glob.glob(os.path.join(base,"calib","*","simulation_results.csv")))
    if not paths: raise SystemExit(f"no runs under {base}/calib")
    W,winfo=glue.set_weights(ROOT,t,a.glue_n,paths)
    print(f"  {len(paths)} weight sets -> ensemble {a.stat} | {winfo['note']}", flush=True)

    # --- A. key scenarios -------------------------------------------------------------------
    tot=combine_sets(paths,"TOTAL",a.stat,W)
    scens=[s for s in a.scenarios.split(",") if s in tot]
    n=len(scens); ncol=3; nrow=int(np.ceil(n/ncol))
    fig,axes=plt.subplots(nrow,ncol,figsize=(4.2*ncol,3.1*nrow),sharex=True,sharey=True)
    axes=np.atleast_1d(axes).ravel()
    for i,s in enumerate(scens): stack(axes[i],tot[s],TITLE.get(s,s.replace("_"," ")))
    for j in range(n,len(axes)): axes[j].axis("off")
    for i in range(0,len(axes),ncol):
        if i<n: axes[i].set_ylabel("% of dwellings",fontsize=9)
    h,l=axes[0].get_legend_handles_labels()
    fig.legend(h,l,loc="lower center",ncol=5,fontsize=9,frameon=False,bbox_to_anchor=(0.5,-0.02))
    fig.suptitle(f"Heating-technology mix 2024–2050 by scenario ({a.stat} over {len(paths)} weight sets; {winfo['note']})",fontsize=11.5)
    fig.tight_layout(rect=[0,0.03,1,0.96])
    fa=os.path.join(figdir,"fig_stacked_scenarios.png"); fig.savefig(fa,dpi=150,bbox_inches="tight"); plt.close(fig)

    # --- B. baseline by ownership -----------------------------------------------------------
    fig,axes=plt.subplots(1,len(OWN),figsize=(3.6*len(OWN),3.3),sharey=True)
    for i,o in enumerate(OWN):
        d=combine_sets(paths,o,a.stat,W)
        stack(axes[i],d["baseline"],OWNNICE[o])
    axes[0].set_ylabel("% of dwellings",fontsize=9)
    h,l=axes[0].get_legend_handles_labels()
    fig.legend(h,l,loc="lower center",ncol=5,fontsize=9,frameon=False,bbox_to_anchor=(0.5,-0.10))
    fig.suptitle(f"Baseline heating-technology mix 2024–2050 by ownership type ({a.stat}; {winfo['note']})",fontsize=11.5)
    fig.tight_layout(rect=[0,0.02,1,0.93])
    fb=os.path.join(figdir,"fig_stacked_ownership.png"); fig.savefig(fb,dpi=150,bbox_inches="tight"); plt.close(fig)
    written=[fa,fb]

    # --- C. every scenario, grouped by mechanism ---------------------------------------------
    if not a.skip_all:
        rows=[m for m in MECHANISMS if m[1][0] in tot and m[2][0] in tot]
        missing=sorted({k for m in MECHANISMS for k,_ in m[1:]} - set(tot))
        if missing: print(f"  note: {len(missing)} scenario(s) absent from the runs: {', '.join(missing)}")
        nrow=len(rows)
        fig,axes=plt.subplots(nrow,2,figsize=(8.6,1.85*nrow),sharex=True,sharey=True)
        axes=np.atleast_2d(axes)
        for r,(mech,left,right) in enumerate(rows):
            for c,(key,lab) in enumerate((left,right)):
                stack(axes[r,c],tot[key],lab)
                axes[r,c].set_title(lab,fontsize=8.5,pad=4)
                axes[r,c].tick_params(labelsize=7)
            # mechanism name as a bold row label in the left margin
            axes[r,0].set_ylabel(mech,fontsize=8.5,fontweight="bold",labelpad=8)
        for c in (0,1): axes[-1,c].set_xlabel("Year",fontsize=8)
        h,l=axes[0,0].get_legend_handles_labels()
        fig.legend(h,l,loc="lower center",ncol=5,fontsize=8.5,frameon=False,bbox_to_anchor=(0.5,-0.012))
        fig.suptitle("Heating-technology mix 2024\u20132050 by scenario, grouped by mechanism"
                     f"\n({a.stat} over the {len(paths)} retained weight sets; {winfo['note']})",fontsize=11)
        fig.supylabel("% of dwellings",fontsize=9,x=0.005)
        fig.tight_layout(rect=[0.01,0.015,1,0.965])
        fc=os.path.join(figdir,"fig_stacked_all_scenarios.png")
        fig.savefig(fc,dpi=150,bbox_inches="tight"); plt.close(fig)
        written.append(fc)

    # --- D. line chart with ensemble bands, + the underlying numbers --------------------------
    sp=spread_over_sets(paths,"TOTAL",W)
    scens_d=[x for x in a.scenarios.split(",") if x in sp]
    n=len(scens_d); ncol=3; nrow=int(np.ceil(n/ncol))
    fig,axes=plt.subplots(nrow,ncol,figsize=(4.4*ncol,3.2*nrow),sharex=True,sharey=True)
    axes=np.atleast_1d(axes).ravel()
    for i,sc in enumerate(scens_d): lines(axes[i],sp[sc],TITLE.get(sc,sc.replace("_"," ")),outer=a.outer)
    for j in range(n,len(axes)): axes[j].axis("off")
    for i in range(0,len(axes),ncol):
        if i<n: axes[i].set_ylabel("% of dwellings",fontsize=9)
    for i in range(max(0,n-ncol),n): axes[i].set_xlabel("Year",fontsize=9)
    h,l=axes[0].get_legend_handles_labels()
    fig.legend(h,l,loc="lower center",ncol=5,fontsize=9,frameon=False,bbox_to_anchor=(0.5,-0.015))
    bh,bl=band_legend_handles(a.outer)
    fig.legend(bh,bl,loc="lower center",ncol=3,fontsize=8.5,frameon=False,
               bbox_to_anchor=(0.5,-0.075),title="shading",title_fontsize=8.5)
    _lo,_hi = ((5,95) if a.outer==90 else (10,90))
    fig.suptitle(f"Heating-technology mix 2024\u20132050 by scenario\n"
                 f"median, interquartile (25\u201375th) and {_lo}\u2013{_hi}th percentile range "
                 f"over {len(paths)} weight sets ({winfo['note']})",fontsize=11)
    fig.tight_layout(rect=[0,0.05,1,0.94])
    fd=os.path.join(figdir,"fig_lines_scenarios.png"); fig.savefig(fd,dpi=150,bbox_inches="tight")
    plt.close(fig); written.append(fd)

    tab=os.path.join(figdir,"ensemble_spread.csv")
    try:                                    # Excel holds a lock on Windows; do not lose the figures
        fh=open(tab,"w",newline="")
    except PermissionError:
        tab=tab.replace(".csv","_new.csv")
        print(f"    ensemble_spread.csv is locked (open in Excel?) -> writing {os.path.basename(tab)}")
        fh=open(tab,"w",newline="")
    with fh:
        w=csv.writer(fh); w.writerow(["scenario","year","technology","min","p5","p10","p25","median",
                                      "mean","p75","p90","p95","max","iqr","p90_minus_p10",
                                      "p95_minus_p5"])
        for sc in sorted(sp):
            for y in (2030,2040,2050):
                if y not in sp[sc]: continue
                for t in ORDER:
                    d=sp[sc][y][t]
                    w.writerow([sc,y,NICE[t]]+[f"{d[k]:.2f}" for k in
                               ("lo","p5","p10","p25","med","mean","p75","p90","p95","hi")]
                               +[f"{d['p75']-d['p25']:.2f}",f"{d['p90']-d['p10']:.2f}",
                                 f"{d['p95']-d['p5']:.2f}"])
    written.append(tab)

    for w in written: print(f"  wrote {w}")

if __name__=="__main__": main()
