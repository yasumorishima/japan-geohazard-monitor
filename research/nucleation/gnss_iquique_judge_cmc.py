"""Iquique 2014 positive control - COMMON-MODE-FILTERED variant. The raw judge
showed a near-field coastal transient (G1+G2 pass) but the far-field controls
exceed it (G3 fail) = a regional common-mode floor. This subtracts the per-epoch
median residual over a far reference ring (250-3000 km) to isolate the localized
offshore deformation, then re-applies G1-G3. If Ruiz et al. (2014)'s documented
slow slip is recoverable at NGL 5-min precision it should emerge here.
"""
import os, gzip, glob, math, json
from datetime import datetime, timedelta
import numpy as np
ROOT="/home/yasu/geo-ml/iq_kenv"
EPI=(-19.61,-70.77); NEAR_KM=150.0; FAR_KM=700.0; SIG_MAX=0.05; RESID_CAP=0.5; EDGE=86400.0
CMC_LO=250.0; CMC_HI=3000.0
T0=datetime(2014,1,1)
def sec(t): return (t-T0).total_seconds()
FS=[sec(datetime(2014,3,16,21,16,29)),sec(datetime(2014,3,17,5,11,34)),sec(datetime(2014,3,22,12,59,59)),sec(datetime(2014,3,23,18,20,1))]
WIN_START=sec(datetime(2014,3,16,21,16,29)); WIN_END=sec(datetime(2014,4,1,23,46,47))
BASE_START=sec(T0+timedelta(days=39)); BASE_END=WIN_START
WINLEN=WIN_END-WIN_START; STEPS=FS
def hav(la,lo):
    R=6371.0;p=math.pi/180
    dla=(EPI[0]-la)*p;dlo=(EPI[1]-lo)*p
    a=math.sin(dla/2)**2+math.cos(la*p)*math.cos(EPI[0]*p)*math.sin(dlo/2)**2
    return 2*R*math.asin(math.sqrt(a))
stations={}
with open(os.path.join(ROOT,"_stations.csv")) as f:
    next(f)
    for ln in f:
        a=ln.strip().split(",")
        if len(a)>=4: stations[a[0]]=(float(a[1]),float(a[2]),a[3])
def load(sta):
    T=[];E=[];N=[];SE=[];SN=[]
    for fp in sorted(glob.glob(os.path.join(ROOT,sta,sta+".2014.*.kenv.gz"))):
        try:
            for ln in gzip.open(fp,"rt"):
                if ln[0]=="s": continue
                p=ln.split()
                if len(p)<16: continue
                T.append(sec(T0+timedelta(days=int(p[6])-1,seconds=int(p[7]))))
                E.append(float(p[8]));N.append(float(p[9]));SE.append(float(p[14]));SN.append(float(p[15]))
        except Exception: pass
    T=np.array(T);E=np.array(E);N=np.array(N);SE=np.array(SE);SN=np.array(SN)
    o=np.argsort(T);return T[o],E[o],N[o],SE[o],SN[o]
def resid(sta):
    T,E,N,SE,SN=load(sta)
    g=(SE<SIG_MAX)&(SN<SIG_MAX);T,E,N=T[g],E[g],N[g]
    bm=(T>=BASE_START)&(T<BASE_END)
    if bm.sum()<500: return None
    def fit(y):
        A=np.vstack([T[bm]-BASE_START,np.ones(bm.sum())]).T
        c,_,_,_=np.linalg.lstsq(A,y[bm],rcond=None);return c
    cE=fit(E);cN=fit(N)
    rE=E-(cE[0]*(T-BASE_START)+cE[1]);rN=N-(cN[0]*(T-BASE_START)+cN[1])
    for ts in STEPS:
        pre=(T>=ts-7200)&(T<ts-600);post=(T>ts+600)&(T<=ts+7200)
        if pre.sum()>=3 and post.sum()>=3:
            m=T>=ts;rE[m]-=(np.median(rE[post])-np.median(rE[pre]));rN[m]-=(np.median(rN[post])-np.median(rN[pre]))
    c=np.sqrt(rE**2+rN**2)<RESID_CAP
    return T[c],rE[c],rN[c]
R={};D={};ROLE={}
for sta,(la,lo,role) in stations.items():
    d=resid(sta)
    if d is None: continue
    R[sta]=d;D[sta]=hav(la,lo);ROLE[sta]=role
allmin=min(min(R[s][0]) for s in R); allmax=max(max(R[s][0]) for s in R)
g0=math.floor(BASE_START/300.0)*300.0; g1=math.ceil((WIN_END+300)/300.0)*300.0
grid=np.arange(g0,g1+1,300.0)
def to_grid(T,r):
    arr=np.full(len(grid),np.nan); idx=np.round((T-g0)/300.0).astype(int)
    ok=(idx>=0)&(idx<len(grid)); arr[idx[ok]]=r[ok]; return arr
GE={s:to_grid(R[s][0],R[s][1]) for s in R}; GN={s:to_grid(R[s][0],R[s][2]) for s in R}
ref=[s for s in R if CMC_LO<D[s]<CMC_HI]
cmE=np.nanmedian(np.vstack([GE[s] for s in ref]),axis=0); cmN=np.nanmedian(np.vstack([GN[s] for s in ref]),axis=0)
for s in R: GE[s]=GE[s]-cmE; GN[s]=GN[s]-cmN
def gnet(ge,gn,a,b):
    m=(grid>=a)&(grid<=b)&~np.isnan(ge)&~np.isnan(gn)
    if m.sum()<40: return None
    t=grid[m];e=ge[m];n=gn[m]; f=t<=a+EDGE;l=t>=b-EDGE
    if f.sum()<5 or l.sum()<5: return None
    return np.median(e[l])-np.median(e[f]),np.median(n[l])-np.median(n[f])
res={}
for s in R:
    ge,gn=GE[s],GN[s]
    nd=gnet(ge,gn,WIN_START,WIN_END)
    if nd is None: continue
    Aw=math.hypot(*nd)
    base=[];a=BASE_START
    while a+WINLEN<=BASE_END:
        nb=gnet(ge,gn,a,a+WINLEN)
        if nb is not None: base.append(math.hypot(*nb))
        a+=86400.0
    if len(base)<5: continue
    p95=float(np.percentile(base,95))
    res[s]=dict(dist=D[s],role=ROLE[s],Aw=Aw,p95=p95,exceed=bool(Aw>p95))
near=[r for r in res.values() if r["dist"]<=NEAR_KM]
far=[r for r in res.values() if r["role"]=="CTRL" and r["dist"]>=FAR_KM]
alls=[r for r in res.values() if r["dist"]<=400]
def tsslope(xs,ys):
    sl=[(ys[j]-ys[i])/(xs[j]-xs[i]) for i in range(len(xs)) for j in range(i+1,len(xs)) if xs[j]!=xs[i]]
    return float(np.median(sl)) if sl else 0.0
g1f=float(np.mean([r["exceed"] for r in near])) if near else 0.0; G1=g1f>=0.5
g2=tsslope([r["dist"] for r in alls],[r["Aw"] for r in alls]); G2=g2<0
g3f=float(np.mean([r["exceed"] for r in far])) if far else 1.0; G3null=(len(far)>0) and (g3f<=0.1)
POS=bool(G1 and G3null and G2)
print(json.dumps(dict(cmc_ref=len(ref),n_near=len(near),n_far=len(far),G1=bool(G1),g1_frac=g1f,
    G2=bool(G2),g2_slope=g2,G3null=bool(G3null),g3_frac=g3f,VERDICT=("POSITIVE" if POS else "NULL"),
    near_Aw_med_mm=float(np.median([r["Aw"] for r in near]))*1000 if near else None,
    near_p95_med_mm=float(np.median([r["p95"] for r in near]))*1000 if near else None,
    far_Aw_med_mm=float(np.median([r["Aw"] for r in far]))*1000 if far else None),indent=1))
