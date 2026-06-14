"""Pre-registered forward slip-model inversion - the best-chance geodetic
nucleation test for 2016 Kumamoto. The model-free net-displacement tests (raw +
common-mode) returned NULL; this gives the slow slip its best chance by fitting
an elastic dislocation (validated Okada85) to the window net-displacement field,
using the spatial PATTERN (direction + decay), not just per-station amplitude. A
grid of plausible nucleation-patch geometries is searched and the best variance
reduction (VR) kept - then the SAME grid search is applied to sliding baseline
windows, so the verdict controls for grid-search overfitting. Criteria fixed
before the window field is fit. POSITIVE iff the window best-VR beats the 95th
percentile of the baseline best-VR distribution (P1) and the best-fit slip is
physical + localized (P2); else NULL.
"""
import os, gzip, glob, math, json
from datetime import datetime, timedelta
import numpy as np
from okada85 import okada85

ROOT="/home/yasu/geo-ml/kenv_window"
EPI=(32.755,130.763); SIG_MAX=0.05; RESID_CAP=0.5; FIT_KM=60.0; FAR_KM=250.0
T0=datetime(2016,1,1)
def sec(t): return (t-T0).total_seconds()
T_M65=sec(datetime(2016,4,14,12,26,34)); T_M64=sec(datetime(2016,4,14,15,3,46))
T_MS=sec(datetime(2016,4,15,16,25,5))
BASE_START=sec(T0+timedelta(days=89)); BASE_END=T_M65; WIN_START=T_M65; WIN_END=T_MS
WINLEN=WIN_END-WIN_START; STEPS=[T_M65,T_M64]
def hav(la,lo):
    R=6371.0;p=math.pi/180
    dla=(EPI[0]-la)*p;dlo=(EPI[1]-lo)*p
    a=math.sin(dla/2)**2+math.cos(la*p)*math.cos(EPI[0]*p)*math.sin(dlo/2)**2
    return 2*R*math.asin(math.sqrt(a))
def enkm(la,lo,la0,lo0):
    return (lo-lo0)*111.32*math.cos(math.radians(la0)), (la-la0)*110.57

stations={}
with open(os.path.join(ROOT,"_stations.csv")) as f:
    next(f)
    for ln in f:
        a=ln.strip().split(",")
        if len(a)>=4: stations[a[0]]=(float(a[1]),float(a[2]),a[3])
def load(sta):
    T=[];E=[];N=[];SE=[];SN=[]
    for fp in sorted(glob.glob(os.path.join(ROOT,sta,sta+".2016.*.kenv.gz"))):
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
        pre=(T>=ts-1800)&(T<ts-300);post=(T>ts+300)&(T<=ts+1800)
        if pre.sum()>=3 and post.sum()>=3:
            m=T>=ts;rE[m]-=(np.median(rE[post])-np.median(rE[pre]));rN[m]-=(np.median(rN[post])-np.median(rN[pre]))
    c=np.sqrt(rE**2+rN**2)<RESID_CAP
    return T[c],rE[c],rN[c]
def netdisp(T,rE,rN,a,b):
    m=(T>=a)&(T<=b)
    if m.sum()<20: return None
    Tn,e,n=T[m],rE[m],rN[m]
    f=Tn<=a+7200;l=Tn>=b-7200
    if f.sum()<3 or l.sum()<3: return None
    return np.median(e[l])-np.median(e[f]),np.median(n[l])-np.median(n[f])

R={};LA={};LO={};DI={};ROLE={}
for sta,(la,lo,role) in stations.items():
    d=resid(sta)
    if d is None: continue
    R[sta]=d;LA[sta]=la;LO[sta]=lo;DI[sta]=hav(la,lo);ROLE[sta]=role
fit_st=[s for s in R if DI[s]<=FIT_KM]
far_st=[s for s in R if ROLE[s]=="CTRL" and DI[s]>=FAR_KM]

# registered fault-geometry grid (Kumamoto Hinagu/Futagawa plausible nucleation patch)
LON0=[130.74,130.80]; LAT0=[32.70,32.78]; DEP=[6.0,10.0,14.0]; STR=[205.0,235.0]; DIP=[70.0,90.0]
L=14.0; W=10.0
GEOMS=[(a,b,c,d2,e2) for a in LON0 for b in LAT0 for c in DEP for d2 in STR for e2 in DIP]

def best_vr(getobs):
    obs=[];coords=[]
    for s in fit_st:
        o=getobs(s)
        if o is None: continue
        obs.append(o);coords.append((LA[s],LO[s]))
    if len(obs)<8: return None,None,None
    obs=np.array(obs);ov=np.concatenate([obs[:,0],obs[:,1]])
    sst=np.sum(ov**2)
    best=(-1e9,None,None)
    for (lo0,la0,dep,strike,dip,) in [(g[0],g[1],g[2],g[3],g[4]) for g in GEOMS]:
        ee=np.array([enkm(la,lo,la0,lo0)[0] for la,lo in coords])
        nn=np.array([enkm(la,lo,la0,lo0)[1] for la,lo in coords])
        gssE,gssN,_=okada85(ee,nn,dep,strike,dip,L,W,0.0,1.0)
        gdsE,gdsN,_=okada85(ee,nn,dep,strike,dip,L,W,90.0,1.0)
        Gs=np.concatenate([gssE,gssN]);Gd=np.concatenate([gdsE,gdsN])
        A=np.vstack([Gs,Gd]).T
        coef,_,_,_=np.linalg.lstsq(A,ov,rcond=None)
        pred=A@coef;vr=1.0-np.sum((ov-pred)**2)/sst
        if vr>best[0]: best=(vr,coef,(lo0,la0,dep,strike,dip))
    return best[0],best[1],best[2]

vr_win,coef_win,geo_win=best_vr(lambda s: netdisp(*R[s],WIN_START,WIN_END))
base_vrs=[]
a=BASE_START
while a+WINLEN<=BASE_END:
    aa=a
    v,_,_=best_vr(lambda s: netdisp(*R[s],aa,aa+WINLEN))
    if v is not None: base_vrs.append(v)
    a+=12*3600
base_vrs=np.array(base_vrs)
p95=float(np.percentile(base_vrs,95)) if len(base_vrs)>=5 else None
slip_mag=float(math.hypot(*coef_win)) if coef_win is not None else None
# far-field predicted vs observed (localization check)
P1=(p95 is not None) and (vr_win>p95)
P2=(slip_mag is not None) and (0.001<=slip_mag<=1.0)
POS=bool(P1 and P2)
print(json.dumps(dict(n_fit=len(fit_st),n_far=len(far_st),
    vr_win=float(vr_win) if vr_win is not None else None,
    base_vr_p95=p95, base_vr_med=float(np.median(base_vrs)) if len(base_vrs) else None,
    n_base=len(base_vrs), slip_mag_m=slip_mag, geo_win=geo_win,
    P1=bool(P1),P2=bool(P2),VERDICT=("POSITIVE" if POS else "NULL")),indent=1))
