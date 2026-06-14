"""Iquique positive control - FIXED-geometry trajectory test. The grid-searched
variants overfit the baseline control (baseline VR 95th pct ~0.41 from 108
geometries x 2 free params), which would mask a real signal. Since the Iquique
megathrust geometry is KNOWN, this fixes the fault (no grid search) and uses the
high-sensitivity full-window linear trajectory matched filter, controlling
window VR against the SAME fixed-geometry fit on baseline windows. This is the
cleanest positive-control test: if the documented slow slip is recoverable at
all from open 5-min kinematic, removing the grid-search overfitting should expose
it. Fixed geometry: offshore megathrust centroid (-71.0, -19.7), depth 25 km,
strike 5, dip 18, L=50 W=40 km (2014 Iquique source).
"""
import os, gzip, glob, math, json
from datetime import datetime, timedelta
import numpy as np
from okada85 import okada85
ROOT="/home/yasu/geo-ml/iq_kenv"
EPI=(-19.61,-70.77); SIG_MAX=0.05; RESID_CAP=0.5; FIT_KM=250.0
GEO=(-71.0,-19.7,25.0,5.0,18.0); L=50.0; W=40.0   # lon0,lat0,depth,strike,dip
T0=datetime(2014,1,1)
def sec(t): return (t-T0).total_seconds()
FS=[sec(datetime(2014,3,16,21,16,29)),sec(datetime(2014,3,17,5,11,34)),sec(datetime(2014,3,22,12,59,59)),sec(datetime(2014,3,23,18,20,1))]
WIN_START=sec(datetime(2014,3,16,21,16,29)); WIN_END=sec(datetime(2014,4,1,23,46,47))
BASE_START=sec(T0+timedelta(days=39)); BASE_END=WIN_START; WINLEN=WIN_END-WIN_START; STEPS=FS
def hav(la,lo):
    R=6371.0;p=math.pi/180
    dla=(EPI[0]-la)*p;dlo=(EPI[1]-lo)*p
    a=math.sin(dla/2)**2+math.cos(la*p)*math.cos(EPI[0]*p)*math.sin(dlo/2)**2
    return 2*R*math.asin(math.sqrt(a))
def enkm(la,lo,la0,lo0): return (lo-lo0)*111.32*math.cos(math.radians(la0)),(la-la0)*110.57
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
def ramp_disp(T,rE,rN,a,b):
    m=(T>=a)&(T<=b)
    if m.sum()<60: return None
    tt=(T[m]-a)/(b-a); A=np.vstack([tt,np.ones(len(tt))]).T
    ce,_,_,_=np.linalg.lstsq(A,rE[m],rcond=None); cn,_,_,_=np.linalg.lstsq(A,rN[m],rcond=None)
    return float(ce[0]),float(cn[0])
R={};LA={};LO={};DI={}
for sta,(la,lo,role) in stations.items():
    d=resid(sta)
    if d is None: continue
    R[sta]=d;LA[sta]=la;LO[sta]=lo;DI[sta]=hav(la,lo)
fit_st=[s for s in R if DI[s]<=FIT_KM]
lo0,la0,dep,strike,dip=GEO
def vr_fixed(getobs):
    obs=[];coords=[]
    for s in fit_st:
        o=getobs(s)
        if o is None: continue
        obs.append(o);coords.append((LA[s],LO[s]))
    if len(obs)<6: return None,None
    obs=np.array(obs);ov=np.concatenate([obs[:,0],obs[:,1]]);sst=np.sum(ov**2)
    ee=np.array([enkm(la,lo,la0,lo0)[0] for la,lo in coords]); nn=np.array([enkm(la,lo,la0,lo0)[1] for la,lo in coords])
    gssE,gssN,_=okada85(ee,nn,dep,strike,dip,L,W,0.0,1.0); gdsE,gdsN,_=okada85(ee,nn,dep,strike,dip,L,W,90.0,1.0)
    A=np.vstack([np.concatenate([gssE,gssN]),np.concatenate([gdsE,gdsN])]).T
    coef,_,_,_=np.linalg.lstsq(A,ov,rcond=None)
    return 1.0-np.sum((ov-A@coef)**2)/sst, float(math.hypot(*coef))
vr_win,slip=vr_fixed(lambda s: ramp_disp(*R[s],WIN_START,WIN_END))
base=[];a=BASE_START
while a+WINLEN<=BASE_END:
    aa=a; v,_=vr_fixed(lambda s: ramp_disp(*R[s],aa,aa+WINLEN))
    if v is not None: base.append(v)
    a+=43200.0
base=np.array(base)
p95=float(np.percentile(base,95)) if len(base)>=5 else None
P1=(p95 is not None) and (vr_win is not None) and (vr_win>p95)
print(json.dumps(dict(stat="fixed_geom_ramp",geo=GEO,n_fit=len(fit_st),vr_win=float(vr_win) if vr_win is not None else None,
    slip_m=slip,base_vr_p95=p95,base_vr_med=float(np.median(base)) if len(base) else None,n_base=len(base),
    P1=bool(P1),VERDICT=("POSITIVE" if P1 else "NULL")),indent=1))
