"""Kumamoto detection-threshold characterization (synthetic injection) - the
symmetric companion to the Iquique injection. Injects a synthetic strike-slip
transient (Futagawa-fault Okada slip S x a 0->1 window ramp, the Kumamoto
mechanism) into the real residuals and finds the minimum slip S* at which the
fixed-geometry trajectory detector's variance reduction crosses the baseline
noise floor. S* -> Mw = the smallest slow slip the reproducible pipeline could
have detected at Kumamoto's onshore station geometry. Compare to the small
aseismic moment Kato et al. (2016) inferred drove the foreshocks.
"""
import os, gzip, glob, math, json
from datetime import datetime, timedelta
import numpy as np
from okada85 import okada85
ROOT="/home/yasu/geo-ml/kenv_window"
EPI=(32.755,130.763); SIG_MAX=0.05; RESID_CAP=0.5; FIT_KM=60.0
GEO=(130.763,32.755,10.0,235.0,65.0); L=14.0; W=10.0; MU=30e9   # Futagawa strike-slip
T0=datetime(2016,1,1)
def sec(t): return (t-T0).total_seconds()
T_M65=sec(datetime(2016,4,14,12,26,34)); T_M64=sec(datetime(2016,4,14,15,3,46))
WIN_START=T_M65; WIN_END=sec(datetime(2016,4,15,16,25,5))
BASE_START=sec(T0+timedelta(days=89)); BASE_END=T_M65; WINLEN=WIN_END-WIN_START; STEPS=[T_M65,T_M64]
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
    if bm.sum()<300: return None
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
ee=np.array([enkm(LA[s],LO[s],la0,lo0)[0] for s in fit_st]); nn=np.array([enkm(LA[s],LO[s],la0,lo0)[1] for s in fit_st])
gssE,gssN,_=okada85(ee,nn,dep,strike,dip,L,W,0.0,1.0); gdsE,gdsN,_=okada85(ee,nn,dep,strike,dip,L,W,90.0,1.0)
Gmat=np.vstack([np.concatenate([gssE,gssN]),np.concatenate([gdsE,gdsN])]).T
Ginj=np.concatenate([gssE,gssN])   # inject in strike-slip direction (Kumamoto mechanism)
def vr_of(ov):
    sst=np.sum(ov**2)
    if sst<=0: return 0.0
    coef,_,_,_=np.linalg.lstsq(Gmat,ov,rcond=None)
    return 1.0-np.sum((ov-Gmat@coef)**2)/sst
def obs_vec(a,b):
    o=[ramp_disp(*R[s],a,b) for s in fit_st]
    if any(x is None for x in o): return None
    o=np.array(o); return np.concatenate([o[:,0],o[:,1]])
basev=[]; bwins=[]; a=BASE_START
while a+WINLEN<=BASE_END:
    ov=obs_vec(a,a+WINLEN)
    if ov is not None: basev.append(vr_of(ov)); bwins.append(ov)
    a+=21600.0
p95=float(np.percentile(basev,95))
def thresh(ovn):
    lo_,hi=0.0,5.0
    if vr_of(ovn+hi*Ginj)<=p95: return None
    for _ in range(40):
        mid=(lo_+hi)/2
        if vr_of(ovn+mid*Ginj)>p95: hi=mid
        else: lo_=mid
    return hi
win=obs_vec(WIN_START,WIN_END)
S_win=thresh(win)
S_b=[thresh(b) for b in bwins]; S_b=[s for s in S_b if s is not None]
def mw(S): M0=MU*(L*1e3)*(W*1e3)*S; return (2.0/3.0)*(math.log10(M0)-9.1)
out=dict(case="kumamoto",n_fit=len(fit_st),baseline_vr_p95=p95,n_base=len(basev),
    S_thresh_realwindow_m=S_win,Mw_thresh_realwindow=(mw(S_win) if S_win else None),
    S_thresh_baseline_median_m=(float(np.median(S_b)) if S_b else None),
    Mw_thresh_baseline_median=(mw(float(np.median(S_b))) if S_b else None),
    patch="14x10km Futagawa strike-slip", note="S*=min detectable uniform strike-slip; Mw via M0=mu*A*S mu30GPa")
print(json.dumps(out,indent=1))
