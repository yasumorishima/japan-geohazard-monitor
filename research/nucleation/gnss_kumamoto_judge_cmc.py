"""Pre-registered prospective geodetic nucleation test for 2016 Kumamoto -
COMMON-MODE-FILTERED variant. Test 1 (gnss_kumamoto_judge.py) returned NULL but
its far-field control showed the apparent near-field window transient is not
localized: ~13 mm appears even at 600-900 km, i.e. a network common-mode
(frame/orbit) floor. This variant applies the standard remedy: at each 5-min
epoch subtract the median secular-detrended residual over a regional reference
ring (80-1000 km from the epicentre, which lacks the ~10 km-scale nucleation
transient), isolating localized deformation, then re-applies the SAME G1-G4
criteria. Thresholds still derive from the quiet baseline only; criteria fixed
before the common-mode-filtered window residual is examined.
"""
import os, gzip, math, glob, json
from datetime import datetime, timedelta
import numpy as np

ROOT="/home/yasu/geo-ml/kenv_window"
EPI=(32.755,130.763)
NEAR_KM=30.0; FAR_KM=250.0; SIG_MAX=0.05; RESID_CAP=0.5
CMC_LO=80.0; CMC_HI=1000.0
T0=datetime(2016,1,1)
def sec(t): return (t-T0).total_seconds()
T_M65=sec(datetime(2016,4,14,12,26,34)); T_M64=sec(datetime(2016,4,14,15,3,46))
T_MS =sec(datetime(2016,4,15,16,25,5))
BASE_START=sec(T0+timedelta(days=89)); BASE_END=T_M65; WIN_START=T_M65; WIN_END=T_MS
WINLEN=WIN_END-WIN_START; STEPS=[T_M65,T_M64]

def hav(la,lo):
    R=6371.0; p=math.pi/180
    dla=(EPI[0]-la)*p; dlo=(EPI[1]-lo)*p
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
    o=np.argsort(T); return T[o],E[o],N[o],SE[o],SN[o]

def resid(sta):
    T,E,N,SE,SN=load(sta)
    g=(SE<SIG_MAX)&(SN<SIG_MAX); T,E,N=T[g],E[g],N[g]
    bm=(T>=BASE_START)&(T<BASE_END)
    if bm.sum()<500: return None
    def fit(y):
        A=np.vstack([T[bm]-BASE_START,np.ones(bm.sum())]).T
        c,_,_,_=np.linalg.lstsq(A,y[bm],rcond=None); return c
    cE=fit(E);cN=fit(N)
    rE=E-(cE[0]*(T-BASE_START)+cE[1]); rN=N-(cN[0]*(T-BASE_START)+cN[1])
    for ts in STEPS:
        pre=(T>=ts-1800)&(T<ts-300); post=(T>ts+300)&(T<=ts+1800)
        if pre.sum()>=3 and post.sum()>=3:
            m=T>=ts
            rE[m]-=(np.median(rE[post])-np.median(rE[pre])); rN[m]-=(np.median(rN[post])-np.median(rN[pre]))
    c=np.sqrt(rE**2+rN**2)<RESID_CAP
    return T[c],rE[c],rN[c]

# build residuals + grid
R={}; D={}; ROLE={}
for sta,(la,lo,role) in stations.items():
    d=resid(sta)
    if d is None: continue
    R[sta]=d; D[sta]=hav(la,lo); ROLE[sta]=role
g0=math.floor(BASE_START/300.0)*300.0; g1=math.ceil((WIN_END+300)/300.0)*300.0
grid=np.arange(g0,g1+1,300.0)
def to_grid(T,r):
    arr=np.full(len(grid),np.nan)
    idx=np.round((T-g0)/300.0).astype(int)
    ok=(idx>=0)&(idx<len(grid)); arr[idx[ok]]=r[ok]; return arr
GE={s:to_grid(R[s][0],R[s][1]) for s in R}; GN={s:to_grid(R[s][0],R[s][2]) for s in R}
ref=[s for s in R if CMC_LO<D[s]<CMC_HI]
cmE=np.nanmedian(np.vstack([GE[s] for s in ref]),axis=0)
cmN=np.nanmedian(np.vstack([GN[s] for s in ref]),axis=0)
for s in R: GE[s]=GE[s]-cmE; GN[s]=GN[s]-cmN

def gnet(ge,gn,a,b):
    m=(grid>=a)&(grid<=b)&~np.isnan(ge)&~np.isnan(gn)
    if m.sum()<20: return None
    t=grid[m]; e=ge[m]; n=gn[m]
    f=t<=a+7200; l=t>=b-7200
    if f.sum()<3 or l.sum()<3: return None
    return np.median(e[l])-np.median(e[f]), np.median(n[l])-np.median(n[f])
def gspeed(ge,gn,a,b,uE,uN):
    m=(grid>=a)&(grid<=b)&~np.isnan(ge)&~np.isnan(gn)
    if m.sum()<10: return None
    t=grid[m]; s=ge[m]*uE+gn[m]*uN
    A=np.vstack([(t-a)/3600.0,np.ones(len(t))]).T
    c,_,_,_=np.linalg.lstsq(A,s,rcond=None); return float(c[0])

res={}
for s in R:
    ge,gn=GE[s],GN[s]
    nd=gnet(ge,gn,WIN_START,WIN_END)
    if nd is None: continue
    dE,dN=nd; Aw=math.hypot(dE,dN); mag=max(Aw,1e-9); uE,uN=dE/mag,dN/mag
    base=[]; a=BASE_START
    while a+WINLEN<=BASE_END:
        nb=gnet(ge,gn,a,a+WINLEN)
        if nb is not None: base.append(math.hypot(*nb))
        a+=6*3600
    if len(base)<5: continue
    p95=float(np.percentile(base,95))
    sf=gspeed(ge,gn,WIN_END-6*3600,WIN_END,uE,uN); si=gspeed(ge,gn,WIN_START,WIN_END-6*3600,uE,uN)
    res[s]=dict(dist=D[s],role=ROLE[s],Aw=Aw,p95=p95,exceed=bool(Aw>p95),sf=sf,si=si)

near=[r for r in res.values() if r["dist"]<=NEAR_KM]
far=[r for r in res.values() if r["role"]=="CTRL" and r["dist"]>=FAR_KM]
alls=[r for r in res.values() if r["dist"]<=200]
def tsslope(xs,ys):
    sl=[(ys[j]-ys[i])/(xs[j]-xs[i]) for i in range(len(xs)) for j in range(i+1,len(xs)) if xs[j]!=xs[i]]
    return float(np.median(sl)) if sl else 0.0
g1_frac=float(np.mean([r["exceed"] for r in near])) if near else 0.0; G1=g1_frac>=0.5
g2_slope=tsslope([r["dist"] for r in alls],[r["Aw"] for r in alls]); G2=g2_slope<0
g3_frac=float(np.mean([r["exceed"] for r in far])) if far else 1.0; G3null=(len(far)>0) and (g3_frac<=0.1)
g4=[abs(r["sf"])>abs(r["si"]) for r in near if r["sf"] is not None and r["si"] is not None]
g4_frac=float(np.mean(g4)) if g4 else 0.0; G4=g4_frac>0.5
POS=bool(G1 and G3null and (G2 or G4))
out=dict(cmc_ref=len(ref),n_near=len(near),n_far=len(far),n_all=len(alls),
    G1=bool(G1),g1_frac=g1_frac,G2=bool(G2),g2_slope=g2_slope,G3null=bool(G3null),g3_frac=g3_frac,
    G4=bool(G4),g4_frac=g4_frac,VERDICT=("POSITIVE" if POS else "NULL"),
    near_Aw_median=float(np.median([r["Aw"] for r in near])) if near else None,
    near_p95_median=float(np.median([r["p95"] for r in near])) if near else None,
    far_Aw_median=float(np.median([r["Aw"] for r in far])) if far else None)
with open("/home/yasu/geo-ml/gnss_nuc_cmc_perstation.json","w") as f: json.dump(res,f,indent=0)
print(json.dumps(out,indent=1))
