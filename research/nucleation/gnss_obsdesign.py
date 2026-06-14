"""Constructive observing-system design from the measured detection floors. The
detectable slip scales as S* ~ C * sigma_noise / |GF|_net, where |GF|_net =
RMS surface displacement the network sees per unit slip (computed exactly with
the validated Okada85), and C*sigma is calibrated from the measured injection
floors (Kumamoto S*=0.077 m, Iquique S*=0.116 m, both on open 5-min kinematic).
Holding the calibration fixed, we forward-compute the Mw floor for hypothetical
networks: seafloor GNSS-A above the offshore rupture (geometry exact), denser
onshore coverage (exact), and a lower-noise data product such as daily cGPS
(noise scaling, labeled as an assumption). Goal: what observing system pushes the
floor below the ~Mw 6 inferred precursor scale?
"""
import os, math, json
import numpy as np
from okada85 import okada85
MU=30e9
def enkm(la,lo,la0,lo0): return (lo-lo0)*111.32*math.cos(math.radians(la0)),(la-la0)*110.57
def stalist(root, epi, fitkm):
    out=[]
    with open(os.path.join(root,"_stations.csv")) as f:
        next(f)
        for ln in f:
            a=ln.strip().split(",")
            if len(a)<4: continue
            la=float(a[1]); lo=float(a[2])
            R=6371.0;p=math.pi/180
            dla=(epi[0]-la)*p;dlo=(epi[1]-lo)*p
            d=2*R*math.asin(math.sqrt(math.sin(dla/2)**2+math.cos(la*p)*math.cos(epi[0]*p)*math.sin(dlo/2)**2))
            if d<=fitkm: out.append((la,lo))
    return out
def gfnet(coords, geo, L, W, inj_rake):
    lo0,la0,dep,strike,dip=geo
    ee=np.array([enkm(la,lo,la0,lo0)[0] for la,lo in coords]); nn=np.array([enkm(la,lo,la0,lo0)[1] for la,lo in coords])
    gE,gN,_=okada85(ee,nn,dep,strike,dip,L,W,inj_rake,1.0)
    return math.sqrt(np.sum(gE**2+gN**2))   # RMS-ish network sensitivity per unit slip (m per 1m slip)
def mw(S,L,W): M0=MU*(L*1e3)*(W*1e3)*S; return (2.0/3.0)*(math.log10(M0)-9.1)

# ---- KUMAMOTO (onshore strike-slip) ----
K_geo=(130.763,32.755,10.0,235.0,65.0); K_L,K_W=14.0,10.0; K_epi=(32.755,130.763)
K_coords=stalist("/home/yasu/geo-ml/kenv_window",K_epi,60.0)
K_gf=gfnet(K_coords,K_geo,K_L,K_W,0.0)      # strike-slip GF
K_Smeas=0.07735                              # measured baseline-median floor (m)
K_C=K_Smeas*K_gf                             # calibration constant (= C*sigma)
# ---- IQUIQUE (offshore thrust) ----
I_geo=(-71.0,-19.7,25.0,5.0,18.0); I_L,I_W=50.0,40.0; I_epi=(-19.61,-70.77)
I_coords=stalist("/home/yasu/geo-ml/iq_kenv",I_epi,250.0)
I_gf=gfnet(I_coords,I_geo,I_L,I_W,90.0)
I_Smeas=0.11617
I_C=I_Smeas*I_gf

# hypothetical Iquique seafloor GNSS-A above the megathrust (r ~ 10-40 km, on the source)
sea=[(-19.5,-71.1),(-19.7,-71.2),(-19.9,-71.0),(-19.6,-70.95),(-19.8,-71.25),(-20.0,-71.15)]
def floor(C,coords,geo,L,W,rake,noise=1.0):
    gf=gfnet(coords,geo,L,W,rake)
    S=C*noise/gf
    return mw(S,L,W), S, gf

res={"kumamoto":{}, "iquique":{}}
# Kumamoto configs
res["kumamoto"]["real_onshore_5min"]=dict(zip(("Mw","S_m","gfnet"),floor(K_C,K_coords,K_geo,K_L,K_W,0.0)))
res["kumamoto"]["onshore_x2_density"]=dict(zip(("Mw","S_m","gfnet"),floor(K_C,K_coords+K_coords,K_geo,K_L,K_W,0.0)))  # density doubles gf by sqrt2 via 2x stations
res["kumamoto"]["daily_cGPS_noise_div3"]=dict(zip(("Mw","S_m","gfnet"),floor(K_C,K_coords,K_geo,K_L,K_W,0.0,noise=1/3.0)))
res["kumamoto"]["x2_dens_plus_daily"]=dict(zip(("Mw","S_m","gfnet"),floor(K_C,K_coords+K_coords,K_geo,K_L,K_W,0.0,noise=1/3.0)))
# Iquique configs
res["iquique"]["real_onshore_5min"]=dict(zip(("Mw","S_m","gfnet"),floor(I_C,I_coords,I_geo,I_L,I_W,90.0)))
res["iquique"]["plus_seafloor_GNSSA"]=dict(zip(("Mw","S_m","gfnet"),floor(I_C,I_coords+sea,I_geo,I_L,I_W,90.0)))
res["iquique"]["seafloor_plus_daily"]=dict(zip(("Mw","S_m","gfnet"),floor(I_C,I_coords+sea,I_geo,I_L,I_W,90.0,noise=1/3.0)))
res["iquique"]["daily_only"]=dict(zip(("Mw","S_m","gfnet"),floor(I_C,I_coords,I_geo,I_L,I_W,90.0,noise=1/3.0)))
res["_calib"]=dict(K_gf=K_gf,K_C=K_C,K_nsta=len(K_coords),I_gf=I_gf,I_C=I_C,I_nsta=len(I_coords),n_seafloor=len(sea))
print(json.dumps(res,indent=1,default=lambda x:round(float(x),4)))
