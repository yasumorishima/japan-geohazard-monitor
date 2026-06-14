import numpy as np
EPS=1e-12
def okada85(e,n,depth,strike,dip,L,W,rake,slip,opn=0.0,nu=0.25):
    e=np.asarray(e,float); n=np.asarray(n,float)
    st=np.radians(strike); dp=np.radians(dip); rk=np.radians(rake)
    U1=np.cos(rk)*slip; U2=np.sin(rk)*slip
    sd=np.sin(dp); cd=np.cos(dp)
    d=depth+sd*W/2.0
    ec=e+np.cos(st)*cd*W/2.0; nc=n-np.sin(st)*cd*W/2.0
    x=np.cos(st)*nc+np.sin(st)*ec+L/2.0
    y=np.sin(st)*nc-np.cos(st)*ec+cd*W
    p=y*cd+d*sd; q=y*sd-d*cd
    def I5(xi,eta,qq,R,db):
        X=np.sqrt(xi**2+qq**2)
        if cd>EPS:
            with np.errstate(all='ignore'):
                val=(1-2*nu)*2.0/cd*np.arctan((eta*(X+qq*cd)+X*(R+X)*sd)/(xi*(R+X)*cd))
            return np.where(xi==0,0.0,val)
        return -(1-2*nu)*xi*sd/(R+db)
    def I4(db,eta,qq,R):
        if cd>EPS: return (1-2*nu)/cd*(np.log(R+db)-sd*np.log(R+eta))
        return -(1-2*nu)*qq/(R+db)
    def I3(eta,qq,R):
        yb=eta*cd+qq*sd; db=eta*sd-qq*cd
        if cd>EPS: return (1-2*nu)*(yb/(cd*(R+db))-np.log(R+eta))+sd/cd*I4(db,eta,qq,R)
        return (1-2*nu)/2.0*(eta/(R+db)+yb*qq/(R+db)**2-np.log(R+eta))
    def I2(eta,qq,R): return (1-2*nu)*(-np.log(R+eta))-I3(eta,qq,R)
    def I1(xi,eta,qq,R):
        db=eta*sd-qq*cd
        if cd>EPS: return (1-2*nu)*(-xi/(cd*(R+db)))-sd/cd*I5(xi,eta,qq,R,db)
        return -(1-2*nu)/2.0*xi*qq/(R+db)**2
    def ux_ss(xi,eta,qq):
        R=np.sqrt(xi**2+eta**2+qq**2)
        u=xi*qq/(R*(R+eta))+I1(xi,eta,qq,R)*sd
        return u+np.where(qq!=0,np.arctan(xi*eta/(qq*R)),0.0)
    def uy_ss(xi,eta,qq):
        R=np.sqrt(xi**2+eta**2+qq**2)
        return (eta*cd+qq*sd)*qq/(R*(R+eta))+qq*cd/(R+eta)+I2(eta,qq,R)*sd
    def uz_ss(xi,eta,qq):
        R=np.sqrt(xi**2+eta**2+qq**2); db=eta*sd-qq*cd
        return (eta*sd-qq*cd)*qq/(R*(R+eta))+qq*sd/(R+eta)+I4(db,eta,qq,R)*sd
    def ux_ds(xi,eta,qq):
        R=np.sqrt(xi**2+eta**2+qq**2)
        return qq/R-I3(eta,qq,R)*sd*cd
    def uy_ds(xi,eta,qq):
        R=np.sqrt(xi**2+eta**2+qq**2)
        u=(eta*cd+qq*sd)*qq/(R*(R+xi))-I1(xi,eta,qq,R)*sd*cd
        return u+np.where(qq!=0,cd*np.arctan(xi*eta/(qq*R)),0.0)
    def uz_ds(xi,eta,qq):
        R=np.sqrt(xi**2+eta**2+qq**2); db=eta*sd-qq*cd
        u=db*qq/(R*(R+xi))-I5(xi,eta,qq,R,db)*sd*cd
        return u+np.where(qq!=0,sd*np.arctan(xi*eta/(qq*R)),0.0)
    def chin(f): return f(x,p,q)-f(x,p-W,q)-f(x-L,p,q)+f(x-L,p-W,q)
    ux=-U1/(2*np.pi)*chin(ux_ss)-U2/(2*np.pi)*chin(ux_ds)
    uy=-U1/(2*np.pi)*chin(uy_ss)-U2/(2*np.pi)*chin(uy_ds)
    uz=-U1/(2*np.pi)*chin(uz_ss)-U2/(2*np.pi)*chin(uz_ds)
    uE=np.sin(st)*ux-np.cos(st)*uy; uN=np.cos(st)*ux+np.sin(st)*uy
    return uE,uN,uz

if __name__=="__main__":
    # Validation: Okada (1985) Table 2, Case 2 (strike-slip and dip-slip).
    # okada85 DEPTH = centroid depth; Table 2 d=4 is the bottom edge, so to
    # reproduce Okada's internal geometry pass DEPTH = 4 - sin(dip)*W/2.
    dip,L,W=70.0,3.0,2.0
    cd=np.cos(np.radians(dip)); sd=np.sin(np.radians(dip))
    e=2-L/2; n=3-cd*W/2; depth=4-sd*W/2
    exp={"strike":(-8.689e-3,-4.298e-3,-2.747e-3),"dip":(-4.682e-3,-3.527e-2,-3.564e-2)}
    ok=True
    for mode,rake,slip in (("strike",0,1),("dip",90,1)):
        uE,uN,uZ=okada85(e,n,depth,90,dip,L,W,rake,slip)
        got=(float(uE),float(uN),float(uZ)); ref=exp[mode]
        d=max(abs(got[i]-ref[i]) for i in range(3)); ok=ok and d<1e-5
        print(mode,"got %.3e %.3e %.3e  maxdiff %.1e"%(got+(d,)))
    print("VALIDATION", "PASS" if ok else "FAIL")
