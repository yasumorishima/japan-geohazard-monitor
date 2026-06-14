# Geodetic positive control - 2014 Iquique (GNSS 5-min kinematic)

**Verdict: NULL across all three methods - the positive control FAILS, which bounds the method's sensitivity and reframes the Kumamoto geodetic null.**

The Kumamoto geodetic test returned null with raw, common-mode-filtered, and forward-inversion methods. To learn whether that null means "no slow slip" or "slow slip below this method's floor", we apply the identical harness to a positive control: the 2014 Iquique (Mw8.1) sequence, where Ruiz et al. (2014, Science) reported a geodetically OBSERVED slow-slip transient in cGPS during the ~16-day foreshock sequence (Mw6.7 2014-03-16 -> Mw8.1 2014-04-01). If the method recovers Ruiz's documented signal, the Kumamoto null is a true absence; if it does not, the null is sensitivity-bounded.

## Data
NGL 5-min kinematic, 27 stations (Iquique coastal box + central-Chile far-field controls), DOY 040-095. The rupture is offshore, so the nearest onshore stations are 68-150 km away (PSGA 68 km, ATJN 75 km, IQQE 99 km).

## Method
Identical to Kumamoto, re-parameterized: 16-day window [Mw6.7, Mw8.1], coseismic steps removed at the four cataloged M>=6 foreshocks, near field <=150 km, 1-day median endpoints, and an offshore-megathrust geometry grid for the inversion (strike 355/10, dip 15/20, depth 15/25/35 km, L=50 W=40 km).

## Result
- Raw judge: NULL. G1 PASS (55% of coastal stations exceed their baseline 95th percentile - a real near-field transient, unlike Kumamoto) and G2 PASS (decay with distance), but G3null FAIL: the central-Chile controls exceed too (83%, 11 mm > near 8 mm) - they sit on an active margin and are not quiet.
- Common-mode-filtered: NULL (G1 64%, G3null FAIL - the controls carry their own deformation).
- Forward megathrust inversion: NULL, decisive. Window best-fit variance reduction = 0.064, far below the baseline-window distribution (median 0.225, 95th percentile 0.460). The near-field transient does not increase toward the rupture - the three closest stations (68-99 km) show the smallest transients while 134 km stations show the largest - so it is not a coherent offshore-source deformation field.

## Reading
The positive control fails: NGL 5-min kinematic prospective net-displacement and grid-search inversion do not recover the Ruiz et al. (2014) documented slow slip. The Kumamoto geodetic null is therefore sensitivity-bounded, not a demonstrated absence: few-mm precursory transients are below the floor set by 5-7 mm kinematic scatter plus regional common-mode. The documented geodetic precursors at Iquique were extracted with bespoke processing - careful station selection, transient time-function modeling, longer-baseline cGPS - beyond this openly-reproducible method. The honest, generalizable conclusion is about the method: a cheap, reproducible prospective pipeline on open 5-min kinematic GNSS does not resolve nucleation-scale slow slip in either case, and the published geodetic precursors required case-specific analysis this pipeline does not replicate.

Scripts: research/nucleation/gnss_iquique_judge.py, gnss_iquique_judge_cmc.py, gnss_iquique_inversion.py. Data: NGL IGS20/kenv (Iquique coastal + central-Chile controls).
