# Iquique 2014 foreshock geometry: the documented FRONT registers as front under the crustal diagnostic

## Question
The foreshock-migration arc established that my crustal M4-6.5 population migrates RADIALLY toward the
nucleus (weak directionality), and argued this is a DISTINCT regime from the literature documented
PROPAGATING-FRONT migrations (Tohoku/Iquique/Kumamoto, 2-10 km/day). That front-rejection rested on
velocity-scale and synthetic calibration -- a qualitative contrast. This test makes it quantitative:
apply the IDENTICAL crustal FRONT/RADIAL geometry diagnostic to the canonical documented front case,
Iquique 2014 (Mw8.1, 2014-04-01), and ask whether it registers as front.

## Data and method
- Catalogue: Sippl et al. (2018) IPOC DD, 101,602 events 2007-2014, ML, GFZ open (latin-1 header).
- Mainshock = the Mw8.1 (lat -19.589, lon -70.940, dep 19.9 km). Foreshocks: pre-mainshock, MC>=2.6,
  mag < M-0.3, within (T,R) window, depth filtered.
- Metrics identical to migration_geometry.py: D = |Spearman(time-order, PC1 projection)| (directional
  progression); EDGE = mainshock percentile-from-centre along PC1 (1=edge/front, 0=centre); AR = PCA
  aspect ratio. Calibrated vs synthetic FRONT/RADIAL end-members at matched n and footprint L.

## Result: Iquique sits in the front direction on both discriminating axes
Crustal population (REAL migrating, n=299): D median 0.136, EDGE median 0.442, AR 2.06.
synth FRONT (sc 0.15): D 0.89 / EDGE 0.88. synth RADIAL: D 0.03-0.09 / EDGE 0.04-0.06.

Iquique foreshocks across windows (depth<=50 km):
  T15 R30 : n265 D0.314 EDGE0.034  (near-source, isotropic = contraction onset only)
  T15 R50 : n419 D0.305 EDGE0.709
  T30 R50 : n540 D0.418 EDGE0.793
  T30 R80 : n634 D0.467 EDGE0.811 AR1.75 PC1az~0deg  (representative)
  T90 R80 : n673 D0.288 EDGE0.759
  T90 R120: n694 D0.337 EDGE0.787

At every window covering the documented along-strike propagation scale (R>=50 km) the signature is
consistently front-leaning: D in [0.29,0.47], EDGE in [0.71,0.81], elongation along PC1 azimuth
~0-10 deg (N-S = trench strike), matching the documented along-strike / up-dip migration direction.
The tight near-source window (R30 km) is isotropic (EDGE 0.03), consistent with the contraction onset
before the along-strike propagation develops.

## Significance (single-realization tests -- NOT a two-population test)
- Iquique D=0.467 is at the 91st percentile of the crustal migrating D distribution; EDGE=0.811 at 87th.
- Matched isotropic-radial null (n=634, L=36.5 km, 2000 sims): P(D>=0.467)=0.0000, P(EDGE>=0.811)=0.0000
  (radial-null medians D 0.032, EDGE 0.028). The Iquique foreshock geometry cannot arise from radial
  collapse.

## Controls (both pass, both strengthen)
- Depth stratification (3-A): foreshock depths are shallow (median 22.6 km; 9% >40 km, 2% >50 km =
  minimal intraslab). The front signature PERSISTS and STRENGTHENS on the interface band: dep<=30 km
  -> D 0.507, EDGE 0.804; dep<=40 km -> D 0.476, EDGE 0.802. Tightening depth RAISES D (0.442 all ->
  0.507) -- intraslab events dilute, not create, the signal. The directionality is interface up-dip.
- Time-shuffle null (3-B): on the interface band (dep<=40, n=585), permuting time labels 3000x gives
  shuffle-D median 0.028 (95pct 0.080) vs D_obs 0.476, P(shuffle>=obs)=0.0000. The directional
  progression is a real time-ordered signal, not a static elongation / box-shape artifact. (EDGE is
  time-invariant; EDGE 0.802 reflects the genuine spatial offset of the foreshock cloud = mainshock at
  the cloud edge.)

## Honest framing (reviewer-required)
- Single well-characterized case, not a two-population separation. CAPABILITY demonstration: the same
  code that places my crustal population near radial places the canonical documented front in the
  upper-decile directional / edge-loaded regime, statistically incompatible with radial collapse.
- Primary discriminating axis is DIRECTIONALITY D; EDGE secondary (crustal EDGE median 0.442 is
  middling, so EDGE alone is a weak discriminator).
- Front vs radial is NOT a clean dichotomy: Iquique radial_rho is also negative (-0.49) -- it contracts
  AND propagates = convergent directional migration. The regimes are separated by the PRESENCE of strong
  directionality, not the absence of contraction.
- Iquique D=0.467 is the 91st crustal percentile -> the crustal upper ~9% tail overlaps it; a few
  crustal sequences are equally directional. The separation is a distribution shift + upper-tail
  placement of the documented front, NOT a binary non-overlapping split.
- Iquique is a SCATTERED front (D~0.47 = ~0.5 L lateral scatter in calibration), not the idealized
  D~0.89 -- expected for a finite-width megathrust foreshock zone.

## Bottom line
The arc contrast-not-confirmation placement is now backed by a direct measurement, not only a
velocity-scale argument: under the identical diagnostic the canonical documented front (Iquique 2014)
lands in the directional / edge-loaded front regime (interface-driven, time-ordered, p<5e-4 vs radial
null), whereas my crustal population median is radial-leaning. Same migration phenomenon, two geometric
regimes, separated by directionality D. Research-only; not productionised.

Assets (RPi5 ~/geo-ml/iquique/): geom_iquique.py, geom_sig.py, geom_ctrl.py.

## Addendum: the crustal geometry DISTRIBUTION -- radial-dominant with a real directional tail

The single-case Iquique result is sharpened by characterising the crustal migrating population geometry
DISTRIBUTION (n=299 migrating sequences), not just its median.

- D quantiles: p10 0.02, p25 0.07, median 0.14, p75 0.27, p90 0.42, p95 0.57 (right-skewed, skew 1.39);
  EDGE median 0.44. The median sequence is radial-leaning, but the upper tail reaches front-like values.
- ~17% (51/299) classify as front-like by the D+EDGE nearest-end-member rule (nearer synth FRONT than
  synth RADIAL in standardised (D,EDGE)). NOTE: front-like is an OPERATIONAL geometry label (directional
  + edge-loaded), NOT a propagating-front mechanism claim -- the isotropic-radial null cannot exclude an
  anisotropic radial process; only the EDGE coupling mitigates that.
- The directional tail is REAL, not small-n estimation noise: per sequence, comparing observed D to the
  isotropic-radial null at that sequence OWN n, 24.4% exceed the 95th null percentile (chance 5%) and
  14.0% exceed the 99th (chance 1%), binomial p=9.3e-30. It STRENGTHENS on well-resolved sequences
  (n>=40: 30.7% exceed the 95th null) -- the opposite of what estimation noise would do.
- NOT bimodal. Raw-D GMM appears to prefer 2 components (dBIC -107), but that is the known artifact of
  fitting Gaussians to a bounded right-skewed variable; on the proper logit(D) scale GMM prefers ONE
  component (dBIC +3.1). The honest statement is a single right-skewed mode with a real heavy directional
  tail, not two modes.
- The tail is MAGNITUDE-INDEPENDENT: front-like vs radial mainshock magnitude is indistinguishable
  (median 4.30 both; Mann-Whitney p=0.41; Cliff delta 0.07), and D-vs-M is flat overall (rho -0.009,
  p=0.88) and within the front-like subset (rho -0.13, p=0.38). The directional tail is not a magnitude
  continuum toward the megathrust-front scale -- consistent with the arc scale-continuum rejection.
- Iquique (documented megathrust front) sits at the 91st D / 87th EDGE crustal percentile -- at/beyond
  the crustal directional tail.

Refinement of the arc wording: the earlier uniformly-radial crustal characterisation was an
OVERSTATEMENT and is corrected to RADIAL-DOMINANT. The pooled rho<0 and the radial median are the
mean/majority behaviour; sequence-level the population carries a real (~17%), magnitude-independent,
directionally-anisotropic minority that the average had hidden. This does NOT weaken contrast-not-
confirmation: megathrust foreshock migration is front-TYPICAL whereas crustal is radial-MAJORITY with a
front-like minority, and the documented great-earthquake front sits at the crustal directional extreme
-- a distribution-shift contrast, not a binary split, and one internal to a weak, operationally-
negligible migration signal. Iquique sharing the crustal directional tail is a geometric-continuity
observation, not a same-mechanism claim.

Assets (RPi5 ~/geo-ml/qtm/): geom_distribution.py, geom_dist_ctrl.py, geom_dist_fix.py.
