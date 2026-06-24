# A real (weak) spatial precursor: foreshocks migrate specifically toward the eventual mainshock — SoCal population

## Why this matters
The b-value (magnitude) and rate channels of the foreshock-traffic-light arc are NULL across five cases and a ~150-mainshock population test: no FTLS RED, no acceleration beyond ordinary triggering. This is the SPATIAL channel, and the first POSITIVE result in the arc. The one foreshock precursor with credible positive literature is MIGRATION toward the nucleation point (Kato et al. 2012 Tohoku, 2016 Iquique, slow-slip-driven on megathrusts). Here we test it as a population on a dense crustal catalogue and -- after passing every adversarial control a reviewer demanded -- find a weak but real, mainshock-specific spatial tendency.

## Method
SoCal QTM catalogue (Ross et al. 2019), 898,597 template-matched relocated events 2008-2017 (relative location ~0.1-1 km), magnitudes to 0.01. Mainshocks = M>=4.0 events that are the largest within +-15 d AND 20 km (declustered local maxima). Foreshocks per mainshock = events within 20 km, [MS-15d, MS), M>=1.0, M < Mmainshock-0.3, time-ordered. Migration metric = per-sequence Spearman rho(foreshock time, epicentral distance-to-mainshock); rho<0 = foreshocks get CLOSER over time (migration toward). Population inference = Wilcoxon/t-test on the per-sequence rho distribution (the sequence is the unit, n=52 at baseline). This is the pre-specified PRIMARY spec; an 8-cell parameter sweep is reported only as (non-independent) sensitivity.

## Result and the controls that it survives
**Baseline:** mean rho = -0.098, median -0.080, 67% of sequences rho<0, Wilcoxon p = 0.0061; 21% (11/52) individually significant at a per-sequence time-shuffle permutation p<0.05 (vs 5% by chance, ~4x). Median last-third-minus-first-third distance = -0.19 km. The effect is robust in SIGN across all 8 parameter settings (mean rho -0.077 to -0.114; significant in 5/8, marginal p 0.09-0.12 in the smallest-n / smallest-R / shortest-T cells).

Every blocking control demanded by adversarial review passes:
| control | result | meaning |
|---|---|---|
| (1) SYNTHETIC uniform null (same counts, uniform-in-disk + uniform-time, real mainshock loc, full pipeline) | mean rho = +0.005 (p=0.85) | the pipeline/metric does NOT manufacture rho<0 -- the decisive artifact test is clean |
| (2) reference = mainshock | -0.098 (p=0.006) | the contraction is real |
| (2) reference = foreshock centroid | +0.006 (p=0.73) | NOT a general contraction toward the cluster centre |
| (2) reference = random foreshock | -0.038 (p=0.34) | not toward an arbitrary point |
| (2) reference = first foreshock | +0.165 (p<1e-4) | foreshocks EXPAND from the first event (Omori-like) |
| (3) partial Spearman controlling magnitude | -0.106 (p=0.006) | NOT a later=larger=better-located artifact |
| (3) narrow magnitude band (Mc..Mc+0.5) | -0.159 (p=0.032) | survives, stronger, with magnitude held nearly fixed |
| (4) 3D hypocentral distance (depth included) | -0.078 (p=0.076) | survives in 3D (depth adds noise; marginal) |

The decisive points: the synthetic uniform null is clean (+0.005), so the negative rho is not a pipeline artifact; and the contraction is SPECIFIC to the eventual mainshock location (-0.098) -- it vanishes against the foreshock centroid (+0.006) or a random point (-0.038). So foreshocks approach the FUTURE HYPOCENTRE specifically, not the cluster in general, and not because later events are larger/better-located. As an independent validity check, the IDENTICAL metric applied to AFTERSHOCKS gives the opposite, expected sign (mean rho +0.121, 72% expanding = classic Omori-Utsu aftershock-zone expansion; foreshock-vs-aftershock Mann-Whitney p<1e-4).

## Honest scope
- **Weak.** rho ~ -0.1 explains ~1% of rank variance; the median approach (-0.19 km) is comparable to the catalogue location precision. The signal is real and direction-coherent but per-sequence tiny -- NOT operationally useful for hypocentre forecasting.
- **Mechanism-agnostic.** This is catalogue geometry only; we have no geodetic/aseismic-slip evidence here. The result is CONSISTENT with slow-slip-driven nucleation loading (the Kato interpretation) but also with cascade/static-triggering concentrating activity onto the nucleation patch; the data cannot distinguish them. We do not claim slow slip, and we note the Kato megathrust mechanism need not be the crustal one.
- **Selection framing.** "Migration" here means later foreshocks lie nearer the eventual epicentre; whether that is continuous propagation or late concentration at the nucleation patch is not resolved (both are precursory in the same operational sense).
- **Not a magnitude/rate result.** This does NOT revive the FTLS b-RED (still null); it is a distinct, spatial channel.

## Conclusion
On a ~50-sequence SoCal crustal population, foreshocks migrate weakly but significantly and SPECIFICALLY toward the eventual mainshock hypocentre (mean Spearman rho ~ -0.1, mainshock-specific, magnitude-controlled, 3D-robust, opposite to aftershock expansion, and absent from a synthetic no-migration null). This is the arc's first positive precursor channel: where the magnitude (b-value) and rate channels carry no open-data signal, the SPATIAL channel does -- a genuine, if weak and operationally-modest, directed tendency consistent with (but not proof of) nucleation-zone loading, generalising the qualitative Kato megathrust migration to a crustal population at population scale.

Assets (RPi5 ~/geo-ml/qtm/): migration_test.py (baseline + per-sequence permutation), migration_harden.py (fore-vs-aft contrast + 8-cell sweep), migration_controls.py (synthetic null, reference-point, magnitude-partial, narrow-band, 3D). Catalogue qtm12.hypo (SCEDC QTM 12dev, Ross et al. 2019). Opus-reviewed (initial verdict FLAWED pending controls; all four blocking controls -- synthetic null, mainshock-specific referencing, magnitude control, 3D -- subsequently run and PASSED, with effect-size and mechanism wording walked back per the review).
