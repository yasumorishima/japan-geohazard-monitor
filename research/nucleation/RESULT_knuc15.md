# knuc15 result — SSL waveform-embedding nucleation probe (test 9 of the arc)

Verdict produced by the pre-registered judge `analyze_knuc15.py` (committed
in commit 94dc4983, BEFORE the kernel ran) applied to the kernel output
`embeddings.csv` (yasunorim/ssl-kumamoto, 2016 Kumamoto foreshock window
[M6.5 2016-04-14 21:26:34 JST, M7.3 2016-04-16 01:25:05 JST)).

## Kernel
- 8 core stations stable across the whole window: IZMH KKCH GKSH HKSH ASKH
  KHKH KAHH FJIH.
- 1797 micro-catalogue events with >= 3 core stations (635 in the early-only
  training window = first 30 percent), tensor (1797, 24, 600) = 8 stations x
  3 components x 6 s @ 100 Hz.
- SimCLR contrastive encoder (augmentations: per-channel amplitude scale,
  time jitter, station dropout, noise) trained EARLY-ONLY; 32-dim L2-normalised
  embedding inferred for all events. Contrastive loss 5.96 -> 3.78 over 120
  epochs (the encoder learned real structure from the raw waveforms, unlike a
  random-data smoke which stayed flat).
- torch 2.5.1+cu121 pinned for Tesla P100/T4 compatibility (the current Kaggle
  image ships torch 2.10+cu128, which dropped sm_60 P100 support).

## Verdict (events in [M6.5, M7.3), 2-h M- and depth-matched bins)
    bins=14  baseline events=635  per-bin events 103/122/157
    P1 trend     : Theil-Sen +0.00340 /h, total 0.0883 (>2*noise 0.0242)
                   BUT monotone-frac 0.07 (need >=0.60)            -> FAIL
    P2 final-step: final-6h drift 0.1266 vs baseline 0.0705 = 1.80x -> PASS
    C1 shuffle   : observed 0.00340 vs time-shuffle null p95 0.00110 -> PASS
    C2 surrogate : full-window slope 0.00340 vs within-early-only
                   intrinsic slope 0.01058                          -> FAIL
    C3 spatial   : slope_near(<=10km, n=1579) 0.00366 vs
                   slope_far(>=20km, n=48) 0.00412                  -> FAIL
    SNR gate     : r(drift, snr) = 0.275                            -> clean
    VERDICT = NULL  (P1 FAIL, C2 FAIL, C3 FAIL; only C1 + P2 pass)

## Interpretation
A late rise in embedding drift toward the mainshock DOES exist (P2 1.80x) and
survives time-label shuffling (C1) — the strongest apparent "signal" of any of
the nine tests. But it fails the two decisive confound controls that separate
genuine nucleation from generic artifacts:
  * C2 (surrogate-prospective): the late-window drift rate (0.0034/h) is SLOWER
    than the intrinsic drift already present WITHIN the stationary early-only
    window (0.0106/h). The rise is consistent with "the longer since training,
    the more novel events look" — a generic temporal/encoder-fit drift, not a
    nucleation-specific acceleration.
  * C3 (spatial null): the drift does NOT localise to the impending rupture;
    events far (>=20 km) from the mainshock epicentre drift as much or more than
    near (<=10 km) events. Genuine nucleation should concentrate near the rupture
    (far n=48 is small — caveat).
  * P1 monotonicity also fails: the drift trajectory is non-monotone.

This is exactly the failure mode the multi-control design was built to expose:
C1 + P2 passing in isolation would have been mis-read as a detection; the
surrogate-prospective and spatial-null controls reveal it as non-nucleation.

## Conclusion
9th consecutive null. Even a modern self-supervised (SimCLR) representation of
raw onshore Hi-net foreshock waveforms, with amplitude/coverage-invariant
augmentations, M/depth-matched binning, and triple confound control, does NOT
prospectively mark the independent Kumamoto mainshock. Combined with tests 1-8
(occurrence/geometry/family/magnitude catalogue channels, below-catalogue
tremor, single-station dv/v), onshore single-case prospective nucleation
detection is exhausted across catalogue statistics, medium elasticity, AND
learned waveform representation. Publication-grade null.
