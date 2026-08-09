# knuc15 result — SSL waveform-embedding nucleation probe (test 9 of the arc)

> ⚠️ **Research record, not a forecast.** This document is part of a personal research log. It is not an earthquake prediction, forecast, warning, advisory, or operational product of any kind, and it issues no alerts. The figures in it are exploratory retrospective metrics computed on public data and have not been validated for operational use. Do not rely on it for safety, evacuation, business, or any other disaster-response decision. For official information in Japan, use the Japan Meteorological Agency (https://www.jma.go.jp/en/) and your local government. Provided as-is, without warranty of any kind; the author accepts no liability for any loss or damage arising from its use.
>
> ⚠️ **免責事項: 本文書は研究記録であり、予報ではありません**
>
> 地震の予知・予測・予報・警報その他の運用情報ではなく、いかなる警報も発信しません。記載の数値は公開データを用いた探索段階の事後的な研究指標であり、実運用に向けた検証は行っていません。安全確保・避難・事業判断その他の防災上の判断には使用しないでください。日本の公式情報は気象庁（https://www.jma.go.jp/）および各自治体の発表をご確認ください。現状のまま無保証で提供され、利用により生じたいかなる損害についても作者は責任を負いません。

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
