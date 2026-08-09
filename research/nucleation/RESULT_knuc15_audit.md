# knuc15 — independent audit of the NULL verdict (Opus, 2026-06-14)

> ⚠️ **Research record, not a forecast.** This document is part of a personal research log. It is not an earthquake prediction, forecast, warning, advisory, or operational product of any kind, and it issues no alerts. The figures in it are exploratory retrospective metrics computed on public data and have not been validated for operational use. Do not rely on it for safety, evacuation, business, or any other disaster-response decision. For official information in Japan, use the Japan Meteorological Agency (https://www.jma.go.jp/en/) and your local government. Provided as-is, without warranty of any kind; the author accepts no liability for any loss or damage arising from its use.
>
> ⚠️ **免責事項: 本文書は研究記録であり、予報ではありません**
>
> 地震の予知・予測・予報・警報その他の運用情報ではなく、いかなる警報も発信しません。記載の数値は公開データを用いた探索段階の事後的な研究指標であり、実運用に向けた検証は行っていません。安全確保・避難・事業判断その他の防災上の判断には使用しないでください。日本の公式情報は気象庁（https://www.jma.go.jp/）および各自治体の発表をご確認ください。現状のまま無保証で提供され、利用により生じたいかなる損害についても作者は責任を負いません。

An independent reviewer re-ran the pre-registered judge (verdict reproduced
exactly), verified the kernel/analyzer time convention (round-trip offset 0,
all 1797 events fall in [M6.5, M7.3)), and audited every component.

## Findings
1. No material bug in `analyze_knuc15.py`. matched-centroid stratum
   re-weighting, baseline/surrogate masks, Theil-Sen, the C1 time-shuffle
   (only the time array is permuted; embedding/mag/depth stay attached to each
   event), the C3 per-subset baselines, and the bootstrap noise are all
   correctly implemented. (Minor: the report-only SNR length-matching is
   sloppy but does not affect the verdict.)
2. C2 (surrogate-prospective) carries a real FAIL-direction bias: because the
   encoder is trained early-only, an encoder "temperature drift" (later events
   are progressively more out-of-training-distribution) inflates the
   within-early intrinsic slope (0.0106/h), making the full-window slope
   (0.0034/h) hard to exceed. So C2 ALONE cannot carry the NULL.
3. C1 + P2 passing is fully explained by that generic temporal/encoder drift;
   there is no independent evidence of a real signal (monotone-frac 0.07,
   near ~= far).
4. The decisive, bias-free evidence is C3 (the spatial null): drift does not
   localise to the impending rupture (slope_near 0.00366 ~= slope_far 0.00412),
   which a genuine nucleation signal would require. C3 is unaffected by the
   encoder-drift bias.

## Conclusion of the audit
The NULL verdict is reliable — it is not an artefact of a bug or of the C2
bias, because C3 (spatial null) and P1 (non-monotonicity) fail independently
and C3 is bias-free.

Two corrections adopted:
* CLAIM MODERATED. Not "9 consecutive nulls => onshore single-case nucleation
  detection is exhausted" (an over-generalisation from one case / one method),
  but: "this event-aligned self-supervised representation did not detect a
  prospective nucleation signal in the 2016 Kumamoto foreshock sequence; the
  spatial null (C3) is the decisive, bias-free evidence."
* For any write-up, re-specify C2 on a RESIDUAL basis (subtract the
  early-only intrinsic drift rate, then require the residual full-window slope
  to exceed the time-shuffle null). This removes the encoder-drift bias; C3 is
  expected to remain FAIL, so the verdict is not expected to change.
