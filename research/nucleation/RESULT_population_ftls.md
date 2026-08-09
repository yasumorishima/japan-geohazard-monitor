# Population test of the FTLS foreshock b-drop: ~150 SoCal mainshocks (QTM), background-definition sensitivity

> ⚠️ **Research record, not a forecast.** This document is part of a personal research log. It is not an earthquake prediction, forecast, warning, advisory, or operational product of any kind, and it issues no alerts. The figures in it are exploratory retrospective metrics computed on public data and have not been validated for operational use. Do not rely on it for safety, evacuation, business, or any other disaster-response decision. For official information in Japan, use the Japan Meteorological Agency (https://www.jma.go.jp/en/) and your local government. Provided as-is, without warranty of any kind; the author accepts no liability for any loss or damage arising from its use.
>
> ⚠️ **免責事項: 本文書は研究記録であり、予報ではありません**
>
> 地震の予知・予測・予報・警報その他の運用情報ではなく、いかなる警報も発信しません。記載の数値は公開データを用いた探索段階の事後的な研究指標であり、実運用に向けた検証は行っていません。安全確保・避難・事業判断その他の防災上の判断には使用しないでください。日本の公式情報は気象庁（https://www.jma.go.jp/）および各自治体の発表をご確認ください。現状のまま無保証で提供され、利用により生じたいかなる損害についても作者は責任を負いません。

## Motivation
The five FTLS b-channel case studies and the controlled split-half experiment are a CASE SERIES, not a population test (a reviewer's standing objection). This is the population test: does a foreshock-phase b-value DROP relative to background (the Gulia and Wiemer 2019 RED light) appear systematically across many mainshocks, and does it survive the incompleteness-robust b-positive estimator (Van der Elst 2021)? Catalogue: SoCal QTM (Ross et al. 2019), 898,597 template-matched events 2008-2017, Mc ~0.3-0.5, magnitudes to 0.01. This tests the FTLS MECHANISM (does foreshock b drop?), not the exact operational sliding-window pipeline.

## Method
Mainshocks = events M>=Mthr that are the largest within +-Tw days AND Rkm (declustered local maxima). For each mainshock, stacked: foreshock window [MS-Tw, MS); aftershock window (MS+1d, MS+Tw] (1-day STAI buffer); and a SPATIALLY-MATCHED quiet-time background = events in the SAME Rkm disk but outside +-Tw of ANY mainshock -- so foreshock and background sample the same regions (controls spatial-b heterogeneity) and are built identically (within-sequence positive magnitude differences >= 0.10, pooled). Estimators: Aki MLE (analysis Mc 1.0, bin 0.1) and b-positive. Significance by SEQUENCE bootstrap (resample whole sequences 1000x), not a Shi-Bolt z (which is inflated by the non-independence of pooled successive differences). Baseline Tw=15d, Rkm=20; sensitivity Tw=7d, Rkm=10. Mthr swept 4.0/4.5/5.0.

## Results
**The apparent RED is background-definition-dependent.** Against the WHOLE-REGION background (all SoCal), the stacked foreshock Aki is 17-23% below background across Mthr 4.0/4.5/5.0 (a large, systematic RED), but b-positive is +6 to +8% ABOVE the whole-region background (the sign flips). The whole-region background mixes high-b geothermal zones (Salton Sea, Coso) with lower-b strike-slip crust, so the foreshock-vs-whole-region contrast is dominated by spatial-b heterogeneity, not a foreshock signal.

**Against a SPATIALLY-MATCHED quiet-time background** (same fault-zone disks, quiet times, identical construction), the drop is small in both estimators:
| Tw / Rkm | mainshocks | fore-seq | Aki foreshock vs matched-bg | b-positive foreshock vs matched-bg (seq-bootstrap 95% CI) |
|---|---|---|---|---|
| 15d / 20km | 156 | 98 | 0.59 vs 0.65 = -8.0% | 0.75 vs 0.80 = -5.9% (CI [-0.078, -0.019]) |
| 7d / 10km | 175 | 78 | 0.56 vs 0.58 = -3.1% | 0.74 vs 0.79 = -7.1% (CI [-0.088, -0.025]) |

So most of the dramatic whole-region RED (-22%) is spatial-b + incompleteness; against the matched background only a small b-positive decrease (~ -6%, bootstrap-significant) survives -- an order of magnitude below the apparent whole-region RED and well below any operational FTLS RED threshold.

**The foreshock window is still incomplete at M>=1.0.** Restricting the foreshock window to M>=1.0, Aki = 0.59 but b-positive = 0.75 (a large gap), and the matched-background also has Aki 0.65 < b+ 0.80 -- both phases carry residual gradual incompleteness above the nominal Mc. Because a smooth sub-Mc roll-off biases b-positive LOW (see the synthesis note), part of even the -6% matched-background b+ decrease may be residual incompleteness rather than a real foreshock b decrease; it is an upper bound on any genuine effect.

**Per-sequence distribution.** For the 20-24 sequences large enough for individual estimates (>=60 fore and aft events), the median foreshock-vs-aftershock drop is small (Aki -1.4% to -5.2%; b+ -1.9% to -3.7%), and only ~6 of ~22 sequences (~25-30%) show a >10% RED in either estimator -- i.e. an FTLS RED is the exception, not the rule, per sequence.

## Conclusion
Across ~150-175 SoCal M>=4 mainshocks, there is NO robust FTLS-magnitude foreshock b-value RED. The apparent RED is not robust to the background definition: it is large (-22% Aki) and even sign-stable only against a tectonically heterogeneous whole-region background, and it collapses to a small (-6% b+, partly residual incompleteness) decrease against a spatially-matched background -- with the sign of the b-positive contrast flipping between the two backgrounds. The Aki drop is further inflated by foreshock-window incompleteness (Aki << b-positive even at M>=1.0). Per sequence, a >10% RED occurs in only ~25-30% of cases. This generalises the five-case nulls to a population and quantifies WHY apparent REDs arise (background-b heterogeneity + analysis-Mc incompleteness, with at most a small real foreshock b decrease), consistent with the parameter-sensitivity critique of Dascher-Cousineau et al. (2020). Scope: tests the foreshock-b-drop mechanism, not the FTLS operational pipeline; a case-population of SoCal crustal events, magnitudes to 0.01 (b-positive dM>=0.10), Mc 1.0.

Assets (RPi5 ~/geo-ml/qtm/): population_test.py / population_test2.py (whole-region background + Mthr sweep) / population_test3.py (spatially-matched background, sequence bootstrap, per-sequence distribution, M>=1.0 incompleteness check, declustering sensitivity). Catalogue qtm12.hypo (SCEDC QTM 12dev, Ross et al. 2019). Opus-reviewed (SOUND-WITH-FIXES; matched-background control, sequence-bootstrap CI, per-sequence distribution, and softened wording all applied -- the matched background revealed the spatial-b confound and a small residual that the whole-region comparison had hidden).
