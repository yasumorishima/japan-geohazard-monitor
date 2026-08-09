# The M5+ forecast AUC ceiling: where skill lives, and why 0.9 is hollow

> ⚠️ **Research record, not a forecast.** This document is part of a personal research log. It is not an earthquake prediction, forecast, warning, advisory, or operational product of any kind, and it issues no alerts. The figures in it are exploratory retrospective metrics computed on public data and have not been validated for operational use. Do not rely on it for safety, evacuation, business, or any other disaster-response decision. For official information in Japan, use the Japan Meteorological Agency (https://www.jma.go.jp/en/) and your local government. Provided as-is, without warranty of any kind; the author accepts no liability for any loss or damage arising from its use.
>
> ⚠️ **免責事項: 本文書は研究記録であり、予報ではありません**
>
> 地震の予知・予測・予報・警報その他の運用情報ではなく、いかなる警報も発信しません。記載の数値は公開データを用いた探索段階の事後的な研究指標であり、実運用に向けた検証は行っていません。安全確保・避難・事業判断その他の防災上の判断には使用しないでください。日本の公式情報は気象庁（https://www.jma.go.jp/）および各自治体の発表をご確認ください。現状のまま無保証で提供され、利用により生じたいかなる損害についても作者は責任を負いません。

A push to raise the operational M5+ forecast AUC toward 0.9, decomposed honestly. Conclusion: the
genuine (probabilistically-skillful) ceiling is AUC ~0.895; AUC 0.9 is reachable only as an
easy-negative metric artifact (negative BSS). Robust against better models AND denser data.

## Setup
USGS / JMA catalogues, Japan box, M5+ target per cell per 34-day window, OOS yearly walk-forward,
HistGradientBoosting + ETAS-kernel features (the production operational_forecast pipeline).

## 1. AUC is spatial-climatology-saturated
Feature-subset OOS AUC (2 deg cells): cell POSITION alone = 0.861; bg (cumulative count) = 0.842;
+6 ETAS temporal kernels lifts the full model only to ~0.858. So the headline AUC (~0.86-0.871 with
the EN+GBT blend) is ~98% spatial climatology; temporal features add ~+0.01.

## 2. Real skill, stripped of easy negatives, is ~0.76
Restricting to seismically ACTIVE cells (>=1 historical M5+): full-model AUC = 0.760 (vs 0.857 all
cells). Trench-core 48 cells: 0.684. Aftershock-primed (recent M>=5.5 within 200 km, 14 d): 0.755,
where temporal adds the most (+0.040 over climatology). Short windows lift the primed AUC: HOR 7d
0.774, 3d 0.809, 1d 0.810 -- the aftershock regime is where temporal physics is genuinely predictable.

## 3. Modeling is exhausted (deep would not help)
Adding the structures an isotropic ETAS cannot represent -- anisotropy, foreshock-migration-toward-cell
(the migration-arc signature), burstiness, neighbour rate -- moves active-region AUC by +0.0001.
Aftershock-physics features (triggering-mainshock distance/time/magnitude, rupture-length scaling,
Bath-law expected max) move the primed 1-3 d AUC by +0.0002 / -0.004. The GBT already extracts the
available discriminative signal; a deep model competes for ~nothing.

## 4. Denser data is also null
JMA unified catalogue (1,395,661 events M>=1, 2011-2023, ~70x denser than USGS M>=4; Kumamoto M7.3 /
Fukushima M7.4 verified). Driver-density sweep (same catalogue, Mc 4 -> 3 -> 2.5, 12x denser drivers):
overall 0.856/0.855/0.859, active 0.760/0.757/0.763, primed 0.767/0.767/0.766 -- flat within noise.
Dense micro-seismicity carries no extra M5+/34 d information at 2 deg.

## 5. AUC 0.9 is reachable but hollow (resolution artifact)
Finer cells raise AUC by adding easy negatives, NOT skill. JMA dense background, AUC | BSS(vs base-rate):
2.0 deg 0.856 | +0.091 ; 1.0 deg 0.895 | +0.042 ; 0.5 deg 0.901 | -0.020. At 0.5 deg AUC crosses 0.9
but BSS goes NEGATIVE (forecast worse than climatology). Per-fold isotonic calibration does not rescue
it (OOS BSS worsens: 1 deg +0.042 -> -0.040, 0.5 deg -0.020 -> -0.079) -- the skill genuinely is not
there; the negative BSS is not mere miscalibration.

## Bottom line
- Genuine skillful ceiling: AUC ~0.895 at 1 deg (BSS +0.042, beats climatology).
- AUC 0.9: attainable as a number (0.9011 at 0.5 deg) but hollow -- negative BSS, easy-negative artifact.
- The ceiling is an INFORMATION limit, not a modeling limit: better models (deep, +0.0002) and denser
  data (JMA, flat) both fail to move it. Consistent with the project nucleation/precursor arc, where
  every open-data channel (b-value, GNSS, InSAR, OBP, waveforms, migration) is null or operationally
  negligible. Research-only; not productionised.

Assets (RPi5 ~/geo-ml/): oef_headroom.py, oef_hardauc.py, oef_active_temporal.py, oef_richfeat.py,
oef_aftershock.py, oef_shortwin.py, oef_aftfeat.py, oef_aft_headroom.py, jma/{build_cache,jma_auc,
jma_resolution,jma_fine,jma_calib}.py + jma/jma_japan.npz (1.4M-event JMA catalogue).
