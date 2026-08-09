# Operational monthly M5+ outlook (Japan)

> ⚠️ **Experimental research output. Not an official forecast. Do not use it for any safety decision.**
>
> The probabilities published in this directory are the output of a personal, experimental research pipeline. They are **not** an official earthquake forecast and are **not** issued, endorsed, or reviewed by any authority. In Japan, official earthquake information and assessment come from the Japan Meteorological Agency (https://www.jma.go.jp/en/) and the Earthquake Research Committee of the Headquarters for Earthquake Research Promotion; use those sources, not this one. Nothing here is a prediction of when, where, or how large an earthquake will be, no alarm or alert is issued at any probability level, and the numbers may be wrong, stale, or missing without notice. The measured skill is modest and is dominated by long-term spatial climatology, as described below. **Do not use this material for evacuation, safety, engineering, insurance, financial, operational, or any other decision affecting people or property.** Everything here is provided as-is, without warranty of any kind, and the author accepts no liability for any loss or damage arising from its use or from reliance on it.
>
> ⚠️ **実験的な研究出力です。公式な予報ではありません。防災上の判断には使用しないでください**
>
> 本ディレクトリで公開している確率値は、個人による実験的な研究パイプラインの出力です。公式な地震予報ではなく、いかなる公的機関の発表・承認・査読も受けていません。日本の地震に関する公式情報および評価は、気象庁（https://www.jma.go.jp/）および地震調査研究推進本部 地震調査委員会が発表するものをご確認ください。本ディレクトリの内容は、地震の発生時期・場所・規模を予知するものではなく、確率の高低にかかわらずいかなる警報も発しません。数値は誤りを含む場合があり、予告なく古くなる、または欠測することがあります。測定されたスキルは限定的で、その大部分は長期的な空間的気候値に由来します（詳細は以下）。避難・安全確保・設計・保険・金融・運用その他、人命または財産に影響する判断には使用しないでください。本内容は現状のまま無保証で提供され、利用または依拠により生じたいかなる損害についても作者は責任を負いません。

Auto-generated probabilistic forecast of an M5+ earthquake (USGS Mw>=5) within the
next 34 days, per 2-degree grid cell over Japan (lat 26-46, lon 128-148), built from
the USGS earthquake catalogue alone (free, fully reproducible).

This is operational earthquake **forecasting**, not prediction: it produces
time-varying probabilities, never deterministic alarms.

**Skill (walk-forward, 34-day embargo): pooled AUC 0.863.** Honest decomposition: a
spatial-climatology baseline (which cells are chronically active) already reaches AUC
0.854, so the genuine time-varying skill added by the ETAS clustering features is only
about +0.9 points. The actionable signal is therefore the **elevation ratio**
(forecast probability divided by that cell's own normal monthly rate): a value above 1
means the cell is temporarily elevated above its baseline, almost always following
recent activity (aftershock / cluster forecasting). Background, non-triggered large
quakes remain unpredictable -- that is the irreducible limit at a one-month horizon.

Method: per-cell magnitude-weighted ETAS intensities (multiple spatial and temporal
scales) plus background climatology, ENET + gradient-boosting ensemble,
isotonic-calibrated. Each monthly run takes a catalogue cut date, records its own
34-day window and generation date inside the JSON, and is committed shortly after:
measured against this repository, the commit has landed two to three days AFTER the
window start (2026-06 window opened 06-29 and was committed 07-01; 2026-07 opened
07-29 and was committed 08-01). The first days of each window are therefore NOT
strictly prospective, and scoring that treats a whole window as out-of-sample is
optimistic by that margin. The forecast_window and generated fields are in every file
so this can be checked rather than taken on trust.
