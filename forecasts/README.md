# Operational monthly M5+ outlook (Japan)

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
isotonic-calibrated. Each monthly forecast is committed before its window opens so it
can be scored prospectively (no hindsight).
