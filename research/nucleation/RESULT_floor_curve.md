# Thirteenth test — Multi-case SSE detection-floor vs network curve (2026-06-15)

**Question.** Generalize the single-case detection-floor results (Kumamoto/Iquique GNSS; 2018 Boso OBP) into a curve across tectonic settings and observing networks: what sets whether an open geodetic pipeline detects a documented slow-slip event — noise, network geometry, or source location?

**Method (reproducible, adversarially hardened).** One matched-filter harness applied to documented SSEs, all using NGL 5-minute kinematic GNSS (`IGS20/kenv`, open) daily-medianed, plus the 2018 Boso seafloor OBP from the twelfth test. Per case: stations auto-selected from the global NGL holdings within a region box and spanning the analysis years; linear detrend with the SSE window masked from the trend fit; per-epoch network common-mode (regional mean) removed; matched filter = projection of the horizontal field onto the unit-slip Okada(1985) surface pattern of the documented fault geometry (output in slip-equivalent metres); step statistic with a window matched to each event's duration (Cascadia/Hikurangi 14 d, Boso 45 d, Guerrero 150 d). Detection = two-sided 95th-percentile of |step| against baseline windows (SSE/control excluded), cross-checked with a 1.96-sigma analytic floor (matched-filter output is slip-equivalent, so its baseline std is the noise floor) and a clean synthetic-injection ROC at a quiet control epoch. Mw via M0 = mu*A*slip (mu=3e10, A = fault L*W).

**Cases & results.**

| Network (region, event) | N sta | area km² | noise std (m, slip-eq) | analytic floor Mw (1.96σ) | injection floor Mw (≥95%) | documented Mw | realized detection | verdict |
|---|---|---|---|---|---|---|---|---|
| Boso onshore GEONET (2018 SSE) | 19 | 1600 | 0.0011 | 5.31 | 6.07 | 6.5 | pct 100 / SNR 57.8 | **detected** |
| Cascadia PBO (2012 ETS) | 34 | 3200 | 0.0077 | 6.07 | 6.42 | 6.7 | pct 96.9 / SNR 2.5 | **detected** |
| Guerrero sparse (2009–10 SSE) | 6 | 12000 | 0.0070 | 6.43 | unstable* | 7.5 | pct 88.1 / SNR 3.5 | marginal |
| Hikurangi GeoNet (2014 Gisborne) | 28 | 2400 | 0.0083 | 6.01 | 6.07 | 7.0 | pct 10.4 / SNR 0.2 | **not detected** |
| Boso seafloor OBP (2018 SSE) | 4 | 1600 | 0.0361 (vertical) | 6.32 | — | 6.5 | pct 89 | marginal |

\*Guerrero injection ROC is unstable: a 150-day window over a 4-year record with recurring SSEs leaves too few clean baseline windows; the analytic floor and observed SNR are the meaningful statistics there. Only ACYA (Acapulco) of the 6 NGL-holding stations spanning 2008–2011 is near-field; the dense UNAM/TLALOCNet near-field is not in the open NGL holdings.

**Findings.**
1. **The noise-limited analytic floor is Mw ~5.3–6.4** after common-mode removal, lowest for dense low-noise onshore networks (Boso onshore 5.3) and highest for the sparse Guerrero geometry (6.4).
2. **Realized detection decouples from the noise floor and is governed by near-field coverage of the slip patch.** Boso onshore (dense network directly flanking the slip) detects its Mw6.5 SSE at SNR 58; Cascadia (dense, deep slip under the network) at SNR 2.5; Guerrero (one near-field station) is marginal at SNR 3.5.
3. **Hikurangi is the decisive case:** the 2014 Gisborne Mw7.0 SSE is *not* detected (pct 10 / SNR 0.2) even though this network has the second-lowest noise floor (Mw6.0). The slip is shallow and offshore, so no onshore station overlies it — a low noise floor is necessary but not sufficient.

**Conclusion.** SSE detection requires BOTH a low noise floor AND near-field coverage of the slip. Offshore shallow sources defeat onshore GNSS regardless of its noise floor (Hikurangi), which is the quantitative case for seafloor geodesy — but seafloor OBP carries its own ocean-noise floor (~Mw6.3, twelfth test). This unifies the onshore-GNSS nucleation arc and the seafloor-OBP arc into one network-design curve, and corrects the earlier "noise environment, not offshore-ness" framing: offshore-ness matters precisely *through* near-field coverage. Reaching the ~Mw6 inferred-precursor scale for an offshore source needs dense seafloor instrumentation AND ocean-noise reduction (regional assimilative model), neither available in open form.

Harness: RPi5 `~/geo-ml/floor_curve.py` (+ `floor_plot.py`); data `~/geo-ml/fc_{cascadia,guerrero,hikurangi,boso}/`. Research only; not productized.
