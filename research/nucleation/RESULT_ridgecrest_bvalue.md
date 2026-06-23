# Independent-magnitude b-value / Foreshock-Traffic-Light test — 2019 Ridgecrest M6.4 to M7.1

## Motivation
Gulia & Wiemer (2019, Nature) proposed the Foreshock-Traffic-Light System (FTLS): a sequence whose b-value DROPS relative to background is flagged as foreshocks of a larger impending event (a RED light). They featured the 2019 Ridgecrest M6.4 to M7.1 sequence as a real-time RED-light success — after the M6.4 their FTLS turned red, and the M7.1 followed about 34 h later. However, Dascher-Cousineau et al. (2020) and Gulia & Wiemer (2021) showed the Ridgecrest FTLS output is highly sensitive to expert-judgment parameter choices (the warning level is ambiguous / parameter-dependent). An earlier test in this program applied the same idea to the 2016 Kumamoto M6.5 to M7.3 sequence with INDEPENDENT network relative magnitudes and found the b-value channel retrospective-only (no prospective temporal precursor). This is the n=2 independent-magnitude reproduction, on Gulia & Wiemer's own showcase case — and a contested one.

## Method (open data, reproducible)
- SCSN broadband HHZ velocity via SCEDC FDSN (AWS Open Data), 17 CI stations, continuous window M6.4 OT (2019-07-04 17:33:49 UTC) minus 1 h to M7.1 OT (2019-07-06 03:19:53 UTC) plus 0.5 h (about 35.3 h), 100 Hz, 2-8 Hz bandpass.
- SeisBench PhaseNet picks grouped (12 s) into events with at least 4 station picks (1950 events).
- INDEPENDENT network relative magnitude per event = median over stations of [log10(peak abs 2-8 Hz velocity in [pick-0.5 s, pick+3 s]) minus the per-station median log-amplitude]. The per-station median absorbs station gain and, in this compact source zone (M6.4 and M7.1 epicentres about 15 km apart, sequence clustered), the common event-station distance term, so no event locations are used. Magnitudes are continuous, on an ML-comparable relative scale. This is valid for a temporal b-value SHAPE measurement (the per-station median removes the common distance term; about 15 km migration adds per-event scatter but does not systematically bias the slope of a roughly 400-event magnitude distribution); it is NOT used for absolute completeness mapping or for any spatial low-b claim.
- Two estimators: (i) maximum-curvature Mc + Aki-Utsu MLE (Mc* = max over 6 h bins of maxc-Mc, +0.1; first 4 h after M6.4 excluded for short-term aftershock incompleteness, STAI); (ii) Van der Elst (2021) b-POSITIVE = log10(e) / mean(positive successive magnitude differences), which needs no Mc and is robust to the transient post-mainshock incompleteness.
- B1: post-M6.4 b vs pre-M6.4 background (the FTLS RED relative-drop test). B2 / B2+: does b DECREASE toward M7.1 (the prospective foreshock signal)?

## Results
1950 events, relative-magnitude range 0 to 5.14, Mc* = 1.80. Counts: pre-M6.4 = 9, M6.4 to M7.1 = 1914, post-M7.1 = 27, at or above Mc* overall = 404.

### B1 — relative-drop test not computable (no background)
The open continuous window opens only 1 h before M6.4 (9 events), so there is no pre-M6.4 background to compute the FTLS relative drop against — exactly as in Kumamoto. (Gulia & Wiemer use a regional/decadal background; a 1 h window has none.) The post-M6.4 maximum-curvature Aki-Utsu value is b = 0.79 +/- 0.05 (n = 231 at or above Mc*), but this CANNOT anchor a RED claim: it stalls at h_before about 14 h (the final approach is too sparse above Mc* = 1.80) and it is biased low by transient post-mainshock incompleteness — precisely the bias the b-positive estimator is designed to bypass.

### B2+ — b-positive (robust), no prospective drop toward M7.1
Whole-sequence b+ = 0.91 (935 positive differences, 1914 events). The explicit, non-overlapping final-approach values are flat-to-rising, with NO drop:
| window | b+ |
|---|---|
| whole M6.4 to M7.1 | 0.91 |
| final 12 h | 0.96 |
| final 6 h | 0.97 |
| final 3 h | 0.93 |

A 60-window moving b+ (N = 120 events) trends slightly UPWARD toward M7.1 (Theil-Sen +0.0043/h, 95% CI +0.0012 to +0.0072; Spearman rho = +0.35, p = 0.006), but the 60 windows overlap heavily, so the effective degrees of freedom are few and that p is anti-conservative — we do NOT claim a significant prospective trend. The endpoint contrast runs the other way and is also small (first-150 b+ = 0.94 vs last-150 b+ = 0.89). Both directions are within b+ uncertainty (about +/-0.05), so the only robust statement is: b+ is stable near ~0.9 with no systematic decline toward M7.1. There is no foreshock b-drop.

## Conclusion
With independent network magnitudes, Ridgecrest shows **no prospective foreshock b-value drop before the M7.1**: the incompleteness-robust b-positive stays near ~0.9 through the final approach (final-12 h / 6 h / 3 h = 0.96 / 0.97 / 0.93) rather than declining, while the low maximum-curvature value (0.79) is attributable to transient post-mainshock incompleteness — so the apparent FTLS RED light is not reproduced at independent-magnitude resolution, consistent with the parameter-sensitivity reported by Dascher-Cousineau et al. (2020). Scope is the prospective TEMPORAL FTLS signal only; the spatial low-b-asperity hypothesis is untested here (it needs event locations). This is the second case (n = 2) on the b-value channel; both Kumamoto (M6.5 to M7.3) and Ridgecrest (M6.4 to M7.1) are null on a prospective temporal b-value precursor, the latter being Gulia & Wiemer's own real-time showcase.

Kernel: `yasunorim/bvalue-ftls-ridgecrest` (knuc27, v2). Dataset: `yasunorim/scsn-ridgecrest-seq` (17 SCSN HHZ mseed, 35 h, SCEDC AWS Open Data). Opus-reviewed (YES; lead with the autocorrelation-immune final-approach b+, soften the moving-window rise, frame 0.79 as an incompleteness artifact, bound scope to temporal FTLS — all applied).
