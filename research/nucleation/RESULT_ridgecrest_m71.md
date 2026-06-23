# Onshore dense-array nucleation test #2 — 2019 Ridgecrest M7.1

## Motivation
The onshore Hi-net Kumamoto M6.5 test (case #1) returned a power-calibrated null on foreshock-rate acceleration, but was a single event. This extends the same matched-filter + injection-floor + powered end-loading methodology to the **best-instrumented foreshock sequence in the literature**, the 2019 Ridgecrest M7.1, on the dense Southern California Seismic Network (SCSN). Ridgecrest is the strongest possible second case: its foreshocks are explicitly described as a "cascade to failure" (Huang et al. 2020 EPSL), yet that study concluded that "slip acceleration as the time to failure approached could largely be ruled out," and reports that the M7.1 nucleated in a local seismicity concentration that "intensified ~3 h before" — exactly the acceleration question this test is built to adjudicate, on the most completely recorded foreshock sequence available.

## Target
2019-07-06 03:19:53 UTC M7.1, 35.7695N / 117.5993W, depth 8 km. It followed the M6.4 foreshock (2019-07-04 17:33 UTC) by ~34 h, so the 12 h pre-M7.1 window sits on the **decaying M6.4 aftershock background** — a key methodological difference from Kumamoto, addressed by an explicit Omori-conditioned null (A6).

## Method (open data, reproducible)
- SCSN broadband **HHZ velocity** (no integration; 100 Hz) via SCEDC FDSN, **17 CI stations within 68 km** (nearest CLC 5.1 km), 12 h pre-M7.1 + 1 h post.
- SeisBench PhaseNet picking; picks grouped (12 s) into matched-filter templates (M7.1 mainshock template + pre-M7.1). The pre-M7.1 window held **10,025 picks / 669 templates** (~31x Kumamoto's 320 / 16); pre-templates were uniform-in-time-stride **capped to 60** to bound the `correlate_template` memory (the uncapped 669-template run was OOM-killed at ~24 GB storing all cross-correlation arrays). **Cap-bias verified**: the 83 new MF detections are not end-concentrated (only 15 of 83 fall in the last 3 h; the burst is mid-window at h_before 6–7), so the rate/acceleration conclusion is cap-robust, and the rate curve is dominated by the 966 catalog (PhaseNet) events regardless.
- obspy `correlate_template` network CC-sum; MAD9 detection (9x MAD of the pre-MS baseline).
- **(A3)** merged catalog+MF event-rate curve. **(A5)** PRIMARY powered last-Xh end-loading test vs a uniform-reshuffle null (binomial + permutation), self-calibrated by injecting inverse-Omori accelerating cascades. **(A6)** Omori-conditioned null (fit modified-Omori on the early part of the window, predict the late part, Poisson test). **(B)** self-injection detection floor.

## Results
17 stations, span 13 h, M7.1 at +12 h (MS in span: True). P picks 10,831 (pre-M7.1 10,025, post-M7.1 797). Merged foreshock catalogue over 12 h: **catalog events = 966, new MF = 83, merged-unique = 1032** — a far denser run-up than Kumamoto (183), because it sits on the active M6.4 aftershock sequence.

### Foreshock rate — no significant end-loading (powered null, robust to baseline shape)
- **Rate is flat**: hourly counts span 78–96 across the 12 h with no trend; last-6 h 88.0/h vs preceding 84.0/h.
- **(A5) PRIMARY powered test (uniform/stationary null)**: last-3 h **obs = 267 vs exp = 258.0, binom_p = 0.269, perm_p = 0.271**; last-6 h **obs = 528 vs exp = 516.0, binom_p = 0.237, perm_p = 0.239** — not significant.
- **(A6) Omori-conditioned null** (the window sits on the decaying M6.4 aftershock background, where a stationary null is anti-conservative): the modified-Omori fit on the early window returns **p ≈ 0** (no resolvable decay 21.8–33.8 h after M6.4 — the aftershock rate is essentially flat this late), so the Omori-decaying and stationary nulls **agree**; predict-late last-3 h exp = 255 obs = 267 **Poisson p = 0.234**, last-6 h exp = 504 obs = 528 **p = 0.148** — still not significant. The powered null thus holds against **both** a stationary and a decaying baseline.
- **Power**: the last-3 h test detects an inverse-Omori accelerating cascade (final-12 h concentration) of **≥50 events with power 1.00 (≥30 → 0.70)**, but ~0 power for n ≤ 20 — the sensitivity floor is **higher than Kumamoto's** (n ≥ 20 → 0.98) because the ~6x denser M6.4-aftershock background (1032 vs 183 events) swamps small injected cascades.
- Spearman(hours_before, count) rho = −0.42, p = 0.17 (weak, non-significant increase). Cumulative N(t) quadratic t² coefficient = +0.348 (weakly convex). Both are consistent with a **slight, non-significant late uptick** — the open-data shadow of the literature's reported ~3 h intensification — below the level the powered test can resolve.

### (B) Self-injection detection floor
| dM | recovered/total |
|---|---|
| 0.0 to −3.0 | 8/8 (1.00) |
| −3.5, −4.0 | 7/8 (0.88) |
| −4.5 | 6/8 (0.75) |
| −5.0 | 2/8 (0.25) |

Self-injection floor (≥50% recovery) = **dM = −4.5** below the M7.1 template (16 ch, MAD9 = 4.251; 8 base windows, min-gap 26.5 s to nearest pick), vs Kumamoto's −5.0 — slightly shallower, consistent with the denser/noisier M6.4-aftershock background raising the floor. This is a **self-template upper bound** (the injected waveform equals the template, so CC ≈ 1), so the floor for real, geometry-mismatched micro-events is shallower.

## Conclusion
In the best-instrumented onshore foreshock sequence available, the 12 h pre-M7.1 Ridgecrest activity (1032 events) shows **no statistically significant end-loading acceleration against either a stationary or an Omori-decaying null** (last-3 h obs 267 vs exp ~255–258, p = 0.23–0.27; last-6 h p = 0.15–0.24), consistent with Kumamoto and with Huang et al. (2020)'s conclusion that time-to-failure slip acceleration is largely ruled out. A slight, non-significant late uptick (88 vs 84/h, convexity +0.348, Spearman p = 0.17) is the open-data shadow of the reported ~3 h intensification, but it is detectable only at higher injected cascade size (n ≥ 30–50) than Kumamoto's n ≥ 20, because the dense M6.4 aftershock background raises the detection floor. The self-injection floor reaches dM = −4.5 (self-template upper bound).

This is the **second onshore high-density case (n = 1 → 2)**; both Kumamoto (M6.5) and Ridgecrest (M7.1) return power-calibrated nulls on nucleation-style acceleration. Genuine further progress requires denser or restricted (borehole/near-fault) data.

Kernel: `yasunorim/nucleation-mf-ridgecrest` (knuc26, v4). Dataset: `yasunorim/scsn-ridgecrest-m71` (17 SCSN HHZ mseed, SCEDC AWS Open Data). Methodology = validated Kumamoto port. Opus-reviewed: conditional sign-off contingent on the A6 Omori-conditioned null, whose pre-stated decision rule (A6 last-3 h / last-6 h p > 0.05 ⇒ powered null stands and is strengthened) is met (p = 0.23 / 0.15).
