# Hi-net onshore nucleation test - 2016 Kumamoto M6.5 foreshock

## Motivation
The onshore nucleation arc returned 11 consecutive pre-registered NULLs (geodetic/catalog), and the S-net arc extended this to offshore waveform matched-filter (M7.4/M7.6/M7.1, all below-Mc NULL). Those tests were limited either by near-field coverage (onshore geodesy) or by a quiescent background (S-net). This test applies the validated matched-filter + injection-floor methodology to **onshore high-density Hi-net**, whose ~10x station density relative to S-net pushes the synthetic detection floor deeper than any prior arc test. The target deliberately has abundant foreshocks, so the question shifts from "is there any below-Mc signal" to "does the foreshock RATE accelerate toward the mainshock (nucleation cascade)?"

## Target
2016 Kumamoto M6.5 foreshock: OT 2016-04-14 21:26:35 JST, 32.788N / 130.704E, depth 9 km. The M7.3 mainshock followed ~28 h later. The M6.5 is itself a foreshock within an active sequence, so the 24.4 h run-up contains substantial catalog seismicity.

## Method (reproducible, open data)
- Hi-net continuous **velocity** waveforms (code 0101, 100 Hz), 30 h window (24.4 h pre-M6.5 + 5.5 h post), stations within 60 km of the hypocentre, via NIED Hi-net cont service. Velocity (no integration).
- SeisBench PhaseNet picking; catalog micro-events grouped into templates (M6.5 mainshock template + 15 pre-M6.5 = 16).
- obspy `correlate_template` network CC-sum matched-filter; MAD-based threshold sweep (MAD9 = 9x MAD of the pre-M6.5 baseline).
- **(A)/(A2)** new pre-M6.5 detections (not within TRIG_INT of an existing catalog pick).
- **(A3)/(A5) foreshock-rate / acceleration test**: 16 catalog-template events + new MF detections, merged and deduped, over the 24.4 h window; tested for acceleration (end-loading) toward the mainshock.
- **(B) self-injection floor**: M6.5 template scaled by 10^dM injected into 8 quiet pre-M6.5 windows; recovery fraction vs dM.

## Results
Data window: span 30.0 h, M6.5 at +24.4 h (MS in span: True). P picks: 5537 total (320 pre-M6.5, 5210 post). Templates: 16. (A first fetch truncated at 24 h by NIED throttling was re-run with max_sta 30->18 so the full 30 h including the M6.5 was retrieved.) Merged foreshock catalogue: **catalog events = 175, new MF = 8, merged-unique = 183** over 24.4 h - this is NOT a "no foreshock" case.

### Foreshock-rate acceleration - powered null (no end-loading)
The acceleration test went through three stages, because the first statistic proved underpowered:
- **(A3) initial test - Spearman(hours-before, hourly count)**: rho = -0.147, p = 0.49 (no significant trend). Hourly counts are a fluctuating swarm (bursts at h_before 23.4=23, 19.4=16, 10.4=15, 7.4=19) with no monotonic rise; last-6 h rate 5.83/h < preceding 8.04/h.
- **(A4) power check on the Spearman**: injecting a synthetic inverse-Omori accelerating cascade into the real background only reaches Spearman p=0.07 even at n=80 injected events - **the binned Spearman is underpowered**. The last-6 h / preceding rate ratio, by contrast, responds cleanly (real 0.73x vs n=80-injection 2.1x). Uniform (null) injection does not false-positive (p=0.38-0.96).
- **(A5) powered primary test - last-Xh event count vs uniform-reshuffle null** (binomial + 200k-permutation), self-calibrated with the same injection:
  - real **last 3 h: obs = 22, exp = 22.5, binom_p = 0.578, perm_p = 0.578** - exactly at the stationary expectation.
  - real **last 6 h: obs = 35, exp = 45.0 (5.83/h), binom_p = 0.967, perm_p = 0.966** - below expectation (if anything quiescent).
  - **power (last-3 h test, alpha=0.05)**: n=10 -> 0.10, **n=20 -> 0.98**, n=30 -> 1.00, n>=50 -> 1.00.

So the last-3 h test would detect an **inverse-Omori-type accelerating cascade (final-12 h concentration, p~1) of >=20 events with power >=0.98**, yet the real run-up shows no end-loading. (The calibration is for this single acceleration shape; it is not a power statement for arbitrary nucleation signals.) The cumulative N(t) quadratic t^2 coefficient is weakly positive (+0.0218), but it arises from the **early-window** clusters (23.4/19.4 h before), not end-loading, so it is not evidence of acceleration.

### (B) Self-injection detection floor
| dM | recovered/total |
|---|---|
| 0.0 to -4.5 | 8/8 (1.00) |
| -5.0 | 5/8 (0.62) |

Self-injection floor (>=50% recovery) = **dM = -5.0** below the M6.5 template (Hi-net dense onshore) - the deepest open-data injection floor in the arc (S-net offshore reached dM-1.5). This is a self-template upper bound (the injected waveform is identical to the template, so CC is near 1), so the floor for real, geometry-mismatched micro-events is shallower.

## Conclusion
In the 24.4 h before the 2016 Kumamoto M6.5, 175 catalog foreshocks are present (not a "no foreshock" case), but their **rate shows no nucleation-style acceleration (end-loading) toward the mainshock**. The powered primary test (last-3 h / last-6 h event count vs uniform-reshuffle null) puts the run-up at or below the stationary expectation (last-3 h obs=22 vs exp=22.5, p=0.58; last-6 h obs=35 vs exp=45, p=0.97), and the test is calibrated to detect an inverse-Omori-type accelerating cascade (final-12 h, p~1) of >=20 events with power >=0.98 - a **power-calibrated null** rather than a null-of-no-power. (Calibration is for a single acceleration shape, not arbitrary nucleation signals.) The Spearman trend (A3) was demoted to secondary after (A4) showed it underpowered, and the weak cumulative convexity is early-window, not end-loading. The self-injection floor reaches dM = -5.0 (~ML 1-2 equivalent under uniform amplitude scaling; distance attenuation and corner-frequency differences uncorrected, so the real-event floor is shallower).

**Complementarity**: this case ("no rate acceleration within an active foreshock swarm") and the S-net cases ("no cascade emerging from a quiescent background") are complementary regimes that jointly support the absence of a detectable nucleation-acceleration signal at open-data resolution. Following the onshore-geodesy 11-NULL and the offshore-waveform NULLs, this is the deepest open-data onshore-waveform nucleation test. **Single-event (2016 Kumamoto) result**; generalization requires more cases / restricted (denser or borehole) data.

Kernel: `yasunorim/nucleation-mf-kumamoto` (knuc25, v6). Dataset: `yasunorim/kumamoto-hinet` (1518 SAC). Reviewed by Opus (sign-off; methodology consult + final sign-off).
