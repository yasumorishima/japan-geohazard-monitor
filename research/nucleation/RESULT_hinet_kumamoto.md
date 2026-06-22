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
- **(A3) foreshock rate time-series**: 16 catalog-template events + new MF detections, merged and deduped (within TRIG_INT), binned hourly over the 24.4 h window; tested for acceleration toward the mainshock (Spearman of hours-before vs count; last-6 h vs preceding rate).
- **(B) self-injection floor**: M6.5 template scaled by 10^dM injected into 8 quiet pre-M6.5 windows; recovery fraction vs dM.

## Results
Data window: span 30.0 h, M6.5 at +24.4 h (MS in span: True). P picks: 5537 total (320 pre-M6.5, 5210 post). Templates: 16. (A first fetch truncated at 24 h by NIED throttling was re-run with max_sta 30->18 so the full 30 h including the M6.5 was retrieved.)

### (A3) Foreshock rate - no acceleration toward the mainshock
- catalog events = **175**, new MF = 8, merged-unique = **183** over 24.4 h.
- Hourly count is a fluctuating swarm (bursts at h_before 23.4=23, 19.4=16, 10.4=15, 7.4=19) with **no monotonic rise**.
- **rate last 6 h = 5.83 /h < preceding 18.4 h = 8.04 /h** (the run-up is, if anything, slightly quieter near the mainshock).
- **Spearman(hours_before, count): rho = -0.147, p = 0.49** -> no significant trend.

### (B) Self-injection detection floor
| dM | recovered/total |
|---|---|
| 0.0 to -4.5 | 8/8 (1.00) |
| -5.0 | 5/8 (0.62) |

Self-injection floor (>=50% recovery) = **dM = -5.0** below the M6.5 template (Hi-net dense onshore) - the deepest open-data injection floor in the arc (S-net offshore reached dM-1.5).

## Conclusion
In the 24.4 h before the 2016 Kumamoto M6.5, 175 catalog foreshocks are present (this is NOT a "no foreshock" case). The merged foreshock rate (183 events) shows **no significant acceleration toward the mainshock** (Spearman p=0.49; last-6 h rate below the preceding rate) - the foreshocks remain a stationary swarm rather than an accelerating nucleation cascade. The self-injection floor reaches dM = -5.0 (~ML 1-2 equivalent under uniform amplitude scaling; distance attenuation and corner-frequency differences uncorrected, so the floor for real micro-events is shallower; the rate-acceleration null applies to events at or above this dM = -5.0 floor).

**Complementarity**: this case ("no rate acceleration within an active foreshock swarm") and the S-net cases ("no cascade emerging from a quiescent background") are complementary regimes that jointly support the absence of a detectable nucleation-acceleration signal. Following the onshore-geodesy 11-NULL and the offshore-waveform NULLs, this is the deepest open-data onshore-waveform nucleation test, and it confirms the absence of pre-M6.5 foreshock-rate acceleration. **Single-event (2016 Kumamoto) result**; generalization requires more cases / restricted (denser or borehole) data.

Kernel: `yasunorim/nucleation-mf-kumamoto` (knuc25). Dataset: `yasunorim/kumamoto-hinet` (1518 SAC). Reviewed by Opus (sign-off).