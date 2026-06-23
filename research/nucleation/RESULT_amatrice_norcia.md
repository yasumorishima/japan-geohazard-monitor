# Independent high-resolution b-value / Foreshock-Traffic-Light test — 2016 Amatrice-Visso-NORCIA (Mw6.5)

## Motivation
Gulia and Wiemer (2019, Nature) proposed the Foreshock-Traffic-Light System (FTLS): when the b-value of an ongoing aftershock sequence DROPS relative to background by more than ~10% (a RED light), the sequence is flagged as foreshocks of a larger impending event; a rate acceleration is the companion signal. Their two headline showcases were the 2016 central-Italy sequence (Amatrice Mw6.0 to NORCIA Mw6.5) and the 2019 Ridgecrest sequence. Dascher-Cousineau et al. (2020) and Gulia and Wiemer (2021) showed the FTLS output is highly sensitive to expert-judgment parameter choices. This program already returned prospective-temporal nulls on the b-value channel for Kumamoto 2016 (test 6) and Ridgecrest 2019 (test 19), both with independent magnitudes. This is the THIRD and final G and W showcase, and the only one of the three on which the full FTLS — including the B1 relative-drop test — is actually computable, because the high-resolution catalogue supplies a real pre-Amatrice background and a Norcia-aftershock comparison phase.

The sequence: Amatrice Mw6.0 (2016-08-24 01:36 UTC) -> Visso Mw5.9 (2016-10-26 19:18 UTC) -> NORCIA Mw6.5 (2016-10-30 06:40 UTC, target, 42.83N/13.11E). Visso struck only 3.47 d before Norcia and is itself a foreshock of Norcia; this is central to the rate analysis.

## Method (open data, reproducible)
- Catalogue: Tan et al. (2021) ML high-resolution catalogue (Zenodo 10.5281/zenodo.4736089, Amatrice_CAT5), 894,435 events, ml_median magnitudes. Magnitude of completeness Mc = 0.20 (max-curvature bin + 0.2). This is a POWERED, dense catalogue, so any null is not attributable to resolution.
- Two b estimators: (i) Aki-Utsu MLE at Mc (bin 0.1, Mc-bin/2 correction); (ii) Van der Elst (2021) b-POSITIVE = log10(e)/mean(positive successive magnitude differences), robust to the time-varying short-term aftershock incompleteness that biases Aki low. b-positive is reported Mc-consistent (events >= Mc) as primary, with all-magnitude and Mc=0.50 variants as invariance checks, plus 3000-sample bootstrap 95% CIs.
- Phases (6 h short-term-incompleteness buffer after each mainshock): pre-Amatrice background (t < Amatrice); foreshock / Amatrice-to-Norcia phase [Amatrice+6h, Norcia); Norcia-aftershock [Norcia+6h, end].
- B1 = FTLS relative-drop test (foreshock-phase b vs background, and vs aftershock). B2 = prospective moving-window b-positive toward Norcia (N=500 events, step N/4; Theil-Sen + Spearman, overlapping windows treated as anti-conservative). Operational-window check = b-positive on the short [Visso, Norcia] interval (where an operational FTLS would actually be running its RED test) and its final 1.0 / 0.5 d.
- Rate channel: A5 end-loading vs a uniform null (exact binomial); A6 Omori extrapolation fit on the Amatrice decay; A7 a two-term Amatrice+Visso Omori superposition (the physically-correct null, since Visso re-triggered the sequence) tested in-sample; and a model-free 0.5 d-binned post-Visso rate profile (no fitting).
- B3 = spatial nearest-neighbour b-positive field over post-Visso foreshocks (15,742 events), subsampled (b-positive is thinning-robust) and re-run at NB3 = 2000 and 4000, K = 100 and 200, with a magnitude-permutation resolvability control (does the spatial b-dispersion exceed a location-shuffled null?).

## Results

### B1 — relative-drop test IS computable, and the RED light is an estimator artifact
This is the only G and W showcase in this program with a real background and aftershock comparison, so the FTLS B1 can be evaluated directly:

| phase | n | Aki b | b-positive (Mc=0.20) [95% CI] |
|---|---|---|---|
| pre-Amatrice background | 621 | 1.00 +/- 0.05 | 1.26 [1.08, 1.48] |
| foreshock (Amatrice->Norcia) | 200,602 | 0.72 | 1.14 [1.12, 1.15] |
| Norcia-aftershock | 691,038 | 0.80 | 1.19 [1.19, 1.20] |

By the Aki estimator the foreshock phase b = 0.72 is 27.5% BELOW background and below the aftershock value (0.80) — a textbook FTLS RED light. But that drop does not survive the incompleteness-robust estimator: the foreshock-phase b-positive is 1.14 (Mc=0.20), a NORMAL-to-high value, not a depressed one. The Aki 0.72 is the documented low-bias of MLE under the time-varying aftershock incompleteness that dominates the foreshock phase (it is, by construction, an ongoing Amatrice aftershock sequence) — precisely the regime b-positive was designed for. The result is invariant to the completeness treatment: all-magnitude b-positive gives foreshock 0.95 vs aftershock 0.94; Mc=0.50 gives 1.13 vs 1.21. In every treatment the foreshock-phase b is normal and is NOT sharply dropped. The small residual foreshock-below-aftershock gap (e.g. 1.14 vs 1.19, formally non-overlapping CIs at this enormous n) is ~4%, an order of magnitude smaller than the spurious -27.5% Aki drop and far below an FTLS RED, and it runs at the level expected from ordinary sequence-to-sequence b scatter. The FTLS RED at Norcia is thus an artifact of the incompleteness-biased estimator.

### B2 / operational window — no prospective b-drop toward Norcia
Moving-window b-positive (N=500) over the whole Amatrice-to-Norcia phase is flat: Theil-Sen +0.0001/day, Spearman rho = 0.041 (p = 0.099, and the overlapping windows make even that anti-conservative). The non-overlapping final approach runs the wrong way for the FTLS: final 4 d b-positive = 1.01, final 1 d = 1.00.

The decisive operational check is the short [Visso, Norcia] interval — Visso Mw5.9 is itself a foreshock 3.47 d before Norcia, so this is the window in which an operational FTLS would have been running its RED test:

| window | b-positive (Mc=0.20) [95% CI] | n_pos |
|---|---|---|
| Visso -> Norcia (3.47 d) | 1.09 [1.06, 1.11] | 7,782 |
| final 1.0 d | 1.14 [1.10, 1.19] | 2,070 |
| final 0.5 d | 1.21 [1.14, 1.27] | 1,069 |

b-positive is flat-to-RISING toward Norcia (1.09 -> 1.14 -> 1.21), the opposite of a RED light. The FTLS would not have fired on the robust estimator.

### Rate channel — the apparent acceleration is the Visso aftershock sequence, and the run-up DECAYS
A naive Omori test produces a dramatic false positive that is instructive: fitting Omori to the Amatrice decay alone (A6) predicts 602 events in the final day but 4,147 are observed (Poisson p = 0.000). That "acceleration" is entirely the Visso Mw5.9 aftershock sequence, which the Amatrice-only baseline ignores. Three corrections remove it:
- **A5 (uniform null):** end-loading in the final 4 d window is +1.8% in the last 1 d (obs 4147 vs exp 4072, p = 0.088, n.s.) and +5% in the last 0.5 d (obs 2136 vs exp 2036, p = 0.009). The 0.5 d "significance" is a large-n triviality (n = 16,286) against a mis-specified null — a uniform rate ignores the Omori structure entirely.
- **A7b (two-term Amatrice+Visso Omori superposition, the correct null, fit in-sample with physical p2 = 0.20):** predicts the final day to within ratio 1.00 (predict 4141 = Amatrice 295 + Visso 3846, obs 4147, Poisson p = 0.46) and the final 0.5 d to ratio 0.94 (predict 2265, obs 2136, p = 0.997). Ordinary cascade triggering accounts for the full rate; there is no excess. (Held-out two-term extrapolations are ill-constrained on the short post-Visso window — p2 collapses to a non-decaying degeneracy — so they are not relied upon; the model-free profile below supersedes them.)
- **Model-free rate profile (no fitting):** the post-Visso event rate in 0.5 d bins toward Norcia is 5000 -> 4890 -> 4698 -> 4414 -> 4388 -> 4078 /day — monotonically DECREASING (Spearman rho = -1.000, p = 0.000), with the final day (4147) BELOW the preceding day (4416), ratio 0.94. The run-up decelerates; there is no acceleration for any in-sample fit to have absorbed.

### B3 — no spatially resolvable low-b asperity (subsample-robust)
A nearest-neighbour b-positive field over the post-Visso foreshocks places the Norcia nucleation patch at the field median, and a magnitude-permutation control shows the field is statistically flat at every setting:

| NB3 | K | Norcia-patch b+ | field median | percentile | control p(null>=obs) |
|---|---|---|---|---|---|
| 2000 | 100 | 0.83 | 0.93 | 0.24 | 1.000 |
| 2000 | 200 | 0.81 | 0.93 | 0.08 | (flat) |
| 4000 | 100 | 0.96 | 0.95 | 0.53 | 1.000 |
| 4000 | 200 | 0.94 | 0.95 | 0.49 | 1.000 |

The apparent low percentile at NB3 = 2000 (0.08-0.24) is subsample ranking-noise: at the larger, better-powered NB3 = 4000 the Norcia patch sits essentially AT the field median (percentile 0.49-0.53), and the observed field dispersion equals the location-shuffled null dispersion (p = 1.000) at both K. The patch b-positive bootstrap CI spans the field median. No low-b asperity is spatially resolvable — consistent with Kumamoto and Ridgecrest B3.

## Conclusion
On the third and final Gulia and Wiemer FTLS showcase, and the only one where the relative-drop test is computable, every channel of the foreshock-traffic-light is null under incompleteness-robust estimators and physically-correct triggering nulls: (1) the B1 RED light (Aki foreshock b 27.5% below background) is an incompleteness artifact — robust b-positive is normal (~1.14) and the operational [Visso, Norcia] window is flat-to-rising; (2) no prospective b-drop (B2); (3) the apparent rate acceleration is the Visso Mw5.9 aftershock sequence — the two-term superposition matches the final-day rate to ratio 1.00 and the run-up rate DECAYS monotonically into Norcia; (4) no spatially resolvable low-b asperity (B3, subsample-robust, control p = 1.000). On an 894,435-event Mc = 0.20 catalogue this is a powered absence, not a resolution limit. Scope: the claim is that the FTLS b-drop and rate-acceleration SIGNATURES are absent under robust estimators and correct nulls — not that the FTLS operational system (which adds expert-tuned spatial mapping and thresholds) is formally falsified; B3 here is a reduced proxy of that spatial component. This is the third independent / high-resolution null on the FTLS b-channel (n = 3 with Kumamoto M6.5->M7.3 and Ridgecrest M6.4->M7.1), all null on a prospective temporal b-precursor, corroborating the parameter-sensitivity critique of Dascher-Cousineau et al. (2020).

Assets (RPi5 ~/geo-ml/amatrice/): amatrice_analysis.py (B1/B2/A5/A6/B3), a7_visso.py (Visso-conditioned A7), amatrice_robust.py (Mc-consistent B1, operational-window, FIX1/7), rate_profile.py (model-free rate), b3_robust.py (B3 NB3=4000). Catalogue cat.csv = Tan et al. (2021) Amatrice_CAT5. Opus-reviewed (SOUND-WITH-FIXES; all seven punch-list items applied — Mc-consistent b-positive as primary, degenerate A7a-0.5d excluded, precursor-absorption answered model-free, A5 down-weighting justified, B3 re-run at larger subsample and both K, scope bounded to the robust signatures, short Visso-to-Norcia operational window added).
