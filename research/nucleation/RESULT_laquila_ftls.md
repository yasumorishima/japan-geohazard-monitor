# Independent b-value / Foreshock-Traffic-Light + acceleration test — 2009 L'Aquila Mw6.3 (the canonical natural foreshock case)

## Motivation
The 2009 L'Aquila Mw6.3 (central Italy) is the most-cited NATURAL foreshock sequence in seismology and the basis of the 2012 manslaughter trial of the Italian hazard commission; its weeks-long escalating foreshock swarm is the textbook example invoked for foreshock-based precursors. It is NOT a Gulia and Wiemer (2019, Nature) FTLS showcase, so this is a genuinely out-of-sample test of whether the FTLS b-drop and the famous L'Aquila rate acceleration survive an incompleteness-robust, prospective re-analysis on open data. Three prior tests in this arc (Kumamoto 2016, Ridgecrest 2019, Amatrice-Norcia 2016) found the FTLS b-channel null under the b-positive estimator. This is the fourth b-channel case and the first applied to a non-showcase, naturally-occurring foreshock sequence.

Mainshock MS = 2009-04-06 01:32:39 UTC, 42.342N 13.380E. Largest early foreshock FS1 = M~3.9-4.3, 2009-03-30 13:38 UTC (6.5 d before MS); a second M3.9 foreshock struck 2009-04-05 20:48 UTC (~4.7 h before MS).

## Method and the two-catalogue escalation (open data)
Estimators: Aki-Utsu MLE and the incompleteness-robust b-positive (Van der Elst 2021, b+ = log10(e)/mean of positive successive magnitude differences), with 3000-sample bootstrap 95% CIs. A b+ window is only reported when it has >= 25 positive differences (npos).

- **Catalogue 1 (sparse):** the ML high-resolution DD catalogue (Zenodo 10.5281/zenodo.16535092, 190,872 events, 2009). It is AFTERSHOCK-dominated: pre-mainshock Mc = 1.20 (post-MS 0.10) and only 506 of 190,872 events precede the mainshock; the Mw6.3 mainshock itself is clipped/absent. On it the acute foreshock b+ was point-low (0.90) but with only 192 events (npos 96) the CI [0.68,1.30] overlapped both the background (1.25) and the aftershock (1.10), the final-approach b was unmeasurable (npos < 25), and the rate did not accelerate. The result was SUGGESTIVE-BUT-UNRESOLVED — a data limitation, not a physics result.
- **Catalogue 2 (dense, used for all results below):** the foreshock-specific template-matched catalogue of Cabrera et al. (2022, JGR; Zenodo 10.5281/zenodo.4776701), 4,978 events 2009-01-03 to the mainshock, pre-MS Mc = 0.80, ~10x denser foreshock coverage than catalogue 1. It resolves the foreshock b and rate. Magnitude bin width 0.1; successive-difference tie fraction 0.134 (ties are dropped by b+ identically in every phase, so the small low-bias is common-mode and cancels in phase contrasts). This catalogue is foreshock-only (it ends at the mainshock), so the within-catalogue early-swarm phase is the primary baseline; the ML aftershock b+ (~1.10) is a cross-catalogue cross-check.

Phases: early swarm [catalogue start, FS1); acute foreshock [FS1, MS); acute FS1-EXCLUDED [FS1+1d, MS) (removes the FS1 aftershock burst, to separate precursory nucleation from ordinary triggering). Mc = 0.5 primary, 0.7 robustness.

## Results (dense Cabrera catalogue)

### B1 + FS1-decomposition — no foreshock b-drop (the sparse-catalogue drop was an artifact)
| phase | n (Mc0.5) | Aki | b+ (Mc0.5) [95% CI] | b+ (Mc0.7) [95% CI] |
|---|---|---|---|---|
| early swarm [start, FS1) | 2006 | 1.02 | 1.04 [0.99, 1.10] | 1.05 [0.99, 1.13] |
| acute [FS1, MS) | 1762 | 0.96 | 1.11 [1.04, 1.19] | 1.10 [1.01, 1.21] |
| acute FS1-EXCLUDED [FS1+1d, MS) | 780 | 1.05 | 1.01 [0.91, 1.14] | 0.99 [0.87, 1.13] |

The foreshock b-positive is NORMAL (~1.0-1.11) in every phase, with no drop relative to the early-swarm baseline (1.04) or the ML aftershock reference (~1.10). Removing the FS1 aftershock burst leaves b+ ~1.0 — so the acute foreshock b is not depressed even before triggering is subtracted. The sparse-catalogue apparent drop to 0.90 was an artifact of its high pre-MS completeness (Mc 1.20) and small sample; at proper completeness (Mc 0.5-0.7) the foreshock b is normal under both estimators. No FTLS RED.

### B2 — flat prospective b; the final-approach point-decline is the 04-05 triggered burst, unresolved
A NON-overlapping moving b+ (N=120 events/window, 31 windows over the full run-up) is FLAT: Theil-Sen -0.0001/day, Spearman rho = 0.015, p = 0.937. (The sparse-catalogue moving decline rho = -0.799 was an autocorrelation + background-to-acute step artifact; it vanishes with non-overlapping windows.) The non-overlapping final-window b+ does show a point-wise decline toward the mainshock — final 5 d / 3 d / 1 d / 0.5 d / 6 h = 0.96 / 0.91 / 0.79 / 0.76 / 0.73 — but every CI is wide and overlapping (final 6 h b+ 0.73, CI [0.49, 1.24], npos 28), so it is statistically UNRESOLVED. It is also confounded: 71 of the 79 final-0.5 d events fall AFTER the 2009-04-05 20:48 M3.9 foreshock, and the b+ of that M3.9-triggered burst alone is 0.87 (npos 27). The final-6 h low b is therefore the ordinary b of a fresh triggered Omori burst, not a resolved precursory nucleation drop; the open data cannot distinguish a genuine last-hours drop from sampling noise on that burst even at Mc 0.5.

### RATE — triggered Omori bursts, overall decay, no smooth acceleration
The acute-window rate (Mc 0.5, 1762 events, 0.5 d bins toward MS) is a sequence of discrete triggered bursts, each Omori-decaying: a large FS1 burst at -6.5 d (738 events day 1), a secondary burst near -3 d, and the M3.9-triggered burst in the final ~5 h. The overall trend is DECAY (Spearman rate-vs-time-toward-MS rho = -0.729, p = 0.005), driven by the early FS1 burst. We do not test against a uniform-rate null because the process is manifestly Omori-clustered (a uniform null is mis-specified and a deficit against it is the expected Omori signature, not evidence). The model-free profile plus burst attribution is the honest statement: there is no smooth power-law acceleration toward the mainshock over and above ETAS/Omori cascade triggering; the final-hours elevation is the 04-05 M3.9 aftershock sequence (71/79 final-0.5 d events post-date it).

### B3 — no low-b nucleation asperity (foreshock field, K-curve with control)
A nearest-neighbour foreshock b-positive field (1762 acute foreshocks) with a magnitude-permutation resolvability control, across K:
| K | MS-patch b+ | field median | patch percentile | control p(null>=obs) | field |
|---|---|---|---|---|---|
| 80 | 1.21 | 1.23 | 0.48 | 0.000 | RESOLVED |
| 110 | 1.20 | 1.15 | 0.57 | 0.812 | unresolved |
| 150 | 0.98 | 1.24 | 0.23 | 0.500 | unresolved |

At the only scale where the spatial b-field is statistically resolved (K = 80, control p < 0.001), the mainshock nucleation patch sits at the field MEDIAN (percentile 0.48) — not a low-b asperity. At larger K the field is unresolved (control p = 0.5-0.8), so the lower patch percentile there (0.23 at K = 150) is ranking noise. There is no resolved low-b nucleation asperity.

## Conclusion
On the densest open foreshock catalogue, the 2009 L'Aquila Mw6.3 — the canonical natural foreshock sequence — shows NO robust FTLS precursor: the foreshock b-value is normal under both estimators (b+ ~1.0-1.1, no drop, and no drop even with the FS1 burst removed); the prospective b is flat (non-overlapping moving rho = 0.015, p = 0.937); the seismicity rate is a chain of triggered Omori bursts with an overall DECAY, not a smooth acceleration; and there is no resolved low-b nucleation asperity (the foreshock field is spatially resolved at K = 80, yet the nucleation patch sits at the field median). The single precursor-consistent remnant — a point-wise b+ decline to ~0.73 in the final 6 h — is statistically unresolved (CI [0.49, 1.24], npos 28) and is the ordinary b of the 2009-04-05 M3.9-triggered burst (b+ 0.87), not a distinguishable nucleation signal.

Crucially, the denser data flipped the verdict: on the sparse aftershock-dominated catalogue the foreshock b looked low (0.90, unresolved), but the foreshock-specific template-matched catalogue (10x denser, Mc 0.80) shows that low value was a high-completeness/small-sample artifact and the true foreshock b is normal. This is the "pursue denser data, do not punt" outcome — the unresolved sparse result is replaced by a clean, honestly-bounded null on the dense data.

Fourth case on the FTLS b-value channel (Kumamoto M6.5->M7.3, Ridgecrest M6.4->M7.1, Amatrice-Visso-Norcia Mw6.5, L'Aquila Mw6.3), all null on a prospective temporal b-precursor under incompleteness-robust estimators, and L'Aquila extends the result from Gulia and Wiemer's own showcases to the most-cited NATURAL foreshock case. Scope: this is a case series of notable/selected mainshocks, not a population test of FTLS skill, and the claim is specifically that the b-drop and rate-acceleration SIGNATURES are absent under b-positive and Omori-aware nulls — not that the FTLS operational system is formally falsified. Windowing follows this arc's foreshock/aftershock-phase convention rather than Gulia and Wiemer's exact regional-background recipe (which open data near the mainshock cannot reproduce), so the test is method-matched to the temporal b-drop and acceleration claims.

Assets (RPi5 ~/geo-ml/laquila/): laquila_analysis.py (sparse ML catalogue, B1/B2/rate/B3), laquila_cabrera.py (dense Cabrera catalogue B1/FS1-decomp/B2/rate/B3), laquila_cab_fix.py (B3 K-curve control + final-burst attribution). Catalogues: cat190.csv (Zenodo 16535092), cabrera.txt (Zenodo 4776701). Opus-reviewed twice (sparse: SOUND-WITH-FIXES, required denser data + FS1-decomposition; dense: SOUND-WITH-FIXES, required the B3 K-curve control and dropping the mis-specified uniform end-load test -- both applied).
