# Round 300: the 1918-1969 record as a precedent for cells without one (global sudden axis)

Pre-registered in FREEZE_A.md (written before roundA.py; amendment 1 made after a label-free pre-run audit and before
any AUC involving P). Data: ISC-GEM Global Instrumental Earthquake Catalogue (International Seismological Centre),
retrieved 2026-10-10 via USGS FDSN `catalog=iscgem`, 1900-01-01 to 1970-01-01, M>=5.5; the round uses 1918-1969,
M>=6.25 (3,145 events). The catalogue file is not committed; the query is in FREEZE_A.md.

## Verdict: REAL (inside precedent group 0)

| | value |
|---|---|
| BASE (logistic on group-0 rows, FULL's 30 columns), mean of 25 window AUCs | 0.60035 |
| PLUS (30 columns + P) | 0.61540 |
| D = PLUS - BASE | +0.015053 |
| floor: field rotation, 61 worlds (d = 30..330) | mean +0.000392, sd 0.003087, max +0.009813 |
| rank, z | 1 of 62, z 4.75 |
| P alone, group-0 AUC | 0.62745 (floor mean 0.48715, max 0.54744) |

An independent reimplementation (verifyA.py) reproduces D to six decimals.

## Post-run audit (measured, reported with the verdict)
- Other nulls that keep the value distribution or avoid the many-to-one map: epicentres rotated (z 6.75), rotated and
  rank-matched to P (z 7.10), kernel mass matched (z 6.69), field rotation rank-matched (z 5.39); rank 1 of 62 in all.
- Not a smooth density of the modern record: the same kernel built causally from USGS 1970+ M>=6.25 (Q) adds +0.0032;
  P on top of BASE + Q adds +0.01174 (z 3.62 against the rotation floor refitted on BASE + Q); Q on top of BASE + P
  adds -0.00015.
- Concentrated: per-window D runs -0.064 to +0.152, 16 of 25 positive, the top 3 windows carry 71%; leaving any one
  window out keeps D at +0.0093 to +0.0183 above the floor maximum. About 20 of 962 group-0 cells carry 67% of D:
  dropping each world's own top 10 keeps rank 1 (z 2.97), its own top 20 gives rank 6 (z 1.09).
- Small on the whole sudden axis: splicing the group-0 model into FULL's per-window predictions gives PLUS - BASE
  +0.00087 (raw splice, rank 3 of 62) and +0.00059 (rank-preserving splice, rank 4 of 62). This is a group-0 result,
  not an improvement of the ACTIVE axis.
- FULL's group-0 figure quoted in the freeze (0.57885) is pair-weighted; on the same simple window mean as BASE it is
  0.58656.
- Leakage checks: no ISC-GEM row from 1970 on; group membership recomputed from events before each cut matches in 33
  of 33 windows.
