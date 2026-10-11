# Round 302: a century-long elapsed-time clock for cells with one or two modern precedents (global sudden axis)

Pre-registered in FREEZE_302.md, written before round302.py. Amendments 1-3 were made after gates-only runs and
amendment 4 after a pre-run code audit, all before any AUC involving the clock column was computed. Data: ISC-GEM
Global Instrumental Earthquake Catalogue (International Seismological Centre), retrieved 2026-10-10 via USGS FDSN
`catalog=iscgem` (the same file as round 300, not committed), plus the USGS 1970+ catalogue of the global arena.

## Why this round
Sizing 301 (size301.py) showed that a static per-cell column knowing the other 22 windows' labels adds only +0.002
to FULL, while knowing the test window's own label adds +0.109; sizing 301b (size301b.py) put +0.085 of that in
cells that fire two or more times, with class 1 rising 0.530 to 0.650 and class 2 0.546 to 0.704. So the room is
per window, and the only per-window clock tried before (round 260) existed only in class 3. The 1918-1969 record
gives cells in classes 1-2 an interval.

## Design (see FREEZE_302.md for the exact text)
- Clock events: isolated M>=6.25 from ISC-GEM 1918-1969 (Gardner-Knopoff isolation against all earlier ISC-GEM rows;
  1,780 events) and from the 1970+ arena catalogue (2,491); events within one Gardner-Knopoff time window are merged.
- Per window and cell, from events before the window's cut only: n, first, last, mean interval mu, elapsed time t.
  Eligible rows: classes 1-2 with n >= 2 (2,686 rows in the 25 scored windows).
- CLK = log of the Brownian passage time (a = 0.5) probability of an event in the forecast window divided by the
  Poisson probability at the same rate, computed in log space and clipped to +-20; zero for ineligible rows.
- BASE = FULL's 30 columns + RATE, LASTPRE, E1, E2; ARM = BASE + CLK. Pooled logistic regression, strictly earlier
  training windows, pair-weighted AUC per window, 25 scored windows.
- Floor PERM: 20 worlds permuting t within strata (window, class, last event before 1970, n == 2).
- REAL only if the ISO gain is positive with t > 2 and above every PERM ISO gain, and the same label replaced by
  any M>=6 (ANY) also gains above every PERM ANY gain.

## Verdict: NULL

| | ISO (isolated M>=6) | ANY (any M>=6) |
|---|---|---|
| BASE, mean of 25 window AUCs | 0.622289 | 0.651420 |
| ARM | 0.621981 | 0.651183 |
| gain | -0.000308 (t -1.53, 12 of 25 up) | -0.000236 (t -1.74, 10 of 25 up) |
| PERM 20 worlds | mean -0.000386, sd 0.000249, max -0.000027 | mean -0.000158, sd 0.000201, max +0.000149 |
| rank of the real gain among 21 | 10 | 13 |
| mean CLK coefficient | -0.0056 (positive in 3 of 25 windows) | -0.0113 |

By class (ISO, BASE to ARM): class 1 0.53329 to 0.53123 (PERM mean -0.00091, sd 0.00185), class 2 0.54539 to
0.54599 (PERM mean -0.00107, sd 0.00257), class 3 0.60236 to 0.60213, class 0 0.57875 to 0.57863; both class 1-2
changes lie inside the floor. CLK alone among eligible rows: AUC 0.52371 (descriptive). The gains sit inside the
PERM floor, so the clock is neither helpful nor harmful here, and with a negative mean coefficient the result is not
described as renewal. BASE - FULL is +0.001426 (t 2.21, 17 of 25 up); it has no floor and is descriptive only.

## Audits (measured)
- Pre-run code audit (label-free): full recount of all 20,986 (window, cell) rows from events before each cut gives
  0 mismatches in n, first, last, mu, t; CLK agrees with a 400-digit mpmath evaluation to 7.5e-7 over all 3,632
  eligible rows; 92 rows whose log1p term underflows are correctly clipped to -20 (186 rows at the low clip, raw
  minimum -153.6); the strata keys do not collide and 92.4% of eligible rows change per PERM world. Three gates
  (G2's future-event plant, G3's asymptote, G4) were found to pass by construction and were backed by the recount.
- Post-run audit: an independent rebuild (verify302.py: its own catalogue parse, isolation, clustering, closed-form
  log-space clock, fitting loop and O(n^2) AUC; labels and FULL's columns shared) reproduces every per-window BASE
  and ARM AUC with difference 0.0, the ISO floor exactly, and the ANY floor to 1.0e-5 (optimizer tolerance in one
  world). No convergence warning; CLK sd among eligible rows 4.2 to 7.0 in every scored window.

What this does not show: only this clock (a = 0.5, M 6.25, n >= 2, Gardner-Knopoff merging) was tested; it is not a
statement that elapsed time carries no information in classes 1-2.
