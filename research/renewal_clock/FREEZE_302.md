# FREEZE round 302 -- century renewal clock in classes 1-2 (written before the script; no CLK/arm AUC read)

SUDDEN AXIS: global ACTIVE; FULL 0.62086 over 25 seen windows (arm 0.5785 over 33).
DIMENSION CEILING: WHICH CELL, PER WINDOW; per-window cell oracle 0.95017.  Sizing 301: a static per-cell column
  that knows the other 22 windows' labels adds +0.002 to FULL, so static maps cannot carry the axis; the self-
  knowledge lift (+0.109; +0.085 from cells firing >= 2 times) is knowledge of the test window's own label.
  No bound has been measured for functions of (elapsed time, interval); this round does not claim one.
WHY THIS ROUND: the remaining room is per-window and largest in classes 1-2 (self-knowledge lifts class 1
  0.530->0.650, class 2 0.546->0.704).  The only per-window clock tested so far (round 260) existed only in class 3
  because it needed 3 isolated M>=6 since 1970.  The ISC-GEM 1918-1969 record gives classes 1-2 an interval.  0.90
  needs class 3 plus two of classes 0-2 (round 244).  Class 0 is excluded: plan audit showed its clock is static
  (100% of eligible class-0 rows have their last event before 1970; t/mu median 5.5; CLK ~ log2) = round 300's ground.

Plan audit (opus, 2026-10-11, label-free, `audit_plan/aud302.log`) changes adopted: log-space BPT (sf underflow
verified: sf(t)=0 at mu 4.7 d, t/mu 4500, true log ratio -7.66 not -20; the audit's "+log2" claim was checked and
rejected for mu 0.1 d, where the true value is -360), clustering of clock events, base controls RATE/LASTPRE/ELIG,
stratified PERM, floors on the ANY control, independent recount gate, calendar 1918-01-01, origin gate.

## Inputs
ISC-GEM `~/claude-scratch/roundA/iscgem_pre1970_m55.csv` md5 21616b17... (prefix asserted in full in code: see
script), 12,072 rows, all before 1970-01-01.  meta197 (1970+ events, keep_iso).  Time unit = days; origin
ORIGIN = 719163.0720601851 (proleptic ordinal of the first global_m45.csv event; asserted from the file).

## Clock events (homogeneous M>=6.25)
(a) ISC-GEM rows dated >= 1918-01-01, M >= 6.25, GK-isolated (size196.gk_isolated) against all earlier ISC-GEM rows.
(b) meta197 keep_iso & dmag >= 6.25.  1970 seam: (b) keeps meta197's isolation (ISC-GEM parents not used; the
    plan audit counted 10 of 2,481 such events affected) -- declared, not corrected.
Cell: build197 floor-lat/lon gid against meta197 `cells`.
Clustering, per cell, events in time order: an event joins the current cluster if (event time - cluster start)
<= GK time window of the cluster's largest magnitude so far (size196 formula); otherwise it starts a new cluster.
Cluster time = its start.  A cluster's membership depends only on earlier events, so truncating at tcut is exact.

## Per window j (tcut = chk238 tcuts[j]; s0 = tdays[lo]-tcut, e = tdays[hi-1]+34-tcut)
n = clusters with start < tcut; first, last their starts; mu = (last-first)/(n-1); t = tcut - last.
ELIG = grp in {1,2} and n >= 2.  For ELIG rows:
  RATE = log(n / (tcut - first)); LASTPRE = 1 if last < 1970-01-01; E1 = 1(grp==1); E2 = 1(grp==2);
  CLK = clip(log P_bpt - log P_poi, -20, 20), a = 0.5, computed from logsf (scipy invgauss, mu=a^2, scale=mu/a^2):
    log P_bpt = logsf(t+s0) - logsf(t) + log1p(-exp(logsf(t+e) - logsf(t+s0)));
    log P_poi = -s0/mu + log1p(-exp(-(e-s0)/mu)).
All five are 0 for non-ELIG rows.

## Arms (pooled logistic, StandardScaler, C=1, max_iter 2000, strict training k<j and hiT[k] <= tcut[j])
BASE = FULL 30 + [RATE, LASTPRE, E1, E2];  ARM = BASE + CLK;  PERM_w = BASE + CLK with permuted t.
Scored on the 25 windows 11..35 with ISO labels (isolated M>=6) and ANY labels (any M>=6), ctl264 masses.

## Floor PERM: 20 worlds (seeds 1000..1019)
Within each window, t is permuted among ELIG rows of the same stratum = (class, LASTPRE, n==2); mu kept.

## Declared reading
R2: ISO gain (ARM - BASE, 25-window mean) > 0, paired t > 2, and > every PERM_w ISO gain.
R3: ANY gain > 0 and > every PERM_w ANY gain.
VERDICT REAL only if R2 and R3; otherwise NULL.  Reported whatever it reads: class 1/2/3/0 AUCs of BASE and ARM
(ISO), BASE - FULL, mean standardized CLK coefficient (the word "renewal" is used only if it is > 0), fraction of
ELIG rows in strata of size 1, column AUC of CLK among ELIG rows (descriptive, not a reading).

## Gates (assert; each can fail)
G0 ISC-GEM md5 and row count; every ISC-GEM time < 1970-01-01; global_m45.csv origin == ORIGIN; post-1970 events
   from meta197 only.
G1 FULL (30 columns, ISO) reproduces 0.62086354 to 1e-8.
G2 independent recount: for 40 random (window, cell) pairs, n/first/last/mu/t recomputed from the unfiltered raw
   event arrays by a separate loop (clusters built on events < tcut only) equal the vectorized values; a planted
   event at tcut+1 added to the raw arrays must leave the recount unchanged AND the vectorized path must be shown to
   change if the planted event is placed at tcut-1 (both directions can fail).
G3 BPT: closed-form cdf vs scipy at 6 points (1e-9), mean == mu; planted (mu 4.7, t/mu 4500) gives -7.66 +- 0.01
   against the exponential-hazard asymptote (h = 1/(2 a^2 mu)); every real ELIG row with sf(t) > 1e-200 agrees with
   round 260's linear-space formula to 1e-9; every ELIG row with t/mu > 1000 agrees with the asymptote to 0.01.
G4 ARM with CLK replaced by zeros reproduces BASE (ISO) to 1e-9.
G5 R3 can fail: the round-263 SHAD column (ctl264 K3) appended to BASE gives ANY gain <= 0.
G6 every scored window has ELIG positive and negative mass in class 1 or 2; PERM strata of size 1 hold <= 10% of
   ELIG rows (checked before any CLK AUC).

## Not claimed
All 25 windows are seen; REAL is exploratory until an unseen window (2027-05-05+ fetch) or a second arena scores it.
Nothing enters the operating forecast.  a = 0.5, M 6.25, n >= 2, clip 20 are not swept.

## Amendment 1 (2026-10-11, after the gates-only run stopped at G3; before any CLK or arm AUC was computed)
The gates-only run (gates302.log) failed G3 "lin" in window 5.  Measured: over 3,632 ELIG rows with sf(t) > 1e-200
the log-space column and the round-260 linear-space formula differ by at most 1.67e-6 (21 rows > 1e-9, 2 > 1e-6,
none > 1e-4), at small t/mu where sf ~ 1 and the linear formula subtracts two numbers near 1.  A 60-digit mpmath
evaluation of two such rows agrees with the log-space value to 1.2e-11 / 1.7e-12 and with the linear value to
2.7e-11 / 5.7e-11, so the linear formula is the less accurate one.  Changes: the G3 "lin" tolerance becomes 1e-5
(measured, not inherited); new gate G3b: 20 random ELIG rows (seed 3021) agree with a 60-digit mpmath evaluation of
the same clipped quantity to 1e-8.  No other change.  Clock events: ISC-GEM 1,780 + 1970+ 2,491 (printed by the run).

## Amendment 2 (2026-10-11, second gates-only run stopped at G2; still before any CLK or arm AUC)
G2's planted past event at tcut-1 (M7.0) does not change the state when tcut-1 falls inside the GK window of the
cell's last cluster -- it joins that cluster, which is the declared clustering rule working, so the gate as written
could fail on correct code.  Change: the planted past event is placed 3,000 days before the cell's first cluster
start with M 6.3 (GK window 726 days < 3,000), and the recount must give exactly n+1 clusters with the same last
start and t.  The planted future event (tcut+1, must leave everything unchanged) is unchanged.  No other change.

## Amendment 3 (2026-10-11, third gates-only run stopped at G5 on a missing file; still before any CLK or arm AUC)
The round-263 SHAD file named by ctl264 (`~/claude-scratch/audit263post/R.npz`) no longer exists.  SHAD was rebuilt
by `shad263.py` with round 263's field() logic; FULL+SHAD reproduces `~/geo-ml/out263.npz["shad"]` per window with
max difference 0.00e+00 (`shad263.log`).  G5 reads `~/claude-scratch/roundA2/shad263.npz` instead; the gate itself
(SHAD appended to BASE must give ANY gain <= 0) is unchanged.  No other change.

## Amendment 4 (2026-10-11, after the opus pre-run code audit; still before any CLK or arm AUC; no code change)
Recorded findings, measured in `audit_code/a1..a4` (no label-based metric on CLK/ARM/PERM was computed):
(a) CLK accuracy over all 3,632 ELIG rows (33 windows) against a 400-digit mpmath evaluation of the same clipped
quantity: max |error| 7.53e-7 (j6 i453), not the 1e-8 that G3b checks on its 20 sampled rows.  Cause: log1p(-exp(d))
with d near 0.  Column sd is about 5, so the frozen formula is kept unchanged.
(b) The log1p divide-by-zero warning comes from 92 ELIG rows where exp(lb-la) rounds to 1.0; they become -inf and
clip to -20.  mpmath gives raw values below -20 for every one of them (186 rows at the low clip; raw minimum -153.6),
so the stored -20 is the correct clipped value.  CLK has no NaN or inf, and neither do the 20 PERM worlds.
(c) Gate strength: G3 "asym" is vacuous on real data (t/mu <= 25.1, mu >= 928.8 d); the G2 future-event plant passes
by construction (recount masks et < tc); G4 (zero column) cannot detect row misalignment.  Supplementary checks run
instead: full recount of all 20,986 (window, cell) rows from events < tcut only gives 0 mismatches in n, first, last,
mu and t; there are 0 tie-sensitive rows; the abslat column equals |clat[EL]| in 33 of 33 windows; and the largest
hiT[k] + 34 - tcut[j] over strict pairs is -299 d.
(d) 55 ISC-GEM events sit exactly at M 6.25 and are included by >=, as the contract states.  The only change is this
text and the FREEZE_MD5 constant in round302.py.
