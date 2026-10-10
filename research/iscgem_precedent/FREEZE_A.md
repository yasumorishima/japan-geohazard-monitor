# Round A freeze (written before the scoring script exists and before any label is read against P)

SUDDEN AXIS: global ACTIVE; FULL 0.62086 over 25 seen windows (11..35); FULL inside precedent group 0 (grp == 0: no
  isolated M>=6 in the cell since 1970 before the cut) 0.57885; group 0 holds 326 positive units of 4,173 and 15.5% of
  positive weight.
DIMENSION CEILING: WHICH CELL, static part; class-0-only reassignment bound 0.73285; per-cell map bound 0.8112.
WHY THIS ROUND below 0.90: 0.90 needs gain from class 3 and at least two of classes 0/1/2; group 0 is where every map
  built from the 1970+ record must rank cells last, and ISC-GEM 1918-1969 is the only open record that gives those
  cells a precedent without new physics.

## Column (fixed)
P = log1p( sum over ISC-GEM events 1918-01-01..1969-12-31 with M >= 6.25 of exp(-d^2 / 2), d = haversine distance in
degrees between the arena cell centre and the epicentre ) per arena cell. 3,145 events (USGS FDSN catalog=iscgem,
fetched 2026-10-10, file iscgem_pre1970_m55.csv md5 recorded by the script). Constant over windows; built entirely
before 1970, before every training and scoring row. No other threshold, kernel, year range or transform is tried.

## Primary statistic (fixed)
For each window j in 11..35: a logistic model (StandardScaler + LogisticRegression C=1.0, max_iter 2000, unweighted
rows as chk238/pre238) fitted only on group-0 rows of the windows k with hiT[k] <= tcut[j] (FULL's strict rule);
arm BASE uses FULL's 27 columns, arm PLUS uses the 27 columns + P. Score: weighted pair AUC (pre238.auc_pair with the
bundle's pw/nw) over group-0 cells of window j. D = mean over the 25 windows of AUC(PLUS) - AUC(BASE).

## Floor (fixed)
Field rotation as round 254 (pre254.rot_map): world d = 5, 10, ..., 355 degrees (71 worlds); P_d = P[rot_map(clat,
clon, d)]; D recomputed with P_d in place of P (refit per world). Gate per world: fixed points (map == identity)
<= 5% of cells; the gate is checked to fail on d = 0.

## Pass rule (fixed)
REAL if D_real > 0, D_real > max over the 71 worlds, and z = (D_real - mean floor) / sd floor >= 2. Otherwise NULL.
Reported with it: AUC(BASE) and AUC(PLUS) means; raw within-group-0 AUC of P alone, real and its 71-world floor;
FULL's group-0 reference 0.57885 is quoted, not recomputed. "N of 25 windows" is not evidence.
Not run (declared now): the ANY-label control (pre-1970 events cannot shadow 1990+ labels; largest GK time reach about
2.7 years) and a stratified shuffle floor (strata preserve rank, not level).

## Prediction written now
I expect a small positive D that does not clear the floor (P restates sm6i/sm5 with R^2 about 0.65 inside group 0),
so NULL is the more likely outcome; a REAL would mean the 1918-1969 record locates group-0 cells beyond what 1970+
smoothing says.

## Amendment 1 (2026-10-10, after a label-free pre-run audit; no AUC involving P has been computed)
- FULL has 30 per-cell columns in sudden_bundle_v1 (cnt/iso/rec x 4 thresholds, trail x 3, mmax, bval, isofrac,
  sm5/sm6i x 3, geo, abslat, n0, k60, lmp, back), not 27; BASE uses those 30 and PLUS uses 30 + P. The code already
  did this; only the text above was wrong.
- Floor worlds are d = 30, 35, ..., 330 degrees (61 worlds), as round 254 used. Reasons, measured without labels:
  d = 5 and 355 put 7.2% and 7.3% of cells on themselves (over the 5% gate), and rotations of 5-25 and 335-355
  degrees keep P correlated with itself (0.66 inside group 0 at d = 5; 0.32-0.47 over all cells at 10-20 and 350),
  so they are not null worlds. The gate is checked on all 61 worlds before any AUC involving P is computed.
- Pass rule unchanged in form: REAL if D_real > 0, D_real > max over the 61 worlds, and z >= 2, with
  z = (D_real - mean floor) / sd floor, sd with ddof = 1.
- Caveat stated before the run: rot_map is many-to-one (distinct target share 0.12-0.43), so a rotated column has fewer
  distinct values than P, and the floor sd may be smaller than for a column as fine as P; z is reported with this.
