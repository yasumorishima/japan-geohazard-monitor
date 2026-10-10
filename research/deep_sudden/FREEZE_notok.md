# Frozen contract: feature-only network on the sudden (ACTIVE) axis

Frozen 2026-10-10, before any window beyond the bundle has been scored by this learner.

- Code: research/deep_sudden/train.py at commit f824698 (not edited for scoring; a new window arrives as a new bundle).
- Arguments: --no-tokens --ntok 128 --epochs 8 --seeds 3, eight RNG streams --seed-offset 0..7; score = mean over
  the streams.
- Training: strictly past windows (hiT[k] <= tcut[j]), FULL's 27 per-cell features, BCE with pos_weight, AdamW 1e-3.
- Score: weighted pair AUC of chk238/pre238 per window, compared with FULL refit on the same windows.
- Seen-window record (exploratory; all 25 windows were seen by earlier rounds): run 37979365292, NOTOK-FULL +0.00636
  (window t 3.48, 18/25, across-stream sd 0.00097); within precedent group 0 +0.0245 (t 2.56).
  Token floor run 37960270592: the event-token channel adds nothing (real model 5th of 9 shuffles, t 0.01).
- Prospective claim, fixed now: on windows not in sudden_bundle_v1 the mean NOTOK-FULL is positive.  Each new window is
  reported as it lands; no verdict before at least 5 new windows, and no setting is changed in between.

## Amendment 2026-10-10 (recording rule; written before any further window is scored)

- A window is recorded once, from a catalogue fetched at least 90 days after the window is complete (window end plus
  the 34-day horizon). For window 36 (calendar 2026) that is a fetch on or after 2027-05-05; with yearly windows the
  fifth recorded window is window 40 (calendar 2030), recordable from 2031-05-05.
- Any earlier build of a window is monitoring only: it is listed in PROSPECTIVE.csv with its catalogue fetch date and
  bundle md5, and is never used for the verdict.
- Reclassified: the window-36 score of update 186 (sudden_bundle_v2, catalogue fetched 2026-09-02, rows 2026-01-01 to
  2026-07-30) is a monitoring record, not one of the five.
- A rebuild of window 36 from a catalogue fetched 2026-10-10 (sudden_bundle_v3) was made only to test the rebuild
  mechanism and is not scored; scoring it after seeing the v2 result would leave a choice between two versions.
- The Japan and New Zealand arenas do not supply windows for this claim: their years are the seen years of the global
  arena, and the 27 features cannot be built identically there.
- Why 90 days: between catalogue fetches of 2026-09-02 and 2026-10-09, no driving event of January-June 2026 changed,
  while July and August events (one to two months old at the first fetch) changed magnitude, cell or presence 60 times;
  90 days keeps the last label-bearing events older than anything observed to move.
- Correction of wording, no change of code: FULL has 30 per-cell columns in the bundle (cnt/iso/rec x 4 thresholds,
  trail x 3, mmax, bval, isofrac, sm5/sm6i x 3, geo, abslat, n0, k60, lmp, back), not 27 as written above; train.py
  has always read all 30.
