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
