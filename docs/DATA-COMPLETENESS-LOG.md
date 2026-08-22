# Data completeness initiative -- step log

Moved out of the README on 2026-08-23, unchanged.  Every step below is the text that was published, byte for byte.  The README keeps the project; this file keeps the record.

## Index

- [Phase 1 Step 1 ✅](#phase-1-step-1)
- [Phase 1 Step 2/2b ✅](#phase-1-step-22b)
- [Phase 1 Step 3 ✅](#phase-1-step-3)
- [Phase 1 Step 4 ✅ (2026-04-17)](#phase-1-step-4-2026-04-17)
- [Phase 1 Step 4b ✅ (2026-04-17)](#phase-1-step-4b-2026-04-17)
- [Phase 1 Step 4c ✅ (2026-04-18)](#phase-1-step-4c-2026-04-18)
- [Phase 1 Step 4d ✅ (2026-04-18)](#phase-1-step-4d-2026-04-18)
- [Phase 1 Step 4e ✅ (2026-04-18)](#phase-1-step-4e-2026-04-18)
- [Phase 1 Step 4f ✅ (2026-04-18)](#phase-1-step-4f-2026-04-18)
- [Phase 1 Step 4g ✅ (2026-04-18)](#phase-1-step-4g-2026-04-18)
- [Phase 1 Step 4h ✅ (2026-04-18)](#phase-1-step-4h-2026-04-18)
- [Phase 1 Step 4i ✅ (2026-04-19)](#phase-1-step-4i-2026-04-19)
- [Phase 1 Step 4j ⚠️ (2026-04-19)](#phase-1-step-4j-2026-04-19)
- [Phase 1 Step 4k ✅ (2026-04-19)](#phase-1-step-4k-2026-04-19)
- [Phase 1 Step 4l ✅ (2026-04-19)](#phase-1-step-4l-2026-04-19)
- [Phase 1 Step 4m ✅ (2026-04-20)](#phase-1-step-4m-2026-04-20)
- [Phase 1 Step 4n ✅ (2026-04-22)](#phase-1-step-4n-2026-04-22)
- [Phase 1 Step 4o ✅ (2026-04-22)](#phase-1-step-4o-2026-04-22)
- [Phase 1 Step 4p ✅ (2026-04-22)](#phase-1-step-4p-2026-04-22)
- [Phase 1 Step 4q ✅ (2026-04-23)](#phase-1-step-4q-2026-04-23)
- [Phase 1 Step 4r ✅ (2026-04-23)](#phase-1-step-4r-2026-04-23)
- [Phase 1 Step 4s ⚠️ (2026-04-24)](#phase-1-step-4s-2026-04-24)
- [Phase 1 Step 4t ✅ (2026-04-24)](#phase-1-step-4t-2026-04-24)
- [Phase 1 Step 4u ✅ (2026-04-24)](#phase-1-step-4u-2026-04-24)
- [Phase 1 Step 4v ✅ (2026-04-24)](#phase-1-step-4v-2026-04-24)
- [Phase 1 Step 4w ✅ (2026-04-24)](#phase-1-step-4w-2026-04-24)
- [Phase 1 Step 4x ✅ (2026-04-25)](#phase-1-step-4x-2026-04-25)
- [Phase 1 Step 4y ✅ (2026-04-25)](#phase-1-step-4y-2026-04-25)
- [Phase 1 Step 4z ✅ (2026-04-25)](#phase-1-step-4z-2026-04-25)
- [Phase 1 Step 4aa ✅ (2026-04-25)](#phase-1-step-4aa-2026-04-25)
- [Phase 1 Step 4ab ✅ (2026-04-26)](#phase-1-step-4ab-2026-04-26)
- [Phase 1 Step 5b ✅ (2026-04-26)](#phase-1-step-5b-2026-04-26)
- [Phase 1 Step 5c ✅ (2026-04-26)](#phase-1-step-5c-2026-04-26)
- [Phase 1 Step 5d ✅ (2026-04-26)](#phase-1-step-5d-2026-04-26)
- [Phase 1 Step 5e ✅ (2026-04-26)](#phase-1-step-5e-2026-04-26)
- [Phase 1 Step 5f ✅ (2026-04-27)](#phase-1-step-5f-2026-04-27)
- [Phase 1 Step 5g ✅ (2026-04-28)](#phase-1-step-5g-2026-04-28)
- [Phase 1 Step 5h ✅ (2026-04-29)](#phase-1-step-5h-2026-04-29)
- [Phase 1 Step 5i ✅ (2026-04-29)](#phase-1-step-5i-2026-04-29)
- [Phase 1 Step 7 ✅ (2026-04-30)](#phase-1-step-7-2026-04-30)
- [Phase 2 (1) fnet_waveform backfill 加速 ✅ (2026-04-30)](#phase-2-1-fnetwaveform-backfill-加速-2026-04-30)
- [Phase 2 (1) gnss_tec backfill 加速 ✅ (2026-05-01)](#phase-2-1-gnsstec-backfill-加速-2026-05-01)
- [Phase 2 (1) ioc_sea_level backfill 加速 ✅ (2026-05-01)](#phase-2-1-iocsealevel-backfill-加速-2026-05-01)
- [Phase 2 (Stage 1) TRANSIENT_FAILURE sentinel for fetch_gnss_tec + fetch_ioc_sealevel ✅ (2026-05-02)](#phase-2-stage-1-transientfailure-sentinel-for-fetchgnsstec-fetchiocsealevel-2026-05-02)
- [Phase 2 (Stage 2.A) ioc_sea_level DART contamination fix ✅ (2026-05-02)](#phase-2-stage-2a-iocsealevel-dart-contamination-fix-2026-05-02)
- [Phase 2 (Stage 2.B) dart_pressure IOC integration ✅ (2026-05-02)](#phase-2-stage-2b-dartpressure-ioc-integration-2026-05-02)
- [Phase 2 (Stage 2.C) IOC chunk-fetch acceleration ✅ (2026-05-02)](#phase-2-stage-2c-ioc-chunk-fetch-acceleration-2026-05-02)
- [Phase 2 (Stage 3) loader threshold fix for sparse-by-design tables ✅ (2026-05-02)](#phase-2-stage-3-loader-threshold-fix-for-sparse-by-design-tables-2026-05-02)
- [Phase 2 (3) Hi-net waveform fetcher Stage 1 ✅ (2026-05-03)](#phase-2-3-hi-net-waveform-fetcher-stage-1-2026-05-03)
- [Phase 2 (3) Hi-net Stage 2 verification + merge job timeout fix ✅ (2026-05-04)](#phase-2-3-hi-net-stage-2-verification-merge-job-timeout-fix-2026-05-04)
- [Phase 2 (3) PR #129 production validation ✅ (2026-05-04)](#phase-2-3-pr-129-production-validation-2026-05-04)
- [Phase 2 (3) PR-2 artifact overlay-only ✅ (2026-05-04)](#phase-2-3-pr-2-artifact-overlay-only-2026-05-04)
- [Phase 2 (3) PR-2 production validation ✅ (2026-05-04)](#phase-2-3-pr-2-production-validation-2026-05-04)
- [Phase 2 (3) Cosmic ray station expansion ✅ (2026-05-04)](#phase-2-3-cosmic-ray-station-expansion-2026-05-04)
- [Light-artifact reset incident + Stage 1 fix ✅ (2026-05-06)](#light-artifact-reset-incident-stage-1-fix-2026-05-06)
- [Stage 3 BQ regression guard ✅ (2026-05-06)](#stage-3-bq-regression-guard-2026-05-06)
- [IOC sea level step timeout fix ✅ (2026-05-07)](#ioc-sea-level-step-timeout-fix-2026-05-07)
- [ISS LIS parallelization + Swarm window Variable extraction (in flight) (2026-05-07)](#iss-lis-parallelization-swarm-window-variable-extraction-in-flight-2026-05-07)
- [Restore step timeout (5/5 reset incident recurrence) hotfix ✅ (2026-05-07)](#restore-step-timeout-55-reset-incident-recurrence-hotfix-2026-05-07)
- [Checkpoint restore timeout + snapshot VACUUM ✅ (2026-05-09)](#checkpoint-restore-timeout-snapshot-vacuum-2026-05-09)
- [Restore step integrity check timeout regression ✅ (2026-05-10)](#restore-step-integrity-check-timeout-regression-2026-05-10)
- [Cloud-native migration: retire RPi5 SSD, Hugging Face as canonical store ✅ (2026-05-28)](#cloud-native-migration-retire-rpi5-ssd-hugging-face-as-canonical-store-2026-05-28)
- [Restore step timeout for the re-seeded 17 GB checkpoint (2026-05-28)](#restore-step-timeout-for-the-re-seeded-17-gb-checkpoint-2026-05-28)
- [Free disk space in the heavy fetch jobs (2026-05-29)](#free-disk-space-in-the-heavy-fetch-jobs-2026-05-29)
- [Cloud snapshot CLI verify uses quick_check (2026-05-29)](#cloud-snapshot-cli-verify-uses-quickcheck-2026-05-29)
- [Serialise master workflow_dispatch with the schedule cron (2026-05-29)](#serialise-master-workflowdispatch-with-the-schedule-cron-2026-05-29)
- [cloud_fraction snapshot disk-full fix → full 2000-2026 coverage ✅ (2026-05-30)](#cloudfraction-snapshot-disk-full-fix-full-2000-2026-coverage-2026-05-30)
- [fnet_waveform 3x backfill acceleration + durable /mnt snapshot fix (2026-05-30)](#fnetwaveform-3x-backfill-acceleration-durable-mnt-snapshot-fix-2026-05-30)
- [light + fetch snapshot disk-full root cause: in-place finalise (drop backup+VACUUM) ✅ (2026-05-31)](#light-fetch-snapshot-disk-full-root-cause-in-place-finalise-drop-backupvacuum-2026-05-31)
- [Drive the last 3 sub-100% tables to completion: gnss_tec timeout + ioc chunk cap (2026-06-01)](#drive-the-last-3-sub-100-tables-to-completion-gnsstec-timeout-ioc-chunk-cap-2026-06-01)
- [gnss_tec split out of `light` + restore quick_check timeout accept (cron regression fix) ✅ (2026-06-03)](#gnsstec-split-out-of-light-restore-quickcheck-timeout-accept-cron-regression-fix-2026-06-03)
- [fetch-hinet runner-abort: auto-rerun band-aid -> per-job overlay restore (root fix) (2026-06-03)](#fetch-hinet-runner-abort-auto-rerun-band-aid---per-job-overlay-restore-root-fix-2026-06-03)
- [so2_column + ioc_sea_level confirmed at present day; data side ready for analysis (2026-06-05)](#so2column-iocsealevel-confirmed-at-present-day-data-side-ready-for-analysis-2026-06-05)
- [HF upload no-shrink guard: byte-size → row-count manifest (2026-07-05)](#hf-upload-no-shrink-guard-byte-size-row-count-manifest-2026-07-05)
- [Merge gated on the light base job: no failure email from GitHub runner-capacity incidents (2026-07-10, `10eb1f6`)](#merge-gated-on-the-light-base-job-no-failure-email-from-github-runner-capacity-incidents-2026-07-10-10eb1f6)
- [Checkpoint chain expiry: the base DB was silently rebuilt from empty (2026-07-24, recovered 2026-07-29, PR #197; guard extended 2026-08-01, PR #198)](#checkpoint-chain-expiry-the-base-db-was-silently-rebuilt-from-empty-2026-07-24-recovered-2026-07-29-pr-197-guard-extended-2026-08-01-pr-198)
- [Artifact inventory bounded by count, and three quiet failure modes in the fetch jobs (2026-08-06, PRs #199-#202)](#artifact-inventory-bounded-by-count-and-three-quiet-failure-modes-in-the-fetch-jobs-2026-08-06-prs-199-202)
- [The three open items from the section above, closed by measurement (2026-08-07, run 31132546412)](#the-three-open-items-from-the-section-above-closed-by-measurement-2026-08-07-run-31132546412)

---


Started 2026-04-11. Target: **100% coverage across all 30 validated tables from 2011-01-01 to 2026-04-17** — no shortcuts, no "good-enough" exclusions. Phase 0 audit classified every fetcher into four failure modes: (1) wiring gaps (`snet_pressure` never invoked, `soil_moisture` commented out), (2) sparse ±M6+ strategy misuse (`lightning`, `ulf_magnetic`, `gnss_tec`, `modis_lst`, `tec` fetch only around major events instead of continuously), (3) continuous-strategy silent stops (`so2_column` at 2014-03, `cloud_fraction` at 2012-01, `geomag_hourly` at 2013-09), (4) physical constraints requiring alternative sources (`satellite_em`→Swarm for 2011-2017, `iss_lis_lightning`→WWLLN for 2011-2017, `snet_waveform`→F-net/Hi-net/DONET for 2011-2016).

### Phase 1 Step 1 ✅

`fetch_snet_pressure.py` rewritten for continuous backfill.

### Phase 1 Step 2/2b ✅

All 3 "stopped" fetchers functional; dedicated `backfill.yml` runs ALL 28+ fetchers every 3 hours (8 cron, 24/7).

### Phase 1 Step 3 ✅

`tec`/`ulf_magnetic`/`gnss_tec` rewritten from sparse ±M6+ to continuous full-range.

### Phase 1 Step 4 ✅ (2026-04-17)

Lightning overhaul — WWLLN (2013-2025) + LIS/OTD (1995-2014). OLR→S3 v02r00. GOES X-ray SWPC time_tag fix.

### Phase 1 Step 4b ✅ (2026-04-17)

Data quality audit resolution — **8/10 contaminated features fixed**. cloud_fraction exception handler order bug (OperationalError subclass of DatabaseError). SO2/cloud timeout 40→90min. soil_moisture fetch step added to backfill.yml (CPC monthly: 15,441 rows verified). solar_wind/goes_proton future-date filter already in place. goes_xray LISIRD .jsond migration + SWPC ISO 8601. Lightning daily: WONTFIX (no free source; Phase 20 monthly sufficient). satellite_em: feature pipeline not wired (low priority).

### Phase 1 Step 4c ✅ (2026-04-18)

Checkpoint overwrite bug — targeted dispatch runs (e.g. `target=cloud_fraction`) uploaded partial-DB checkpoints that overwrote full checkpoints from scheduled runs, resetting SO2/cloud_fraction progress to 0 rows every cycle. Fix: skip checkpoint upload for targeted dispatches. Verified: SO2 4.5M rows (→2016-05, past 2015 threshold), cloud_fraction 523K rows (→2013-02), OLR 3.4M rows (→2026-04-14 preliminary tracking OK).

### Phase 1 Step 4d ✅ (2026-04-18)

BQ overwrite hole closed. Step 4c only guarded checkpoint upload; the Upload-to-BigQuery step still ran for targeted dispatches and `load_raw_to_bq.py` (WRITE_TRUNCATE on all 31 tables) overwrote BQ with whatever the restored checkpoint contained. Real damage: `lightning_thunder_hour` regressed 31,824→24,480 rows (2013-2025→2013-2022) on 2026-04-17 after parallel `target=soil_moisture` + `target=cloud_fraction` dispatches. Fix in 71f1b2e mirrors the Step 4c if-guard onto Upload-to-BigQuery.

### Phase 1 Step 4e ✅ (2026-04-18)

5h hard-limit kill-chain. Three consecutive scheduled runs (16:03/19:02/21:34 UTC) cancelled at exactly 5h with MODIS LST + SO2 + cloud_fraction in series eating the entire window — Snapshot succeeded but Upload-checkpoint and Upload-to-BigQuery never ran, so cloud/SO2 were progressing at ~1/10 the expected pace. Restructured `backfill.yml` from a single 1024-line job into three parallel jobs: `heavy` (290m, 5 fetchers: MODIS LST/SO2/cloud/snet_waveform/snet_pressure), `light` (200m, 26 fetchers), and `merge` (60m, `needs:[heavy,light]`, `if:always()`) which downloads both DB artifacts and runs the new `scripts/merge_checkpoints.py` (per-table ATTACH+INSERT with `--require-base` fail-closed and explicit BEGIN/COMMIT/ROLLBACK; 7 smoke tests passing). Light is base, heavy overlays its 5 owned tables via DELETE-then-INSERT to discard stale restored rows. Merge job retains the Step 4c+4d target-guards for checkpoint and BQ upload.

### Phase 1 Step 4f ✅ (2026-04-18)

heavy timeout + Upload-artifact guard. Step 4e's 290m heavy timeout proved too short — observed run (24595180615) measured MODIS LST 1h13m + SO2 1h22m + cloud >1h13m (cancelled mid-fetch by 290m job timeout) and `Upload heavy.db artifact` was skipped (plain `if:` evaluates false on job cancel), so merge fell into base-only path and BQ heavy-owned tables were WRITE_TRUNCATE'd back to the restored checkpoint state. Fix in 9bd6315 raises heavy timeout 290→350m (under GitHub's 360m hard limit) and adds `if: always() && steps.snapshot.outcome == 'success'` to both `Upload {heavy,light}.db artifact` steps so the artifact survives a job-level cancel. Real damage from this incident was limited — `MIN_ROWS_TO_UPLOAD=1000` in `load_raw_to_bq.py` saved so2_column (4.5M)/cloud_fraction (523K)/snet_pressure/modis_lst from rollback because their restored counts were 0 or <1000; only snet_waveform (36,313 rows) was overwritten. lightning_thunder_hour recovered to 31,824 rows automatically.

### Phase 1 Step 4g ✅ (2026-04-18)

5h hard-limit recurred. Heavy was running MODIS LST + SO2 + cloud_fraction + S-net waveform + S-net pressure serially for 4h52m; raising timeout to 350m could not absorb it, so scheduled runs kept getting cancelled. Commit 6e226b9 splits heavy into **5 parallel fetch jobs (fetch-modis/fetch-so2/fetch-cloud/fetch-snet/light)**, each emitting its own artifact, with merge aggregating them via N-way overlay. `scripts/merge_checkpoints.py` extended for N-way support (`--overlay PATH:TABLES` repeatable, `--require-base` fail-closed, Windows `C:\` path handling via `rpartition(":")`); 10 smoke tests all PASS. Verified with a real `target=all` run completing in 2h08m28s.

### Phase 1 Step 4h ✅ (2026-04-18)

Fixed a pre-existing merge_checkpoints.py bug where overlay tables with 0 rows would wipe the accumulated base rows (the real cause of the cloud_fraction 523K→0 regression) in commit 87c4769. Before DELETE, check `SELECT COUNT(*) FROM overlay.table`; if overlay_count==0 and base>0, skip with log `overlay empty; base preserved (N rows)`. Added `test_empty_overlay_preserves_base` + `test_nonempty_overlay_still_replaces` to local smoke tests, 10/10 PASS.

### Phase 1 Step 4i ✅ (2026-04-19)

Identified why the cloud.db artifact was repeatedly rejected by merge with "database disk image is malformed". A temporary `diagnostic-cloud` job (target=diagnostic + diag_run_id input) was added to `backfill.yml` to run `PRAGMA integrity_check` / `SELECT COUNT(*)` / `ATTACH DATABASE` right after download — the artifact was **corrupt from the start** (page-level corruption: `Tree X page Y cell Z: invalid page number ...`). Root cause: the Snapshot DB step only ran `integrity_check` on the **source**; `src.backup(dst)` can silently produce a corrupted snapshot when the fetcher is terminated by SIGKILL, and with dst unverified the poisoned artifact was being uploaded. Commits bbce59d + cda044f add post-backup integrity_check to all 5 snapshot blocks (fetch-modis/so2/cloud/snet/light): `verify = sqlite3.connect(dst); r2 = verify.execute('PRAGMA integrity_check').fetchone()[0]; if r2 != 'ok': sys.exit(1)`. A bad snapshot → fetch-X failure → `Upload *.db artifact` skipped by the `steps.snapshot.outcome == 'success'` guard → merge treats the overlay as missing and preserves the base.

### Phase 1 Step 4j ⚠️ (2026-04-19)

Snapshot verify via subprocess CLI. Commit 9abdde3 switched verification to `sqlite3 "$db" "PRAGMA integrity_check"` run through a subprocess after `os.sync()` to bypass a suspected Python-sqlite3 false-OK. However, schedule run 24626051376 again surfaced malformed on the merge side, proving **subprocess-mode integrity_check also fails to detect this corruption** (root cause unresolved; presumed SQLite page cache vs. on-disk mismatch). The change is kept as a harmless additional defensive layer.

### Phase 1 Step 4k ✅ (2026-04-19)

Identified the real root cause of cloud_fraction malformed. `fetch_cloud_fraction.py` with `CLOUD_MAX_DATES=2000` could not finish within the 90min job timeout and was SIGKILL'd at ~920/2000 dates, leaving the DB half-written with residual B-tree inconsistency that only surfaced when merge called ATTACH. `integrity_check` returns false-OK here because local page checks pass even while internal inconsistencies remain. Commit cee73c0 lowered `CLOUD_MAX_DATES` from 2000 to 900 to eliminate the SIGKILL. Validation run 24629240566: fetch-cloud finished cleanly in 1h44m, cloud_fraction overlay applied 0→175,421 rows, malformed error gone.

### Phase 1 Step 4l ✅ (2026-04-19)

Even at `CLOUD_MAX_DATES=900`, measured throughput is ~6.75s/date (slower than the 5.5s estimate), so runs still timed out at 800/900. Commit bd84e49 bumped the fetch-cloud timeout from 90 to 120 minutes (since it runs in parallel, this only affects merge wait time, not overall wall clock). Observed: cloud_fraction grew 733K → **1.33M rows (+80%)**, covering 2011-01 → 2014-09. The remaining ~12 years continue to backfill via the 8 runs/day schedule.

### Phase 1 Step 4m ✅ (2026-04-20)

Discord notification noise reduction. Commit d32c627 changed the merge job's `Notify Discord` step to send only on failure or when all tables reach ≥95%. The routine progress 📊 notification exits early via `sys.exit(0)`, silencing the 8-notifications/day schedule while preserving failure alerts and the 🎉 milestone notification.

### Phase 1 Step 4n ✅ (2026-04-22)

fetch-cloud job-level timeout raised from 120m to 150m (commit 2638e0e). The step-level `timeout-minutes: 120` on "Fetch MODIS cloud fraction" plus setup/snapshot overhead caused the job to exceed its own 120m job limit, cancelling before the Upload artifact step. Job limit is now 150m, giving ~25m buffer; the step limit remains 120m to prevent SIGKILL DB corruption.

### Phase 1 Step 4o ✅ (2026-04-22)

Permanent-failure Issue spam eliminated (commit 23210bc). `lightning` (Blitzortung archive restricted) and `snet_pressure` (win32tools unavailable on Linux) were failing every run and triggering a new GitHub Issue each time — 50 issues created in 2 days. Both sources excluded from Issue trigger conditions; their failures are now logged as expected permanent failures without creating Issues. Issues #34–#83 bulk-closed.

### Phase 1 Step 4p ✅ (2026-04-22)

snet_waveform coverage fix. The coverage metric was using total-run days as denominator, making oldest-first gap-fill look like 3.2% even as rows accumulated. Changed denominator to snet_start-based days; fallback to oldest-first full-history scan when window yields 0 dates and coverage < 100%.

### Phase 1 Step 4q ✅ (2026-04-23)

SO2 diagnostic message corrected (commit 6177496). `fetch_omi_so2.py` was logging "credentials invalid for GES DISC data access" for any 0-record result regardless of cause. The OMI OMSO2G archive physically ends at ~2025-06-08 due to instrument degradation; auth is not the issue. Message updated to explain data-end and instruct users to check for HTTP 4xx in WARNING logs if auth failure is suspected.

### Phase 1 Step 4r ✅ (2026-04-23)

Restore checkpoint step silent hang fixed (commit 2653224). All 5 "Restore previous database checkpoint" steps across fetch-modis/fetch-so2/fetch-cloud/fetch-snet/light lacked a `timeout-minutes`, so a hung `gh run download` call could consume the entire 150m job budget, leaving the actual fetch and artifact upload steps never executed (confirmed: fetch-modis run 24789150140 showed step `in_progress` with no `completed_at` after 150m). Added `timeout-minutes: 10` to all 5 restore steps; `continue-on-error: true` ensures the fetch proceeds cleanly even when restore times out.

### Phase 1 Step 4s ⚠️ (2026-04-24)

bd7cd81 aggressive-timeout regression identified and reverted. Step 4r's `timeout-minutes: 10` alone was insufficient to prevent hangs, so commit bd7cd81 additionally wrapped `python3 check_db_integrity.py` with `timeout 30` (and salvage with `timeout 120`) intending to fail fast. Result: `PRAGMA integrity_check` on the 9.9GB checkpoint actually takes ~107s (measured in Step 4t), so every integrity check hit rc=124, was mis-diagnosed as corruption, ran salvage (also timed out), retried against the next older artifact, and exhausted the retry budget. Observed failure shape: 4/5 pre-revert schedule runs failed at Restore with `fetch-cloud`/`fetch-modis` jobs dying at 2:48-6:02 min with step `in_progress` / `conclusion=null` (runner-kill pattern). Commit 841b88c reverts the five `timeout 30`/`timeout 120` command wrappers.

### Phase 1 Step 4t ✅ (2026-04-24)

Integrity-check benchmark workflow added (commit 0e33fb9). New `.github/workflows/measure-integrity.yml` (workflow_dispatch only, `memo` input required) downloads the latest `backfill-checkpoint-*` artifact on a GH runner, then measures DL time, per-table COUNT(*), `PRAGMA quick_check`, `PRAGMA integrity_check`, and `scripts/check_db_integrity.py --mode=full`. First run on 9.9GB / 40 tables: DL 60.37s, per-table COUNT total ~0.5s, quick_check 14.72s, integrity_check 106.25s, full check 107.23s. Timings are reported via `$GITHUB_STEP_SUMMARY` with `awk -v a="$t1" -v b="$t0" 'BEGIN{printf "%.2f", a-b}'` and a `trap` that closes the markdown code fence on timeout-kill. Independent `measure-integrity` concurrency group so it cannot stall backfill.

### Phase 1 Step 4u ✅ (2026-04-24)

Two-stage integrity check design (commit 957e147). `scripts/check_db_integrity.py` gains `--mode=quick|full` — quick runs only `PRAGMA quick_check` (~15s on 9.9GB, catches structural/page-level corruption); full runs `PRAGMA integrity_check` + per-table `COUNT(*)` (~107s, catches index-level corruption). Restore path uses **quick** with `timeout 60`; Init DB path uses **full** with `timeout 300` as a safety net; salvage stays at `timeout 300`. New `.github/workflows/weekly-integrity-audit.yml` runs `--mode=full` every Monday 00:00 JST (Sunday 15:00 UTC), tries `backfill-checkpoint-*` first and falls back to legacy `database-checkpoint-*` artifact names, and opens an `integrity-audit` labelled Issue on detection. Independent concurrency group (`weekly-integrity-audit`) keeps it off the backfill queue.

### Phase 1 Step 4v ✅ (2026-04-24)

Artifact-download timeout widened 60s→180s (commit 2839e3a). The Step 4u smoke test revealed all 5 fetch jobs failing at `gh run download` with rc=124 — Step 4t had measured DL at 60.37s, right at the 60s timeout boundary, and ordinary network variance pushed every attempt over. All 5 Restore steps now use `timeout 180 gh run download` (3× margin). Post-fix smoke (24868276554): light Restore 1m46s ✅, Init 1m45s ✅. First post-Phase 3.1 production schedule run (24870429565, 03:18 UTC 2026-04-24, 18min cron delay) measured Restore + Init per fetch job: fetch-cloud 1m44s + 1m35s, fetch-snet 1m45s + 1m46s, fetch-modis 1m27s + 1m46s, light 1m39s + 1m46s, fetch-so2 4m10s + 1m23s — target window 3-5 min cleared across all 5 jobs (fetch-so2 is the outlier but well under the 180s DL timeout and 10-min step timeout). bd7cd81 regression is fully resolved.

### Phase 1 Step 4w ✅ (2026-04-24)

Phase 3.1 production stability confirmed across two consecutive schedule runs. Run 24870429565 (03:00 UTC cron, +18m delay) completed end-to-end success: all 5 fetch jobs passed Restore + Init, merge job ran 49min (04:46→05:35 UTC), Discord notify success, `Auto-rerun failed fetch jobs` and `Create issue on failure` both correctly `skipped`. Fetch durations: fetch-cloud 23.76min (fastest), fetch-snet 40.85min, light 44.28min, fetch-so2 45.26min, fetch-modis 87.66min (bottleneck, 73% of the 120m job timeout). Run 24874187228 also success: all 5 Restore times 1m23s–2m43s, total wall clock 2h40m (05:39→08:19 UTC), merge 47min, Discord + auto-rerun behavior identical to the previous run. Task #20 resolution: the apparent "2026-04-24 00:00 UTC schedule skip" was not a skip — GitHub Actions fired the 00:00 UTC cron at 05:39:41 UTC (+5h39m delay) as run 24874187228, and the 06:00 UTC cron followed at 07:48:37 UTC (+1h48m delay) as run 24878479422. The `concurrency: backfill-data` group with `cancel-in-progress: false` correctly queued 24878479422 behind 24874187228 instead of cancelling it. Severe scheduler delay is a GitHub Actions infrastructure condition, not a code issue; no mitigation required.

### Phase 1 Step 4x ✅ (2026-04-25)

snet_waveform permanent-skip marker for structurally empty (date,sensor) pairs (PR #88, commit 0091be7). Audit of 5 consecutive schedule runs (24899330313/24906601022/24913124903/24920790861/24923422139) showed each consumed ~120 of its 120-request budget but only added ~33 unique dates per run because ~40% of attempted dates returned zero records — the same dates (cable outage windows, station downtime, pre-deployment dates) were retried indefinitely, capping projected completion at 80-150 days vs. memory's earlier "~11 days" estimate. Fix: `snet_failed_dates(date_str, sensor_type, last_failed_at, retry_count, reason)` table created in `ensure_table()`; `_fetch_day` raises new `HinetQuotaError`/`HinetAuthError` exceptions (carrying `partial_results`) instead of silent partial returns so `_fetch_and_save` can distinguish quota stops from genuine no-data and persist any segments fetched before the quota hit; zero-record returns invoke `_mark_failed_sync` (INSERT OR IGNORE + UPDATE retry_count++) to atomically increment the counter; `main()` merges existing pairs with `get_failed_dates()` (threshold = `MAX_RETRIES_BEFORE_SKIP=3`) into a `skip_pairs` set used by all three `dates_to_fetch` construction loops (recent, backfill, full historical scan). `get_failed_dates` docstring documents the manual reset SQL for cases where transient NIED maintenance happens to span three consecutive schedule runs. Smoke test (6 cases) covers table creation, columns, mark/get cycle, threshold boundary, exception payloads, and the `MAX_RETRIES_BEFORE_SKIP` constant; Opus subagent review clean. Expected effect: per-run new-date ratio rises from ~60% to 95+%, projected completion shrinks from 80-150 days to **30-50 days**. First production exposure begins on the 12:00 UTC 2026-04-25 schedule run.

### Phase 1 Step 4y ✅ (2026-04-25)

Coverage report formula normalization (PR #89, commit f6c60aa). `pct = n_dates / total_days * 100` with `total_days = (today - 2011-01-01).days` produced impossible >100% values for tables holding pre-2011 data (e.g. `cosmic_ray`, `solar_wind`, `goes_xray`). Added `WHERE <date_expr> >= "2011-01-01"` filter to all 32 coverage queries in both the main coverage report (line ~1982) and the failure issue body (line ~2236), 64 edits total. Year-month granularity (`lightning_thunder_hour`, `lightning_lis_otd`) uses `"2011-01"`; `geomag_hourly` AND-joins after the existing `WHERE station="KAK"`. SQLite in-memory mock smoke test for all 4 query patterns (plain substr, raw column, multiplier, existing-WHERE) passed.

### Phase 1 Step 4z ✅ (2026-04-25)

merge_checkpoints.py shrink-protection guard (PR #90, commit 8dfeb2f). Step 4h's empty-overlay guard only blocked `overlay_count == 0`; `modis_lst` exhibited a different shrink pattern — overlay had non-empty but smaller-than-base rows (e.g. 488 → 338) when fetch-modis started from a failed checkpoint restore and only wrote newly fetched rows. Append-only fetchers always restore the prior checkpoint then INSERT-OR-IGNORE new rows, so `overlay_count < before` is now the trigger condition (deficit logged). Smoke test rewrote `test_nonempty_overlay_still_replaces` (which incidentally tested now-rejected shrink behavior) into `test_grown_overlay_replaces_base` and added `test_shrunk_overlay_preserves_base` for the modis_lst 488→338 regression scenario; 11/11 PASS.

### Phase 1 Step 4aa ✅ (2026-04-25)

snet_pressure deprecated end-to-end (PR #91, commit aed65bb, −575 lines). HinetPy network code `0120A` is S-net acceleration data, not pressure; HinetPy exposes no code for S-net BPR (bottom pressure recorder) measurements (canonical reference: [HinetPy/header.py L83-86](https://github.com/seisman/HinetPy/blob/master/HinetPy/header.py#L83-L86)). The fetcher has produced 0 rows since inception and never can via this path. Replaced the 580-line `scripts/fetch_snet_pressure.py` body with a 25-line deprecation stub (logs reason, exits 0, kept as tombstone). Removed `snet_pressure` from `backfill.yml` (target option, fetch-snet job conditional, fetch step, overlay spec, output ref, win32tools conditional, validate list, Discord env, alerts comment, both coverage queries), `analysis.yml`, `load_raw_to_bq.py`, `validate_data.py`, `audit_artifact.py`, `merge_checkpoints.py` docstring, and `docs/DATA_QUALITY_ISSUES.md` (status `open` → `deprecated 2026-04-25` with root cause). All Python files compile clean, both YAMLs parse, merge_checkpoints smoke test 11/11 unchanged. README docs already reflected the conclusion (Phase 18-19 lines: "pressure channels absent in all 4 codes").

### Phase 1 Step 4ab ✅ (2026-04-26)

lightning (Blitzortung) deprecated end-to-end (PR #93, commit 63b7a17, −560 lines). Blitzortung historical archive returns HTML access-restricted response for every queried month, University of Bonn sferics archive is EU-only (no Japan coverage), Blitzortung live API only exposes the most recent ~2 hours (no historical backfill). The `lightning` table has been at 0 rows since project start; active lightning coverage is provided by `iss_lis_lightning` (NASA ISS LIS, 7,819 rows, 2017-2023), `lightning_thunder_hour` (WWLLN, 31,824 rows, 2013-2025), and `lightning_lis_otd` (NASA LIS/OTD monthly climatology, 20,808 rows, 1995-2014). Replaced 586-line `scripts/fetch_blitzortung.py` body with a 38-line deprecation stub (logs reason, exits 0). Removed `lightning` from `backfill.yml` (target option, fetch step, output ref, env var, coverage list, both coverage queries, Discord notify, alerts comment), `load_raw_to_bq.py`, `validate_data.py`, `audit_artifact.py`, and updated `docs/DATA_QUALITY_ISSUES.md` (per-feature contamination row + status table → `deprecated 2026-04-26`). All Python files compile clean, YAML parses, merge_checkpoints smoke 11/11 unchanged.

### Phase 1 Step 5b ✅ (2026-04-26)

ESA Swarm A satellite EM fetcher (PR #95, merge commit b8dadd0). Replaces deprecated CSES portion of `fetch_cses_satellite.py` with `scripts/fetch_swarm_em.py` (425 lines, viresclient SDK based). Fetches `SW_OPER_MAGA_LR_1B` (F + B_NEC + CHAOS-Core residual) and `SW_OPER_EFIA_LP_1B` (Ne, Te) over Japan bbox (lat 20-50, lon 120-155) for 2014-01-01 onward. MAG and EFI stored as separate rows (`source = SWARM_A_MAG` / `SWARM_A_EFI`) with per-source resume_date to avoid 1Hz/2Hz join NaN issues and prevent one source's transient failure from masking the other's gap (CodeRabbit Major fix in commit eccc714). `residuals=True` requests VirES-side B residual calculation (no client-side vector subtraction). 401/403 hard-fail via `SwarmAuthError` (no silent skip). `asynchronous=False` for CI efficiency. Auth via `SWARM_TOKEN` GH secret (Bearer token from https://vires.services/accounts/tokens). New `swarm_em` table + `viresclient>=0.11.0` requirement; `fetch_cses_satellite.py` reduced to geomag_hourly only.

### Phase 1 Step 5c ✅ (2026-04-26)

NIED F-net broadband waveform fetcher (PR #97). Adds Full Range Seismograph Network as second alternative-source-fusion module after Swarm (Step 5b). New `scripts/fetch_fnet_waveform.py` (~1040 lines) fetches HinetPy network code `0103` from 73 land-based broadband stations across Japan, sampling rate 100 Hz, 3-component. Initial rollout via stratified-latitude sampling of 15 stations (`FNET_MAX_STATIONS` env, `FNET_STATIONS=ALL` to disable filter); `client.select_stations` retries with stripped `N.` prefix on failure to tolerate HinetPy code naming variant. Same feature schema as `snet_waveform` (rms/hv_ratio/lf_power/hf_power/spectral_slope/vlf_power/vlf_hv_ratio) for cross-source ML parity. SAC channel filter limits to `BH*` prefix only — F-net distributes both BH (broadband 100 Hz) and LH (long-period 1 Hz) for some stations and mixing fs in the PSD path corrupted feature values; fs consistency check across Z/N/E rejects mismatched triplets. 13 geographic regions (F-HKD..F-OKN) with half-open `[lo, hi)` lat boundaries replace S-net cable segments. New `fnet_waveform` + `fnet_failed_dates` tables; the latter mirrors the snet permanent-skip pattern. Integrated into `fetch-snet` job sequentially with `SNET_MAX_REQUESTS=60` + `FNET_MAX_REQUESTS=60` (shared NIED 150-slot daily quota), per-step `timeout-minutes: 75 + 75`, F-net step `if: always() && (target in [all, fnet_waveform, ''])` so it runs even when the S-net step times out. `install_win32tools` step extended to include `target=fnet_waveform` (CodeRabbit round-3 Critical: without this, single-target dispatch silently produced 0 records and permanent-skipped every date after 3 retries). CodeRabbit reviews across 3 rounds (round-1 7 findings / round-3 Critical+Nitpick) all addressed; docstring coverage 24/24 100%; smoke test on RPi5 verified module import, `ensure_table` schema, region classification spot checks (Hokkaido / Kanto / Kyushu-N / Okinawa / Izu).

### Phase 1 Step 5d ✅ (2026-04-26)

snet_waveform metric integrity (PR #98, merge commit `a618a3e`). `_save_records_sync` switched to `cursor.rowcount` so duplicate INSERT-OR-IGNORE rows count as `skipped` instead of `inserted`; `get_event_loop` → `get_running_loop` (Python 3.10+); removed a `MAX_BACKFILL_DAYS_PER_RUN * 3` over-allocation that was immediately truncated. Diff +7/-6.

### Phase 1 Step 5e ✅ (2026-04-26)

F-net `FNET_START_STR` anchor changed 1995-08-01 → 2000-01-01 (PR #99, merge commit `0d675bc`) to eliminate gap_days over-estimation while keeping the docstring "August 1995" historical reference. Diff +3/-1.

### Phase 1 Step 5f ✅ (2026-04-27)

F-net SAC channel filter critical fix (PR #100, merge commit `8ff0c4e`) — 🔴 the Step 5c F-net step had been silently producing 0 records since rollout. `channel.upper().startswith("BH")` assumed SEED 3-char component naming (BHU/BHN/BHE), but HinetPy `extract_sac` writes single-char components (`N.STATION.{U,N,E}.SAC`), so every SAC was rejected and each chunk logged `42 SAC data successfully extracted` → `0 stations processed`; BQ `data-platform-490901.geohazard.fnet_waveform` confirmed never created (cron run 24964462897 evidence). Replaced startswith with `{"U": "Z", "N": "X", "E": "Y"}` component-to-axis mapping; fs consistency check across the triplet still rejects mixed broadband/long-period combinations. CodeRabbit round-1 nitpick (stale "BH*" comment refresh) addressed in round 2. Verification of `fnet_waveform` row count > 0 deferred to next cron run after merge.

### Phase 1 Step 5g ✅ (2026-04-28)

F-net SAC channel filter true root-cause fix (PR #105, merge commit `d64f7f6`). After Step 5f (PR #100) replaced `BH*` with single-char `{U, N, E}`, the F-net step still produced 0 records. Debug instrumentation added in PR #104 (`fa6e757`) revealed `extract_sac` actually outputs **2-char component names** (`N.ISIF.NB.SAC`, `N.ADMF.EB.SAC`, ...): trailing `B` = broadband 100 Hz, trailing `A` = long-period 1 Hz. Replaced the single-char filter with `{"UB": "Z", "NB": "X", "EB": "Y"}` to select broadband only (long-period rejected to keep the PSD-path fs consistent). Verified on dispatch run 25086162850: `extract_sac -> 42 SAC files`, `station_files: 14 stations, comps=['X', 'Y', 'Z']`, `~13 stations processed` per date.

### Phase 1 Step 5h ✅ (2026-04-29)

light job `Free disk space` step (PR #106, merge commit `6d685e3d`). Three consecutive cron runs on HEAD `d64f7f6` (16:54 / 19:42 / 21:50 UTC 2026-04-28) failed with `System.IO.IOException: No space left on device : '/home/runner/actions-runner/cached/2.334.0/_diag/Worker_*.log'` during `Snapshot DB for light artifact` -- the runner's own diagnostic log filled the GitHub-hosted runner's 14 GB default disk in ~30 min of serial fetcher output. Added a leading step that removes pre-installed toolchains the workflow does not use (`/usr/share/dotnet`, `/usr/local/lib/android`, `/opt/ghc`, `/opt/hostedtoolcache/CodeQL`) for ~30 GB of headroom. Verified end-to-end on dispatch run 25086162850: light ✅, merge ✅, `merge_checkpoints.py` reported `fnet_waveform: 0 -> 429 rows (overlay applied)`.

### Phase 1 Step 5i ✅ (2026-04-29)

Per-table BQ row-count threshold override (PR #107, merge commit `9c519cf5`). With Steps 5g and 5h merged, dispatch run 25086162850 reached the BQ upload step but logged `fnet_waveform: 429 rows (< 1000 minimum) -- skipping to protect BQ data`. `MIN_ROWS_TO_UPLOAD = 1000` is calibrated to protect populated tables from corrupt-empty SQLite `WRITE_TRUNCATE`, but `fnet_waveform` is event-targeted (M6+ +/-N day windows x ~14 stations x 3 components) and naturally bootstraps in the low hundreds. Added `TABLE_MIN_ROWS_OVERRIDE = {"fnet_waveform": 100}`; the other 30 tables retain the 1000-row floor. CodeRabbit round-1 RUF003 nitpick (Unicode `x` / `->` in comment) addressed by ASCII-only rewrite in round 2. BQ `geohazard.fnet_waveform` table creation verification deferred to the next cron run on HEAD `9c519cf5`.

### Phase 1 Step 7 ✅ (2026-04-30)

Eliminated the trailing-echo non-fatal masking pattern (PR #110, merge commit `ef8a8a9`). 28 occurrences of the form `<fetcher invocation> || echo "..."` in `backfill.yml` were actively masking shell exit codes to 0, making `step.outcome=success` even when fetchers failed. Combined with `continue-on-error: true` and `outputs.fetch_X: steps.X.outcome` propagation, this silently broke the merge job `Create issue on failure` detection (line 2185). The `== "failure"` check on 30+ fetcher outputs never matched, so failures went undetected for an unknown stretch of time. Fixes: (a) 24 fetcher steps -- removed trailing `|| echo` so `continue-on-error: true` alone gives correct semantics (step exits non-zero, `outcome=failure`, workflow continues, merge job sees real failure state). (b) `Notify Discord` step -- added `continue-on-error: true`, removed two trailing `2>/dev/null || echo "Discord notification failed (non-fatal)"` so Discord 5xx now logs to step output without failing the job. (c) `COVERAGE=$(... || echo "Coverage unavailable")` at line 2305 retained -- legitimate shell substitution fallback for issue body content. With this merged, the **Phase 1 Data Completeness Initiative is structurally complete**: all 31 tables backfilling 24/7, BQ sync gated by row-count thresholds, F-net waveform end-to-end pipeline verified (429 rows / 13 stations on 2026-04-29), HinetPy graceful partial-save on SIGTERM (Phase D3), and now failure detection finally wired through to GitHub Issues + Discord notifications.

### Phase 2 (1) fnet_waveform backfill 加速 ✅ (2026-04-30)

PR #112 (merge commit `780f2ed`) for the `fnet_waveform` stalling at 13 stations x 12 days = 429 rows and emitting `F-net Waveform -- No Data` Discord notifications every cron run. Six-axis fix: (A) backfill while loop now applies `skip_dates` check inside the iteration so it advances past saved/failed dates instead of stalling at the cutoff boundary (root cause). (B) History scan reversed from oldest-first to newest-first so dates with higher likelihood of having actual broadband data are tried before old dates outside F-net data range. (C) `FNET_MAX_STATIONS` default raised 15 -> 73 -- HinetPy `get_continuous_waveform` takes the whole network in one request (12,000 channel-min limit covers 73 stations comfortably), so request count is unchanged while record output scales ~5x. (D) `MAX_BACKFILL_DAYS_PER_RUN` raised 5 -> 30 (recent 28 + backfill 30 = 58, within `MAX_REQUESTS_PER_RUN=60`). (E) `get_failed_dates` now also checks `last_failed_at` against `FAILED_DATES_RETRY_AFTER_DAYS=30` so dates failed more than 30 days ago roll back into the fetch pool. (F) New `smoke_test_phase_2_fnet.py` covers constants and retry rollback semantics (in-memory aiosqlite); existing Phase D3 smoke tests still pass. Expected outcome: `No Data` notifications stop within 24h, 429 -> 1500+ rows, ~50 days for full historical coverage.

### Phase 2 (1) gnss_tec backfill 加速 ✅ (2026-05-01)

PR #114 (merge commit `d8a389d`) for the `gnss_tec` table stalling at 840 days / 873k rows / last fetched 2013-04-19 -- the Step 3 oldest-first strategy advanced only ~21 days/day after 14 days, projecting ~7.5 months to cover the residual 4,759-day span. Six-axis fix matching the F-net Phase 2 (1) pattern: (A) `GNSS_TEC_MAX_DATES` default raised 30 -> 200 (env override preserved via `${{ vars.GNSS_TEC_MAX_DATES || '200' }}`). (B) HTTP fan-out via `asyncio.Semaphore(4)` + `as_completed` -- 4 dates in flight, SQLite writes serialized to avoid lock contention. (C) Per-date rate-limit sleep 2.0s -> 0.5s. (D) New `gnss_tec_failed_dates` table with `retry_count` tracking so 0-record dates do not retry indefinitely. (E) `FAILED_DATES_RETRY_AFTER_DAYS=30` rolls expired skip entries back into the fetch pool when Nagoya ISEE archive coverage expands. (F) Skip set in `main()` is the union of existing-dates and failed-skip-dates so both classes are excluded before the slice. New `smoke_test_phase_2_gnss.py` covers constants, retry rollback semantics across the 30-day boundary, and union skip composition (5 tests pass; with F-net Phase 2 + Phase D3 = 17 tests total green). Expected outcome: throughput ~80-100 days/day, 5,599-day target fully covered in ~50 days, and archive 404 dates auto-recover when Nagoya publication catches up.

### Phase 2 (1) ioc_sea_level backfill 加速 ✅ (2026-05-01)

PR #116 (merge commit `f4af2e62`) for the `ioc_sea_level` table stalling at 637k rows / 34 days span (2026-03-12 -> 2026-04-15) -- the existing fetcher hardcoded `FETCH_DAYS=45` and re-fetched the same recent window every cron run, so historical coverage never extended past the trailing 45-day buffer. Probe against the IOC SLSMF API confirmed historical `timestart=2020-01-01` returns 1-min cadence records, so the bottleneck was the fixed window, not the API. Six-axis fix matching the gnss_tec PR #114 pattern, generalised to per-station date keying: (A) `FETCH_DAYS=45` removed; replaced with oldest-first iteration over `(date, station)` pairs from `BACKFILL_START=2011-01-01` to yesterday UTC. (B) HTTP fan-out via `asyncio.Semaphore(2)` + `as_completed` -- 2 fetches in flight, lower than gnss_tec's 4 to respect IOC's "~1 req/min recommended" guidance. (C) Per-fetch rate-limit sleep `IOC_RATE_LIMIT_SLEEP=1.0s` (env). (D) New `ioc_sealevel_failed_dates` table with composite PK `(station_code, date_str)` and `retry_count` tracking so per-station 0-record dates do not retry indefinitely. (E) `FAILED_DATES_RETRY_AFTER_DAYS=30` rolls expired skip pairs back into the fetch pool. (F) New helper `build_target_pairs()` iterates dates outermost so all stations advance together rather than one station racing ahead, with a short-circuit when `IOC_MAX_FETCHES<=0`. `IOC_MAX_FETCHES` default 200, with `IOC_PARALLEL_FETCHES`, `IOC_RATE_LIMIT_SLEEP`, `IOC_MAX_FETCHES` all override-able via Repository Variables. New `smoke_test_phase_2_ioc.py` covers constants, composite-key retry rollback semantics, build_target_pairs ordering+cap+zero short-circuit (8 tests pass). Note: `dart_pressure` was originally co-listed in Phase 2 (1) but probing NDBC's historical archive at all 3 known URL patterns returned 404 across years and stations, so it has been demoted to Phase 2 (3) physical-constraint integration -- the IOC API already returns DART buoy stations (`diw2`, `dphi`, etc.) as part of its station list, so the natural follow-up is a unified IOC-based ingest. Expected outcome: 30 stations x 200 fetches/cron x 8 cron/day = 1,600 fetches/day -> per-station catch-up ~53 days/day, 5,500-day span fully covered in ~100 days.

### Phase 2 (Stage 1) TRANSIENT_FAILURE sentinel for fetch_gnss_tec + fetch_ioc_sealevel ✅ (2026-05-02)

PR #118 fixes a CodeRabbit Round 1 actionable from PR #116 (ioc_sea_level acceleration): both `fetch_gnss_tec.py` and `fetch_ioc_sealevel.py` were unable to distinguish "definitive 0-record date" (200 OK with empty body, or 404) from "transient HTTP failure" (5xx / 429 / timeout / 200 OK + HTML overload page / 200 OK + JSON decode error / 200 OK + non-list payload). The pre-fix behaviour collapsed both into a definitive failure marker that incremented `retry_count`, so a 30-day API outage burned all retries on legitimate dates and 30-day-blacklisted them.

Three-axis fix in both fetchers: (A) New `_TransientFailure` sentinel class with `__slots__ = ()` and a singleton instance `TRANSIENT_FAILURE`. (B) `fetch_station_data` (ioc) and `try_fetch` (gnss_tec) return `list[dict] | None | _TransientFailure` (3-value contract): list = parsed records (possibly empty for 200 OK no-data / 404), `None` = no data after retries (gnss_tec only, used for hour-grain aggregation in `fetch_date`), `TRANSIENT_FAILURE` = transient failure that should NOT advance `retry_count`. (C) `main()` uses `if rows is TRANSIENT_FAILURE` ordering BEFORE `elif rows:` truthiness check, so the sentinel cannot be misread as truthy data even though `bool(TRANSIENT_FAILURE) == True` (slots class with no `__bool__`). New `transient_skipped` counter surfaces in the run summary log so ops can verify the contract is firing. gnss_tec also gains a missing `asyncio.sleep(2 ** attempt)` in the non-200 branch (existing latent bug, fixed in the same PR).

Smoke test `smoke_test_phase_2_error_classification.py` (25 tests): IOC error classification (11 cases — 5xx / 429 / timeout / connection error / HTML body / JSON decode / non-list payload / 200 OK empty / 200 OK valid / 404 / mid-retry recovery), GNSS-TEC error classification (6 cases mirroring IOC), GNSS-TEC `fetch_date` hour-grain aggregation (5 cases — all hours OK / partial transient / partial empty / all empty / all transient), sentinel identity (3 cases — IOC sentinel `is not` None/`[]` / GNSS sentinel `is not` None/`[]` / per-module independence via `is` between the two sentinels). Verified live on cron run 25242527288 (sha `7a8303a8` post-merge): GNSS-TEC `0 dates failed (definitive), 0 dates transient-skipped` (200/200 success), IOC sea level `38 pairs inserted, 162 pairs failed (definitive), 0 pairs transient-skipped` (transient/definitive correctly separated).

### Phase 2 (Stage 2.A) ioc_sea_level DART contamination fix ✅ (2026-05-02)

PR #119 fixes a long-standing data-quality issue uncovered while planning Stage 2 (3) `dart_pressure` integration: the IOC SLSMF stationlist API does not separate sensor types, so `fetch_ioc_sealevel.py` was inadvertently including DART buoys (sensor=`prt`) -- which report ocean bottom pressure as water column height (~5779 m for `dtok`) -- in the `ioc_sea_level` table alongside coastal tide gauges (~1 m sea-level deviation). Because `load_phase13_ioc_sealevel` computes `AVG(sea_level_m) GROUP BY DATE` without per-station weighting, the OBP rows dominated the daily mean and corrupted the `ioc_sealevel_anomaly` feature. Six DART codes (`dtok`, `dtok2`, `dryu`, `dryu2`, `dsen`, `drus`) had inserted ~15,933 contaminated rows.

Three-axis fix: (A) `ALLOWED_SENSORS` allow-list (`rad`, `pwl`, `bub`, `prs`, `flt`, `wls`, `enc`, `aqu`) added to `fetch_ioc_sealevel.py`. Allow-list rather than deny-list so future unknown sensor types are skipped by default until classified manually. Sensor filter runs BEFORE the `MAX_STATIONS=30` cap so DART buoys cannot push tide gauges out of the cap budget. (B) Defense-in-depth check in `parse_ioc_data` -- per-record `sensor` field is also checked against `ALLOWED_SENSORS`, dropping any leaked `prt` records even when the station-level filter let the station through (covers IOC's multi-sensor merged response streams). (C) New `scripts/cleanup_ioc_sealevel_dart.py` one-off migration script (default dry-run, `--yes` to execute, `--station-codes`/`--min-date`/`--max-date`/`--skip-bq` for re-use). Removes the 6 known DART codes from sqlite + BigQuery, deleting `ioc_sealevel_failed_dates` entries first to prevent race re-fetching.

Smoke test `smoke_test_phase_2_ioc_sensor_filter.py` (16 tests): allow-list constants, station-level filter (rad pass / prt reject / null reject / unknown reject / case-insensitive / bbox-first), cap-order guarantee (5 tide gauges + 50 DART -> only 5 returned), parse-time defense (record-level prt drop). Run order after merge: `python scripts/cleanup_ioc_sealevel_dart.py --yes` once, then the next ml_prediction.py cron run will recompute `ioc_sealevel_anomaly` correctly from the cleaned database (features are re-derived from raw rows on every run, no model retrain needed). Stage 2 (3) -- `dart_pressure` migration from NDBC to IOC `prt` sensor -- is the Stage 2.B follow-up.

Follow-up PR #120 added `--skip-sqlite` to `cleanup_ioc_sealevel_dart.py` (symmetric with the existing `--skip-bq`) for hosts where the IOC fetcher writes only to BigQuery and the local sqlite mirror has no `ioc_sea_level` table -- discovered when the cleanup script was first run on RPi5 and failed with `OperationalError: no such table: ioc_sea_level`. The same PR makes `_sqlite_inspect` / `_sqlite_delete` gracefully skip with a warning when the IOC tables are absent (case-insensitive `"no such table"` match, mirroring `scripts/fetch_cloud_fraction.py:82`), so a wrong-host run no longer crashes mid-execution. The BigQuery DELETE itself was executed via direct `bq query` from the developer host (Windows `subprocess.run(["bq", ...])` in the script does not resolve `bq.cmd` cleanly) and removed exactly **15,933 rows** matching the pre-cleanup audit count.

### Phase 2 (Stage 2.B) dart_pressure IOC integration ✅ (2026-05-02)

Stage 2.B closes the historical gap in `dart_pressure` left by NDBC's realtime-only ~45-day window. Probing the IOC SLSMF stationlist confirmed that 4 DART buoys near Japan (`dtok`, `dtok2`, `dryu`, `dryu2`) are republished there with `sensor="prt"` and the same physical observable -- water column height in metres at 15-min cadence, slevel ~5779 m -- going back to 2011. New fetcher `scripts/fetch_ioc_dart.py` writes those into the existing `dart_pressure` schema using the IOC 4-letter code as `station_id`, which is a disjoint namespace from NDBC's numeric ids (`21413`, `21418`, etc.) so `UNIQUE(station_id, observed_at)` cannot collide between the two fetchers. NDBC keeps owning the realtime 45-day window; IOC backfills 2011 -> present in oldest-first order.

Same Phase 2 (1) acceleration shape as `fetch_ioc_sealevel.py`: `Semaphore(IOC_DART_PARALLEL_FETCHES=2)` + `as_completed` for 2-in-flight, per-fetch `IOC_DART_RATE_LIMIT_SLEEP=1.0s`, `IOC_DART_MAX_FETCHES=200` per cron run, new `dart_pressure_failed_dates` table with composite PK `(station_id, date_str)` + `retry_count`, `FAILED_DATES_RETRY_AFTER_DAYS=30` rolloff, `_TransientFailure` sentinel + 3-value return contract from Stage 1. `ALLOWED_SENSORS = frozenset({"prt"})` only, with defense-in-depth at both station-list and per-record level so a non-DART sensor cannot leak into `dart_pressure`. New `smoke_test_phase_2_ioc_dart.py` (16 tests) covers acceleration constants, station-list filter (prt pass / non-prt reject / missing-code reject / outside-bbox reject / case-insensitive), parse-time defense (record-level non-prt drop, missing per-record sensor allowed, invalid-record drop), build_target_pairs (oldest-first, skip composition, cap, zero short-circuit), failed-pairs SQL retry-rollback, and TRANSIENT_FAILURE sentinel singleton identity.

Workflow.yml runs `fetch_ioc_dart` immediately after `fetch_ioc_sealevel` in the light job with the same continue-on-error + 15-min timeout shape. Repository Variables `IOC_DART_PARALLEL_FETCHES` / `IOC_DART_RATE_LIMIT_SLEEP` / `IOC_DART_MAX_FETCHES` are optional overrides; defaults work out of the box. Expected outcome: 4 stations x 200 fetches/cron x 8 cron/day = 6,400 fetches/day cap (rarely reached given only 4 stations), 5,500-day span fully covered in ~50 days; `dart_pressure` rows extend from 11,217 (NDBC realtime 48-day window) to a coverage matching the IOC archive depth (typically 2011-04 to present per buoy, with per-station gaps).

### Phase 2 (Stage 2.C) IOC chunk-fetch acceleration ✅ (2026-05-02)

After Stage 2.A/2.B the IOC SLSMF backfill ETA was still ~3.5 months for `ioc_sea_level` and ~14 days for `dart_pressure` because both fetchers issued **one HTTP request per (station, day)** even though the IOC `query=data` endpoint accepts arbitrary `[timestart, timestop]` ranges. A live probe against `code=abas&timestart=2026-04-01&timestop=2026-04-30` returned **43,060 records / 3.0 MB / 23 s** in a single request, confirming a 30x request-count reduction is available with no API contract change.

Three-axis fix in both `fetch_ioc_sealevel.py` and `fetch_ioc_dart.py`: (A) New `CHUNK_DAYS=30` (env override `IOC_CHUNK_DAYS` / `IOC_DART_CHUNK_DAYS`) — `fetch_one_chunk()` issues a single 30-day data query instead of 30 single-day queries. (B) New `MAX_CHUNKS_PER_CRON=72` (env override `IOC_MAX_CHUNKS` / `IOC_DART_MAX_CHUNKS`) replaces the `MAX_FETCHES=200` per-day cap; 72 chunks × 30 days = 2,160 station-days per cron, × 8 cron/day = 17,280 station-days/day, against ~165,000 station-days residual = ~10-day completion. (C) New `ioc_sealevel_failed_chunks` and `dart_pressure_failed_chunks` tables track 0-record chunks at chunk grain with the same `retry_count` + 30-day rolloff semantics as the legacy `failed_dates` tables (which coexist for backward compatibility — old per-day failure entries naturally roll off via the 30-day retry window, no migration required).

`build_target_chunks()` skips a chunk only when EVERY day inside is already in `existing_per_station`; partial coverage triggers a fetch and `INSERT OR IGNORE` deduplicates redundant days at write time. New `smoke_test_phase_2_ioc_chunk.py` (12 tests) covers chunk constants, oldest-first chunk iteration, full-coverage skip, partial-missing fetch, failed-chunk skip, max-chunks cap + zero short-circuit, and failed-chunks SQL rollover for both modules.

Expected ETA improvement: `ioc_sea_level` 3.5 months → ~10 days (2026-05-12 頃), `dart_pressure` (IOC backfill portion) 14 days → ~3 days. The 1-day legacy code path (`build_target_pairs`, `mark_failed_pair`, `MAX_FETCHES`) remains in the modules for backward compatibility but is unused by `main()` — new deployments configure `IOC_MAX_CHUNKS` instead of `IOC_MAX_FETCHES`.

**Downstream double-counting note for `load_phase13_dart_pressure`**: the same physical buoy can appear under two distinct `station_id` values (IOC `dtok` ≈ NDBC `21413`, both ~30.53N 152E). During the NDBC realtime 45-day window the two fetchers will both write the same observation, so a naive `AVG(water_height_m) GROUP BY DATE` across all stations would double-weight overlap days for that buoy. Stage 2.A fixed an analogous DART-into-`ioc_sea_level` AVG contamination; the same class of bias should be considered when `load_phase13_dart_pressure` is implemented (e.g. `AVG` per-station first, then aggregate, or build a `station_id` equivalence map).

### Phase 2 (Stage 3) loader threshold fix for sparse-by-design tables ✅ (2026-05-02)

PR #124 closes a long-standing visibility gap discovered while computing the all-31-table BigQuery coverage view: `modis_lst` and `nightlight` were both **absent from BigQuery despite their fetchers running successfully and writing to SQLite**, because `load_raw_to_bq.py` applies a global `MIN_ROWS_TO_UPLOAD = 1000` protective floor (designed to prevent an empty/corrupted SQLite from `WRITE_TRUNCATE`-ing the BigQuery data to zero on a bad cron). Verified in cron run #25242527288 merge job log:

```
modis_lst: 341 rows (< 1000 minimum) — skipping to protect BQ data
nightlight: 950 rows (< 1000 minimum) — skipping to protect BQ data
```

Both row counts are realistic for the design: `modis_lst` is M5.5+ event-targeted ±14 day windows for 5 km cells (intentionally sparse, 341 rows spanning 2010-09-14 → 2026-03-22), and `nightlight` is the VNP46A4 annual product over Japan tiles `h29v05` / `h29v06` (~70 cells × 14 years = ~950 rows by design, +70/year growth).

Two-axis fix mirroring the existing `fnet_waveform: 100` precedent (low-density-by-design tables that still need a `>0` floor to detect corruption): `TABLE_MIN_ROWS_OVERRIDE` now contains `"modis_lst": 50` (current 341, 6.8x headroom) and `"nightlight": 100` (current 950, 1.4-year safety margin against annual growth). Both thresholds are well above zero so an empty/corrupted SQLite still gets blocked, while legitimate sparse data passes through. After this lands, the next cron run uploads both tables to BigQuery and the all-31-table coverage view becomes complete (modis_lst event-sparse expected ~6%, nightlight annual-grain expected ~24% — both reflecting fetcher design limits, not coverage gaps).



### Phase 2 (3) Hi-net waveform fetcher Stage 1 ✅ (2026-05-03)

PR #127 (merge commit `ded92d7`) adds the long-pending NIED Hi-net high-sensitivity short-period waveform fetcher as the third member of the cross-source waveform feature family alongside `snet_waveform` (2016+, Pacific seabed) and `fnet_waveform` (2000+, broadband land). Hi-net (~800 borehole-installed stations operating since 2004, network code `0101`, 1 Hz natural-period 3-component velocity sensors at 100 Hz) gives pre-2016 land-based coverage that S-net structurally cannot. New `scripts/fetch_hinet_waveform.py` (~1100 lines) mirrors the `fetch_fnet_waveform.py` Phase 1 Step 5c structure with Hi-net-specific adaptations: `HINET_NETWORK_CODE="0101"`, `HINET_START_STR="2004-04-01"` (NIED archive epoch), VLF analysis intentionally omitted (the 1 Hz natural period sensor cannot reliably resolve 0.01–0.1 Hz, so `vlf_power` / `vlf_hv_ratio` columns are kept NULL for schema parity with snet/fnet rather than computed), and SAC channel mapping defensively accepts both 1-char (`U` / `N` / `E`) and 2-char (`UD` / `NS` / `EW`) component naming because HinetPy `extract_sac` can emit either depending on the channel-table mapping (the F-net debug history at PR #100/#105 burned us once with this exact axis already). New `fetch-hinet` GHA job runs on its own runner with default `HINET_MAX_REQUESTS=30` and `HINET_MAX_STATIONS=30`; `merge` job extends `needs:` and adds `--overlay /tmp/hinet/geohazard.db:hinet_waveform`. `load_raw_to_bq.py` registers a 16-column `hinet_waveform` mapping identical in shape to `fnet_waveform` plus a `TABLE_MIN_ROWS_OVERRIDE["hinet_waveform"] = 100` low-density-by-design floor. Quota note: HinetPy ~200 req/day quota is per NIED account (session cookie), shared with `fetch-snet` (~144 req/cron) even on separate runners, so the conservative Hi-net default of 30 keeps total at 174 with margin. New `smoke_test_phase_2_hinet.py` (4 tests covering constants and 30-day retry-after rolloff) brings the Phase 2 smoke total to 36/36 green. Opus subagent review applied 2 MUST-FIX (per-account quota docstring + defensive 1+2-char channel mapping) and 4 SHOULD-FIX (drop dead `BAND_VLF`/`VLF_FFT_WINDOW_SEC`, snet/fnet comment parity, BQ override rationale comment, `HINET_MAX_STATIONS` env default alignment between code and workflow) before merge. Stage 2 verification (live credential one-shot probe + first cron run BQ load) follows in the next cron cycle.



### Phase 2 (3) Hi-net Stage 2 verification + merge job timeout fix ✅ (2026-05-04)

Stage 2 verification confirmed the Hi-net pipeline functioning end-to-end on the first post-merge cron run 25276071313 (`fetch-hinet` job ✅ at 12:30:20 UTC): network code `0101` / 797 stations reported / stratified-latitude sample of 30 reduced to 29 active (1 station no-data), 87 channels per query (29 stations × 3 components), and `comps=['X','Y','Z']` always present in the saved record dict — confirming the M2 defensive 1+2-char SAC channel mapping `{"U":"Z","UD":"Z","N":"X","NS":"X","E":"Y","EW":"Y"}` correctly normalises both naming conventions HinetPy `extract_sac` can emit. Total inserted: 609 records over 9 days (2026-04-24 → 2026-05-02), the 0.1% baseline coverage expected from a single 30-station × 9-day cron tranche. `~7 "Fail to request some data. Skipped"` warnings per run observed (HinetPy upstream signal, non-fatal, monitored for cumulative drift).

However, two consecutive subsequent runs (25276071313 / 25279499629) hit the merge job's `timeout-minutes: 60` ceiling and were cancelled at the Snapshot step, blocking BigQuery upload for two cron cycles. PR #129 (merge commit `24091959b`) diagnoses and fixes the structural issue: the most recent successful merge before Hi-net (run 74109114261) already ran 58 minutes — 1 minute under the 60-minute job timeout — with the breakdown 5 min downloads + 13 min `merge_checkpoints.py` (six-overlay shutil.copy + ATTACH+DELETE+INSERT + integrity_check) + 10 min Snapshot + 4 min Upload checkpoint + 25 min `load_raw_to_bq.py` WRITE_TRUNCATE of 31 BQ tables. Hi-net Stage 1 added one ~2 GB artifact download (~1 min) plus one overlay-copy step pushing the total past 60 min and starving Snapshot of finish time. Investigation of `scripts/merge_checkpoints.py:_verify()` revealed it already runs `PRAGMA integrity_check` on the same `dst` (`data/geohazard.db`) before the merge step exits with success (`__main__` wires `merge()` return value to `sys.exit(0 if ok else 1)`), making the Snapshot step's `PRAGMA integrity_check` a duplicate. The Snapshot step's `src.backup(dst)` was a 2 GB full-DB rewrite intended for compaction + WAL flush, but `load_raw_to_bq.py` reads SQLite → Pandas → BigQuery regardless of page fragmentation and the `Upload checkpoint artifact` step references `path: data/geohazard.db` as a single file with no glob expansion, so neither downstream consumer needs compaction — only the WAL flush, which `wal_checkpoint(TRUNCATE)` provides on its own. Two changes: (a) `timeout-minutes: 60 → 150` aligns the merge job with the heavy-job tier (`fetch-snet` / `fetch-hinet` 240, `light` 200) and stays under the 180-min cron period to avoid `concurrency: cancel-in-progress: false` queue overlap; (b) Snapshot step body simplified to `PRAGMA wal_checkpoint(TRUNCATE)` only — the redundant `integrity_check` and the `src.backup(dst) + mv geohazard_snapshot.db data/geohazard.db` block are removed with rationale comments preserved inline. Expected impact: Snapshot step ~10 min → ~5 sec; merge job total ~58 min → ~48 min, with the 150 min ceiling providing headroom for future data growth before the deeper restructuring planned as follow-up PRs (fetch artifact size each ~2 GB → overlay-only ~tens of MB; BQ upload incremental rather than WRITE_TRUNCATE — the latter pending careful design due to the `lightning_thunder_hour` 31,824 → 24,480 incident on 2026-04-17). Opus subagent review applied 1 SHOULD-FIX (soften "30+ min hang" comment wording to "exceeded 60 min timeout" since cancelled-job runner logs are not retained for direct hang-step evidence). Production verification deferred to the next cron run on master `24091959b`.


### Phase 2 (3) PR #129 production validation ✅ (2026-05-04)

The merge timeout fix (PR #129, master `24091959b`) was validated on the first post-merge cron run 25291434309 (head `b700959f`). All 18 steps of the merge job completed successfully in `47m54s` (vs the prior ~58 min ceiling-grazing baseline) with the breakdown: Step 11 Merge `9m12s` / **Step 12 Snapshot `<1s`** (was ~10 min) / Step 13 Coverage `1s` / Step 14 Upload checkpoint `3m56s` / Step 15 Upload to BigQuery `26m05s`. The Snapshot step's 10-min collapse to under one second confirms `src.backup(dst)` 2 GB rewrite + the duplicate `PRAGMA integrity_check` were the entire cost — the new `PRAGMA wal_checkpoint(TRUNCATE)`-only body adds no measurable time. The 150 min ceiling now provides ~100 min of headroom for future data growth.

Empirical breakdown was independently obtained by triggering the dedicated `Measure Integrity Check Performance` workflow on the post-merge state (run 25291668045 / 2026-05-03 21:50Z): `PRAGMA integrity_check` 122s, `PRAGMA quick_check` 16s, `scripts/check_db_integrity.py --mode=full` 124s. This refines the original PR #129 attribution: actual `integrity_check` was ~2 min and `src.backup(dst)` was ~8 min of the 10-min total, so the redundant `integrity_check` removal contributed ~20% of the saved minutes and `src.backup(dst)` removal contributed ~80%. The two-removal sum still lands at 10 min as expected, but the per-component cost ratio is now empirically grounded rather than estimated.

BigQuery `snet_waveform.distinct_days` advanced 1927 → 1940 (+13 days, `max_date` 2026-05-02 unchanged) on this single recovery cycle. The four preceding failed cycles (25276071313 / 25279499629 / 25283693176 / 25287932837) had run their `fetch-snet`/`fetch-hinet`/`light` jobs to success but failed at the merge job's Step 12 Snapshot, so Step 14 Upload checkpoint never ran — meaning the `data/geohazard.db` mutations from those four runs were not persisted to the cross-run artifact, and each subsequent run started from the same pre-failure checkpoint and re-fetched the same target dates only to lose them again at the next Snapshot failure. The +13 / cycle observation reflects normal `MAX_REQUESTS_PER_RUN=144` gap-driven progress for one successful cycle, not the 4-cycle accumulation initially expected. Catch-up to 100% snet completeness (~3,500 distinct days, 2016-08-15 to present) is therefore on a `+13/cycle × 8 cycle/day = +104 days/day` trajectory, projecting ~15 days from 2026-05-04 (i.e. ~2026-05-19), subject to NIED Hi-net session-cookie quota holding at the snet 144 + hinet 30 = 174 < 200/day margin.


### Phase 2 (3) PR-2 artifact overlay-only ✅ (2026-05-04)

PR #132 (master `c0b8e69`) reduces per-cron GHA artifact volume from `~11.3 GB` (six full-DB uploads at ~1.87 GB each) to a projected `~2.1 GB` total — a 5.4x reduction. The wasted ~9.2 GB came from the merge step's `scripts/merge_checkpoints.py --overlay PATH:TABLES` protocol, which only reads the 1–2 owned tables from each non-light artifact yet was being handed the entire `data/geohazard.db` (every fetcher's ~29 unrelated tables along with it). A new helper `scripts/extract_overlay.py` now builds a fresh, compact overlay DB containing only each job's owned tables via `ATTACH DATABASE 'file:src.db?mode=ro' + INSERT INTO main.<table> SELECT * FROM src.<table>` — read-only ATTACH hardens against accidental writes to the live fetch DB, and `VACUUM` at the end ensures the artifact reflects only used pages. Indexes are intentionally not copied, since merge_checkpoints.py reads each overlay only with linear `SELECT * FROM overlay.<table>` — no index lookups are performed and skipping them shrinks artifacts further with zero merge-step impact.

Per-fetch ownership and projected artifact sizes (full DB → overlay):

| Job | Owned tables | Projected overlay |
| --- | --- | --- |
| `fetch-modis` | `modis_lst` | ~30 MB (60x) |
| `fetch-so2` | `so2_column` | ~50 MB (38x) |
| `fetch-cloud` | `cloud_fraction` | ~40 MB (47x) |
| `fetch-snet` | `snet_waveform`, `fnet_waveform` | ~100 MB (19x) |
| `fetch-hinet` | `hinet_waveform` | ~5 MB (370x) |
| `fetch-light` | (full DB — base) | 1872 MB (unchanged) |

The light job continues to upload the full DB unchanged — it serves as `merge_checkpoints.py --base`, which must contain every table for the prior-cron snapshot rows to survive in tables whose owner job was skipped or cancelled.

Each non-light fetch job picks up an `Extract owned-table overlay` step inserted between the existing snapshot step and the upload-artifact step. The new step is gated by `if: steps.snapshot.outcome == 'success'` and uses `continue-on-error: true` with a `python3 scripts/extract_overlay.py ... && mv data/geohazard_overlay.db data/geohazard.db` chain, so an extract failure (unexpected schema, disk full, etc.) leaves the original `data/geohazard.db` in place and the existing `if: always() && steps.snapshot.outcome == 'success'` upload step still uploads it as the full DB — graceful degradation that falls back to the pre-PR-2 path with no regression. The merge step itself is unchanged: `merge_checkpoints.py --overlay PATH:TABLES` reads only the listed tables from each artifact and works identically against either a stripped overlay or a full DB. A `--src == --dst` guard in `extract_overlay.py` prevents the destructive case where a typo in workflow YAML could otherwise unlink the live DB before ATTACH could read it (covered by `scripts/smoke_test_extract_overlay.py` test 6 of 8). Schema verification across all 6 owned tables confirmed plain `id INTEGER PRIMARY KEY AUTOINCREMENT` + composite `UNIQUE(...)` (no `WITHOUT ROWID`, no `STRICT`), so `extract_overlay.py`'s `CREATE TABLE` copy from `sqlite_master.sql` works for every owned table.

Review trail (3 commits squashed into `c0b8e69`): initial `e26f22e` → CodeRabbit nitpicks `2196bf3` (`subprocess.run(timeout=120)` for CI hang prevention; `_qident()` SQL identifier quoting helper, defensive against future callers despite all current production names being simple ASCII) → Opus subagent re-review fixes `c38699d` (the `--src == --dst` guard above as MUST-FIX, URI-form read-only ATTACH and removal of unnecessary `journal_mode = MEMORY` as SHOULD-FIX). Smoke test grew 7 → 8 cases on RPi5 (Python 3.11 / sqlite3 3.40.1) covering schema preservation, multi-table extraction, missing-table tolerance, empty table, size reduction (336x on synthetic data), `--src == --dst` rejection, missing-source error, and `dst` overwrite. CodeRabbit "No actionable comments" on the final state. Production validation (actual artifact size measurement on the next successful cron run) is deferred to a follow-up entry once observed.


### Phase 2 (3) PR-2 production validation ✅ (2026-05-04)

PR #132 was validated end-to-end on cron run 25299739664 (the second post-PR-2 cycle, head `c0b8e69` (the PR #132 merge commit; PR #133 docs merged 2026-05-04 04:29Z, after this run was triggered at 03:35Z)). The merge job ran all 18 steps to success in `~41m34s`, with the breakdown: Step 11 Merge `10m42s` / **Step 12 Snapshot `<1s`** / Step 13 Coverage `19s` / Step 14 Upload checkpoint `3m33s` / Step 15 Upload to BigQuery `26m52s`. The `<1s` Snapshot step preserves the PR #129 effect for a second consecutive cycle, confirming the `src.backup(dst)` removal stays effective once new data is added.

Artifact sizes measured directly from the cron run's artifact metadata:

| Artifact | Size | vs full DB (~1885 MB each) | Projection |
| --- | --- | --- | --- |
| `backfill-light` | 1,885 MB | (base, unchanged) | 1,872 MB ✓ |
| `backfill-modis` | **8.7 KB** | 225,000x smaller | ~30 MB (vastly under) |
| `backfill-so2` | 299 MB | 6.3x smaller | ~50 MB (6x over) |
| `backfill-cloud` | 111 MB | 17.0x smaller | ~40 MB |
| `backfill-snet` | 63 MB | 30.0x smaller | ~100 MB |
| `backfill-hinet` | **91 KB** | 21,250x smaller | ~5 MB (vastly under) |
| **Total** | **2,358 MB** | **4.8x vs ~11.3 GB (6 jobs)** | ~2,097 MB / 5.4x |

The 4.8x reduction (Total vs ~11.3 GB of 6 full-DB uploads, vs the projected 5.4x) is dominated by `so2_column` running 6x over its projection — the table holds 19.6 M rows (the heaviest non-light table) and the projection underweighted it. The under-projection is the opposite direction: `modis_lst` (343 rows of monthly composites) and `hinet_waveform` (696 rows from Stage 1 deployment) shrink to bytes-class artifacts because their tables are nearly empty in absolute terms. The 4.8x ratio still represents `~9 GB` of GHA artifact storage avoided per cron cycle and a similar reduction in the merge job's 6-artifact download time, which is the dominant goal of PR-2 — the projection miss has no operational consequence beyond updating expectations for follow-up tuning.

BigQuery `snet_waveform` advanced 1,940 → 1,955 distinct_days (+15) on this single cycle, with `max_date` advancing 2026-05-02 → 2026-05-03 — the first front advancement observed since the PR #131 validation entry. The +15 / cycle delta is slightly higher than the +13 norm seen in the prior cycle, reflecting that `MAX_REQUESTS_PER_RUN=144` allocates work over different gap-day clusters as backfill progresses. Catch-up trajectory remains in the `+13–15 / cycle × 8 cycle/day = +104–120 days/day` envelope projected in the PR #131 entry, with the residual snet completeness gap of `1,955 → ~3,500` distinct days now estimated at `~13 days` (i.e. ~2026-05-17), shaving ~2 days off the prior 2026-05-19 projection.

Front-stall observation: as part of the same session a 31-table BigQuery coverage snapshot revealed three tables with stale fronts that were not previously catalogued — `gnss_tec` at `max_date=2024-11-20` (5.5 months stale), `so2_column` at `max_date=2025-06-08` (~11 months stale), and `iss_lis_lightning` at `max_date=2023-06-20` (3 years stale, likely upstream ISS LIS instrument shutdown). For the first two, root-cause inspection of `scripts/fetch_gnss_tec.py:445-447` and the analogous SO2 fetcher confirmed the design is correct: `dates_to_fetch = sorted(all_dates - skip_set)[:MAX_DATES]` is oldest-first, so recent dates sit at the tail of the missing-set queue and the four-cycle cron failure burst preceding PR #129 compounded the gap. No fetcher change is needed; the post-PR-#129 stable cron schedule should clear the gnss_tec backlog (728 missing dates at 200/cron × 8/day = ~1 day arithmetic, or ~1 week in practice including the existing failure-skip 30-day grace re-entries) and advance the front naturally, with so2 following at a slower rate due to denser fill-value periods in the 2011–2016 OMI archive. `iss_lis_lightning` is left as-is until upstream archive availability is confirmed.


### Phase 2 (3) Cosmic ray station expansion ✅ (2026-05-04)

PR #136 (master `92b4326`) expanded `fetch_nmdb_cosmicray.py` from 3 NMDB stations (IRKT/OULU/PSNM) to 9 stations (added APTY/JUNG/ATHN/ROME/BKSN/AATB) for global summed-rate density and improved Forbush-decrease detection latitude coverage. Total `cosmic_ray` rows grew to **47,018 rows** (2011-01-01 → 2026-05-05) at fixed 11-year retrospective coverage. Throughput cost: ~6 sec added per cron (NMDB API, BasicAuth, 1-req/station/day cycle), well within light-job budget. Feature `cosmic_ray_anomaly` reuses the same 27-day solar rotation baseline; the 9-station summed-rate denoises geomagnetically-induced bursts that single-station data could not separate from genuine anomalies.

### Light-artifact reset incident + Stage 1 fix ✅ (2026-05-06)

On 2026-05-05 05:46Z (cron run 25360056249, job 74357886362) a post-restore `PRAGMA integrity_check` on the ~1.87 GB `data/geohazard.db` ran past the 5-min `timeout 300` and was reported by `check_db_integrity.py` with `rc=124` (GNU coreutils timeout exit code). The Restore step's `case` statement only matched `rc=0`, so the timeout was misclassified as corruption: `Existing DB is corrupt -- removing and re-initialising` (06:08:42Z) → `init_db()` recreated 25 empty tables (06:08:43Z) → the ensuing Upload-to-BigQuery step's `WRITE_TRUNCATE` propagated the empty SQLite state to BigQuery, severely regressing 22 tables. Worst impact (BQ snapshot 2026-05-05 23:17Z): `ulf_magnetic` 9.07M rows (32% recovered), `tec` 1.33M rows (18%), `gnss_tec` 1.45M rows (24%), `iss_lis_lightning` 1,049 rows (14%), `swarm_em` 1,899 rows partial, and `ioc_sea_level` 2.51M rows (8%) regressed to 2011-07-29 max date.

PR #137 (master `f182e09`) hardens the Restore step in 6 fetch jobs (`fetch-modis`/`fetch-so2`/`fetch-cloud`/`fetch-snet`/`fetch-hinet`/`light`) on two axes: (a) `timeout` raised 300 → 1200 (4x) to accommodate growing DB size; (b) explicit `case "$integrity_rc" in 124|137|143)` branch logs `::warning::full integrity check timed out (rc=$integrity_rc) -- keeping DB (quick check passed at restore step)` and preserves the DB, falling back on the quick-check that already ran during artifact restore. Real corruption (any `rc` other than 0/124/137/143) still triggers the `init_db()` rebuild, but spurious timeouts no longer destroy progress.

### Stage 3 BQ regression guard ✅ (2026-05-06)

PR #138 (master `818a405`) adds a protective layer in `scripts/load_raw_to_bq.py` to prevent future SQLite-side data loss from propagating to BigQuery via `WRITE_TRUNCATE`. Mechanism: per-table, before the load job, `_query_bq_count(client, bq_table)` (fail-open semantics: `NotFound` → 0, any other exception → log warning + return 0) reads the current BQ row count. `_should_skip_regression(...)` then computes `ratio = sqlite_count / bq_count` and skips the upload when `ratio < REGRESSION_THRESHOLD = 0.5` (i.e. the SQLite side has lost more than 50% of BQ's row count). 13 smoke tests (`scripts/smoke_test_bq_regression_guard.py`) cover the count helper, the BQ-empty case, threshold edges, and the bypass path.

Operations control via two new Repository Variables: (1) `BQ_FORCE_OVERWRITE=true` is set during the post-incident recovery window so the 6 still-recovering tables can refill BQ without being blocked by the 50% guard — when the variable is true, `_should_skip_regression` short-circuits with `bypassing regression guard` warning. (2) `IOC_MAX_CHUNKS=300` (default 72) accelerates the slowest recovery path: `fetch_ioc_sealevel.py`'s per-cron chunk budget rises 4x (300 chunks ÷ 30 stations = 10 chunks/station/cron = ~300 days/station/cron), projected to shorten `ioc_sea_level` recovery from ~14.7 days (cron-default rate) to ~3.7 days (measured 14h-rate × 4). Both variables will be removed once recovery completes (~2026-05-13 projected); the guard then activates by default for normal operations.

### IOC sea level step timeout fix ✅ (2026-05-07)

PR #139 (master `9f1e87c`) bumps the `Fetch IOC sea level monitoring` step `timeout-minutes` from 15 to 35 in `.github/workflows/backfill.yml`. The hardcoded 15-min ceiling conflicted with Repository Variable `IOC_MAX_CHUNKS=300` (4x speedup vs default 72) introduced 2026-05-06 to compress `ioc_sea_level` recovery ETA from ~14.7d to ~3.7d, where worst-case execution under `IOC_MAX_CHUNKS=300` reaches ~25 minutes. Cron run 25448586828 (2026-05-06 16:43Z) hit `The action Fetch IOC sea level monitoring has timed out after 15 minutes` while `continue-on-error: true` masked the failure at the cron-aggregate level (no Discord alert); the next cron 25457367115 at 19:47Z completed in 10m14s, confirming non-deterministic outcome on gap distribution. Following the same pattern as PR #136 cosmic_ray (20→35), the step now has a 35-min budget with ~10min margin over the worst case. Scope deliberately narrow: `dart_pressure` (line 2041) and `IOC DART` (line 2067) keep their 15-min timeouts (unaffected by IOC sea level vars and still using default 72-chunk caps that complete well within 15 min).

### ISS LIS parallelization + Swarm window Variable extraction (in flight) (2026-05-07)

PR #143 (Draft, branch `perf/iss-lis-parallel-and-swarm-window`) parallelizes `scripts/fetch_iss_lis_lightning.py`'s granule download loop and externalizes `SWARM_MAX_DAYS` to a Repository Variable in `.github/workflows/backfill.yml`. The 2026-05-07 11:00Z BigQuery snapshot identified `iss_lis_lightning` (+10.2 d/cron, ETA 5/29) and `swarm_em` (+0.057 y/cron, ETA 5/27) as the two slowest critical-path tables under the post-`IOC_MAX_CHUNKS=300` rate, displacing the original 2026-05-13 completion ETA by ~16 days; ISS LIS' main loop was sequential (`for granule in granules: download → parse → per-granule SQLite commit → asyncio.sleep(0.3)`) at 1.73 s/granule × 700 granules = 1212 s/cron, and Swarm completed in 60 s (90-day window cap, polar-orbit Japan-bbox pass density ≈ 21 days/cron). The rewrite introduces `asyncio.Semaphore(PARALLEL_FETCHES)` + `asyncio.as_completed` + 50-granule batch insert; Swarm's existing inline `SWARM_MAX_DAYS=90` becomes `${{ vars.SWARM_MAX_DAYS || '90' }}` (script already env-driven, no code change). Defaults preserve prior behaviour (PARALLEL=1 keeps the 0.3 s rate-limit sleep per granule); merge alone changes nothing observable, and Repository Variables `ISS_LIS_PARALLEL_FETCHES`, `ISS_LIS_MAX_GRANULES`, `SWARM_MAX_DAYS` will be set post-merge to apply the speedup gradually. Projected effect: PARALLEL=4/GRANULES=1500 → +21.9 d/cron (ETA 2026-05-17); PARALLEL=6/GRANULES=2500 → +36.4 d/cron (ETA 2026-05-13). iss_lis step `timeout-minutes` raised 20→35 to fit the larger granule budget (cosmic_ray PR #136 / IOC PR #139 same pattern). Opus subagent review found no logic issues; smoke run **25500322730** (target=iss_lis on the PR branch, default values verifying backward compatibility) is in flight at the time of writing.

### Restore step timeout (5/5 reset incident recurrence) hotfix ✅ (2026-05-07)

PR #144 (master `e4a877c`) raises the `Restore previous database checkpoint` step `timeout-minutes` from 10 to 25 in all six fetch jobs (`fetch-modis` L147 / `fetch-so2` L399 / `fetch-cloud` L669 / `fetch-snet` L969 / `fetch-hinet` L1275 / `light` L1589 in `.github/workflows/backfill.yml`). On 2026-05-07 a BigQuery snapshot taken after cron run 25491776405 completed showed a -87% to -93% regression across 26 tables (`ulf_magnetic` 19.44M → 1.30M, `tec` 2.85M → 189K, `gnss_tec` 3.11M → 208K with max_date receding 8 years from 2019-03 to 2011-07, `ioc_sea_level` 31.48M → 3.93M with max_date receding 3 years from 2014-06 to 2011-10) — the same propagation pattern as the 2026-05-05 reset incident. Light job log analysis surfaced `[12:15:09] ##[error]The action 'Restore previous database checkpoint' has timed out after 10 minutes` from cron 25491776405; the on-disk DB had grown to ~15 GB (vs the typical ~1.8 GB) due to recovery-period free-page accumulation, causing the 479 MB checkpoint artifact's `unzip + cp` to exceed the 10-min step budget. With `continue-on-error: true`, the timeout was reported but execution continued into `Init DB if missing or corrupt`, where the `[ ! -f data/geohazard.db ]` test (or a follow-on quick-`integrity_check`) routed into `init_db()` — recreating 25 tables empty, after which the fetcher steps refilled to a thin state (cosmic_ray ran 31m38s vs the typical 2m20s, INTERMAGNET 18m43s vs 5s, both confirming a full-backfill-from-empty cycle), the snapshot step uploaded a ~179 MB thin artifact, and the merge job's `WRITE_TRUNCATE` propagated the thin SQLite state to BigQuery. The PR #138 regression guard (`ratio = sqlite_count / bq_count < 0.5 → skip`) was bypassed because Repository Variable `BQ_FORCE_OVERWRITE=true` had been set during the 2026-05-06 recovery window. PR #137 had already raised the post-restore integrity-check sub-shell `timeout 300 → 1200`, but that was a different timeout than the action step's `timeout-minutes: 10`, which remained unchanged from the pre-incident configuration and is the root cause this PR addresses. Scope deliberately narrow: only the six restore steps move; line 1922 (`fetch_iers`) and line 2011 (`fetch_goes_proton`) keep their unrelated `timeout-minutes: 10` settings. Smoke deferred (numeric-only, no logic change, emergency context); Opus subagent + caller diff verify + CodeRabbit `pass`; squash-merged 2026-05-07 23:33Z. Immediate post-merge action removed `BQ_FORCE_OVERWRITE` from Repository Variables to re-enable the regression guard. Cron 25523898337 (in-flight at the time of merge, running on the pre-PR-#144 workflow snapshot) completed shortly after with restore step `dur_sec=65` (the just-uploaded thin artifact extracted in seconds, fitting well under even the old 10-min ceiling), the fetcher steps populated the working DB, and the merge job's BQ load — now seeing the dynamically-fetched `BQ_FORCE_OVERWRITE` absence — let the guard evaluate `ratio = robust_sqlite / thin_bq > 1`, passing the upload and restoring the four worst tables to 26–35% of their pre-incident row counts in a single cycle (`ulf_magnetic` 1.30M → 5.18M, `tec` 189K → 761K, `gnss_tec` 208K → 832K with max_date advancing to 2013-03, `ioc_sea_level` 3.93M → 10.11M with max_date advancing to 2012-08); `swarm_em` 4,081 → 1,075 (max_date regressing to 2014-12) and `iss_lis_lightning` 2,057 unchanged are flagged for follow-up. The expression-timing observation (Variables resolve at step evaluation, not workflow-start snapshot) is documented for future incident response.

### Checkpoint restore timeout + snapshot VACUUM ✅ (2026-05-09)

PR #147 (master `57146ce`) addresses the next-cron restore-failure pattern that emerged after the 2026-05-08 `f12fc20` WAL leakage fix landed: post-fix snapshots grew to ~2.14 GB and the per-attempt `timeout 180` (3 min) inside the `Restore previous database checkpoint` step's artifact-fetch loop was too tight to download the most-recent checkpoint, causing every fresh cron to fall back to an older pre-fix snapshot. The 2026-05-09 03:27Z cron 25590448050 was the first observable case — its `gh run download` of artifact 6892725739 (2,144,848,871 bytes) consumed `8m07s` across two `timeout 180` retries before bailing to the 2026-05-08 19:19Z pre-fix snapshot 25574831633, voiding all backfill progress between 19:19Z and 04:37Z. The PR #143 single-cycle measurement of `iss_lis_lightning` +270 rows / `swarm_em` +270 rows turned out to be illusion under this fallback regime. Two-axis fix: (a) `timeout 180 → 600` across 6 artifact-fetch sites (light + 5 fetch jobs), and (b) `dst.execute('VACUUM')` after `dst.execute('PRAGMA journal_mode = DELETE')` in 5 snapshot Python blocks (light + 4 fetch jobs) — `src.backup(dst)` copies all pages including freed ones, so without VACUUM the snapshot stays at full DB size and exacerbates the timeout. The first push (`09f4bc0`) used `dst.execute("VACUUM")`, which the smoke run 25597338995 promptly broke with `Snapshot failed: name 'VACUUM' is not defined` — the outer `python3 -c "..."` shell expansion stripped the inner double quotes and `VACUUM` was evaluated as a Python identifier. The fix `a76af5e` switched to single-quoted `'VACUUM'` matching the surrounding `'PRAGMA journal_mode = DELETE'` convention. Smoke validation required two helper PRs (#149 schedule cron OFF, #151 schedule cron restore) because GitHub Actions concurrency group `backfill-data` enforces `in_progress=1 + queued=1 = 2` ceiling — the 12:52Z `schedule` trigger 25601598513 displaced the queued workflow_dispatch 25599920825, which was cancelled before getting CPU; PR ブランチ schedule comment-out is ineffective because schedule triggers fire on the default branch yaml. With schedule disabled by PR #149, run **25603689148** completed all 7 jobs success on the PR branch: fetch-hinet `Restore previous database checkpoint` 20m06s (vs 8m07s give-up under timeout 180, confirming the post-fix ~2.14 GB snapshot now fits in the 600s budget), `Init DB if missing or corrupt` 2m31s success (no `init_db()` reset triggered, confirming the restore-failure-induced overlay shrunk pattern is gone — the schedule run 25598309905 that completed 30 minutes earlier had logged `hinet_waveform: overlay has 725 rows but base has 7,705 rows (overlay shrunk by 6,980 rows) -- KEEPING base (likely cause: checkpoint restore failure or fetcher init_db reset)`), `Snapshot DB` 12m21s success (VACUUM completes the post-snapshot compaction without breaking the integrity check). PR #151 restored the schedule cron 39 minutes after PR #147 squash merge. Next-cron observation will quantify the row-growth restoration on the now-stable checkpoint chain.

### Restore step integrity check timeout regression ✅ (2026-05-10)

PR #153 (master `ab67bd09`) addresses a structural regression that surfaced in cron run 25621535145 (the first schedule-triggered run after PR #147 merge). Surface-level conclusion was `success` for all 7 jobs and BQ rows did increment (`hinet_waveform` +174 / `iss_lis_lightning` +51 / `swarm_em` +268), but inspection of the merge-job log showed three fetch jobs producing empty or near-empty overlays — `modis_lst: overlay has 0 rows but base has 297 rows -- KEEPING base`, `snet_waveform: overlay has 8,195 rows but base has 59,732 rows (overlay shrunk by 51,537 rows) -- KEEPING base`, `fnet_waveform: overlay has 754 rows but base has 3,786 rows (overlay shrunk by 3,032 rows) -- KEEPING base` — and the run survived only because the merge logic correctly preserved the base from `light.db`. The post-PR-#147 checkpoint snapshot is 14–15 GB (correctly compacted via the `'VACUUM'` single-quote fix, but still large because the underlying data is large), and the `Restore previous database checkpoint` step wraps integrity validation in tight timeouts that no longer fit at this size: `timeout 60 python3 scripts/check_db_integrity.py data/geohazard.db --mode=quick` fires (rc=124) on 14–15 GB DB — the empirically-observed full integrity check at the next step took 411 seconds, and the quick variant is plausibly 1–3 minutes — and `timeout 300 python3 scripts/salvage_db.py` also fires (rc=124) at this scale; the step-level `timeout-minutes: 25` then admits only 3 attempts (~7 min each: 2 min download + 60s integrity timeout + 300s salvage timeout) before the yaml-level kill fires. Evidence from the fetch-snet job log (run 25621535145, job 75209015888): `[06:12:09] trying artifact: backfill-checkpoint-25603689148` → `[06:15:11] copied (14G) -- checking integrity` → `[06:16:11] corrupted (rc=124) -- attempting per-table salvage` → `[06:21:11] salvage failed (rc=124) -- trying next artifact` (repeated 3 times, all rc=124, ending with `##[error]The action 'Restore previous database checkpoint' has timed out after 25 minutes.`). The DBs are not actually corrupt — rc=124 is GNU `timeout`'s exit code for the wrap timing out, not an `integrity_check` failure indication. The `light` job alone got lucky: its yaml timeout fell during the 3rd attempt's salvage phase (started 06:34:38, yaml killed at 06:38:32), leaving the 14–15 GB DB on disk for the subsequent `Init DB if missing or corrupt` step's `timeout 1200 python3 scripts/check_db_integrity.py data/geohazard.db` (full check, no `--mode=quick`) to validate; the full check completed in 411 seconds and returned OK, so `light.db` survived as the merge-base for all six overlay-eligible tables. The other fetch jobs (hinet, snet, modis) ended up with no DB → fresh `init_db()` schema → fetch step adds only newly-discovered rows → small post-fetch DB → small overlay → `KEEPING base` in merge. Fix in `.github/workflows/backfill.yml`: extend three timeouts uniformly across all 6 fetch jobs' Restore step blocks — `timeout 60 → 300` for `check_db_integrity.py --mode=quick` (5× buffer over observed ceiling), `timeout 300 → 600` for `salvage_db.py` (proportional to per-table copy time at 14–15 GB), and step-level `timeout-minutes: 25 → 60` (allow all 5 candidate artifacts to be tried at the new ~10-min-each pace). 18 insertions / 18 deletions, zero logic changes — only numeric ceiling extensions. Smoke deferred (numeric-only, no logic change, the in-flight cron 25623486360 fired at 07:58Z on the pre-#153 master `b0305559` and reproduces the same regression as a control); the next schedule cron 25626191594 (queued behind 25623486360 in the `backfill-data` concurrency group, fired post-merge with `headSha=ab67bd09`) is the verification probe — expected to surface `Restored from backfill-checkpoint-XXX` on attempt 1 (no `corrupted (rc=124)` lines) and produce non-shrunk overlays for `snet_waveform` ≈ 59,732 rows and `fnet_waveform` ≈ 3,786 rows, with BQ row deltas materialising across all overlay-eligible tables instead of only the `hinet_waveform`/`iss_lis_lightning`/`swarm_em` triplet that survived via `light`'s lucky-timing path.

### Cloud-native migration: retire RPi5 SSD, Hugging Face as canonical store ✅ (2026-05-28)

PR #166 (master `73b015b`) migrates the merge job off the retired RPi5 self-hosted runner + USB SSD primary tier (the SSD died permanently 2026-05-27 and is not being rebuilt; the RPi5 itself continues on microSD only). Until this PR the merge job was the sole consumer of `runs-on: [self-hosted, Linux, ARM64, rpi5-geohazard]` and the only writer to `/mnt/ssd/geohazard/geohazard.db`, so every scheduled backfill since the outage had been stuck `pending` on the offline runner — 28 consecutive cron ticks were `cancelled` by concurrency cleanup, the last surviving `backfill-checkpoint-*` artifact was a degraded ~587 MB salvage (run 25921630816), and the only intact 16.5 GB full-history copy was the public Hugging Face dataset [`yasumorishima/japan-geohazard`](https://huggingface.co/datasets/yasumorishima/japan-geohazard) (uploaded 2026-05-27 from the last good GHA checkpoint). Changes in `.github/workflows/backfill.yml`: (a) merge `runs-on: ubuntu-latest`, (b) scratch artifacts download to `/mnt/merge/{light,modis,so2,cloud,snet,hinet}` on the runner's ephemeral disk while `dst data/geohazard.db` stays on the root fs, after a leading `Free up runner disk` step reclaims Android/.NET/GHC/CodeQL toolcaches — splitting the ~33 GB peak (`merge_checkpoints.py` calls `shutil.copyfile(base, dst)` so base and dst co-exist) across two filesystems, (c) the `Mirror merged DB to RPi5 SSD` step is replaced by `Upload canonical DB to Hugging Face` which runs `scripts/hf_sync.py` (new). The HF upload is gated to `github.event_name == 'schedule' && github.event.schedule == '0 0 * * *'` — one ~16.5 GB git-LFS commit per day, not 8 — and `hf_sync.py` calls `HfApi.super_squash_history` after a verified upload to fold LFS history into a single commit (bounds `usedStorage`, otherwise 16.5 GB × N pushes would exhaust HF storage). Two defensive guards in `hf_sync.py`: (1) source must pass `PRAGMA integrity_check == 'ok'` before any upload (fail-closed); (2) **no-shrink guard** refuses upload when the source is smaller than `--min-fraction` (default 0.95) of the current remote `geohazard.db` — backfill grows monotonically, so a smaller DB signals a degraded/partial merge (e.g. built on a salvaged checkpoint), and without this guard such a run could overwrite then irreversibly squash away the full canonical history. The existing `backfill-checkpoint-<run_id>` artifact (retention 30 d) is unchanged and remains the per-run working state restored by each fetch job — GitHub-internal transfer, free, and the `master`-only / non-targeted gating on `Upload checkpoint artifact` (carried over from PR #4c/#4d) keeps a partial-DB dispatch run from poisoning the chain. New companion workflow `.github/workflows/reseed-checkpoint-from-hf.yml` (`workflow_dispatch` with required `memo`) downloads the canonical DB from HF via `hf download`, runs the quick integrity check, and re-publishes it as a fresh `backfill-checkpoint-<run_id>` (30-day retention) so the chain head can be re-seeded after any future outage that exceeds the artifact retention window. Verification ran in three layers: (1) reseed dispatch (run 26546306912) downloaded 17 GB to `data/geohazard.db`, `quick_check OK`, uploaded a 2.3 GB compressed artifact `backfill-checkpoint-26546306912` (2026-05-28 00:21:41Z, newest `created_at`, so the restore step's `sort_by(created_at)|reverse` picks it over the 587 MB salvage); (2) a targeted dispatch `target=earthquakes` (run 26547391358) ran the new merge end-to-end on `ubuntu-latest` (runner null, /dev/root 145 G, py 3.12.13) — merge job `df -h /` showed 90 G → 107 G free after toolcache cleanup, `Download light.db artifact` placed the full 16.5 GB base into `/mnt/merge/light`, `merge_checkpoints.py` correctly tolerated the absent skipped-fetch overlays (`overlay missing: /mnt/merge/{modis,so2,cloud,snet,hinet}/geohazard.db (tables [...] left as-is in base -- prior-checkpoint rows retained)`), `Snapshot merged DB`, `Coverage report`, and `Notify Discord` all `success`, while `Upload canonical DB to Hugging Face` and `Upload checkpoint artifact` were both `skipped` (confirming the schedule-only and master+non-targeted gates fire as designed for a feature-branch/targeted dispatch — neither the chain nor the HF canonical was touched by the proof); (3) the first full schedule cron after merge will exercise the same path with all six fetch jobs producing overlays and (on the UTC 00:00 tick) the live HF upload + squash. An Opus subagent code review flagged the 33 GB disk peak as the residual unknown, which the proof run resolved by measurement (107 G free ≫ 33 G peak). The Roadmap `Data Backfill` row and the `Persistence Tiers` table above are rewritten to match the new topology; the Historical BigQuery section is unchanged (BQ remains retired as of PR #156, 2026-05-11).

### Restore step timeout for the re-seeded 17 GB checkpoint (2026-05-28)

PR #168 (master `825cc4a`) raises the `Restore previous database checkpoint` step `timeout-minutes` across the fetch jobs to fit the re-seeded chain head. After PR #166 re-seeded the checkpoint chain from the 16.5 GB Hugging Face canonical (reseed run `26546306912` produced a 2.3 GB compressed / ~17 GB expanded `backfill-checkpoint-*`), the first full schedule cron's Restore steps needed more time to download and expand the much larger artifact than the prior thin checkpoints. Verified by the 2026-05-28 15:00 UTC cron (run `26585020487`): every Restore step completed successfully under the extended budget. That run then surfaced the next bottleneck — disk exhaustion — addressed in PR #169.

### Free disk space in the heavy fetch jobs (2026-05-29)

PR #169 (master `6c8992f`) adds a `Free disk space` step — the same ~30 GB toolcache reclaim (Android / .NET / GHC / CodeQL) the `light` job already runs — to the head of `steps:` (before checkout) in all five heavy fetch jobs (`fetch-so2`/`fetch-modis`/`fetch-cloud`/`fetch-snet`/`fetch-hinet`). The 2026-05-28 15:00 UTC full cron (run `26585020487`) confirmed PR #168's Restore timeouts but then hit `no space left on device` while expanding the 17 GB checkpoint into the restore tmpdir — `fetch-so2`/`fetch-modis`/`fetch-cloud`/`fetch-hinet` all failed and merge fell through; only `light` (which already had the step) and `fetch-snet` survived. Each GitHub-hosted job has its own ephemeral disk, so the identical step fixes all five. Verified: smoke run `26608470556` (target=so2) and full run `26611195476` (target=all) both passed every heavy job's Restore step.

### Cloud snapshot CLI verify uses quick_check (2026-05-29)

PR #170 (master `10873c8`) fixes the real reason `cloud_fraction` had stayed at 0 rows for ~13 days even though the PR #165 earthaccess fetch worked. The `fetch-cloud` Snapshot step ran `integrity_check` twice: an unbounded Python `src.execute('PRAGMA integrity_check')` (passes) and then a second `sqlite3` CLI subprocess running `PRAGMA integrity_check; SELECT COUNT(*) FROM cloud_fraction` with `timeout=600`. On the ~17 GB DB the CLI `integrity_check` exceeded 600 s (run `26599363437`), failing the snapshot, so `Extract overlay` and `Upload cloud.db artifact` were skipped and merge saw no cloud overlay — cloud_fraction stayed 0 despite the fetch producing 600,141 records. The CLI verify is switched to `quick_check` (full structural rigor is retained by the upstream unbounded Python `integrity_check`; the fresh-subprocess `COUNT(*)` still catches on-disk truncation / page corruption) and its timeout raised to 1200 s. Verified: full run `26611195476` merge coverage reported `cloud_fraction: 0 -> 600,141 rows` (2011-01-01 to 2013-06-18) and uploaded the checkpoint.

### Serialise master workflow_dispatch with the schedule cron (2026-05-29)

PR #171 (master `8346345`) keys the concurrency group off `github.ref` instead of `github.event_name`. Previously a manual `workflow_dispatch` on master (e.g. `target=all`) got group `backfill-data-refs/heads/master` while the cron used `backfill-data-master`, so the two never serialised and could run concurrently (observed 2026-05-29: dispatch `26611195476` overlapped schedule `26614399138`, each restoring its own base checkpoint, so they did not compound). The group is now `backfill-data-${{ github.ref == 'refs/heads/master' && 'master' || github.ref }}`: anything on master (schedule or manual dispatch) shares `backfill-data-master` and serialises, while a feature-branch dispatch keeps its own ref-scoped queue (preserving the PR-smoke isolation from PR #157). Back-to-back `target=all` dispatches now compound on the chain, enabling safe manual acceleration of the remaining backfill (cloud_fraction toward 2026; `gnss_tec`/`ioc_sea_level`/`fnet_waveform` remain source-API rate-limited).


### cloud_fraction snapshot disk-full fix → full 2000-2026 coverage ✅ (2026-05-30)

PR #172 (master `02f181a`) resolves the second disk-full failure that had stalled `cloud_fraction` at 2018-05 for ~13 days. After PR #170 unblocked the snapshot CLI verify, each fetch job's `Snapshot DB` step (`src.backup(dst)` makes a second ~17 GB copy of the working DB on the root fs) began overflowing the ~40 GB root filesystem as the DB grew: `fetch-so2`/`light` Snapshot steps failed with `database or disk is full`, their artifacts never uploaded, and merge collapsed with no cloud overlay applied. PR #172 extends the `Free disk space` step in all six fetch jobs from the partial `/opt/hostedtoolcache/CodeQL` reclaim to the full `/opt/hostedtoolcache` + `/usr/share/swift` + `/usr/local/.ghcup` + `/usr/local/share/boost` plus `docker image prune -af` (~+14 GB). Validated by dispatch `26665425278`: all six Snapshot steps passed and merge advanced cloud_fraction 1,794,367 → 2,388,663 rows (2018-05-23 → 2020-10-30). Three further runs compounded the chain: 2020-10 → 2023-04 → 2025-09 → **2026-05-29 (present day)**, **4,081,703 rows spanning the full MODIS Terra record from 2000-02-24**. cloud_fraction is now temporally complete. Secondary observation: as the DB grows, individual fetch jobs approach the 2 h job timeout, so `fetch-so2`/`light` were seen `cancelled` during the post-upload cleanup phase, but the artifact upload completes first so merge still succeeds and the checkpoint persists (run conclusion `cancelled` while the merge job is `success`). The durable `/mnt`-staged-snapshot fix considered earlier proved unnecessary: the toolcache reclaim alone keeps the root fs under budget. Remaining backfill is bounded only by external sources: `fnet_waveform` has a 2011-2023 gap and `gnss_tec`/`ioc_sea_level` are source-API rate-limited; ended-mission datasets (`lightning_lis_otd` 1995-2014, `iss_lis_lightning` 2017-2023) are already complete.

### fnet_waveform 3x backfill acceleration + durable /mnt snapshot fix (2026-05-30)

PR #174 (master `7f882dd`) makes the fnet backfill batch size configurable: `MAX_BACKFILL_DAYS_PER_RUN` now reads `FNET_MAX_BACKFILL_DAYS` (default 30, unchanged) from a repository variable. With `FNET_MAX_BACKFILL_DAYS=90` + `FNET_MAX_REQUESTS=90`, each `target=all` run fetches 90 backfill days instead of 30 -- keeping the per-run NIED request budget at snet 58 + fnet 90 ~= 148 (under the ~150 ceiling) and the fnet step at ~42 min (well under the 75 min budget). Verified on run `26688350050`: the step logged `Fetch schedule: 0 recent + 90 backfill = 90 items`, completed in 39 min with no throttling (`NoData:0`, only 3 genuinely-missing dates marked), and advanced the fnet frontier 2023-04-16 -> 2023-01-16 in a single run (coverage 11.8% -> 12.8%). This cuts the 2000-2023 gap-fill ETA from ~35 days to ~12 days at the existing 8 cron runs/day. Only `target=all` runs advance the checkpoint; targeted `target=fnet_waveform` dispatches are guarded out of checkpoint upload by design, so they never persist.

PR #175 (master `ab46542`) fixes a recurrence of the disk-full failure that PR #172's toolcache reclaim was thought to have settled. Once `cloud_fraction` reached its full ~4.08M rows, the DB grew enough that the `Snapshot DB` step's second ~17 GB `src.backup(dst)` copy -- plus the `VACUUM` temp, which SQLite places in `$SQLITE_TMPDIR`/`/tmp` (root fs) -- again overflowed the root filesystem: the `light` job's snapshot failed with `database or disk is full`, so merge refused with `base missing` and the checkpoint stopped advancing for ~5 consecutive runs (last good merge 02:14 UTC, run `26671709570`). The fix redirects all six snapshot steps' `src.backup` destination to `/mnt/snap` (the ~74 GB ephemeral scratch the merge job already uses) and sets `SQLITE_TMPDIR=/mnt/snap` so the VACUUM temp lands there too, dropping the root-fs peak from ~34 GB to ~17 GB. The earlier "durable /mnt fix unnecessary" note no longer holds -- the DB outgrew the toolcache headroom, making the /mnt move the structural fix. Validation run `26696792708` (target=all) is in flight at time of writing.

### light + fetch snapshot disk-full root cause: in-place finalise (drop backup+VACUUM) ✅ (2026-05-31)

PR #175's `/mnt/snap` redirect did not actually fix the disk-full: GitHub-hosted `ubuntu-latest` has a **single root filesystem with no separate `/mnt` mount** (verified by `df` inside the runner -- only `/dev/root 145G`, no `/dev/sdb` or `/mnt` line), so `/mnt/snap` was just another directory on root and `SQLITE_TMPDIR=/mnt/snap` gave the `VACUUM` temp no separate volume. Validation run `26696792708` therefore failed exactly as before: `light`'s `Snapshot DB` step hit `database or disk is full` and merge refused with `base missing`, leaving the checkpoint stalled. Root cause: the `light` job (and, until PR #179, every fetch job) `src.backup(dst)` the full DB (a 2nd ~24 GB copy) then `VACUUM` (a 3rd ~24 GB temp) on the same root fs -- ~72 GB demanded against ~119 GB free, which `cloud_fraction`'s 100% fill pushed over the edge.

PR #177 (master `38950241`) finalises the `light` snapshot **in place**: `wal_checkpoint(TRUNCATE)` -> `journal_mode=DELETE` (keeps the 2026-05-08 Tree 98196 malformed-image guard) -> full `integrity_check` (fail-closed) -> header `page_count*page_size == filesize` check, dropping the backup copy and `VACUUM` entirely (root peak ~72 GB -> ~24 GB). Since `light` owns the full base DB, the working file *is* the artifact. PR #179 (master `29c34713`) applies the identical in-place finalise to the 5 fetch jobs (`fetch-modis`/`so2`/`cloud`/`snet`/`hinet`) -- each only extracts a small owned-table overlay afterwards, so the full backup was never needed. Verified end-to-end on dispatch `26727326390`: all 6 fetch + light snapshots succeeded, merge succeeded, the checkpoint persisted, and `fnet_waveform` (owned by `fetch-snet`, which had been failing every cron on disk-full) advanced 2023-01-20 -> 2022-10-26 (+1,342 rows) -- unblocking the backfill the disk-full had stalled.

### Drive the last 3 sub-100% tables to completion: gnss_tec timeout + ioc chunk cap (2026-06-01)

With `cloud_fraction` complete and the snapshot pipeline healthy, a code+log audit isolated the only three tables short of full coverage and classified each by its true blocker (none is a source ceiling): **fnet_waveform** (`2022-10-26 ->`, MIN-side 2011-2022 gap) is rate-limited by the NIED ~150 req/run budget (snet 60 + fnet 90 = 150) and self-completes in ~6 days of the existing cron -- left unchanged; **gnss_tec** (stuck at `2025-09-03`) was a genuine bug, its `Fetch GNSS-TEC` step `timeout-minutes: 30` killing the step before it could clear its 398 missing dates (`MAX_DATES=200`/run, 12 MB/file, parallelism 4); **ioc_sea_level** (`-> 2019-11`) is an oldest-first 30-day-chunk walk that processed 250 chunks / ~7 M rows in ~10 min of its 35 min budget.

PR #181 (master `7be8429e`) raises only the `fetch_gnss_tec` step (unique `id: fetch_gnss_tec` anchor) `timeout-minutes` 30 -> 90; the `light` job's 200 min budget absorbs it (historical ~112 min, worst case ~172 min). Repository variable `IOC_MAX_CHUNKS` was raised 300 -> 500 (still within the 35 min step budget at the unchanged parallelism-2 / 1 s-sleep pace, so no extra load on the IOC API). Recent ~2 months of gnss_tec are genuine Nagoya-ISEE publication lag and self-fill as the source publishes. Verification dispatch `26755016749` was cancelled, and #181 in fact triggered a `light`-job timeout regression (the 90-min GNSS fetch pushed the ~24-fetcher `light` monolith past its 200-min budget) -- fixed in the next entry.

### gnss_tec split out of `light` + restore quick_check timeout accept (cron regression fix) ✅ (2026-06-03)

PR #181's `fetch_gnss_tec` 30 -> 90 min step bump did not fit the `light` job after all: combined with the upstream TEC steps, the 90-min GNSS fetch pushed the ~24-fetcher `light` monolith past its 200-min job timeout mid-GNSS (run `26770811693`), cancelling the job, failing its `Snapshot DB` step, producing no `light.db` base, and collapsing merge with `base missing and --require-base set`. Every schedule cron went red and `fnet_waveform`/`gnss_tec`/`ioc_sea_level` stalled. The #181 verification dispatch `26755016749` had itself been cancelled, so #181 shipped without a green end-to-end run.

PR #183 (master `72dece92`) splits `gnss_tec` into a dedicated `fetch-gnss` job -- the same overlay pattern `cloud_fraction` uses, cloned from the proven `fetch-so2` job minus its Earthdata step, with the #177/#179 in-place snapshot (no backup/VACUUM), job timeout 210 / fetch step 120 -- and removes the GNSS step + output from `light` (dropping it back to ~123 min) while wiring the gnss overlay, download, `needs`, and auto-rerun condition into merge. Branch verification `26798010229`: light 123 min, fetch-gnss success (gnss_tec 2025-09-03 -> 2025-11-08), merge success.

The first master self-heal run `26817022739` was then cancelled by a deeper, pre-existing cause that the fast-restore branch run had masked: the canonical DB had grown to ~28 GB (cloud_fraction at 100%), and the Restore step's `timeout 1200 ... check_db_integrity.py --mode=quick` no longer finishes within 1200 s, returning rc=124 (GNU `timeout`'s exit code). The restore loop misread that timeout as corruption, ran a per-table salvage that also timed out (1800 s), discarded the 28 GB artifact, and retried the next -- burning the entire 90-min restore budget without restoring (`copied (28G)` -> `corrupted (rc=124)` -> `salvage failed (rc=124)`, twice), so `light` again hit its 200-min timeout.

PR #186 (master `bad111be`) treats integrity rc 124/137/143 as accept-the-restored-full-DB instead of corruption, across all 7 restore blocks (modis/so2/cloud/snet/hinet/light/gnss). This is safe because the checkpoint was already integrity-verified when the merge job created it (`merge_checkpoints.py:_verify`), the `Init DB` step already tolerates the same timeout codes (`keeping DB`), and the `Snapshot DB` step re-verifies fail-closed; it also removes the empty-base zero-out risk of falling through to a fresh `init_db()`. Verified end-to-end on full-fix dispatch `26859610782` (master `bad111be`): light success in 148 min (restore 21 min, no timeout), merge success, checkpoint uploaded (chain advanced), coverage `gnss_tec -> 2025-12-02`, `ioc_sea_level -> 2022-02-01`, `fnet_waveform` MIN -> 2021-08-12, `cloud_fraction` 100%. That run's overall conclusion was `failure` only from non-blocking `fetch-hinet` (transient runner abort) + `fetch-so2` (120-min job-budget cancel) -- both overlays skipped with base retained, merge still succeeded. Residual non-blocking follow-ups: as the DB keeps growing, `quick_check` will routinely time out so restore's integrity gate effectively shifts to the Init/Snapshot steps (a future timeout raise or verification-method change), and the tightest-budget fetch jobs (`fetch-so2`) approach their job timeouts.

### fetch-hinet runner-abort: auto-rerun band-aid -> per-job overlay restore (root fix) (2026-06-03)

The same ~28 GB canonical DB behind PR #186 also made the **fetch jobs themselves** fragile. Each split-out fetch job (`fetch-modis`/`so2`/`cloud`/`snet`/`hinet`/`gnss`) restored the full `backfill-checkpoint-` artifact (~28 GB, ~17 GB compressed) on its `Restore previous database checkpoint` step purely to resume an incremental fetch of its own one or two tables -- even though it only ever *uploads* a small `backfill-<job>-` owned-table overlay (via `extract_overlay.py`). Expanding the 17 GB artifact to ~28 GB on a disk/RAM-constrained `ubuntu-latest` runner intermittently aborted the runner mid-restore (lost-communication: the step has no conclusion and the job log is `BlobNotFound`). `fetch-hinet` drew the short straw on two consecutive runs (`26859610782`, `26860883895`), stalling `hinet_waveform`.

PR #188 (master `0eb9c3e3`) added `fetch-hinet` to the merge job's `Auto-rerun failed fetch jobs` condition (it had been omitted when Hi-net was added later than the other fetch jobs), giving hinet the same one-shot rerun-on-a-fresh-runner the other jobs already had. But that only *retries* the runner death; the 28 GB restore load is unchanged.

PR #189 (master `1087b047`) removes the load itself: each fetch job now restores its **own** small `backfill-<job>-` overlay first (it already carries every accumulated row of the owned table), falling back to the full `backfill-checkpoint-` / `database-checkpoint-` only when no overlay exists (first run / 7-day artifact expiry / chain break). Own-overlay acceptance is strict (clean `rc=0` quick_check **and** owned table(s) non-empty, else discard and fall through to the full checkpoint). `fetch-modis` is deliberately excluded (kept on full-checkpoint restore) because `fetch_modis_lst.py` reads the cross-table `earthquakes` master to pick its M5.5+ fetch targets, which a `modis_lst`-only overlay would not carry; `light` (the base builder) is also unchanged. Selection is by `${{ github.job }}` so all 7 restore blocks stay byte-identical (modis/light hit the empty default). Verified end-to-end on run `26887516355` (master `1087b047`): `fetch-hinet`/`so2`/`cloud` each restored their own overlay in seconds with no runner abort or disk-full -- `Restored from own-overlay backfill-hinet-26872438320 (hinet_waveform total=33,104 rows)` in ~3 s, `backfill-so2-... (so2_column total=19,638,143 rows)` in ~22 s, `backfill-cloud-... (cloud_fraction total=6,341,996 rows)` in ~7 s -- versus the ~20-minute full restore that had been killing the runner. This eliminates the runner-abort failure mode and keeps restore cost flat as the DB grows.

### so2_column + ioc_sea_level confirmed at present day; data side ready for analysis (2026-06-05)

Production confirmation that the two forward-stale fixes shipped on 2026-06-04 actually reached the present day. On the latest successful schedule run `26950644293` (merge 2026-06-04 18:15 UTC) the merge coverage log reports `so2_column: 20,368,938 rows (2004-10-01 -> 2026-06-03)` and `ioc_sea_level: 169,187,466 rows (2011-01-01 -> 2026-06-04 16:30)` -- both at the current day, validating PR #191 (OMSO2G.004 repoint, previously stalled at 2025-06) and PR #192 (dual-ended IOC chunk walk, previously stalled at 2023-10). `gnss_tec` sits at 2025-12-22 (Nagoya-ISEE publication lag, at the source frontier). The only remaining moving gap is `fnet_waveform`, whose MIN is walking backward toward 2000 (now 2019-03-03, ~2-3 weeks of cron remaining) -- but `fnet_waveform` is a raw seismic data asset that appears only in the fetch/merge/validate/BigQuery paths and is **not** a model feature (the waveform feature group in `src/features.py` is `snet_waveform`, already at present day), so its backfill does not block the analysis/ML pipeline. With the 2026-06-04 forward-feature-freshness audit (Phase 21) complete and so2/ioc now confirmed fresh at inference time, the active feature set is clean for a fresh retrain -- the data side is ready for full-scale analysis (Phase 19 ML retraining).

### HF upload no-shrink guard: byte-size → row-count manifest (2026-07-05)

The daily Hugging Face upload (`scripts/hf_sync.py`, UTC-00:00 cron only) silently stopped landing for ~11 days — the canonical `geohazard.db` was last committed 2026-06-24, and every daily `merge` job failed at the "Upload canonical DB to Hugging Face" step with `refusing to upload -- source DB is smaller than 95% of the remote canonical copy`. **No data was lost.** The `merge` Coverage report confirmed the produced DB was complete and current (`ioc_sea_level` 49,323,251 rows to 2026-07-04, `ulf_magnetic` 9.07M, etc.); the DB had simply been VACUUM/compacted from a bloated ~44 GB to a lean ~18 GB while row counts stayed identical or greater. The old no-shrink guard compared raw **file bytes** (`local < 0.95 × remote`), so a legitimate compaction was mistaken for a degraded/partial merge and blocked forever (the remote stayed the bloated 44 GB, unreachable at the 95% floor). Rows, not bytes, are the real degradation signal.

Fix (commits `3072403` + `5dba70e`): `hf_sync.py`'s byte guard is replaced by a **row-count no-regression guard** — per-table counts are compared against a small sidecar manifest `geohazard.db.rowcounts.json` stored next to the DB on HF; upload is refused only if a table drops below `--min-fraction` (0.95) of its previous count (a dropped/truncated table), while a compaction that preserves rows passes cleanly. Retained: `PRAGMA integrity_check` (fail-closed) plus a new absolute floor `--min-abs-gb` (5 GB) that catches a catastrophically tiny merge even with no prior manifest. The post-upload manifest refresh + `super_squash_history` are best-effort, so a successful DB upload is never re-reported as a job failure (which had been firing a daily error email). The manual `hf-upload-checkpoint.yml` reseeder was routed through the guarded `hf_sync.py` (previously a raw `hf upload` that bypassed every safeguard). Verified end-to-end on run `28733347900`: the canonical refreshed to 2026-07-05T07:38 (super-squash applied) and the manifest was seeded with all 42 tables — pipeline un-stuck, and the daily upload now compacts-and-uploads without false failures. A daily CI-health cron agent additionally checks the canonical's freshness (not just the run's colour) so any future stall is caught proactively rather than by an error email.

### Merge gated on the light base job: no failure email from GitHub runner-capacity incidents (2026-07-10, `10eb1f6`)

On 2026-07-09 a GitHub-side hosted-runner capacity incident ("The job was not acquired by
Runner of type hosted even after multiple attempts" — the annotation on every cancelled
fetch job of run 29016325610) cancelled all fetch jobs mid-run. The merge job runs under
`if: always()` and its artifact downloads are `continue-on-error`, but the merge step then
executed `merge_checkpoints.py --require-base` with no base artifact, exited 1, and turned a
self-healing infrastructure hiccup into a run-conclusion `failure` and a notification email.
No data was affected — the checkpoint chain simply resumed on the next 3-hour cron (run
29055052853 completed fetch→light→merge fully green). Fix: the merge step is now gated with
`if: needs.light.result == 'success'`. When the base job was cancelled by infra the merge
SKIPS (run stays `cancelled`, which does not email — verified against the 7 email-less
cancelled runs that same day), while a genuine light-job failure still fails the run (and
emails) via the light job itself. Downstream steps already key off merge/snapshot success
and skip cleanly; the Discord notifier still posts its skip embed. Reviewed (no findings)
with each claim re-verified against the workflow source.

### Checkpoint chain expiry: the base DB was silently rebuilt from empty (2026-07-24, recovered 2026-07-29, PR #197; guard extended 2026-08-01, PR #198)

The `backfill-checkpoint-*` artifact chain expired and the `light` job rebuilt the canonical
history from an empty file without any signal. The restore log of run 30078904889 -- the first
success after a nine-day drought -- is unambiguous: all five `backfill-checkpoint-` candidates
and all three legacy `database-checkpoint-` names failed to download, then
`No usable checkpoint found -- will init fresh DB` followed by
`Database initialized: ./data/geohazard.db`. Two conditions had to coincide. `e759fe5`
(2026-07-15) cut checkpoint retention from 30 days to 3; and every scheduled run between
2026-07-15 08:08 and 2026-07-24 08:26 was cancelled while queued -- a run takes longer than the
3-hour cron interval, so under `cancel-in-progress: false` the queued duplicate is dropped -- so
no new checkpoint was produced for nine days and the three-day chain aged out.

**No data was lost.** `hf_sync.py`'s row-count no-regression guard (added 2026-07-05, see the
section above) did exactly what it exists for and refused every subsequent publish
(`refusing to upload -- row-count regression in 9 table(s)`), leaving the canonical copy on
Hugging Face intact. The rebuilt chain against that canonical copy, from the coverage report of
run 30363600697: `tec` 0 rows (canonical 6,440,742), `iss_lis_lightning` and `nightlight` missing
as tables (952 / 929), `ioc_sea_level` 120,280,581 (176,345,275), `dart_pressure` 394,999
(973,419). `gnss_tec` and `hinet_waveform` had meanwhile grown past the canonical counts, so the
per-job overlay artifacts still carried rows worth keeping.

The degradation ran for five days unreported because `Create issue on failure` was wired only to
the fetch-step outputs and `steps.merge.outcome`. The Hugging Face upload is a later step in the
same job, so a guard trip failed the run while matching no clause -- the 2026-07-25 and
2026-07-27 failures filed nothing, and the last auto-filed issue was from 2026-06-03. The same
failure also skipped `Upload checkpoint artifact` (a plain `if: steps.snapshot.outcome ==
'success'` does not run after a failed step), so each guard trip additionally cost that run its
checkpoint -- thinning the very chain whose thinning caused the incident.

Fix (PR #197, `bad49202`): the `light` job gains a `Guard against rebuilding the base DB from
empty` step that fails a scheduled run when the restore step reports `restored=none` instead of
initialising an empty DB; the Hugging Face step gains `id: hf_upload`; `Create issue on failure`
now also fires on `steps.hf_upload.outcome == 'failure'` and on
`needs.light.outputs.restore_restored == 'none'`, both bypassing the attempt-1 suppression
(an auto-rerun cannot un-expire artifacts); and `Coverage report` / `Upload checkpoint artifact`
move to `(success() || failure())` so a guard trip no longer starves the chain.
`restore_restored` was already exported by the `light` job and had no consumer until now.

Recovery: `reseed-checkpoint-from-hf.yml` (run 30431629728) pulled the canonical DB -- **42 GB**,
not the ~16.5 GB its header comment still assumed -- passed `check_db_integrity.py --mode=quick`,
and republished it as `backfill-checkpoint-30431629728` at the head of the chain with 30-day
retention. The schedule was disabled for the duration so no degraded checkpoint could supersede
it, since runs are serialised and the next one would otherwise restore the old chain before the
reseed landed. `merge_checkpoints.py` keeps base rows whenever an overlay is smaller, so the
newer `gnss_tec` / `hinet_waveform` rows in the surviving per-job overlays merge back on top of
the restored canonical base rather than being discarded. Verification of the first full run on
the reseeded chain (run 30439350491) was still in progress when this entry was written.

Follow-up (2026-08-01, PR #198, `1bfc9112`): the recovery is verified closed -- the Hugging Face canonical resumed updating on 2026-07-31 with the row-count guard passing, its manifest shows the degraded tables restored (`tec` 6,440,742 rows, `ioc_sea_level` 176,594,085, `iss_lis_lightning` and `nightlight` present again), and five fresh checkpoint artifacts were produced on 2026-07-31 alone, so the chain is rebuilding its own depth. One gap remained: the #197 guard fires only on `restored=none`, while the restore loop has a second way to hand back a thin base -- the `<artifact>-salvaged` per-table copy out of a corrupt checkpoint, table-incomplete by construction. `scripts/check_base_rowcounts.py` now compares whatever base a scheduled run restored against the `geohazard.db.rowcounts.json` manifest that `hf_sync.py` publishes next to the canonical DB (public dataset, no token; per-table 95 percent floor, matching hf_sync.py's own guard; a missing table reads as zero rows; an unreachable manifest warns and passes, because a Hugging Face outage must not stop the pipeline), so a degraded base of either kind stops a scheduled run before it spends three hours fetching onto it, and `Create issue on failure` gains a `degraded_base` clause wired the same way as the #197 clauses.


### Artifact inventory bounded by count, and three quiet failure modes in the fetch jobs (2026-08-06, PRs #199-#202)

A "100% of Actions storage" alert traced back to the same knob that caused the 2026-07-24 chain
expiry, turned the other way. The live inventory measured 614.05 GB against a 0.5 GB included
quota: `backfill-checkpoint-` 62 artifacts / 381.72 GB, `backfill-light-` 31 / 195.21 GB, the rest
37 GB. Billing is not the issue, since public-repository usage is discounted to net zero (August,
this repo: $84.22 gross, $0.00 net), but exceeding the quota hard-fails `actions/upload-artifact`
across every repository on the account, so the inventory has to come down regardless.

Cutting `retention-days` is the wrong lever, and cutting it is exactly what broke the chain on
2026-07-24. The checkpoint chain is the pipeline's working state, so retention is really the number
of days the schedule may stall before the chain dies; storage meanwhile is copies times size
(~6.2 GB each, 8 runs/day). Shortening retention therefore trades the safety margin away for a
third of the space. PR #199 inverts it: `retention-days` goes back to 30 for `backfill-checkpoint-`
and `backfill-light-`, and a `Prune old artifacts` step in `merge` keeps the newest 8 of each prefix
and deletes the rest. 8 is one day of runs rather than a bare minimum, because two consumers need
history -- the restore step falls back to progressively older checkpoints when the newest fails its
integrity check, and `diagnose-merge.yml` pulls `backfill-light-<arbitrary run>` to isolate which
overlay introduced a corruption -- and artifact deletion, unlike expiry, destroys that evidence.
Running the shipped script against the live inventory took it to 137.94 GB (8 checkpoints /
50.41 GB, 8 light / 50.41 GB, the per-table overlays left untouched on their 7-day retention at
~25 GB). The prune step is deliberately not gated on the checkpoint upload succeeding: while the
account is over quota every upload hard-fails, so such a gate would deadlock the one step that
frees the space.

Investigating why `fetch-modis` was cancelled at 155 minutes on every scheduled run -- `modis_lst`
frozen at 316 rows / 2026-06-18 -- found the MODIS fetch itself healthy (66 minutes, 413 records)
and the budget spent elsewhere. Run 31075628300, job 92535296284: `gh run download` attempt 1 died
at exactly rc=124 / 600 s and the step fell back to an older checkpoint; attempt 2 plus the copy of
the 42 GB DB took 12 minutes; the restore `quick_check` timed out at 1200 s and was accepted anyway;
the Init DB full `integrity_check` timed out at 1200 s and kept the DB anyway; and `Snapshot DB` was
killed 25 minutes in. Fifty minutes of a 150-minute budget went to checks whose result is discarded
by construction. PR #200 raises the job to 240 minutes -- `fetch-modis` and `light` are the only
jobs that restore the full 42 GB checkpoint (`OWN_PREFIX` is empty for both), and `light` was
already at 200 while `fetch-snet` and `fetch-hinet` were at 240 -- widens the download window to
1800 s in all seven restore blocks, and skips the Init DB full check above 20 GiB. Nothing is lost
by skipping it: corruption is gated at write time by `merge_checkpoints.py`'s `_verify` and by the
untimed `PRAGMA integrity_check` in `Snapshot DB`, both of which must pass before an artifact is
uploaded.

The next run exposed a second, quieter mode. Run 31084273068's `fetch-modis` reported success in 52
minutes: five consecutive 600 s download timeouts, the legacy `database-checkpoint-` fallback
404ing, then `No usable checkpoint found -- will init fresh DB`, `TOTAL: 0 earthquake + 0 control =
0 LST records`, and a 159744-byte overlay uploaded green. `merge_checkpoints.py` refused it
(`overlay empty ... KEEPING base`, the guard added after the 2026-04-18 `cloud_fraction` 523K to 0
regression), so no data was lost -- but the 50 minutes of failed downloads were invisible unless you
opened the restore log. The `Guard against rebuilding the base DB from empty` added to `light` after
the 2026-07-24 incident had never been extended to the six fetch jobs. PR #201 adds it to all six
and closes two holes in it. The condition matched only `restored == 'none'`, but the restore step is
`continue-on-error` with `timeout-minutes: 90` and writes that output on its last line, so a step
killed at 90 minutes leaves it empty -- now the likely path rather than a corner case, since three
hung 1800 s downloads reach the 90-minute cap where five 600 s ones used to fit inside it. And a
guard failure reached neither alert path, because `RESTORE_*` carries the restore *step* outcome
(success, since the step itself finished) while every `fetch_*` output is `skipped` rather than
`failure`; the job results are now wired into both the Discord `step_results` and the
`Create issue on failure` condition.

PR #202 closes the last route in. Both the guard and `light`'s `base_check` were keyed on
`github.event_name == 'schedule'`, so dispatching this workflow on master while the chain was gone
bypassed them -- and a master dispatch with target `all` or `''` is explicitly allowed to upload a
checkpoint, which would make the empty base the next base and let every later scheduled run sail
past the guard on `restored != 'none'`. Both are now keyed on `github.ref == 'refs/heads/master'`: a
scheduled run always runs on the default branch (the checkpoint upload step already relies on this),
and a feature-branch dispatch cannot upload a checkpoint, so it stays exempt. The daily Hugging Face
upload and the auto-rerun of failed fetch jobs remain schedule-only by intent.

Verification, first scheduled run carrying the changes (31111077876, fetch-modis job 92648738758):
restore 27m34s against 41m43s before, the Init DB full check 0s against 20m (skipped by size), the
MODIS fetch 62m40s, and `Snapshot DB` **completed for the first time, in 34m56s** -- the step that
had only ever been measured as a 25-minute lower bound before being killed. Job total 135m08s
against the new 240-minute budget, so the budget now rests on measurement rather than arithmetic.
The empty-base guard correctly did not fire, the restore having succeeded.

The run failed anyway, for a reason outside this pipeline: GitHub opened a critical Actions
incident at 15:22 UTC that day and the Actions component went to major outage. Both full-restore
jobs were annotated `The hosted runner lost communication with the server` and the merge job
`was not acquired by Runner of type hosted even after multiple attempts`; the next scheduled run sat
queued for over two hours. fetch-modis died during `Upload modis.db artifact`, after `Snapshot DB`
had already succeeded -- so the budget question is answered, but `modis_lst` has still not advanced,
and the prune step has not yet been observed running on a real merge.

That outage also exposed a second-order effect worth recording. `retention-days: 30` only applies
to artifacts a *post-fix run* uploads, and no run has managed to upload one -- so the nine live
checkpoints are all still on the old 3-day retention, and the newest expires 2026-08-09T14:10:05Z.
If the incident outlasts that, the chain expires exactly as it did on 2026-07-24, with the
difference that the guard added above now fails the run loudly instead of rebuilding from empty,
and the Hugging Face canonical is current (last modified that morning, 44,614,766,592 bytes /
41.55 GiB). Recovery would be `reseed-checkpoint-from-hf.yml`, which uploads with
`retention-days: 30` and so closes the exposure in one run. Checking that it could actually do
that turned up the same mis-sized-budget shape a third time: its only successful run
(30431629728, 2026-07-29) used 41m05s of a 60-minute job budget -- disk 30s, HF download 2m16s,
integrity check 25m14s, artifact upload 12m58s -- and both dominant steps scale with a database
that grows daily. A 19-minute margin on the one workflow that runs only when everything else has
already failed is not a margin, so the budget is now 120 minutes, and the `~16.5GB` figure its
header comment and disk-cleanup step still carried is corrected to the measured size. (The
25m14s integrity check also independently corroborates the `quick_check` timeout above: the same
check is what exceeds the 1200-second window inside the backfill restore step.)


### The three open items from the section above, closed by measurement (2026-08-07, run 31132546412)

The GitHub Actions incident was still open -- the status page reported the Actions component in
major outage -- when a scheduled run acquired runners and completed. It is the first complete
backfill since the incident began, and it settles all three things the section above left hanging.

The budget fix holds in production. All seven fetch jobs finished `success`, `fetch-modis` among
them; the job that had been killed by its own 150-minute limit, then killed again by the outage
mid-upload, now runs end to end. The empty-base guard added in PR #201 correctly did not fire,
the restore having found a live checkpoint, which is also the first evidence that the broadened
condition does not misfire on the normal path.

`retention-days: 30` is now actually in effect. The checkpoint this run uploaded,
`backfill-checkpoint-31132546412`, was created 2026-08-07T04:10:22Z and expires 2026-09-06 --
thirty days, against the three-day artifacts every previous run had left behind. The chain
deadline of 2026-08-09T14:10:05Z is therefore no longer live, and `reseed-checkpoint-from-hf.yml`
was not needed.

The prune step ran, and its log is the proof rather than the artifact count:

```
backfill-checkpoint- : 10 live (keep 8)
  deleted 8919925468
  deleted 8914736261
backfill-light- : 10 live (keep 8)
  deleted 8917607128
  deleted 8913384780
backfill-modis- : 3 live (keep 8)
  nothing to prune
```

Both full-size prefixes now sit at exactly eight live artifacts, and the small `modis` prefix
correctly took no action. The inventory is bounded by count as intended.

One item did not resolve, and the honest reading of it is not the obvious one. `modis_lst` is
still 316 rows ending 2026-06-18: the merge reports `316 -> 316 rows (+0)` with the overlay
applied, not rejected. The fetch itself is not failing -- it walked all 625 land events and
returned `413 records from 613 fetches`, plus 77 control records -- so what it produced this run
were rows whose `(latitude, longitude, observed_date)` keys already existed. The first hypothesis,
that the event list was stale, is wrong: the same merge reports `earthquakes: 28,955 rows` running
to 2026-08-06T17:50, and there are fifteen M5+ events in the box after 2026-06-18, seven of which
clear the script's `magnitude >= 5.5` and land-polygon filters, including the M6.8 of 2026-07-28.

What the coverage report does show is that every satellite-derived table lags by a comparable
margin: `so2_column` ends 2026-06-08, `cloud_fraction` 2026-06-29, `gravity_mascon` 2026-04-16.
A `modis_lst` frontier of 2026-06-18, read on 2026-08-07, sits inside that band rather than
outside it, and the M6.8 is ten days before this run -- too recent for an eight-day composite to
have been published and subset. So the evidence available supports product publication latency
over a defect in the fetch, and the earlier characterisation of this as a silent-green failure is
withdrawn for want of support. Two checks would settle it: whether the frontier advances over the
next runs as the July composites publish, and a direct query to the ORNL DAAC subset API for the
late-July window at that epicentre.

Both were run on 2026-08-08 and the item is closed as publication latency, not a defect. The
frontier did not move: the next successful run (31220084577) again reports
`modis_lst: 316 -> 316 (+0)`. The direct query settles it properly, because it asks the source
rather than inferring from a band -- the ORNL DAAC `dates` endpoint for the Kumamoto epicentre
(32.79N, 130.75E) returns 1,209 available dates for MOD11A2 ending at **2026-06-18**, with the
2026-05 onwards tail being 05-01, 05-09, 05-17, 05-25, 06-02, 06-10, 06-18 at the product's
eight-day spacing. The table's frontier and the source's last published date are the same day, so
the fetcher is fully caught up and there is nothing missing to recover. One incidental
observation, recorded without a conclusion attached: the `dates` endpoint for the daily product
`MOD11A1` returns 404 at this point, while `main()` decides daily-versus-8-day availability from
the global `/products` listing -- a listing entry and a per-point 404 can therefore disagree. That
is unrelated to the frontier question settled here.
