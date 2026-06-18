# S-net offshore nucleation test - M7.4 Miyako-oki (2026-04-20)

## Motivation
The onshore nucleation arc returned 11 consecutive pre-registered NULLs. The arc's
synthetic-injection analysis showed the true cause is **near-field coverage**: offshore
megathrust nucleation is geometrically invisible to onshore arrays (detection floor far
above the precursor scale). This test changes the **observing system** to S-net (NIED's
150-node seafloor seismometer array directly above the Japan Trench) to ask whether
near-field offshore data can resolve nucleation that onshore could not.

## Target
M7.4, 2026-04-20 16:52:58 JST, 39.999N / 142.985E, depth 25 km (Miyako-oki). S-net-era,
shallow (interface-proximal), directly under the densest S-net coverage. The global
catalog shows the **epicentre region was quiescent (M3+) in the immediate run-up** (the
nearest large activity was a M6.5 swarm ~55-65 km SE on 2026-03-26), making this an ideal
case to search for below-catalog (Mc<3) nucleation with near-field data.

## Method (reproducible, open data)
- S-net continuous acceleration (code 0120A, A1X/A1Y/A1Z, 100 Hz) fetched via NIED Hi-net
  cont service (5-min request cap) for the final 24 h, in three 8-h GHA chunks.
- SeisBench PhaseNet (instance) picking on the 45 near-field stations (<=180 km of the
  hypocentre); accel components mapped X/Y/Z->N/E/Z; seafloor station depths from SAC stel.
- pyocto OctoAssociator (1D NE-Japan layered model, P-dominant association: S-picks on
  accelerometers are sparse, so n_s_picks=0); offshore box, station elevations = seafloor depth.
- **Pre-registered nucleation judge** (same protocol as the verified arc: time-localisation
  + spatial structure + permutation): of the pre-mainshock catalogue, (P1) Theil-Sen slope
  of distance-to-hypocentre vs time with a 200-shuffle permutation test, (P2) early/late
  event-rate halves, (P3) final-20%-time median distance vs overall. PASS requires a
  significant negative migration slope AND rate acceleration AND closing proximity.

## Results (consistent across windows)
| Window | events (pre-MS) | within 60 km | migration slope (perm p) | rate early/late | verdict |
|---|---|---|---|---|---|
| final 8 h  | 13 | 2 | +0.60 km/h (0.59) | 6/7   | NULL |
| final 16 h | 33 | 1 | -1.51 km/h (0.050) | 16/17 | NULL |
| final 24 h | 68 | 9 | -0.15 km/h (0.46) | 34/34 | NULL |

Final 24 h (2026-04-19 18:04 -> 2026-04-20 16:56): 2271 picks -> **70 located events**, 68
pre-mainshock. The catalogue is dominated by **distant regional activity (median ~92 km**;
the 2026-03-26 swarm region and trench-axis seismicity). Only **9/68 events fall within
60 km** of the hypocentre, and they are **scattered in time (22.6 -> 4.8 h before, ~one per
2-3 h) and distance (23-57 km, none <20 km), with no approach toward the hypocentre and no
clustering near the mainshock time** (closest approach 23 km occurs 16.6 h before; the last
near event, 4.8 h before, is 27 km away). Rate is flat (34/34), migration slope ~0
(p=0.46).

## Conclusion
**Constructive NULL - the first offshore near-field nucleation test.** Even with S-net's
near-field resolution, the M7.4 nucleated **without a detectable accelerating / migrating
foreshock cascade** at the hypocentre in the 24 h before. This extends the onshore 11-NULL
arc to the offshore near field: changing the observing system to seafloor coverage directly
above the source still does not reveal nucleation for this event.

## Caveats / robustness
- The pipeline demonstrably **locates near-hypocentre events when they exist** (de-risk on
  the mainshock hour recovered the M7.4 + immediate aftershocks ~24 km from USGS), so the
  near-absence of pre-mainshock near-hypocentre activity is a finding, not a blind spot.
- No empirical station corrections were applied (1D model; S-net sediment delays give
  ~2-5 s station terms -> ~tens-of-km absolute location uncertainty). Relative-trend tests
  (migration, rate) are robust to a roughly systematic bias, and the **absence of a
  near-hypocentre cluster is robust to this uncertainty**.
- Completeness is association-limited (n_picks>=6 on accel); a complementary near-source
  single-station pick-rate detector is a natural next robustness check.

## Assets
GHA `fetch-hinet-research.yml` (net=0120A, span_min=5); RPi5 `~/geo-ml/knuc18/`
(`nucleation_snet_analysis.py`); Kaggle datasets `snet-m74-c1/c2/c3`, kernel
`nucleation-snet-analysis`.


## Robustness: association-independent near-source pick-rate (closes the completeness caveat)
To rule out that association (n_picks>=6) hid a near-hypocentre cascade, the raw PhaseNet
P-pick rate was examined per station over the final 24 h (no association). The station
directly above the hypocentre, **N.S4N19 (10.2 km)**, shows **0-3 P-picks/hour, flat,
near-zero, with Theil-Sen slope 0.000** and no rise toward the mainshock. The near group
(<=40 km, 5 stations) is also flat (Theil-Sen 0.000; first-12 h 126 picks vs last-12 h 108,
i.e. slightly decreasing), while far stations (60-150 km, 18 stations) carry the decaying
regional swarm (slope -0.69/h). **The seafloor station 10 km above the source records no
accelerating micro-seismicity before the M7.4**, confirming the NULL is not an
association/completeness artefact: nucleation is absent (not merely unassociated) even at
near-field resolution.
