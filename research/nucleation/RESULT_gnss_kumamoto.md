# Geodetic nucleation test - 2016 Kumamoto (GNSS 5-min kinematic)

**Verdict: NULL (pre-registered; raw and common-mode-filtered).** Eleventh test of the nucleation arc, and the first geodetic one.

The arc's ten waveform tests concluded that the reported precursors must live in observables the seismometer-waveform representation cannot access - geodesy / precise relative relocation. This test takes the geodetic channel directly: does the aseismic surface-displacement transient of the slow slip Kato et al. (2016) inferred drove the Kumamoto foreshocks become prospectively resolvable in high-rate GNSS during the 28 h between the Mj6.5 foreshock and the Mj7.3 mainshock?

## Data
NGL 5-minute kinematic (IGS20/kenv) E/N/U, openly downloadable, no credentials. 105 stations: 97 inside a 31.9-33.7N / 129.8-131.7E box around the epicentre plus 8 Kinki-Kanto far-field controls; DOY 090-108 (2016-03-30 to 04-17). Quiet-day horizontal precision ~5-7 mm per 5-min epoch; the two closest GEONET stations (G071 5.3 km, J465 9.7 km) sit essentially on the rupture.

## Method (pre-registered before the window residual was examined)
Per station: secular detrend on the pre-foreshock baseline (DOY 090 -> Mj6.5), Heaviside removal of the Mj6.5/Mj6.4 coseismic steps, 0.5 m cap on shaking outliers. Window net horizontal transient = (last-2h median) - (first-2h median) of the residual over [Mj6.5, Mj7.3]. Per-station threshold = 95th percentile of the same statistic over sliding 28 h baseline windows. Criteria: G1 >=50% of near-field (<=30 km) stations exceed their baseline p95; G2 amplitude decays with epicentral distance; G3 far-field controls null (<=10% exceed); G4 final-6h slip speed > earlier (accelerating slow slip vs decelerating afterslip). POSITIVE iff G1 and G3null and (G2 or G4). A second pre-registered variant applied common-mode filtering (subtract the per-epoch median residual over an 80-1000 km regional reference ring) before re-applying G1-G4.

## Result
- Raw: NULL. G1 FAIL (44% of near-field exceed), G3null FAIL (far-field controls exceed at 43%; far_Aw 13.2 mm ~ near_Aw 14.6 mm). The apparent near-field transient is a network common-mode floor - ~13 mm appears even 600-900 km away - not a localized signal. G2 PASS (weak decay), G4 PASS (moot once G1/G3 fail).
- Common-mode filtered: NULL, cleaner. G1 FAIL (33%), G3null FAIL (controls exceed at 71%).
- The two closest stations do show a marginal window transient above their own baseline (G071 5.3 km: 20 mm; J465 9.7 km: 15 mm), but it is not a spatially-coherent, control-surviving localized field: a majority of near-field stations do not exceed, and far-field controls exceed at an equal or higher rate.

## Forward slip-model inversion (best-chance, pre-registered)

The two model-free tests above use only per-station displacement magnitude. This third test gives the slow slip its best chance by fitting an elastic dislocation (validated Okada85 numpy port, reproducing Okada 1985 Table 2 Case 2 to <3e-6) to the full window net-displacement field, exploiting the spatial PATTERN (direction + distance decay). A grid of plausible nucleation-patch geometries (Hinagu/Futagawa strike 205/235 deg, dip 70/90, depth 6/10/14 km, two lon/lat centroids, L=14 km W=10 km) is searched and the best variance reduction (VR) kept; to control for the overfitting that a free grid search introduces, the SAME search is applied to 29 sliding baseline windows.

Result: the window best-fit VR is **0.117** - the optimally-placed slip model explains only 12% of the pre-mainshock net-displacement field - while the same grid search achieves a **median 0.173 and 95th-percentile 0.304 on quiet baseline windows**. The window field is therefore *less* slip-model-like than baseline noise (P1 FAIL), so the verdict is **NULL**. The best geometry was a Hinagu-fault patch at 14 km depth with 0.46 m best-fit slip - physically admissible but explaining nothing the grid search does not also find in quiet periods. Even a physical forward model with the spatial pattern extracts no coherent slow-slip signature.

## Reading
At GEONET 5-min kinematic precision the 2016 Kumamoto pre-mainshock aseismic transient is not prospectively resolvable as a coherent localized deformation field above the network common-mode/noise floor, with model-free (raw and common-mode-filtered) net-displacement statistics or a best-chance forward slip-model inversion. This is consistent with the slow slip Kato et al. (2016) inferred from foreshock migration being at or below geodetic resolution - it was inferred from seismicity, not measured as a clear geodetic transient. The geodetic channel - the observable the waveform arc had pointed to - therefore also does not prospectively mark the Kumamoto nucleation.

Caveats: a 28 h net-displacement statistic is a blunt detector; a forward slip-model inversion fitting the two closest stations, or a multi-case geodetic extension, are the natural next levers. Scripts: research/nucleation/gnss_kumamoto_judge.py (blob 888d2569), gnss_kumamoto_judge_cmc.py (blob 45bef8b2). Data: NGL IGS20/kenv, fetched via dl_kenv.py.
