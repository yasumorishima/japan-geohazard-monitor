# Observing-system design from the measured detection floors

The geodetic nucleation tests returned null with a measured detection floor of Mw ~5.6 (Kumamoto, onshore) to ~6.9 (Iquique, offshore). This study turns that null into a constructive question: what observing system would push the floor below the ~Mw 6 inferred-precursor scale? The detectable slip scales as S* ~ C*sigma / |GF|_net, where |GF|_net is the RMS surface displacement the network sees per unit slip (computed exactly with the validated Okada85) and C*sigma is calibrated from the two measured injection floors. The two calibration constants agree to 14% (0.0079 vs 0.0092), confirming the floor difference between the cases is geometry, not data quality. Holding the calibration fixed, the Mw floor is forward-computed for hypothetical networks (geometry exact via Okada85; alternative-data-product noise as a labeled scaling).

## Result (Mw detection floor)

| Configuration | Kumamoto (onshore strike-slip) | Iquique (offshore thrust) |
|---|---|---|
| real onshore, 5-min kinematic | 5.6 | 6.5 |
| + 2x onshore density | 5.5 | - |
| + low-noise data (daily cGPS, noise/3) | 5.3 | 6.2 |
| + seafloor GNSS-A on the rupture | - | 6.1 |
| seafloor + low-noise | - | 5.8 |
| 2x density + low-noise | 5.2 | - |

## Reading

The lever depends on tectonic setting:
- **Offshore megathrust (Iquique)**: seafloor GNSS-A above the rupture is the dominant lever - placing stations at ~10-40 km on the source (versus 75-250 km onshore) raises network sensitivity 3.6x and drops the floor from Mw 6.5 to 6.1; combined with a daily-cGPS-level noise reduction it reaches Mw 5.8, below the ~Mw 6 inferred precursor. In other words the documented Iquique slow slip WOULD be prospectively detectable with seafloor geodesy plus low-noise processing; onshore networks alone cannot reach it.
- **Onshore crustal (Kumamoto)**: the dense near-fault network is already good (Mw 5.6); the main lever is a lower-noise data product (daily cGPS -> Mw 5.3) and density (-> Mw 5.2), already at or below the inferred precursor scale.

So the negative result is constructive: prospective geodetic nucleation detection at the ~Mw 6 scale is reachable, but it requires seafloor instrumentation for offshore sources and lower-noise / denser processing onshore - not the open 5-min kinematic onshore pipeline tested here. Caveats: the noise scaling for alternative data products is an assumed factor (the geometry term is exact via Okada85), and GNSS-A per-epoch noise is in reality higher than onshore, though the geometry gain dominates.

Script: research/nucleation/gnss_obsdesign.py.
