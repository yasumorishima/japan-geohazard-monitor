# Thirteenth test — Multi-case SSE detection-floor vs network curve (2026-06-15)

**Question.** Generalize the single-case detection-floor results (Kumamoto/Iquique GNSS; 2018 Boso OBP) into a curve across tectonic settings and observing networks: what sets whether an open geodetic pipeline detects a documented slow-slip event — noise, network geometry, or source location?

**Method (reproducible, adversarially hardened).** One matched-filter harness applied to documented SSEs, all using NGL 5-minute kinematic GNSS (`IGS20/kenv`, open) daily-medianed, plus the 2018 Boso seafloor OBP from the twelfth test. Per case: stations auto-selected from the global NGL holdings within a region box and spanning the analysis years; linear detrend with the SSE window masked from the trend fit; per-epoch network common-mode (regional mean) removed; matched filter = projection of the horizontal field onto the unit-slip Okada(1985) surface pattern of the documented fault geometry (output in slip-equivalent metres); step statistic with a window matched to each event's duration (Cascadia/Hikurangi 14 d, Boso 45 d, Guerrero 150 d). Detection = two-sided 95th-percentile of |step| against baseline windows (SSE/control excluded), cross-checked with a 1.96-sigma analytic floor (matched-filter output is slip-equivalent, so its baseline std is the noise floor) and a clean synthetic-injection ROC at a quiet control epoch. Mw via M0 = mu*A*slip (mu=3e10, A = fault L*W).

**Cases & results.**

| Network (region, event) | N sta | area km² | noise std (m, slip-eq) | analytic floor Mw (1.96σ) | injection floor Mw (≥95%) | documented Mw | realized detection | verdict |
|---|---|---|---|---|---|---|---|---|
| Boso onshore GEONET (2018 SSE) | 19 | 1600 | 0.0011 | 5.31 | 6.07 | 6.5 | pct 100 / SNR 57.8 | **detected** |
| Cascadia PBO (2012 ETS) | 34 | 3200 | 0.0077 | 6.07 | 6.42 | 6.7 | pct 96.9 / SNR 2.5 | **detected** |
| Guerrero sparse (2009–10 SSE) | 6 | 12000 | 0.0070 | 6.43 | unstable* | 7.5 | pct 88.1 / SNR 3.5 | marginal |
| Hikurangi GeoNet (2014 Gisborne) | 28 | 2400 | 0.0083 | 6.01 | 6.07 | 7.0 | pct 10.4 / SNR 0.2 | **not detected** |
| Boso seafloor OBP (2018 SSE) | 4 | 1600 | 0.0361 (vertical) | 6.32 | — | 6.5 | pct 89 | marginal |

\*Guerrero injection ROC is unstable: a 150-day window over a 4-year record with recurring SSEs leaves too few clean baseline windows; the analytic floor and observed SNR are the meaningful statistics there. Only ACYA (Acapulco) of the 6 NGL-holding stations spanning 2008–2011 is near-field; the dense UNAM/TLALOCNet near-field is not in the open NGL holdings.

**Findings.**
1. **The noise-limited analytic floor is Mw ~5.3–6.4** after common-mode removal, lowest for dense low-noise onshore networks (Boso onshore 5.3) and highest for the sparse Guerrero geometry (6.4).
2. **Realized detection decouples from the noise floor and is governed by near-field coverage of the slip patch.** Boso onshore (dense network directly flanking the slip) detects its Mw6.5 SSE at SNR 58; Cascadia (dense, deep slip under the network) at SNR 2.5; Guerrero (one near-field station) is marginal at SNR 3.5.
3. **Hikurangi is the decisive case:** the 2014 Gisborne Mw7.0 SSE is *not* detected (pct 10 / SNR 0.2) even though this network has the second-lowest noise floor (Mw6.0). The slip is shallow and offshore, so no onshore station overlies it — a low noise floor is necessary but not sufficient.

**Conclusion.** SSE detection requires BOTH a low noise floor AND near-field coverage of the slip. Offshore shallow sources defeat onshore GNSS regardless of its noise floor (Hikurangi), which is the quantitative case for seafloor geodesy — but seafloor OBP carries its own ocean-noise floor (~Mw6.3, twelfth test). This unifies the onshore-GNSS nucleation arc and the seafloor-OBP arc into one network-design curve, and corrects the earlier "noise environment, not offshore-ness" framing: offshore-ness matters precisely *through* near-field coverage. Reaching the ~Mw6 inferred-precursor scale for an offshore source needs dense seafloor instrumentation AND ocean-noise reduction (regional assimilative model), neither available in open form.

Harness: RPi5 `~/geo-ml/floor_curve.py` (+ `floor_plot.py`); data `~/geo-ml/fc_{cascadia,guerrero,hikurangi,boso}/`. Research only; not productized.

## Addendum — detection floor vs network density (station-subsampling curves)

Turning the 5-point scatter into actual curves: for the two detected cases, random station subsets (N = 4…all, 25 draws each) are run through the identical harness and the floor / realized detection is tabulated vs N (`floor_density.py`, fixed seed).

- **Boso onshore (strong near-field signal):** the documented SSE is detected at **pct 100 for every N down to 4 stations** — the slip is directly under the network, so even a handful of gauges suffice; added stations only refine the analytic noise floor (Mw 5.50 at N=4 → 5.31 at N=19, monotone).
- **Cascadia ETS (weak, distributed ~few-mm signal):** realized detection **climbs monotonically with density — pct 32 (N=4) → 97 (N=34), crossing 95% only near N≈28.** A weak signal needs many stations to beat the noise; near-field presence alone is not enough when the per-station displacement is millimetric.

**Caveat (honest):** the analytic floor computed at very low N is biased optimistically because per-epoch common-mode removal estimated from few stations overfits and erases real correlated variance (visible as Cascadia's low-N floor sitting *below* its high-N floor). The realized-detection-vs-N curve (which uses the real event) is the robust deliverable; the analytic-floor-vs-N is clean only where N is not tiny (Boso).

**Takeaway:** "detection floor vs network" is two regimes — for a strong near-field source a few stations suffice and density buys a lower floor; for a weak/distributed source density is the binding requirement to reach detection at all. (`research/nucleation/floor_density.png`)

## Controlled test — same margin, depth as the only major variable (Hikurangi deep vs shallow)

To de-confound "network quality vs source location" directly, two SSEs on the **same Hikurangi subduction margin**, recorded by the **same onshore NZ GeoNet network type** (NGL kenv), differing mainly in source depth/location:

- **Gisborne 2014 — shallow (~12 km), offshore** (short ~2-week event, centered-step detector): **NOT detected, pct 10 / SNR 0.2** (28 stations).
- **Manawatu 2010–11 — deep (30–40 km), inland-projecting** (long ~1.5-yr event, post-minus-pre **offset detector** — the centered-step detector is blind to a multi-month ramp and must be replaced for long SSEs): **DETECTED, pct 100** (offset −24 mm, the largest among 362 baseline windows; 35 stations).

The deep, inland-projecting SSE produces a clear cumulative onshore offset that no baseline window matches; the shallow offshore SSE of *higher* published magnitude (Mw7.0 vs ~7.0) is invisible to the same onshore network because the slip sits offshore beyond the near field. **This confirms, with depth as the dominant variable, that detectability is governed by the source's position relative to the network (near-field coverage), not by network quality.** (`research/nucleation/hik_compare.png`)

*Honest caveats:* the two events differ in duration (hence different, duration-appropriate detectors) and use different station subsets (East Cape vs lower North Island), so depth/offshore-ness is the dominant but not the sole difference. The long-event offset detector was added so that slow ramps (Manawatu, Guerrero) are not spuriously nulled by the centered-step statistic; under it Guerrero improves to pct 78 (still marginal — only one near-field open-holding station).

## Robustness — colored-noise / heavy-tail floor and area sensitivity (reviewer hardening)

The headline floors above use a Gaussian 1.96σ analytic floor, which assumes the matched-filter baseline-step distribution is normal. To address that (and to ground the floor in an empirical, threshold-consistent statistic rather than a parametric one), the floor is recomputed as the **95th percentile of the empirical |baseline step| distribution** — which automatically absorbs temporal autocorrelation (colored noise) and heavy tails, and is exactly the threshold the detection percentile uses.

| network | Gaussian 1.96σ floor Mw | empirical 95th-pct floor Mw | shift |
|---|---|---|---|
| Boso onshore | 5.31 | 5.56 | +0.25 |
| Cascadia | 6.07 | 6.11 | +0.03 |
| Hikurangi | 6.01 | 5.99 | −0.02 |
| Manawatu (offset) | 6.50 | 6.39 | −0.11 |
| Guerrero (offset) | 6.77 | 6.83 | +0.06 |

**The floors are robust to the noise-distribution assumption: the colored-noise/heavy-tail correction moves them by ≤0.25 Mw (largest where the std is smallest, Boso), and is negligible elsewhere.** The earlier Gaussian floors were at most marginally optimistic. **Area sensitivity:** since M0 = μ·A·slip, the floor Mw carries a systematic of ±0.40 Mw per factor-of-4 change in the assumed fault area A (a concentrated slip patch of A/4 lowers the floor by 0.40 Mw; a 4× larger patch raises it by 0.40). The honest headline is therefore: **empirical detection floor Mw ~5.6–6.8 across the five open networks, with a ±0.4 Mw area systematic** — and all qualitative conclusions (noise sets the floor; near-field coverage decides realization; Hikurangi's offshore Mw7.0 invisible onshore) are unchanged. Remaining論文-grade item: a full cross-station correlated surrogate (block bootstrap of the multi-station residuals) would tighten the colored-noise treatment beyond the single-trace empirical percentile used here.
