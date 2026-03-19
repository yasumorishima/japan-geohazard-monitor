# Data Source Licenses & Usage Requirements

This document tracks the usage policies for all data sources used in the Japan Geohazard Monitor project. **Check this before publishing any results.**

Last reviewed: 2026-03-19

## Quick Reference

| Severity | Sources |
|---|---|
| 🔴 **Strict (report required)** | NIED Hi-net/S-net |
| 🟡 **Non-commercial only** | INTERMAGNET, NMDB, IOC Sea Level |
| 🟢 **Open (citation required)** | GFZ Kp, Global CMT, COMET LiCSAR, JMA, GSI, P2P, Kakioka |
| ⚪ **Public domain** | NOAA (DART/SWPC/NDBC/ERDDAP), NASA Earthdata |
| ❓ **Unconfirmed** | Nagoya Univ. ISEE GNSS-TEC, CODE (Univ. Bern) TEC |

---

## 🔴 NIED Hi-net / S-net / DONET

- **License**: Custom (most restrictive)
- **DOI**: https://doi.org/10.17598/NIED.0003
- **Requirements**:
  1. Acknowledge NIED and ALL data-providing institutions in acknowledgments
  2. Send reprints/publications to NIED (address below)
  3. Cite DOI: 10.17598/NIED.0003
  4. Cite: Okada et al. (2004) Earth Planets Space 56:xv-xxviii, doi:10.1186/BF03353076
  5. Report all results (including non-published uses) to NIED
- **Prohibited**: Redistribution of raw waveform/source data
- **Warning**: Non-compliance may result in service termination
- **JMA via NIED**: Additionally acknowledge "気象庁・文部科学省が協力してデータを処理した結果を使用した" and list all waveform-providing institutions
- **Reprint address**:
  ```
  〒305-0006 茨城県つくば市天王台3-1
  防災科学技術研究所
  地震津波火山観測研究センター
  高感度地震観測管理室
  ```

### Citation template
```
National Research Institute for Earth Science and Disaster Resilience (2019),
NIED Hi-net, National Research Institute for Earth Science and Disaster Resilience,
doi:10.17598/NIED.0003.
```

---

## 🟡 INTERMAGNET (geomagnetic hourly data)

- **License**: CC BY-NC 4.0
- **Non-commercial only**: Commercial use requires written permission from operating institute
- **Requirements**:
  1. Use acknowledgment template (below)
  2. Cite DOIs for definitive data (2013+) when available
  3. Send citations to INTERMAGNET Secretary
- **Reference**: https://intermagnet.org/data_conditions.html

### Acknowledgment template (multi-observatory)
```
The results presented in this paper rely on data collected at magnetic
observatories. We thank the national institutes that support them and
INTERMAGNET for promoting high standards of magnetic observatory practice.
```

---

## 🟡 NMDB (cosmic ray neutron monitors)

- **License**: Non-commercial, per-station restrictions
- **Requirements**:
  1. Use NMDB acknowledgment (below)
  2. Add per-station acknowledgments (see www.nmdb.eu/station)
- **Reference**: https://www.nmdb.eu

### Acknowledgment template
```
We acknowledge the NMDB database (www.nmdb.eu), founded under the
European Union's FP7 programme (contract no. 213007) for providing data.
```

### Per-station (used in this project)
- **IRKT** (Irkutsk): Check www.nmdb.eu/station/IRKT
- **OULU** (Oulu): Check www.nmdb.eu/station/OULU
- **PSNM** (Doi Inthanon): Check www.nmdb.eu/station/PSNM

---

## 🟡 IOC Sea Level Station Monitoring Facility

- **License**: Non-commercial
- **DOI**: https://doi.org/10.14284/482
- **Requirements**: Cite VLIZ/IOC with DOI
- **Prohibited**: Commercial use — contact original data providers for commercial access
- **Note**: Data is raw (no QC), intended for station availability assessment

### Citation
```
Flanders Marine Institute (VLIZ); Intergovernmental Oceanographic Commission (IOC)
(2026): Sea level station monitoring facility.
Accessed at https://www.ioc-sealevelmonitoring.org on [date].
DOI: 10.14284/482
```

---

## 🟢 GFZ Kp Index

- **License**: CC BY 4.0
- **DOI**: https://doi.org/10.5880/Kp.0001
- **Requirements**: Cite GFZ as data source, cite DOI and reference paper
- **Reference**: Matzka et al. (2021) doi:10.1029/2020SW002641

---

## 🟢 Global CMT Project

- **License**: Citation required
- **Requirements**: Cite in published work

### Citation
```
Ekström, G., M. Nettles, and A.M. Dziewoński (2012),
The global CMT project 2004–2010: Centroid-moment tensors for 13,017 earthquakes,
Phys. Earth Planet. Inter. 200-201:1-9,
doi:10.1016/j.pepi.2012.04.002
```

---

## 🟢 COMET LiCSAR (InSAR)

- **License**: Copernicus Sentinel data terms
- **Requirements**:
  1. Use acknowledgment template
  2. Cite at least one reference paper

### Acknowledgment template
```
LiCSAR contains modified Copernicus Sentinel data [year] analysed by the
Centre for the Observation and Modelling of Earthquakes, Volcanoes and Tectonics (COMET).
LiCSAR uses JASMIN, the UK's collaborative data analysis environment (http://jasmin.ac.uk).
```

### References (cite at least one)
- Lazecký et al. (2020) Remote Sensing 12(15):2430
- Morishita et al. (2020) Remote Sensing 12(3):424

---

## 🟢 JMA (気象庁) — Earthquake, AMeDAS, Volcano

- **License**: PDL1.0 (≈ CC BY 4.0)
- **Requirements**: Source attribution: "Source: Japan Meteorological Agency website"
- **Restrictions**: Meteorological Service Act applies to forecast services (Article 17 licensing, Article 23 warning restrictions)
- **Reference**: https://www.jma.go.jp/jma/en/copyright.html

---

## 🟢 GSI GEONET (国土地理院)

- **License**: PDL1.0
- **Requirements**: "Source: GSI website (URL of relevant page)"
- **Note**: Some content (SAR, Active Fault Maps) has third-party rights
- **Reference**: https://www.gsi.go.jp/ENGLISH/page_e30286.html

---

## 🟢 P2P地震情報

- **License**: CC BY 4.0 (for JMA data pre-2021/4/4)
- **Requirements**: Attribute 気象庁 for earthquake data, 地理院タイル for maps
- **Commercial use**: OK

---

## 🟢 Kakioka Magnetic Observatory (WDC Kyoto)

- **License**: JMA terms
- **Requirements**: DOI assigned per dataset (see kakioka-jma.go.jp)
- **Contact**: kakioka@met.kishou.go.jp

---

## ⚪ NOAA (DART / SWPC / NDBC / ERDDAP)

- **License**: Public domain (US Government)
- **Requirements**: Do not claim ownership, do not imply NOAA endorsement
- **Includes**: DART bottom pressure, GOES X-ray/proton/electron flux, Kp (SWPC), MUR SST
- **Reference**: https://www.weather.gov/disclaimer

---

## ⚪ USGS Earthquake Data

- **License**: Public domain (US Government)
- **Requirements**: Standard academic citation

---

## ⚪ NASA Earthdata Sources

- **License**: Open data policy
- **Applies to**: MODIS LST, GRACE/GRACE-FO, OMI SO2, SMAP, Ocean color, Cloud fraction, VIIRS, OMNIWeb solar wind
- **Requirements**: Cite specific datasets per NASA data citation policy
- **Reference**: https://www.earthdata.nasa.gov/learn/use-data/data-citation

---

## ⚪ UHSLC (Univ. Hawaii Sea Level Center)

- **License**: © UHSLC
- **Requirements**: Standard academic citation
- **Note**: South African stations require SANHO permission (hydrosan@iafrica.com)

---

## 🟢 Nagoya University ISEE GNSS-TEC

- **License**: © Nagoya University. All rights reserved.
- **Requirements**:
  1. Cite the database description paper (Shinbori et al., see below)
  2. Acknowledge IUGONET database and NICT Science Cloud
  3. List GNSS data providers used (see gnss_provider_list.html) — notably GEONET (GSI) data is included
- **Data URL**: https://stdb2.isee.nagoya-u.ac.jp/GPS/GPS-TEC/
- **Provider list**: https://stdb2.isee.nagoya-u.ac.jp/GPS/GPS-TEC/gnss_provider_list.html

### Acknowledgment template (from paper)
```
We used the Inter-university Upper atmosphere Global Observation NETwork (IUGONET)
database (IUGONET Type-A) and data analysis software (UDAS).
The GNSS data collection and processing were performed using the NICT Science Cloud.
```

### Citation
```
Shinbori, A., Otsuka, Y., Sori, T., Tsugawa, T., & Nishioka, M. (2022).
Statistical behavior of large-scale ionospheric disturbances from high latitudes
to mid-latitudes during geomagnetic storms using 20-yr GNSS-TEC data.
Journal of Geophysical Research: Space Physics, 127.
DOI: 10.1029/2021JA029687
```

### GNSS data providers to acknowledge
RINEX data from 50+ providers including: UNAVCO, CDDIS, GEONET (GSI), EUREF, and others listed at gnss_provider_list.html.

---

## 🟢 CODE (University of Bern) Ionosphere TEC / GIM

- **License**: Academic use, citation required
- **Requirements**: "When using these products to generate your own results please use the related references"
- **DOI**: https://doi.org/10.48350/197025 (final products)
- **Download**: http://www.aiub.unibe.ch/download/CODE

### Citation (for final product series — ionosphere GIM included)
```
Dach, Rolf; Schaer, Stefan; Arnold, Daniel; Brockmann, Elmar;
Kalarus, Maciej Sebastian; Lasser, Martin; Stebler, Pascal; Jäggi, Adrian (2024).
CODE final product series for the IGS.
Published by Astronomical Institute, University of Bern.
URL: https://www.aiub.unibe.ch/download/CODE
DOI: 10.48350/197025
```

---

## Pre-Publication Checklist

Before publishing any results using this project's data:

- [ ] NIED: DOI cited, acknowledgment included, reprint prepared for mailing
- [ ] INTERMAGNET: Acknowledgment template included, non-commercial confirmed
- [ ] NMDB: Acknowledgment template included, per-station credits checked
- [ ] IOC: DOI cited, non-commercial confirmed
- [ ] GFZ Kp: DOI cited
- [ ] Global CMT: Citation included
- [ ] COMET LiCSAR: Acknowledgment + paper citation included
- [ ] JMA: Source attribution included
- [ ] GSI: Source attribution included
- [ ] Nagoya Univ. GNSS-TEC: Shinbori et al. (2022) cited, IUGONET + NICT acknowledged
- [ ] CODE Bern: Dach et al. (2024) DOI: 10.48350/197025 cited
- [ ] NASA: Dataset-specific citations included
