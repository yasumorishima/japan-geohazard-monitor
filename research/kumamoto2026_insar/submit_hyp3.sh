#!/bin/bash
# Submit the two HyP3 InSAR jobs for the 2026 Kumamoto earthquake.
#
# Needs an Earthdata Login bearer token in $EDL_TOKEN (or in the file named by
# $EDL_TOKEN_FILE). Get one at https://urs.earthdata.nasa.gov/ -> Generate Token.
# Never commit the token.
#
# Both pairs must use identical processing parameters: the damage proxy is a
# difference of their coherences, so any parameter change contaminates it.

set -euo pipefail

TOKEN="${EDL_TOKEN:-$(cat "${EDL_TOKEN_FILE:?set EDL_TOKEN or EDL_TOKEN_FILE}")}"

PRE=S1D_IW_SLC__1SDV_20260704T211627_20260704T211654_003529_00641D_023B
MID=S1D_IW_SLC__1SDV_20260716T211628_20260716T211655_003704_006A0F_6D30
POST=S1D_IW_SLC__1SDV_20260728T211629_20260728T211656_003879_00700D_0231

payload=$(mktemp)
cat > "$payload" <<JSON
{
  "jobs": [
    {"job_type": "INSAR_GAMMA", "name": "kumamoto2026-coseismic-p163",
     "job_parameters": {"granules": ["$MID", "$POST"],
       "looks": "10x2", "apply_water_mask": true,
       "include_wrapped_phase": true, "include_los_displacement": true,
       "include_inc_map": true, "include_dem": true}},
    {"job_type": "INSAR_GAMMA", "name": "kumamoto2026-preseismic-p163",
     "job_parameters": {"granules": ["$PRE", "$MID"],
       "looks": "10x2", "apply_water_mask": true}}
  ]
}
JSON

# Add "validate_only": true to the body to check acceptance without spending credits.
curl -sS --max-time 60 -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data @"$payload" \
  https://hyp3-api.asf.alaska.edu/jobs | jq -r '.jobs[]? | "\(.job_id) \(.name) \(.status_code) cost=\(.credit_cost)"'

rm -f "$payload"

# Poll until both are SUCCEEDED, then read .files[].url for the download links.
# Those URLs are unauthenticated but expire about two weeks after processing.
