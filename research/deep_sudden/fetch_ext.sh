#!/bin/bash
# Fetch the catalogue rows ext197.py splices onto the stored pre-2026 copy: 2025 (for gate E1) and 2026 up to now.
# Same query as ~/geo-ml/fetch221.sh (USGS FDSN, keyless, M>=4.5, orderby=time-asc).  Each year's row count is
# checked against the count endpoint at the same end time, and against the 20,000-row service limit.
set -eu
D=${CAT_EXT_DIR:-$HOME/claude-scratch/cat_ext}
mkdir -p "$D"
END=$(date -u +%Y-%m-%dT%H:%M:%S)
Q="https://earthquake.usgs.gov/fdsnws/event/1"
fetch() {  # $1 start  $2 end  $3 out
  curl -sf --max-time 300 "$Q/query?format=csv&starttime=$1&endtime=$2&minmagnitude=4.5&orderby=time-asc" > "$3.tmp"
  n=$(grep -c "^[0-9]" "$3.tmp")
  c=$(curl -sf --max-time 180 "$Q/count?format=text&starttime=$1&endtime=$2&minmagnitude=4.5")
  [ "$n" -eq "$c" ] || { echo "count mismatch $1..$2: rows $n, count endpoint $c"; exit 1; }
  [ "$n" -lt 19000 ] || { echo "near the 20000 limit $1..$2: $n"; exit 1; }
  mv "$3.tmp" "$3"; echo "$1..$2  $n rows -> $3"
}
fetch 2025-01-01 2026-01-01 "$D/y2025_now.csv"
out="$D/y2026_now.csv"; : > "$out.all"; first=1
y=2026; ynow=$(date -u +%Y)
while [ "$y" -le "$ynow" ]; do
  e=$((y+1))-01-01; [ "$y" -eq "$ynow" ] && e=$END
  fetch "$y-01-01" "$e" "$D/chunk_$y.csv"
  if [ "$first" = 1 ]; then head -1 "$D/chunk_$y.csv" >> "$out.all"; first=0; fi
  grep "^[0-9]" "$D/chunk_$y.csv" >> "$out.all"; rm "$D/chunk_$y.csv"; y=$((y+1))
done
mv "$out.all" "$out"; echo "fetched at $END" > "$D/FETCHED_AT"; cat "$D/FETCHED_AT"
