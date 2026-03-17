#!/bin/bash
# Download feature matrix from GitHub Actions artifact to Google Drive.
# Run on RPi5 via cron after analysis.yml completes.
#
# Cron example (check every Monday 8:00 JST = 23:00 UTC Sunday):
#   0 23 * * 0 /home/yasu/download_matrix_to_drive.sh
#
# Prerequisites:
#   - gh CLI authenticated
#   - Google Drive for Desktop syncing G:/マイドライブ/kaggle/geohazard/

set -euo pipefail

REPO="yasumorishima/japan-geohazard-monitor"
DRIVE_DIR="/home/yasu/gdrive/kaggle/geohazard"
TEMP_DIR="/tmp/geohazard_matrix"

echo "$(date): Checking latest analysis run..."

# Get latest successful run
RUN_ID=$(gh run list --repo "$REPO" --workflow "analysis.yml" --status success --limit 1 --json databaseId --jq '.[0].databaseId')

if [ -z "$RUN_ID" ]; then
    echo "No successful runs found"
    exit 0
fi

echo "Latest run: $RUN_ID"

# Check if already downloaded
MARKER="$DRIVE_DIR/.last_run_id"
if [ -f "$MARKER" ] && [ "$(cat "$MARKER")" = "$RUN_ID" ]; then
    echo "Already downloaded run $RUN_ID, skipping"
    exit 0
fi

# Download artifact
mkdir -p "$TEMP_DIR"
echo "Downloading artifact from run $RUN_ID..."
gh run download "$RUN_ID" --repo "$REPO" --name "analysis-results-$RUN_ID" --dir "$TEMP_DIR" 2>/dev/null || {
    echo "Artifact download failed (may have expired)"
    exit 0
}

# Copy feature matrix to Drive
mkdir -p "$DRIVE_DIR"
if [ -f "$TEMP_DIR/feature_matrix.json" ]; then
    cp "$TEMP_DIR/feature_matrix.json" "$DRIVE_DIR/feature_matrix.json"
    echo "Feature matrix copied to $DRIVE_DIR"
fi

# Copy level-0 predictions for stacking reference
for f in "$TEMP_DIR"/level0_predictions_*.json; do
    [ -f "$f" ] && cp "$f" "$DRIVE_DIR/"
done

# Copy CSEP benchmark if available
for f in "$TEMP_DIR"/csep_benchmark_*.json; do
    [ -f "$f" ] && cp "$f" "$DRIVE_DIR/"
done

# Mark as downloaded
echo "$RUN_ID" > "$MARKER"

# Cleanup
rm -rf "$TEMP_DIR"

echo "$(date): Done. Files synced to $DRIVE_DIR"
