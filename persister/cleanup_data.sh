#!/bin/bash
set -euo pipefail

# --- Configuration ---
# IMPORTANT: Replace this with your actual GCS bucket path.
GCS_BUCKET="gs://kfac-quant-db/Binance-Option"

GCLOUD_BIN="/usr/bin/gcloud" 
# The directory inside the container where the .parquet files are stored.
DATA_DIR="/data"
# --- End of Configuration ---


# Get the current date in YYMMDD format, using UTC to match Binance's data.
CURRENT_DATE_YYMMDD=$(date -u +%y%m%d)

echo "--- Starting Daily Archive and Cleanup ---"
echo "Current UTC Date (YYMMDD): $CURRENT_DATE_YYMMDD"
echo "Archive Destination: $GCS_BUCKET"
echo "Searching for expired files in: $DATA_DIR"

# Check if the data directory exists
if [ ! -d "$DATA_DIR" ]; then
  echo "Data directory $DATA_DIR not found. Exiting."
  exit 1
fi

# Find all .parquet files and loop through them.
# The `find` command is safer than a glob if there are many files.
find "$DATA_DIR" -maxdepth 1 -type f -name "*.parquet" | while read -r FILE_PATH; do
  FILENAME=$(basename "$FILE_PATH")

  # Extract the date (e.g., "251107") from the filename "BTC-251107-..."
  FILE_DATE_YYMMDD=$(echo "$FILENAME" | cut -d'-' -f2)

  # Check if the extracted date is a valid 6-digit number
  if ! [[ "$FILE_DATE_YYMMDD" =~ ^[0-9]{6}$ ]]; then
    echo "Skipping file with invalid date format: $FILENAME"
    continue
  fi

  # Compare the file's date with the current date.
  # If the file's date is less than today, it's expired.
  if [ "$FILE_DATE_YYMMDD" -lt "$CURRENT_DATE_YYMMDD" ]; then
    echo "Found expired file: $FILENAME"

    # 1. Attempt to copy the file to GCS
    echo "  -> Archiving to $GCS_BUCKET ..."
    "$GCLOUD_BIN" storage cp "$FILE_PATH" "$GCS_BUCKET/"

    # 2. Check if the copy command was successful (exit code 0)
    if [ $? -eq 0 ]; then
      # 3. If successful, delete the local file
      echo "  -> Archive successful. Deleting local file."
      rm "$FILE_PATH"
    else
      # 4. If copy failed, print an error and DO NOT delete the local file
      echo "  -> ERROR: Failed to archive $FILENAME. The local file will be kept." >&2
    fi
  fi
done

echo "--- Archive and Cleanup Complete ---"
