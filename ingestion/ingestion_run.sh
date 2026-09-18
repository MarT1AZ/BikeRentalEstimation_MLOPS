#!/usr/bin/env bash

file="ingestion.py"

# Get the latest commit of the ingestion script.
py_commit_id=$(git log -1 --format='%H' -- "$file")

file="ingestion_run.sh"

# Get the latest commit of this Bash script.
sh_commit_id=$(git log -1 --format='%H' -- "$file")

: "${py_commit_id:?please commit ingestion.py first}"
: "${sh_commit_id:?please commit ingestion_run.sh first}"
: "${S3_BUCKET:?S3_BUCKET must be set}"

echo "Running bike rental aggregration"
echo "S3 bucket name : $S3_BUCKET"

# Include source commit IDs as tags on the uploaded S3 version.

uv run python ingestion.py \
  --start-year 2022 \
  --end-year 2022 \
  --start-month 1 \
  --end-month 2 \
  --cache-zip \
  --cache-path ingestion_run/cache \
  --save-s3 "s3://$S3_BUCKET/extract_data" \
  --s3-tag "description=test_run" \
  --s3-tag "script_commit_id=$py_commit_id" \
  --s3-tag "bash_script_commit_id=$sh_commit_id"
