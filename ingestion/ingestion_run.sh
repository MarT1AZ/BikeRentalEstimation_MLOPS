#!/usr/bin/env bash

: "${S3_BUCKET:?S3_BUCKET must be set}"

echo "Running bike rental aggregration"
echo "S3 bucket name : $S3_BUCKET"

uv run python ingestion.py \
  --start-year 2022 \
  --end-year 2025 \
  --start-month 1 \
  --end-month 12 \
  --cache-zip \
  --cache-path ingestion/cache \
  --save-local ingestion/output \
  --save-s3 s3://$S3_BUCKET/extract_data
