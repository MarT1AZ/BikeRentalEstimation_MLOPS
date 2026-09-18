#!/usr/bin/env bash

# Save the generated dataset locally so DVC can track it.
base_run_path_name="ingestion_run"
file_name=bike_rental_hourly_data_2022_2025

uv run python ingestion.py \
  --start-year 2022 \
  --end-year 2025 \
  --start-month 1 \
  --end-month 12 \
  --cache-zip \
  --cache-path $base_run_path_name/cache \
  --save-local $base_run_path_name/output \
  --output-filename $file_name.csv
