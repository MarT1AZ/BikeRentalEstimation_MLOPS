#!/usr/bin/env bash


export MLFLOW_TRACKING_URL="http://172.31.47.128:5000"

uv run python knn_regressor_hyperparams_search.py \
  --random-state 42 \
  --max-trials 15 \
  --experiment-name bikerental-knn-regression-gridsearch
