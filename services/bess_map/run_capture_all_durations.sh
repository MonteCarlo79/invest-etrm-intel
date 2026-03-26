#!/bin/bash
set -e

for D in 4 2; do
  echo "▶ Running capture pipeline for duration_h=$D"
  python run_capture_pipeline_patched_v9_fixed_v3.py \
    --schema marketdata \
    --duration-h $D \
    --power-mw 1 \
    --roundtrip-eff 0.85 \
    --model ols_da_time_v1 \
    --env /app/.env
done

echo "✅ All durations finished"
