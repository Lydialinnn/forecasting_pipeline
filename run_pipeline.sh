#!/bin/bash
set -e

# Weekly forecasting pipeline entrypoint (Cloud Run job).
#
# PIPELINE_MODE=production (default):
#   holdout 0 — train through the latest complete week, forecast the FUTURE.
#   Skips evaluation/winner models: with no holdout there are no actuals to
#   score, and rebuilding them would WIPE the winner mapping. forecasting_total
#   reads the winner table from the last evaluation run.
#
# PIPELINE_MODE=evaluation (run occasionally, e.g. monthly):
#   holdout 8 — full rebuild including evaluation + winner refresh.
#   NOTE: leaves the latest forecast vintages trained 13 weeks back; run a
#   production execution right after to restore current forecasts.

MODE="${PIPELINE_MODE:-production}"
TARGET="${DBT_TARGET:-prod}"

# PIPELINE_MODE=sku_refresh: daily incremental upsert of the SKU label table only.
if [ "$MODE" = "sku_refresh" ]; then
  echo "=== SKU label refresh (int_sku_description incremental) ==="
  dbt build --profiles-dir . --target "$TARGET" --select int_sku_description
  exit 0
fi

if [ "$MODE" = "evaluation" ]; then
  HOLDOUT="${FORECAST_HOLDOUT_WEEKS:-10}"
else
  HOLDOUT=0
fi
VARS="{forecast_holdout_weeks: ${HOLDOUT}}"

echo "=== Pipeline mode: $MODE | holdout weeks: $HOLDOUT | dbt target: $TARGET ==="

echo "=== 1/3 dbt: staging -> weekly model input (+ tests) ==="
dbt build --profiles-dir . --target "$TARGET" --select +fct_weekly_sku_sales --vars "$VARS"

echo "=== 2/3 Kalman Cloud Run job (waits for completion) ==="
python trigger_kalman.py "$HOLDOUT"

echo "=== 3/3 dbt: forecast models -> downstream ==="
if [ "$MODE" = "evaluation" ]; then
  dbt build --profiles-dir . --target "$TARGET" \
    --select forecast_timesfm forecast_arima forecast_naive_baseline forecast_results_unioned+ \
    --vars "$VARS"
  # preserve this evaluation's winners for cross-cycle consistency analysis
  dbt run-operation snapshot_winner_history --args "{holdout_weeks: $HOLDOUT}" \
    --profiles-dir . --target "$TARGET"
else
  dbt build --profiles-dir . --target "$TARGET" \
    --select forecast_timesfm forecast_arima forecast_naive_baseline forecast_results_unioned forecasting_total forecast_display \
    --vars "$VARS"
fi

echo "=== Pipeline complete ($MODE) ==="
