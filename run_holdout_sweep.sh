#!/bin/bash
set -e

# run_holdout_sweep.sh — ONE-OFF sensitivity analysis, run LOCALLY.
# For each holdout length, re-runs the full evaluation (all 4 models incl.
# Kalman) and snapshots forecast_model_winner into winner_holdout_sweep with a
# holdout_weeks column, then prints a summary of winners per setting.
#
# CAVEAT for interpreting results: changing the holdout changes BOTH the
# scoring window AND the training cutoff (train ends max_week - holdout), so
# each setting trains on different data and scores different weeks. Read the
# output as a ROBUSTNESS check ("does the winner survive across evaluation
# designs?"), not as "which window length is correct."
#
# Runtime ~30-40 min (8 iterations x [kalman + dbt + ARIMA retrain]).
# Cost: 8 ARIMA CREATE MODELs (cents) + 8 kalman executions (cents).
# Leaves the LAST holdout's vintages as latest — run an evaluation (normal
# holdout) or production execution afterwards to restore the real state.

HOLDOUTS=(8 9 10 11 12 13 14 15)
PROJECT="valor-sales"
REGION="northamerica-northeast2"
DATASET="valor_weekly_forecasting"
SWEEP_TABLE="${DATASET}.winner_holdout_sweep"
EVAL_SWEEP_TABLE="${DATASET}.evaluation_holdout_sweep"   # full per-sku × per-model metric detail

echo "=== building fct layer once (holdout-independent) ==="
dbt build --profiles-dir . --select +fct_weekly_sku_sales

FIRST=1
for H in "${HOLDOUTS[@]}"; do
  echo ""
  echo "================ holdout = $H weeks ================"
  VARS="{forecast_holdout_weeks: $H}"

  gcloud run jobs execute valor-kalman-forecast \
    --region="$REGION" --project="$PROJECT" \
    --update-env-vars FORECAST_HOLDOUT_WEEKS=$H --wait

  dbt build --profiles-dir . \
    --select forecast_timesfm forecast_arima forecast_naive_baseline forecast_results_unioned+ \
    --vars "$VARS"

  if [ "$FIRST" = "1" ]; then
    bq query --use_legacy_sql=false --project_id="$PROJECT" \
      "create or replace table \`${SWEEP_TABLE}\` as
       select $H as holdout_weeks, current_date() as sweep_run_date, t.*
       from \`${DATASET}.forecast_model_winner\` t"
    bq query --use_legacy_sql=false --project_id="$PROJECT" \
      "create or replace table \`${EVAL_SWEEP_TABLE}\` as
       select $H as holdout_weeks, current_date() as sweep_run_date, t.*
       from \`${DATASET}.forecast_evaluation\` t"
    FIRST=0
  else
    bq query --use_legacy_sql=false --project_id="$PROJECT" \
      "insert into \`${SWEEP_TABLE}\`
       select $H, current_date(), t.* from \`${DATASET}.forecast_model_winner\` t"
    bq query --use_legacy_sql=false --project_id="$PROJECT" \
      "insert into \`${EVAL_SWEEP_TABLE}\`
       select $H, current_date(), t.* from \`${DATASET}.forecast_evaluation\` t"
  fi
done

echo ""
echo "=== SWEEP SUMMARY: winner counts (and volume share) per holdout ==="
bq query --use_legacy_sql=false --project_id="$PROJECT" "
select
  holdout_weeks,
  selected_model,
  count(*) as n_skus,
  round(100 * sum(total_actual) / sum(sum(total_actual)) over (partition by holdout_weeks), 1) as pct_volume
from \`${SWEEP_TABLE}\`
group by 1, 2
order by 1, n_skus desc"

echo ""
echo "NOTE: latest forecast vintages now reflect holdout=${HOLDOUTS[${#HOLDOUTS[@]}-1]}."
echo "Restore real state with an evaluation and/or production pipeline execution."
