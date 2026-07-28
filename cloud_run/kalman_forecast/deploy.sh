#!/bin/bash
set -e

# Deploy the Kalman weekly forecaster as a Cloud Run JOB (batch, no HTTP server).
# Run from this folder: ./deploy.sh
#
# ORDERING MATTERS on the weekly refresh: this job reads fct_weekly_sku_sales,
# so the sequence is:
#   1. dbt build --select fct_daily_sku_sales fct_weekly_sku_sales
#   2. this job (gcloud run jobs execute $JOB_NAME ...)
#   3. dbt build the forecast/evaluation models (with include_kalman: true)
# If you schedule this independently of the dbt run, it will train on whatever
# vintage of fct_weekly_sku_sales exists at trigger time.

# ---- Edit these if needed ----
JOB_NAME="valor-kalman-forecast"
PROJECT="valor-sales"
REGION="northamerica-northeast2"
SERVICE_ACCOUNT="valor-scheduler-invoker@valor-sales.iam.gserviceaccount.com"
BQ_DATASET="valor_weekly_forecasting"   # must match profiles.yml dataset + _sources.yml forecasting_external
HOLDOUT_WEEKS="10"                      # mirror dbt var forecast_holdout_weeks; set 0 for production (pipeline overrides per execution)
HORIZON_WEEKS="10"                      # mirror dbt var forecast_horizon_weeks
SCHEDULE="30 6 * * 1"                   # Monday 6:30 AM Toronto (after the 6:00 inventory job)
# ------------------------------

echo "--> Deploying Cloud Run job: $JOB_NAME"
gcloud run jobs deploy "$JOB_NAME" \
  --source . \
  --region="$REGION" \
  --project="$PROJECT" \
  --task-timeout=3600 \
  --memory=2Gi \
  --max-retries=1 \
  --service-account="$SERVICE_ACCOUNT" \
  --set-env-vars="GCP_PROJECT=${PROJECT},BQ_DATASET=${BQ_DATASET},FORECAST_HOLDOUT_WEEKS=${HOLDOUT_WEEKS},HORIZON_WEEKS=${HORIZON_WEEKS}"

echo "--> Done. Manual run: gcloud run jobs execute $JOB_NAME --region=$REGION --project=$PROJECT"

# Optional weekly Cloud Scheduler trigger — leave commented until the dbt weekly
# run is scheduled too, so the ordering above stays under your control.
#
# SCHEDULER_NAME="${JOB_NAME}-trigger"
# RUN_URI="https://run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${JOB_NAME}:run"
# if gcloud scheduler jobs describe "$SCHEDULER_NAME" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
#   ACTION="update"
# else
#   ACTION="create"
# fi
# gcloud scheduler jobs "$ACTION" http "$SCHEDULER_NAME" \
#   --location="$REGION" \
#   --project="$PROJECT" \
#   --schedule="$SCHEDULE" \
#   --time-zone="America/Toronto" \
#   --uri="$RUN_URI" \
#   --http-method=POST \
#   --oauth-service-account-email="$SERVICE_ACCOUNT"
