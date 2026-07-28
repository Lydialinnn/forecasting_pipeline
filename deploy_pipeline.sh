#!/bin/bash
set -e

# Deploy the weekly forecasting pipeline as a Cloud Run JOB + Monday scheduler.
# Run from the project root: ./deploy_pipeline.sh
#
# The job chains: dbt (staging->fct_weekly) -> kalman job (waits) -> dbt (models->downstream).
# IAM the service account needs (one-time):
#   roles/run.invoker (to execute the kalman job)
#   roles/iam.serviceAccountUser on itself (actAs for the kalman execution)
#   BigQuery dataEditor + jobUser (dbt builds)
#
# Occasional EVALUATION run (refreshes the winner mapping, holdout 13):
#   gcloud run jobs execute valor-weekly-forecast-pipeline \
#     --region=northamerica-northeast2 --project=valor-sales \
#     --update-env-vars PIPELINE_MODE=evaluation --wait
#   ...then run once more without overrides to restore production vintages.

# ---- Edit these if needed ----
JOB_NAME="valor-weekly-forecast-pipeline"
PROJECT="valor-sales"
REGION="northamerica-northeast2"
SCHEDULER_LOCATION="northamerica-northeast1"   # Cloud Scheduler is not available in northeast2; Montreal scheduler triggers the Toronto job (URI is global)
SERVICE_ACCOUNT="valor-scheduler-invoker@valor-sales.iam.gserviceaccount.com"
SCHEDULE="0 8 * * 1"                    # Monday 8:00 AM Toronto
# ------------------------------

echo "--> Deploying Cloud Run job: $JOB_NAME"
gcloud run jobs deploy "$JOB_NAME" \
  --source . \
  --region="$REGION" \
  --project="$PROJECT" \
  --task-timeout=7200 \
  --memory=1Gi \
  --max-retries=0 \
  --service-account="$SERVICE_ACCOUNT" \
  --set-env-vars="PIPELINE_MODE=production,DBT_TARGET=prod"

echo "--> Creating/updating Cloud Scheduler trigger ($SCHEDULE America/Toronto)"
SCHEDULER_NAME="${JOB_NAME}-trigger"
RUN_URI="https://run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "$SCHEDULER_NAME" --location="$SCHEDULER_LOCATION" --project="$PROJECT" >/dev/null 2>&1; then
  ACTION="update"
else
  ACTION="create"
fi

gcloud scheduler jobs "$ACTION" http "$SCHEDULER_NAME" \
  --location="$SCHEDULER_LOCATION" \
  --project="$PROJECT" \
  --schedule="$SCHEDULE" \
  --time-zone="America/Toronto" \
  --uri="$RUN_URI" \
  --http-method=POST \
  --oauth-service-account-email="$SERVICE_ACCOUNT"

echo "--> Creating/updating DAILY sku-label refresh trigger (9:00 AM Toronto)"
SKU_SCHEDULER_NAME="${JOB_NAME}-sku-refresh-trigger"
SKU_BODY='{"overrides":{"containerOverrides":[{"env":[{"name":"PIPELINE_MODE","value":"sku_refresh"}]}]}}'

if gcloud scheduler jobs describe "$SKU_SCHEDULER_NAME" --location="$SCHEDULER_LOCATION" --project="$PROJECT" >/dev/null 2>&1; then
  SKU_ACTION="update"
else
  SKU_ACTION="create"
fi

gcloud scheduler jobs "$SKU_ACTION" http "$SKU_SCHEDULER_NAME" \
  --location="$SCHEDULER_LOCATION" \
  --project="$PROJECT" \
  --schedule="0 9 * * *" \
  --time-zone="America/Toronto" \
  --uri="$RUN_URI" \
  --http-method=POST \
  --message-body="$SKU_BODY" \
  --oauth-service-account-email="$SERVICE_ACCOUNT"

echo "--> Done. Manual run: gcloud run jobs execute $JOB_NAME --region=$REGION --project=$PROJECT --wait"
