#!/bin/bash
set -e

# Deploy the daily stock log as a Cloud Run JOB (batch, no HTTP server),
# then create/update the Cloud Scheduler trigger.
# Run from this folder: ./deploy.sh

# ---- Edit these if needed ----
JOB_NAME="valor-daily-inventory-log"
PROJECT="valor-sales"
REGION="northamerica-northeast2"
SERVICE_ACCOUNT="valor-scheduler-invoker@valor-sales.iam.gserviceaccount.com"
SHOPIFY_SECRET_ID="valor_shopify_cred"   
SCHEDULE="0 6 * * *"                       # daily 6:00 AM Toronto
# ------------------------------

echo "--> Deploying Cloud Run job: $JOB_NAME"
gcloud run jobs deploy "$JOB_NAME" \
  --source . \
  --region="$REGION" \
  --project="$PROJECT" \
  --task-timeout=3600 \
  --memory=1Gi \
  --max-retries=1 \
  --service-account="$SERVICE_ACCOUNT" \
  --set-env-vars="GCP_PROJECT=${PROJECT},BQ_DATASET=valor_inventory_logs,SHOPIFY_SECRET_ID=${SHOPIFY_SECRET_ID}"

echo "--> Creating/updating Cloud Scheduler trigger ($SCHEDULE America/Toronto)"
SCHEDULER_NAME="${JOB_NAME}-trigger"
RUN_URI="https://run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "$SCHEDULER_NAME" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
  ACTION="update"
else
  ACTION="create"
fi

# gcloud scheduler jobs "$ACTION" http "$SCHEDULER_NAME" \
#   --location="$REGION" \
#   --project="$PROJECT" \
#   --schedule="$SCHEDULE" \
#   --time-zone="America/Toronto" \
#   --uri="$RUN_URI" \
#   --http-method=POST \
#   --oauth-service-account-email="$SERVICE_ACCOUNT"

# echo "--> Done. Manual run: gcloud run jobs execute $JOB_NAME --region=$REGION --project=$PROJECT"
