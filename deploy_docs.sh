#!/bin/bash
set -e

# Deploy the dbt docs (lineage + model documentation) as a Cloud Run SERVICE.
# Run from the project root: ./deploy_docs.sh
# Re-run after model changes to refresh the published docs.

SERVICE_NAME="valor-forecasting-docs"
IMAGE="gcr.io/valor-sales/valor-forecasting-docs"
PROJECT="valor-sales"
REGION="northamerica-northeast2"
SERVICE_ACCOUNT="valor-scheduler-invoker@valor-sales.iam.gserviceaccount.com"

echo "=== 1. Generating fresh dbt docs locally ==="
dbt docs generate --profiles-dir .

echo "=== 2. Preparing files for Google Cloud Build ==="
# Temporarily swap the pipeline Dockerfile for the docs one
mv Dockerfile Dockerfile.pipeline_backup
mv Dockerfile.docs Dockerfile

# Copy target/ to a new name to bypass .gitignore / .gcloudignore
cp -r target public_docs

# Guarantee cleanup on exit (success or failure)
trap "mv Dockerfile Dockerfile.docs; mv Dockerfile.pipeline_backup Dockerfile; rm -rf public_docs; echo 'Files safely restored and cleaned up.'" EXIT

echo "=== 3. Building the nginx docs image ==="
gcloud builds submit --tag "$IMAGE" --project="$PROJECT"

echo "=== 4. Deploying docs to Cloud Run service ==="
gcloud run deploy "$SERVICE_NAME" \
  --image "$IMAGE" \
  --project="$PROJECT" \
  --region="$REGION" \
  --service-account="$SERVICE_ACCOUNT" \
  --port 8080 \
  --allow-unauthenticated

echo "=== Deployment complete! dbt docs are live. ==="
