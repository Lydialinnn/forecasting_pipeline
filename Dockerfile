# Weekly forecasting pipeline job: dbt + Kalman trigger.
# (deploy_docs.sh temporarily swaps this with Dockerfile.docs — see that script.)

FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-bigquery google-cloud-run

WORKDIR /app
COPY dbt_project.yml profiles.yml run_pipeline.sh trigger_kalman.py ./
COPY models models
COPY macros macros
COPY tests tests
RUN chmod +x run_pipeline.sh

ENTRYPOINT ["./run_pipeline.sh"]
