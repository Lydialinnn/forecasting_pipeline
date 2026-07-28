"""Trigger the valor-kalman-forecast Cloud Run job and WAIT for completion.

Used by run_pipeline.sh between the two dbt stages. Passes the pipeline's
holdout setting to the job as an env override so dbt and Kalman always train
on the same cutoff.

Usage: python trigger_kalman.py <holdout_weeks>
"""

import sys

from google.cloud import run_v2

PROJECT = "valor-sales"
REGION = "northamerica-northeast2"
KALMAN_JOB = "valor-kalman-forecast"


def main() -> None:
    holdout_weeks = sys.argv[1] if len(sys.argv) > 1 else "0"
    name = f"projects/{PROJECT}/locations/{REGION}/jobs/{KALMAN_JOB}"
    print(f"Triggering {name} with FORECAST_HOLDOUT_WEEKS={holdout_weeks} ...")

    client = run_v2.JobsClient()
    request = run_v2.RunJobRequest(
        name=name,
        overrides=run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(
                    env=[run_v2.EnvVar(name="FORECAST_HOLDOUT_WEEKS", value=str(holdout_weeks))]
                )
            ]
        ),
    )
    operation = client.run_job(request=request)
    execution = operation.result(timeout=3600)  # blocks until the execution finishes

    if execution.failed_count:
        raise SystemExit(f"Kalman job FAILED ({execution.failed_count} failed tasks): {execution.name}")
    print(f"Kalman job succeeded: {execution.name}")


if __name__ == "__main__":
    main()
