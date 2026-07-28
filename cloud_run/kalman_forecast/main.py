"""Kalman filter (local linear trend) SKU forecaster, WEEKLY grain.

Cloud Run Job. Reads fct_weekly_sku_sales from BigQuery and forecasts
FORECAST_HORIZON_WEEKS weeks ahead, appending the vintage to the
forecast_kalman table (standardized schema).

Low-stock handling happens UPSTREAM at daily grain: fct_daily_sku_sales imputes
censored days (net_qty_unconstrained) before weekly aggregation, so this job
trains on the weekly unconstrained sums directly — no NaN masking. The daily
version's seasonal=7 component is dropped (no day-of-week effect in weekly
totals; 52-week annual seasonality needs 2+ years of history).

Env vars / args:
    GCP_PROJECT             default 'valor-sales'
    BQ_DATASET              default 'valor_forecasting_dbt'
    FORECAST_HOLDOUT_WEEKS  default '13' (mirror the dbt var; '0' in production)
    HORIZON_WEEKS           default '13'
    CONFIDENCE_LEVEL        default '0.9'
"""

import logging
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
from google.cloud import bigquery
from statsmodels.tsa.statespace.structural import UnobservedComponents

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kalman_forecast")

PROJECT = os.environ.get("GCP_PROJECT", "valor-sales")
DATASET = os.environ.get("BQ_DATASET", "valor_forecasting_dbt")
HOLDOUT_WEEKS = int(os.environ.get("FORECAST_HOLDOUT_WEEKS", "10"))
HORIZON_WEEKS = int(os.environ.get("HORIZON_WEEKS", "10"))
CONFIDENCE = float(os.environ.get("CONFIDENCE_LEVEL", "0.9"))
MIN_TRAIN_WEEKS = int(os.environ.get("MIN_TRAIN_WEEKS", "8"))  # min training weeks per SKU (skip below)
MODEL_NAME = "kalman_llt"
OUTPUT_TABLE = f"{PROJECT}.{DATASET}.forecast_kalman"

OUTPUT_SCHEMA = [
    bigquery.SchemaField("model_name", "STRING"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("forecast_week_start", "DATE"),
    bigquery.SchemaField("forecast_value", "FLOAT64"),
    bigquery.SchemaField("lower_bound", "FLOAT64"),
    bigquery.SchemaField("upper_bound", "FLOAT64"),
    bigquery.SchemaField("confidence_level", "FLOAT64"),
    bigquery.SchemaField("forecast_run_date", "DATE"),
    bigquery.SchemaField("train_end_week", "DATE"),
]


def read_training_data(client: bigquery.Client) -> pd.DataFrame:
    query = f"""
        select sku, week_start, net_qty_unconstrained
        from `{PROJECT}.{DATASET}.fct_weekly_sku_sales`
        where has_min_history and is_active
          and week_start <= (
                select date_sub(max(week_start), interval @holdout_weeks week)
                from `{PROJECT}.{DATASET}.fct_weekly_sku_sales`
          )
        order by sku, week_start
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("holdout_weeks", "INT64", HOLDOUT_WEEKS)
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def forecast_sku(series: pd.DataFrame, train_end: date) -> pd.DataFrame | None:
    """Fit local linear trend on weekly unconstrained sums."""
    y = series.set_index("week_start")["net_qty_unconstrained"].astype(float)

    if y.notna().sum() < MIN_TRAIN_WEEKS:
        return None

    try:
        model = UnobservedComponents(y.values, level="local linear trend")
        result = model.fit(disp=False)
        fc = result.get_forecast(steps=HORIZON_WEEKS)
        mean = fc.predicted_mean
        ci = fc.conf_int(alpha=1 - CONFIDENCE)
        lower = ci[:, 0] if isinstance(ci, np.ndarray) else ci.iloc[:, 0].values
        upper = ci[:, 1] if isinstance(ci, np.ndarray) else ci.iloc[:, 1].values
    except Exception as exc:  # per-series failure: log and skip
        log.warning("SKU %s failed: %s", series["sku"].iloc[0], exc)
        return None

    weeks = [train_end + timedelta(weeks=i + 1) for i in range(HORIZON_WEEKS)]
    return pd.DataFrame(
        {
            "model_name": MODEL_NAME,
            "sku": series["sku"].iloc[0],
            "forecast_week_start": weeks,
            "forecast_value": np.maximum(mean, 0.0),
            "lower_bound": np.maximum(lower, 0.0),
            "upper_bound": np.maximum(upper, 0.0),
            "confidence_level": CONFIDENCE,
            "forecast_run_date": date.today(),
            "train_end_week": train_end,
        }
    )


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    log.info(
        "Holdout weeks: %s | horizon weeks: %s | output: %s",
        HOLDOUT_WEEKS, HORIZON_WEEKS, OUTPUT_TABLE,
    )

    df = read_training_data(client)
    log.info("Loaded %s rows, %s SKUs", len(df), df["sku"].nunique())

    results = []
    for sku, grp in df.groupby("sku"):
        out = forecast_sku(grp, grp["week_start"].max())
        if out is not None:
            results.append(out)

    if not results:
        log.error("No forecasts produced — aborting without write.")
        raise SystemExit(1)

    output = pd.concat(results, ignore_index=True)
    log.info("Writing %s rows (%s SKUs) to %s", len(output), output["sku"].nunique(), OUTPUT_TABLE)

    # Replace today's vintage if re-run same day, then append.
    client.query(f"""
        create table if not exists `{OUTPUT_TABLE}` (
            model_name string,
            sku string,
            forecast_week_start date,
            forecast_value float64,
            lower_bound float64,
            upper_bound float64,
            confidence_level float64,
            forecast_run_date date,
            train_end_week date
        )
        partition by forecast_run_date
    """).result()
    client.query(
        f"delete from `{OUTPUT_TABLE}` where forecast_run_date = current_date()"
    ).result()
    job = client.load_table_from_dataframe(
        output,
        OUTPUT_TABLE,
        job_config=bigquery.LoadJobConfig(
            schema=OUTPUT_SCHEMA, write_disposition="WRITE_APPEND"
        ),
    )
    job.result()
    log.info("Done.")


if __name__ == "__main__":
    main()
