-- forecast_timesfm.sql — TimesFM 2.0 via BigQuery AI.FORECAST, WEEKLY grain.
-- No covariates; low-stock correction happens upstream at daily grain and is
-- carried in the weekly net_qty_unconstrained sums. AI.FORECAST infers the
-- 7-day frequency from the gap-free weekly timestamps.

{{ config(
    materialized='incremental',
    partition_by={'field': 'forecast_run_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite'
) }}

select
    'timesfm_2_0' as model_name,
    sku,
    date(forecast_timestamp) as forecast_week_start,
    forecast_value,
    prediction_interval_lower_bound as lower_bound,
    prediction_interval_upper_bound as upper_bound,
    confidence_level,
    current_date() as forecast_run_date,
    (select date_sub(max(week_start), interval {{ var('forecast_holdout_weeks', 0) }} week)
     from {{ ref('fct_weekly_sku_sales') }}) as train_end_week
from AI.FORECAST(
    (
        select sku, week_start, net_qty_unconstrained
        from {{ ref('fct_weekly_sku_sales') }}
        where has_min_history and is_active
          and week_start <= (
                select date_sub(max(week_start), interval {{ var('forecast_holdout_weeks', 0) }} week)
                from {{ ref('fct_weekly_sku_sales') }}
          )
    ),
    data_col => 'net_qty_unconstrained',
    timestamp_col => 'week_start',
    id_cols => ['sku'],
    model => 'TimesFM 2.0',
    horizon => {{ var('order_horizon_weeks', 10) }},
    confidence_level => 0.9
)
