-- forecast_arima.sql — ARIMA_PLUS on weekly unconstrained sums.
-- The is_low_stock XREG regressor is dropped: at weekly grain all models train
-- on the same series (daily low-stock imputation aggregated upstream), so plain
-- ARIMA_PLUS suffices. holiday_region removed — holiday effects are daily-scale.
-- NOTE: the pre_hook retrains the BQML model on every run (main cost knob).

{{ config(
    materialized='incremental',
    partition_by={'field': 'forecast_run_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite',
    pre_hook="""
        create or replace model {{ target.schema }}.arima_weekly_sku
        options (
            model_type = 'ARIMA_PLUS',
            time_series_timestamp_col = 'week_start',
            time_series_data_col = 'net_qty_unconstrained',
            time_series_id_col = 'sku',
            horizon = {{ var('order_horizon_weeks', 10) }},
            data_frequency = 'WEEKLY',
            auto_arima_max_order = 3
        ) as
        select sku, week_start, net_qty_unconstrained
        from {{ ref('fct_weekly_sku_sales') }}
        where has_min_history and is_active
          and week_start <= (
                select date_sub(max(week_start), interval {{ var('forecast_holdout_weeks', 0) }} week)
                from {{ ref('fct_weekly_sku_sales') }}
          )
    """
) }}

-- Output capping is NOT done here — it happens centrally for ALL models in
-- forecast_results_unioned (output_cap_multiple × the SKU's p75 reference).
-- Keeping a second, ARIMA-only cap here would duplicate that logic and drift
-- out of sync with it. Negative values are floored at 0 below.
with raw_forecast as (

    select
        sku,
        date(forecast_timestamp) as forecast_week_start,
        forecast_value,
        prediction_interval_lower_bound as lower_bound,
        prediction_interval_upper_bound as upper_bound
    from ML.FORECAST(
        model {{ target.schema }}.arima_weekly_sku,
        struct({{ var('order_horizon_weeks', 10) }} as horizon, 0.9 as confidence_level)
    )
)

select
    'arima_plus' as model_name,
    r.sku,
    r.forecast_week_start,
    greatest(r.forecast_value, 0) as forecast_value,
    greatest(r.lower_bound,   0) as lower_bound,
    greatest(r.upper_bound,   0) as upper_bound,
    0.9 as confidence_level,
    current_date() as forecast_run_date,
    (select date_sub(max(week_start), interval {{ var('forecast_holdout_weeks', 0) }} week)
     from {{ ref('fct_weekly_sku_sales') }}) as train_end_week
from raw_forecast r
