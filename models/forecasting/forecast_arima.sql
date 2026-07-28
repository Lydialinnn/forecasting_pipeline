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

-- Per-SKU cap: ARIMA_PLUS's trend/drift component can linearly extrapolate a
-- short upswing into an absurd forecast (e.g. a SKU that peaked at 9/wk projected
-- to 57/wk). Clip each weekly value to arima_cap_multiple × the SKU's historical
-- peak week — generous enough to allow real growth, tight enough to kill runaway
-- drift. Floored at 0 (negative weekly demand is nonsensical).
with sku_weeks as (
    select
        sku,
        net_qty_unconstrained,
        low_stock_days,
        row_number() over (partition by sku order by week_start) as wk_rank
    from {{ ref('fct_weekly_sku_sales') }}
),

sku_cap as (
    -- cap basis = 75th PERCENTILE of the weeks after winsor_ref_exclude_weeks (2),
    -- matching the input winsorization in fct_weekly_sku_sales. Not max (3× max is
    -- effectively no cap) and not median (median proved size-biased: it removed
    -- ~19% of real units from the smallest SKUs vs ~2% from the largest). A weekly
    -- forecast above 3× p75 is implausible as *normal* demand.
    -- Heavily censored weeks (low_stock_days > scoring_max_low_stock_days) are
    -- excluded — they read near zero and would over-tighten the cap.
    -- Fallback to the all-weeks p75 for SKUs with no usable reference weeks.
    -- nullif(...,0) on BOTH: a very sparse SKU can have p75 = 0, and cap = 0 would
    -- clip the entire forecast to zero. Treat 0 as "no usable reference" ->
    -- cap_qty null -> no clipping (see the ifnull in the final select).
    select
        sku,
        coalesce(
            nullif(
                approx_quantiles(
                    if(wk_rank > {{ var('winsor_ref_exclude_weeks', 2) }}
                       and low_stock_days <= {{ var('scoring_max_low_stock_days', 3) }},
                       net_qty_unconstrained, null),
                    100
                )[safe_offset(75)], 0
            ),
            nullif(approx_quantiles(net_qty_unconstrained, 100)[safe_offset(75)], 0)
        ) * {{ var('arima_cap_multiple', 3) }} as cap_qty
    from sku_weeks
    group by 1
),

raw_forecast as (
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
    -- ifnull(cap, value): least(x, NULL) is NULL in BigQuery, so a null cap
    -- (no usable median reference) must mean "no clipping", not "null forecast".
    greatest(least(r.forecast_value, ifnull(c.cap_qty, r.forecast_value)), 0) as forecast_value,
    greatest(least(r.lower_bound,   ifnull(c.cap_qty, r.lower_bound)),   0) as lower_bound,
    greatest(least(r.upper_bound,   ifnull(c.cap_qty, r.upper_bound)),   0) as upper_bound,
    0.9 as confidence_level,
    current_date() as forecast_run_date,
    (select date_sub(max(week_start), interval {{ var('forecast_holdout_weeks', 0) }} week)
     from {{ ref('fct_weekly_sku_sales') }}) as train_end_week
from raw_forecast r
left join sku_cap c on c.sku = r.sku
