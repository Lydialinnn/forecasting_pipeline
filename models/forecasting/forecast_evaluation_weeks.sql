-- forecast_evaluation_weeks.sql — sku × week × model scoring detail (latest vintage).
-- One row per scored holdout week. Only meaningful when forecast_holdout_weeks > 0.
--
-- Low-stock handling (hybrid): forecasts are scored against the UNCONSTRAINED
-- weekly actual (raw + daily imputation for censored days), so lightly censored
-- weeks stay scoreable — important for OOS-prone SKUs that would otherwise fall
-- under min_scored_weeks. But weeks that are MOSTLY imputed
-- (low_stock_days > scoring_max_low_stock_days) are excluded: the imputation is
-- ~the same formula as naive_4wk_avg, so grading against a mostly-imputed
-- "actual" would hand naive an artificial win exactly where demand is unknown.
--
-- is_extreme_week (feeds winner selection):
--   actual > 0 : abs pct error > extreme_ape_threshold (default 50%)
--   actual = 0 : predicted >= extreme_zero_week_units (default 5) — a fixed-unit
--                floor, since any positive forecast has infinite pct error vs 0.

{{ config(materialized='table') }}

with latest_forecasts as (
    select *
    from {{ ref('forecast_results_unioned') }}
    qualify forecast_run_date = max(forecast_run_date) over (partition by model_name)
),

actuals as (
    select sku, week_start, net_qty_raw, net_qty_unconstrained, low_stock_days
    from {{ ref('fct_weekly_sku_sales') }}
),

joined as (
    select
        f.model_name,
        f.sku,
        f.forecast_week_start as week_start,
        a.net_qty_unconstrained as actual,   -- demand estimate: raw + imputed censored days
        a.net_qty_raw as actual_raw,         -- observed (censored) sales, for reference
        a.low_stock_days,
        f.forecast_value as predicted,
        f.lower_bound,
        f.upper_bound,
        f.train_end_week,
        date_diff(f.forecast_week_start, f.train_end_week, day)
            <= {{ var('order_horizon_weeks', 8) }} * 7 as in_order_window
    from latest_forecasts f
    join actuals a
      on a.sku = f.sku and a.week_start = f.forecast_week_start
    -- exclude majority-imputed weeks (see header)
    where a.low_stock_days <= {{ var('scoring_max_low_stock_days', 3) }}
)

select
    *,
    predicted - actual as err,
    abs(predicted - actual) as abs_err,
    safe_divide(abs(predicted - actual), actual) as ape,
    -- interval diagnostics: was the actual inside the model's stated 90% band? (coverage)
    actual between lower_bound and upper_bound as in_interval,
    -- is the band narrowed?
    safe_divide(upper_bound - lower_bound, nullif(predicted, 0)) as rel_interval_width,
    case
        when actual > 0
            then abs(predicted - actual) / actual > {{ var('extreme_ape_threshold', 0.5) }}
        else predicted >= {{ var('extreme_zero_week_units', 5) }}
    end as is_extreme_week
from joined
