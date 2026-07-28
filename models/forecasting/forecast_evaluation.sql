-- forecast_evaluation.sql — per model × sku aggregates over the scored holdout weeks.
-- "actual" = unconstrained weekly demand estimate (see forecast_evaluation_weeks).
-- PRIMARY metric: n_extreme_weeks — the count of sku-weeks with a big miss
-- (see forecast_evaluation_weeks). We care about which model avoids large
-- variances, not which nails the 56-day total.
-- SECONDARY: total_abs_pct_err (order-window base-demand total sanity check)
-- and weekly wape (shape diagnostic / tiebreaker).

{{ config(materialized='table') }}

select
    model_name,
    sku,

    count(*) as n_weeks_scored,

    -- PRIMARY: extreme-miss count over the scored holdout weeks
    -- (holdout = order window = 8, so no separate order-window count; the
    -- in_order_window flag only matters again if holdout is ever set > 8)
    countif(is_extreme_week) as n_extreme_weeks,
    safe_divide(countif(is_extreme_week), count(*)) as extreme_week_rate,

    -- order-window totals (window length = var order_horizon_weeks)
    sum(if(in_order_window, actual, 0)) as total_actual,
    sum(if(in_order_window, predicted, 0)) as total_forecast,
    safe_divide(
        abs(sum(if(in_order_window, predicted, 0)) - sum(if(in_order_window, actual, 0))),
        sum(if(in_order_window, actual, 0))
    ) as total_abs_pct_err,

    -- interval diagnostics (nominal 90%): 
    -- coverage_rate ≈ 0.9 + narrow width = genuinely good
    -- coverage_rate << 0.9 + narrow = overconfident;
    -- coverage_rate ≈ 0.9 + wide = honest but vague
    safe_divide(countif(in_interval), count(*)) as coverage_rate,
    -- median (not mean): robust to a single near-zero-forecast week spiking the ratio, check the median of each sku*model's band narrow-level
    approx_quantiles(rel_interval_width, 100)[offset(50)] as median_rel_interval_width,

    -- weekly-shape diagnostics (full holdout)
    safe_divide(sum(abs_err), sum(actual)) as wape,
    avg(abs_err) as mae,
    sqrt(avg(pow(err, 2))) as rmse,
    avg(if(actual > 0, abs_err / actual, null)) as mape  -- reported only

from {{ ref('forecast_evaluation_weeks') }}
group by 1, 2
