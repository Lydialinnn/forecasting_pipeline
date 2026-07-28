-- forecast_naive_baseline.sql — mandatory benchmark floor, WEEKLY grain.
-- Forecast = avg of the last 4 complete training weeks (flat); ±1.645σ rough 90% band.
-- (The daily same-weekday logic collapses to a plain 4-week average at weekly grain.)

{{ config(
    materialized='incremental',
    partition_by={'field': 'forecast_run_date', 'data_type': 'date'},
    incremental_strategy='insert_overwrite'
) }}

with train as (
    select *
    from {{ ref('fct_weekly_sku_sales') }}
    where has_min_history and is_active
      and week_start <= (
            select date_sub(max(week_start), interval {{ var('forecast_holdout_weeks', 0) }} week)
            from {{ ref('fct_weekly_sku_sales') }}
      )
),

train_end as (
    select max(week_start) as train_end_week from train
),

last_4_weeks as (
    select
        t.sku,
        avg(t.net_qty_unconstrained) as avg_qty,
        stddev(t.net_qty_unconstrained) as sd_qty
    from train t
    cross join train_end e
    where t.week_start > date_sub(e.train_end_week, interval 4 week)
    group by 1
),

horizon_weeks as (
    select t.sku, w as forecast_week_start
    from (select distinct sku from train) t
    cross join train_end e
    cross join unnest(generate_date_array(
        date_add(e.train_end_week, interval 1 week),
        date_add(e.train_end_week, interval {{ var('order_horizon_weeks', 10) }} week),
        interval 7 day
    )) as w
)

select
    'naive_4wk_avg' as model_name,
    h.sku,
    h.forecast_week_start,
    coalesce(n.avg_qty, 0) as forecast_value,
    greatest(coalesce(n.avg_qty, 0) - 1.645 * coalesce(n.sd_qty, 0), 0) as lower_bound,
    coalesce(n.avg_qty, 0) + 1.645 * coalesce(n.sd_qty, 0) as upper_bound,
    0.9 as confidence_level,
    current_date() as forecast_run_date,
    (select train_end_week from train_end) as train_end_week
from horizon_weeks h
left join last_4_weeks n
    on n.sku = h.sku
