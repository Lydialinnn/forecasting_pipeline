-- forecast_model_winner.sql — per-SKU winner by FEWEST EXTREME WEEKS
-- (weekly misses > extreme_ape_threshold; see forecast_evaluation_weeks),
-- naive_4wk_avg as the floor: a candidate must beat naive on
-- (n_extreme_weeks, then total_abs_pct_err) for that SKU, else keeps naive.
-- Tiebreakers: order-window total error, then weekly wape.
-- sales_category: volume quartile of total_actual across eligible SKUs.

{{ config(materialized='table') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by sku
            order by n_extreme_weeks asc, total_abs_pct_err asc, wape asc
        ) as rank_by_extremes
    from {{ ref('forecast_evaluation') }}
    where n_weeks_scored >= {{ var('min_scored_weeks', 8) }}
      and total_actual > 0
),

winners as (
    select * from ranked where rank_by_extremes = 1
),

labeled as (
    select
        *,
        ntile(4) over (order by total_actual desc) as sales_quartile
    from winners
)

select
    w.sku,
    d.sku_description,
    case
        when w.model_name = 'naive_4wk_avg' then 'naive_4wk_avg'
        when w.n_extreme_weeks < n.n_extreme_weeks then w.model_name
        when w.n_extreme_weeks = n.n_extreme_weeks
             and w.total_abs_pct_err < n.total_abs_pct_err then w.model_name
        else 'naive_4wk_avg'
    end as selected_model,

    w.total_actual,
    case w.sales_quartile
        when 1 then '1 - first_quarter_selling_sku'
        when 2 then '2 - second_quarter_selling_sku'
        when 3 then '3 - third_quarter_selling_sku'
        when 4 then '4 - fourth_quarter_selling_sku'
    end as sales_category,

    w.n_extreme_weeks as winner_extreme_weeks,
    n.n_extreme_weeks as naive_extreme_weeks,
    w.n_weeks_scored,
    w.total_abs_pct_err as winner_total_err,
    n.total_abs_pct_err as naive_total_err

from labeled w
left join {{ ref('forecast_evaluation') }} n
    on n.sku = w.sku and n.model_name = 'naive_4wk_avg'
left join {{ ref('int_sku_description') }} d
    on d.sku = w.sku
