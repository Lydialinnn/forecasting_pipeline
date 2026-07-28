-- fct_weekly_sku_sales.sql
-- Shared MODEL INPUT at weekly grain. Grain: sku × week_start (Mon-start), gap-free per SKU.
-- Low-stock imputation happens upstream at DAILY grain in fct_daily_sku_sales
-- (net_qty_unconstrained); this model only aggregates to ISO weeks.
-- Only COMPLETE weeks (exactly 7 spine days) are kept: the in-progress trailing
-- week and any partial launch week are dropped so every row is a uniform 7-day total.

{{ config(
    materialized='table',
    partition_by={'field': 'week_start', 'data_type': 'date'},
    cluster_by=['sku']
) }}

with weekly as (
    select
        sku,
        date_trunc(sales_date, week(monday)) as week_start,
        count(*) as n_days,
        sum(net_qty_raw) as net_qty_raw,
        sum(net_qty_unconstrained) as net_qty_unconstrained,
        countif(is_low_stock) as low_stock_days
    from {{ ref('fct_daily_sku_sales') }}
    group by 1, 2
),

complete_weeks as (
    select * from weekly where n_days = 7
),

-- Winsorize net_qty_unconstrained per SKU to curb the launch-echo problem:
-- launch-week spikes get imputed forward by fct_daily_sku_sales's trailing-avg
-- rule, producing a fake elevated plateau that drift-based models (ARIMA, Kalman)
-- read as a downward trend. Cap at input_winsor_multiple × the SKU's own
-- ceiling = input_winsor_multiple × MEDIAN of the weeks after
-- winsor_ref_exclude_weeks (2). The exclusion is short on purpose so even young
-- SKUs get a ceiling (a launch_weeks-long exclusion would leave <12-week SKUs
-- uncapped); safe at 2 because imputation is off during launch_weeks in
-- fct_daily, so weeks 3+ are genuine raw values, not launch echoes.
-- The ceiling is applied to EVERY week (including 1-2, which are excluded only
-- from *defining* it), so it trims launch spikes AND mid-life spikes down toward
-- typical demand. Applies once a reference week exists (SKU >= 3 complete weeks);
-- younger SKUs pass through. net_qty_raw is NEVER modified.
ranked as (
    select
        *,
        row_number() over (partition by sku order by week_start) as wk_rank
    from complete_weeks
),

sku_winsor_cap as (
    select
        sku,
        -- 75th PERCENTILE of the reference weeks (not max, not median):
        --  * max would make the ceiling unreachable for weeks inside the reference
        --    set (any such week is <= the max), so only weeks 1-2 could ever clip;
        --  * median proved strongly size-biased in testing — it removed ~19% of
        --    real units from the smallest SKUs vs ~2% from the largest, and left
        --    ~24% of the catalogue (median = 0, intermittent sellers) with no
        --    ceiling at all. p75 removes a near-flat ~2-5% across all SKU sizes.
        -- Heavily censored weeks are EXCLUDED: an OOS week reads near zero (no
        -- imputation at all inside launch_weeks) and would drag the reference down,
        -- over-tightening the ceiling. Same threshold as evaluation scoring.
        -- nullif(p75, 0): a very sparse SKU can still have p75 = 0 — then cap = 0
        -- and least(x, 0) would zero out its ENTIRE history. Treat 0 as "no usable
        -- reference" and disable the cap (null -> pass-through).
        nullif(
            approx_quantiles(
                if(wk_rank > {{ var('winsor_ref_exclude_weeks', 2) }}
                   and low_stock_days <= {{ var('scoring_max_low_stock_days', 3) }},
                   net_qty_unconstrained, null),
                100
            )[safe_offset(75)], 0
        ) * {{ var('input_winsor_multiple', 3) }} as cap_qty
    from ranked
    group by 1
),

winsorized as (
    select
        r.sku,
        r.week_start,
        r.net_qty_raw,
        -- exposed for auditing: same value on every week of a SKU. A week where
        -- net_qty_unconstrained = winsor_cap_qty was reduced to the ceiling.
        -- NULL = no ceiling applied for this SKU (no usable reference weeks).
        round(c.cap_qty, 1) as winsor_cap_qty,
        -- cap once a post-launch peak exists (SKU older than launch_weeks);
        -- for younger SKUs cap_qty is null → pass through unchanged
        case
            when c.cap_qty is not null then least(r.net_qty_unconstrained, c.cap_qty)
            else r.net_qty_unconstrained
        end as net_qty_unconstrained,
        r.low_stock_days
    from ranked r
    left join sku_winsor_cap c using (sku)
),

global_max as (
    select max(week_start) as max_week from winsorized
),

flags as (
    select
        w.*,
        count(*) over (partition by w.sku) >= {{ var('forecast_min_history_weeks', 13) }}
            as has_min_history,
        max(case when w.net_qty_raw > 0
                 and w.week_start > date_sub(g.max_week,
                        interval {{ var('forecast_activity_window_weeks', 9) }} week)
            then 1 else 0 end) over (partition by w.sku) = 1
            as is_active
    from winsorized w
    cross join global_max g
)

select * from flags
