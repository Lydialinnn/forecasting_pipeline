-- fct_daily_sku_sales.sql
-- Shared forecasting input. Grain: sku × sales_date, gap-free per SKU.
-- Reads staging directly: global exclusions via the shared macro,
-- plus the forecasting-specific scope filters below.

{{ config(
    materialized='table',
    partition_by={'field': 'sales_date', 'data_type': 'date'},
    cluster_by=['sku']
) }}

with filtered_sales as (
    select
        o.sku,
        date(o.order_date) as sales_date,
        sum(o.net_qty) as net_qty
    from {{ ref('stg_valor_shopify__order_line_item') }} o
    -- forecasting product scope: EXACT brand_category_key whitelist
    -- (var forecasting_brand_category_list in dbt_project.yml; case-insensitive
    -- equality — no substring matching)
    join {{ ref('int_sku_description') }} d
        on d.sku = o.sku
       and upper(d.brand_category_key) in (
            {%- for b in var("forecasting_brand_category_list") %}
            '{{ b | upper }}'{{ "," if not loop.last }}
            {%- endfor %}
       )
    where {{ global_order_exclusions('o') }}
      -- forecasting-only exclusions (excise/flavour menu already covered by the global macro)
      and not regexp_contains(lower(o.product_title),
            r'variety carton|tester tips|variety stand|marketing')
      and not regexp_contains(lower(o.customer_name), r'lucka lepage|lk hardware')
    group by 1, 2
),

sku_bounds as (
    select sku, min(sales_date) as first_sale_date
    from filtered_sales
    group by 1
),

global_max as (
    select max(sales_date) as max_date from filtered_sales
),

spine as (
    select b.sku, d as sales_date
    from sku_bounds b
    cross join global_max g
    cross join unnest(generate_date_array(b.first_sale_date, g.max_date)) as d
),

joined as (
    select
        s.sku,
        s.sales_date,
        coalesce(f.net_qty, 0) as net_qty_raw,
        inv.sku is not null as is_low_stock,  -- presence-only join; inv qty columns intentionally not carried
        -- launch/settling period: no imputation here (see below)
        s.sales_date < date_add(b.first_sale_date, interval {{ var('launch_weeks', 8) }} * 7 day)
            as in_launch_window
    from spine s
    join sku_bounds b on b.sku = s.sku
    left join filtered_sales f
        on f.sku = s.sku and f.sales_date = s.sales_date
    left join {{ ref('stg_low_stock_list') }} inv
        on inv.sku = s.sku and inv.inventory_date = s.sales_date
),

unconstrained as (
    select
        *,
        -- low-stock days = censored demand: impute the trailing AVERAGE DAILY RATE
        -- over NON-low-stock days — all days in the window, NOT same-weekday.
        -- (Same-weekday averaging used only ~4 samples, so one spiky weekday
        -- skewed the estimate; all-days gives 28/56 samples and is far steadier.)
        -- Takes the GREATEST of: 28-day rate, 56-day rate, and net_qty_raw, so
        -- (a) a declining 28-day window can't under-impute below the longer view,
        -- (b) the value is always floored at raw — on a censored day true demand
        --     is at least what actually sold, stockouts beyond 8 weeks are moot anyway.
        -- is_active drops the SKU after forecast_activity_window_weeks of no sales.
        -- LAUNCH WINDOW: during the first launch_weeks weeks we do NOT impute
        -- (raw only) — launch demand is too noisy to reconstruct, and imputing
        -- there echoes the launch spike into later weeks.
        case
            when is_low_stock and not in_launch_window then
                greatest(
                    coalesce(
                        avg(case when not is_low_stock then net_qty_raw end) over (
                            partition by sku
                            order by unix_date(sales_date)
                            range between 28 preceding and 1 preceding
                        ), 0),
                    coalesce(
                        avg(case when not is_low_stock then net_qty_raw end) over (
                            partition by sku
                            order by unix_date(sales_date)
                            range between 56 preceding and 1 preceding
                        ), 0),
                    net_qty_raw
                )
            else net_qty_raw
        end as net_qty_unconstrained
    from joined
)

-- NOTE: eligibility flags (has_min_history / is_active) live at the WEEKLY
-- grain in fct_weekly_sku_sales — the daily layer only imputes.
select * from unconstrained
