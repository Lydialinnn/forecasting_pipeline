-- forecast_display.sql — one-table source for Tableau/BI fan charts, WEEKLY grain.
-- Actuals are CROSS-JOINED to the model list so every model "section" carries
-- its own copy of the actuals: filtering on model_name keeps actual + forecast
-- + bounds together for that model. Latest vintage per model only.
-- Grain: model_name × sku × week_start.

{{ config(materialized='view') }}

with latest_forecasts as (

    select *
    from {{ ref('forecast_results_unioned') }}
    qualify forecast_run_date = max(forecast_run_date) over (partition by model_name)

),

model_list as (

    select distinct model_name from latest_forecasts

),

actuals_by_model as (

    select
        m.model_name,
        a.sku,
        a.week_start as ds,
        a.net_qty_raw as actual,
        a.net_qty_unconstrained as actual_unconstrained,
        a.low_stock_days,
        a.winsor_cap_qty
    from {{ ref('fct_weekly_sku_sales') }} a
    cross join model_list m

)

select
    coalesce(a.model_name, f.model_name) as model_name,
    coalesce(a.sku, f.sku) as sku,
    d.sku_description,
    final_model.selected_model,
    coalesce(a.ds, f.forecast_week_start) as ds,

    -- qty columns scaled from cartons to individual UNITS via the SKU's
    -- pack size (_extracted_unit_conversion; coalesced to 1 if unknown)
    a.actual * coalesce(d._extracted_unit_conversion, 1) as actual,
    a.actual_unconstrained * coalesce(d._extracted_unit_conversion, 1) as actual_unconstrained,
    a.low_stock_days,        -- weeks with low_stock_days > scoring_max_low_stock_days excluded from scoring
    -- the SKU's spike ceiling (same value every week; NULL = no ceiling applied).
    -- A week where actual_unconstrained = winsor_cap_qty was reduced to it.
    a.winsor_cap_qty * coalesce(d._extracted_unit_conversion, 1) as winsor_cap_qty,
    f.forecast_value * coalesce(d._extracted_unit_conversion, 1) as forecast_value,
    f.lower_bound * coalesce(d._extracted_unit_conversion, 1) as lower_bound,
    f.upper_bound * coalesce(d._extracted_unit_conversion, 1) as upper_bound,
    d._extracted_unit_conversion,
    f.confidence_level,
    f.forecast_run_date,
    f.train_end_week,

    case
        when f.forecast_week_start is not null and a.actual is not null then 'test (holdout)'
        when f.forecast_week_start is not null then 'forecast only'
        else 'train (history)'
    end as period

from actuals_by_model a
full outer join latest_forecasts f
    on  f.model_name = a.model_name
    and f.sku = a.sku
    and f.forecast_week_start = a.ds
left join {{ ref('int_sku_description') }} d
    on d.sku = coalesce(a.sku, f.sku)
left join {{ ref('forecasting_total') }} final_model
    on final_model.sku = coalesce(a.sku, f.sku)
