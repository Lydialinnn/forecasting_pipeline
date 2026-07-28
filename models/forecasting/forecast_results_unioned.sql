-- All model forecasts, standardized WEEKLY schema. Kalman (Cloud Run) joins in
-- once var include_kalman = true and the external table exists.
--
-- CENTRAL OUTPUT CAP: forecast_value is capped here, for EVERY model, at
-- output_cap_multiple × the SKU's p75 reference week (winsor_ref_p75 from
-- fct_weekly_sku_sales — same reference that anchors the input winsorization).
-- Why here and not inside each model:
--   * one implementation instead of one per model — no model can be "the exception"
--     (Kalman runs in a separate Cloud Run job and previously had no cap at all,
--     so its local-linear-trend could extrapolate past the ceiling into the PO number)
--   * this is a view, so the cap is applied at READ time: changing
--     output_cap_multiple re-caps everything without re-running any model
--   * everything downstream (evaluation, winner selection, forecasting_total,
--     forecast_display) reads through this view, so all inherit the cap
-- lower_bound / upper_bound are deliberately NOT capped: they express each model's
-- genuine uncertainty, and capping them would corrupt the coverage /
-- interval-width diagnostics in forecast_evaluation.
-- A null cap (SKU with no usable p75 reference) means NO capping, not a null
-- forecast — hence ifnull(cap, forecast_value).

{{ config(materialized='view') }}

with all_models as (

    select * from {{ ref('forecast_timesfm') }}
    union all
    select * from {{ ref('forecast_arima') }}
    union all
    select * from {{ ref('forecast_naive_baseline') }}
    {% if var('include_kalman', false) %}
    union all
    select * from {{ source('forecasting_external', 'forecast_kalman') }}
    {% endif %}

),

sku_output_cap as (

    -- one row per SKU; winsor_ref_p75 is identical on every week of a SKU
    select
        sku,
        max(winsor_ref_p75) * {{ var('output_cap_multiple', 3) }} as cap_qty
    from {{ ref('fct_weekly_sku_sales') }}
    group by 1

)

select
    f.model_name,
    f.sku,
    f.forecast_week_start,
    greatest(least(f.forecast_value, ifnull(c.cap_qty, f.forecast_value)), 0) as forecast_value,
    f.lower_bound,      -- uncapped on purpose (see header)
    f.upper_bound,      -- uncapped on purpose (see header)
    f.confidence_level,
    f.forecast_run_date,
    f.train_end_week

from all_models f
left join sku_output_cap c using (sku)
