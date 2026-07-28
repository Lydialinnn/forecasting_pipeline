-- forecasting_total.sql
-- Per SKU: the winning model's latest-vintage forecast summed over the next
-- {{ var('order_horizon_weeks', 8) }} weeks, in units and cartons.
-- This is BASE demand (expected normal run-rate), NOT a purchase-order quantity:
-- known future promos / launches / one-offs are added on top by the buyer.
-- NOTE on bounds: summing weekly 90% bounds OVERSTATES the interval of the
-- 8-week total (weekly errors partly cancel). Treat lower/upper as a
-- conservative envelope, not a true 90% interval for the total.

{{ config(materialized='table') }}

with latest_forecasts as (
    select *
    from {{ ref('forecast_results_unioned') }}
    qualify forecast_run_date = max(forecast_run_date) over (partition by model_name)
),

-- SKUs without a winner row (too young to have been evaluated: launched less
-- than ~holdout+training weeks ago) fall back to var('fallback_model').
sku_model_choice as (
    select
        f.sku,
        coalesce(w.selected_model, '{{ var("fallback_model", "naive_4wk_avg") }}') as chosen_model,
        w.sku is null as is_fallback_model
    from (select distinct sku from latest_forecasts) f
    left join {{ ref('forecast_model_winner') }} w
      on w.sku = f.sku
),

winner_forecasts as (
    select f.*, c.is_fallback_model
    from latest_forecasts f
    join sku_model_choice c
      on c.sku = f.sku and c.chosen_model = f.model_name
    where date_diff(f.forecast_week_start, f.train_end_week, day)
            <= {{ var('order_horizon_weeks', 8) }} * 7
),

model_totals as (

    select
        f.sku,
        f.model_name as selected_model,
        f.is_fallback_model,   -- true = SKU too young to evaluate; using var('fallback_model')
        f.forecast_run_date,
        min(f.forecast_week_start) as window_start_week,
        max(f.forecast_week_start) as window_end_week,
        count(*) as n_forecast_weeks,
        round(sum(f.forecast_value), 1) as forecast_total_qty
        -- round(sum(f.lower_bound), 1) as forecast_total_lower,   -- conservative envelope
        -- round(sum(f.upper_bound), 1) as forecast_total_upper,   -- conservative envelope
    from winner_forecasts f
    group by 1, 2, 3, 4

),

-- BOOTSTRAP: recently-selling SKUs with NO model forecasts at all (too young to
-- pass has_min_history, i.e. < forecast_min_history_weeks complete weeks).
-- order-window total = run-rate projection:
--   sum(sales) / iso-weeks-available (INCLUDING partial weeks) × order_horizon_weeks
bootstrap_totals as (

    select
        a.sku,
        'bootstrap_weekly_avg' as selected_model,
        true as is_fallback_model,
        current_date() as forecast_run_date,
        cast(null as date) as window_start_week,
        cast(null as date) as window_end_week,
        {{ var('order_horizon_weeks', 8) }} as n_forecast_weeks,
        round(
            sum(a.net_qty_unconstrained)
            / count(distinct date_trunc(a.sales_date, week(monday)))
            * {{ var('order_horizon_weeks', 8) }}, 1
        ) as forecast_total_qty
    from {{ ref('fct_daily_sku_sales') }} a
    left join (select distinct sku from latest_forecasts) f
        on f.sku = a.sku
    where f.sku is null  -- no model output for this SKU
    group by 1
    -- activity guard: dead SKUs without forecasts don't get bootstrapped
    having max(case when a.net_qty_raw > 0 then a.sales_date end)
        > date_sub(current_date(), interval {{ var('forecast_activity_window_weeks', 9) }} * 7 day)

),

unioned_totals as (
    select * from model_totals
    union all
    select * from bootstrap_totals
),

-- SKU context: how new is it, and how much of its history was out of stock.
-- Both help a buyer judge how much to trust the base-demand number.
sku_context as (

    select
        w.sku,
        min(d.first_sale_date) as first_sale_date,
        sum(w.low_stock_days) as n_oos_days_total
    from {{ ref('fct_weekly_sku_sales') }} w
    join (
        select sku, min(sales_date) as first_sale_date
        from {{ ref('fct_daily_sku_sales') }}
        group by 1
    ) d using (sku)
    group by 1

),

-- MODEL DIVERGENCE: how much the candidate models DISAGREE on the order-window
-- total for this SKU. Works at holdout=0 (no actuals needed) — it compares the
-- models to each other, not to truth. Tight agreement = the winner's number is
-- well-supported; wide scatter = the winner is one opinion among very different
-- ones, so treat it with more caution / buffer.
all_model_window_totals as (
    select
        sku,
        model_name,
        -- floor at 0: negative demand is nonsensical, and a negative total would
        -- flip the sign of divergence_ratio below (mislabelling divergent SKUs as "agree")
        greatest(sum(forecast_value), 0) as model_total
    from latest_forecasts
    where date_diff(forecast_week_start, train_end_week, day)
            <= {{ var('order_horizon_weeks', 10) }} * 7
    group by 1, 2
),

divergence as (
    select
        sku,
        count(*) as n_models_comparable,
        round(min(model_total), 1) as min_model_total,
        round(max(model_total), 1) as max_model_total,
        -- spread ÷ mean, always >= 0 (numerator >= 0, floored totals keep mean >= 0):
        -- 0 = identical, 1 = range equals the average forecast. nullif guards the
        -- all-zero case (every model forecasts 0) -> null -> 'n/a' label.
        safe_divide(max(model_total) - min(model_total), nullif(avg(model_total), 0)) as divergence_ratio
    from all_model_window_totals
    group by 1
)

select
    t.sku,
    d.sku_description,
    d.brand_category_key,
    t.selected_model,
    t.is_fallback_model,
    t.forecast_run_date,
    t.window_start_week,
    t.window_end_week,
    t.n_forecast_weeks,
    t.forecast_total_qty,
    d._extracted_unit_conversion,
    round((t.forecast_total_qty * d._extracted_unit_conversion), 1) as forecast_total_unit,

    -- SKU age: how long the SKU had been selling by the start of the forecast
    -- window (falls back to today for bootstrap SKUs, which have no window)
    x.first_sale_date,
    round(date_diff(coalesce(t.window_start_week, current_date()), x.first_sale_date, day) / 7.0, 1)
        as sku_age_weeks,
    -- total OOS days across history: a high count means much of this SKU's demand
    -- signal was censored, so the base-demand number rests on imputed weeks
    x.n_oos_days_total,

    -- model-agreement diagnostic (null for bootstrap SKUs with no model forecasts)
    v.n_models_comparable,
    round(v.divergence_ratio, 2) as model_divergence_ratio,
    case
        when v.divergence_ratio is null or v.n_models_comparable < 2 then 'n/a (single model)'
        when abs(v.divergence_ratio) <= {{ var('model_divergence_low', 0.25) }} then '1 - models agree'
        when abs(v.divergence_ratio) <= {{ var('model_divergence_high', 0.75) }} then '2 - medium'
        else '3 - models disagree'
    end as model_divergence

from unioned_totals t
left join {{ ref('int_sku_description') }} d
    on d.sku = t.sku
left join divergence v
    on v.sku = t.sku
left join sku_context x
    on x.sku = t.sku
