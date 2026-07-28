-- All model forecasts, standardized WEEKLY schema. Kalman (Cloud Run) joins in
-- once var include_kalman = true and the external table exists.

{{ config(materialized='view') }}

select * from {{ ref('forecast_timesfm') }}
union all
select * from {{ ref('forecast_arima') }}
union all
select * from {{ ref('forecast_naive_baseline') }}
{% if var('include_kalman', false) %}
union all
select * from {{ source('forecasting_external', 'forecast_kalman') }}
{% endif %}
