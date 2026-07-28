-- Fails if any week_start is not a Monday (dayofweek 2 in BigQuery, Sunday = 1).

select *
from {{ ref('fct_weekly_sku_sales') }}
where extract(dayofweek from week_start) != 2
