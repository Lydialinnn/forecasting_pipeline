-- Fails if any (sku, week_start) appears more than once in fct_weekly_sku_sales.

select sku, week_start, count(*) as n
from {{ ref('fct_weekly_sku_sales') }}
group by 1, 2
having count(*) > 1
