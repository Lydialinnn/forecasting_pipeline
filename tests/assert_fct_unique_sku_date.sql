-- Fails if any (sku, sales_date) appears more than once in fct_daily_sku_sales.

select sku, sales_date, count(*) as n
from {{ ref('fct_daily_sku_sales') }}
group by 1, 2
having count(*) > 1
