-- Fails if stg_low_stock_list has duplicate (sku, inventory_date) rows.

select sku, inventory_date, count(*) as n
from {{ ref('stg_low_stock_list') }}
group by 1, 2
having count(*) > 1
