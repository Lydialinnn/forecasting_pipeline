-- Fails if any imputed demand value is negative.

select *
from {{ ref('fct_daily_sku_sales') }}
where net_qty_unconstrained < 0
