-- Fails if any weekly imputed demand value is negative.

select *
from {{ ref('fct_weekly_sku_sales') }}
where net_qty_unconstrained < 0
