-- Fails if any SKU has date gaps (rows per SKU must equal date_diff(max,min)+1).
-- Gap-free input is REQUIRED: AI.FORECAST infers frequency from timestamps.

select
    sku,
    count(*) as n_rows,
    date_diff(max(sales_date), min(sales_date), day) + 1 as expected_rows
from {{ ref('fct_daily_sku_sales') }}
group by 1
having count(*) != date_diff(max(sales_date), min(sales_date), day) + 1
