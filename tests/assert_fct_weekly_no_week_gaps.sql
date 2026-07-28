-- Fails if any SKU has missing weeks (rows must equal weeks between min and max).
-- Gap-free weekly input is REQUIRED: AI.FORECAST infers frequency from timestamps.

select
    sku,
    count(*) as n_rows,
    date_diff(max(week_start), min(week_start), day) / 7 + 1 as expected_rows
from {{ ref('fct_weekly_sku_sales') }}
group by 1
having count(*) != date_diff(max(week_start), min(week_start), day) / 7 + 1
