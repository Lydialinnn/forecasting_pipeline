{{ config(materialized='view') }}

-- Low-stock sku-days feeding the imputation in fct_daily_sku_sales.
-- Semantics downstream: a (sku, inventory_date) row present = low stock that day;
-- absent = NOT low stock. Only row PRESENCE is used by the models (is_low_stock).
--
-- Source: daily_sku_stock_metrics (maintained, all SKUs, daily Cloud Run job:
-- cloud_run/daily_stock_log). The metrics table itself is unfiltered — the
-- low-stock threshold lives HERE so it can change without redeploying the job:
-- flagged when inventory_qty <= 0 OR stock_in_days <= 1.
--
-- TODO: backfill daily_sku_stock_metrics with historical data (separate files)
-- so imputation covers dates before the job went live.

-- dedup per (remapped) sku-day: the 90939->91386 swap can produce two rows for
-- the same sku-date (old + native), which would fan out the low-stock join in
-- fct_daily_sku_sales and break assert_fct_unique_sku_date. Keep the worst case
-- (min qty / min stock-days) — presence is what matters downstream anyway.
select
    REGEXP_REPLACE(sku, r'^90939', '91386') AS sku,
    snapshot_date as inventory_date,
    min(inventory_qty) as inv_in_day,
    min(stock_in_days) as stock_in_days
from {{ source('valor_inventory', 'daily_sku_stock_metrics') }}
where inventory_qty <= 0
   -- or stock_in_days <= 1 -- excluding this because this will overstated the sales for the inital lauching weeks, more harm than good
group by 1, 2
