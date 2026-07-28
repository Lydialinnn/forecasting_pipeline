-- Lean staging: only the columns the forecasting pipeline needs.

with source as (
    select * from {{ source('valor_shopify', 'order_line_item') }}
)

select
    order_date,
    -- swap SKU prefix 90939 -> 91386, keeping the suffix
    REGEXP_REPLACE(sku, r'^90939', '91386') AS sku,
    -- swap the flavour name in the titles
    REPLACE(product_title, 'SOUR PEACH ICE', 'TANGY PEACH ICE') AS product_title,
    REPLACE(sku_variant_title, 'SOUR PEACH ICE', 'TANGY PEACH ICE') AS sku_variant_title,
    customer_name,
    net_qty,
    CAST(unit_price AS NUMERIC) AS unit_price,  -- kept for availability (e.g. future is_promo / zero-price regressor); not used downstream yet
    cancel_at,
    last_updated_at

from source