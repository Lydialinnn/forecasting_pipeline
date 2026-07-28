-- Passthrough of valor_margin_dbt.int_product_category (built by the other project).

-- Dedup to 1 row per (remapped) SKU: the 90939->91386 swap can collide with a
-- native 91386 row; both share brand/category (business-confirmed), so keeping
-- either is lossless. Without this, int_sku_description fans out and
-- fct_daily_sku_sales double-counts the SKU's sales.
select
    REGEXP_REPLACE(SKU, r'^90939', '91386') AS SKU,
    Brand,
    category_formatted

from {{ source('valor_margin', 'int_product_category') }}
qualify row_number() over (
    partition by REGEXP_REPLACE(SKU, r'^90939', '91386')
    order by Brand, category_formatted
) = 1
