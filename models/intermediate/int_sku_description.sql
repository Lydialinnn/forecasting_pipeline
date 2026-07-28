-- int_sku_description.sql
-- One row per SKU (display/label table, never a series ID — see forecasting spec).
-- sku_description + _extracted_unit_conversion from the SKU's latest order record;
-- brand_category_key from valor_margin's product category view.
--
-- INCREMENTAL (merge on sku): daily runs only scan order rows newer than the
-- stored last_updated_at watermark and upsert the affected SKUs.
-- NOTE: a SKU only refreshes when it gets a NEW order row — if valor_margin
-- recategorizes a dormant SKU, run with --full-refresh to pick it up
-- (worth doing occasionally, e.g. monthly).

{{ config(
    materialized='incremental',
    unique_key='sku',
    incremental_strategy='merge'
) }}

with latest_sku_record as (

    select
        o.sku,
        concat(
            upper(trim(o.product_title)),
            coalesce(nullif(concat('_', upper(trim(o.sku_variant_title))), '_'), '')
        ) as sku_description,
        COALESCE(CAST(REGEXP_EXTRACT(o.product_title, r'(?i)\(\s*(\d+)\s*PCS?/C') AS INT64), 1) as _extracted_unit_conversion,
        o.last_updated_at

    from {{ ref('stg_valor_shopify__order_line_item') }} o

    where {{ global_order_exclusions('o') }}
    {% if is_incremental() %}
      and o.last_updated_at > (select max(last_updated_at) from {{ this }})
    {% endif %}

    qualify row_number() over (
        partition by o.sku
        order by o.order_date desc, o.last_updated_at desc
    ) = 1

)

select
    l.sku,
    l.sku_description,
    l._extracted_unit_conversion,
    coalesce(CONCAT(upper({{ valor_brand_base('dp.Brand') }}), '_', dp.category_formatted), 'unknown') as brand_category_key,
    l.last_updated_at

from latest_sku_record l
left join {{ ref('stg_valor_margin__product_category') }} dp
    on l.sku = dp.SKU
