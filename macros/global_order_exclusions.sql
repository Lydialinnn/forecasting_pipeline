{#
  Macro: global_order_exclusions
  Purpose: single definition of the global exclusion filters
           (mirrors valor_margin_dbt/int_fulfilled_sales — keep in sync).
  Usage:   where {{ global_order_exclusions('o') }}
#}

{% macro global_order_exclusions(o) %}
    {{ o }}.cancel_at is null
    -- ONLY globally exclude these three:
    and not REGEXP_CONTAINS(LOWER(COALESCE({{ o }}.product_title, 'none')), r'excise|tax|fee|custom item|flavour menu|\brma\b|return variance') -- custom item: shipping
    and not REGEXP_CONTAINS(COALESCE({{ o }}.sku, 'none'), r'-STH$')
    and not REGEXP_CONTAINS(COALESCE({{ o }}.customer_name, 'none'), r'^STLTH')
    and COALESCE({{ o }}.sku, 'none') != 'none'
    and not {{ o }}.net_qty = 0
{% endmacro %}
