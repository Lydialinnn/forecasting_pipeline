{#
  Macro: valor_brand_base
  Purpose: remove stamp type in valor brand
  Copied from valor_margin_dbt — keep in sync if the original changes.
#}

{% macro valor_brand_base(valor_brand) %}
   upper(TRIM(REGEXP_REPLACE({{ valor_brand }}, r'^LK|\([^)]*\)|\[[^\]]*\]|\)', '')))
{% endmacro %}
