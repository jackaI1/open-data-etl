{% macro product_key(day='snapshot_date', code='drug_code') -%}
cast({{ day }} as {{ dbt.type_string() }}) || ':' || cast({{ code }} as {{ dbt.type_string() }})
{%- endmacro %}
{% macro company_key() -%}
cast(snapshot_date as {{ dbt.type_string() }}) || ':' || coalesce(company_code, 'UNKNOWN')
{%- endmacro %}
