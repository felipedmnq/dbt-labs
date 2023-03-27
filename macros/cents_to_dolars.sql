{% macro cents_to_dolars() -%}
    amount / 100
{%- endmacro %}

{% macro cents_to_dolars_with_param(column_name) -%}
    {{ column_name }} / 100
{%- endmacro %}

{% macro cents_to_dollars_percent(column_name, decimal_places=2) -%}
    round(1.0 * {{ column_name }} / 100, {{ decimal_places }})
{%- endmacro %}