{% macro limit_data_in_dev(column_name, dev_days_of_data=3) -%}
    {% if target.name == 'default' %}
        WHERE {{ column_name }} >= DATEADD('day', -{{ dev_days_of_data }}, CURRENT_TIMESTAMP)
    {% endif %}
{%- endmacro -%}