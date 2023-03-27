{% macro grant_select(schema=target.schema, role=target.role) %}
    {% set query %}
        GRANT USAGE ON SCHEMA {{ schema }} TO role {{ role }};
        GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO role {{ role }};
        GRANT SELECT ON ALL VIEWS IN SCHEMA {{ schema }} TO role {{ role }}
    {% endset %}
    {{ log('[INFO] - Granting select on all tables and views on schema ' ~ schema ~ ' to role ' ~ role, info=True) }}
    {% do run_query(query) %}
    {{ log('[INFO] - Privileges granted', info=True) }}
{% endmacro %}