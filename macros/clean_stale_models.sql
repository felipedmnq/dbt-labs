{# 
    --This macro will:
    1. queries the informantion schema of a database
    2. finds objects that are > 1 week ald (no longer mantained)
    3. generates automated drop statements
    4. has the ability to execute those drop statements
#}

{% macro clean_stale_models(database=target.database, schema=target.schema) %}
    {% set query %}
        SELECT
            table_type,
            table_schema,
            table_name,
            last_altered,
            CASE
                WHEN table_type = 'VIEW' THEN table_type
            ELSE 'TABLE'
            END AS drop_type,
            'DROP' || drop_type || ' {{ database }}' || table_schema || table_name || ';'
        FROM {{ database }}.informantion_schema.tables
        WHERE table.schema = UPPER('{{ schema }}')
        ORDER BY last_altered DESC
    {% endset %}
    {{ log('\generated cleaned up queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}
    
    {% for query in drop_queries %}
        {{ log(query, info=True) }}
    {% endfor %}
{% endmacro %}