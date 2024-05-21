{% macro get_collab_tables() %}
    {% set query %}
        select table_name
        from information_schema.tables
        where table_schema = 'raw_data_2024'
          and (
            table_name like '04_2024' or
            table_name like '05_2024' or
            table_name like '06_2024' or
            table_name like '07_2024' or
            table_name like '08_2024' or
            table_name like '09_2024' or
            table_name like '10_2024' or
            table_name like '11_2024' or
            table_name like '12_2024'
          )
        union all
        select table_name
        from information_schema.tables
        where table_schema = 'raw_data_2025'
          and (
            table_name like '01_2025' or
            table_name like '02_2025' or
            table_name like '03_2025'
          )
    {% endset %}

    {% set results = run_query(query) %}
    {% set table_list = [] %}

    {% if execute %}
        {% for row in results %}
            {% do table_list.append("raw_data_" ~ row.table_name[-4:] ~ '."' ~ row.table_name ~ '"') %}
        {% endfor %}
    {% endif %}

    {{ return(table_list) }}
{% endmacro %}
