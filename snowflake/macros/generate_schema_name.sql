{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set env = var('env') -%}

    {%- if custom_schema_name is none -%}
        {{ env }}_CURATED
    {%- else -%}
        {{ env }}_{{ custom_schema_name | upper }}
    {%- endif -%}

{%- endmacro %}
