{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is none or custom_alias_name == "main" -%}
        {{ node.name }}
    {%- else -%}
        {% set node_name = node.name ~ '_' ~ custom_alias_name %}
        {{ node_name | trim }}
    {%- endif -%}
{%- endmacro %}
