{#
  By default dbt names schemas "<target schema>_<custom schema>", e.g. analytics_staging.
  This override uses the custom name as-is: staging, intermediate, marts, reference.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
