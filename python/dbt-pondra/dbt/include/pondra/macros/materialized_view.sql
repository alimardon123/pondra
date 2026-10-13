{#- Pondra keeps a materialized view current as rows are written: there is nothing to refresh. The empty
    statement is what the materialization skips, and the log says so for each model that asks. -#}

{% macro pondra__refresh_materialized_view(relation) -%}
  {%- do log("dbt-pondra: " ~ relation ~ " is kept current by Pondra as rows are written, so it is not refreshed", info=True) -%}
{%- endmacro %}


{% macro pondra__get_replace_materialized_view_sql(relation, sql) %}
  create or replace materialized view {{ relation }} as
    {{ sql }};
{% endmacro %}


{#- Pondra refuses to rename a materialized view ("DROP it, and CREATE it under the new name"). A table or view
    rebuilt in place renames the old materialized view to its backup and drops the backup at the end, so
    the old one is dropped here instead. The new table is made before this runs, so a failed build keeps it. -#}
{% macro pondra__rename_relation(from_relation, to_relation) -%}
  {%- if from_relation.is_materialized_view -%}
    {% call statement('rename_relation') -%}
      drop materialized view if exists {{ from_relation.render() }}
    {%- endcall %}
  {%- else -%}
    {{ default__rename_relation(from_relation, to_relation) }}
  {%- endif -%}
{%- endmacro %}
