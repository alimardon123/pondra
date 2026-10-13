{#- The default strategy: with a unique_key, one MERGE (a single commit on Pondra, where delete+insert
    is two statements); without one, an append. -#}

{% macro pondra__get_incremental_default_sql(arg_dict) %}
  {% if arg_dict["unique_key"] %}
    {% do return(get_incremental_merge_sql(arg_dict)) %}
  {% else %}
    {% do return(get_incremental_append_sql(arg_dict)) %}
  {% endif %}
{% endmacro %}

{#- dbt's MERGE, with one change: the SET list leaves out the key columns. Pondra refuses an UPDATE
    that sets a table's own key (the ON clause already equates the key, so it can't change anyway).
    Everything else is dbt's statement, so the merge_update_columns and merge_exclude_columns
    configs and incremental_predicates keep working. -#}
{% macro pondra__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
  {%- set keys = [] -%}
  {%- if unique_key is string -%}{%- set keys = [unique_key] -%}{%- elif unique_key -%}{%- set keys = unique_key -%}{%- endif -%}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set non_key_columns = dest_columns | rejectattr("name", "in", keys) | list -%}
  {%- set update_columns = get_merge_update_columns(config.get('merge_update_columns'), config.get('merge_exclude_columns'), non_key_columns) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {%- for key in keys -%}
    {%- do predicates.append("DBT_INTERNAL_SOURCE." ~ key ~ " = DBT_INTERNAL_DEST." ~ key) -%}
  {%- endfor -%}
  {%- if not keys -%}{%- do predicates.append('FALSE') -%}{%- endif -%}

  {{ sql_header if sql_header is not none }}

  merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on {{ "(" ~ predicates | join(") and (") ~ ")" }}

  {% if keys and update_columns %}
  when matched then update set
    {% for column_name in update_columns -%}
      {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
      {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
  {% endif %}

  when not matched then insert
    ({{ dest_cols_csv }})
  values
    ({{ dest_cols_csv }})
{%- endmacro %}
