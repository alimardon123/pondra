{#- A model's table options: a key and a layout are written into CREATE TABLE, the way Pondra spells them
    (docs: sql/table-options). A model with none of them is written exactly as dbt-postgres writes it. -#}

{% macro pondra__create_table_as(temporary, relation, sql) -%}
  {%- set key = config.get('primary_key') -%}
  {%- set options = pondra_table_options() -%}
  {%- if temporary or (not key and not options) -%}
    {{ return(postgres__create_table_as(temporary, relation, sql)) }}
  {%- endif -%}

  {#- Pondra can't write these with a layout yet, so they are refused rather than left out without a word -#}
  {%- if config.get('contract').enforced -%}
    {{ exceptions.raise_compiler_error("dbt-pondra: primary_key and the layout configs can't be used with an enforced contract yet") }}
  {%- endif -%}
  {%- if config.get('unlogged') -%}
    {{ exceptions.raise_compiler_error("dbt-pondra: unlogged has no Pondra table option") }}
  {%- endif -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  {#- A key is named in the column list; with only the key there, Pondra takes the columns from the query -#}
  create table {{ relation }}
  {%- if key %} (PRIMARY KEY ({{ pondra_names(key) }})){% endif %}
  {%- if options %} with ({{ options | join(', ') }}){% endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro pondra_table_options() %}
  {#- Each option a model sets, as Pondra's WITH option: partition_by = 'day(ts)', cluster_by = 'a, b', order_by = 'seen' -#}
  {#- (a quote in a value is doubled, so the option stays one string) -#}
  {%- set options = [] -%}
  {%- if config.get('partition_by') -%}
    {%- do options.append("partition_by = '" ~ (config.get('partition_by') | replace("'", "''")) ~ "'") -%}
  {%- endif -%}
  {%- if config.get('cluster_by') -%}
    {%- do options.append("cluster_by = '" ~ (pondra_names(config.get('cluster_by')) | replace("'", "''")) ~ "'") -%}
  {%- endif -%}
  {%- if config.get('order_by') -%}
    {%- do options.append("order_by = '" ~ (pondra_one_name(config.get('order_by')) | replace("'", "''")) ~ "'") -%}
  {%- endif -%}
  {{ return(options) }}
{% endmacro %}


{% macro pondra_names(names) -%}
  {#- one column name, or a list of them, comma-separated -#}
  {%- if names is string -%}
    {{ names }}
  {%- else -%}
    {{ names | join(', ') }}
  {%- endif -%}
{%- endmacro %}


{% macro pondra_one_name(name) %}
  {#- order_by is Pondra's SEQUENCE BY, which takes one column: the row with the greatest value of it wins -#}
  {%- if name is string -%}
    {{ return(name) }}
  {%- elif name | length == 1 -%}
    {{ return(name[0]) }}
  {%- else -%}
    {{ exceptions.raise_compiler_error("dbt-pondra: order_by takes one column, not " ~ name | join(', ')) }}
  {%- endif -%}
{% endmacro %}
