# dbt-pondra

dbt for Pondra: dbt-postgres over a node's Postgres port, plus table keys and layouts, MERGE for
incremental models, and materialized views that Pondra keeps current.

Install: `pip install ./python/dbt-pondra` (dbt-core 1.9 or later). Start a node with
`pondra serve <lake> --pg 127.0.0.1:5432`.

`profiles.yml` takes the keys of `type: postgres`:

    dev:
      type: pondra
      host: 127.0.0.1
      port: 5432
      user: u           # a user of the node; an open node takes any
      password: x
      dbname: lake      # the lake's folder name

Configs:

    {{ config(materialized='table', primary_key='id', partition_by='day(ts)',
              cluster_by=['region', 'ts'], order_by='seen') }}

- `primary_key`: a column or a list. A plain INSERT of a key that is there is refused.
- `partition_by`: an expression, such as `day(ts)`. `cluster_by`: a column or a list.
- `order_by`: one column of a keyed table; the greatest value among a key's rows is current.
- `incremental` with `unique_key`: `merge` (one MERGE) by default; `append` and `delete+insert` as in dbt-postgres.
- `materialized_view`: kept current by Pondra, so a plain run only logs that. A changed query needs `--full-refresh`.

Limits: no enforced contract or `unlogged` with a key or layout; a table a materialized view reads can't be rebuilt while the view exists (Pondra refuses the rename).
