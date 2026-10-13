"""dbt's materialized view class, with Pondra's answers to the three things it asks.

Pondra keeps a materialized view current as rows are written, so the one test that waits for a refresh
asserts that the view is already current (overridden below); everything else is dbt's.
"""
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic


# Counts, not EXISTS: Pondra refuses EXISTS as a value (0A000). The columns are named: Pondra refuses two
# unnamed scalar subqueries in one projection.
SCHEMA_TYPES = """
select
  (select count(*) from pg_matviews where schemaname = '{schema}' and matviewname = '{name}') as matviews,
  (select count(*) from pg_views where schemaname = '{schema}' and viewname = '{name}') as views,
  (select count(*) from pg_tables where schemaname = '{schema}' and tablename = '{name}') as tables
"""


class TestMaterializedViewBasicPondra(MaterializedViewBasic):
    @staticmethod
    def insert_record(project, table, record):
        project.run_sql(f"insert into {table} (id, value) values ({record[0]}, {record[1]})")

    @staticmethod
    def refresh_materialized_view(project, materialized_view):
        # dbt sends REFRESH; Pondra takes it and has nothing to do
        project.run_sql(f"refresh materialized view {materialized_view}")

    @staticmethod
    def query_row_count(project, relation):
        return project.run_sql(f"select count(*) from {relation}", fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation):
        sql = SCHEMA_TYPES.format(schema=relation.schema, name=relation.identifier)
        matviews, views, tables = project.run_sql(sql, fetch="one")
        if matviews:
            return "materialized_view"
        if views:
            return "view"
        return "table" if tables else None

    def test_materialized_view_only_updates_after_refresh(
        self, project, my_materialized_view, my_seed
    ):
        # Pondra's view is current at once: a row written to the table is in the view before any refresh
        table_start = self.query_row_count(project, my_seed)
        view_start = self.query_row_count(project, my_materialized_view)
        self.insert_record(project, my_seed, (4, 400))
        table_mid = self.query_row_count(project, my_seed)
        view_mid = self.query_row_count(project, my_materialized_view)
        self.refresh_materialized_view(project, my_materialized_view)
        table_end = self.query_row_count(project, my_seed)
        view_end = self.query_row_count(project, my_materialized_view)

        assert table_start < table_mid == table_end
        assert view_start < view_mid == view_end == table_end
