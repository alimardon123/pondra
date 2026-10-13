"""Pondra's own parts: each config a model can set, checked against the table Pondra made.

A key and a layout are read back with SHOW CREATE TABLE (Pondra's own statement), a MERGE by the
adapter response's code, and a materialized view by the rows it has with no refresh.
"""
import pytest
from dbt.tests.util import run_dbt

SRC_CSV = "id,name,amount\n1,ann,10\n2,bob,20\n3,cy,30\n"
EVENTS_CSV = "id,region,ts,seen,amount\n1,eu,2026-09-01 10:00:00,2026-09-01 10:00:00,5\n2,us,2026-09-02 11:00:00,2026-09-02 11:00:00,6\n"

MODELS = {
    "keyed.sql": (
        "{{ config(materialized='table', primary_key='id', partition_by='day(ts)', "
        "cluster_by=['region', 'ts'], order_by='seen') }}\n"
        "select id, region, ts, seen, amount from {{ ref('events') }}\n"
    ),
    "plain.sql": "{{ config(materialized='table') }}\nselect id, region from {{ ref('events') }}\n",
    "merged.sql": (
        "{{ config(materialized='incremental', unique_key='id') }}\n"
        "select id, name, amount from {{ ref('src') }}\n"
    ),
    "merged_keyed.sql": (
        "{{ config(materialized='incremental', unique_key='id', primary_key='id') }}\n"
        "select id, name, amount from {{ ref('src') }}\n"
    ),
    "mv.sql": "{{ config(materialized='materialized_view') }}\nselect id, amount from {{ ref('src') }}\n",
    "bad_order.sql": (
        "{{ config(materialized='table', primary_key='id', order_by=['seen', 'amount']) }}\n"
        "select id, seen, amount from {{ ref('events') }}\n"
    ),
    "scaled.sql": (
        "{{ config(materialized='materialized_view') }}\n"
        "select id, amount * {{ var('scale', 1) }} as scaled from {{ ref('src') }}\n"
    ),
    "base_tbl.sql": "{{ config(materialized='table') }}\nselect id, amount from {{ ref('src') }}\n",
    "over_tbl.sql": "{{ config(materialized='materialized_view') }}\nselect id, amount from {{ ref('base_tbl') }}\n",
}


# Module-level fixtures override dbt's; pytest 9 deprecates class-scoped fixtures written as methods.
@pytest.fixture(scope="class")
def seeds():
    return {"src.csv": SRC_CSV, "events.csv": EVENTS_CSV}


@pytest.fixture(scope="class")
def models():
    return dict(MODELS)


class TestPondraConfigs:
    def show(self, project, name):
        return project.run_sql(f"SHOW CREATE TABLE {project.test_schema}.{name}", fetch="one")[0]

    def test_key_and_layout_are_written(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "keyed"])
        ddl = self.show(project, "keyed")
        assert "PRIMARY KEY (id)" in ddl
        assert "PARTITION BY day(ts)" in ddl
        assert "CLUSTER BY (region, ts)" in ddl
        assert "SEQUENCE BY seen" in ddl

    def test_key_is_kept_by_key(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "keyed"])
        with pytest.raises(Exception, match="duplicate key"):
            project.run_sql(f"insert into {project.test_schema}.keyed (id, region, ts, seen, amount) "
                            "values (1, 'eu', timestamp '2026-09-03 00:00:00', timestamp '2026-09-03 00:00:00', 1)")

    def test_model_without_config_is_a_plain_table(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "plain"])
        ddl = self.show(project, "plain")
        assert "PRIMARY KEY" not in ddl
        assert "PARTITION BY" not in ddl
        assert "CLUSTER BY" not in ddl
        assert "SEQUENCE BY" not in ddl

    def test_merge_is_the_default_with_a_unique_key(self, project):
        run_dbt(["seed"])
        # The schema is shared by the class, so the first run starts the table over: it is not a MERGE.
        first = run_dbt(["run", "--select", "merged", "--full-refresh"])
        assert first[0].adapter_response["code"] != "MERGE"
        project.run_sql(f"update {project.test_schema}.src set amount = 99 where id = 2")
        project.run_sql(f"insert into {project.test_schema}.src values (4, 'dee', 40)")
        second = run_dbt(["run", "--select", "merged"])
        assert second[0].adapter_response["code"] == "MERGE"
        rows = project.run_sql(f"select id, name, amount from {project.test_schema}.merged order by id", fetch="all")
        assert [tuple(r) for r in rows] == [(1, "ann", 10), (2, "bob", 99), (3, "cy", 30), (4, "dee", 40)]

    def test_merge_into_a_keyed_table(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "merged_keyed"])
        project.run_sql(f"update {project.test_schema}.src set name = 'bobby' where id = 2")
        run_dbt(["run", "--select", "merged_keyed"])
        rows = project.run_sql(f"select id, name from {project.test_schema}.merged_keyed order by id", fetch="all")
        assert [tuple(r) for r in rows] == [(1, "ann"), (2, "bobby"), (3, "cy")]
        assert "PRIMARY KEY (id)" in self.show(project, "merged_keyed")

    def test_materialized_view_sees_new_rows_without_a_refresh(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "mv"])
        count = lambda: project.run_sql(f"select count(*) from {project.test_schema}.mv", fetch="one")[0]
        before = count()
        project.run_sql(f"insert into {project.test_schema}.src values (5, 'eve', 50)")
        assert count() == before + 1
        run_dbt(["run", "--select", "mv"])  # dbt's refresh: taken, and nothing to do
        assert count() == before + 1

    def test_changed_query_needs_full_refresh(self, project):
        # Pondra doesn't expose a view's query text, so a changed query is seen only by --full-refresh,
        # which runs CREATE OR REPLACE; a plain run refreshes (a no-op) and keeps the old query.
        def scaled():
            rows = project.run_sql(f"select scaled from {project.test_schema}.scaled order by id", fetch="all")
            return [r[0] for r in rows]

        run_dbt(["seed"])
        run_dbt(["run", "--select", "scaled", "--vars", "{scale: 1}"])
        assert scaled() == [10, 20, 30]
        run_dbt(["run", "--select", "scaled", "--vars", "{scale: 10}"])
        assert scaled() == [10, 20, 30]
        run_dbt(["run", "--select", "scaled", "--vars", "{scale: 10}", "--full-refresh"])
        assert scaled() == [100, 200, 300]

    def test_refresh_says_it_is_kept_current_once(self, project, capfd):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "mv"])
        capfd.readouterr()  # what the first run printed is not counted
        run_dbt(["run", "--select", "mv"])
        printed = capfd.readouterr().out
        assert printed.count("is kept current by Pondra") == 1

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="Pondra gap: ALTER TABLE RENAME is refused while a materialized view follows the "
                              "table (\"followed by materialized view … drop them first\"); Postgres 16 renames it "
                              "(the view follows the table by OID), and dbt-postgres's DROP … CASCADE of the "
                              "backup then drops the view without a word")
    def test_rebuilt_table_under_a_materialized_view(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "base_tbl", "over_tbl"])
        run_dbt(["run", "--select", "base_tbl"])  # a table is rebuilt by renaming: the view is in the way

    def test_order_by_takes_one_column(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--select", "bad_order"], expect_pass=False)
        assert "order_by takes one column" in results[0].message
