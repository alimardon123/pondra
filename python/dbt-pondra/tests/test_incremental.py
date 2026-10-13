"""dbt's incremental classes: unique keys (one MERGE by default) and merge_exclude_columns."""
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import BaseMergeExcludeColumns
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization


class TestIncrementalUniqueKeyPondra(BaseIncrementalUniqueKey):
    pass


class TestMergeExcludeColumnsPondra(BaseMergeExcludeColumns):
    pass


class TestIncrementalPredicatesDeleteInsertPondra(BaseIncrementalPredicates):
    pass


class TestTableMaterializationPondra(BaseTableMaterialization):
    pass
