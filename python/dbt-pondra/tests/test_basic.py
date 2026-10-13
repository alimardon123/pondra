"""dbt's own adapter suite (dbt-tests-adapter), run as it is written: the basic classes."""
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp


class TestSimpleMaterializationsPondra(BaseSimpleMaterializations):
    pass


class TestSingularTestsPondra(BaseSingularTests):
    pass


class TestSingularTestsEphemeralPondra(BaseSingularTestsEphemeral):
    pass


class TestEmptyPondra(BaseEmpty):
    pass


class TestEphemeralPondra(BaseEphemeral):
    pass


class TestIncrementalPondra(BaseIncremental):
    pass


class TestGenericTestsPondra(BaseGenericTests):
    pass


class TestSnapshotCheckColsPondra(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampPondra(BaseSnapshotTimestamp):
    pass


class TestAdapterMethodPondra(BaseAdapterMethod):
    pass


class TestDocsGeneratePondra(BaseDocsGenerate):
    pass
