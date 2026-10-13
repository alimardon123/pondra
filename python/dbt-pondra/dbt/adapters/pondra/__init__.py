from dbt.adapters.base import AdapterPlugin

from dbt.adapters.pondra.connections import PondraCredentials
from dbt.adapters.pondra.impl import PondraAdapter
from dbt.include import pondra

# Everything dbt-postgres does and this package doesn't replace comes from the postgres plugin.
Plugin = AdapterPlugin(
    adapter=PondraAdapter,  # type: ignore
    credentials=PondraCredentials,
    include_path=pondra.PACKAGE_PATH,
    dependencies=["postgres"],
)
