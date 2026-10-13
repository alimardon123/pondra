from dataclasses import dataclass, field
from typing import FrozenSet, List, Optional, Union

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.postgres.impl import PostgresAdapter, PostgresConfig
from dbt.adapters.postgres.relation import PostgresRelation

from dbt.adapters.pondra.connections import PondraConnectionManager


@dataclass(frozen=True, eq=False, repr=False)
class PondraRelation(PostgresRelation):
    # A materialized view can be replaced in place (CREATE OR REPLACE), as a table or view can.
    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {RelationType.View, RelationType.Table, RelationType.MaterializedView}  # type: ignore
        )
    )


@dataclass
class PondraConfig(PostgresConfig):
    """A model's Pondra table options, written into CREATE TABLE (see macros/table.sql)."""

    partition_by: Optional[str] = None
    cluster_by: Optional[Union[str, List[str]]] = None
    order_by: Optional[Union[str, List[str]]] = None
    primary_key: Optional[Union[str, List[str]]] = None


class PondraAdapter(PostgresAdapter):
    ConnectionManager = PondraConnectionManager
    Relation = PondraRelation
    AdapterSpecificConfigs = PondraConfig
