from dataclasses import dataclass

from dbt.adapters.postgres.connections import PostgresConnectionManager, PostgresCredentials


@dataclass
class PondraCredentials(PostgresCredentials):
    """The profile keys of `type: postgres`, under `type: pondra`: Pondra's Postgres port takes the same ones."""

    @property
    def type(self):
        return "pondra"


class PondraConnectionManager(PostgresConnectionManager):
    TYPE = "pondra"
