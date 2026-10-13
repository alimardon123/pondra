"""SQLAlchemy 2.0 dialects for Pondra, over `pondra.dbapi` (no server needed) or the Postgres port:

    create_engine("pondra:///path/to/lake")          a folder (or "pondra://" / "pondra:///:memory:": a lake in memory)
    create_engine("pondra+http://user:pw@host:8080") a node over HTTP (?token=… or user and password)
    create_engine("pondra+https://host")             the same over HTTPS
    create_engine("pondra+psycopg://u@host:5432/db") the Postgres port, with psycopg

Pondra's table options are dialect keywords: Table("t", …, pondra_partition_by="day(ts)",
pondra_cluster_by=["region", "ts"], pondra_order_by="ts") writes WITH (partition_by = …, cluster_by = …,
order_by = …), and Inspector.get_table_options reads them back from SHOW CREATE TABLE.

SQLAlchemy is imported only here; the client never needs it. The wheel declares the dialects as entry
points (python/pyproject.toml's `sqlalchemy.dialects`), so `pip install pondra sqlalchemy` is all a user
installs; `register()` makes the same names known without them (tests, and a checkout run from its folder).
Where Alembic is installed, its migrations run on Pondra as on Postgres (`PondraImpl`, at the end).
"""
import json
import re
import uuid

from sqlalchemy import Table, exc
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler, PGDialect
from sqlalchemy.dialects.postgresql.pg_catalog import INT2VECTOR, OIDVECTOR
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.dialects.postgresql.types import PGUuid
from sqlalchemy.engine import reflection

# Pondra answers as PostgreSQL 16 (the Postgres port says "16.0" to SHOW server_version and to version()).
SERVER_VERSION = (16, 0)
# The isolation levels Pondra runs (invariant 323). AUTOCOMMIT is the driver's own (each statement alone).
LEVELS = ("READ COMMITTED", "REPEATABLE READ")


def _check_level(level):
    """Refuse a level Pondra doesn't run, before any statement is sent."""
    name = level.replace("_", " ").upper()
    if name not in (*LEVELS, "AUTOCOMMIT"):
        raise exc.ArgumentError(f"Pondra runs READ COMMITTED and REPEATABLE READ, not {name} (AUTOCOMMIT too): "
                                "it refuses SERIALIZABLE by name rather than run it as something less")


def _quoted(value):
    """A table option's value as WITH (…) takes it: a list is joined with commas, a quote doubled."""
    if isinstance(value, (list, tuple)):
        value = ", ".join(value)
    return "'" + str(value).replace("'", "''") + "'"


class PondraDDLCompiler(PGDDLCompiler):
    def post_create_table(self, table):
        """Pondra's options after the column list: `WITH (partition_by = '…', cluster_by = '…', order_by = '…')`.
        PostgreSQL's own options (PARTITION BY and the rest) come from PostgreSQL's compiler, as they are."""
        text = super().post_create_table(table)
        opts = table.dialect_options["pondra"]
        given = [f"{key} = {_quoted(opts[key])}" for key in ("partition_by", "cluster_by", "order_by") if opts[key]]
        return text + (f"\n WITH ({', '.join(given)})" if given else "")


# SHOW CREATE TABLE writes Pondra's options as PARTITION BY, CLUSTER BY (in parentheses) and SEQUENCE BY.
_SHOWN = {"pondra_partition_by": re.compile(r"^PARTITION BY (.+?);?$", re.M),
          "pondra_cluster_by": re.compile(r"^CLUSTER BY \((.*)\);?$", re.M),
          "pondra_order_by": re.compile(r"^SEQUENCE BY (.+?);?$", re.M)}


def _options_of(connection, preparer, table_name, schema):
    """Pondra's table options of a table, as the WITH clause spells them (`day(ts)`, `region, ts`, `ts`)."""
    name = preparer.quote(table_name)
    if schema:
        name = f"{preparer.quote_schema(schema)}.{name}"
    text = connection.exec_driver_sql(f"SHOW CREATE TABLE {name}").scalar()
    out = {}
    for key, pattern in _SHOWN.items():
        found = pattern.search(text)
        if found:
            out[key] = found.group(1)
    return out


class _PondraUUID(PGUuid):
    """A uuid is text in the lake (in one spelling), so over HTTP its values come back as text. With as_uuid
    they come back as UUIDs here, as the driver gives them on PostgreSQL (the port types the column as uuid,
    so psycopg hands over UUIDs already: those are kept as they are)."""

    def result_processor(self, dialect, coltype):
        if self.as_uuid:  # (psycopg makes a UUID itself: the port types the column as uuid)
            return lambda value: value if value is None or isinstance(value, uuid.UUID) else uuid.UUID(value)
        return None


def _identity_as_dict(row):
    """A column's row as a dict, its identity's options read from their JSON text when they come as text."""
    row = dict(row)
    options = row.get("identity_options")
    if isinstance(options, str):
        row["identity_options"] = json.loads(options)
    return row


class _SpacedNumbers:
    """An int2vector or oidvector (an index's columns, `indkey`; its options, `indoption`) as Postgres prints
    one and the Postgres port's catalog answers it: its numbers, space-separated (`1 2`). SQLAlchemy reads
    psycopg's and pg8000's the same way; a vector inside an expression comes as a list and stays one."""

    def result_processor(self, dialect, coltype):
        return lambda value: value if value is None or isinstance(value, list) else [int(p) for p in value.split()]


class _Int2Vector(_SpacedNumbers, INT2VECTOR):
    pass


class _OidVector(_SpacedNumbers, OIDVECTOR):
    pass


class _Parts:
    """Pondra's parts, shared by all four dialects: the table options, the isolation levels, the DDL."""

    name = "pondra"
    ddl_compiler = PondraDDLCompiler
    # SQLAlchemy keys a dialect option by the part after the dialect's prefix: `pondra_partition_by` is
    # `partition_by` here, read back as `pondra_partition_by` by get_table_options.
    construct_arguments = [*PGDialect.construct_arguments,
                           (Table, {"partition_by": None, "cluster_by": None, "order_by": None})]

    def __init__(self, isolation_level=None, **kw):
        # Checked when the engine is made, so SERIALIZABLE never reaches a node.
        if isolation_level is not None:
            _check_level(isolation_level)
        super().__init__(isolation_level=isolation_level, **kw)

    def _assert_and_set_isolation_level(self, dbapi_conn, level):
        _check_level(level)
        super()._assert_and_set_isolation_level(dbapi_conn, level)

    @reflection.cache
    def get_table_options(self, connection, table_name, schema=None, **kw):
        """Pondra's table options, read back from SHOW CREATE TABLE (what WITH (…) wrote)."""
        return _options_of(connection, self.identifier_preparer, table_name, schema)

    def _get_columns_info(self, rows, domains, enums, schema):
        """An identity column's options come as JSON text over HTTP and in process, where no driver decodes it.
        The port types them json, so psycopg hands over a dict already; SQLAlchemy and Alembic read a dict."""
        return super()._get_columns_info([_identity_as_dict(row) for row in rows], domains, enums, schema)


class PondraDialect(_Parts, PGDialect):
    """The dialect over `pondra.dbapi`: `pondra://`, `pondra:///folder`, and (with `PondraHttpDialect`) HTTP."""

    driver = "pondra"
    supports_statement_cache = True
    default_paramstyle = "numeric_dollar"
    colspecs = {**PGDialect.colspecs, sqltypes.Uuid: _PondraUUID, INT2VECTOR: _Int2Vector, OIDVECTOR: _OidVector}

    @classmethod
    def import_dbapi(cls):
        from . import dbapi
        return dbapi

    def create_connect_args(self, url):
        """`pondra://` and `pondra:///:memory:` (a lake in this process's memory), `pondra:////abs/lake` and
        `pondra:///rel/lake` (a folder), and for the HTTP drivers `pondra+http://user:password@host:port`
        (`?token=…`; a folder of lakes' database after the port, as `/db/{name}`). The `connect()` target
        and options, as keyword arguments; other query parameters are options too."""
        options = dict(url.query)
        kwargs = {"token": options.pop("token", None), **options}
        if self.driver in ("http", "https"):
            kwargs["target"] = f"{self.driver}://{url.host}" + (f":{url.port}" if url.port else "") + (f"/db/{url.database}" if url.database else "")
            if url.username is not None:
                kwargs["user"], kwargs["password"] = url.username, url.password
        else:
            kwargs["target"] = url.database or None
        return [], kwargs

    def _get_server_version_info(self, connection):
        # Not asked: SHOW server_version and version() are refused over HTTP, where the lake is reached
        # through the node's own API (no catalog answers them). The port answers 16.0 for both, so this
        # is the same answer.
        return SERVER_VERSION

    def _set_backslash_escapes(self, connection):
        # Not asked: SHOW standard_conforming_strings is refused over HTTP. Pondra's strings are standard
        # (quotes doubled, backslashes plain), as the port says ("on").
        self._backslash_escapes = False

    def get_isolation_level_values(self, dbapi_conn):
        return (*LEVELS, "AUTOCOMMIT")

    def get_isolation_level(self, dbapi_connection):
        # Not asked: SHOW TRANSACTION ISOLATION LEVEL is refused over HTTP. The driver knows what it began.
        return "AUTOCOMMIT" if dbapi_connection.autocommit else dbapi_connection.isolation_level

    def set_isolation_level(self, dbapi_connection, level):
        if level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
        else:
            dbapi_connection.autocommit = False
            dbapi_connection.isolation_level = level  # (the next BEGIN starts this level)


class PondraHttpDialect(PondraDialect):
    driver = "http"
    supports_statement_cache = True


class PondraHttpsDialect(PondraDialect):
    driver = "https"
    supports_statement_cache = True


class PondraDialect_psycopg(_Parts, PGDialect_psycopg):
    """The Postgres port through psycopg: PostgreSQL's dialect, with Pondra's parts. The port answers the
    catalog and SHOW, so nothing is overridden here but the parts above."""

    driver = "psycopg"
    supports_statement_cache = True
    colspecs = {**PGDialect_psycopg.colspecs, sqltypes.Uuid: _PondraUUID}


# Each dialect by the name a URL gives it: `pondra://` is the DB-API's, `pondra+<driver>://` each driver's.
_DIALECTS = {"pondra": PondraDialect, **{f"pondra.{d.driver}": d for d in (PondraDialect, PondraHttpDialect, PondraHttpsDialect, PondraDialect_psycopg)}}


def register():
    """Make the dialects' names known to SQLAlchemy, as the wheel's entry points do."""
    for name, dialect in _DIALECTS.items():
        registry.register(name, __name__, dialect.__name__)


# Alembic finds a dialect's migration code by its name, so where Alembic is installed Pondra's is
# Postgres's with two changes. DDL in a transaction runs at once (ROLLBACK doesn't undo it: ADR-067),
# so Alembic isn't told it can roll a failed migration's DDL back. And its version table is kept
# without a key: Alembic moves its one row from version to version with an UPDATE of that key, which
# a keyed table refuses (delete the row, then insert it), while an UPDATE of a plain table's row works.
try:
    from alembic.ddl.postgresql import PostgresqlImpl
except ImportError:
    PostgresqlImpl = None

if PostgresqlImpl is not None:
    class PondraImpl(PostgresqlImpl):
        __dialect__ = "pondra"
        transactional_ddl = False

        def version_table_impl(self, *, version_table, version_table_schema, version_table_pk, **kw):
            return super().version_table_impl(version_table=version_table, version_table_schema=version_table_schema,
                                              version_table_pk=False, **kw)
