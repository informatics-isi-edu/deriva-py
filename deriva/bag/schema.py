"""SQLAlchemy ORM construction from a Deriva catalog model.

This module is **Phase 1** of the two-phase database construction
pattern shared between :class:`~deriva.bag.database.BagDatabase`,
:class:`~deriva.bag.builder.BagBuilder`, and deriva-ml's ``local_db``
subsystem:

1. **Phase 1** (:class:`SchemaBuilder`): from an ERMrest
   :class:`~deriva.core.ermrest_model.Model` (live catalog *or*
   ``schema.json`` file), create a SQLAlchemy ORM — engine, metadata,
   automap base, table classes, cross-schema relationships — with no
   data loaded.
2. **Phase 2** (:class:`~deriva.bag.loader.DataLoader`): fill the
   tables from a :class:`~deriva.bag.sources.DataSource`. The source
   can be a bag's CSVs, a live catalog query, a pandas DataFrame, an
   in-memory iterable of dicts, or the local execution SQLite.

The two-phase split lets the same ORM be filled from different
sources. ``BagDatabase`` calls both phases internally;
``local_db`` builds the ORM Phase-1-only and writes to it via
domain-specific code; the new ``BagBuilder`` (Phase 1 + Phase 2 with
a write-out sink) uses the same machinery in reverse.

The result of Phase 1 is a :class:`SchemaORM` value object that
bundles the engine, metadata, automap base, and a few lookup
methods. Callers hold the :class:`SchemaORM` for the duration of
their work and call :meth:`SchemaORM.dispose` when done (or use it
as a context manager).

Lifted from ``deriva_ml.model.schema_builder``. The lift makes two
changes:

- Engine creation routes through
  :func:`deriva.bag.sqlite_helpers.create_wal_engine` so the WAL +
  pragma policy is uniform across deriva.bag and local_db.
- The CSV-to-Python type decorators (``ERMRestBoolean``,
  ``StringToFloat``, ``StringToInteger``, ``StringToDateTime``,
  ``StringToDate``) are imported from
  :mod:`deriva.bag.database` rather than re-defined; they're the
  same decorators in both places, and keeping one copy avoids the
  surprise where two ``ERMRestBoolean`` classes (from different
  modules) compare unequal.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Generator, Type

from deriva.core.ermrest_model import Column as DerivaColumn
from deriva.core.ermrest_model import Model
from deriva.core.ermrest_model import Table as DerivaTable
from deriva.core.ermrest_model import Type as DerivaType
from sqlalchemy import (
    JSON,
    MetaData,
    String,
    create_engine,
    event,
    inspect,
    select,
)
from sqlalchemy import Column as SQLColumn
from sqlalchemy import ForeignKeyConstraint as SQLForeignKeyConstraint
from sqlalchemy import Table as SQLTable
from sqlalchemy import UniqueConstraint as SQLUniqueConstraint
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import AutomapBase, automap_base
from sqlalchemy.orm import backref, foreign, relationship
from sqlalchemy.sql.type_api import TypeEngine

# Re-export the type decorators from deriva.bag.database so callers
# that import them from here see the same classes BagDatabase uses.
# (Defining a second copy would risk subtle isinstance() mismatches.)
from deriva.bag.database import (  # noqa: F401  (re-export)
    ERMRestBoolean,
    StringToDate,
    StringToDateTime,
    StringToFloat,
    StringToInteger,
)
from deriva.bag.sqlite_helpers import create_wal_engine

logger = logging.getLogger(__name__)


# =============================================================================
# SchemaORM — value object holding Phase-1 output
# =============================================================================


class SchemaORM:
    """Container for the SQLAlchemy ORM built by :class:`SchemaBuilder`.

    A ``SchemaORM`` bundles the engine, metadata, automap base, and a
    handful of lookup methods. It is the **result** of Phase 1: no
    data is loaded yet, but every table exists in SQLite with the
    right columns/keys/FKs, and every table has a generated ORM
    class.

    Attributes:
        engine: SQLAlchemy :class:`Engine` for database connections.
        metadata: SQLAlchemy :class:`MetaData` describing every
            table SchemaBuilder created.
        Base: SQLAlchemy :class:`AutomapBase` carrying the generated
            ORM classes.
        model: The source ERMrest :class:`Model` the ORM was built
            from. Held for introspection (the loader walks it to
            decide FK order, for example).
        schemas: List of schema names that were included.
        use_schemas: ``True`` for file-based databases (which use
            attached per-schema files and dotted table names),
            ``False`` for in-memory (which fold the schema name into
            the table name with an underscore separator).

    Example:
        >>> from sqlalchemy import text
        >>> # Build a tiny model, get the ORM, query a table.
        >>> # See SchemaBuilder.build for a full example.
        >>> # (Doctest skipped — requires a Model.)
    """

    def __init__(
        self,
        engine: Engine,
        metadata: MetaData,
        Base: AutomapBase,
        model: Model,
        schemas: list[str],
        class_prefix: str,
        use_schemas: bool = True,
    ):
        """Initialize the value object.

        Args:
            engine: SQLAlchemy :class:`Engine`.
            metadata: SQLAlchemy :class:`MetaData` with all tables.
            Base: Automap base carrying ORM classes.
            model: Source ERMrest :class:`Model`.
            schemas: Schemas that were included in the ORM.
            class_prefix: Prefix used for ORM class names (a per-instance
                random-ish string so multiple ``SchemaORM`` instances
                don't clash in the SQLAlchemy registry).
            use_schemas: ``True`` for file-based databases, ``False``
                for in-memory.
        """
        self.engine = engine
        self.metadata = metadata
        self.Base = Base
        self.model = model
        self.schemas = schemas
        self._class_prefix = class_prefix
        self._use_schemas = use_schemas
        self._disposed = False

    def list_tables(self) -> list[str]:
        """List all tables in the database.

        Returns:
            Sorted list of fully-qualified table names (the keys of
            ``self.metadata.tables`` — typically ``"schema.table"``
            for file-based databases, ``"schema_table"`` for
            in-memory).
        """
        tables = list(self.metadata.tables.keys())
        tables.sort()
        return tables

    def find_table(self, table_name: str) -> SQLTable:
        """Find a table by name with flexible matching.

        Tries exact match, then ``schema.table`` ↔ ``schema_table``
        conversion (for in-memory databases), then a suffix match on
        the table-name portion.

        Args:
            table_name: Table name in any supported form:
                ``"schema.table"``, ``"schema_table"``, or just
                ``"table"``.

        Returns:
            SQLAlchemy :class:`Table` object.

        Raises:
            KeyError: If no table matches.
        """
        # Try exact match first
        if table_name in self.metadata.tables:
            return self.metadata.tables[table_name]

        # Try converting schema.table to schema_table format (for in-memory)
        if "." in table_name and not self._use_schemas:
            converted_name = table_name.replace(".", "_").replace("-", "_")
            if converted_name in self.metadata.tables:
                return self.metadata.tables[converted_name]

        # Try matching just the table name part
        for full_name, table in self.metadata.tables.items():
            # Handle . separator (file-based)
            if "." in full_name and full_name.split(".")[-1] == table_name:
                return table
            # Handle _ separator (in-memory) - match suffix after first _
            if "_" in full_name and "." not in full_name:
                parts = full_name.split("_", 1)
                if len(parts) > 1 and parts[1] == table_name:
                    return table
                # Also check if it ends with the table name
                if full_name.endswith(f"_{table_name}"):
                    return table

        raise KeyError(f"Table {table_name} not found")

    def get_orm_class(self, table_name: str) -> Any | None:
        """Get the ORM class for a table by name.

        Args:
            table_name: Table name (any form
                :meth:`find_table` accepts).

        Returns:
            SQLAlchemy ORM class for the table, or ``None`` if no
            mapper exists.

        Raises:
            KeyError: If the table itself is not found.
        """
        sql_table = self.find_table(table_name)
        return self.get_orm_class_for_table(sql_table)

    def get_orm_class_for_table(
        self, table: SQLTable | DerivaTable | str
    ) -> Any | None:
        """Get the ORM class for a table object.

        Accepts a SQLAlchemy :class:`Table`, a deriva-py :class:`Table`,
        or a string name.

        Args:
            table: The table to look up.

        Returns:
            SQLAlchemy ORM class, or ``None`` if no mapper is
            registered for it.
        """
        if isinstance(table, DerivaTable):
            # Try schema.table format first (file-based), then
            # schema_table (in-memory).
            table_key = f"{table.schema.name}.{table.name}"
            table = self.metadata.tables.get(table_key)
            if table is None and not self._use_schemas:
                table_key = (
                    f"{table.schema.name}_{table.name}".replace("-", "_")
                )
                table = self.metadata.tables.get(table_key)
        if isinstance(table, str):
            table = self.find_table(table)
        if table is None:
            return None

        for mapper in self.Base.registry.mappers:
            if mapper.persist_selectable is table or table in mapper.tables:
                return mapper.class_
        return None

    def get_table_contents(
        self, table: str
    ) -> Generator[dict[str, Any], None, None]:
        """Yield all rows from a table as plain dictionaries.

        Args:
            table: Table name (any form :meth:`find_table` accepts).

        Yields:
            One ``dict[str, Any]`` per row, keyed by column name.

        Example:
            ::

                for row in orm.get_table_contents("Subject"):
                    print(row["Name"])
        """
        sql_table = self.find_table(table)
        with self.engine.connect() as conn:
            result = conn.execute(select(sql_table))
            for row in result.mappings():
                yield dict(row)

    @staticmethod
    def is_association_table(
        table_class,
        min_arity: int = 2,
        max_arity: int = 2,
        unqualified: bool = True,
        pure: bool = True,
        no_overlap: bool = True,
        return_fkeys: bool = False,
    ):
        """Check whether an ORM class represents an association table.

        An association table is a table whose entire content is a
        composite unique key over two or more foreign-key columns,
        each pointing at a different parent table. It carries no
        domain content of its own; its purpose is to record the
        many-to-many relationship between its endpoints.

        The detection is configurable: ``min_arity``/``max_arity``
        control how many FKs the candidate must have;
        ``unqualified``/``pure``/``no_overlap`` toggle stricter
        structural requirements.

        Args:
            table_class: SQLAlchemy ORM class to check.
            min_arity: Minimum number of FKs covered by the
                association's unique key (default 2).
            max_arity: Maximum number of FKs (default 2). ``None``
                for unbounded.
            unqualified: If ``True``, the unique key must contain
                *only* FK columns (no extra qualifier columns).
            pure: If ``True``, the table must have no non-key
                non-system columns (the table is *only* the
                association).
            no_overlap: If ``True``, FK column sets must be disjoint
                (no column participates in two FKs).
            return_fkeys: If ``True``, return the matching FK
                relationships; otherwise return the arity.

        Returns:
            If ``return_fkeys=False``: integer arity if the candidate
            is an association, ``False`` otherwise.
            If ``return_fkeys=True``: set of FK relationships, or
            ``False``.

        Raises:
            ValueError: If ``min_arity < 2`` or
                ``max_arity < min_arity``.
        """
        if min_arity < 2:
            raise ValueError("An association cannot have arity < 2")
        if max_arity is not None and max_arity < min_arity:
            raise ValueError("max_arity cannot be less than min_arity")

        mapper = inspect(table_class).mapper
        system_cols = {"RID", "RCT", "RMT", "RCB", "RMB"}

        non_sys_cols = {
            col.name for col in mapper.columns if col.name not in system_cols
        }

        unique_columns = [
            {c.name for c in constraint.columns}
            for constraint in inspect(table_class).local_table.constraints
            if isinstance(constraint, SQLUniqueConstraint)
        ]

        non_sys_key_colsets = {
            frozenset(uc)
            for uc in unique_columns
            if uc.issubset(non_sys_cols) and len(uc) > 1
        }

        if not non_sys_key_colsets:
            return False

        # Choose the longest compound key — when multiple unique
        # keys exist, the largest is the most likely "this is the
        # association" candidate.
        row_key = sorted(non_sys_key_colsets, key=lambda s: len(s), reverse=True)[0]
        foreign_keys = list(inspect(table_class).relationships.values())

        covered_fkeys = {
            fkey
            for fkey in foreign_keys
            if {c.name for c in fkey.local_columns}.issubset(row_key)
        }
        covered_fkey_cols: set[str] = set()

        if len(covered_fkeys) < min_arity:
            return False
        if max_arity is not None and len(covered_fkeys) > max_arity:
            return False

        for fkey in covered_fkeys:
            fkcols = {c.name for c in fkey.local_columns}
            if no_overlap and fkcols.intersection(covered_fkey_cols):
                return False
            covered_fkey_cols.update(fkcols)

        if unqualified and row_key.difference(covered_fkey_cols):
            return False

        if pure and non_sys_cols.difference(row_key):
            return False

        return covered_fkeys if return_fkeys else len(covered_fkeys)

    def get_association_class(
        self,
        left_cls: Type[Any],
        right_cls: Type[Any],
    ) -> tuple[Any, Any, Any] | None:
        """Find an association class connecting two ORM classes.

        Walks ``left_cls``'s relationships looking for any that
        terminate at an association table whose two endpoints are
        ``left_cls`` and ``right_cls``.

        Args:
            left_cls: One end of the relationship.
            right_cls: The other end.

        Returns:
            ``(association_class, left_relationship_attr,
            right_relationship_attr)`` if an association is found;
            ``None`` otherwise.
        """
        for _, left_rel in inspect(left_cls).relationships.items():
            mid_cls = left_rel.mapper.class_
            is_assoc = self.is_association_table(mid_cls, return_fkeys=True)

            if not is_assoc:
                continue

            assoc_local_columns_left = list(is_assoc)[0].local_columns
            assoc_local_columns_right = list(is_assoc)[1].local_columns

            found_left = found_right = False

            for r in inspect(left_cls).relationships.values():
                remote_side = list(r.remote_side)[0]
                if remote_side in assoc_local_columns_left:
                    found_left = r
                if remote_side in assoc_local_columns_right:
                    found_left = r
                    # Swap if backwards
                    assoc_local_columns_left, assoc_local_columns_right = (
                        assoc_local_columns_right,
                        assoc_local_columns_left,
                    )

            for r in inspect(right_cls).relationships.values():
                remote_side = list(r.remote_side)[0]
                if remote_side in assoc_local_columns_right:
                    found_right = r

            if found_left and found_right:
                return (
                    mid_cls,
                    found_left.class_attribute,
                    found_right.class_attribute,
                )

        return None

    def dispose(self) -> None:
        """Release the SQLAlchemy registry and engine.

        Idempotent. After ``dispose()``, the instance should not be
        used further; methods that touch the engine or registry will
        raise.
        """
        if self._disposed:
            return

        if hasattr(self, "Base") and self.Base is not None:
            self.Base.registry.dispose()
        if hasattr(self, "engine") and self.engine is not None:
            self.engine.dispose()

        self._disposed = True

    def __del__(self) -> None:
        """Best-effort cleanup at garbage-collection time.

        ``__del__`` runs at unpredictable points, including
        interpreter shutdown when SQLAlchemy module-level globals
        (registries, engines) may already be partially torn down. In
        that race we'd see ``AttributeError: 'NoneType' object has no
        attribute '_dispose_registries'`` printed via
        ``Exception ignored in:`` — benign but noisy. Swallow
        everything here; the explicit ``dispose()`` callable from
        ``__exit__`` and from callers still raises normally.
        """
        try:
            self.dispose()
        except Exception:
            pass

    def __enter__(self) -> "SchemaORM":
        """Context manager entry — returns self."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit — calls :meth:`dispose`."""
        self.dispose()
        return False


# =============================================================================
# SchemaBuilder — Phase 1 of the two-phase pattern
# =============================================================================


class SchemaBuilder:
    """Build a SQLAlchemy ORM from an ERMrest :class:`Model`.

    The builder is **Phase 1** of the two-phase pattern: it creates
    SQLite tables matching every ERMrest table in the included
    schemas, adds keys + FKs, sets up ORM classes via SQLAlchemy
    automap with predictable class names, and wires cross-schema
    relationships that the automap pass alone wouldn't connect. The
    output is a :class:`SchemaORM` value object the caller can fill
    via :class:`~deriva.bag.loader.DataLoader` (Phase 2) or use as
    an ORM target for direct writes.

    Two database-path modes:

    - **File-based** (the default for production use): each ERMrest
      schema goes into its own ``{schema}.db`` file inside the
      parent directory of ``database_path``. The main file (whose
      path is ``database_path`` itself, or ``{database_path}/main.db``
      when ``database_path`` is a directory) holds no tables; the
      schemas are surfaced via ``ATTACH``. Table names appear in
      SQLAlchemy as ``schema.table``.

    - **In-memory** (``database_path=":memory:"``, the default for
      tests): everything goes in one SQLite memory database with no
      ATTACH (SQLite can't ATTACH the same in-memory database
      twice). Schema names are folded into the table name with an
      underscore separator (e.g. ``deriva_ml_Dataset``) and dashes
      are normalized to underscores.

    Example (file-based, real-world shape)::

        from deriva.core import ErmrestCatalog
        from deriva.bag.schema import SchemaBuilder

        catalog = ErmrestCatalog(...)
        model = catalog.getCatalogModel()
        builder = SchemaBuilder(
            model,
            schemas=["deriva-ml", "domain"],
            database_path=Path("/tmp/my-cache"),
        )
        orm = builder.build()
        try:
            for row in orm.get_table_contents("Subject"):
                print(row["Name"])
        finally:
            orm.dispose()
    """

    #: Map from ERMrest type names to SQLAlchemy column types. The
    #: integer/float/timestamp types route through ``StringTo*``
    #: decorators so CSV string values can be loaded directly without
    #: a separate conversion step.
    _TYPE_MAP = {
        "boolean": ERMRestBoolean,
        "date": StringToDate,
        "float4": StringToFloat,
        "float8": StringToFloat,
        "int2": StringToInteger,
        "int4": StringToInteger,
        "int8": StringToInteger,
        "json": JSON,
        "jsonb": JSON,
        "timestamptz": StringToDateTime,
        "timestamp": StringToDateTime,
    }

    def __init__(
        self,
        model: Model,
        schemas: list[str],
        database_path: Path | str = ":memory:",
    ):
        """Initialize the builder.

        Args:
            model: ERMrest :class:`Model` (from a live catalog via
                ``catalog.getCatalogModel()`` or from a file via
                ``Model.fromfile("file-system", path)``).
            schemas: Names of the ERMrest schemas to include. Tables
                in schemas not listed are silently ignored.
            database_path: Where to put the SQLite file(s).
                ``":memory:"`` for an in-memory database (default).
                A path ending in ``.db`` becomes the main file; a
                directory path puts ``main.db`` and per-schema
                ``{schema}.db`` files inside it.
        """
        self.model = model
        self.schemas = schemas
        self.database_path = (
            Path(database_path) if database_path != ":memory:" else database_path
        )

        # Filled in during build()
        self.engine: Engine | None = None
        self.metadata: MetaData | None = None
        self.Base: AutomapBase | None = None
        self._class_prefix: str = ""

    @staticmethod
    def _sql_type(deriva_type: DerivaType) -> TypeEngine:
        """Map an ERMrest column type to its SQLAlchemy counterpart.

        Args:
            deriva_type: ERMrest type object.

        Returns:
            SQLAlchemy column type class. Unknown types fall back to
            :class:`sqlalchemy.String`, which works for any value that
            survives the CSV round-trip as a string.
        """
        return SchemaBuilder._TYPE_MAP.get(deriva_type.typename, String)

    def _is_key_column(
        self, column: DerivaColumn, table: DerivaTable
    ) -> bool:
        """Return True if ``column`` is the table's RID primary key.

        ERMrest catalogs have many declared keys, but only RID is the
        canonical primary key. We tag *only* RID as ``primary_key=True``
        on the SQLAlchemy column; other keys become non-PK unique
        constraints.
        """
        return (
            column in [key.unique_columns[0] for key in table.keys]
            and column.name == "RID"
        )

    def build(self) -> SchemaORM:
        """Build the SQLAlchemy ORM structure.

        Creates SQLite tables, applies keys + foreign keys, runs
        automap to generate ORM classes, then walks the model once
        more to wire up cross-schema relationships that automap
        alone wouldn't connect (automap operates on one MetaData; FKs
        crossing into another attached database don't form
        relationships automatically).

        Returns:
            A :class:`SchemaORM` value object. The caller is
            responsible for calling :meth:`SchemaORM.dispose` (or
            using it as a context manager).

        Note:
            In-memory databases collapse schema prefixes into the
            table name with an underscore separator (e.g.
            ``deriva_ml_Dataset``), because SQLite can't ATTACH the
            same in-memory database under multiple aliases.
        """
        # Per-instance prefix prevents two SchemaORM instances from
        # accidentally sharing ORM class names in the SQLAlchemy
        # registry.
        self._class_prefix = f"_{id(self)}_"

        # In-memory mode skips schema attachment.
        self._use_schemas = self.database_path != ":memory:"

        if self.database_path == ":memory:":
            # In-memory engines can't use create_wal_engine
            # (WAL on :memory: is a no-op anyway, and we'd lose
            # the convenient bare-string DSN). Use create_engine
            # directly. The other pragmas (foreign_keys, busy_timeout)
            # aren't needed for in-memory operation.
            self.engine = create_engine("sqlite:///:memory:", future=True)
        else:
            # File-based: route through the project's WAL factory so
            # the per-bag and per-local-db SQLite files share one
            # engine policy.
            if isinstance(self.database_path, Path):
                if self.database_path.suffix == ".db":
                    main_db = self.database_path
                else:
                    main_db = self.database_path / "main.db"
            else:
                main_db = Path(self.database_path)

            self.engine = create_wal_engine(main_db)

            # Attach schema-specific databases at connect time.
            event.listen(self.engine, "connect", self._attach_schemas)

        self.metadata = MetaData()
        self.Base = automap_base(metadata=self.metadata)

        self._create_tables()

        logger.info(
            "Built ORM for schemas %s with %d tables",
            self.schemas,
            len(self.metadata.tables),
        )

        return SchemaORM(
            engine=self.engine,
            metadata=self.metadata,
            Base=self.Base,
            model=self.model,
            schemas=self.schemas,
            class_prefix=self._class_prefix,
            use_schemas=self._use_schemas,
        )

    def _attach_schemas(self, dbapi_conn, _conn_record):
        """Attach one ``{schema}.db`` file per ERMrest schema."""
        cur = dbapi_conn.cursor()
        db_dir = (
            self.database_path
            if self.database_path.is_dir()
            else self.database_path.parent
        )
        for schema in self.schemas:
            schema_file = (db_dir / f"{schema}.db").resolve()
            # ATTACH doesn't support parameter binding; escape the
            # alias in case it contains a hyphen (``deriva-ml``).
            alias_safe = schema.replace('"', '""')
            cur.execute(
                f"ATTACH DATABASE '{schema_file}' AS \"{alias_safe}\""
            )
        cur.close()

    def _create_tables(self) -> None:
        """Create SQLite tables and configure the automap base."""

        def col(model, name: str):
            """Look up a column on an ORM class.

            Some columns are exposed as attribute names that match the
            column name; some are reachable only via the underlying
            ``__table__.c`` mapping. Try the attribute first, fall
            back to the table.
            """
            try:
                return getattr(model, name).property.columns[0]
            except AttributeError:
                return model.__table__.c[name]

        def guess_attr_name(col_name: str) -> str:
            """Pick a relationship attribute name from a FK column name."""
            return col_name[:-3] if col_name.lower().endswith("_id") else col_name

        def make_table_name(schema_name: str, table_name: str) -> str:
            """Compute the SQLAlchemy table name for a (schema, table)."""
            if self._use_schemas:
                return f"{schema_name}.{table_name}"
            # In-memory: fold schema into the name.
            return f"{schema_name}_{table_name}"

        database_tables: list[SQLTable] = []

        for schema_name in self.schemas:
            if schema_name not in self.model.schemas:
                logger.warning(f"Schema {schema_name} not found in model")
                continue

            for table in self.model.schemas[schema_name].tables.values():
                database_columns: list[SQLColumn] = []

                for c in table.columns:
                    database_column = SQLColumn(
                        name=c.name,
                        type_=self._sql_type(c.type),
                        comment=c.comment,
                        default=c.default,
                        primary_key=self._is_key_column(c, table),
                        nullable=c.nullok,
                    )
                    database_columns.append(database_column)

                if self._use_schemas:
                    database_table = SQLTable(
                        table.name,
                        self.metadata,
                        *database_columns,
                        schema=schema_name,
                    )
                else:
                    full_name = f"{schema_name}_{table.name}".replace("-", "_")
                    database_table = SQLTable(
                        full_name, self.metadata, *database_columns
                    )

                # Non-RID unique constraints.
                for key in table.keys:
                    key_columns = [c.name for c in key.unique_columns]
                    database_table.append_constraint(
                        SQLUniqueConstraint(*key_columns, name=key.name[1])
                    )

                # FK constraints, but only same-schema ones. Cross-schema
                # FKs go through the relationship pass below — SQLite
                # can declare them across attached databases but
                # SQLAlchemy's referential resolution gets confused, so
                # we model the cross-schema link only as a Python
                # relationship.
                for fk in table.foreign_keys:
                    if fk.pk_table.schema.name not in self.schemas:
                        continue
                    if fk.pk_table.schema.name != schema_name:
                        continue

                    if self._use_schemas:
                        refcols = [
                            f"{schema_name}.{c.table.name}.{c.name}"
                            for c in fk.referenced_columns
                        ]
                    else:
                        ref_table_name = (
                            f"{schema_name}_{fk.pk_table.name}".replace("-", "_")
                        )
                        refcols = [
                            f"{ref_table_name}.{c.name}"
                            for c in fk.referenced_columns
                        ]

                    database_table.append_constraint(
                        SQLForeignKeyConstraint(
                            columns=[f"{c.name}" for c in fk.foreign_key_columns],
                            refcolumns=refcols,
                            name=fk.name[1],
                            comment=fk.comment,
                        )
                    )

                database_tables.append(database_table)

        # Create all tables.
        with self.engine.begin() as conn:
            self.metadata.create_all(
                conn, tables=database_tables, checkfirst=True
            )

        # Predictable ORM class naming.
        def name_for_scalar_relationship(
            _base, local_cls, referred_cls, constraint
        ):
            cols = list(constraint.columns) if constraint is not None else []
            if len(cols) == 1:
                name = cols[0].key
                # If the relationship name would collide with the
                # column name, suffix it with ``_rel``.
                if name in {c.key for c in local_cls.__table__.columns}:
                    name += "_rel"
                return name
            return constraint.name or referred_cls.__name__.lower()

        def name_for_collection_relationship(
            _base, local_cls, referred_cls, constraint
        ):
            backref_name = constraint.name.replace("_fkey", "_collection")
            return backref_name or (
                referred_cls.__name__.lower() + "_collection"
            )

        def classname_for_table(_base, tablename, table):
            return self._class_prefix + tablename.replace(".", "_").replace("-", "_")

        # Run automap.
        self.Base.prepare(
            self.engine,
            name_for_scalar_relationship=name_for_scalar_relationship,
            name_for_collection_relationship=name_for_collection_relationship,
            classname_for_table=classname_for_table,
            reflect=True,
        )

        # Cross-schema relationships. Automap only sees FKs within a
        # single MetaData scope; cross-schema FKs need to be wired
        # manually as Python relationships (viewonly so they don't
        # try to write through the FK back into SQLite, where the
        # constraint isn't declared).
        for schema_name in self.schemas:
            if schema_name not in self.model.schemas:
                continue

            for table in self.model.schemas[schema_name].tables.values():
                for fk in table.foreign_keys:
                    if fk.pk_table.schema.name not in self.schemas:
                        continue
                    if fk.pk_table.schema.name == schema_name:
                        continue

                    table_name = make_table_name(schema_name, table.name)
                    table_class = self._get_orm_class_by_name(table_name)
                    foreign_key_column_name = fk.foreign_key_columns[0].name
                    foreign_key_column = col(
                        table_class, foreign_key_column_name
                    )

                    referenced_table_name = make_table_name(
                        fk.pk_table.schema.name, fk.pk_table.name
                    )
                    referenced_class = self._get_orm_class_by_name(
                        referenced_table_name
                    )
                    referenced_column = col(
                        referenced_class, fk.referenced_columns[0].name
                    )

                    relationship_attr = guess_attr_name(foreign_key_column_name)
                    backref_attr = fk.name[1].replace("_fkey", "_collection")

                    # Local import: avoid SQLAlchemy import cycle at
                    # module top.
                    from sqlalchemy.orm import RelationshipProperty
                    from sqlalchemy.orm.attributes import InstrumentedAttribute

                    existing_attr = getattr(table_class, relationship_attr, None)
                    is_relationship = isinstance(
                        existing_attr, InstrumentedAttribute
                    ) and isinstance(existing_attr.property, RelationshipProperty)
                    if not is_relationship:
                        setattr(
                            table_class,
                            relationship_attr,
                            relationship(
                                referenced_class,
                                foreign_keys=[foreign_key_column],
                                primaryjoin=(
                                    foreign(foreign_key_column)
                                    == referenced_column
                                ),
                                backref=backref(backref_attr, viewonly=True),
                                viewonly=True,
                            ),
                        )

        self.Base.registry.configure()

    def _get_orm_class_by_name(self, table_name: str) -> Any | None:
        """Look up an ORM class by table name during build.

        Handles both ``schema.table`` (file-based) and ``schema_table``
        (in-memory) name forms.
        """
        if table_name in self.metadata.tables:
            sql_table = self.metadata.tables[table_name]
        else:
            # Try the schema_table form for in-memory.
            if "." in table_name and not self._use_schemas:
                converted_name = table_name.replace(".", "_").replace("-", "_")
                if converted_name in self.metadata.tables:
                    sql_table = self.metadata.tables[converted_name]
                else:
                    sql_table = None
            else:
                sql_table = None
                for full_name, table in self.metadata.tables.items():
                    table_part = (
                        full_name.split(".")[-1]
                        if "." in full_name
                        else full_name.split("_", 1)[-1]
                        if "_" in full_name
                        else full_name
                    )
                    if (
                        table_part == table_name
                        or full_name.endswith(f"_{table_name}")
                    ):
                        sql_table = table
                        break

        if sql_table is None:
            raise KeyError(f"Table {table_name} not found")

        for mapper in self.Base.registry.mappers:
            if mapper.persist_selectable is sql_table or sql_table in mapper.tables:
                return mapper.class_
        return None
