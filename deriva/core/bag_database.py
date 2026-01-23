"""Schema-independent SQLite database creation from BDBags.

This module provides the BagDatabase class which converts a BDBag (Big Data Bag)
into a SQLite database. It handles:

- Reading the ERMrest schema.json and creating corresponding SQLite tables
- Loading CSV data files into the database
- Localizing asset file paths using fetch.txt
- Setting up SQLAlchemy ORM mappings

This is a generic implementation that works with any BDBag containing:
- data/schema.json - ERMrest catalog schema
- data/*.csv - Table data files
- fetch.txt - Remote file references (optional)

For DerivaML-specific functionality (datasets, versions, features), see
the DatabaseModel class in deriva-ml which extends this class.
"""

from __future__ import annotations

import json
import logging
from csv import reader
from pathlib import Path
from typing import Any, Generator, Type

from dateutil import parser
from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
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
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, backref, foreign, relationship
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import TypeDecorator
from urllib.parse import urlparse

from deriva.core.ermrest_model import Column as DerivaColumn
from deriva.core.ermrest_model import Model
from deriva.core.ermrest_model import Table as DerivaTable
from deriva.core.ermrest_model import Type as DerivaType


logger = logging.getLogger(__name__)


# Type converters for loading CSV string data into SQLite with proper types

class ERMRestBoolean(TypeDecorator):
    """Convert ERMrest boolean strings to Python bool."""
    impl = Boolean
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> bool | None:
        if value in ("Y", "y", 1, True, "t", "T"):
            return True
        elif value in ("N", "n", 0, False, "f", "F"):
            return False
        elif value is None:
            return None
        raise ValueError(f"Invalid boolean value: {value!r}")


class StringToFloat(TypeDecorator):
    """Convert string to float, handling empty strings."""
    impl = Float
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> float | None:
        if value == "" or value is None:
            return None
        return float(value)


class StringToInteger(TypeDecorator):
    """Convert string to integer, handling empty strings."""
    impl = Integer
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> int | None:
        if value == "" or value is None:
            return None
        return int(value)


class StringToDateTime(TypeDecorator):
    """Convert string to datetime, handling empty strings."""
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value == "" or value is None:
            return None
        return parser.parse(value)


class StringToDate(TypeDecorator):
    """Convert string to date, handling empty strings."""
    impl = Date
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value == "" or value is None:
            return None
        return parser.parse(value).date()


# Standard asset table columns
ASSET_COLUMNS = {"Filename", "URL", "Length", "MD5", "Description"}


class BagDatabase:
    """Schema-independent SQLite database created from a BDBag.

    This class reads a BDBag directory and creates a SQLite database containing
    all the table data from the bag's CSV files. It uses the bag's schema.json
    to create proper table structures with foreign key relationships.

    The database is created in the specified database directory, with separate
    SQLite files for each schema (to work around SQLite's lack of schema support).

    Attributes:
        bag_path: Path to the BDBag directory.
        model: ERMrest Model loaded from the bag's schema.json.
        engine: SQLAlchemy engine for the main database.
        metadata: SQLAlchemy MetaData with table definitions.
        Base: SQLAlchemy automap base for ORM classes.
        schemas: List of schema names found in the bag.
        snaptime: Catalog snapshot time from schema.json.

    Example:
        >>> db = BagDatabase(
        ...     bag_path=Path("/path/to/bag"),
        ...     database_dir=Path("/path/to/cache"),
        ...     schemas=["domain-schema", "deriva-ml"]
        ... )
        >>> for row in db.get_table_contents("Image"):
        ...     print(row["Filename"])
    """

    def __init__(
        self,
        bag_path: Path,
        database_dir: Path,
        schemas: list[str],
    ):
        """Create a SQLite database from a BDBag.

        Args:
            bag_path: Path to the BDBag directory containing data/ subdirectory.
            database_dir: Directory where SQLite database files will be created.
                The database uses the bag's parent directory name (which typically
                includes a checksum) to ensure uniqueness.
            schemas: List of schema names to load from the bag. Tables from other
                schemas are ignored.
        """
        self.bag_path = bag_path
        self.schemas = schemas

        # Load the ERMrest model from schema.json
        schema_file = bag_path / "data/schema.json"
        self.model = Model.fromfile("file-system", schema_file)

        with schema_file.open("r") as f:
            self.snaptime = json.load(f)["snaptime"]

        # Extract the bag checksum from the cache directory name (e.g., "3YM_abc123...")
        # This ensures the database is recreated when the bag content changes.
        bag_cache_dir = bag_path.parent.name
        self.database_dir = database_dir / bag_cache_dir
        self.database_dir.mkdir(parents=True, exist_ok=True)

        # Create SQLAlchemy engine and metadata
        self.engine = create_engine(
            f"sqlite:///{(self.database_dir / 'main.db').resolve()}",
            future=True
        )
        self.metadata = MetaData()
        self.Base = automap_base(metadata=self.metadata)

        # Generate a unique prefix for ORM class names to prevent sharing between instances
        self._class_prefix = f"_{id(self)}_"

        # Attach event listener for schema attachment
        event.listen(self.engine, "connect", self._attach_schemas)

        # Build the database
        self._create_tables()
        self._load_data()

        logger.info(
            "Created database for bag %s in %s",
            bag_path.name,
            self.database_dir,
        )

    def _attach_schemas(self, dbapi_conn, _conn_record):
        """Attach schema-specific SQLite databases."""
        cur = dbapi_conn.cursor()
        for schema in self.schemas:
            schema_file = (self.database_dir / f"{schema}.db").resolve()
            cur.execute(f"ATTACH DATABASE '{schema_file}' AS '{schema}'")
        cur.close()

    @staticmethod
    def _sql_type(deriva_type: DerivaType) -> TypeEngine:
        """Map ERMrest type to SQLAlchemy type with CSV string conversion."""
        return {
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
        }.get(deriva_type.typename, String)

    def _is_key_column(self, column: DerivaColumn, table: DerivaTable) -> bool:
        """Check if column is the primary key (RID)."""
        return column in [key.unique_columns[0] for key in table.keys] and column.name == "RID"

    def _create_tables(self) -> None:
        """Create SQLite tables from the ERMrest schema."""

        def col(model, name: str):
            """Get column from ORM class, handling both attribute and table column access."""
            try:
                return getattr(model, name).property.columns[0]
            except AttributeError:
                return model.__table__.c[name]

        def guess_attr_name(col_name: str) -> str:
            """Generate relationship attribute name from column name."""
            return col_name[:-3] if col_name.lower().endswith("_id") else col_name

        database_tables: list[SQLTable] = []

        for schema_name in self.schemas:
            if schema_name not in self.model.schemas:
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

                database_table = SQLTable(
                    table.name, self.metadata, *database_columns, schema=schema_name
                )

                # Add unique constraints
                for key in table.keys:
                    key_columns = [c.name for c in key.unique_columns]
                    database_table.append_constraint(
                        SQLUniqueConstraint(*key_columns, name=key.name[1])
                    )

                # Add foreign key constraints (within same schema only for now)
                for fk in table.foreign_keys:
                    if fk.pk_table.schema.name not in self.schemas:
                        continue
                    if fk.pk_table.schema.name != schema_name:
                        continue

                    database_table.append_constraint(
                        SQLForeignKeyConstraint(
                            columns=[f"{c.name}" for c in fk.foreign_key_columns],
                            refcolumns=[
                                f"{schema_name}.{c.table.name}.{c.name}"
                                for c in fk.referenced_columns
                            ],
                            name=fk.name[1],
                            comment=fk.comment,
                        )
                    )

                database_tables.append(database_table)

        # Create all tables
        with self.engine.begin() as conn:
            self.metadata.create_all(conn, tables=database_tables)

        # Configure ORM class naming
        def name_for_scalar_relationship(_base, local_cls, referred_cls, constraint):
            cols = list(constraint.columns) if constraint is not None else []
            if len(cols) == 1:
                name = cols[0].key
                if name in {c.key for c in local_cls.__table__.columns}:
                    name += "_rel"
                return name
            return constraint.name or referred_cls.__name__.lower()

        def name_for_collection_relationship(_base, local_cls, referred_cls, constraint):
            backref_name = constraint.name.replace("_fkey", "_collection")
            return backref_name or (referred_cls.__name__.lower() + "_collection")

        def classname_for_table(_base, tablename, table):
            return self._class_prefix + tablename.replace(".", "_").replace("-", "_")

        # Build ORM mappings
        self.Base.prepare(
            self.engine,
            name_for_scalar_relationship=name_for_scalar_relationship,
            name_for_collection_relationship=name_for_collection_relationship,
            classname_for_table=classname_for_table,
            reflect=True,
        )

        # Add cross-schema relationships
        for schema_name in self.schemas:
            if schema_name not in self.model.schemas:
                continue

            for table in self.model.schemas[schema_name].tables.values():
                for fk in table.foreign_keys:
                    if fk.pk_table.schema.name not in self.schemas:
                        continue
                    if fk.pk_table.schema.name == schema_name:
                        continue

                    table_name = f"{schema_name}.{table.name}"
                    table_class = self.get_orm_class_by_name(table_name)
                    foreign_key_column_name = fk.foreign_key_columns[0].name
                    foreign_key_column = col(table_class, foreign_key_column_name)

                    referenced_table_name = f"{fk.pk_table.schema.name}.{fk.pk_table.name}"
                    referenced_class = self.get_orm_class_by_name(referenced_table_name)
                    referenced_column = col(referenced_class, fk.referenced_columns[0].name)

                    relationship_attr = guess_attr_name(foreign_key_column_name)
                    backref_attr = fk.name[1].replace("_fkey", "_collection")

                    # Check if relationship already exists
                    existing_attr = getattr(table_class, relationship_attr, None)
                    from sqlalchemy.orm import RelationshipProperty
                    from sqlalchemy.orm.attributes import InstrumentedAttribute

                    is_relationship = isinstance(existing_attr, InstrumentedAttribute) and isinstance(
                        existing_attr.property, RelationshipProperty
                    )
                    if not is_relationship:
                        setattr(
                            table_class,
                            relationship_attr,
                            relationship(
                                referenced_class,
                                foreign_keys=[foreign_key_column],
                                primaryjoin=foreign(foreign_key_column) == referenced_column,
                                backref=backref(backref_attr, viewonly=True),
                                viewonly=True,
                            ),
                        )

        # Configure mappers for this instance only
        self.Base.registry.configure()

    def _build_asset_map(self) -> dict[str, str]:
        """Build a map from remote URLs to local file paths using fetch.txt.

        Returns:
            Dictionary mapping URL paths to local file paths.
        """
        fetch_map = {}
        fetch_file = self.bag_path / "fetch.txt"

        if not fetch_file.exists():
            logger.info(f"No fetch.txt in bag {self.bag_path.name}")
            return fetch_map

        try:
            with fetch_file.open(newline="\n") as f:
                for row in f:
                    # Rows in fetch.txt are tab-separated: URL, size, local_path
                    fields = row.split("\t")
                    if len(fields) >= 3:
                        local_file = fields[2].replace("\n", "")
                        local_path = f"{self.bag_path}/{local_file}"
                        fetch_map[urlparse(fields[0]).path] = local_path
        except Exception as e:
            logger.warning(f"Error reading fetch.txt: {e}")

        return fetch_map

    def _is_asset_table(self, table_name: str) -> bool:
        """Check if a table is an asset table (has Filename, URL, etc. columns)."""
        for schema_name in self.schemas:
            if schema_name in self.model.schemas:
                if table_name in self.model.schemas[schema_name].tables:
                    table = self.model.schemas[schema_name].tables[table_name]
                    return ASSET_COLUMNS.issubset({c.name for c in table.columns})
        return False

    def _get_table_schema(self, table_name: str) -> str | None:
        """Find which schema contains a table."""
        for schema_name in self.schemas:
            if schema_name in self.model.schemas:
                if table_name in self.model.schemas[schema_name].tables:
                    return schema_name
        return None

    def _localize_asset_row(
        self,
        row: list,
        asset_indexes: tuple[int, int] | None,
        asset_map: dict[str, str],
    ) -> tuple:
        """Replace URL with local path in asset table row.

        Args:
            row: List of column values.
            asset_indexes: (filename_index, url_index) or None if not asset table.
            asset_map: URL to local path mapping.

        Returns:
            Tuple of updated column values.
        """
        if asset_indexes:
            file_column, url_column = asset_indexes
            url = row[url_column]
            if url and url in asset_map:
                row[file_column] = asset_map[url]
            elif url:
                # Keep original if not in map
                pass
        return tuple(row)

    def _load_data(self) -> None:
        """Load CSV data files into the SQLite database."""
        data_path = self.bag_path / "data"
        asset_map = self._build_asset_map()

        for csv_file in data_path.rglob("*.csv"):
            table_name = csv_file.stem
            schema_name = self._get_table_schema(table_name)

            if schema_name is None:
                logger.debug(f"Skipping {table_name} - not in configured schemas")
                continue

            sql_table = self.metadata.tables.get(f"{schema_name}.{table_name}")
            if sql_table is None:
                logger.warning(f"Table {schema_name}.{table_name} not found in metadata")
                continue

            with csv_file.open(newline="") as csvfile:
                csv_reader = reader(csvfile)
                column_names = next(csv_reader)

                # Get asset column indexes if this is an asset table
                asset_indexes = None
                if self._is_asset_table(table_name):
                    try:
                        asset_indexes = (
                            column_names.index("Filename"),
                            column_names.index("URL"),
                        )
                    except ValueError:
                        pass

                # Load data
                with self.engine.begin() as conn:
                    rows = [
                        self._localize_asset_row(list(row), asset_indexes, asset_map)
                        for row in csv_reader
                    ]
                    if rows:
                        conn.execute(
                            sqlite_insert(sql_table).on_conflict_do_nothing(),
                            [dict(zip(column_names, row)) for row in rows],
                        )

    def dispose(self) -> None:
        """Dispose of SQLAlchemy resources.

        Call this when done with the database to properly clean up connections.
        After calling dispose(), the instance should not be used further.
        """
        if hasattr(self, "_disposed") and self._disposed:
            return

        if hasattr(self, "Base"):
            self.Base.registry.dispose()
        if hasattr(self, "engine"):
            self.engine.dispose()

        self._disposed = True

    def __del__(self) -> None:
        """Cleanup resources when garbage collected."""
        self.dispose()

    def __enter__(self) -> "BagDatabase":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - dispose resources."""
        self.dispose()
        return False

    # =========================================================================
    # Query Methods
    # =========================================================================

    def list_tables(self) -> list[str]:
        """List all tables in the database.

        Returns:
            List of fully-qualified table names (schema.table), sorted.
        """
        tables = list(self.metadata.tables.keys())
        tables.sort()
        return tables

    def find_table(self, table_name: str) -> SQLTable:
        """Find a table by name.

        Args:
            table_name: Table name, with or without schema prefix.

        Returns:
            SQLAlchemy Table object.

        Raises:
            KeyError: If table not found.
        """
        # Try exact match first
        if table_name in self.metadata.tables:
            return self.metadata.tables[table_name]

        # Try matching just the table name part
        for full_name, table in self.metadata.tables.items():
            if full_name.split(".")[-1] == table_name:
                return table

        raise KeyError(f"Table {table_name} not found")

    def get_table_contents(self, table: str) -> Generator[dict[str, Any], None, None]:
        """Retrieve all rows from a table as dictionaries.

        Args:
            table: Table name (with or without schema prefix).

        Yields:
            Dictionary for each row with column names as keys.
        """
        sql_table = self.find_table(table)
        with self.engine.connect() as conn:
            result = conn.execute(select(sql_table))
            for row in result.mappings():
                yield dict(row)

    def get_orm_class_by_name(self, table_name: str) -> Any | None:
        """Get the ORM class for a table by name.

        Args:
            table_name: Table name, with or without schema prefix.

        Returns:
            SQLAlchemy ORM class for the table.

        Raises:
            KeyError: If table not found.
        """
        sql_table = self.find_table(table_name)
        return self.get_orm_class_for_table(sql_table)

    def get_orm_class_for_table(self, table: SQLTable | DerivaTable | str) -> Any | None:
        """Get the ORM class for a table.

        Args:
            table: SQLAlchemy Table, Deriva Table, or table name.

        Returns:
            SQLAlchemy ORM class, or None if not found.
        """
        if isinstance(table, DerivaTable):
            table = self.metadata.tables.get(f"{table.schema.name}.{table.name}")
        if isinstance(table, str):
            table = self.find_table(table)
        if table is None:
            return None

        for mapper in self.Base.registry.mappers:
            if mapper.persist_selectable is table or table in mapper.tables:
                return mapper.class_
        return None

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
        """Check if an ORM class represents an association table.

        An association table links two or more tables through foreign keys,
        with a composite unique key covering those foreign keys.

        Args:
            table_class: SQLAlchemy ORM class to check.
            min_arity: Minimum number of foreign keys (default 2).
            max_arity: Maximum number of foreign keys (default 2).
            unqualified: If True, reject associations with extra key columns.
            pure: If True, reject associations with extra non-key columns.
            no_overlap: If True, reject associations with shared FK columns.
            return_fkeys: If True, return the foreign keys instead of arity.

        Returns:
            If return_fkeys=False: Integer arity if association, False otherwise.
            If return_fkeys=True: Set of foreign keys if association, False otherwise.
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

        # Choose longest compound key
        row_key = sorted(non_sys_key_colsets, key=lambda s: len(s), reverse=True)[0]
        foreign_keys = list(inspect(table_class).relationships.values())

        covered_fkeys = {
            fkey for fkey in foreign_keys
            if {c.name for c in fkey.local_columns}.issubset(row_key)
        }
        covered_fkey_cols = set()

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

        Args:
            left_cls: First ORM class.
            right_cls: Second ORM class.

        Returns:
            Tuple of (association_class, left_relationship, right_relationship),
            or None if no association found.
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
                return mid_cls, found_left.class_attribute, found_right.class_attribute

        return None
