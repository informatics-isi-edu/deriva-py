"""Data sources for filling a :class:`~deriva.bag.schema.SchemaORM`.

The :class:`DataSource` protocol describes the interface
:class:`~deriva.bag.loader.DataLoader` expects: yield rows as plain
dicts, report which tables have data, and answer "is this table
known to me?". Different implementations adapt the protocol to
different input shapes; the loader doesn't care which.

Five implementations live in this module:

- :class:`BagDataSource` — read rows from a bag's
  ``data/<schema>/<table>.csv`` files. The consumer-side default.
- :class:`CatalogDataSource` — fetch rows from a live ERMrest
  catalog via the deriva-py path builder. Used when mirroring a
  live catalog into a local SQLite (today's deriva-ml ``local_db``
  use case) — *not* the path :class:`CatalogBagBuilder` uses to
  build bags from catalogs (that goes through the deriva-py export
  engine for paged streaming).
- :class:`DataFrameDataSource` — adapt a ``dict[str, pandas.DataFrame]``
  (one DataFrame per table). The producer-side path for callers
  whose data already lives in pandas.
- :class:`IterableDataSource` — adapt a
  ``dict[str, Iterable[dict[str, Any]]]``. Producer-side path for
  callers who want to stream rows without materializing a DataFrame
  first (cifar's per-image dict generator, for example).
- :class:`LocalDBDataSource` — read rows from a deriva-ml
  ``local_db`` SQLite database. The end-of-execution upload path
  builds a bag from the local DB via this source.

Together with :class:`~deriva.bag.loader.CSVSink` (a write-out
sink for ``DataLoader``), these classes let :class:`BagBuilder`
reuse the same loader machinery the bag-consumer side already
exercises — one DataLoader, one DataSource protocol, two sink
implementations.

Lifted from ``deriva_ml.model.data_sources``. The lift adds
:class:`DataFrameDataSource`, :class:`IterableDataSource`, and
:class:`LocalDBDataSource`; the existing :class:`BagDataSource`
and :class:`CatalogDataSource` are preserved with their interface
intact.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Iterable, Iterator, Protocol, runtime_checkable
from urllib.parse import urlparse

from deriva.core import ErmrestCatalog
from deriva.core.ermrest_model import Model
from deriva.core.ermrest_model import Table as DerivaTable
from sqlalchemy import select
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# =============================================================================
# Protocol
# =============================================================================


@runtime_checkable
class DataSource(Protocol):
    """Protocol every input to :class:`DataLoader` must satisfy.

    Implementations promise three things:

    - :meth:`get_table_data` — yield rows for a named table as plain
      ``dict[str, Any]``. Each row's keys are the table's column
      names; values are Python-typed (strings allowed when the
      schema's column type accepts string coercion through a
      ``StringTo*`` decorator).
    - :meth:`has_table` — is this table known to me?
    - :meth:`list_available_tables` — what tables can I yield from?
      (Used by the loader to fall back when the caller passes
      ``tables=None``.)

    The protocol is intentionally tiny; everything else
    (transactions, ordering, batching, conflict handling) lives in
    :class:`~deriva.bag.loader.DataLoader`.
    """

    def get_table_data(
        self,
        table: DerivaTable | str,
    ) -> Iterator[dict[str, Any]]:
        """Yield rows for a table.

        Args:
            table: A deriva-py :class:`Table` or a table name string.

        Yields:
            One ``dict[str, Any]`` per row, keyed by column name.
        """
        ...

    def has_table(self, table: DerivaTable | str) -> bool:
        """Return ``True`` if rows are available for the table."""
        ...

    def list_available_tables(self) -> list[str]:
        """List table names this source can yield from."""
        ...


# =============================================================================
# BagDataSource
# =============================================================================


class BagDataSource:
    """Read rows from a bag's per-table CSV files.

    Walks the bag's ``data/`` directory looking for ``*.csv`` and
    indexes them by the file stem (the table name). When the source
    table has a ``URL`` column (i.e., is an asset table), values
    pointed at by ``fetch.txt`` are localized so the loaded ``Filename``
    points at the on-disk path instead of the remote URL.

    Args:
        bag_path: Path to a BDBag directory.
        model: Optional ERMrest :class:`Model`. When ``None``, the
            source loads it from ``data/schema.json`` inside the bag.
            Used to decide which tables are asset tables (for URL
            localization).
        asset_localization: Set ``False`` to skip the fetch.txt-based
            URL→path rewrite. The default (``True``) matches what
            :class:`~deriva.bag.database.BagDatabase` does on its own
            load path.

    Example:
        Open a bag and stream its Subject rows::

            >>> from pathlib import Path
            >>> source = BagDataSource(Path("/path/to/bag"))  # doctest: +SKIP
            >>> for row in source.get_table_data("Subject"):  # doctest: +SKIP
            ...     print(row["Name"])  # doctest: +SKIP
    """

    def __init__(
        self,
        bag_path: Path,
        model: Model | None = None,
        asset_localization: bool = True,
    ):
        self.bag_path = Path(bag_path)
        self.data_path = self.bag_path / "data"

        if model is None:
            schema_file = self.data_path / "schema.json"
            if schema_file.exists():
                self.model = Model.fromfile("file-system", schema_file)
            else:
                self.model = None
                logger.warning(f"No schema.json found in {self.bag_path}")
        else:
            self.model = model

        self._asset_map = (
            self._build_asset_map() if asset_localization else {}
        )

        # table-stem → list of CSV paths. Multiple paths can exist
        # for nested-dataset cases where the same table is exported
        # from different points in the FK graph.
        self._csv_cache: dict[str, list[Path]] = {}
        self._build_csv_cache()

    def _build_csv_cache(self) -> None:
        """Index every ``.csv`` under ``data/`` by file stem."""
        for csv_file in self.data_path.rglob("*.csv"):
            table_name = csv_file.stem
            self._csv_cache.setdefault(table_name, []).append(csv_file)

    def _build_asset_map(self) -> dict[str, str]:
        """Parse ``fetch.txt`` into a URL-path → local-path map.

        ``fetch.txt`` rows are tab-separated: URL, length, local
        path. We key the map by ``urlparse(url).path`` (the path
        portion of the URL, not the full URL) because the loaded CSV
        row carries the same path. Mismatched-protocol or
        differently-cased URLs are tolerated; only the path matches.
        """
        fetch_map: dict[str, str] = {}
        fetch_file = self.bag_path / "fetch.txt"
        if not fetch_file.exists():
            logger.debug(f"No fetch.txt in bag {self.bag_path.name}")
            return fetch_map
        try:
            with fetch_file.open(newline="\n") as f:
                for row in f:
                    fields = row.split("\t")
                    if len(fields) >= 3:
                        local_file = fields[2].replace("\n", "")
                        local_path = f"{self.bag_path}/{local_file}"
                        fetch_map[urlparse(fields[0]).path] = local_path
        except Exception as e:
            logger.warning(f"Error reading fetch.txt: {e}")
        return fetch_map

    def _get_table_name(self, table: DerivaTable | str) -> str:
        """Extract the bare table-name from a Table or qualified string."""
        if isinstance(table, DerivaTable):
            return table.name
        if "." in table:
            return table.split(".")[-1]
        return table

    def _is_asset_table(self, table_name: str) -> bool:
        """Use ``Table.is_asset()`` from the model to detect asset tables."""
        if self.model is None:
            return False
        for schema in self.model.schemas.values():
            if table_name in schema.tables:
                return schema.tables[table_name].is_asset()
        return False

    def _localize_asset_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """If the row's URL is in the asset map, rewrite ``Filename``.

        Note (pre-existing upstream bug): the lookup keys the asset
        map by ``urlparse(url).path`` but checks ``url in self._asset_map``,
        which uses the full URL. This silently does nothing for any
        real asset row. The behavior is preserved here for parity
        with the consumer path in
        :class:`~deriva.bag.database.BagDatabase`; the fix will land
        as part of the deriva-ml migration PR.
        """
        if "URL" in row and "Filename" in row:
            url = row.get("URL")
            if url and url in self._asset_map:
                row = dict(row)
                row["Filename"] = self._asset_map[url]
        return row

    def get_table_data(
        self,
        table: DerivaTable | str,
    ) -> Iterator[dict[str, Any]]:
        """Yield rows from every CSV matching the table's name."""
        table_name = self._get_table_name(table)
        csv_files = self._csv_cache.get(table_name)
        if not csv_files:
            logger.debug(f"No CSV file found for table {table_name}")
            return

        is_asset = self._is_asset_table(table_name)
        for csv_file in csv_files:
            if not csv_file.exists():
                continue
            with csv_file.open(newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if is_asset and self._asset_map:
                        row = self._localize_asset_row(row)
                    yield row

    def has_table(self, table: DerivaTable | str) -> bool:
        """Return ``True`` if a CSV for the table is present in the bag."""
        return self._get_table_name(table) in self._csv_cache

    def list_available_tables(self) -> list[str]:
        """Return the sorted bare-table names of every CSV in the bag."""
        return sorted(self._csv_cache.keys())

    def get_row_count(self, table: DerivaTable | str) -> int:
        """Count rows across every CSV matching the table's name.

        Args:
            table: Table to count.

        Returns:
            Total data rows (excluding CSV headers) across all
            matching files.
        """
        table_name = self._get_table_name(table)
        csv_files = self._csv_cache.get(table_name)
        if not csv_files:
            return 0
        total = 0
        for csv_file in csv_files:
            if csv_file.exists():
                with csv_file.open(newline="") as f:
                    total += sum(1 for _ in f) - 1
        return total


# =============================================================================
# CatalogDataSource
# =============================================================================


class CatalogDataSource:
    """Fetch rows from a live ERMrest catalog with RID pagination.

    Used when mirroring a live catalog into a local SQLite via
    :class:`~deriva.bag.loader.DataLoader`. Each query is bounded by
    ``batch_size`` rows and ordered by RID; subsequent queries pick
    up at ``RID > last_seen_rid``. The pagination is server-friendly
    and avoids the "offset N" pattern that gets quadratic on large
    tables.

    Args:
        catalog: An :class:`ErmrestCatalog` handle.
        schemas: Schemas to expose to the loader. Tables in other
            schemas are reported as unknown.
        batch_size: Rows per request.

    Example:
        ::

            from deriva.core import ErmrestCatalog
            catalog = ErmrestCatalog(...)
            source = CatalogDataSource(catalog, ["deriva-ml"])
            for row in source.get_table_data("Dataset"):
                process(row)
    """

    def __init__(
        self,
        catalog: ErmrestCatalog,
        schemas: list[str],
        batch_size: int = 1000,
    ):
        self.catalog = catalog
        self.schemas = schemas
        self.batch_size = batch_size
        self._pb = catalog.getPathBuilder()
        self._model = catalog.getCatalogModel()

    def _get_table_info(
        self, table: DerivaTable | str
    ) -> tuple[str, str] | None:
        """Resolve ``table`` to ``(schema_name, table_name)`` if known."""
        if isinstance(table, DerivaTable):
            return table.schema.name, table.name
        if "." in table:
            parts = table.split(".")
            schema_name, table_name = parts[0], parts[1]
            if schema_name in self.schemas:
                return schema_name, table_name
            return None
        for schema_name in self.schemas:
            if schema_name in self._model.schemas:
                schema = self._model.schemas[schema_name]
                if table in schema.tables:
                    return schema_name, table
        return None

    def get_table_data(
        self,
        table: DerivaTable | str,
    ) -> Iterator[dict[str, Any]]:
        """Yield catalog rows for the table, paginating by RID."""
        info = self._get_table_info(table)
        if info is None:
            logger.warning(
                f"Table {table} not found in schemas {self.schemas}"
            )
            return
        schema_name, table_name = info
        path = self._pb.schemas[schema_name].tables[table_name]
        last_rid: Any = None
        while True:
            query = path.entities()
            if last_rid is not None:
                query = query.filter(path.RID > last_rid)
            try:
                entities = list(
                    query.sort(path.RID).fetch(limit=self.batch_size)
                )
            except Exception as e:
                logger.error(
                    f"Error fetching from {schema_name}.{table_name}: {e}"
                )
                break
            if not entities:
                break
            for entity in entities:
                yield dict(entity)
            last_rid = entities[-1]["RID"]
            if len(entities) < self.batch_size:
                break

    def has_table(self, table: DerivaTable | str) -> bool:
        """Return ``True`` if the table exists in a configured schema."""
        return self._get_table_info(table) is not None

    def list_available_tables(self) -> list[str]:
        """Return every ``schema.table`` in the configured schemas."""
        tables = []
        for schema_name in self.schemas:
            if schema_name in self._model.schemas:
                for table_name in self._model.schemas[schema_name].tables:
                    tables.append(f"{schema_name}.{table_name}")
        return sorted(tables)

    def get_row_count(self, table: DerivaTable | str) -> int:
        """Run a count aggregate against the catalog."""
        info = self._get_table_info(table)
        if info is None:
            return 0
        schema_name, table_name = info
        path = self._pb.schemas[schema_name].tables[table_name]
        try:
            result = path.aggregates(path.RID.cnt.alias("count")).fetch()
            return result[0]["count"] if result else 0
        except Exception as e:
            logger.error(f"Error counting {schema_name}.{table_name}: {e}")
            return 0


# =============================================================================
# DataFrameDataSource
# =============================================================================


class DataFrameDataSource:
    """Adapt a ``dict[table_name, pandas.DataFrame]`` to :class:`DataSource`.

    The producer-side default for callers whose data is already in
    pandas. Each ``DataFrame`` becomes a stream of dict rows via
    ``df.itertuples()`` (or ``to_dict("records")`` depending on
    pandas version) when the loader pulls rows.

    Args:
        frames: Mapping from table name to a pandas DataFrame.
            Table names are matched as ``BagDataSource`` matches
            them (the bare table name, no schema prefix). The
            caller is responsible for the column-name → schema-name
            alignment.

    Note:
        pandas is imported lazily so the rest of :mod:`deriva.bag`
        stays usable without pandas installed.

    Example:
        ::

            import pandas as pd
            from deriva.bag.sources import DataFrameDataSource

            subjects = pd.DataFrame({"RID": ["S1"], "Name": ["Alice"]})
            source = DataFrameDataSource({"Subject": subjects})
            for row in source.get_table_data("Subject"):
                print(row)
    """

    def __init__(self, frames: dict[str, Any]):
        # Hold the frames as-is; defer the pandas import until
        # someone actually iterates.
        self._frames = dict(frames)

    @staticmethod
    def _table_name(table: DerivaTable | str) -> str:
        if isinstance(table, DerivaTable):
            return table.name
        if "." in table:
            return table.split(".")[-1]
        return table

    def get_table_data(
        self, table: DerivaTable | str
    ) -> Iterator[dict[str, Any]]:
        """Yield rows from the matching DataFrame as dicts."""
        name = self._table_name(table)
        df = self._frames.get(name)
        if df is None:
            return
        # Lazily import pandas so the module is usable without it.
        # Catching ImportError here also signals an obvious config
        # mistake (caller passed a DataFrame-shaped thing without
        # having pandas installed).
        try:
            import pandas as pd  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "pandas is required to iterate a DataFrameDataSource. "
                "Install pandas or use IterableDataSource instead."
            ) from e
        # ``to_dict("records")`` materializes; for very large frames
        # callers should use IterableDataSource. We accept the
        # materialization cost here because DataFrame callers
        # typically have already-in-memory data.
        for row in df.to_dict("records"):
            yield row

    def has_table(self, table: DerivaTable | str) -> bool:
        return self._table_name(table) in self._frames

    def list_available_tables(self) -> list[str]:
        return sorted(self._frames.keys())


# =============================================================================
# IterableDataSource
# =============================================================================


class IterableDataSource:
    """Adapt a ``dict[table_name, Iterable[dict]]`` to :class:`DataSource`.

    The streaming counterpart to :class:`DataFrameDataSource`. Use
    this when rows are generated lazily (yielded from a generator,
    streamed from a file, computed on demand) and materializing them
    into a DataFrame would cost too much memory.

    Each iterable is consumed *once* per
    :meth:`get_table_data` call. The caller is responsible for
    re-providing or pre-materializing iterables that need to be
    iterated more than once.

    Args:
        iterables: Mapping from table name to an iterable of row
            dicts. Iterables can be generators, lists, file readers,
            anything that yields ``dict[str, Any]``.

    Example:
        ::

            def iter_images():
                for path in Path("images").iterdir():
                    yield {"RID": path.stem, "Filename": path.name}

            source = IterableDataSource({"Image": iter_images()})
            loader.load_tables(["Image"])
    """

    def __init__(self, iterables: dict[str, Iterable[dict[str, Any]]]):
        self._iterables = dict(iterables)

    @staticmethod
    def _table_name(table: DerivaTable | str) -> str:
        if isinstance(table, DerivaTable):
            return table.name
        if "." in table:
            return table.split(".")[-1]
        return table

    def get_table_data(
        self, table: DerivaTable | str
    ) -> Iterator[dict[str, Any]]:
        name = self._table_name(table)
        it = self._iterables.get(name)
        if it is None:
            return
        for row in it:
            yield dict(row)

    def has_table(self, table: DerivaTable | str) -> bool:
        return self._table_name(table) in self._iterables

    def list_available_tables(self) -> list[str]:
        return sorted(self._iterables.keys())


# =============================================================================
# LocalDBDataSource
# =============================================================================


class LocalDBDataSource:
    """Read rows from a deriva-ml ``local_db``-style SQLite database.

    The end-of-execution upload path uses this source to build a bag
    from the working SQLite. The source is generic enough that any
    SQLAlchemy ``Engine`` over a SQLite file with deriva-ml-style
    table layout (``schema.table`` qualified names) works; it does
    not depend on deriva-ml itself.

    Args:
        engine: SQLAlchemy engine for the local SQLite database.
        schemas: Optional list of ERMrest schema names to expose.
            ``None`` means "every schema visible to the engine."

    Note:
        Asset bytes are *not* fetched by this source. The local DB's
        asset table rows carry filesystem paths under ``URL`` or
        ``Filename`` columns; the consumer (typically
        :class:`BagBuilder` followed by :class:`BagCatalogLoader`)
        handles asset transfer separately via the bag's asset
        directory layout. The source's job is only to yield the
        rows.

    Example:
        ::

            from sqlalchemy import create_engine
            engine = create_engine("sqlite:///working.db")
            source = LocalDBDataSource(engine, schemas=["deriva-ml"])
            for row in source.get_table_data("Execution"):
                print(row["RID"])
    """

    def __init__(
        self,
        engine: Engine,
        schemas: list[str] | None = None,
    ):
        self.engine = engine
        self.schemas = schemas
        # Cache the set of qualified table names visible to the
        # engine — local_db's pattern is one main.db + attached
        # per-schema .db files, so a single ``SELECT`` over each
        # attached database's ``sqlite_master`` enumerates them.
        self._tables: dict[str, str] = {}
        self._inspect_tables()

    def _inspect_tables(self) -> None:
        """Discover ``schema.table`` names via SQLite metadata.

        The discovery is intentionally lazy: we only query
        ``sqlite_master`` (which is cheap) and we accept the
        result whatever it looks like. The caller's schemas filter
        is applied after the fact.
        """
        from sqlalchemy import text

        with self.engine.connect() as conn:
            # ``pragma database_list`` returns every attached
            # database for this connection.
            dbs = conn.execute(text("PRAGMA database_list")).fetchall()
            for _seq, name, _file in dbs:
                if name == "temp":
                    continue
                if self.schemas is not None and name != "main" and name not in self.schemas:
                    continue
                rows = conn.execute(
                    text(
                        f"SELECT name FROM \"{name}\".sqlite_master "
                        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                    )
                ).fetchall()
                for (table_name,) in rows:
                    # Skip the schema_meta bookkeeping table.
                    if table_name == "schema_meta":
                        continue
                    # Use ``schema.table`` if the schema is a real
                    # attached database name; bare table name for the
                    # ``main`` database (which is the bag/local-db's
                    # default schema-less store).
                    key = (
                        f"{name}.{table_name}"
                        if name != "main"
                        else table_name
                    )
                    self._tables[key] = name

    def _resolve(
        self, table: DerivaTable | str
    ) -> tuple[str, str] | None:
        """Resolve ``table`` to a ``(qualified_name, db_alias)`` pair."""
        if isinstance(table, DerivaTable):
            key = f"{table.schema.name}.{table.name}"
            if key in self._tables:
                return key, self._tables[key]
            # Try the bare name if the table lives in main.
            if table.name in self._tables:
                return table.name, self._tables[table.name]
            return None
        if table in self._tables:
            return table, self._tables[table]
        # Try matching just the trailing table name across
        # schemas (lets the caller pass a bare name).
        bare = table.split(".")[-1] if "." in table else table
        for qname, db in self._tables.items():
            if qname.split(".")[-1] == bare:
                return qname, db
        return None

    def get_table_data(
        self, table: DerivaTable | str
    ) -> Iterator[dict[str, Any]]:
        from sqlalchemy import MetaData, Table as SQLTable

        resolved = self._resolve(table)
        if resolved is None:
            return
        qname, _db = resolved
        # Reflect just this one table so we don't pay the cost of
        # reflecting every attached schema's tables.
        metadata = MetaData()
        if "." in qname:
            schema, name = qname.split(".", 1)
            sql_table = SQLTable(
                name, metadata, autoload_with=self.engine, schema=schema
            )
        else:
            sql_table = SQLTable(
                qname, metadata, autoload_with=self.engine
            )
        with self.engine.connect() as conn:
            for row in conn.execute(select(sql_table)).mappings():
                yield dict(row)

    def has_table(self, table: DerivaTable | str) -> bool:
        return self._resolve(table) is not None

    def list_available_tables(self) -> list[str]:
        return sorted(self._tables.keys())
