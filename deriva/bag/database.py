"""Schema-independent SQLAlchemy access to BDBags.

This module provides :class:`BagDatabase`, which opens a BDBag
directory as a SQLAlchemy ORM layer. It reads the bag's
``data/schema.json`` (ERMrest model description), builds SQLite
tables matching that schema via :class:`~deriva.bag.schema.SchemaBuilder`,
and loads the CSVs in ``data/<schema>/<table>.csv``. Asset paths
are resolved on demand by :meth:`BagDatabase.resolve_asset_local_path`
(which consults ``fetch.txt`` and the bag-profile embedded-asset
directory layout) — not at load time.

The on-disk SQLite layout is one ``main.db`` plus one attached
``{schema}.db`` per ERMrest schema. SQLAlchemy hides the multi-file
layout behind a single engine, metadata, and ORM ``Base`` — all
constructed by :class:`~deriva.bag.schema.SchemaBuilder`, which
applies the project's WAL + pragma policy via
:func:`~deriva.bag.sqlite_helpers.create_wal_engine`.
:class:`BagDatabase` then calls
:func:`~deriva.bag.sqlite_helpers.ensure_schema_meta` with
:data:`~deriva.bag.profile.BAG_SCHEMA_VERSION` against the resulting
engine to guard against on-disk-newer-than-code drift before
loading the bag's CSVs.

This is a generic implementation; it knows about the deriva-bag
profile but not about datasets, executions, features, or any other
deriva-ml domain concept.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Generator
from urllib.parse import urlparse

from deriva.core.ermrest_model import Model

from deriva.bag.profile import BAG_SCHEMA_VERSION
from deriva.bag.schema import SchemaBuilder, SchemaORM
from deriva.bag.sqlite_helpers import ensure_schema_meta


logger = logging.getLogger(__name__)


class BagDatabase:
    """Schema-independent SQLite database created from a BDBag.

    Opens a BDBag directory and creates a SQLite database containing
    all the table data from the bag's CSV files. The bag's
    ``data/schema.json`` describes the ERMrest model; SQLite tables
    matching that schema (with foreign-key constraints) are produced
    by :class:`~deriva.bag.schema.SchemaBuilder`. The database is
    created under ``database_dir / <bag-cache-dirname>``, with
    separate per-schema SQLite files attached to a main ``main.db``.

    Attributes:
        bag_path: Path to the BDBag directory.
        database_dir: Directory holding the per-bag SQLite files.
        snaptime: Catalog snapshot time from schema.json.
        orm: The :class:`~deriva.bag.schema.SchemaORM` built from the
            bag's schema. ``BagDatabase`` exposes ``engine``,
            ``metadata``, ``Base``, ``model``, ``schemas`` as
            forwarding properties so callers don't have to reach
            through ``orm``.

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
        """Open a BDBag as a SQLite-backed ORM.

        Args:
            bag_path: Path to the BDBag directory containing
                ``data/``.
            database_dir: Directory where SQLite database files will
                be created. The database uses the bag's parent
                directory name (which typically includes a checksum)
                to ensure uniqueness across bag versions.
            schemas: List of schema names to load from the bag.
                Tables from other schemas are ignored.
        """
        self.bag_path = bag_path

        schema_file = bag_path / "data/schema.json"
        model = Model.fromfile("file-system", schema_file)
        with schema_file.open("r") as f:
            self.snaptime = json.load(f)["snaptime"]

        # The bag-cache directory name typically includes a checksum
        # so distinct bag versions land in distinct subdirectories.
        bag_cache_dir = bag_path.parent.name
        self.database_dir = database_dir / bag_cache_dir
        self.database_dir.mkdir(parents=True, exist_ok=True)

        # Phase 1: SchemaBuilder builds the engine, metadata, automap
        # Base, and cross-schema relationships. ``database_dir`` is a
        # Path without ``.db`` suffix, so SchemaBuilder puts main.db
        # inside it and attaches per-schema {schema}.db files.
        self.orm: SchemaORM = SchemaBuilder(
            model, schemas, database_path=self.database_dir
        ).build()

        # ``model`` is exposed as a regular attribute (not a @property)
        # so subclasses can compose with classes that assign ``self.model``
        # in their own __init__ -- specifically deriva-ml's DerivaModel,
        # which combines with BagDatabase to form DatabaseModel and
        # sets self.model from its own constructor argument. The value
        # we store here is the same ERMrest Model passed into the
        # SchemaORM (it doesn't mutate after construction).
        self.model = self.orm.model

        # Verify the on-disk schema version. Catches the case where a
        # newer deriva.bag wrote this cache and an older version is
        # trying to open it; raises SchemaVersionError with a clear
        # message rather than failing on a missing column later.
        ensure_schema_meta(self.orm.engine, expected_version=BAG_SCHEMA_VERSION)

        # Phase 2: load the bag's CSVs into the mirror.
        self._load_data()

        logger.info(
            "Created database for bag %s in %s",
            bag_path.name,
            self.database_dir,
        )

    # ------------------------------------------------------------------
    # Properties forwarding to the composed SchemaORM.
    # ------------------------------------------------------------------

    @property
    def engine(self):
        return self.orm.engine

    @property
    def metadata(self):
        return self.orm.metadata

    @property
    def Base(self):
        return self.orm.Base

    @property
    def schemas(self) -> list[str]:
        return self.orm.schemas

    # ------------------------------------------------------------------
    # Asset helpers (bag-specific).
    # ------------------------------------------------------------------

    def _build_asset_map(self) -> dict[str, str]:
        """Build (and memoize) a map from remote URLs to local file paths.

        Parses ``fetch.txt`` once per :class:`BagDatabase` instance
        and caches the result on ``self._asset_map_cache``. Asset-
        row resolution (:meth:`resolve_asset_local_path`) calls this
        per row; without memoization a bag with N asset rows reads
        ``fetch.txt`` N times.

        The map is keyed by **both** the full URL and the URL's path
        component. CSV rows typically carry whichever form the
        catalog stored — sometimes a full ``https://hatrac.../foo``,
        sometimes a relative ``/hatrac/...``.

        Returns:
            Dictionary mapping URL (or URL path) to local file path.
        """
        cached = getattr(self, "_asset_map_cache", None)
        if cached is not None:
            return cached

        fetch_map: dict[str, str] = {}
        fetch_file = self.bag_path / "fetch.txt"

        if not fetch_file.exists():
            logger.info(f"No fetch.txt in bag {self.bag_path.name}")
            self._asset_map_cache = fetch_map
            return fetch_map

        try:
            with fetch_file.open(newline="\n") as f:
                for row in f:
                    # Rows in fetch.txt are tab-separated: URL, size, local_path
                    fields = row.split("\t")
                    if len(fields) >= 3:
                        full_url = fields[0]
                        local_file = fields[2].replace("\n", "")
                        local_path = f"{self.bag_path}/{local_file}"
                        # Map both the full URL and the path-only form
                        # so callers hit whichever shape the row carries.
                        fetch_map[full_url] = local_path
                        fetch_map[urlparse(full_url).path] = local_path
        except Exception as e:
            logger.warning(f"Error reading fetch.txt: {e}")

        self._asset_map_cache = fetch_map
        return fetch_map

    def resolve_asset_local_path(
        self,
        table_name: str,
        row: dict[str, Any],
    ) -> str | None:
        """Find the bag-local path for an asset row's bytes.

        Asks two questions, in order:

        1. Is the row's ``URL`` in :meth:`_build_asset_map`?
           That map is populated from the bag's ``fetch.txt`` —
           the clone/MINID path (bytes downloaded by
           ``bdb.materialize`` and recorded with their bag-local
           destination).
        2. Otherwise, is there a file at the profile-standard
           embedded-asset path
           ``data/asset/{table}/{rid}/{filename}``? Constructive
           bags built by :meth:`BagBuilder.add_asset` write
           bytes there directly.

        Args:
            table_name: Asset table name (e.g. ``"Image"``).
            row: Row dict with at least ``URL``, ``RID``,
                ``Filename`` keys.

        Returns:
            Absolute path to the bag-local file as a string, or
            ``None`` if no bytes are present in the bag for this
            row.
        """
        url = row.get("URL")
        if url:
            asset_map = self._build_asset_map()
            if url in asset_map:
                return asset_map[url]

        rid = row.get("RID")
        filename = row.get("Filename")
        if not rid or not filename:
            return None
        embedded = (
            self.bag_path
            / "data"
            / "asset"
            / table_name
            / rid
            / filename
        )
        if embedded.is_file():
            return str(embedded)
        return None

    def _load_data(self) -> None:
        """Load the bag's CSVs into the SQLite mirror, FK-ordered.

        The bag may legitimately ship dangling FK refs (an anchor-
        scoped walk can leave referencing rows whose target is
        outside the chosen slice); SQLite's ``foreign_keys=ON`` would
        refuse such rows. Authoritative FK validation happens later
        when :class:`~deriva.bag.catalog_loader.BagCatalogLoader`
        writes into the destination catalog and applies the policy's
        :class:`~deriva.bag.traversal.DanglingFKStrategy`. So we
        turn FK enforcement off for the duration of the load, then
        restore it.

        Delegates the row movement to
        :class:`~deriva.bag.loader.DataLoader` driving
        :class:`~deriva.bag.sources.BagDataSource` into
        :class:`~deriva.bag.loader.SQLiteSink` — the same pipeline
        every other consumer of the bag uses.
        """
        # Local imports: ``loader`` and ``sources`` both depend on
        # this module's :class:`BagDatabase` through ``schema``, so
        # the top-level cycle is real.
        from sqlalchemy import event

        from deriva.bag.loader import DataLoader, SQLiteSink
        from deriva.bag.sources import BagDataSource

        source = BagDataSource(self.bag_path)
        # FK enforcement is connection-scoped; toggle via a connect
        # listener so every connection the pool hands out during the
        # load has the pragma off. Dispose the existing pool first so
        # the listener fires on the next checkout — any connection
        # already issued during schema build would not see it.
        def _fk_off(dbapi_conn, _conn_record):
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys = OFF")
            cur.close()

        engine = self.orm.engine
        engine.dispose()
        event.listen(engine, "connect", _fk_off)
        try:
            try:
                loader = DataLoader(
                    self.orm, source, sink=SQLiteSink(self.orm)
                )
                loader.load_tables()
            finally:
                # Always remove the listener — but if removal itself
                # raises (it shouldn't, but defensive), still run the
                # final dispose() below to flush pool connections.
                event.remove(engine, "connect", _fk_off)
        finally:
            # Force new connections to re-pick up the default FK=ON
            # behaviour from create_wal_engine's connect listener.
            engine.dispose()

    def dispose(self) -> None:
        """Dispose of SQLAlchemy resources.

        Call this when done with the database to properly clean up
        connections. After calling ``dispose()``, the instance should
        not be used further. Idempotent.
        """
        if getattr(self, "_disposed", False):
            return
        if hasattr(self, "orm"):
            self.orm.dispose()
        self._disposed = True

    def __del__(self) -> None:
        """Best-effort cleanup at garbage-collection time.

        Intentionally delegates to ``self.dispose()`` (which in turn
        calls ``self.orm.dispose()``). ``SchemaORM`` also defines
        ``__del__`` with its own GC-safety swallow; the two
        ``__del__``s are deliberately redundant — the
        ``BagDatabase`` instance and its owned ``SchemaORM`` are
        usually collected separately, and one finishing first must
        not leave the other to run with half-torn-down state.
        """
        try:
            self.dispose()
        except Exception:
            pass

    def __enter__(self) -> "BagDatabase":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.dispose()
        return False

    # ------------------------------------------------------------------
    # Query methods — forward to the composed SchemaORM.
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Return every fully-qualified table name in the database, sorted."""
        return self.orm.list_tables()

    def find_table(self, table_name):
        """Look up a SQLAlchemy ``Table`` by name."""
        return self.orm.find_table(table_name)

    def get_table_contents(
        self, table: str
    ) -> Generator[dict[str, Any], None, None]:
        """Yield rows from a table as plain dictionaries."""
        yield from self.orm.get_table_contents(table)

    def get_orm_class_by_name(self, table_name: str) -> Any | None:
        """Look up the ORM class for a table by name."""
        return self.orm.get_orm_class(table_name)

    def get_orm_class_for_table(self, table) -> Any | None:
        """Look up the ORM class for a table (SQLAlchemy or deriva-py)."""
        return self.orm.get_orm_class_for_table(table)

    def get_association_class(
        self, left_cls: Any, right_cls: Any
    ) -> tuple[Any, Any, Any] | None:
        """Find an association class connecting two ORM classes.

        See :meth:`SchemaORM.get_association_class` for the
        signature and semantics.
        """
        return self.orm.get_association_class(left_cls, right_cls)

