"""Bag-oriented data movement for Deriva catalogs.

The ``deriva.bag`` submodule provides a single, unified pipeline for
moving catalog content across boundaries via BDBags. Every supported
data-movement operation factors through a bag:

- **Catalog → bag** (slice, dataset export, clone): write a bag from a
  live ERMrest catalog using :class:`CatalogBagBuilder`.
- **In-memory → bag** (programmatic build, end-of-execution upload):
  write a bag from caller-supplied data or a local SQLite database
  using :class:`BagBuilder`.
- **Bag → catalog** (clone destination, dataset re-import,
  end-of-execution commit): load a bag into a destination ERMrest
  catalog using :class:`BagCatalogLoader`.
- **Bag → SQLAlchemy** (consumption): open a bag as a SQLAlchemy ORM
  using :class:`BagDatabase`.

All bags conform to the **deriva-bag profile**, a BagIt Profile JSON
document hosted under :mod:`deriva.bag.profiles`. The profile declares
the bag's internal layout (``data/schema.json``, ``data/<schema>/<table>.csv``,
``data/asset/<table>/<RID>/<filename>``, optional ``metadata/`` for
provenance). Producers stamp the profile identifier into
``bag-info.txt`` so consumers can validate via
:func:`bdbag.bdbag_api.validate_bag_profile`.

Internally, SQLAlchemy :class:`~sqlalchemy.MetaData` is the canonical
schema vocabulary across producers and consumers. Producers accept
multiple input forms (deriva-py ``typed.SchemaDef`` for callers already
pushing schemas to a catalog, or a SQLAlchemy ``MetaData`` directly);
all are normalized to ``MetaData`` via :mod:`deriva.bag.schema_io`.

See ADR-0006 (``docs/adr/0006-bag-oriented-data-movement.md`` in the
deriva-ml repository) for the full design rationale.
"""

from __future__ import annotations

from deriva.bag.anchors import (
    Anchor,
    AnchorKind,
    RIDAnchor,
    TableAnchor,
)
from deriva.bag.builder import BagBuilder
from deriva.bag.cache_index import BagCacheIndex
from deriva.bag.catalog_builder import CatalogBagBuilder
from deriva.bag.catalog_loader import (
    BagCatalogLoader,
    LoadReport,
    TableClass,
    TableLoadStats,
)
from deriva.bag.database import BagDatabase
from deriva.bag.loader import (
    CSVSink,
    DataLoader,
    ForeignKeyOrderer,
    Sink,
    SQLiteSink,
)
from deriva.bag.path_walker import (
    EdgeFilter,
    SchemaPathWalker,
)
from deriva.bag.profile import (
    BAG_SCHEMA_VERSION,
    BAGIT_PROFILE_IDENTIFIER,
)
from deriva.bag.schema import SchemaBuilder, SchemaORM
from deriva.bag.sources import (
    BagDataSource,
    CatalogDataSource,
    DataFrameDataSource,
    DataSource,
    IterableDataSource,
    LocalDBDataSource,
)
from deriva.bag.sqlite_helpers import (
    SchemaVersionError,
    create_wal_engine,
    ensure_schema_meta,
)
from deriva.bag.traversal import (
    AssetMode,
    ContentConflictStrategy,
    DanglingFKStrategy,
    FKTraversalPolicy,
    VocabExport,
)

__all__ = [
    # Anchors
    "Anchor",
    "AnchorKind",
    "RIDAnchor",
    "TableAnchor",
    # Producers
    "BagBuilder",
    "CatalogBagBuilder",
    # Loader
    "BagCatalogLoader",
    "LoadReport",
    "TableClass",
    "TableLoadStats",
    # Consumer
    "BagDatabase",
    # Cache
    "BagCacheIndex",
    # Loader primitives
    "CSVSink",
    "DataLoader",
    "ForeignKeyOrderer",
    "Sink",
    "SQLiteSink",
    # Path walker (shared FK-graph primitive)
    "EdgeFilter",
    "SchemaPathWalker",
    # Schema
    "SchemaBuilder",
    "SchemaORM",
    # Sources
    "BagDataSource",
    "CatalogDataSource",
    "DataFrameDataSource",
    "DataSource",
    "IterableDataSource",
    "LocalDBDataSource",
    # Profile constants
    "BAG_SCHEMA_VERSION",
    "BAGIT_PROFILE_IDENTIFIER",
    # SQLite helpers
    "SchemaVersionError",
    "create_wal_engine",
    "ensure_schema_meta",
    # Traversal
    "AssetMode",
    "ContentConflictStrategy",
    "DanglingFKStrategy",
    "FKTraversalPolicy",
    "VocabExport",
]
