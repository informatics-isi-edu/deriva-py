"""Bag-oriented data movement for Deriva catalogs.

The ``deriva.bag`` submodule provides a single, unified pipeline for
moving catalog content across boundaries via BDBags. Every supported
data-movement operation factors through a bag:

- **Catalog → bag** (slice, dataset export, clone): write a bag from a
  live ERMrest catalog using :class:`CatalogBagBuilder` (forthcoming).
- **In-memory → bag** (programmatic build, end-of-execution upload):
  write a bag from caller-supplied data or a local SQLite database
  using :class:`BagBuilder` (forthcoming).
- **Bag → catalog** (clone destination, dataset re-import,
  end-of-execution commit): load a bag into a destination ERMrest
  catalog using :class:`BagCatalogLoader` (forthcoming).
- **Bag → SQLAlchemy** (consumption): open a bag as a SQLAlchemy ORM
  using :class:`BagDatabase` (forthcoming, lifted from
  :mod:`deriva.core.bag_database`).

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

This module is built incrementally; the foundation pieces
(:mod:`~deriva.bag.profile`, :mod:`~deriva.bag.sqlite_helpers`, the
profile JSON document) land first. Producer classes
(:class:`BagBuilder`, :class:`CatalogBagBuilder`,
:class:`BagCatalogLoader`) and the consumer-side move of
:class:`BagDatabase` follow in subsequent commits.
"""

from __future__ import annotations

__all__: list[str] = []
