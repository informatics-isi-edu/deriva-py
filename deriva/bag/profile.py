"""Deriva-bag profile constants and helpers.

The *deriva-bag profile* is the structural contract ``deriva.bag``
imposes on a BDBag carrying catalog content. It slots into BDBag's
existing **BagIt Profile** mechanism (a JSON document validatable by
:func:`bdbag.bdbag_api.validate_bag_profile`, declared via the
``BagIt-Profile-Identifier`` field in ``bag-info.txt``).

The profile constrains:

- ``data/schema.json`` — ERMrest model description for every table in
  the bag.
- ``data/<schema>/<table>.csv`` — one CSV per table, ERMrest CSV format.
- ``data/asset/<asset_table>/<RID>/<filename>`` — asset bytes (when
  embedded) or referenced via ``fetch.txt``.
- ``metadata/`` — optional provenance: the producer-side ``Anchor``
  list + ``FKTraversalPolicy``, build timestamps, and (for catalog
  walkers) the source-catalog handle.

This module owns:

- The **profile identifier URL** stamped into ``bag-info.txt``.
- The **profile JSON document** path (under ``deriva/bag/profiles/``).
- **Path constants** for every file the profile defines.
- **Schema version** for the Bag SQLAlchemy layer (so a newer
  ``deriva.bag`` that changes the SQLite layout can detect old caches).
- The **metadata-writer** that serializes producer-side provenance.
- The **archive-as-zip** helper that turns a bag directory into a
  ``.zip`` archive of the same name.

It does *not* know about ERMrest types, catalog connections, or
producer/consumer machinery. Those live in sibling modules
(:mod:`~deriva.bag.schema_io`, :mod:`~deriva.bag.database`, etc.).
"""

from __future__ import annotations

import json
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Profile identifier
# ---------------------------------------------------------------------------

#: URL of the deriva-bag BagIt Profile JSON document.
#:
#: Stamped into ``bag-info.txt`` under the ``BagIt-Profile-Identifier``
#: key by every ``deriva.bag`` producer. Consumers may pass this URL
#: (or a local path) to :func:`bdbag.bdbag_api.validate_bag_profile`
#: to confirm a bag's structural conformance.
#:
#: The URL points at the raw JSON file on the ``deriva-ml`` branch of
#: the deriva-py repository while the profile is still evolving. When
#: the profile stabilizes a versioned permalink (e.g., a tag-pinned
#: URL) will be minted and the constant updated.
BAGIT_PROFILE_IDENTIFIER = (
    "https://raw.githubusercontent.com/informatics-isi-edu/"
    "deriva-py/deriva-ml/deriva/bag/profiles/deriva-bag-profile.json"
)

#: Filesystem path to the bundled profile JSON document.
#:
#: Useful when validating a bag without network access — pass this
#: path as ``profile_path`` to :func:`bdbag.bdbag_api.validate_bag_profile`.
PROFILE_JSON_PATH = Path(__file__).parent / "profiles" / "deriva-bag-profile.json"


# ---------------------------------------------------------------------------
# Bag-internal path conventions
# ---------------------------------------------------------------------------

#: Path of the ERMrest schema description, relative to the bag root.
SCHEMA_JSON_PATH = "data/schema.json"

#: Relative path template for a table's CSV file. Format args:
#: ``schema`` (ERMrest schema name), ``table`` (table name).
TABLE_CSV_TEMPLATE = "data/{schema}/{table}.csv"

#: Relative path template for an asset file. Format args:
#: ``table`` (asset table name), ``rid`` (asset row's RID),
#: ``filename`` (asset's filename column value).
ASSET_FILE_TEMPLATE = "data/asset/{table}/{rid}/{filename}"

#: Top-level directory (relative to the bag root) for producer-side
#: provenance files.
METADATA_DIR = "metadata"

#: Filename inside ``metadata/`` that carries the producer's policy
#: and anchor information (when present).
PROVENANCE_FILENAME = "deriva-bag-provenance.json"


# ---------------------------------------------------------------------------
# Bag SQLAlchemy schema version
# ---------------------------------------------------------------------------

#: Schema version of the Bag SQLAlchemy layer.
#:
#: Bumped whenever the on-disk SQLite layout produced by
#: :class:`~deriva.bag.database.BagDatabase` changes in a way that an
#: older version of the code would not be able to read. The constant
#: is consulted by :func:`~deriva.bag.sqlite_helpers.ensure_schema_meta`
#: on every database open.
#:
#: The Bag SQLAlchemy layer is *derived* from a bag's CSVs at open
#: time, so bumping this version invalidates the SQLite cache but does
#: not change the bag's on-disk profile — bags remain readable, the
#: SQLite layer gets regenerated.
BAG_SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Provenance writer
# ---------------------------------------------------------------------------


def write_provenance(
    bag_dir: Path,
    *,
    producer: str,
    anchors: list[dict[str, Any]] | None = None,
    policy: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    """Write the deriva-bag provenance file under ``metadata/``.

    Provenance is the human- and machine-readable record of *how* the
    bag was produced: who built it, with what anchors, under what FK
    traversal policy. Round-tripping (catalog → bag → catalog) uses it
    to reconstruct the producer's intent when needed.

    Provenance is optional in the profile — a bag without
    ``metadata/deriva-bag-provenance.json`` is still profile-conformant,
    just less self-describing.

    Args:
        bag_dir: Path to the bag directory (the parent of ``data/``).
            The ``metadata/`` directory is created if it doesn't exist.
        producer: Short identifier for the producer that wrote the bag.
            By convention, the dotted name of the producing class —
            e.g. ``"deriva.bag.builder.BagBuilder"`` or
            ``"deriva.bag.catalog_builder.CatalogBagBuilder"``.
        anchors: Optional serialized anchor list, as ``Anchor`` model
            dumps. May be ``None`` for use cases that don't have a
            meaningful anchor concept (programmatic build, end-of-execution
            upload).
        policy: Optional serialized ``FKTraversalPolicy``, as a model
            dump dict. May be ``None`` if no traversal happened (again,
            programmatic builds).
        extra: Optional caller-supplied extension fields. Merged into
            the provenance object under an ``extra`` key — never
            overwriting the well-known keys.

    Returns:
        The path to the written provenance file.

    Example:
        >>> import json
        >>> import tempfile
        >>> from pathlib import Path
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     bag = Path(tmp) / "bag"
        ...     bag.mkdir()
        ...     out = write_provenance(
        ...         bag,
        ...         producer="example.Producer",
        ...         extra={"note": "demo"},
        ...     )
        ...     prov = json.loads(out.read_text())
        ...     (prov["producer"], prov["extra"]["note"])
        ('example.Producer', 'demo')
    """
    metadata_dir = bag_dir / METADATA_DIR
    metadata_dir.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "profile_identifier": BAGIT_PROFILE_IDENTIFIER,
        "producer": producer,
        # UTC timestamp so cross-host comparisons stay meaningful.
        "built_at": datetime.now(timezone.utc).isoformat(),
        "anchors": anchors or [],
        "policy": policy,
        "extra": extra or {},
    }

    out_path = metadata_dir / PROVENANCE_FILENAME
    # ``sort_keys=True`` makes the file content-deterministic for
    # round-trip and checksum comparisons. ``indent=2`` keeps it
    # human-readable.
    out_path.write_text(json.dumps(payload, sort_keys=True, indent=2))
    return out_path


# ---------------------------------------------------------------------------
# Archive helper
# ---------------------------------------------------------------------------


def archive_bag(bag_dir: Path, archive_path: Path | None = None) -> Path:
    """Zip a bag directory into a single ``.zip`` archive.

    The deriva-bag profile standardizes on zip archives (not tar/tgz).
    The archive is written next to the source directory by default,
    with the same stem and a ``.zip`` extension. The source directory
    is left in place.

    Args:
        bag_dir: Path to the bag directory.
        archive_path: Optional explicit output path. When ``None``,
            defaults to ``{bag_dir}.zip`` in the same parent directory.

    Returns:
        The path to the written archive.

    Raises:
        FileNotFoundError: If ``bag_dir`` does not exist or is not a
            directory.

    Example:
        >>> import tempfile
        >>> from pathlib import Path
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     bag = Path(tmp) / "demo-bag"
        ...     bag.mkdir()
        ...     (bag / "data").mkdir()
        ...     (bag / "data" / "schema.json").write_text("{}")
        ...     archive = archive_bag(bag)
        ...     archive.name
        'demo-bag.zip'
    """
    bag_dir = Path(bag_dir)
    if not bag_dir.is_dir():
        raise FileNotFoundError(f"Bag directory not found: {bag_dir}")

    if archive_path is None:
        archive_path = bag_dir.with_suffix(".zip")
    archive_path = Path(archive_path)
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    # ZIP_DEFLATED gives ~3-5x size reduction on typical CSVs and
    # schema JSON; asset files are usually already compressed (PNG,
    # JPEG, etc.) so we don't lose much when they pass through.
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # ``rglob("*")`` walks the directory recursively. We write
        # files with paths relative to the bag's *parent*, so the
        # archive unpacks to recreate the bag directory at the top
        # level (matching BDBag's expectations).
        for entry in bag_dir.rglob("*"):
            if entry.is_file():
                zf.write(entry, entry.relative_to(bag_dir.parent))

    logger.info("Archived bag %s to %s", bag_dir.name, archive_path)
    return archive_path
