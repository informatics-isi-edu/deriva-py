"""Content-addressed cache index for downloaded / built bags.

The deriva-bag pipeline keeps a per-host/catalog cache of bag
directories on disk. Bags are *content-addressed* — the on-disk
directory name is the bag's BDBag checksum, not any anchoring
RID. This works uniformly across the three producer cases:

- A bag rooted in one RID (today's dataset-download case).
- A bag rooted in many RIDs (a multi-RID slice or an
  end-of-execution upload that touched several tables).
- A bag with no anchoring RID (a catalog-wide clone).

The directory name is content-addressed, but callers still ask
RID-shaped questions ("what bags have I cached for dataset X?")
so we need a *reverse index* alongside the directories. This
module owns that index.

On-disk layout::

    {cache_root}/
        index.sqlite                          # this module
        bags/
            {checksum_1}/
                bag/                          # the bag directory
                db/main.db                    # Bag SQLAlchemy main file
                db/{schema}.db                # attached per-schema files
            {checksum_2}/
                ...

Index schema (versioned via ``schema_meta``):

- ``bags(checksum, profile_id, built_at, anchor_summary_json,
  size_bytes)`` — one row per cached bag.
- ``bag_anchor_rids(checksum, table, rid)`` — flat reverse index
  from anchor RIDs to bag checksums. Lets callers ask
  ``SELECT checksum FROM bag_anchor_rids WHERE table='Dataset'
  AND rid=?`` in one query — replaces the ``{rid}_*`` directory
  glob that today's :mod:`deriva_ml.dataset.bag_cache` uses.

The index uses :func:`deriva.bag.sqlite_helpers.create_wal_engine`
so it inherits the same WAL + pragma policy as every other SQLite
file in :mod:`deriva.bag`, including concurrent-reader safety.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from deriva.bag.sqlite_helpers import (
    SchemaVersionError,
    create_wal_engine,
    ensure_schema_meta,
)

logger = logging.getLogger(__name__)


#: Schema version for the cache index itself.
#:
#: Bumped whenever the ``bags`` / ``bag_anchor_rids`` tables
#: change in a backward-incompatible way. Independent of
#: ``BAG_SCHEMA_VERSION`` (which governs the per-bag SQLite
#: layout) because the two layers evolve separately.
CACHE_INDEX_SCHEMA_VERSION = 1


_DDL_BAGS = """
CREATE TABLE IF NOT EXISTS bags (
    checksum            TEXT PRIMARY KEY,
    profile_id          TEXT,
    built_at            TEXT NOT NULL,
    anchor_summary_json TEXT,
    size_bytes          INTEGER
)
"""

_DDL_ANCHORS = """
CREATE TABLE IF NOT EXISTS bag_anchor_rids (
    checksum TEXT NOT NULL,
    "table"  TEXT NOT NULL,
    rid      TEXT NOT NULL,
    PRIMARY KEY (checksum, "table", rid),
    FOREIGN KEY (checksum) REFERENCES bags(checksum) ON DELETE CASCADE
)
"""

# Speeds up the "which bags reference this RID?" query that
# replaces the old directory-glob lookup.
_DDL_ANCHORS_LOOKUP_IDX = """
CREATE INDEX IF NOT EXISTS bag_anchor_rids_table_rid_idx
ON bag_anchor_rids ("table", rid)
"""


class BagCacheIndex:
    """SQLite-backed index of cached bags, keyed by checksum.

    Each :class:`BagCacheIndex` owns one ``index.sqlite`` file
    inside a host/catalog cache root. The bag directories
    themselves live under ``{cache_root}/bags/{checksum}/``;
    the index records metadata plus the reverse-lookup table from
    anchor RIDs to checksums.

    Args:
        cache_root: Directory that holds ``index.sqlite`` and the
            ``bags/`` subtree. Created if it doesn't exist.

    Example:
        >>> import tempfile
        >>> from pathlib import Path
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     idx = BagCacheIndex(Path(tmp))
        ...     idx.record(
        ...         checksum="abc123",
        ...         profile_id="https://example.org/profile.json",
        ...         anchors=[("Dataset", "D1")],
        ...     )
        ...     idx.find_bags_for_rid(table="Dataset", rid="D1")
        ['abc123']
    """

    def __init__(self, cache_root: Path):
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self._engine: Engine = create_wal_engine(
            self.cache_root / "index.sqlite"
        )
        # Schema versioning + table creation. ensure_schema_meta
        # raises SchemaVersionError if the on-disk version is
        # higher than what this code understands.
        try:
            ensure_schema_meta(
                self._engine,
                expected_version=CACHE_INDEX_SCHEMA_VERSION,
            )
        except SchemaVersionError:
            self._engine.dispose()
            raise
        self._create_tables()

    @property
    def bags_dir(self) -> Path:
        """Directory holding the per-bag ``{checksum}/`` subdirectories."""
        return self.cache_root / "bags"

    def bag_dir_for(self, checksum: str) -> Path:
        """Compute the bag directory path for a checksum.

        The directory may or may not exist on disk — call
        :meth:`record` to add metadata for it, or
        :meth:`is_cached` to test.
        """
        return self.bags_dir / checksum

    def _create_tables(self) -> None:
        """Ensure the index tables exist."""
        with self._engine.begin() as conn:
            conn.execute(text(_DDL_BAGS))
            conn.execute(text(_DDL_ANCHORS))
            conn.execute(text(_DDL_ANCHORS_LOOKUP_IDX))

    # ------------------------------------------------------------------
    # Record / forget
    # ------------------------------------------------------------------

    def record(
        self,
        *,
        checksum: str,
        profile_id: str | None = None,
        anchors: Iterable[tuple[str, str]] = (),
        anchor_summary: dict[str, Any] | None = None,
        size_bytes: int | None = None,
        built_at: datetime | None = None,
    ) -> None:
        """Record a bag in the index.

        Idempotent: re-recording a bag with the same checksum
        updates the metadata and *replaces* the anchor list
        (because the bag's contents are content-addressed; if the
        anchor RIDs differ for the same checksum, the most recent
        producer's claim wins).

        Args:
            checksum: The bag's BDBag checksum. Used as the
                content-addressed identifier.
            profile_id: Optional BagIt-Profile-Identifier URL. Helps
                future tooling tell deriva-bag profile bags apart
                from other BDBag flavors.
            anchors: An iterable of ``(table, rid)`` pairs to put
                in the reverse index. May be empty.
            anchor_summary: Optional dict (e.g., a serialized
                ``Anchor`` list) recorded as JSON for provenance.
            size_bytes: Optional bag size on disk. Useful for
                cache eviction strategies.
            built_at: Optional explicit timestamp. Defaults to
                ``datetime.now(timezone.utc)``.
        """
        when = (built_at or datetime.now(timezone.utc)).isoformat()
        anchor_json = (
            json.dumps(anchor_summary, sort_keys=True)
            if anchor_summary is not None
            else None
        )
        # Materialize the anchors iterable up front so we can use
        # it twice (in the executemany below and in error
        # diagnostics if anything fails).
        anchor_rows = [
            {"checksum": checksum, "table": t, "rid": r}
            for t, r in anchors
        ]
        with self._engine.begin() as conn:
            # Upsert the bags row.
            conn.execute(
                text(
                    "INSERT INTO bags "
                    "(checksum, profile_id, built_at, anchor_summary_json, size_bytes) "
                    "VALUES (:checksum, :profile_id, :built_at, :anchor_summary_json, :size_bytes) "
                    "ON CONFLICT(checksum) DO UPDATE SET "
                    "profile_id = excluded.profile_id, "
                    "built_at = excluded.built_at, "
                    "anchor_summary_json = excluded.anchor_summary_json, "
                    "size_bytes = excluded.size_bytes"
                ),
                {
                    "checksum": checksum,
                    "profile_id": profile_id,
                    "built_at": when,
                    "anchor_summary_json": anchor_json,
                    "size_bytes": size_bytes,
                },
            )
            # Replace anchors: delete the existing rows and re-insert.
            # The FK ON DELETE CASCADE would handle this, but we
            # want to keep the bags row stable across re-records, so
            # we delete only the anchor rows.
            conn.execute(
                text(
                    'DELETE FROM bag_anchor_rids WHERE checksum = :c'
                ),
                {"c": checksum},
            )
            if anchor_rows:
                conn.execute(
                    text(
                        "INSERT INTO bag_anchor_rids "
                        '(checksum, "table", rid) '
                        "VALUES (:checksum, :table, :rid)"
                    ),
                    anchor_rows,
                )

    def forget(self, checksum: str) -> bool:
        """Remove a bag's index entry. Returns True if a row was deleted.

        Does *not* remove the on-disk bag directory or
        SQLAlchemy database files — caller is responsible for
        that (and for the careful ordering: forget the index
        before rm-rf'ing the directory so the index doesn't
        outlive its referent).
        """
        with self._engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM bags WHERE checksum = :c"),
                {"c": checksum},
            )
            # SQLite reports affected rows via rowcount.
            return bool(result.rowcount)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def is_cached(self, checksum: str) -> bool:
        """Return ``True`` if a bag with this checksum is in the index."""
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM bags WHERE checksum = :c LIMIT 1"
                ),
                {"c": checksum},
            ).first()
        return row is not None

    def get(self, checksum: str) -> dict[str, Any] | None:
        """Return the index metadata for a bag, or ``None`` if missing.

        The returned dict carries ``checksum``, ``profile_id``,
        ``built_at``, ``anchor_summary`` (parsed back from JSON),
        and ``size_bytes``.
        """
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT checksum, profile_id, built_at, "
                    "anchor_summary_json, size_bytes "
                    "FROM bags WHERE checksum = :c"
                ),
                {"c": checksum},
            ).mappings().first()
        if row is None:
            return None
        record = dict(row)
        if record.get("anchor_summary_json"):
            record["anchor_summary"] = json.loads(
                record["anchor_summary_json"]
            )
        else:
            record["anchor_summary"] = None
        record.pop("anchor_summary_json", None)
        return record

    def find_bags_for_rid(self, *, table: str, rid: str) -> list[str]:
        """Reverse-lookup: what checksums name this RID as an anchor?

        Replaces the ``{rid}_*`` directory glob of the old
        single-RID-per-bag layout.

        Args:
            table: ERMrest table name (bare, no schema prefix).
            rid: RID value.

        Returns:
            List of checksums (most recent first by ``built_at``).
            Empty list if no bag in the index claims this RID.
        """
        with self._engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT b.checksum FROM bag_anchor_rids r "
                    "JOIN bags b ON b.checksum = r.checksum "
                    'WHERE r."table" = :t AND r.rid = :r '
                    "ORDER BY b.built_at DESC"
                ),
                {"t": table, "r": rid},
            ).all()
        return [r[0] for r in rows]

    def list_bags(self) -> list[dict[str, Any]]:
        """Return every bag in the index, most-recently-built first."""
        with self._engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT checksum, profile_id, built_at, "
                    "anchor_summary_json, size_bytes "
                    "FROM bags ORDER BY built_at DESC"
                )
            ).mappings().all()
        out: list[dict[str, Any]] = []
        for row in rows:
            record = dict(row)
            if record.get("anchor_summary_json"):
                record["anchor_summary"] = json.loads(
                    record["anchor_summary_json"]
                )
            else:
                record["anchor_summary"] = None
            record.pop("anchor_summary_json", None)
            out.append(record)
        return out

    def total_size_bytes(self) -> int:
        """Sum of ``size_bytes`` across every bag in the index.

        Returns 0 when the index is empty or no bag has a recorded
        size. Useful for cache-eviction policies.
        """
        with self._engine.connect() as conn:
            value = conn.execute(
                text(
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM bags"
                )
            ).scalar()
        return int(value or 0)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def dispose(self) -> None:
        """Release the SQLAlchemy engine. Idempotent."""
        if self._engine is not None:
            self._engine.dispose()

    def __enter__(self) -> "BagCacheIndex":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.dispose()
        return False


__all__ = ["BagCacheIndex", "CACHE_INDEX_SCHEMA_VERSION"]
