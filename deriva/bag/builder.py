"""Constructive bag producer for in-memory and local-DB inputs.

:class:`BagBuilder` writes a deriva-bag profile bag from caller-
supplied schema + data + assets. Unlike :class:`CatalogBagBuilder`
(which drives the deriva-py export engine against a live catalog),
``BagBuilder`` is for the cases where the producer *has* no source
catalog:

- **Programmatic build** — cifar / Kaggle / TF-dataset cases.
  Caller supplies a schema and pandas DataFrames (or iterables of
  dict rows).
- **End-of-execution upload** — ``Execution.commit_execution``
  builds a bag from the working local_db SQLite, then hands it to
  :class:`BagCatalogLoader`.

Internals tie together pieces from elsewhere in :mod:`deriva.bag`:

- :mod:`schema_io` accepts the caller's schema (a SQLAlchemy
  ``MetaData``, deriva-py ``typed.SchemaDef`` list, or both) and
  normalizes it to ``MetaData``. ``MetaData → ERMrest JSON``
  writes ``data/schema.json``.
- :class:`SchemaBuilder` builds a SQLAlchemy ORM (Phase 1) from
  the normalized model.
- :class:`DataLoader` + :class:`CSVSink` drive the
  caller-supplied :class:`DataSource` through the ORM in
  FK-safe order, writing ``data/<schema>/<table>.csv``.
- Asset bytes are copied into ``data/asset/<table>/<rid>/<filename>``
  via :meth:`add_asset`. Caller can also call
  :meth:`add_asset_reference` to emit ``fetch.txt`` entries (the
  resulting bag is a *holey* bag rather than materialized).
- :mod:`profile` helpers write ``metadata/`` provenance and (in
  :meth:`finalize`) archive the bag as zip.
- ``bdb.make_bag`` adds the BDBag-spec scaffolding (manifests,
  ``bagit.txt``, ``bag-info.txt`` with the deriva-bag profile
  identifier).

Usage shape::

    from deriva.bag.builder import BagBuilder
    from sqlalchemy import MetaData, Column, Table, String

    metadata = MetaData()
    Table("Subject", metadata,
        Column("RID", String, primary_key=True),
        Column("Name", String),
        schema="demo",
    )

    with BagBuilder(metadata=metadata, output_dir=Path("bag")) as bb:
        bb.add_rows("Subject", [{"RID": "S1", "Name": "Alice"}])
        bb.add_asset("Image", "I1", Path("local/image.png"))
        bag_path = bb.finalize()
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Iterable

from deriva.core.ermrest_model import Model
from sqlalchemy import MetaData

from deriva.bag.loader import CSVSink, DataLoader
from deriva.bag.profile import (
    ASSET_FILE_TEMPLATE,
    BAGIT_PROFILE_IDENTIFIER,
    SCHEMA_JSON_PATH,
    archive_bag,
    write_provenance,
)
from deriva.bag.schema import SchemaBuilder
from deriva.bag.schema_io import (
    metadata_to_ermrest_json,
    typed_schema_def_to_metadata,
)
from deriva.bag.sources import DataSource, IterableDataSource

logger = logging.getLogger(__name__)


def _file_md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Stream the file at ``path`` and return its lowercase hex MD5.

    Used both for embedded asset records (so the bag's MD5
    manifest is correct without a second pass) and for
    fetch.txt-referenced assets where we have the bytes locally
    and want to commit to a checksum at reference-write time.
    """
    md5 = hashlib.md5()
    with path.open("rb") as f:
        # 1MB chunks keeps memory bounded for big assets while
        # not paying per-read syscall overhead for small ones.
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()


class BagBuilder:
    """Build a deriva-bag profile bag from in-memory inputs.

    Three input vocabularies are accepted at construction:

    - SQLAlchemy ``MetaData`` (the canonical internal form). Pass
      ``metadata=...``.
    - deriva-py ``typed.SchemaDef`` list. Pass ``schema_defs=[...]``.
      Internally normalized to ``MetaData`` via
      :func:`schema_io.typed_schema_def_to_metadata`.
    - Both — the typed defs are merged into the supplied
      ``MetaData``.

    At least one must be provided.

    Args:
        output_dir: Directory where the bag will be written. Created
            if missing. The directory's content is owned by this
            builder for the lifetime of the instance.
        metadata: Optional SQLAlchemy ``MetaData``. Provides the
            schema for the tables that will be written.
        schema_defs: Optional list of deriva-py
            ``typed.SchemaDef`` objects (or any object exposing
            ``schema_name`` / ``tables`` / ``prejson()``-style
            attributes). Merged into ``metadata`` if both are
            given, otherwise used to build a fresh ``MetaData``.
        producer: Identifier stamped into the bag's
            ``metadata/deriva-bag-provenance.json`` ``producer``
            field. Defaults to ``"deriva.bag.builder.BagBuilder"``.
        extra_provenance: Optional caller-supplied dict, merged
            into the provenance file under ``extra``.

    Raises:
        ValueError: If neither ``metadata`` nor ``schema_defs`` is
            provided.

    Example:
        >>> from sqlalchemy import MetaData, Column, Table, String
        >>> from pathlib import Path
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     md = MetaData()
        ...     Table("Subject", md,
        ...         Column("RID", String, primary_key=True),
        ...         Column("Name", String),
        ...         schema="demo",
        ...     )
        ...     out = Path(tmp) / "bag"
        ...     with BagBuilder(metadata=md, output_dir=out) as bb:
        ...         bb.add_rows("Subject", [{"RID": "S1", "Name": "A"}])
        ...         bag = bb.finalize(make_bdbag=False)
        ...     (bag / "data" / "demo" / "Subject.csv").exists()
        True
    """

    def __init__(
        self,
        output_dir: Path,
        *,
        metadata: MetaData | None = None,
        schema_defs: list[Any] | None = None,
        producer: str = "deriva.bag.builder.BagBuilder",
        extra_provenance: dict[str, Any] | None = None,
    ):
        if metadata is None and not schema_defs:
            raise ValueError(
                "BagBuilder requires either metadata= or schema_defs="
            )

        # Normalize the schema input to a SQLAlchemy ``MetaData``.
        # ``typed_schema_def_to_metadata`` accepts an optional
        # existing MetaData and merges into it, so we can support
        # both-args by passing the caller's metadata through.
        if schema_defs:
            self.metadata = typed_schema_def_to_metadata(
                schema_defs,
                metadata=metadata,
            )
        else:
            assert metadata is not None  # narrowed by the check above
            self.metadata = metadata

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.producer = producer
        self.extra_provenance = dict(extra_provenance or {})

        # Accumulate the rows for each table as the caller adds
        # them. The Sink is what eventually writes them to disk.
        # Storing them in memory first (rather than writing each
        # add_rows call out immediately) lets us run DataLoader's
        # FK-ordered walk over the whole input at finalize time.
        self._pending_rows: dict[str, list[dict[str, Any]]] = {}

        # fetch.txt-referenced assets. Keyed by URL so duplicate
        # references get deduplicated; the value is the full
        # manifest entry the make_bag call will write out.
        self._fetch_entries: dict[str, dict[str, Any]] = {}

        # Track copy-asset destinations so finalize() can compute
        # MD5s, sizes, and assert that no two add_asset calls
        # collided on the same destination path.
        self._embedded_assets: dict[Path, dict[str, Any]] = {}

        self._finalized = False

    # ------------------------------------------------------------------
    # Schema access (mostly for callers that want to introspect what
    # tables BagBuilder knows about before adding rows).
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Sorted list of ``schema.table`` names this builder knows about."""
        return sorted(self.metadata.tables.keys())

    # ------------------------------------------------------------------
    # Row addition
    # ------------------------------------------------------------------

    def add_row(self, table: str, row: dict[str, Any]) -> None:
        """Add a single row for ``table``.

        ``table`` may be a bare table name or a qualified
        ``schema.table`` string; the builder resolves it against
        the supplied metadata.

        Args:
            table: Table name.
            row: Dict keyed by column name. Extra keys are
                tolerated (the CSV sink writes only known columns);
                missing keys produce empty CSV cells.
        """
        self._check_not_finalized()
        qname = self._resolve_table(table)
        self._pending_rows.setdefault(qname, []).append(dict(row))

    def add_rows(
        self,
        table: str,
        rows: Iterable[dict[str, Any]] | Any,
    ) -> int:
        """Add many rows for ``table``.

        Accepts any iterable of dict rows (list, generator,
        DataFrame). If a pandas DataFrame is detected, it's
        materialized via ``to_dict('records')`` — caller stays
        in control of memory by deciding whether to pass a
        DataFrame or a generator.

        Args:
            table: Table name.
            rows: Iterable of dict rows, or a pandas DataFrame.

        Returns:
            Number of rows added.
        """
        self._check_not_finalized()
        qname = self._resolve_table(table)

        # Detect a DataFrame without importing pandas at module
        # import time. Anything with ``to_dict`` and ``columns``
        # attributes is treated as DataFrame-shaped — adequate
        # duck-typing for the cases that matter.
        if hasattr(rows, "to_dict") and hasattr(rows, "columns"):
            records = rows.to_dict("records")
        else:
            records = list(rows)

        bucket = self._pending_rows.setdefault(qname, [])
        for row in records:
            bucket.append(dict(row))
        return len(records)

    def _resolve_table(self, table: str) -> str:
        """Map a bare or qualified table name to a ``schema.table`` key."""
        if table in self.metadata.tables:
            return table
        # Try every metadata key for a bare-name match.
        candidates = [
            qname
            for qname in self.metadata.tables
            if qname.split(".")[-1] == table
        ]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            raise ValueError(
                f"Ambiguous table name {table!r}: matches "
                f"{sorted(candidates)}"
            )
        raise KeyError(
            f"Table {table!r} not declared in builder's metadata"
        )

    # ------------------------------------------------------------------
    # Asset handling
    # ------------------------------------------------------------------

    def add_asset(
        self,
        table: str,
        rid: str,
        source_path: Path,
        *,
        filename: str | None = None,
    ) -> None:
        """Copy an asset file into the bag at the profile-standard path.

        The destination is
        ``{output_dir}/data/asset/{table}/{rid}/{filename}``,
        where ``filename`` defaults to the source file's name.

        Args:
            table: Asset table name (e.g., ``"Image"``).
            rid: RID of the asset row this file belongs to.
            source_path: Path to the file on local disk. The
                file is copied into the bag eagerly so the bag
                can be archived without depending on the source
                still being present.
            filename: Override the destination filename. Defaults
                to ``source_path.name``.

        Raises:
            FileNotFoundError: If ``source_path`` doesn't exist.
            ValueError: If a different file was already added at
                the same destination (e.g., two callers used the
                same RID + filename pair).
        """
        self._check_not_finalized()
        source_path = Path(source_path)
        if not source_path.is_file():
            raise FileNotFoundError(
                f"Asset source not found: {source_path}"
            )
        name = filename or source_path.name
        rel = ASSET_FILE_TEMPLATE.format(
            table=table, rid=rid, filename=name
        )
        dest = self.output_dir / rel

        if dest in self._embedded_assets:
            existing = self._embedded_assets[dest]
            if existing["source_path"] != source_path:
                raise ValueError(
                    f"Asset destination {dest} already populated from a "
                    f"different source: existing={existing['source_path']}, "
                    f"new={source_path}"
                )
            # Same source, same dest — idempotent re-add.
            return

        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest)
        # Record the asset so finalize() can compute the bag's
        # checksum manifests correctly. MD5 is deferred until
        # finalize to keep add_asset fast in tight loops.
        self._embedded_assets[dest] = {
            "table": table,
            "rid": rid,
            "filename": name,
            "source_path": source_path,
            "rel_path": rel,
        }

    def add_assets(
        self,
        table: str,
        mapping: dict[str, Path],
        *,
        filenames: dict[str, str] | None = None,
    ) -> int:
        """Bulk-add assets for one table.

        Args:
            table: Asset table name.
            mapping: ``{rid: source_path, ...}``.
            filenames: Optional ``{rid: filename, ...}`` overriding
                each asset's destination filename. RIDs absent
                from this dict default to the source path's name.

        Returns:
            Number of assets added.
        """
        self._check_not_finalized()
        filenames = filenames or {}
        for rid, source in mapping.items():
            self.add_asset(
                table,
                rid,
                Path(source),
                filename=filenames.get(rid),
            )
        return len(mapping)

    def add_asset_reference(
        self,
        *,
        table: str,
        rid: str,
        filename: str,
        url: str,
        length: int,
        md5: str | None = None,
    ) -> None:
        """Record a fetch.txt reference instead of embedding the bytes.

        Produces a *holey* bag — the consumer must call
        ``bdb.materialize(bag)`` to download the referenced file
        before reading it. Useful for very large assets where
        bag size matters more than self-containedness.

        Args:
            table: Asset table name.
            rid: RID of the asset row.
            filename: Destination filename inside the bag (the
                path under ``data/asset/{table}/{rid}/``).
            url: Remote URL of the asset bytes.
            length: File size in bytes. Required by BDBag's
                fetch.txt format.
            md5: Optional MD5 in lowercase hex. Strongly
                recommended for integrity checking at fetch time.

        Raises:
            ValueError: If a different ``(url, filename)`` was
                already added under this RID/table.
        """
        self._check_not_finalized()
        rel = ASSET_FILE_TEMPLATE.format(
            table=table, rid=rid, filename=filename
        )
        if url in self._fetch_entries:
            existing = self._fetch_entries[url]
            if existing["filename"] != rel:
                raise ValueError(
                    f"URL {url} already referenced under a different "
                    f"path: existing={existing['filename']}, new={rel}"
                )
            return

        entry: dict[str, Any] = {
            "url": url,
            "length": int(length),
            "filename": rel,
        }
        if md5:
            entry["md5"] = md5
        self._fetch_entries[url] = entry

    # ------------------------------------------------------------------
    # Schema-from-source helper
    # ------------------------------------------------------------------

    def write_from_source(
        self,
        source: DataSource,
        *,
        tables: list[str] | None = None,
        batch_size: int = 1000,
    ) -> dict[str, int]:
        """Pump rows from a :class:`DataSource` into the pending buffers.

        Convenience for the LocalDB / DataFrame / Iterable cases
        where the caller would otherwise loop and call
        :meth:`add_rows` per table. Internally builds an in-memory
        :class:`SchemaORM` and runs :class:`DataLoader` with a
        custom sink that routes rows into ``_pending_rows`` instead
        of CSVs (the CSV write happens later, in :meth:`finalize`,
        once we know the full FK-safe order).

        Args:
            source: Any :class:`DataSource` implementation.
            tables: Optional explicit subset; defaults to every
                table the source reports that's also in the
                builder's metadata.
            batch_size: Rows per source-iteration batch.

        Returns:
            ``{qualified_table: rows_added}``.
        """
        self._check_not_finalized()

        # Reach the available-tables intersection ourselves so we
        # avoid building a full SchemaORM for a no-op call.
        available = set(source.list_available_tables())
        known = set(self.metadata.tables.keys())
        # Match available against known via both qualified and
        # bare names.
        candidates: list[str] = []
        if tables is not None:
            for t in tables:
                qname = self._resolve_table(t)
                candidates.append(qname)
        else:
            for qname in known:
                bare = qname.split(".")[-1]
                if qname in available or bare in available:
                    candidates.append(qname)

        counts: dict[str, int] = {}
        for qname in candidates:
            bare = qname.split(".")[-1]
            count = self.add_rows(
                qname, source.get_table_data(bare)
            )
            counts[qname] = count
        return counts

    # ------------------------------------------------------------------
    # Finalize
    # ------------------------------------------------------------------

    def finalize(
        self,
        *,
        make_bdbag: bool = True,
        archive: bool = False,
        record_provenance: bool = True,
    ) -> Path:
        """Flush pending rows to CSVs, write provenance, make the bag.

        The full finalize sequence:

        1. Write ``data/schema.json`` from the metadata.
        2. Run :class:`DataLoader` + :class:`CSVSink` over the
           pending rows in FK-safe order; this writes
           ``data/<schema>/<table>.csv`` for every table that
           received row additions.
        3. (Optional) Write the provenance file under
           ``metadata/``.
        4. (Optional) If any ``add_asset_reference`` calls were
           made, write the remote-file-manifest JSONL beside the
           bag.
        5. (Optional) Call ``bdb.make_bag`` to add BDBag scaffolding
           (manifests, ``bagit.txt``, ``bag-info.txt`` with the
           deriva-bag profile identifier).
        6. (Optional) Archive the bag as zip.

        After finalize, the builder is closed — further calls to
        ``add_row`` / ``add_rows`` / ``add_asset`` raise.

        Args:
            make_bdbag: When ``True`` (default), call
                ``bdb.make_bag``. Disable for tests that only want
                to inspect the data/ subtree, or for downstream
                tooling that owns the BDBag step itself.
            archive: When ``True``, archive the bag as
                ``{output_dir}.zip``. Default ``False``.
            record_provenance: When ``True`` (default), write the
                deriva-bag-provenance.json file under ``metadata/``.

        Returns:
            Path to the bag directory (or, when ``archive=True``,
            still the directory — the archive lives next to it
            with the same stem and ``.zip`` extension).
        """
        self._check_not_finalized()
        try:
            self._write_schema_json()
            self._write_pending_rows()
            if record_provenance:
                self._write_provenance_file()
            if make_bdbag:
                self._make_bdbag()
            if archive:
                archive_bag(self.output_dir)
        finally:
            self._finalized = True
        return self.output_dir

    def _write_schema_json(self) -> None:
        """Write ``data/schema.json`` from the metadata."""
        doc = metadata_to_ermrest_json(self.metadata)
        # Constructive bags have no source-catalog snapshot, but the
        # ``snaptime`` field is part of the ERMrest wire format.
        # metadata_to_ermrest_json defaults it to None; that's fine.
        out = self.output_dir / SCHEMA_JSON_PATH
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(doc, indent=2, sort_keys=True))

    def _write_pending_rows(self) -> None:
        """Run DataLoader+CSVSink over the pending row buffers.

        We build a fresh in-memory :class:`SchemaORM` purely to
        get the FK-ordering machinery; no rows are inserted into
        the SchemaORM's SQLite. The CSV writes happen via the
        sink directly off the iterables.
        """
        if not self._pending_rows:
            return

        # Build an in-memory ORM so the DataLoader has somewhere
        # to ask "what's the FK order?". The actual writes go to
        # CSVSink.
        model = self._metadata_as_model()
        schemas = sorted(
            {qname.split(".")[0] for qname in self.metadata.tables}
        )
        with ExitStack() as stack:
            orm = stack.enter_context(
                SchemaBuilder(model, schemas, database_path=":memory:").build()
            )
            sink = stack.enter_context(CSVSink(self.output_dir, model))
            # Re-key the pending rows into bare table names so
            # IterableDataSource can find them by either form.
            bare_rows = {
                qname.split(".")[-1]: rows
                for qname, rows in self._pending_rows.items()
            }
            source = IterableDataSource(bare_rows)
            loader = DataLoader(orm, source, sink=sink)
            loader.load_tables(list(self._pending_rows.keys()))

    def _metadata_as_model(self) -> Model:
        """Turn our SQLAlchemy MetaData into an ERMrest Model."""
        # Round-trip through ERMrest JSON. This is mildly wasteful
        # (we already have a MetaData!) but it gives the
        # SchemaBuilder + DataLoader machinery the deriva-py Model
        # they expect. A direct MetaData→Model converter could
        # replace this if profile coverage shows it matters.
        doc = metadata_to_ermrest_json(self.metadata)
        # Model.fromfile needs a file path; write to a temp file.
        import tempfile

        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False
        ) as f:
            json.dump(doc, f)
            path = f.name
        return Model.fromfile("file-system", path)

    def _write_provenance_file(self) -> None:
        """Write ``metadata/deriva-bag-provenance.json``."""
        write_provenance(
            self.output_dir,
            producer=self.producer,
            anchors=None,  # constructive bags have no anchors
            policy=None,
            extra=self.extra_provenance,
        )

    def _make_bdbag(self) -> None:
        """Invoke ``bdb.make_bag`` to add manifest scaffolding."""
        # Local import — bdbag is a non-trivial dependency, and
        # callers using ``finalize(make_bdbag=False)`` shouldn't
        # have to install it.
        from bdbag import bdbag_api as bdb

        remote_manifest_path = self._write_remote_manifest_if_any()

        # Stamp the deriva-bag profile identifier into
        # bag-info.txt so consumers can validate against it.
        bag_metadata = {
            "BagIt-Profile-Identifier": BAGIT_PROFILE_IDENTIFIER,
        }

        try:
            bdb.make_bag(
                str(self.output_dir),
                metadata=bag_metadata,
                remote_file_manifest=remote_manifest_path,
                update=True,
                idempotent=True,
            )
        finally:
            # We wrote the remote manifest beside the bag (where
            # callers can inspect it for debugging) but make_bag
            # has already consumed it; leave it in place.
            pass

    def _write_remote_manifest_if_any(self) -> str | None:
        """Write the JSONL remote-file manifest, if any references exist.

        Returns the path to the manifest file, or None if no
        references were added.
        """
        if not self._fetch_entries:
            return None
        path = self.output_dir / "remote-file-manifest.json"
        with path.open("w", encoding="utf-8") as f:
            for entry in self._fetch_entries.values():
                f.write(json.dumps(entry) + "\n")
        logger.info(
            "BagBuilder: wrote %d remote-file-manifest entries to %s",
            len(self._fetch_entries),
            path,
        )
        return str(path)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _check_not_finalized(self) -> None:
        if self._finalized:
            raise RuntimeError(
                "BagBuilder has been finalized; further mutations "
                "are not allowed"
            )

    def __enter__(self) -> "BagBuilder":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Don't auto-finalize — caller may have aborted mid-build
        # and not want a half-built bag to land. Just release any
        # outstanding resources (currently none beyond what
        # finalize would do).
        return False


__all__ = ["BagBuilder"]
