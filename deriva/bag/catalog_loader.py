"""Bag → catalog loader.

:class:`BagCatalogLoader` is the third piece of the bag pipeline
(after :class:`BagBuilder` and :class:`CatalogBagBuilder`). It
takes a deriva-bag profile bag and writes its contents into a
destination ERMrest catalog: rows in FK-safe order, asset bytes
via deriva-py's existing upload recipe, dangling-FK resolution
per the policy.

Two surfaces:

- :meth:`run` (sync) — drives the load and returns a
  :class:`LoadReport`.
- :meth:`arun` (async) — same shape, returns the same report.
  ``run()`` is a thin wrapper around ``arun()`` for callers in
  sync contexts.

The loader does **not** implement its own Hatrac upload — it
hands each asset row to
:meth:`~deriva.transfer.upload.deriva_upload.DerivaUpload._hatracUpload`,
which picks chunked vs. single-PUT based on file size and (when
the destination already has a matching MD5) skips byte transfer
via :meth:`HatracStore.put_loc`'s built-in HEAD-then-PUT path.
Server-side dedup is therefore transparent to the loader:
:attr:`TableLoadStats.assets_attempted` counts upload invocations
only; whether bytes actually transferred is decided downstream.

Cross-process transfer-state resumption — the persistent
``.deriva-upload-state-*.json`` machinery in
:class:`~deriva.transfer.upload.deriva_upload.DerivaUpload` — is
**not** wired up here. The bag-loader is a one-shot driver; the
state-file overhead would buy resume-on-crash at the cost of a
file lock that would serialize concurrent loads to the same bag.
A future caller that needs resume can opt in by populating
``self._uploader.transfer_state`` (initialised to ``{}`` by
:meth:`DerivaUpload.minimal_for_upload`, which the loader's
:meth:`_get_uploader` calls) and then invoking
:meth:`DerivaUpload.loadTransferState` before :meth:`run`.

Catalog row insertion is the loader's own responsibility, not
the uploader's: row writes route through deriva-py's
:meth:`~deriva.core.datapath._TableWrapper.insert` (obtained via
:meth:`ErmrestCatalog.getPathBuilder`) in :meth:`_insert_rows`,
with row reconciliation, RID remap, and dangling-FK handling
layered in by the table-class machinery
(:meth:`_load_vocabulary_table`,
:meth:`_load_match_by_columns_table`,
:meth:`_load_content_table`).

For non-asset tables, the loader walks the bag's SQLAlchemy ORM
(via :class:`BagDatabase`) in :class:`ForeignKeyOrderer`-computed
order and inserts rows via ERMrest's bulk endpoint. The
``dangling_fk_strategy`` field on :class:`FKTraversalPolicy`
governs how rows whose FK targets are missing get handled
(``FAIL`` / ``DELETE`` / ``NULLIFY``).

Bag-state validation: :meth:`FKTraversalPolicy.validate_with_bag_state`
is called *before* any rows are inserted, rejecting the
``holey + UPLOAD_*`` combinations with a clear error pointing at
``bdb.materialize(bag)`` or the ``ROWS_ONLY`` workaround.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from deriva.core import ErmrestCatalog
from deriva.core.ermrest_model import Table as DerivaTable

from deriva.bag.database import BagDatabase
from deriva.bag.loader import ForeignKeyOrderer
from deriva.bag.profile import HATRAC_PATH_PREFIX
from deriva.bag.traversal import (
    DEFAULT_EXCLUDE_SCHEMAS,
    AssetMode,
    ContentConflictStrategy,
    DanglingFKStrategy,
    FKTraversalPolicy,
)


class TableClass(StrEnum):
    """How :class:`BagCatalogLoader` should treat each in-scope table.

    Determined at the start of the load by :meth:`_classify_table`
    from the bag's schema model and the active policy:

    - ``VOCABULARY``: table looks like a controlled vocabulary
      (has the canonical ``ID``/``URI``/``Name``/``Description``/
      ``Synonyms`` columns per
      :meth:`~deriva.core.ermrest_model.Table.is_vocabulary`).
      Reconciled by ``Name`` against the destination; existing rows
      contribute a source-RID → destination-RID entry to the
      loader's remap so child rows can be rewritten.
    - ``MATCH_BY_COLUMNS``: table is listed in
      :attr:`FKTraversalPolicy.match_by_columns`. Reconciled by
      the supplied column list (e.g. ``["URL"]`` for content-
      addressed asset tables); same remap-and-rewrite shape as
      vocab, just driven by a caller-supplied key instead of the
      hard-coded ``Name``.
    - ``CONTENT``: every other in-scope table. Inserted by RID;
      collisions resolved per ``policy.content_on_conflict``.

    ``match_by_columns`` takes precedence over the vocabulary
    structural check: explicit caller intent overrides implicit
    structural classification. (A table satisfying
    ``is_vocabulary()`` is rarely in ``match_by_columns`` anyway —
    if the caller bothered to name custom match columns, they
    almost certainly mean to use them.)

    System schemas and out-of-bag schemas are filtered out earlier
    (by :meth:`_table_in_scope`) and never reach the classifier.
    """

    VOCABULARY = "vocabulary"
    MATCH_BY_COLUMNS = "match_by_columns"
    CONTENT = "content"

logger = logging.getLogger(__name__)


# =============================================================================
# Report
# =============================================================================


@dataclass
class TableLoadStats:
    """Per-table load outcome."""

    table: str
    """Qualified table name (``schema.table``)."""

    rows_inserted: int = 0
    """Number of rows ERMrest accepted."""

    rows_skipped_orphan: int = 0
    """Rows dropped because they had dangling FK references
    (only nonzero when ``dangling_fk_strategy == DELETE``)."""

    rows_nullified_orphan: int = 0
    """Rows whose dangling FK columns were set to NULL (only
    nonzero when ``dangling_fk_strategy == NULLIFY``)."""

    assets_attempted: int = 0
    """Number of asset rows for which :meth:`_hatracUpload` was
    invoked. The deriva-py uploader's HEAD-then-PUT path (inside
    :meth:`HatracStore.put_loc` when ``chunked=True``) decides
    server-side whether to transfer bytes or skip-on-MD5-match;
    the loader does not distinguish the two outcomes in this
    counter. Zero in ``ROWS_ONLY`` mode (asset bytes aren't pushed
    at all)."""

    rows_matched_by_name: int = 0
    """Vocabulary rows that already existed on the destination
    (matched by ``Name``); the bag's source RID was remapped to the
    destination's RID for any child rows that reference it.
    Always zero for non-vocabulary tables."""

    rows_matched_by_columns: int = 0
    """Rows reconciled via
    :attr:`FKTraversalPolicy.match_by_columns` — i.e., a row in the
    bag whose match-column values already exist on the destination.
    Same remap-and-rewrite semantics as ``rows_matched_by_name``,
    driven by a caller-supplied unique key instead of the hard-coded
    ``Name``. Always zero for tables not listed in
    ``match_by_columns``."""

    rows_skipped_on_conflict: int = 0
    """Content rows whose RID already existed on the destination,
    skipped per ``policy.content_on_conflict='skip_by_rid'``.
    Always zero with ``FAIL`` (which raises instead)."""


@dataclass
class LoadReport:
    """Result of a :meth:`BagCatalogLoader.run` / ``arun`` call."""

    bag_path: Path
    """Path to the bag that was loaded."""

    catalog_id: str
    """Catalog ID of the destination."""

    table_stats: dict[str, TableLoadStats] = field(default_factory=dict)
    """Per-table outcome, keyed by qualified table name."""

    @property
    def total_rows_inserted(self) -> int:
        return sum(s.rows_inserted for s in self.table_stats.values())

    @property
    def total_orphans_handled(self) -> int:
        return sum(
            s.rows_skipped_orphan + s.rows_nullified_orphan
            for s in self.table_stats.values()
        )


# =============================================================================
# BagCatalogLoader
# =============================================================================


class BagCatalogLoader:
    """Load a bag into a destination ERMrest catalog.

    Args:
        catalog: Destination :class:`ErmrestCatalog`. Must be
            writable; the loader inserts rows and (in non-
            ``ROWS_ONLY`` asset modes) uploads asset bytes.
        bag: Path to the bag directory or a pre-opened
            :class:`BagDatabase`.
        policy: :class:`FKTraversalPolicy`. Defaults to a sensible
            default.
        database_dir: Where :class:`BagDatabase` should put its
            SQLite cache. Defaults to ``{bag.parent}/.bag-db``.

    Example:
        Synchronous load with default policy::

            from pathlib import Path
            from deriva.bag.catalog_loader import BagCatalogLoader

            loader = BagCatalogLoader(
                catalog=dest_catalog,
                bag=Path("/tmp/my-bag"),
            )
            report = loader.run()
            print(f"Loaded {report.total_rows_inserted} rows")

        Async load::

            report = await loader.arun()
    """

    def __init__(
        self,
        *,
        catalog: ErmrestCatalog,
        bag: Path | BagDatabase,
        policy: FKTraversalPolicy | None = None,
        database_dir: Path | None = None,
    ):
        self.catalog = catalog
        self.policy = policy or FKTraversalPolicy()

        # Accept either an already-open BagDatabase or a bag path
        # to open. The latter is more convenient for scripts; the
        # former lets callers reuse a single BagDatabase across
        # multiple operations (read + load).
        self._owned_bag_db = False
        if isinstance(bag, BagDatabase):
            self.bag_db = bag
            self.bag_path = bag.bag_path
        else:
            self.bag_path = Path(bag)
            db_dir = (
                database_dir
                if database_dir is not None
                else self.bag_path.parent / ".bag-db"
            )
            db_dir.mkdir(parents=True, exist_ok=True)
            self.bag_db = BagDatabase(
                bag_path=self.bag_path,
                database_dir=db_dir,
                schemas=self._infer_schemas_from_bag(self.bag_path),
            )
            self._owned_bag_db = True

        # Detect "holey" state: a fetch.txt referencing files
        # whose local presence has not been materialized.
        self.holey = self._is_holey()

        # Validate the bag-state × asset-mode matrix before any
        # work. Fails fast with a clear message — no point making
        # the user wait through schema setup to find out the
        # mode is incompatible.
        self.policy.validate_with_bag_state(holey=self.holey)

        # RID remap: source-catalog RID → destination-catalog RID,
        # keyed by (schema, table). Populated as vocabulary rows are
        # reconciled (match-by-name); consumed during content-row
        # insertion to rewrite FK columns that reference vocab rows.
        # Identity entries are recorded too so callers can introspect
        # which vocab rows survived as-is. See ADR-0001.
        self._rid_remap: dict[tuple[str, str], dict[str, str]] = {}

        # PathBuilder is lazily built on first read/write.
        # ``ErmrestCatalog.getPathBuilder()`` walks /schema once;
        # cache the result so subsequent reads/writes reuse the
        # same wrapper tree.
        #
        # First build asks the catalog for a refreshed wrapper
        # (``refresh=True``). The catalog-instance cache may have
        # been warmed by an earlier caller in this same process,
        # *before* the destination schema was extended (e.g. a
        # ``create_asset`` between the catalog connect and the
        # bag load). Without ``refresh=True`` here, the loader
        # inherits that stale snapshot and ``schemas[s].tables[t]``
        # raises ``KeyError`` for any table added since.
        self._path_builder = None

    def _ensure_path_builder(self) -> Any:
        """Lazily build (and refresh) the loader's path-builder wrapper.

        The first call asks the catalog for ``refresh=True`` so
        the wrapper reflects the destination's live schema at
        load time, not whatever earlier caller warmed the cache.
        Subsequent calls in the same load reuse the wrapper.
        """
        if self._path_builder is None:
            self._path_builder = self.catalog.getPathBuilder(refresh=True)
        return self._path_builder

    def _table_wrapper(self, table: DerivaTable):
        """Return the deriva-py ``_TableWrapper`` for ``table``.

        Wraps the catalog's lazy PathBuilder so the loader's
        insert/update/read calls go through deriva-py's
        URL-encoding, batching, and retry machinery instead of
        hand-rolled paths.
        """
        pb = self._ensure_path_builder()
        return pb.schemas[table.schema.name].tables[table.name]

    @staticmethod
    def _infer_schemas_from_bag(bag_path: Path) -> list[str]:
        """Peek at the bag's schema.json and return loadable schemas.

        The export engine serializes the *entire* source catalog
        model into ``schema.json``, including ERMrest's structural
        schemas (``public``, ``WWW``, ``_acl_admin``). The walker
        skips those tables when building the bag, so they carry no
        data, but their schema sections are still present. Loading
        them produces SQLAlchemy automap classes with no usable
        primary key — which then crashes the cross-schema FK loop
        with ``'NoneType' has no attribute '__table__'``.

        Filter the system schemas out here so the loader only ever
        sees user-content schemas.
        """
        schema_file = bag_path / "data" / "schema.json"
        if not schema_file.exists():
            return []
        with schema_file.open() as f:
            doc = json.load(f)
        return [
            name
            for name in doc.get("schemas", {})
            if name not in DEFAULT_EXCLUDE_SCHEMAS
        ]

    def _is_holey(self) -> bool:
        """Return True if the bag has unresolved fetch.txt entries."""
        fetch_file = self.bag_path / "fetch.txt"
        if not fetch_file.exists():
            return False
        with fetch_file.open() as f:
            for row in f:
                fields = row.split("\t")
                if len(fields) >= 3:
                    local = fields[2].strip()
                    local_path = self.bag_path / local
                    if not local_path.exists():
                        return True
        return False

    # ------------------------------------------------------------------
    # Public entry points — run / arun
    # ------------------------------------------------------------------

    def run(self) -> LoadReport:
        """Synchronously load the bag and return the :class:`LoadReport`.

        Thin wrapper around :meth:`arun`. Detects whether the caller
        is already inside a running event loop — that's the common
        case in Jupyter/papermill kernels, where the kernel itself
        owns the main-thread loop — and re-enters it via
        :mod:`nest_asyncio`. Outside a loop, falls back to
        :func:`asyncio.run`.

        Callers in already-async contexts should call :meth:`arun`
        directly to avoid the nested-loop overhead.

        Raises:
            ImportError: only when running inside a notebook kernel
                **and** :mod:`nest_asyncio` is not installed.
                ``nest_asyncio`` is a runtime soft dependency — the
                bag-loader is import-safe without it; the cost only
                appears for in-notebook callers.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            # Notebook context: re-enter the active loop.
            # ``nest_asyncio`` is imported lazily so the module
            # stays importable without it.
            import nest_asyncio

            nest_asyncio.apply()
            return loop.run_until_complete(self.arun())
        return asyncio.run(self.arun())

    async def arun(self) -> LoadReport:
        """Async entry point. Drives the full load pipeline."""
        report = LoadReport(
            bag_path=self.bag_path,
            catalog_id=str(self.catalog.catalog_id),
        )

        # Compute the FK-safe table order from the bag's model.
        # We use the model embedded in the BagDatabase (parsed
        # from data/schema.json) rather than fetching the
        # destination catalog's model, because the bag is the
        # source of truth for *what to load* — the destination's
        # schema is presumed to be compatible.
        schemas = list(self.bag_db.model.schemas.keys())
        orderer = ForeignKeyOrderer(
            self.bag_db.model,
            schemas,
            intentional_cycles=self.policy.intentional_cycles,
        )
        # Restrict to tables that are actually in scope per policy.
        tables_in_scope = [
            self.bag_db.model.schemas[s].tables[t]
            for s in schemas
            for t in self.bag_db.model.schemas[s].tables
            if self._table_in_scope(s, t)
        ]
        ordered = orderer.get_insertion_order(tables_in_scope)

        # FK cycles get one edge dropped by the orderer to make a
        # topological sort possible. Those dropped FKs can't be
        # satisfied at insert time (the target row hasn't landed
        # yet), so we defer them: insert each affected row with
        # the FK column set to NULL, then patch it in a second
        # pass after all tables are loaded.
        #
        # Pre-flight: a deferred FK column must be nullable. If
        # any cycle-cut FK is on a NOT-NULL column we can't satisfy
        # both ordering and the constraint — raise so the caller
        # knows the bag can't be loaded as-is.
        self._init_cycle_deferred_state(orderer)

        for table in ordered:
            stats = await self._load_table(table)
            qname = f"{table.schema.name}.{table.name}"
            report.table_stats[qname] = stats

        # Second pass: patch the deferred-FK columns we nulled
        # during insert. Skipped if no cycles were broken.
        if self._deferred_fk_values:
            await self._apply_deferred_fk_updates()

        return report

    def _init_cycle_deferred_state(
        self, orderer: ForeignKeyOrderer
    ) -> None:
        """Initialize the deferred-FK state from the orderer.

        After ``get_insertion_order`` runs, the orderer can tell us
        which FK edges it dropped to break cycles. We translate
        those into per-table sets of FK column names — those
        columns will be sent as NULL on first-pass insert and
        patched in :meth:`_apply_deferred_fk_updates` after every
        table has landed.

        Raises:
            ValueError: If any cycle-cut FK is on a NOT-NULL
                column. We can't satisfy both the insertion order
                (which requires the FK be deferrable) and the
                constraint (which forbids NULL), so the caller
                must adjust the schema or the bag.
        """
        # ``{(schema, table): {col1, col2, ...}}`` — columns to
        # null on insert and patch later.
        self._deferred_fk_cols: dict[
            tuple[str, str], set[str]
        ] = {}
        # ``{(schema, table): {rid: {col: value, ...}}}`` — the
        # values we deferred, keyed by the bag's RID, populated
        # during ``_load_content_table``.
        self._deferred_fk_values: dict[
            tuple[str, str], dict[str, dict[str, Any]]
        ] = {}

        non_nullable_violations: list[str] = []
        for dep_table, fk in orderer.cycle_broken_edges():
            key = (dep_table.schema.name, dep_table.name)
            cols = self._deferred_fk_cols.setdefault(key, set())
            for fk_col in fk.foreign_key_columns:
                if not fk_col.nullok:
                    non_nullable_violations.append(
                        f"{dep_table.schema.name}.{dep_table.name}."
                        f"{fk_col.name}"
                    )
                cols.add(fk_col.name)

        if non_nullable_violations:
            raise ValueError(
                f"BagCatalogLoader cannot load bag at {self.bag_path} "
                f"into catalog {self.catalog.catalog_id}: an FK in a "
                "cycle must be deferred to second-pass PUT, but the "
                "FK column is declared NOT NULL — first-pass insert "
                "would fail. Affected column(s): "
                + ", ".join(sorted(non_nullable_violations))
                + ". Either make the column nullable in the schema "
                "or remove the cycle on the source side."
            )

    async def _apply_deferred_fk_updates(self) -> None:
        """Second pass: update each row's deferred FK column values.

        For every ``(schema, table)`` that had cycle-cut FKs, walk
        the saved ``{rid: {col: value}}`` map and call
        ``_TableWrapper.update`` to fill in the columns that were
        sent as NULL on the first-pass insert.

        ``update(correlation={"RID"}, targets=<cols>)`` uses ERMrest's
        ``/attributegroup`` PUT under the hood — same wire shape as
        the previous hand-rolled implementation, but with batching,
        retry, and URL-encoding from deriva-py.
        """
        for (
            schema_name,
            table_name,
        ), per_row in self._deferred_fk_values.items():
            if not per_row:
                continue
            cols = sorted(
                self._deferred_fk_cols[(schema_name, table_name)]
            )
            payload: list[dict[str, Any]] = []
            for rid, col_values in per_row.items():
                row: dict[str, Any] = {"RID": rid}
                for col in cols:
                    # Some rows may not have a value for every
                    # deferred column (sparse). Omit those — ERMrest
                    # leaves the column at its previous value.
                    if col in col_values:
                        row[col] = col_values[col]
                payload.append(row)

            pb = self._ensure_path_builder()
            tw = pb.schemas[schema_name].tables[table_name]

            # ``functools.partial`` binds the loop variables at this
            # iteration; ``asyncio.to_thread`` then invokes it with
            # no further args.
            await asyncio.to_thread(
                functools.partial(
                    tw.update,
                    payload,
                    correlation={"RID"},
                    targets=cols,
                )
            )

    def _table_in_scope(self, schema_name: str, table_name: str) -> bool:
        """Apply policy.exclude_schemas/exclude_tables/schemas filter."""
        if schema_name in self.policy.exclude_schemas:
            return False
        if (schema_name, table_name) in self.policy.exclude_tables:
            return False
        if (
            self.policy.schemas is not None
            and schema_name not in self.policy.schemas
        ):
            return False
        return True

    def _classify_table(self, table: DerivaTable) -> TableClass:
        """Decide whether a table is reconciled by name or by RID.

        See :class:`TableClass` for the policy. Order:

        1. **``match_by_columns``** — explicit caller intent.
           A ``(schema, table)`` key in the policy dict overrides
           the structural vocabulary check.
        2. **Vocabulary structural check** —
           :meth:`deriva.core.ermrest_model.Table.is_vocabulary`
           checks for the canonical vocab column shape
           (``ID``/``URI``/``Name``/``Description``/``Synonyms``).
        3. **Content** — every other in-scope table.
        """
        key = (table.schema.name, table.name)
        if key in self.policy.match_by_columns:
            return TableClass.MATCH_BY_COLUMNS
        if table.is_vocabulary():
            return TableClass.VOCABULARY
        return TableClass.CONTENT

    # ------------------------------------------------------------------
    # Per-table load
    # ------------------------------------------------------------------

    async def _load_table(self, table: DerivaTable) -> TableLoadStats:
        """Load one table: rows + (if asset) bytes.

        Dispatches to one of three row-load paths based on
        :meth:`_classify_table`'s precedence ladder:

        1. **``MATCH_BY_COLUMNS``** — table is listed in
           :attr:`FKTraversalPolicy.match_by_columns`. Explicit
           caller intent wins over structural classification.
        2. **``VOCABULARY``** — structural detection
           (``Table.is_vocabulary()``). Matched by ``Name``.
        3. **``CONTENT``** — every other in-scope table. Inserted
           by RID with :attr:`FKTraversalPolicy.content_on_conflict`
           controlling collisions.

        See :class:`TableClass` for the full rule. All three
        paths apply :class:`DanglingFKStrategy` and run
        ``_rewrite_fks`` before insert (the rewrite handles
        composite-FK skipping with a one-shot warning per FK).
        """
        qname = f"{table.schema.name}.{table.name}"
        stats = TableLoadStats(table=qname)

        # Pull every row for this table out of the bag's SQLite
        # mirror. The mirror was populated from the bag's CSVs at
        # BagDatabase open time, so this is a fast local read.
        try:
            rows = list(
                self.bag_db.get_table_contents(table.name)
            )
        except KeyError:
            # Table is declared in the schema but no CSV present
            # in the bag — legitimate "empty table" outcome.
            return stats

        if not rows:
            return stats

        # Normalize empty-string nullable values to ``None``. The
        # bag's CSVs serialize NULL as the empty string (BDBag/CSV
        # convention); SQLite preserves the empty string verbatim,
        # which then trips the dangling-FK check ("'' not in
        # parent {...}") and produces ERMrest 400s on insert.
        # Convert at the row boundary so downstream code sees real
        # ``None`` for unset FKs.
        rows = [self._coerce_empty_to_null(table, row) for row in rows]

        # Apply dangling-FK strategy before any other handling.
        # Rows with missing parents are either dropped (DELETE),
        # patched (NULLIFY), or cause us to bail (FAIL).
        rows, skipped, nullified = self._apply_dangling_fk_strategy(
            table, rows
        )
        stats.rows_skipped_orphan = skipped
        stats.rows_nullified_orphan = nullified

        if not rows:
            return stats

        table_class = self._classify_table(table)
        if table_class == TableClass.VOCABULARY:
            await self._load_vocabulary_table(table, rows, stats)
        elif table_class == TableClass.MATCH_BY_COLUMNS:
            await self._load_match_by_columns_table(table, rows, stats)
        else:
            await self._load_content_table(table, rows, stats)

        if (
            table.is_asset()
            and self.policy.asset_mode != AssetMode.ROWS_ONLY
        ):
            # Hand off each asset row to deriva-py's
            # :meth:`DerivaUpload._hatracUpload`, which handles
            # chunking-by-size, HEAD-then-PUT dedup against the
            # destination's MD5, and (when wired up) per-chunk
            # transfer-state. Dedup-vs-transfer is decided
            # server-side inside :meth:`HatracStore.put_loc` and
            # is not surfaced to the loader; ``assets_attempted``
            # counts upload invocations only.
            stats.assets_attempted = await self._upload_assets(table, rows)

        return stats

    # ------------------------------------------------------------------
    # Vocabulary path (match-by-name + RID remap)
    # ------------------------------------------------------------------

    async def _load_vocabulary_table(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
        stats: TableLoadStats,
    ) -> None:
        """Reconcile a vocabulary table by ``Name``.

        For each bag row:

        - If the destination already has a row with the same ``Name``,
          record ``src_rid → dst_rid`` in the loader's remap table
          (so child rows that reference this vocab entry get
          rewritten to the destination's RID at insert time). The
          bag row is **not** inserted — the destination already has
          authoritative content for it.
        - Otherwise, insert the bag row (with its source RID
          preserved via ``?nondefaults=RID,RCT,RCB``). Record an
          identity remap entry so downstream lookups still find the
          RID.

        Vocabularies are the one place where the source and
        destination can hold the **same logical term** under
        different RIDs — every other table class is RID-stable
        across the clone. See ADR-0001 for the design rationale.
        """
        schema_name = table.schema.name
        existing_by_name = await self._fetch_existing_vocab_by_name(
            schema_name, table.name
        )

        new_rows: list[dict[str, Any]] = []
        remap = self._rid_remap.setdefault((schema_name, table.name), {})
        for row in rows:
            src_rid = row.get("RID")
            name = row.get("Name")
            if name is None or src_rid is None:
                # Defensive: a vocab row should always have both.
                # Treat as a new row and let ERMrest's insert path
                # surface any constraint violation.
                new_rows.append(row)
                continue
            dst_rid = existing_by_name.get(name)
            if dst_rid is not None:
                remap[src_rid] = dst_rid
                stats.rows_matched_by_name += 1
            else:
                # Mark source RID as authoritative for itself; the
                # insert preserves the RID, so the identity entry
                # makes the remap lookup uniform for child rows.
                remap[src_rid] = src_rid
                new_rows.append(row)

        if new_rows:
            # FK columns on a vocab row are unusual (most vocab
            # tables are FK-terminal) but legitimate — a vocab
            # row may reference another vocab row, and that
            # parent might have been remapped on the same load.
            # Apply the same FK-rewrite pass that
            # ``_load_content_table`` runs.
            new_rows = [self._rewrite_fks(table, row) for row in new_rows]
            inserted = await self._insert_rows(table, new_rows)
            stats.rows_inserted = inserted

    async def _fetch_existing_vocab_by_name(
        self, schema_name: str, table_name: str
    ) -> dict[str, str]:
        """Return ``{Name: RID}`` for every existing row in the table.

        Reads the whole vocab table via deriva-py's PathBuilder —
        vocabularies are small enough that pagination isn't worth
        the round trips.

        ``Name`` is assumed unique by the vocabulary contract
        (:meth:`~deriva.core.ermrest_model.Table.is_vocabulary`
        requires a ``Name`` key column). A duplicate is a
        destination-side data error; we log a warning and drop
        all but the first RID rather than silently picking one.
        """
        pb = self._ensure_path_builder()
        tw = pb.schemas[schema_name].tables[table_name]

        def _do_get() -> list[dict[str, Any]]:
            return list(
                tw.attributes(
                    tw.column_definitions["Name"],
                    tw.column_definitions["RID"],
                ).fetch()
            )

        rows = await asyncio.to_thread(_do_get)
        result: dict[str, str] = {}
        for row in rows:
            name = row.get("Name")
            if not name:
                continue
            if name in result:
                logger.warning(
                    "Vocabulary table %s.%s has duplicate Name %r at "
                    "destination (RIDs %r and %r); the first is kept "
                    "for remap purposes. Investigate the destination's "
                    "vocab data — Name is supposed to be unique.",
                    schema_name,
                    table_name,
                    name,
                    result[name],
                    row.get("RID"),
                )
                continue
            result[name] = row["RID"]
        return result

    # ------------------------------------------------------------------
    # Match-by-columns path (caller-supplied unique key + RID remap)
    # ------------------------------------------------------------------

    async def _load_match_by_columns_table(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
        stats: TableLoadStats,
    ) -> None:
        """Reconcile a table by the caller-supplied match columns.

        Same shape as :meth:`_load_vocabulary_table` but parameterized
        by the column list from
        :attr:`FKTraversalPolicy.match_by_columns`. For each bag row:

        - Compute the row's composite key from the named columns.
        - If the destination has a row with the same composite key,
          record ``src_rid → dst_rid`` in the loader's remap so
          child rows that FK-reference this row get rewritten at
          insert time. The bag row is **not** inserted.
        - Otherwise, insert the bag row and record an identity
          remap entry.

        Rows whose match-key has any ``None`` component fall
        through to the insert path — composite NULL semantics are
        ambiguous and we don't want to silently match e.g. two
        ``(NULL, NULL)`` rows as the same logical entity.
        """
        schema_name = table.schema.name
        key = (schema_name, table.name)
        match_cols = self.policy.match_by_columns[key]

        existing_by_key = await self._fetch_existing_by_columns(
            schema_name, table.name, match_cols
        )

        # FK rewrite is applied **up front**, before the match-key
        # is computed. ``match_by_columns`` is commonly set on
        # association tables (e.g. ``{Asset}_Asset_Type``) whose
        # match columns are themselves FK references into other
        # tables that were deduped on the same load. If we matched
        # on bag-side source RIDs, the lookup would miss the
        # already-existing destination row (it's keyed by the
        # destination RID) and we'd insert a duplicate — failing
        # the destination's unique-key constraint with 409.
        # Rewriting first means the match query asks the right
        # question: "does the destination already have this row
        # under its canonical RIDs?"
        rows = [self._rewrite_fks(table, row) for row in rows]

        new_rows: list[dict[str, Any]] = []
        remap = self._rid_remap.setdefault(key, {})
        for row in rows:
            src_rid = row.get("RID")
            match_key = tuple(row.get(col) for col in match_cols)
            if src_rid is None or any(v is None for v in match_key):
                # Either no RID (defensive — should not happen for
                # an in-scope bag row) or one of the match columns
                # is NULL. Either way, don't try to match; insert
                # as new and let any uniqueness constraint at the
                # destination surface its own error.
                new_rows.append(row)
                continue
            dst_rid = existing_by_key.get(match_key)
            if dst_rid is not None:
                remap[src_rid] = dst_rid
                stats.rows_matched_by_columns += 1
            else:
                # Identity remap — same rationale as vocab: the
                # insert preserves the RID, so child rows still
                # find it via the remap.
                remap[src_rid] = src_rid
                new_rows.append(row)

        if new_rows:
            inserted = await self._insert_rows(table, new_rows)
            stats.rows_inserted = inserted

    async def _fetch_existing_by_columns(
        self,
        schema_name: str,
        table_name: str,
        match_cols: list[str],
    ) -> dict[tuple[Any, ...], str]:
        """Return ``{(col1_value, col2_value, ...): RID}`` for all rows.

        Reads the whole table via deriva-py's PathBuilder, projecting
        only the match columns plus RID. For asset tables, the match
        columns are typically a single content-addressed key
        (e.g. ``["URL"]``), so the result is small.

        Rows whose match-key has any ``None`` component are
        dropped from the result map; the caller's matching loop
        already declines to match such rows, so including them
        here would only invite collisions on ``(None, None, ...)``
        bag rows.

        Duplicates on the composite match key are a destination-side
        data error (``match_by_columns`` is expected to identify a
        row uniquely per :attr:`FKTraversalPolicy.match_by_columns`'s
        contract). A duplicate is logged and the first RID is kept.
        """
        pb = self._ensure_path_builder()
        tw = pb.schemas[schema_name].tables[table_name]
        projection = [tw.column_definitions[col] for col in match_cols] + [
            tw.column_definitions["RID"]
        ]

        def _do_get() -> list[dict[str, Any]]:
            return list(tw.attributes(*projection).fetch())

        rows = await asyncio.to_thread(_do_get)
        result: dict[tuple[Any, ...], str] = {}
        for row in rows:
            key = tuple(row.get(col) for col in match_cols)
            if any(v is None for v in key):
                continue
            if key in result:
                logger.warning(
                    "match_by_columns table %s.%s has duplicate %r=%r "
                    "at destination (RIDs %r and %r); the first is "
                    "kept. Verify the destination's data — these "
                    "columns are supposed to uniquely identify a row.",
                    schema_name,
                    table_name,
                    match_cols,
                    key,
                    result[key],
                    row.get("RID"),
                )
                continue
            result[key] = row["RID"]
        return result

    # ------------------------------------------------------------------
    # Content path (RID-stable + conflict policy)
    # ------------------------------------------------------------------

    async def _load_content_table(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
        stats: TableLoadStats,
    ) -> None:
        """Insert a content table's rows, applying the RID remap.

        Before posting, every row's FK columns that reference a
        vocabulary table get rewritten through the loader's
        remap (e.g., ``Dataset.Dataset_Type`` switches from the
        source-catalog vocab RID to the destination's).

        Conflict handling on RID collision is per
        ``policy.content_on_conflict``:

        - ``FAIL``: a 409 from ERMrest propagates to the caller as
          an :class:`HTTPError`. Default.
        - ``SKIP_BY_RID``: existing destination RIDs are filtered out
          of the payload before POST. The number of skipped rows is
          recorded as ``rows_skipped_on_conflict``.
        """
        rewritten = [self._rewrite_fks(table, row) for row in rows]

        # If this table has FK columns that the orderer dropped to
        # break a cycle, those columns can't be sent on insert
        # (the target row hasn't landed yet). Save the values so
        # ``_apply_deferred_fk_updates`` can patch them in the
        # second pass, then null them in the insert payload.
        key = (table.schema.name, table.name)
        deferred_cols = self._deferred_fk_cols.get(key)
        if deferred_cols:
            per_row = self._deferred_fk_values.setdefault(key, {})
            for row in rewritten:
                rid = row.get("RID")
                if rid is None:
                    continue
                stash: dict[str, Any] = {}
                for col in deferred_cols:
                    if col in row and row[col] is not None:
                        stash[col] = row[col]
                        row[col] = None
                if stash:
                    per_row[rid] = stash

        skip_on_conflict = (
            self.policy.content_on_conflict
            == ContentConflictStrategy.SKIP_BY_RID
        )

        if not rewritten:
            return

        attempted = len(rewritten)
        inserted = await self._insert_rows(
            table, rewritten, on_conflict_skip=skip_on_conflict
        )
        stats.rows_inserted = inserted
        if skip_on_conflict:
            # ``_TableWrapper.insert`` with ``onconflict=skip`` returns
            # only the rows the destination actually accepted. Anything
            # we sent but didn't get back was skipped.
            stats.rows_skipped_on_conflict += attempted - inserted

    def _rewrite_fks(
        self, table: DerivaTable, row: dict[str, Any]
    ) -> dict[str, Any]:
        """Translate FK columns that reference remapped parents.

        For each single-column FK on ``table`` whose target table
        has an entry in :attr:`_rid_remap`, replace the row's FK
        value with the destination-catalog RID. Rows are mutated
        on a shallow-copied dict so the bag's in-memory rows stay
        clean (asset-upload code may want the originals).

        Composite (multi-column) FKs are not rewritten. They're
        vanishingly rare in deriva-ml-shaped catalogs (where every
        FK targets ``RID``) but legal in general ERMrest schemas.
        The loader logs a warning the first time it sees one per
        ``(schema, table, fk_name)`` so a maintainer running clone-via-
        bag on a less constrained schema gets a clear hint that the
        remap won't apply.
        """
        if not self._rid_remap:
            return row
        out = dict(row)
        for fk in table.foreign_keys:
            if len(fk.foreign_key_columns) != 1:
                self._warn_composite_fk_skipped(table, fk)
                continue
            src_col = fk.foreign_key_columns[0].name
            tgt_table = fk.pk_table
            remap = self._rid_remap.get(
                (tgt_table.schema.name, tgt_table.name)
            )
            if not remap:
                continue
            src_value = out.get(src_col)
            if src_value is None:
                continue
            if src_value in remap:
                out[src_col] = remap[src_value]
        return out

    def _warn_composite_fk_skipped(
        self, table: DerivaTable, fk: Any
    ) -> None:
        """Log a one-shot warning per composite FK encountered.

        Internal helper for :meth:`_rewrite_fks`. The remap can't
        rewrite a composite FK because the destination's column
        values for the multiple columns aren't independently
        knowable from a single source-RID remap entry. Callers
        on deriva-ml-shaped catalogs (RID-FK only) never see this
        warning; callers running clone-via-bag on a general
        ERMrest catalog get one warning per offending FK and a
        clear pointer at the limitation.

        Suppression is per ``(schema, table, fk_name)``, so a
        repeated row over the same FK during the same load only
        logs once. State lives on ``self._composite_fk_warned`` —
        which is initialised lazily so existing tests that
        construct ``BagCatalogLoader`` directly without going
        through ``__init__`` keep working.
        """
        warned = getattr(self, "_composite_fk_warned", None)
        if warned is None:
            warned = self._composite_fk_warned = set()
        # Each FK has a list of (schema, name) tuples; flatten for
        # a stable key.
        fk_key = (
            table.schema.name,
            table.name,
            tuple(fk.names) if hasattr(fk, "names") else id(fk),
        )
        if fk_key in warned:
            return
        warned.add(fk_key)
        logger.warning(
            "Composite FK on %s.%s (columns=%r → %s.%s) skipped by "
            "_rewrite_fks. The remap cannot rewrite composite FKs; "
            "rows of this table will be inserted with their bag-side "
            "FK values verbatim. Confirm the destination catalog has "
            "the referenced rows under those values.",
            table.schema.name,
            table.name,
            [c.name for c in fk.foreign_key_columns],
            fk.pk_table.schema.name,
            fk.pk_table.name,
        )

    def _apply_dangling_fk_strategy(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], int, int]:
        """Resolve dangling FK references per the policy.

        Returns ``(survivors, skipped, nullified)``. The strategy:

        - ``FAIL`` — if any row has a dangling FK, raise ValueError
          with the count.
        - ``DELETE`` — drop offending rows from the survivor list.
        - ``NULLIFY`` — set the dangling FK column to ``None`` (if
          the column is nullable) and keep the row.
        - ``PRESERVE`` — short-circuit; trust the destination
          catalog's FK constraint. No bag-side validation, no
          counter updates. Use when the bag deliberately ships
          rows whose FK parents live at the destination but
          aren't in the bag (e.g. the end-of-execution upload).
        """
        # PRESERVE skips the whole bag-side check. The destination
        # catalog's FK constraint is the authority; if a parent row
        # is missing there, ERMrest's insert will fail with HTTP 409
        # — the real error surfaces unfiltered. Returning ``rows``
        # verbatim with zero counters matches the "no orphans found"
        # success path so callers don't need to special-case the
        # report numbers.
        if self.policy.dangling_fk_strategy == DanglingFKStrategy.PRESERVE:
            return rows, 0, 0

        # Build the set of valid parent RIDs per FK column once per
        # ``(schema, table, ref_col)`` parent identity. The cache
        # lives on the loader so multiple in-scope tables that FK
        # into the same parent share one scan.
        if not hasattr(self, "_parent_rid_cache"):
            self._parent_rid_cache: dict[
                tuple[str, str, str], set[str]
            ] = {}

        parent_rids: dict[str, set[str]] = {}
        bag_schemas = set(self.bag_db.schemas)
        for fk in table.foreign_keys:
            if not fk.foreign_key_columns:
                continue
            fk_col = fk.foreign_key_columns[0].name
            pk_table = fk.pk_table
            # FKs into out-of-bag schemas (e.g., the system FKs on
            # ``RCB``/``RMB`` that point at ``public.ERMrest_Client``)
            # can never be validated from the bag alone — the parent
            # rows live outside the bag's scope. Treat them as
            # always-valid; the destination catalog will resolve
            # them on its own when the rows land.
            if pk_table.schema.name not in bag_schemas:
                continue
            ref_col = fk.referenced_columns[0].name
            cache_key = (pk_table.schema.name, pk_table.name, ref_col)
            cached = self._parent_rid_cache.get(cache_key)
            if cached is not None:
                parent_rids[fk_col] = cached
                continue
            try:
                parent_rows = list(
                    self.bag_db.get_table_contents(pk_table.name)
                )
            except KeyError:
                # Parent table missing entirely from the bag.
                # Every row's FK is dangling against this parent.
                parent_rids[fk_col] = set()
                self._parent_rid_cache[cache_key] = set()
                continue
            # Build the valid-rid set off the actual referenced
            # column; typically RID.
            rids = {
                row[ref_col] for row in parent_rows if row.get(ref_col)
            }
            self._parent_rid_cache[cache_key] = rids
            parent_rids[fk_col] = rids

        if not parent_rids:
            return rows, 0, 0

        survivors: list[dict[str, Any]] = []
        skipped = 0
        nullified = 0
        for row in rows:
            row_has_dangling = False
            for fk_col, valid in parent_rids.items():
                value = row.get(fk_col)
                if value is None:
                    # NULL FK — already-resolved, not dangling.
                    continue
                if value not in valid:
                    row_has_dangling = True
                    if (
                        self.policy.dangling_fk_strategy
                        == DanglingFKStrategy.NULLIFY
                    ):
                        # Column must be nullable in the schema
                        # for this to be a valid catalog row; we
                        # leave that check to ERMrest's response.
                        row[fk_col] = None
                        nullified += 1
                    elif (
                        self.policy.dangling_fk_strategy
                        == DanglingFKStrategy.DELETE
                    ):
                        # Drop the row entirely.
                        pass
                    else:
                        # FAIL: collect the violations and raise.
                        raise ValueError(
                            f"Dangling FK detected in {table.name}.{fk_col} "
                            f"(value={value!r} not in parent {valid}); "
                            f"use dangling_fk_strategy=DELETE or NULLIFY "
                            f"to relax."
                        )
            if (
                row_has_dangling
                and self.policy.dangling_fk_strategy
                == DanglingFKStrategy.DELETE
            ):
                skipped += 1
                continue
            survivors.append(row)

        return survivors, skipped, nullified

    @staticmethod
    def _coerce_pg_array(value: Any) -> Any:
        """Convert a PostgreSQL CSV array literal into a JSON array.

        Bag CSVs preserve ``text[]`` / ``int[]`` etc. as PostgreSQL's
        literal-array form (``{}``, ``{a,b}``, ``{1,2,3}``). ERMrest's
        JSON ingest expects real arrays; sending the literal triggers
        ``cannot call json_array_elements_text on a scalar`` on the
        server side. Coerce here so callers see normal Python lists.

        Defensive about non-string and already-decoded inputs — pass
        them through unchanged.
        """
        if value is None or not isinstance(value, str):
            return value
        if not (value.startswith("{") and value.endswith("}")):
            return value
        inner = value[1:-1]
        if not inner:
            return []
        # Naive split is fine for the common case (text[] of simple
        # identifiers, int[] of digits). Embedded commas in quoted
        # strings aren't produced by the current bag walker.
        return [part.strip().strip('"') for part in inner.split(",")]

    @staticmethod
    def _coerce_empty_to_null(
        table: DerivaTable, row: dict[str, Any]
    ) -> dict[str, Any]:
        """Convert empty-string values on nullable columns to ``None``.

        The bag's CSVs serialize NULL as the empty string (BDBag/CSV
        convention has no native NULL sentinel). SQLite preserves
        the empty string; downstream consumers — the dangling-FK
        check, ERMrest's JSON ingest, the vocab match-by-name path
        — all expect real ``None`` for unset values. Coerce at the
        row boundary so we don't have to special-case the empty
        string in every consumer.

        Only columns that are declared nullable in the schema are
        coerced; an empty string on a NOT-NULL column is left alone
        (and will fail ERMrest's validation, which is the right
        behavior — it signals a real data problem).
        """
        nullable_cols = {
            c.name for c in table.column_definitions if c.nullok
        }
        if not nullable_cols:
            return row
        out = dict(row)
        for col in nullable_cols:
            if col in out and out[col] == "":
                out[col] = None
        return out

    @staticmethod
    def _coerce_datetimes(row: dict[str, Any]) -> dict[str, Any]:
        """Convert ``datetime.date`` / ``datetime.datetime`` values to ISO strings.

        SQLite-mirror rows come back with Python date/datetime
        objects (via :class:`deriva.bag.database.StringToDate` and
        :class:`StringToDateTime`). :func:`json.dumps` can't
        serialize those directly, and ERMrest expects ISO-8601
        text on the wire. Walk the row, replace each date/datetime
        with its ``isoformat()`` string, and leave other types
        alone.

        Returns a new dict so the caller's input isn't mutated.
        """
        import datetime

        return {
            k: (v.isoformat() if isinstance(v, (datetime.date, datetime.datetime)) else v)
            for k, v in row.items()
        }

    async def _insert_rows(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
        *,
        on_conflict_skip: bool = False,
    ) -> int:
        """Bulk-insert rows via deriva-py's ``_TableWrapper.insert``.

        Picks ``nondefaults`` from ``policy.preserve_provenance``:

        - **True** (clone semantics): ``nondefaults={"RID","RCT","RCB"}``
          preserves the source's creation audit data; matches the
          canonical clone path.
        - **False** (commit semantics): ``nondefaults={"RID"}`` so the
          server sets ``RCT``/``RCB``. The bag's serialized empty
          strings for these columns are stripped explicitly
          because ERMrest rejects ``""`` for timestamp /
          ERMrest_Client typed columns.

        ``on_conflict_skip=True`` adds ``?onconflict=skip`` so the
        destination silently ignores rows whose RIDs already exist
        — subsumes the old SKIP_BY_RID pre-fetch + filter.

        ``_TableWrapper.insert`` provides retry/backoff, batching,
        and URL-encoding for free.
        """
        if not rows:
            return 0

        nondefaults: set[str] = {"RID"}
        if self.policy.preserve_provenance:
            nondefaults |= {"RCT", "RCB"}
        else:
            # Commit semantics: strip RCT/RCB/RMT/RMB from the row
            # dict entirely. The bag built them via
            # ``BagBuilder.add_row`` from a row that didn't supply
            # these system columns, so they're either absent
            # (already fine) or empty strings (the bag's SQLite
            # mirror serializes NULL as ``""`` via CSV — and
            # ERMrest rejects empty strings for timestamp /
            # ERMrest_Client columns with a 400). Removing them
            # lets the server's defaults populate ``RCT`` (now())
            # and ``RCB`` (current user) at insert time.
            _SYSTEM_COLUMNS = ("RCT", "RCB", "RMT", "RMB")
            rows = [
                {k: v for k, v in row.items() if k not in _SYSTEM_COLUMNS}
                for row in rows
            ]

        # Coerce array-typed columns from PostgreSQL literal form
        # (``{a,b}``) into real JSON arrays for the wire.
        array_cols = [
            c.name
            for c in table.column_definitions
            if getattr(c.type, "is_array", False)
        ]
        if array_cols:
            for row in rows:
                for col in array_cols:
                    if col in row:
                        row[col] = self._coerce_pg_array(row[col])

        # Coerce date/datetime values back to ISO strings. The bag's
        # SQLite mirror returns Python ``datetime.date`` /
        # ``datetime.datetime`` objects via the type decorators in
        # :mod:`deriva.bag._column_types` (``StringToDate`` /
        # ``StringToDateTime``); ``json.dumps`` can't serialize those
        # directly and ERMrest expects ISO strings on the wire.
        rows = [self._coerce_datetimes(r) for r in rows]

        tw = self._table_wrapper(table)

        def _do_insert() -> int:
            result = tw.insert(
                rows,
                nondefaults=nondefaults,
                on_conflict_skip=on_conflict_skip,
            )
            # With ``onconflict=skip`` the destination returns only
            # the rows it actually accepted; otherwise the result
            # mirrors the input. ``len()`` walks the result-set's
            # fetched data.
            return len(list(result))

        # ``asyncio.to_thread`` keeps the sync HTTP call from
        # blocking the loop; for the small-row batches we see,
        # the overhead is negligible.
        return await asyncio.to_thread(_do_insert)

    async def _upload_assets(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
    ) -> int:
        """Push asset bytes to the destination Hatrac via deriva-py's uploader.

        For each asset row in ``rows`` the loader hands the
        bag-local file path and the source-catalog URL to
        :meth:`DerivaUpload._hatracUpload`, which:

        - Picks chunked vs. single-PUT based on file size (default
          chunk threshold is ``DEFAULT_CHUNK_SIZE``).
        - HEADs the destination first; when ``Content-MD5`` matches
          the supplied MD5 it returns the existing object location
          without transferring bytes (server-side dedup). The
          loader does not surface the dedup-vs-transfer distinction
          to the caller — :attr:`TableLoadStats.assets_attempted`
          counts upload invocations only.
        - With ``UPLOAD_FORCE``, skips the HEAD and pushes
          unconditionally.

        Rows missing a local file or URL are warned and skipped
        (they don't count toward ``assets_attempted``). HTTP
        errors during upload propagate to the caller — the
        loader's job is to surface them, not to swallow them.

        Args:
            table: The asset table being uploaded (used for the
                bag-local path resolution and for diagnostic
                logging).
            rows: The bag rows to upload. Each row supplies
                ``URL`` (destination Hatrac path source),
                ``Filename`` (catalog-facing name; resolved to
                the on-disk path by
                :meth:`BagDatabase.resolve_asset_local_path`),
                and ``MD5`` (used both for dedup HEAD-match and
                for upload-job verification).

        Returns:
            Number of asset rows for which :meth:`_hatracUpload`
            was invoked. Sets :attr:`TableLoadStats.assets_attempted`
            at the caller.
        """
        uploader = self._get_uploader()
        force = self.policy.asset_mode == AssetMode.UPLOAD_FORCE

        attempted = 0
        for row in rows:
            url = row.get("URL")
            if not url:
                logger.warning(
                    "asset row %s.%s/RID=%s missing URL; skipping",
                    table.schema.name, table.name, row.get("RID"),
                )
                continue
            # Resolve the bag-local path on demand. The row's
            # ``Filename`` column holds the catalog-facing name
            # (which gets inserted verbatim); the actual on-disk
            # bytes live elsewhere — either where ``fetch.txt``
            # placed them (clone bags) or at the profile-standard
            # embedded-asset path (constructive bags). The bag
            # database knows both layouts.
            local_path = self.bag_db.resolve_asset_local_path(
                table.name, row
            )
            if not local_path:
                logger.warning(
                    "asset row %s.%s/RID=%s has no bytes available in the "
                    "bag (no fetch.txt match for URL=%r and no embedded "
                    "asset at data/asset/%s/%s/%s); skipping byte upload",
                    table.schema.name, table.name, row.get("RID"),
                    url, table.name, row.get("RID"),
                    row.get("Filename"),
                )
                continue
            hatrac_path = self._hatrac_path_for(url)
            if hatrac_path is None:
                logger.warning(
                    "asset row %s.%s/RID=%s URL=%r is not a hatrac URL; "
                    "skipping",
                    table.schema.name, table.name, row.get("RID"),
                    url,
                )
                continue

            md5 = row.get("MD5") or None

            # Hand off to deriva-py's uploader. ``_hatracUpload``
            # decides chunking based on file size and does
            # HEAD-then-PUT dedup against the destination's MD5
            # internally; when ``force`` is True it skips the HEAD
            # and pushes unconditionally.
            await asyncio.to_thread(
                uploader._hatracUpload,
                hatrac_path,
                str(local_path),
                md5=md5,
                chunked=True,
                force=force,
            )
            attempted += 1

        return attempted

    def _get_uploader(self) -> Any:
        """Return a minimal :class:`DerivaUpload` for asset uploads.

        Constructed lazily on first asset-upload via
        :meth:`DerivaUpload.minimal_for_upload`, which skips the
        full ``__init__`` (config file, SIGINT handler, credential
        resolution). The loader is a one-shot driver — it only
        needs :meth:`DerivaUpload._hatracUpload`, which depends on
        ``store``, ``server_url``, and the (empty) transfer-state
        machinery.
        """
        if getattr(self, "_uploader", None) is not None:
            return self._uploader

        from deriva.core import HatracStore
        from deriva.transfer.upload.deriva_upload import DerivaUpload

        deriva_server = self.catalog.deriva_server
        store = HatracStore(
            deriva_server.scheme,
            deriva_server.server,
            credentials=self.catalog.get_credentials(),
        )
        self._uploader = DerivaUpload.minimal_for_upload(
            store=store,
            server_url=f"{deriva_server.scheme}://{deriva_server.server}",
        )
        return self._uploader

    @staticmethod
    def _hatrac_path_for(url: str) -> str | None:
        """Extract the unversioned ``/hatrac/...`` path from a hatrac URL.

        Accepts full ``https://host/hatrac/...`` and bare
        ``/hatrac/...``. Versioned URLs (with a trailing
        ``:VERSIONID``) get stripped to the base path — Hatrac
        assigns versions on PUT, and uploading directly to a
        versioned URL returns ``405 Method Not Allowed``. The
        source's version is preserved in the catalog row's
        ``URL`` column, but the upload target is the unversioned
        name.

        Returns ``None`` for anything else — non-hatrac URLs (CDN
        refs, external links) aren't uploadable as Hatrac objects.
        """
        from urllib.parse import urlparse

        if url.startswith(HATRAC_PATH_PREFIX):
            path = url
        else:
            parsed = urlparse(url)
            if not parsed.path.startswith(HATRAC_PATH_PREFIX):
                return None
            path = parsed.path
        # Strip the ``:VERSIONID`` suffix if present. Versioned
        # objects can't be written to directly; the unversioned
        # parent path is the upload target.
        version_sep = path.rfind(":")
        last_slash = path.rfind("/")
        if version_sep > last_slash:
            path = path[:version_sep]
        return path

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def dispose(self) -> None:
        """Release the BagDatabase if this loader opened it."""
        if self._owned_bag_db:
            self.bag_db.dispose()

    def __enter__(self) -> "BagCatalogLoader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.dispose()
        return False


__all__ = [
    "BagCatalogLoader",
    "LoadReport",
    "TableClass",
    "TableLoadStats",
]
