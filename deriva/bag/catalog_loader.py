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

The loader does **not** implement its own asset uploader; it
delegates each per-file upload to deriva-py's
:class:`~deriva.transfer.upload.deriva_upload.DerivaUpload`
recipe, which already handles Hatrac MD5-based dedupe, catalog
row reconciliation (so additional metadata on the bag's row
survives a re-upload), pre-allocated RID handling, and
transfer-state resumption.

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
from deriva.bag.traversal import (
    DEFAULT_EXCLUDE_SCHEMAS,
    AssetMode,
    ContentConflictStrategy,
    DanglingFKStrategy,
    FKTraversalPolicy,
)


class _TableClass(StrEnum):
    """How :class:`BagCatalogLoader` should treat each in-scope table.

    Determined at the start of the load by :meth:`_classify_table`
    from the bag's schema model:

    - ``VOCABULARY``: table looks like a controlled vocabulary
      (has the canonical ``ID``/``URI``/``Name``/``Description``/
      ``Synonyms`` columns per
      :meth:`~deriva.core.ermrest_model.Table.is_vocabulary`).
      Reconciled by ``Name`` against the destination; existing rows
      contribute a source-RID → destination-RID entry to the
      loader's remap so child rows can be rewritten.
    - ``CONTENT``: every other in-scope table. Inserted by RID;
      collisions resolved per ``policy.content_on_conflict``.

    System schemas and out-of-bag schemas are filtered out earlier
    (by :meth:`_table_in_scope`) and never reach the classifier.
    """

    VOCABULARY = "vocabulary"
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

    assets_uploaded: int = 0
    """Asset files transferred to the destination Hatrac (zero
    for ``ROWS_ONLY`` mode or when Hatrac dedupe found a match)."""

    assets_deduped: int = 0
    """Asset files that already existed in the destination Hatrac
    with a matching MD5; bytes not transferred."""

    rows_matched_by_name: int = 0
    """Vocabulary rows that already existed on the destination
    (matched by ``Name``); the bag's source RID was remapped to the
    destination's RID for any child rows that reference it.
    Always zero for non-vocabulary tables."""

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

        Thin wrapper around :meth:`arun`. Callers in already-async
        contexts should call ``arun`` directly to avoid the
        nested-loop overhead.
        """
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
        orderer = ForeignKeyOrderer(self.bag_db.model, schemas)
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
                "BagCatalogLoader cannot load this bag: an FK in a "
                "cycle must be deferred to second-pass PUT, but the "
                "FK column is declared NOT NULL — first-pass insert "
                "would fail. Affected column(s): "
                + ", ".join(sorted(non_nullable_violations))
                + ". Either make the column nullable in the schema "
                "or remove the cycle on the source side."
            )

    async def _apply_deferred_fk_updates(self) -> None:
        """Second pass: PUT each row's deferred FK column values.

        For every ``(schema, table)`` that had cycle-cut FKs, walk
        the saved ``{rid: {col: value}}`` map and issue one PUT
        per row to fill in the columns that were sent as NULL on
        the first-pass insert.

        ERMrest's ``/attributegroup/{schema}:{table}/RID;col1,col2``
        endpoint accepts a JSON array of update rows, each
        carrying the keying column (``RID``) plus the target
        columns. Using PUT-by-attributegroup (rather than
        ``/entity/`` PUT) avoids re-sending every column on the
        row.
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

            url = (
                f"/attributegroup/{schema_name}:{table_name}"
                f"/RID;{','.join(cols)}"
            )

            def _do_put(payload=payload, url=url) -> None:
                response = self.catalog.put(url, json=payload)
                response.raise_for_status()

            await asyncio.to_thread(_do_put)

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

    def _classify_table(self, table: DerivaTable) -> _TableClass:
        """Decide whether a table is reconciled by name or by RID.

        See :class:`_TableClass` for the policy. Vocabulary detection
        delegates to
        :meth:`deriva.core.ermrest_model.Table.is_vocabulary`, which
        checks for the canonical vocab column shape (``ID``/``URI``/
        ``Name``/``Description``/``Synonyms``). Everything else is
        ``CONTENT``.
        """
        if table.is_vocabulary():
            return _TableClass.VOCABULARY
        return _TableClass.CONTENT

    # ------------------------------------------------------------------
    # Per-table load
    # ------------------------------------------------------------------

    async def _load_table(self, table: DerivaTable) -> TableLoadStats:
        """Load one table: rows + (if asset) bytes.

        Dispatches to a vocabulary or content path based on
        :meth:`_classify_table`. Both paths still apply the
        :class:`DanglingFKStrategy` from the policy.
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

        if self._classify_table(table) == _TableClass.VOCABULARY:
            await self._load_vocabulary_table(table, rows, stats)
        else:
            await self._load_content_table(table, rows, stats)

        if (
            table.is_asset()
            and self.policy.asset_mode != AssetMode.ROWS_ONLY
        ):
            # Upload asset bytes. For UPLOAD_IF_MISSING the upload
            # recipe checks Hatrac via HEAD and skips bytes when
            # MD5 matches; we count those skips for the report.
            uploaded, deduped = await self._upload_assets(table, rows)
            stats.assets_uploaded = uploaded
            stats.assets_deduped = deduped

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
            inserted = await self._insert_rows(table, new_rows)
            stats.rows_inserted = inserted

    async def _fetch_existing_vocab_by_name(
        self, schema_name: str, table_name: str
    ) -> dict[str, str]:
        """Return ``{Name: RID}`` for every existing row in the table.

        One GET to ``/attributegroup/{schema}:{table}/Name;RID`` covers
        the whole table — vocabularies are small enough that pagination
        isn't worth the round trips.
        """
        path = (
            f"/attributegroup/"
            f"{schema_name}:{table_name}/Name;RID"
        )

        def _do_get() -> list[dict[str, Any]]:
            response = self.catalog.get(path)
            response.raise_for_status()
            return response.json()

        rows = await asyncio.to_thread(_do_get)
        return {row["Name"]: row["RID"] for row in rows if row.get("Name")}

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

        if (
            self.policy.content_on_conflict
            == ContentConflictStrategy.SKIP_BY_RID
        ):
            existing_rids = await self._fetch_existing_rids(
                table.schema.name, table.name
            )
            kept: list[dict[str, Any]] = []
            for row in rewritten:
                if row.get("RID") in existing_rids:
                    stats.rows_skipped_on_conflict += 1
                else:
                    kept.append(row)
            rewritten = kept

        if not rewritten:
            return

        inserted = await self._insert_rows(table, rewritten)
        stats.rows_inserted = inserted

    def _rewrite_fks(
        self, table: DerivaTable, row: dict[str, Any]
    ) -> dict[str, Any]:
        """Translate FK columns that reference remapped parents.

        For each single-column FK on ``table`` whose target table
        has an entry in :attr:`_rid_remap`, replace the row's FK
        value with the destination-catalog RID. Rows are mutated
        on a shallow-copied dict so the bag's in-memory rows stay
        clean (asset-upload code may want the originals).
        """
        if not self._rid_remap:
            return row
        out = dict(row)
        for fk in table.foreign_keys:
            if len(fk.foreign_key_columns) != 1:
                # Composite FKs into vocabularies are vanishingly
                # rare in deriva-ml-shaped catalogs; punt.
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

    async def _fetch_existing_rids(
        self, schema_name: str, table_name: str
    ) -> set[str]:
        """Return every RID currently present in ``schema.table``.

        Used by the ``SKIP_BY_RID`` content-conflict path to filter
        the insert payload. For tables with very large row counts
        this is one pass through the destination — that's fine for
        the resume-a-partial-load use case.
        """
        path = f"/attribute/{schema_name}:{table_name}/RID"

        def _do_get() -> list[dict[str, Any]]:
            response = self.catalog.get(path)
            response.raise_for_status()
            return response.json()

        rows = await asyncio.to_thread(_do_get)
        return {row["RID"] for row in rows if row.get("RID")}

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
        """
        # Build the set of valid parent RIDs per FK column once.
        # Each table.foreign_keys element points at a single
        # column (or composite); for the simple single-column
        # case we look up the parent's RIDs in the bag.
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
            try:
                parent_rows = list(
                    self.bag_db.get_table_contents(pk_table.name)
                )
            except KeyError:
                # Parent table missing entirely from the bag.
                # Every row's FK is dangling against this parent.
                parent_rids[fk_col] = set()
                continue
            # The pk side is whichever column the FK references;
            # typically RID. Build the valid-rid set off the
            # actual referenced column.
            ref_col = fk.referenced_columns[0].name
            parent_rids[fk_col] = {
                row[ref_col] for row in parent_rows if row.get(ref_col)
            }

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
    ) -> int:
        """Bulk-insert rows via ERMrest's /entity endpoint.

        Routes through ``catalog.post`` (sync). The async wrapper
        is mostly defensive — today's :class:`ErmrestCatalog` API
        is sync; running it inside ``asyncio.to_thread`` keeps
        the surface consistent with ``arun`` semantics so the
        loader can be embedded in async pipelines without
        blocking the event loop.
        """
        # Preserve provenance for creation (RID, RCT, RCB) and let
        # the destination set modification (RMT, RMB) on insert.
        # This matches deriva-py's canonical catalog-clone paths
        # (``ErmrestCatalog.clone_catalog`` and ``asyncio/clone.py``):
        # creation timestamps and user are real audit data worth
        # preserving; modification timestamps would be overwritten
        # on the very next update anyway.
        qname = f"{table.schema.name}:{table.name}"
        url = f"/entity/{qname}?nondefaults=RID,RCT,RCB"

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
        # :mod:`deriva.bag.database` (``StringToDate`` /
        # ``StringToDateTime``); ``json.dumps`` can't serialize those
        # directly and ERMrest expects ISO strings on the wire.
        rows = [self._coerce_datetimes(r) for r in rows]

        def _do_insert() -> int:
            response = self.catalog.post(url, json=rows)
            response.raise_for_status()
            return len(rows)

        # ``asyncio.to_thread`` keeps the sync HTTP call from
        # blocking the loop; for the small-row batches we see,
        # the overhead is negligible.
        return await asyncio.to_thread(_do_insert)

    async def _upload_assets(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """Push asset bytes to the destination Hatrac. Returns ``(uploaded, deduped)``.

        For each asset row in ``rows``:

        - Use ``Filename`` (now a local path inside the bag — see
          :meth:`BagDatabase._localize_asset_row`) as the source
          file.
        - Use the path component of the row's ``URL`` (the
          source-catalog Hatrac URL) as the destination Hatrac
          path. Source and destination share the same logical
          ``/hatrac/{table}/...`` layout, so the path is portable
          across catalogs.
        - With ``UPLOAD_IF_MISSING`` (default), HEAD the
          destination first and skip the byte transfer when the
          MD5 already matches.
        - With ``UPLOAD_FORCE``, re-upload unconditionally.

        Rows missing a local file or URL are warned and skipped
        (they count as neither uploaded nor deduped). HTTP errors
        during upload propagate to the caller — the loader's job
        is to surface them, not to swallow them.
        """
        hatrac = self._dest_hatrac_store()

        uploaded = 0
        deduped = 0
        force = self.policy.asset_mode == AssetMode.UPLOAD_FORCE

        for row in rows:
            local_path = row.get("Filename")
            url = row.get("URL")
            if not local_path or not url:
                logger.warning(
                    "asset row %s.%s/RID=%s missing Filename or URL; skipping",
                    table.schema.name, table.name, row.get("RID"),
                )
                continue
            if not Path(local_path).is_file():
                logger.warning(
                    "asset row %s.%s/RID=%s Filename=%r does not exist on "
                    "disk; skipping (was the bag materialized?)",
                    table.schema.name, table.name, row.get("RID"),
                    local_path,
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

            # HEAD-then-PUT so we can count dedupes accurately. For
            # ``UPLOAD_FORCE`` skip the HEAD and just push.
            if not force and await self._hatrac_already_has(
                hatrac, hatrac_path, md5
            ):
                deduped += 1
                continue

            await asyncio.to_thread(
                hatrac.put_loc,
                hatrac_path,
                local_path,
                md5=md5,
                force=force,
            )
            uploaded += 1

        return uploaded, deduped

    def _dest_hatrac_store(self) -> Any:
        """Return a :class:`HatracStore` bound to the destination host.

        Cached on the loader since every asset upload reuses the
        same store. The store reuses the catalog's credentials.
        """
        if getattr(self, "_hatrac_store", None) is not None:
            return self._hatrac_store
        from deriva.core import HatracStore

        deriva_server = self.catalog.deriva_server
        self._hatrac_store = HatracStore(
            deriva_server.scheme,
            deriva_server.server,
            credentials=self.catalog._credentials,
        )
        return self._hatrac_store

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

        if url.startswith("/hatrac/"):
            path = url
        else:
            parsed = urlparse(url)
            if not parsed.path.startswith("/hatrac/"):
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

    async def _hatrac_already_has(
        self,
        hatrac: Any,
        hatrac_path: str,
        md5: str | None,
    ) -> bool:
        """HEAD the destination Hatrac object; True iff MD5 matches.

        Missing MD5 means we can't be sure dedupe is safe, so we
        return False (forcing an upload). A 404 on HEAD also means
        we need to upload.
        """
        if not md5:
            return False
        import requests

        def _head() -> bool:
            try:
                r = hatrac.head(hatrac_path)
            except requests.HTTPError as e:
                if getattr(e.response, "status_code", None) == 404:
                    return False
                raise
            if r.status_code != 200:
                return False
            return r.headers.get("Content-MD5") == md5

        return await asyncio.to_thread(_head)

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
    "TableLoadStats",
]
