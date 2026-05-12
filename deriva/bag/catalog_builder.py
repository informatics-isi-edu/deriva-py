"""Walking bag producer — catalog → bag via the deriva-py export engine.

:class:`CatalogBagBuilder` writes a deriva-bag profile bag from a
live ERMrest catalog. It does **not** implement its own row
fetcher or BDBag finalizer; it generates an *export specification*
from :class:`Anchor` + :class:`FKTraversalPolicy` and hands the
spec to deriva-py's :class:`GenericDownloader`. The downloader
already does the heavy work — paged async ERMrest queries, MD5
manifest generation, asset fetching, BDBag finalization, optional
S3 upload and MINID minting.

The producer split between :class:`BagBuilder` and
:class:`CatalogBagBuilder` is deliberate (see ADR-0006): the
constructive case has no source catalog and so can't use the
export engine; the walking case has one and must, because losing
the engine's streaming / pagination / asset-fetch features would
be a regression.

Spec-generation algorithm (high-level):

1. Resolve every :class:`Anchor` to a starting set of RIDs per
   table. :class:`RIDAnchor` is verified against the source
   catalog up-front; missing RIDs raise.
2. Walk the FK graph from the starting set, recording every
   reachable table. Honor :class:`FKTraversalPolicy`
   (``schemas``, ``exclude_schemas``, ``exclude_tables``,
   ``max_depth``). Vocab tables are followed inbound-only
   (hardcoded — prevents the loop-back explosion).
3. Generate one export-spec ``query_processors`` entry per
   reached ``(anchor_table, path)`` pair, plus one extra
   processor per asset table (the engine writes asset rows and
   fetches their bytes in separate phases).
4. If ``vocab_export=FULL`` add a standalone CSV processor per
   reached vocabulary table.
5. Add ``data/schema.json`` as an env+json processor so the
   resulting bag carries the source catalog's model.
6. Hand the spec to :class:`GenericDownloader`.

This module is the smallest of the three producer classes — the
hard work lives in :mod:`schema_io`, :mod:`anchors`,
:mod:`traversal`, and the engine.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from deriva.core import ErmrestCatalog
from deriva.core.ermrest_model import Table as DerivaTable

from deriva.bag.anchors import (
    Anchor,
    AnchorKind,
    PathAnchor,
    RIDAnchor,
    TableAnchor,
)
from deriva.bag.profile import (
    BAGIT_PROFILE_IDENTIFIER,
    write_provenance,
)
from deriva.bag.traversal import (
    DEFAULT_EXCLUDE_SCHEMAS,
    FKTraversalPolicy,
    VocabExport,
)

logger = logging.getLogger(__name__)


#: Backwards-compat alias for the canonical default-exclude set.
#: New code should import :data:`DEFAULT_EXCLUDE_SCHEMAS` from
#: :mod:`deriva.bag.traversal` directly.
_SYSTEM_SCHEMAS = DEFAULT_EXCLUDE_SCHEMAS


class CatalogBagBuilder:
    """Build a bag from a live ERMrest catalog.

    Args:
        catalog: Connected :class:`ErmrestCatalog`. Used for both
            anchor validation (the up-front ``?RID=in(...)`` check)
            and as the source the export engine queries.
        anchors: List of :class:`Anchor` objects describing where
            the walk starts. Overlaps are silently deduped;
            :class:`RIDAnchor` RIDs are validated.
        policy: :class:`FKTraversalPolicy` for the walk. Defaults
            to a sensible default policy if omitted.
        output_dir: Directory to receive the bag.
        producer: Identifier stamped into the bag's provenance.
            Defaults to ``"deriva.bag.catalog_builder.CatalogBagBuilder"``.

    Example:
        Build a bag rooted at a few Subject RIDs::

            from deriva.bag.anchors import RIDAnchor
            from deriva.bag.catalog_builder import CatalogBagBuilder
            from deriva.bag.traversal import FKTraversalPolicy

            cb = CatalogBagBuilder(
                catalog=catalog,
                anchors=[RIDAnchor(table="Subject", rids=["S1", "S2"])],
                policy=FKTraversalPolicy(schemas={"isa"}),
                output_dir=Path("/tmp/slice-bag"),
            )
            bag_path = cb.build()
    """

    def __init__(
        self,
        *,
        catalog: ErmrestCatalog,
        anchors: list[Anchor],
        output_dir: Path,
        policy: FKTraversalPolicy | None = None,
        producer: str = (
            "deriva.bag.catalog_builder.CatalogBagBuilder"
        ),
    ):
        if not anchors:
            raise ValueError(
                "CatalogBagBuilder requires at least one anchor"
            )
        self.catalog = catalog
        self.anchors: list[Anchor] = list(anchors)
        self.policy = policy or FKTraversalPolicy()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.producer = producer

        # Cached after first build() call.
        self._model: Any | None = None
        self._spec: dict[str, Any] | None = None
        self._reached_tables: set[tuple[str, str]] = set()
        # Per-table FK path from an anchor. Each value is the
        # ordered list of ``(schema, table)`` segments starting at
        # the anchor's table and ending at the reached table. Used
        # by :meth:`_table_query_path` to scope a non-anchor
        # table's query to "rows reachable from the anchor via this
        # FK path". When a table is reached from a
        # :class:`TableAnchor` (the whole-table anchor), the path
        # is recorded but the query is left unfiltered — every row
        # is in scope by construction.
        #
        # ``_table_paths`` records the *first* (BFS-shortest) path
        # discovered per target; ``_table_path_set`` records every
        # path discovered, so :meth:`_build_export_spec` can emit
        # one query_processor per FK route. Multi-path emission is
        # required when the BFS-shortest path goes through a table
        # that has no rows for the slice (e.g.,
        # Dataset → Dataset_File → File → Image when the actual
        # Image rows live on Dataset → Subject → Image).
        self._table_paths: dict[
            tuple[str, str], list[tuple[str, str]]
        ] = {}
        self._table_path_set: dict[
            tuple[str, str], list[tuple[tuple[str, str], ...]]
        ] = {}
        # Anchor-table set: which entries in ``_table_paths`` are
        # themselves anchors (vs. reached via the FK walk). The
        # anchor table's query is RID-filtered (or full-table for
        # ``TableAnchor``); everything else's query chains through
        # the path.
        self._anchor_tables: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self) -> Path:
        """Drive the export engine and return the bag directory path.

        Returns:
            Path to the on-disk bag directory.

        Raises:
            ValueError: If anchor validation fails (e.g.,
                :class:`RIDAnchor` names RIDs that don't exist in
                the source catalog).
        """
        self._validate_anchors()
        self._compute_reached_tables()
        spec = self._build_export_spec()
        self._spec = spec
        bag_path = self._run_export(spec)
        self._write_provenance_file()
        return bag_path

    def get_export_spec(self) -> dict[str, Any]:
        """Return the export specification without running it.

        Useful for diagnostics, dry runs, and tests. The spec is
        computed on first call (or first :meth:`build` call) and
        cached.
        """
        if self._spec is None:
            self._validate_anchors()
            self._compute_reached_tables()
            self._spec = self._build_export_spec()
        return self._spec

    @property
    def reached_tables(self) -> set[tuple[str, str]]:
        """The ``{(schema, table), ...}`` set the walk would include.

        Available after :meth:`build` or :meth:`get_export_spec`
        has been called. Useful for diagnostics — callers can
        confirm whether a particular table was in scope before
        spending the time on a real export.
        """
        return set(self._reached_tables)

    # ------------------------------------------------------------------
    # Anchor validation
    # ------------------------------------------------------------------

    def _validate_anchors(self) -> None:
        """Fail-fast on RIDAnchor RIDs that don't exist in the catalog.

        Issues one ``?RID=any(...)`` query per ``RIDAnchor`` table
        directly against ERMrest (the datapath ``_ColumnWrapper``
        doesn't expose ``in_``), comparing the returned RIDs
        against the anchor's claimed list. Missing RIDs raise
        with the list of misses.

        :class:`TableAnchor` and :class:`PathAnchor` are not
        validated here — TableAnchor is permissive of empty
        tables; PathAnchor's empty result is warned but allowed
        at walk time.
        """
        from urllib.parse import quote as urlquote

        model = self._get_model()
        for anchor in self.anchors:
            if anchor.kind != AnchorKind.RID:
                continue
            assert isinstance(anchor, RIDAnchor)
            # Resolve the table → (schema, table). We accept bare
            # table names (deriva-ml tradition) and try each
            # candidate schema.
            schema_name, table_name = self._resolve_table(
                model, anchor.table
            )
            # ERMrest's ``RID=any(v1,v2,...)`` filter returns just
            # the RIDs that actually exist. One query per anchor.
            rid_list = ",".join(urlquote(r) for r in anchor.rids)
            path = (
                f"/attribute/{schema_name}:{table_name}"
                f"/RID=any({rid_list})/RID"
            )
            try:
                response = self.catalog.get(path)
                response.raise_for_status()
                results = response.json()
            except Exception as e:
                # If the catalog query fails for reasons unrelated
                # to anchor correctness (auth, network), surface
                # the error rather than masking it as "anchors
                # missing".
                logger.error(
                    "Anchor validation failed for %s: %s",
                    anchor.table,
                    e,
                )
                raise
            found = {r["RID"] for r in results if r.get("RID")}
            missing = [rid for rid in anchor.rids if rid not in found]
            if missing:
                raise ValueError(
                    f"RIDAnchor on {schema_name}.{table_name} names "
                    f"{len(missing)} RID(s) not present in the source "
                    f"catalog: {missing[:10]!r}"
                    + (" ..." if len(missing) > 10 else "")
                )

    def _resolve_table(
        self, model: Any, table_name: str
    ) -> tuple[str, str]:
        """Find ``(schema, table)`` for an anchor's bare table name."""
        # Accept ``schema.table`` form directly.
        if "." in table_name:
            schema, name = table_name.split(".", 1)
            if (
                schema in model.schemas
                and name in model.schemas[schema].tables
            ):
                return schema, name
            raise KeyError(
                f"Table {table_name!r} not found in catalog schemas"
            )
        # Bare name: search through scoped schemas only (so anchors
        # don't accidentally resolve to system tables).
        candidates: list[tuple[str, str]] = []
        for schema_name, schema in model.schemas.items():
            if self._is_excluded_schema(schema_name):
                continue
            if table_name in schema.tables:
                candidates.append((schema_name, table_name))
        if not candidates:
            raise KeyError(
                f"Table {table_name!r} not found in catalog schemas"
            )
        if len(candidates) > 1:
            raise ValueError(
                f"Ambiguous table name {table_name!r}: matches "
                f"{sorted(candidates)}"
            )
        return candidates[0]

    # ------------------------------------------------------------------
    # FK walk → reachable-table set
    # ------------------------------------------------------------------

    def _compute_reached_tables(self) -> None:
        """BFS the FK graph from the anchor set, honoring the policy.

        The walk is bidirectional (FKs followed in both directions)
        except for vocabulary tables, which are entered but not
        exited — preventing the Subject → Species →
        every-other-Subject explosion.

        Records two side outputs in addition to ``_reached_tables``:

        * :attr:`_anchor_tables` — the anchor-set tables themselves.
        * :attr:`_table_paths` — the FK-walk path from the *first*
          anchor that reached each table. Used by
          :meth:`_table_query_path` to scope each non-anchor table's
          query to rows reachable via that FK path. BFS guarantees
          the recorded path is one of the shortest, which is what
          we want for an anchor-scoped slice.
        """
        from collections import deque

        model = self._get_model()

        # Walk strategy:
        #
        # - The queue carries one (table, path) entry per FK route
        #   we've discovered to that table. We dequeue *all* routes
        #   to a given table even when the table itself is already
        #   in ``reached_tables`` — that's the whole point of
        #   multi-path emission. The simple-path guard in
        #   :meth:`_enqueue_if_in_scope_with_path` (drops a path
        #   that would re-enter a table it already visits) prevents
        #   infinite walks on cycles.
        # - ``paths`` records the BFS-shortest path discovered per
        #   reached table (backwards-compat with single-path callers).
        # - ``path_set`` records every distinct simple path
        #   discovered per reached table so
        #   :meth:`_build_export_spec` can emit one query_processor
        #   per FK route. Bounded by ``max_paths`` to keep the spec
        #   finite on densely-connected catalogs.
        queue: deque[
            tuple[str, str, int, tuple[tuple[str, str], ...]]
        ] = deque()
        reached: set[tuple[str, str]] = set()
        anchor_tables: set[tuple[str, str]] = set()
        paths: dict[tuple[str, str], list[tuple[str, str]]] = {}
        path_set: dict[
            tuple[str, str], list[tuple[tuple[str, str], ...]]
        ] = {}

        for anchor in self.anchors:
            schema_name, table_name = self._resolve_table(
                model, anchor.table
            )
            key = (schema_name, table_name)
            anchor_tables.add(key)
            if key not in paths:
                paths[key] = [key]
            path_set.setdefault(key, [])
            anchor_path: tuple[tuple[str, str], ...] = (key,)
            if anchor_path not in path_set[key]:
                path_set[key].append(anchor_path)
            queue.append((schema_name, table_name, 0, anchor_path))

        max_depth = self.policy.max_depth
        max_paths = self._max_paths_per_table()
        while queue:
            schema_name, table_name, depth, current_path = queue.popleft()
            key = (schema_name, table_name)
            reached.add(key)

            # Depth bound (None = unbounded).
            if max_depth is not None and depth >= max_depth:
                continue

            table = model.schemas[schema_name].tables[table_name]
            # Terminal tables: traverse INTO but not OUT.
            # - Vocabulary tables (detected by canonical column
            #   shape) are always terminal: following inbound FKs
            #   from a vocab term would chase every row in the
            #   catalog that uses it.
            # - Tables in :attr:`FKTraversalPolicy.terminal_tables`
            #   get the same treatment. Used for "provenance" tables
            #   like ``Execution`` that aggregate cross-anchor state
            #   — walking *through* them mixes anchor scopes via
            #   shared rows. Rows still land in the slice (other
            #   rows' FKs into the terminal table resolve) but the
            #   walker doesn't fan out from them.
            if table.is_vocabulary():
                continue
            if (schema_name, table_name) in self.policy.terminal_tables:
                continue

            # Outbound: FKs we declare to other tables.
            for fk in table.foreign_keys:
                self._enqueue_if_in_scope_with_path(
                    fk.pk_table,
                    depth + 1,
                    current_path,
                    queue,
                    paths,
                    path_set,
                    max_paths,
                )
            # Inbound: FKs other tables declare to us.
            for fk in table.referenced_by:
                self._enqueue_if_in_scope_with_path(
                    fk.table,
                    depth + 1,
                    current_path,
                    queue,
                    paths,
                    path_set,
                    max_paths,
                )

        self._reached_tables = reached
        self._anchor_tables = anchor_tables
        self._table_paths = paths
        self._table_path_set = path_set

    def _max_paths_per_table(self) -> int:
        """Cap on distinct FK paths emitted per target table.

        Multi-path emission walks every simple route; densely
        connected schemas (m FKs between n tables) can in theory
        produce many. The cap is a finiteness guard, not a tuning
        knob — in practice every catalog we've seen tops out at
        2–3 paths to any given table. The default is generous so
        the cap is never the explanation for missing rows.
        """
        return 16

    def _enqueue_if_in_scope_with_path(
        self,
        table: DerivaTable,
        depth: int,
        prefix_path: tuple[tuple[str, str], ...],
        queue: "deque[tuple[str, str, int, tuple[tuple[str, str], ...]]]",
        paths: dict[tuple[str, str], list[tuple[str, str]]],
        path_set: dict[
            tuple[str, str], list[tuple[tuple[str, str], ...]]
        ],
        max_paths: int,
    ) -> None:
        """Queue a candidate with its FK-path prefix, if policy allows.

        Records the BFS-shortest path on first sight (in ``paths``)
        plus every distinct simple path discovered (in ``path_set``,
        bounded by ``max_paths``). The caller's BFS still gates
        the *expansion* of each table by ``visited``; this helper
        only records the route the walk arrived by.

        Cycles are guarded by the simple-path test (``key in
        prefix_path``): a path that would re-enter a table it
        already visits is dropped.
        """
        schema_name = table.schema.name
        table_name = table.name
        key = (schema_name, table_name)
        if self._is_excluded_schema(schema_name):
            return
        if self._is_excluded_table(schema_name, table_name):
            return
        if (
            self.policy.schemas is not None
            and schema_name not in self.policy.schemas
        ):
            return
        if key in prefix_path:
            # Simple-path guard: don't walk back into a table we've
            # already used on this path. ERMrest joins on natural
            # FK relationships and the same join twice would be a
            # loop in the query.
            return
        candidate_path: tuple[tuple[str, str], ...] = prefix_path + (key,)
        if key not in paths:
            paths[key] = list(candidate_path)
        bucket = path_set.setdefault(key, [])
        if candidate_path in bucket:
            # We've already enqueued this exact path; the dequeue
            # will walk its descendants once. Re-enqueueing would
            # do redundant work.
            return
        if len(bucket) >= max_paths:
            # Path budget exhausted; record the new path is dropped
            # rather than silently emitting an over-large spec.
            logger.debug(
                "max_paths=%d reached for %s.%s; dropping path %s",
                max_paths, schema_name, table_name, candidate_path,
            )
            return
        bucket.append(candidate_path)
        queue.append((schema_name, table_name, depth, candidate_path))

    def _is_excluded_schema(self, schema_name: str) -> bool:
        return (
            schema_name in _SYSTEM_SCHEMAS
            or schema_name in self.policy.exclude_schemas
        )

    def _is_excluded_table(
        self, schema_name: str, table_name: str
    ) -> bool:
        return (schema_name, table_name) in self.policy.exclude_tables

    # ------------------------------------------------------------------
    # Export-spec generation
    # ------------------------------------------------------------------

    def _build_export_spec(self) -> dict[str, Any]:
        """Translate the reachable-table set into a deriva-py export spec.

        The spec follows the same shape :class:`GenericDownloader`
        consumes today (the same shape ``CatalogGraph.generate_dataset_download_spec``
        produces in deriva-ml). Differences:

        - We accept a more general anchor set (not just one
          ``Dataset`` row), so the filter is parameterized.
        - We honor :class:`FKTraversalPolicy` fields uniformly
          rather than dataset-specific rules.
        - We stamp the deriva-bag profile identifier into
          ``bag_metadata`` so downstream consumers can validate.
        """
        model = self._get_model()
        catalog_id = self.catalog.catalog_id
        host_url = (
            f"{self.catalog.deriva_server.scheme}://"
            f"{self.catalog.deriva_server.server}"
        )

        query_processors: list[dict[str, Any]] = []

        # data/schema.json — env+json processor.
        query_processors.append(
            {
                "processor": "json",
                "processor_params": {
                    "query_path": "/schema",
                    "output_path": "schema",
                },
            }
        )

        # One CSV processor per reached table. The query path
        # restricts to the rows reachable from the anchor set:
        # we issue a "table" query first (so the engine has the
        # full row set) and let the FK walk in the bag's
        # subsequent ingest define which rows are kept.
        #
        # For tables where we have an explicit anchor (RID or
        # path), filter on the anchor's RID list. For tables
        # reached via FK only, we fetch the full intersection
        # ERMrest computes — which is what callers want for
        # "give me everything reachable from these anchors."
        anchor_rid_filter = self._anchor_rid_filters()

        for schema_name, table_name in sorted(self._reached_tables):
            table_obj = model.schemas[schema_name].tables[table_name]
            key = (schema_name, table_name)

            # Vocabulary tables with ``vocab_export == FULL`` get
            # the unfiltered ``/entity/{schema}:{table}`` query —
            # the full controlled vocabulary regardless of which
            # terms the slice happens to reference. This is what
            # a clone-style consumer wants: every FK reference into
            # a vocab table must resolve at the destination. One
            # processor, no per-path branching.
            is_vocab_full = (
                table_obj.is_vocabulary()
                and self.policy.vocab_export == VocabExport.FULL
            )
            if is_vocab_full:
                query_processors.append(
                    {
                        "processor": "csv",
                        "processor_params": {
                            "query_path": (
                                f"/entity/{schema_name}:{table_name}"
                            ),
                            "output_path": (
                                f"{schema_name}/{table_name}"
                            ),
                            "paged_query": True,
                        },
                    }
                )
            else:
                # Non-vocab (or REFERENCED_ONLY vocab): emit one
                # processor per FK path discovered by the walker.
                # The bag's loader (BagDatabase._load_data) reads
                # every file that resolves to ``{schema}.{table}``
                # and unions rows by RID — see deriva/bag/database.py.
                #
                # ``output_path`` carries the path's table chain so
                # multi-path CSVs land at distinct on-disk locations
                # under ``data/{schema}/``. The loader's index is
                # keyed by CSV stem only, so a per-route subdirectory
                # is the natural separator.
                for fk_path in self._fk_paths_for(key):
                    qpath = self._table_query_path(
                        schema_name,
                        table_name,
                        anchor_rid_filter,
                        fk_path,
                    )
                    dest = self._output_path_for(
                        schema_name, table_name, fk_path
                    )
                    query_processors.append(
                        {
                            "processor": "csv",
                            "processor_params": {
                                "query_path": qpath,
                                "output_path": dest,
                                "paged_query": True,
                            },
                        }
                    )
            if table_obj.is_asset():
                # The engine's ``fetch`` processor reads URL/length/
                # filename/md5 columns from each asset row and
                # downloads the bytes to the templated output_path.
                # One fetch processor per table is enough — assets
                # are addressed by RID at the destination so the
                # rows the multi-path CSVs landed already carry
                # everything ``fetch`` needs.
                query_processors.append(
                    {
                        "processor": "fetch",
                        "processor_params": {
                            "query_path": (
                                f"/attribute/{schema_name}:{table_name}"
                                f"/url:=URL,length:=Length,"
                                f"filename:=Filename,md5:=MD5,"
                                f"asset_rid:=RID"
                            ),
                            "output_path": (
                                f"asset/{{asset_rid}}/{table_name}"
                            ),
                        },
                    }
                )

        spec: dict[str, Any] = {
            "bag": {
                "bag_name": self.output_dir.name,
                "bag_algorithms": ["md5", "sha256"],
                "bag_archiver": "zip",
                "bag_metadata": {
                    "BagIt-Profile-Identifier": BAGIT_PROFILE_IDENTIFIER,
                },
                "bag_idempotent": True,
            },
            "catalog": {
                "host": host_url,
                "catalog_id": catalog_id,
                "query_processors": query_processors,
            },
        }

        return spec

    def _anchor_rid_filters(self) -> dict[tuple[str, str], list[str]]:
        """Return the ``{(schema, table): [rid, ...]}`` filter table.

        For RID and Path anchors, callers want the bag restricted
        to those starting rows plus their FK neighborhood. The
        export engine doesn't compute "FK neighborhood" itself —
        each query_processors entry is an independent query — so
        we encode the anchor restriction directly on the anchor
        table's CSV query and let other reached tables fetch all
        of their (visible) rows. The neighborhood emerges from
        the row-level access policies the catalog enforces.
        """
        filters: dict[tuple[str, str], list[str]] = {}
        model = self._get_model()
        for anchor in self.anchors:
            schema_name, table_name = self._resolve_table(
                model, anchor.table
            )
            if isinstance(anchor, RIDAnchor):
                filters.setdefault(
                    (schema_name, table_name), []
                ).extend(anchor.rids)
            elif isinstance(anchor, PathAnchor):
                filters.setdefault(
                    (schema_name, table_name), []
                ).extend(anchor.rids)
            # TableAnchor: no filter (full table).
        # Dedup each filter list while preserving order.
        return {
            key: list(dict.fromkeys(rids))
            for key, rids in filters.items()
        }

    def _fk_paths_for(
        self, key: tuple[str, str]
    ) -> list[tuple[tuple[str, str], ...]]:
        """Return the FK paths the walker discovered to ``key``.

        Falls back to ``[(key,)]`` for callers (mainly tests) that
        invoke export-spec generation without having recorded any
        paths — produces the legacy single-path full-table query.
        """
        bucket = self._table_path_set.get(key)
        if bucket:
            return bucket
        # Fall back to the BFS-shortest path or the table itself.
        fallback = self._table_paths.get(key)
        if fallback:
            return [tuple(fallback)]
        return [(key,)]

    @staticmethod
    def _output_path_for(
        schema_name: str,
        table_name: str,
        fk_path: tuple[tuple[str, str], ...],
    ) -> str:
        """Build a per-path on-disk subdirectory under ``data/``.

        Single-element paths (anchor tables) land at
        ``{schema}/{table}``; multi-element paths land at
        ``{schema}/{p1}_..._{pn-1}/{table}`` so two routes to the
        same target table land in distinct files. The bag DB's
        loader keys by CSV stem (``{table}.csv``), so per-route
        subdirectories are enough to keep file paths unique.
        """
        if len(fk_path) <= 1:
            return f"{schema_name}/{table_name}"
        intermediate = "_".join(
            seg_table for _seg_schema, seg_table in fk_path[:-1]
        )
        return f"{schema_name}/{intermediate}/{table_name}"

    def _table_query_path(
        self,
        schema_name: str,
        table_name: str,
        anchor_filters: dict[tuple[str, str], list[str]],
        fk_path: tuple[tuple[str, str], ...] | None = None,
    ) -> str:
        """Build the ERMrest query path for a reached table.

        Three cases:

        1. **Anchor table with a RID/Path filter.** Filter by RID
           list against the anchor's table directly:
           ``/entity/{schema}:{table}/RID=any(rid1,rid2,...)``.
        2. **Anchor table with no filter (TableAnchor).** Fetch
           every row: ``/entity/{schema}:{table}``.
        3. **Non-anchor table reached via the FK walk.** Build a
           chained ERMrest path that scopes the rows to those
           reachable from the anchor's filter through the FK path
           the caller specifies:
           ``/entity/{anchor}/RID=any(...)/{step2}/{step3}/...``.
           ERMrest's natural-FK join semantics handle the joins.

        Case (3) is the one that prevents anchor-scoped slices from
        over-fetching. Without it, a single Dataset RID anchor pulls
        every Dataset_Version row (regardless of which Dataset they
        reference) and the loader's dangling-FK strategy then fires
        on the rows whose parents aren't in the slice.
        """
        key = (schema_name, table_name)
        if fk_path is None:
            fk_path = tuple(self._table_paths.get(key, [key]))

        # Case 1/2: this *is* an anchor table. Use the existing
        # anchor-RID filter (or fall back to the full-table query
        # for TableAnchor / non-RID anchors). Path length 1 means
        # the caller supplied just the anchor itself.
        if key in self._anchor_tables and len(fk_path) <= 1:
            rids = anchor_filters.get(key)
            if rids:
                joined = ",".join(rids)
                return (
                    f"/entity/{schema_name}:{table_name}"
                    f"/RID=any({joined})"
                )
            return f"/entity/{schema_name}:{table_name}"

        # Case 3: this table was reached via the FK walk from one
        # of the anchor tables. Build a chained path. The first
        # segment carries the anchor's RID filter (when the anchor
        # is a RIDAnchor/PathAnchor); subsequent segments are
        # bare ``{schema}:{table}`` joins that ERMrest resolves via
        # the natural FK relationship.
        anchor_key = fk_path[0]
        anchor_schema, anchor_table = anchor_key
        rids = anchor_filters.get(anchor_key)
        if rids:
            joined = ",".join(rids)
            head = (
                f"/entity/{anchor_schema}:{anchor_table}"
                f"/RID=any({joined})"
            )
        else:
            head = f"/entity/{anchor_schema}:{anchor_table}"
        # Append the rest of the path (skipping the anchor itself).
        tail = "".join(
            f"/{seg_schema}:{seg_table}" for seg_schema, seg_table in fk_path[1:]
        )
        return head + tail

    # ------------------------------------------------------------------
    # Export-engine driver
    # ------------------------------------------------------------------

    def _run_export(self, spec: dict[str, Any]) -> Path:
        """Invoke :class:`GenericDownloader` with the generated spec."""
        # Local import — the export engine pulls in a lot of
        # transitive deps; callers who only want
        # ``get_export_spec`` (e.g., for inspection) shouldn't
        # have to pay that cost.
        from deriva.transfer.download.deriva_download import (
            GenericDownloader,
        )

        deriva_server = self.catalog.deriva_server
        downloader = GenericDownloader(
            server={
                "host": deriva_server.server,
                "protocol": deriva_server.scheme,
                "catalog_id": str(self.catalog.catalog_id),
            },
            config=spec,
            output_dir=str(self.output_dir),
            credentials=self.catalog._credentials,
        )
        downloader.download()

        # GenericDownloader returns nothing meaningful; the bag
        # lives at output_dir/{bag_name}. We named the bag
        # output_dir.name in the spec, so that's where it lands.
        bag_path = self.output_dir / self.output_dir.name
        if not bag_path.exists():
            # Fall back to scanning for a single sub-directory —
            # some downloader versions name the bag differently.
            candidates = [
                p for p in self.output_dir.iterdir() if p.is_dir()
            ]
            if len(candidates) == 1:
                bag_path = candidates[0]
        return bag_path

    def _write_provenance_file(self) -> None:
        """Stamp the bag's metadata/ with anchor + policy info."""
        anchors_serialized = [a.model_dump(mode="json") for a in self.anchors]
        policy_serialized = self.policy.model_dump(mode="json")
        bag_path = self.output_dir / self.output_dir.name
        if not bag_path.exists():
            return
        write_provenance(
            bag_path,
            producer=self.producer,
            anchors=anchors_serialized,
            policy=policy_serialized,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_model(self) -> Any:
        """Cache the catalog model — one round-trip per CatalogBagBuilder."""
        if self._model is None:
            self._model = self.catalog.getCatalogModel()
        return self._model


__all__ = ["CatalogBagBuilder"]
