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
    RIDAnchor,
    TableAnchor,
)
from deriva.bag.path_walker import SchemaPathWalker
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
        rid_sets: Optional ``{(schema, table): [RID, ...]}`` map. When
            supplied, the export spec emits one rid-set csv processor per
            reached non-vocab table (flat ``{schema}/{table}`` output,
            RID set carried inline) instead of one csv processor per FK
            path. The engine chunks the RID set and appends to a single
            clean CSV, so the loader gets one file per table with no
            union needed. When ``None`` (the default), per-FK-path
            emission is used unchanged. Vocab-FULL and asset-fetch
            processors are unaffected.

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
        rid_sets: dict[tuple[str, str], list[str]] | None = None,
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
        self.rid_sets = rid_sets

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

    def iter_table_datapaths(
        self,
    ) -> dict[tuple[str, str], list[tuple[Any, Any, bool]]]:
        """Return live-catalog datapaths for every reached FK path.

        For each table the walk reached, return one
        :class:`~deriva.core.datapath.DataPath` per FK route from
        an anchor to that table. Callers evaluate the datapath
        against the live catalog (optionally layering additional
        filters first) and consume the results — typically as
        RID-union semantics across the multiple paths to the same
        target.

        This is the read-only sibling of :meth:`build`: same FK
        walk, same anchor-RID filtering, but instead of writing a
        bag it hands back evaluable datapath objects. Used by
        callers that need to *count* or *check predicates against*
        the bag's contents without paying the cost of materializing
        the bag (drift detection, size estimation, anchor-scoped
        membership tests).

        Anchor filtering is applied at the root of each datapath:
        :class:`RIDAnchor` produces a
        ``filter(table.RID.in_(rids))`` predicate on the anchor
        table; :class:`TableAnchor` produces an unfiltered root.
        Subsequent path segments use :meth:`pathBuilder.link` with
        explicit ``on=`` clauses for composite FKs and implicit
        resolution for simple FKs.

        The walk is run on first call (or first :meth:`build` /
        :meth:`get_export_spec` call) and the reached-tables /
        paths state is cached on the instance. The datapath objects
        themselves are constructed fresh on each call — they're
        cheap, and callers may want to apply different per-call
        filters (e.g., ``RMT > T_release`` for drift detection).

        Returns:
            ``{(schema, table): [(datapath, pb_table, is_asset), ...]}``
            keyed by reached table. Each value is a list with one
            entry per FK route reaching that table. ``datapath`` is
            an evaluable :class:`~deriva.core.datapath.DataPath`;
            ``pb_table`` is the terminal table's pathBuilder handle
            (needed for column access in caller-side filters);
            ``is_asset`` mirrors :meth:`Table.is_asset` on the
            terminal table.

        Example:
            Count members in an anchor-scoped slice::

                cb = CatalogBagBuilder(catalog=cat, anchors=anchors,
                                       output_dir=tmp)
                table_dps = cb.iter_table_datapaths()
                for (schema, table), entries in table_dps.items():
                    rids = set()
                    for dp, pb_table, _is_asset in entries:
                        rids.update(
                            row["RID"]
                            for row in dp.attributes(pb_table.RID).fetch()
                        )
                    print(f"{schema}.{table}: {len(rids)} unique rows")
        """
        if not self._reached_tables:
            self._validate_anchors()
            self._compute_reached_tables()

        model = self._get_model()
        pb = self.catalog.getPathBuilder()
        anchor_filters = self._anchor_rid_filters()

        out: dict[tuple[str, str], list[tuple[Any, Any, bool]]] = {}
        for key in sorted(self._reached_tables):
            schema_name, table_name = key
            target_table = model.schemas[schema_name].tables[table_name]
            target_pb = pb.schemas[schema_name].tables[table_name]
            is_asset = target_table.is_asset()
            entries: list[tuple[Any, Any, bool]] = []

            for fk_path in self._fk_paths_for(key):
                dp = self._build_datapath_for_path(
                    pb, model, fk_path, anchor_filters
                )
                entries.append((dp, target_pb, is_asset))

            out[key] = entries
        return out

    def iter_reached_paths(
        self,
    ) -> dict[tuple[str, str], list[tuple[tuple[str, str], ...]]]:
        """Return the symbolic FK paths the walker discovered.

        For each ``(schema, table)`` the walk reached, return the list
        of FK paths from an anchor table to that target. Each path is
        a tuple of ``(schema, table)`` segments starting at the anchor
        and ending at the target. Read-only sibling of
        :meth:`iter_table_datapaths`: same walk, same anchor scoping,
        but returns the *symbolic* path set (tuples of names) rather
        than live datapath objects.

        Used by consumers that need to emit *symbolic* path expressions
        — e.g., Chaise export annotations that must work for any future
        row, not a specific RID. The annotation pipeline in deriva-ml
        consumes this method to map each reached table chain to a
        Chaise ``source`` dict.

        The walk is run on first call (or first :meth:`build` /
        :meth:`get_export_spec` / :meth:`iter_table_datapaths` call)
        and cached on the instance. The returned dict is a fresh copy
        — callers may mutate it.

        Returns:
            ``{(schema, table): [fk_path, ...]}`` keyed by reached
            table. Each ``fk_path`` is a tuple of ``(schema, table)``
            segments. Single-element paths denote anchor tables;
            multi-element paths denote FK-reached tables.

        Example:
            Enumerate every symbolic chain from a Dataset anchor::

                cb = CatalogBagBuilder(
                    catalog=cat,
                    anchors=[TableAnchor(table="Dataset")],
                    output_dir=tmp,
                )
                for (schema, table), paths in cb.iter_reached_paths().items():
                    for path in paths:
                        chain = "/".join(f"{s}:{t}" for s, t in path)
                        print(f"{schema}.{table} via {chain}")
        """
        if not self._reached_tables:
            self._validate_anchors()
            self._compute_reached_tables()
        return {
            key: list(self._fk_paths_for(key))
            for key in sorted(self._reached_tables)
        }

    # ------------------------------------------------------------------
    # Anchor validation
    # ------------------------------------------------------------------

    def _validate_anchors(self) -> None:
        """Fail-fast on RIDAnchor RIDs that don't exist in the catalog.

        Issues one query per ``RIDAnchor`` table through the
        deriva-py path builder, using the ``column.in_(values)``
        operator added in deriva-py #242. The query fetches just
        the RIDs that exist; missing RIDs raise with the list of
        misses.

        :class:`TableAnchor` is not validated here — empty tables
        are a legitimate outcome.
        """
        model = self._get_model()
        pb = self.catalog.getPathBuilder()
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
            try:
                table_path = pb.schemas[schema_name].tables[table_name]
                results = list(
                    table_path.filter(table_path.RID.in_(anchor.rids))
                    .attributes(table_path.RID)
                    .fetch()
                )
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
        """Run the shared FK walk from the anchor set.

        Delegates the bidirectional BFS to
        :class:`~deriva.bag.path_walker.SchemaPathWalker`. The walker
        carries the scope rules (schema allow/deny, table deny,
        terminal-tables, ``max_depth``) and the per-target path-set
        recording; this method translates anchors to roots, calls the
        walker, and populates the builder's caches.

        Vocab tables are treated as leaves (entered but not exited)
        and the ``max_paths`` cap (16 by default) keeps the spec
        finite on densely-connected catalogs.
        """
        model = self._get_model()

        # Translate anchors → (schema, table) roots and remember
        # which keys are anchors so :meth:`_table_query_path` can
        # detect them.
        anchor_tables: set[tuple[str, str]] = set()
        roots: list[tuple[str, str]] = []
        for anchor in self.anchors:
            key = self._resolve_table(model, anchor.table)
            anchor_tables.add(key)
            roots.append(key)

        walker = SchemaPathWalker(model=model, policy=self.policy)
        path_set = walker.walk_bfs(
            roots, max_paths_per_target=16
        )

        # Backwards-compat shape: ``_table_paths`` is a flat
        # ``{key: list[(schema, table)]}`` — the BFS-shortest path
        # for each reached target. Pick the first recorded route
        # (BFS order = shortest-first).
        paths: dict[tuple[str, str], list[tuple[str, str]]] = {
            key: list(routes[0]) for key, routes in path_set.items()
        }

        self._reached_tables = set(path_set.keys())
        self._anchor_tables = anchor_tables
        self._table_paths = paths
        self._table_path_set = path_set

    def _is_excluded_schema(self, schema_name: str) -> bool:
        return (
            schema_name in DEFAULT_EXCLUDE_SCHEMAS
            or schema_name in self.policy.exclude_schemas
        )

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
            elif self.rid_sets is not None and not table_obj.is_vocabulary():
                # Format B: one rid-set csv processor per table — flat
                # output_path, RID set carried inline. The engine chunks
                # the RID set and appends to one clean CSV (get_as_file
                # rid_set). Replaces the per-FK-path emission; the loader
                # gets one file per table (no union needed).
                #
                # Vocab tables are excluded from this branch: they're
                # referenced by Name, not RID, so a reachability map
                # carries no RID set for them. A REFERENCED_ONLY vocab
                # must fall through to the per-FK-path ``else`` below, or
                # it would get an empty rid-set CSV and its FK references
                # wouldn't resolve at the destination.
                if key not in self.rid_sets:
                    logger.warning(
                        "rid_sets has no entry for reached table %s:%s; "
                        "emitting empty rid-set CSV (possible incomplete "
                        "reachability map)",
                        schema_name,
                        table_name,
                    )
                rids = self.rid_sets.get(key, [])
                query_processors.append(
                    {
                        "processor": "csv",
                        "processor_params": {
                            "rid_table": f"{schema_name}:{table_name}",
                            "rid_set": rids,
                            "output_path": f"{schema_name}/{table_name}",
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
        # is a RIDAnchor); subsequent segments are
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

    def _build_datapath_for_path(
        self,
        pb: Any,
        model: Any,
        fk_path: tuple[tuple[str, str], ...],
        anchor_filters: dict[tuple[str, str], list[str]],
    ) -> Any:
        """Construct a linked datapath for one FK route.

        Applies the anchor's RID filter at the root (when the
        first segment is a :class:`RIDAnchor` table) and chains
        :meth:`pathBuilder.link` calls for each
        subsequent segment. Composite FKs are linked with an
        explicit ``on=`` clause derived from the model; simple FKs
        rely on datapath's implicit resolution.

        Used by :meth:`iter_table_datapaths`. Mirrors the
        FK-linking logic that :meth:`_table_query_path` produces as
        an ERMrest URL string, but produces an evaluable datapath
        object instead.

        Args:
            pb: Live :meth:`ErmrestCatalog.getPathBuilder` handle.
            model: Live :meth:`ErmrestCatalog.getCatalogModel` handle.
            fk_path: The FK route as a tuple of ``(schema, table)``
                segments. Single-segment paths produce a root-only
                datapath; multi-segment paths chain ``link`` calls.
            anchor_filters: ``{(schema, table): [rid, ...]}`` from
                :meth:`_anchor_rid_filters`. RIDs for the root
                segment, if any, become a ``filter(RID.in_(rids))``
                predicate.

        Returns:
            A datapath object ready to evaluate against the
            catalog.
        """
        root_schema, root_table = fk_path[0]
        root_pb = pb.schemas[root_schema].tables[root_table]

        rids = anchor_filters.get((root_schema, root_table))
        if rids:
            dp = root_pb.filter(root_pb.RID.in_(rids))
        else:
            dp = root_pb

        prev_segment = fk_path[0]
        prev_pb = root_pb
        for segment in fk_path[1:]:
            seg_schema, seg_table = segment
            cur_pb = pb.schemas[seg_schema].tables[seg_table]
            on_clause = self._composite_fk_on_clause(
                model, prev_segment, segment, prev_pb, cur_pb
            )
            if on_clause is not None:
                dp = dp.link(cur_pb, on=on_clause)
            else:
                dp = dp.link(cur_pb)
            prev_segment = segment
            prev_pb = cur_pb

        return dp

    @staticmethod
    def _composite_fk_on_clause(
        model: Any,
        prev_segment: tuple[str, str],
        cur_segment: tuple[str, str],
        prev_pb: Any,
        cur_pb: Any,
    ) -> Any | None:
        """Return an explicit ``on=`` clause for composite FKs, else ``None``.

        Datapath resolves simple (single-column) FKs implicitly:
        ``a.link(b)`` figures out the column pair on its own.
        Composite FKs (multi-column primary/foreign keys) are
        ambiguous because datapath can't tell which combination of
        columns the caller meant; an explicit ``on=`` clause is
        required.

        This method inspects the deriva-py :class:`Model` for FK
        constraints between the two segments. If exactly one FK
        constraint exists and it's single-column, return ``None``
        and let datapath resolve implicitly. If the constraint is
        multi-column, build an ``AND`` of equality predicates
        across the column pairs and return that.

        Multi-FK ambiguity (two distinct FK constraints between
        the same two tables) is **not** resolved here — the walker
        records each FK route as a separate path, so multi-FK
        ambiguity surfaces as multiple entries in
        :attr:`_table_path_set`, each handled independently. This
        method only resolves *intra-FK* composite-column joins.

        Args:
            model: Live catalog model.
            prev_segment: Source ``(schema, table)``.
            cur_segment: Target ``(schema, table)``.
            prev_pb / cur_pb: PathBuilder handles for the two
                tables.

        Returns:
            The composite ``on=`` predicate, or ``None`` for simple
            FKs.
        """
        prev_schema, prev_table = prev_segment
        cur_schema, cur_table = cur_segment
        prev_model_table = model.schemas[prev_schema].tables[prev_table]
        cur_model_table = model.schemas[cur_schema].tables[cur_table]

        # Find the FK constraint(s) linking the two tables in
        # either direction. Each constraint is a list of
        # ``(prev_col, cur_col)`` pairs.
        constraints: list[list[tuple[Any, Any]]] = []
        for fk in prev_model_table.foreign_keys:
            if fk.pk_table is cur_model_table:
                constraints.append(
                    list(
                        zip(
                            fk.foreign_key_columns,
                            fk.referenced_columns,
                        )
                    )
                )
        for fk in prev_model_table.referenced_by:
            if fk.table is cur_model_table:
                # FK points cur → prev; flip so pairs read
                # (prev_col, cur_col) consistently.
                constraints.append(
                    list(
                        zip(
                            fk.referenced_columns,
                            fk.foreign_key_columns,
                        )
                    )
                )

        # Datapath resolves implicitly when there's exactly one
        # constraint and it's single-column. Multi-constraint
        # ambiguity is the walker's concern (it records the
        # separate routes); single-column unambiguous links
        # don't need an explicit on=.
        if not constraints:
            return None
        if len(constraints) > 1:
            # Multiple FK constraints between the same pair of
            # tables; the walker records each as its own path, so
            # at this point we're handling exactly one of them.
            # Without additional context we can't tell which one
            # this is; let datapath resolve implicitly. The walker
            # is responsible for ensuring the path set captures
            # the relevant routes.
            return None
        pairs = constraints[0]
        if len(pairs) <= 1:
            return None

        # Composite: build an AND of equality predicates.
        conditions = []
        for prev_col, cur_col in pairs:
            left = getattr(prev_pb, prev_col.name)
            right = getattr(cur_pb, cur_col.name)
            conditions.append(left == right)
        combined = conditions[0]
        for cond in conditions[1:]:
            combined = combined & cond
        return combined

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
            credentials=self.catalog.get_credentials(),
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
