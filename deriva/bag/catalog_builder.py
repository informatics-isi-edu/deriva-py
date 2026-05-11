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
from deriva.bag.traversal import FKTraversalPolicy, VocabExport

logger = logging.getLogger(__name__)


#: System schemas the walker always excludes, on top of whatever
#: the policy specifies.
_SYSTEM_SCHEMAS: frozenset[str] = frozenset(
    {"public", "_acl_admin", "WWW"}
)


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

        Issues one ``?RID=in(...)`` query per ``RIDAnchor`` table,
        comparing the returned RIDs against the anchor's claimed
        list. Missing RIDs raise with the list of misses.

        :class:`TableAnchor` and :class:`PathAnchor` are not
        validated here — TableAnchor is permissive of empty
        tables; PathAnchor's empty result is warned but allowed
        at walk time.
        """
        pb = self.catalog.getPathBuilder()
        model = self._get_model()
        for anchor in self.anchors:
            if anchor.kind != AnchorKind.RID:
                continue
            assert isinstance(anchor, RIDAnchor)
            # Resolve the table → (schema, table) so we can build
            # a pb path. We accept bare table names (deriva-ml
            # tradition) and try each candidate schema.
            schema_name, table_name = self._resolve_table(
                model, anchor.table
            )
            entity_path = pb.schemas[schema_name].tables[table_name]
            # filter().fetch() with the RID in-set returns just
            # the RIDs that actually exist.
            try:
                results = list(
                    entity_path.filter(
                        entity_path.RID == anchor.rids[0]
                        if len(anchor.rids) == 1
                        else entity_path.RID.in_(anchor.rids)
                    ).attributes(entity_path.RID).fetch()
                )
            except Exception as e:  # pragma: no cover - network path
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
            found = {r["RID"] for r in results}
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
        """
        model = self._get_model()

        # Start with the anchored tables themselves.
        frontier: set[tuple[str, str, int]] = set()  # (schema, table, depth)
        visited: set[tuple[str, str]] = set()
        for anchor in self.anchors:
            schema_name, table_name = self._resolve_table(
                model, anchor.table
            )
            frontier.add((schema_name, table_name, 0))

        max_depth = self.policy.max_depth
        while frontier:
            current = frontier.pop()
            schema_name, table_name, depth = current
            key = (schema_name, table_name)
            if key in visited:
                continue
            visited.add(key)

            # Depth bound (None = unbounded).
            if max_depth is not None and depth >= max_depth:
                continue

            table = model.schemas[schema_name].tables[table_name]
            # Vocab tables: don't follow FKs back out of them.
            # We pulled in their referenced terms by getting here;
            # following inbound FKs from them would chase every
            # row that references this vocab.
            if table.is_vocabulary():
                continue

            # Outbound: FKs we declare to other tables.
            for fk in table.foreign_keys:
                pk_table = fk.pk_table
                self._enqueue_if_in_scope(
                    pk_table,
                    depth + 1,
                    frontier,
                    visited,
                )
            # Inbound: FKs other tables declare to us. Skip if we
            # are a vocab (vocabs don't propagate outward — see
            # above), but we already returned in that case.
            for fk in table.referenced_by:
                src_table = fk.table
                self._enqueue_if_in_scope(
                    src_table,
                    depth + 1,
                    frontier,
                    visited,
                )

        self._reached_tables = visited

    def _enqueue_if_in_scope(
        self,
        table: DerivaTable,
        depth: int,
        frontier: set[tuple[str, str, int]],
        visited: set[tuple[str, str]],
    ) -> None:
        """Add a candidate table to the BFS frontier if policy allows."""
        schema_name = table.schema.name
        table_name = table.name
        key = (schema_name, table_name)
        if key in visited:
            return
        if self._is_excluded_schema(schema_name):
            return
        if self._is_excluded_table(schema_name, table_name):
            return
        if (
            self.policy.schemas is not None
            and schema_name not in self.policy.schemas
        ):
            return
        frontier.add((schema_name, table_name, depth))

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
            # Asset tables get an extra ``fetch`` processor that
            # downloads the bytes to data/asset/{table}/{rid}/...
            # alongside their row CSV.
            qpath = self._table_query_path(
                schema_name, table_name, anchor_rid_filter
            )
            dest = f"{schema_name}/{table_name}"
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

            # vocab_export=FULL: emit a standalone CSV of every
            # term in the vocab, separate from the FK-bounded
            # query above. CatalogGraph._export_vocabulary today
            # does this for dataset bags.
            if (
                table_obj.is_vocabulary()
                and self.policy.vocab_export == VocabExport.FULL
            ):
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

    def _table_query_path(
        self,
        schema_name: str,
        table_name: str,
        anchor_filters: dict[tuple[str, str], list[str]],
    ) -> str:
        """Build the ERMrest query path for a reached table.

        - Anchored tables: filter by RID in the anchor list.
        - Other tables: fetch every row (the catalog's row-level
          policies decide what the caller sees).
        """
        rids = anchor_filters.get((schema_name, table_name))
        if rids:
            # ERMrest accepts ``filter=RID=v1;RID=v2;...`` and the
            # cleaner ``RID=any(v1,v2,...)``. ``any(...)`` is more
            # explicit; use it.
            joined = ",".join(rids)
            return (
                f"/entity/{schema_name}:{table_name}"
                f"/RID=any({joined})"
            )
        return f"/entity/{schema_name}:{table_name}"

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
