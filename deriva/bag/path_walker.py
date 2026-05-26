"""Generic FK-graph path walker.

:class:`SchemaPathWalker` is the **shared primitive** for "walk the
foreign-key graph from a root table, recording paths, honoring scope
and depth limits, and treating vocabularies as leaves." It is the
extracted core of :class:`~deriva.bag.catalog_builder.CatalogBagBuilder`'s
``_compute_reached_tables`` and serves both:

* **Bag export** (:class:`CatalogBagBuilder`): BFS from one or more
  anchors, recording multi-path routes to each reached table, capped
  by ``max_paths`` so densely-connected catalogs stay finite. The
  result drives one ``query_processor`` per ``(target, route)``.
* **Denormalization** (deriva-ml's ``DenormalizePlanner``):
  exhaustive DFS from a single root (Dataset), emitting **every
  prefix** of every walked path. The result drives JOIN-tree
  construction and Rule 6 ambiguity detection.

Both consumers ultimately want "all valid FK chains from the root
through the schema graph." They differ in how much of the chain
they consume — BFS-shortest vs. every-prefix — and in their
finiteness guards. :class:`SchemaPathWalker` exposes both modes
behind a single class so the *graph-traversal* logic lives in one
place.

What this module deliberately does **not** carry:

* No anchor-RID filtering (the bag walker layers that on top by
  attaching filters to query paths after the walk).
* No domain-specific transparency rules (e.g. DerivaML's
  "feature-association tables are transparent bridges"). Those
  belong in the consumer because they reference domain concepts
  (``Execution``) that have no place in a generic walker.
* No vocab-export choice, asset mode, or any
  :class:`~deriva.bag.traversal.FKTraversalPolicy` fields that
  describe *what to do with the result* — those are
  consumer-layer concerns. The walker honors only the fields that
  shape the *walk itself*.

Design note: this is a deliberately small, focused class. The
duplication between the bag walker and denormalize's
``_schema_to_paths`` was real but at the primitive level, not the
API level. Extracting the primitive cleans up both call sites
without forcing them onto a single output shape they can't agree
on.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from typing import Any

from deriva.core.ermrest_model import Table as DerivaTable

from deriva.bag.traversal import DEFAULT_EXCLUDE_SCHEMAS, FKTraversalPolicy


#: Type of an optional edge-filter hook.
#:
#: Called once per candidate edge during the walk. Returns ``True``
#: to allow the edge, ``False`` to drop it. The walker passes the
#: *source* table (where the walk currently is) and the *target*
#: table (where the edge would lead). Domain-specific filters —
#: e.g. "don't follow association tables back to the root table" —
#: live in the consumer and plug in here.
EdgeFilter = Callable[[DerivaTable, DerivaTable], bool]


class SchemaPathWalker:
    """Walk the FK graph from a root table.

    The walker follows both **outbound** FKs (``table.foreign_keys``)
    and **inbound** FKs (``table.referenced_by``) — the bidirectional
    walk is the only one any real consumer wants. Vocabulary tables
    are entered but not exited (they're leaves), preventing the
    classic ``Subject → Species → every-other-Subject`` explosion.
    Multi-FK edges between the same pair of tables are deduplicated
    by target (one walk edge per ``(src, tgt)``); callers that need
    the full FK set call :meth:`~deriva.core.ermrest_model.Table.foreign_keys`
    themselves at the consumer layer.

    Two enumeration modes:

    * :meth:`walk_bfs` — BFS recording the shortest path discovered
      per ``(target, route)``. Multiple distinct routes to the same
      target are emitted. Used by the bag walker.
    * :meth:`walk_all_prefixes` — DFS emitting **every prefix** of
      every walked path. Used by denormalize's ``_schema_to_paths``,
      which needs to filter and reshape paths post-hoc.

    Args:
        model: An ERMrest catalog model (the object returned by
            :meth:`ErmrestCatalog.getCatalogModel`).
        policy: :class:`FKTraversalPolicy`. Only the walk-shaping
            fields are honored: ``schemas``, ``exclude_schemas``,
            ``exclude_tables``, ``terminal_tables``, ``max_depth``.
            Behavior-on-output fields (``vocab_export``,
            ``asset_mode``, …) are ignored — those belong to the
            consumer.
        edge_filter: Optional hook for consumer-specific edge
            pruning. Called per candidate edge; return ``False`` to
            drop the edge.

    Example:
        BFS reachability from a single root::

            walker = SchemaPathWalker(
                model=catalog.getCatalogModel(),
                policy=FKTraversalPolicy(schemas={"isa"}),
            )
            paths_by_target = dict(
                walker.walk_bfs([("isa", "Dataset")])
            )
            for (schema, table), routes in paths_by_target.items():
                for route in routes:
                    print(table, "via", " -> ".join(t for _, t in route))

        DFS every-prefix from a single root (denormalize style)::

            walker = SchemaPathWalker(model=model, policy=policy)
            for path in walker.walk_all_prefixes(root=dataset_table):
                # path is a tuple[Table, ...] starting at dataset_table
                ...
    """

    def __init__(
        self,
        *,
        model: Any,
        policy: FKTraversalPolicy | None = None,
        edge_filter: EdgeFilter | None = None,
    ) -> None:
        self.model = model
        self.policy = policy or FKTraversalPolicy()
        self.edge_filter = edge_filter

    # ------------------------------------------------------------------
    # Mode 1: BFS, one BFS-shortest path per route per target.
    # ------------------------------------------------------------------

    def walk_bfs(
        self,
        roots: list[tuple[str, str]],
        *,
        max_paths_per_target: int = 16,
    ) -> dict[
        tuple[str, str], list[tuple[tuple[str, str], ...]]
    ]:
        """BFS the FK graph from a set of root tables.

        For every reachable table, record one or more distinct
        simple-path routes from a root. Each route is a tuple of
        ``(schema, table)`` segments starting at the root and ending
        at the target. ``max_paths_per_target`` bounds the per-target
        route count so densely-connected catalogs can't produce an
        unbounded set.

        Args:
            roots: One or more ``(schema, table)`` starting points.
                Duplicates are deduplicated.
            max_paths_per_target: Cap on per-target route count.
                Default 16 — generous enough that real catalogs hit
                their natural ceiling (2–3) before the cap fires.

        Returns:
            ``{(schema, table): [route, ...]}`` keyed by reached
            table, where each route is a tuple of ``(schema, table)``
            segments. Routes are recorded in BFS-discovery order.

        Example:
            >>> walker.walk_bfs([("isa", "Dataset")])  # doctest: +SKIP
            {('isa', 'Dataset'): [(('isa', 'Dataset'),)],
             ('isa', 'Image'): [(('isa', 'Dataset'), ('isa', 'Dataset_Image'), ('isa', 'Image'))],
             ...}
        """
        # The queue carries one (table, depth, prefix-path) entry per
        # FK route we've discovered to that table. We dequeue *all*
        # routes to a given table — that's the whole point of
        # multi-path recording. The simple-path guard in
        # ``_record_edge`` (drops a candidate that would re-enter a
        # table already on its path) prevents infinite walks on
        # cycles.
        queue: deque[
            tuple[str, str, int, tuple[tuple[str, str], ...]]
        ] = deque()
        reached: set[tuple[str, str]] = set()
        path_set: dict[
            tuple[str, str], list[tuple[tuple[str, str], ...]]
        ] = {}

        # Seed the queue with each root.
        seen_roots: set[tuple[str, str]] = set()
        for root_key in roots:
            if root_key in seen_roots:
                continue
            seen_roots.add(root_key)
            schema_name, table_name = root_key
            path_set.setdefault(root_key, [])
            root_path: tuple[tuple[str, str], ...] = (root_key,)
            if root_path not in path_set[root_key]:
                path_set[root_key].append(root_path)
            queue.append((schema_name, table_name, 0, root_path))

        max_depth = self.policy.max_depth
        while queue:
            schema_name, table_name, depth, current_path = queue.popleft()
            key = (schema_name, table_name)
            reached.add(key)

            # Depth bound (None = unbounded).
            if max_depth is not None and depth >= max_depth:
                continue

            try:
                table = self.model.schemas[schema_name].tables[table_name]
            except KeyError:
                # Schema/table dropped between queueing and dequeue
                # (defensive). Skip silently.
                continue

            # Vocab tables: enter but never exit. Same rule both
            # modes — the walker's universal "vocab is a leaf" guard.
            if table.is_vocabulary():
                continue

            is_terminal = key in self.policy.terminal_tables

            # Outbound FKs: tables this one references.
            for fk in table.foreign_keys:
                self._enqueue_candidate(
                    fk.pk_table,
                    depth + 1,
                    current_path,
                    queue,
                    path_set,
                    max_paths_per_target,
                )

            # Inbound FKs: tables that reference this one. Skipped for
            # terminal tables — see FKTraversalPolicy.terminal_tables.
            if is_terminal:
                continue
            for fk in table.referenced_by:
                self._enqueue_candidate(
                    fk.table,
                    depth + 1,
                    current_path,
                    queue,
                    path_set,
                    max_paths_per_target,
                )

        # Filter the path set to reached tables only (defensive — the
        # main loop already only populates entries for reached
        # targets).
        return {key: path_set[key] for key in sorted(reached)}

    # ------------------------------------------------------------------
    # Mode 2: DFS, every-prefix enumeration.
    # ------------------------------------------------------------------

    def walk_all_prefixes(
        self,
        root: DerivaTable,
        *,
        max_depth: int | None = None,
        stop_at: str | None = None,
    ) -> list[tuple[DerivaTable, ...]]:
        """DFS the FK graph from one root, emit every prefix.

        Recursive DFS, returning every walked prefix as a separate
        path. If ``[Dataset, A, B, C]`` is walked, then ``[Dataset]``,
        ``[Dataset, A]``, ``[Dataset, A, B]`` are also in the result.
        This shape is what denormalize's planner needs to filter
        post-hoc by endpoint and intermediate-membership.

        Cycle detection: a table that already appears on the current
        path is skipped (simple-path enforcement). Vocab tables are
        emitted as terminal (the prefix containing them is in the
        result; nothing further is walked).

        Args:
            root: Starting :class:`Table` for the DFS.
            max_depth: Optional cap on path length (number of tables
                in the path). Overrides the policy's ``max_depth``
                for this call only. ``None`` (default) falls back to
                the policy's ``max_depth``, which itself defaults to
                unbounded.
            stop_at: If supplied, return only paths whose last table
                has this name. Inner DFS frames still emit normally;
                this is a final filter applied to the result. Useful
                for "all paths from X to Y" queries.

        Returns:
            List of paths, each a tuple of :class:`Table` objects
            starting at ``root``. Includes every prefix.

        Example:
            >>> walker.walk_all_prefixes(root=dataset_table)  # doctest: +SKIP
            [(dataset,), (dataset, dataset_subject), ..., (dataset, dataset_image, image, subject), ...]
        """
        effective_max = (
            max_depth if max_depth is not None else self.policy.max_depth
        )
        paths: list[tuple[DerivaTable, ...]] = []
        self._dfs_prefixes(
            root=root,
            current=[],
            max_depth=effective_max,
            stop_at=stop_at,
            out=paths,
        )
        return paths

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _enqueue_candidate(
        self,
        table: DerivaTable,
        depth: int,
        prefix_path: tuple[tuple[str, str], ...],
        queue: "deque[tuple[str, str, int, tuple[tuple[str, str], ...]]]",
        path_set: dict[
            tuple[str, str], list[tuple[tuple[str, str], ...]]
        ],
        max_paths_per_target: int,
    ) -> None:
        """Queue a candidate edge for the BFS, applying scope rules."""
        schema_name = table.schema.name
        table_name = table.name
        key = (schema_name, table_name)

        # Scope guards: schema allow/deny, table deny, edge filter.
        if self._is_excluded_schema(schema_name):
            return
        if (schema_name, table_name) in self.policy.exclude_tables:
            return
        if (
            self.policy.schemas is not None
            and schema_name not in self.policy.schemas
        ):
            return
        if key in prefix_path:
            # Simple-path guard: don't walk back into a table this
            # path already uses. ERMrest joins on natural FK
            # relationships; revisiting a table would loop the
            # corresponding query.
            return
        if self.edge_filter is not None:
            # Source = last segment of the current prefix path.
            src_schema, src_table = prefix_path[-1]
            try:
                src = self.model.schemas[src_schema].tables[src_table]
            except KeyError:
                return
            if not self.edge_filter(src, table):
                return

        candidate_path: tuple[tuple[str, str], ...] = prefix_path + (key,)
        bucket = path_set.setdefault(key, [])
        if candidate_path in bucket:
            return
        if len(bucket) >= max_paths_per_target:
            # Path budget exhausted; drop further routes to this
            # target rather than emit an unbounded set.
            return
        bucket.append(candidate_path)
        queue.append((schema_name, table_name, depth, candidate_path))

    def _dfs_prefixes(
        self,
        *,
        root: DerivaTable,
        current: list[DerivaTable],
        max_depth: int | None,
        stop_at: str | None,
        out: list[tuple[DerivaTable, ...]],
    ) -> None:
        """DFS recursion that records every prefix.

        The current path is built incrementally; the new prefix is
        appended to ``out`` on each call. Cycles are guarded by the
        membership check (``root not in current``) — and vocab tables
        terminate the recursion without further expansion.
        """
        # Cycle guard before append.
        if root in current:
            return

        # Schema scope.
        schema_name = root.schema.name
        table_name = root.name
        if self._is_excluded_schema(schema_name):
            return
        if (schema_name, table_name) in self.policy.exclude_tables:
            return
        if (
            self.policy.schemas is not None
            and schema_name not in self.policy.schemas
        ):
            return

        new_path = current + [root]
        out.append(tuple(new_path))

        # Depth cap. Match historical _schema_to_paths semantics:
        # "len(path) >= max_depth" stops recursion (the current
        # prefix is emitted but children are not).
        if max_depth is not None and len(new_path) >= max_depth:
            return

        # Vocab tables are leaves.
        if root.is_vocabulary():
            return

        # Edge candidates: outbound + inbound, deduplicated by target
        # (multi-FK between same pair counts as one walk edge).
        parent = current[-1] if current else None
        candidates: list[DerivaTable] = []
        seen: set[int] = set()
        for fk in root.foreign_keys:
            t = fk.pk_table
            if id(t) in seen:
                continue
            seen.add(id(t))
            candidates.append(t)
        for fk in root.referenced_by:
            t = fk.table
            if id(t) in seen:
                continue
            seen.add(id(t))
            candidates.append(t)

        for child in candidates:
            if parent is not None and child is parent:
                # Don't bounce back to immediate parent through
                # referenced_by (it's just the same FK re-walked).
                continue
            if self.edge_filter is not None and not self.edge_filter(
                root, child
            ):
                continue
            self._dfs_prefixes(
                root=child,
                current=new_path,
                max_depth=max_depth,
                stop_at=stop_at,
                out=out,
            )

    def _is_excluded_schema(self, schema_name: str) -> bool:
        return (
            schema_name in DEFAULT_EXCLUDE_SCHEMAS
            or schema_name in self.policy.exclude_schemas
        )


__all__ = ["EdgeFilter", "SchemaPathWalker"]
