"""Load rows from a :class:`DataSource` into a sink with FK ordering.

This module contains Phase 2 of the two-phase pattern:

- :class:`ForeignKeyOrderer` — compute a topologically-sorted
  insertion order for a set of tables based on the ERMrest model's
  FKs. Tables that are referenced must be filled before tables that
  reference them.
- :class:`DataLoader` — drive a :class:`DataSource` (input) through
  the ordered list of tables, writing each table's rows into a
  *sink*.
- Two sinks:
  - :class:`SQLiteSink` — the existing behavior used by
    :class:`~deriva.bag.database.BagDatabase`. Inserts rows into a
    SQLAlchemy ORM (built by :class:`~deriva.bag.schema.SchemaBuilder`),
    with batching and conflict handling.
  - :class:`CSVSink` — writes rows out as ``data/<schema>/<table>.csv``
    files inside a bag directory. Used by :class:`BagBuilder` on the
    constructive producer path.

The split between "loader" and "sink" lets the same FK-ordered
walk feed multiple write targets without re-implementing the
graph traversal or the source-iteration logic.

Lifted from ``deriva_ml.model.data_loader`` and
``deriva_ml.model.fk_orderer``. The lift adds :class:`CSVSink`
and the split between sink-abstract :class:`DataLoader` and the
specific :class:`SQLiteSink`/:class:`CSVSink` implementations.
"""

from __future__ import annotations

import csv
import logging
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, Callable, Protocol, runtime_checkable

from deriva.core.ermrest_model import Model
from deriva.core.ermrest_model import Table as DerivaTable
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError

from deriva.bag.profile import TABLE_CSV_TEMPLATE
from deriva.bag.schema import SchemaORM
from deriva.bag.sources import DataSource

logger = logging.getLogger(__name__)


# =============================================================================
# ForeignKeyOrderer
# =============================================================================


class ForeignKeyOrderer:
    """Compute FK-safe insertion order over a set of tables.

    Topologically sorts tables so referenced tables are filled
    before the tables that reference them. Cycles are broken by
    removing the last edge in the offending cycle and retrying;
    when ``handle_cycles=False`` the underlying
    :class:`graphlib.CycleError` is raised.

    Args:
        model: ERMrest :class:`Model` to walk for FK relationships.
        schemas: Schemas to consider. FKs into other schemas are
            ignored (the caller decides whether out-of-scope
            references matter).
    """

    def __init__(self, model: Model, schemas: list[str]):
        self.model = model
        self.schemas = set(schemas)
        # Both qualified and unqualified names map to Table objects
        # so callers can pass either form into get_insertion_order.
        self._table_cache: dict[str, DerivaTable] = {}
        self._build_table_cache()
        # Edges the orderer dropped while breaking cycles, populated
        # by ``_break_cycles_and_sort``. Each entry is
        # ``(dependent_table_qname, dropped_dependency_qname)`` —
        # i.e., ``dependent`` had an FK to ``dropped_dep`` that the
        # sort removed to make a topological order possible.
        #
        # Callers reading this for two-phase insert (load with the
        # cycle edge's FK nulled, then patch in a second pass) can
        # use :meth:`cycle_broken_edges` to introspect the list
        # after :meth:`get_insertion_order` runs.
        self._cycle_broken_edges: list[tuple[str, str]] = []

    def _build_table_cache(self) -> None:
        """Index every in-scope table under both qualified and bare names."""
        for schema_name in self.schemas:
            if schema_name not in self.model.schemas:
                continue
            schema = self.model.schemas[schema_name]
            for table_name, table in schema.tables.items():
                self._table_cache[f"{schema_name}.{table_name}"] = table
                if table_name not in self._table_cache:
                    self._table_cache[table_name] = table

    def _to_table(self, t: str | DerivaTable) -> DerivaTable:
        """Coerce ``t`` to a deriva-py :class:`Table`."""
        if isinstance(t, DerivaTable):
            return t
        if t in self._table_cache:
            return self._table_cache[t]
        raise ValueError(f"Table {t} not found in schemas {self.schemas}")

    def _table_key(self, t: DerivaTable) -> str:
        return f"{t.schema.name}.{t.name}"

    def get_dependencies(
        self, table: str | DerivaTable
    ) -> set[DerivaTable]:
        """Return the set of tables ``table`` must be inserted after."""
        t = self._to_table(table)
        dependencies: set[DerivaTable] = set()
        for fk in t.foreign_keys:
            pk_table = fk.pk_table
            if pk_table.schema.name in self.schemas:
                # Self-references aren't dependencies — they're
                # cycles-of-one that the sort layer handles by
                # inserting NULLs and patching later.
                if self._table_key(pk_table) != self._table_key(t):
                    dependencies.add(pk_table)
        return dependencies

    def get_dependents(
        self, table: str | DerivaTable
    ) -> set[DerivaTable]:
        """Return the set of tables that reference ``table`` via FK."""
        t = self._to_table(table)
        dependents: set[DerivaTable] = set()
        for schema_name in self.schemas:
            if schema_name not in self.model.schemas:
                continue
            for other_table in self.model.schemas[schema_name].tables.values():
                if self._table_key(other_table) == self._table_key(t):
                    continue
                for fk in other_table.foreign_keys:
                    if self._table_key(fk.pk_table) == self._table_key(t):
                        dependents.add(other_table)
                        break
        return dependents

    def _build_dependency_graph(
        self,
        tables: list[str | DerivaTable] | None = None,
    ) -> dict[str, set[str]]:
        """Build the FK adjacency map for a candidate table set."""
        if tables is None:
            table_objs: list[DerivaTable] = []
            for schema_name in self.schemas:
                if schema_name in self.model.schemas:
                    table_objs.extend(
                        self.model.schemas[schema_name].tables.values()
                    )
        else:
            table_objs = [self._to_table(t) for t in tables]

        table_keys = {self._table_key(t) for t in table_objs}
        graph: dict[str, set[str]] = {}
        for t in table_objs:
            key = self._table_key(t)
            deps: set[str] = set()
            for fk in t.foreign_keys:
                pk_key = self._table_key(fk.pk_table)
                if pk_key in table_keys and pk_key != key:
                    deps.add(pk_key)
            graph[key] = deps
        return graph

    def get_insertion_order(
        self,
        tables: list[str | DerivaTable] | None = None,
        handle_cycles: bool = True,
    ) -> list[DerivaTable]:
        """Compute a FK-safe insertion order.

        Args:
            tables: Candidate set. ``None`` orders every table in the
                configured schemas.
            handle_cycles: Break cycles when ``True`` (default);
                propagate :class:`CycleError` when ``False``.

        Returns:
            Tables in insertion order — fill from first to last.

        Raises:
            CycleError: If cycles exist and ``handle_cycles=False``.
        """
        graph = self._build_dependency_graph(tables)
        try:
            ts = TopologicalSorter(graph)
            ordered_keys = list(ts.static_order())
        except CycleError as e:
            if handle_cycles:
                ordered_keys = self._break_cycles_and_sort(graph, e)
            else:
                raise
        return [self._table_cache[key] for key in ordered_keys]

    def get_deletion_order(
        self,
        tables: list[str | DerivaTable] | None = None,
        handle_cycles: bool = True,
    ) -> list[DerivaTable]:
        """Return the reverse of :meth:`get_insertion_order`."""
        return list(
            reversed(self.get_insertion_order(tables, handle_cycles))
        )

    def _break_cycles_and_sort(
        self,
        graph: dict[str, set[str]],
        error: CycleError,
        _depth: int = 0,
    ) -> list[str]:
        """Remove one cycle-causing edge and retry the topological sort.

        The recursion bound is ``len(graph)`` — a schema can't
        have more independent cycles than edges. Exceeding it is
        a bug (either in the FK extraction step or in the cycle
        detector), not a recoverable state. Earlier behaviour was
        to log an error and return ``list(graph.keys())`` — an
        arbitrary order with no ``_cycle_broken_edges`` entries
        recorded. That made downstream two-phase-insert callers
        (which consume ``cycle_broken_edges``) silently produce
        ``FK constraint`` failures instead of a clear "could not
        break cycles" error. The audit (§4.5) recommended raising
        instead; this is that fix.
        """
        # Defensive recursion bound: can't have more cycles than edges.
        max_depth = len(graph)
        if _depth > max_depth:
            raise RuntimeError(
                "Too many cycles to break in FK dependency graph "
                f"(depth exceeded {max_depth}). This is a bug in "
                "the cycle detector or the FK extraction step — a "
                "real schema cannot have more independent cycles "
                "than edges. Inspect the remaining graph: "
                f"{dict(graph)}"
            )

        cycle = list(error.args[1]) if len(error.args) > 1 else []
        if cycle:
            logger.warning(
                f"Breaking cycle in FK dependencies: {' -> '.join(cycle)}"
            )
            edge_removed = False
            if len(cycle) >= 3:
                # CycleError reports cycle as [A, B, C, A]; remove
                # the last edge so we minimize disturbance to the
                # rest of the graph.
                dep_node = cycle[-2]
                node = cycle[-1]
                if node in graph and dep_node in graph[node]:
                    graph[node].remove(dep_node)
                    # ``node`` had an FK to ``dep_node`` (because
                    # ``dep_node in graph[node]`` means node depends
                    # on dep_node, i.e. node has an FK pointing at
                    # dep_node). Record that for callers doing
                    # two-phase load.
                    self._cycle_broken_edges.append((node, dep_node))
                    edge_removed = True
            if not edge_removed:
                for i in range(len(cycle) - 1):
                    dep_node, node = cycle[i], cycle[i + 1]
                    if node in graph and dep_node in graph[node]:
                        graph[node].remove(dep_node)
                        self._cycle_broken_edges.append(
                            (node, dep_node)
                        )
                        edge_removed = True
                        break

        try:
            ts = TopologicalSorter(graph)
            return list(ts.static_order())
        except CycleError as e:
            return self._break_cycles_and_sort(graph, e, _depth + 1)

    def cycle_broken_edges(self) -> list[tuple[DerivaTable, "DerivaForeignKey"]]:
        """Return the FK edges that were dropped to break cycles.

        Each entry is ``(dependent_table, foreign_key)`` — the
        ``foreign_key`` that was on ``dependent_table`` and that the
        orderer cut to make a topological sort possible.

        Callers doing a two-phase insert use this to know which FK
        columns must be sent as ``NULL`` on the first-pass insert
        (the cycle's pre-existing target row isn't there yet) and
        then patched in a second-pass ``PUT``. See
        :class:`~deriva.bag.catalog_loader.BagCatalogLoader` for the
        consumer.

        The list is empty until :meth:`get_insertion_order` has
        run (and remains empty if no cycles were detected). Multiple
        edges may be dropped if there were multiple distinct cycles.
        """
        from deriva.core.ermrest_model import ForeignKey as DerivaForeignKey

        out: list[tuple[DerivaTable, DerivaForeignKey]] = []
        for dependent_qname, dropped_dep_qname in self._cycle_broken_edges:
            dependent_table = self._table_cache.get(dependent_qname)
            dropped_target = self._table_cache.get(dropped_dep_qname)
            if dependent_table is None or dropped_target is None:
                continue
            # Find the FK on ``dependent_table`` whose pk_table is
            # ``dropped_target``. There may be more than one in
            # principle; we yield each one.
            for fk in dependent_table.foreign_keys:
                if (
                    fk.pk_table.schema.name == dropped_target.schema.name
                    and fk.pk_table.name == dropped_target.name
                ):
                    out.append((dependent_table, fk))
        return out

    def validate_insertion_order(
        self,
        tables: list[str | DerivaTable],
    ) -> list[tuple[str, str, str]]:
        """Check that an externally-supplied order satisfies FKs.

        Walks ``tables`` left to right tracking which tables have
        been "seen"; for each table, every FK whose target is in the
        candidate set must already be in the seen-set.

        Args:
            tables: Candidate order to validate.

        Returns:
            A list of ``(table, missing_dependency, fk_name)``
            tuples. Empty if the order is valid.
        """
        table_objs = [self._to_table(t) for t in tables]
        seen_keys: set[str] = set()
        violations: list[tuple[str, str, str]] = []
        candidate_keys = {self._table_key(x) for x in table_objs}
        for t in table_objs:
            key = self._table_key(t)
            for fk in t.foreign_keys:
                pk_key = self._table_key(fk.pk_table)
                if pk_key == key:
                    continue
                if pk_key not in candidate_keys:
                    continue
                if pk_key not in seen_keys:
                    violations.append((key, pk_key, fk.name[1]))
            seen_keys.add(key)
        return violations

    def get_all_tables(self) -> list[DerivaTable]:
        """Return every table in the configured schemas."""
        tables: list[DerivaTable] = []
        for schema_name in self.schemas:
            if schema_name in self.model.schemas:
                tables.extend(
                    self.model.schemas[schema_name].tables.values()
                )
        return tables

    def find_cycles(self) -> list[list[str]]:
        """Find every FK dependency cycle in the configured schemas.

        DFS-based cycle detection over the full FK graph. Each
        cycle is returned as a list of qualified table keys
        (``schema.table``) starting and ending at the same node
        (e.g. ``["A", "B", "A"]``). The result may contain
        overlapping cycles when the graph has multiple interlocking
        cycles.

        Useful for diagnostics — :meth:`get_insertion_order`
        handles cycles automatically by breaking edges, but
        callers who want to *see* the cycles (to fix them in the
        source schema, or to log them) can call this directly.

        Returns:
            A list of cycles. Empty list if the FK graph is a DAG.

        Example:
            >>> # Given a schema where A → B → A is a cycle:
            >>> orderer.find_cycles()                # doctest: +SKIP
            [['demo.A', 'demo.B', 'demo.A']]
        """
        graph = self._build_dependency_graph()
        cycles: list[list[str]] = []

        visited: set[str] = set()
        rec_stack: set[str] = set()
        path: list[str] = []

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    # The cycle is the path slice from the
                    # neighbor's first appearance to here, plus
                    # the neighbor again to close the loop.
                    idx = path.index(neighbor)
                    cycles.append(path[idx:] + [neighbor])
            path.pop()
            rec_stack.remove(node)
            return False

        for node in graph:
            if node not in visited:
                dfs(node)

        return cycles


# =============================================================================
# Sink protocol + implementations
# =============================================================================


@runtime_checkable
class Sink(Protocol):
    """Where :class:`DataLoader` writes the rows it pulls.

    Two implementations live in this module: :class:`SQLiteSink`
    (writes into a :class:`SchemaORM`'s SQLite tables) and
    :class:`CSVSink` (writes into bag-profile CSV files).

    Sinks may be opened and closed multiple times by the loader as
    it walks the ordered table list; the protocol guarantees nothing
    about flushes or transactions between calls — implementations
    are free to batch internally.
    """

    def write_rows(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
    ) -> int:
        """Write a batch of rows for ``table``. Return how many landed.

        The sink may legitimately drop rows (e.g., SQLiteSink with
        ``on_conflict="ignore"`` drops PK collisions). The return
        value is the number of rows the sink wrote, not the number
        it was given.
        """
        ...


class SQLiteSink:
    """Write rows into a :class:`SchemaORM`'s SQLite tables.

    The original :class:`DataLoader` ``_insert_batch`` logic lives
    here, factored out so the same loader can target a CSV sink
    instead. Uses SQLite's ``ON CONFLICT`` clause for ``ignore``
    and ``replace`` conflict handling.

    Args:
        orm: A :class:`SchemaORM` from
            :class:`~deriva.bag.schema.SchemaBuilder`.
        on_conflict: How to handle PK collisions during insert.
            One of ``"ignore"`` (skip the conflicting row),
            ``"replace"`` (upsert non-PK columns), or ``"error"``
            (raise).
    """

    def __init__(
        self,
        orm: SchemaORM,
        on_conflict: str = "ignore",
    ):
        if on_conflict not in ("ignore", "replace", "error"):
            raise ValueError(
                f"on_conflict must be 'ignore', 'replace', or 'error'; "
                f"got {on_conflict!r}"
            )
        self.orm = orm
        self.on_conflict = on_conflict

    def write_rows(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0
        try:
            sql_table = self.orm.find_table(
                f"{table.schema.name}.{table.name}"
            )
        except KeyError:
            logger.warning(
                f"Sink: SQLAlchemy table for "
                f"{table.schema.name}.{table.name} not found"
            )
            return 0

        try:
            if self.on_conflict == "ignore":
                stmt = sqlite_insert(sql_table).on_conflict_do_nothing()
            elif self.on_conflict == "replace":
                # Derive the conflict-key column set from the actual
                # primary-key definition rather than hard-coding
                # ``RID`` — the protocol contract claims a general
                # sink, and the audit (§D.6) flagged the hard-code as
                # a leaky deriva-ml-shape assumption.
                pk_cols = [c.name for c in sql_table.primary_key.columns]
                stmt = sqlite_insert(sql_table)
                update_cols = {
                    c.name: c
                    for c in stmt.excluded
                    if c.name not in pk_cols
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_cols,
                    set_=update_cols,
                )
            else:
                stmt = sql_table.insert()

            with self.orm.engine.begin() as conn:
                conn.execute(stmt, rows)
            return len(rows)
        except IntegrityError as e:
            # FK / unique constraint violations are the conflict
            # case the ``on_conflict`` knob is supposed to handle.
            # For ``ignore`` and ``replace`` the SQLite-side ON
            # CONFLICT clause already absorbs them, so reaching this
            # branch means the conflict was something the clause
            # didn't catch (e.g. an FK violation, which ON CONFLICT
            # doesn't suppress). Log and surface per the policy.
            logger.error(
                f"Sink: integrity error inserting into {sql_table.name}: {e}"
            )
            if self.on_conflict == "error":
                raise
            return 0


class CSVSink:
    """Write rows to bag-profile CSV files.

    Each ``write_rows`` call writes (or appends to) one CSV at
    ``{output_dir}/data/{schema}/{table}.csv``. Column names are
    determined from the schema (via the ERMrest :class:`Model`)
    on the first call to a given table, so even sparse rows produce
    a consistent column order.

    Use as the sink for :class:`BagBuilder` when constructing a bag
    from in-memory data, a local SQLite, or any other input that
    flows through :class:`DataLoader`.

    Args:
        output_dir: Path to the bag directory (the parent of
            ``data/``). The directory is created if it doesn't exist.
        model: ERMrest :class:`Model` describing the schemas the
            sink will write. Used to determine column order.

    Example:
        ::

            sink = CSVSink(output_dir=Path("bags/cifar"), model=model)
            loader = DataLoader(orm, source, sink=sink)
            loader.load_tables()
    """

    def __init__(self, output_dir: Path, model: Model):
        self.output_dir = Path(output_dir)
        self.model = model
        # Per-table writer state: file handle + DictWriter + columns.
        self._writers: dict[str, dict[str, Any]] = {}

    def _columns_for(self, table: DerivaTable) -> list[str]:
        """Return column names in schema order for ``table``."""
        return [c.name for c in table.columns]

    def _writer_for(self, table: DerivaTable):
        """Get or create the DictWriter for a table, opening the file."""
        qname = f"{table.schema.name}.{table.name}"
        state = self._writers.get(qname)
        if state is not None:
            return state["writer"]
        # Open the file fresh; truncate if it exists so a re-build
        # produces deterministic output.
        path = self.output_dir / TABLE_CSV_TEMPLATE.format(
            schema=table.schema.name, table=table.name
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = path.open("w", newline="")
        columns = self._columns_for(table)
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        self._writers[qname] = {
            "file": fh,
            "writer": writer,
            "columns": columns,
            "path": path,
        }
        return writer

    def write_rows(
        self,
        table: DerivaTable,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0
        writer = self._writer_for(table)
        # DictWriter ignores extra keys when ``extrasaction='raise'``
        # is not set; we want the simpler behavior of writing only
        # the columns we know about. Calling writer.writerow with a
        # subset/restricted dict drops extras automatically.
        for row in rows:
            writer.writerow(row)
        return len(rows)

    def close(self) -> None:
        """Close every open file handle.

        Call after the loader finishes (or via context-manager
        ``__exit__``). Idempotent.
        """
        for state in self._writers.values():
            try:
                state["file"].close()
            except Exception:
                pass
        self._writers.clear()

    def __enter__(self) -> "CSVSink":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def __del__(self) -> None:
        """Safety-net cleanup at GC time.

        Callers should use the sink as a context manager
        (``with CSVSink(...) as sink:``). If they don't, this
        ensures the per-table file handles get closed before the
        object is collected. Errors are swallowed — at interpreter
        shutdown, the file handle's underlying module may already
        be unloaded.
        """
        try:
            self.close()
        except Exception:
            pass


# =============================================================================
# DataLoader
# =============================================================================


class DataLoader:
    """Drive a :class:`DataSource` through an FK-ordered table list.

    The loader is the meeting point of the input side (a
    :class:`DataSource` — bag CSVs, a catalog, a DataFrame, a local
    SQLite, an iterable) and the output side (a :class:`Sink` —
    SQLite via SchemaORM, or CSV files in a bag directory).

    Args:
        schema_orm: A :class:`SchemaORM`. The orderer walks its
            ``model`` to compute FK order; the
            :class:`SQLiteSink` (when used) writes through its engine.
        data_source: Any :class:`DataSource` implementation.
        sink: Optional explicit sink. Defaults to
            :class:`SQLiteSink` for the consumer-side default
            (loading rows into the SchemaORM's SQLite).

    Example:
        Load a bag into a SQLite:

        ::

            orm = SchemaBuilder(model, schemas).build()
            source = BagDataSource(bag_path)
            loader = DataLoader(orm, source)
            counts = loader.load_tables()

        Write a bag from a DataFrame source:

        ::

            sink = CSVSink(output_dir=Path("bag-out"), model=model)
            source = DataFrameDataSource({"Image": images_df})
            loader = DataLoader(orm, source, sink=sink)
            with sink:
                loader.load_tables(["Image"])
    """

    def __init__(
        self,
        schema_orm: SchemaORM,
        data_source: DataSource,
        sink: Sink | None = None,
    ):
        self.orm = schema_orm
        self.source = data_source
        self.sink: Sink = (
            sink if sink is not None else SQLiteSink(schema_orm)
        )
        self.orderer = ForeignKeyOrderer(
            schema_orm.model,
            schema_orm.schemas,
        )

    def load_tables(
        self,
        tables: list[str | DerivaTable] | None = None,
        batch_size: int = 1000,
        progress_callback: Callable[[str, int, int], None] | None = None,
    ) -> dict[str, int]:
        """Load every table in the source (or a restricted subset).

        Args:
            tables: Optional explicit list of tables to load. When
                ``None``, the loader walks every table the source
                reports via :meth:`DataSource.list_available_tables`
                that also exists in the schema ORM.
            batch_size: How many rows to accumulate before calling
                the sink. Higher values amortize SQL/IO overhead;
                lower values reduce memory and improve progress
                granularity.
            progress_callback: Optional callback invoked after each
                table is loaded. Receives ``(table_key, row_count,
                total_tables)`` so callers can build progress bars.

        Returns:
            ``{qualified_table_name: rows_loaded, ...}``.
        """
        if tables is None:
            available = set(self.source.list_available_tables())
            orm_tables = set(self.orm.list_tables())
            # Match available against ORM via both qualified and
            # bare names — sources are inconsistent about whether
            # they include schema prefixes.
            tables_to_load: list[str] = []
            for orm_table in orm_tables:
                bare = orm_table.split(".")[-1]
                if orm_table in available or bare in available:
                    tables_to_load.append(orm_table)
        else:
            tables_to_load = [
                t
                if isinstance(t, str)
                else f"{t.schema.name}.{t.name}"
                for t in tables
            ]

        try:
            ordered = self.orderer.get_insertion_order(tables_to_load)
        except ValueError as e:
            logger.warning(f"Could not compute FK ordering: {e}")
            ordered = [
                self.orderer._to_table(t) if isinstance(t, str) else t
                for t in tables_to_load
                if self._table_exists(t)
            ]

        counts: dict[str, int] = {}
        total_tables = len(ordered)
        for i, table in enumerate(ordered):
            table_key = f"{table.schema.name}.{table.name}"
            count = self._load_table(table, batch_size)
            counts[table_key] = count
            if progress_callback:
                progress_callback(table_key, count, total_tables)
            if count > 0:
                logger.info(f"Loaded {count} rows into {table_key}")
        return counts

    def _table_exists(self, table: str | DerivaTable) -> bool:
        """Cheap check: does the table exist in the ORM?"""
        try:
            if isinstance(table, str):
                self.orm.find_table(table)
            else:
                self.orm.find_table(f"{table.schema.name}.{table.name}")
            return True
        except KeyError:
            return False

    def _load_table(
        self,
        table: DerivaTable,
        batch_size: int,
    ) -> int:
        """Pull every row for ``table`` from the source into the sink."""
        if not self.source.has_table(table):
            logger.debug(
                f"No data for {table.schema.name}.{table.name} in source"
            )
            return 0

        rows_loaded = 0
        batch: list[dict[str, Any]] = []
        for row in self.source.get_table_data(table):
            batch.append(row)
            if len(batch) >= batch_size:
                rows_loaded += self.sink.write_rows(table, batch)
                batch = []
        if batch:
            rows_loaded += self.sink.write_rows(table, batch)
        return rows_loaded

    def load_table(
        self,
        table: str | DerivaTable,
        batch_size: int = 1000,
    ) -> int:
        """Load a single table without computing an FK-safe order.

        Use this when you know the table's dependencies are already
        in place (or when you're feeding a sink that doesn't enforce
        FK constraints, like CSVSink).
        """
        if isinstance(table, str):
            table = self.orderer._to_table(table)
        return self._load_table(table, batch_size)

    def get_load_order(
        self,
        tables: list[str | DerivaTable] | None = None,
    ) -> list[str]:
        """Return the FK-safe load order without actually loading."""
        if tables is None:
            available = self.source.list_available_tables()
            tables = [t for t in available if self._table_exists(t)]
        ordered = self.orderer.get_insertion_order(tables)
        return [f"{t.schema.name}.{t.name}" for t in ordered]

    def validate_load_order(
        self,
        tables: list[str | DerivaTable],
    ) -> list[tuple[str, str, str]]:
        """Defer to :meth:`ForeignKeyOrderer.validate_insertion_order`."""
        return self.orderer.validate_insertion_order(tables)
