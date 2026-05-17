"""Tests for :mod:`deriva.bag.loader` — DataLoader, sinks, FK orderer."""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from deriva.bag.loader import (
    CSVSink,
    DataLoader,
    ForeignKeyOrderer,
    SQLiteSink,
)
from deriva.bag.schema import SchemaBuilder
from deriva.bag.sources import IterableDataSource
from deriva.core.ermrest_model import Model


def _model_with_fk(snaptime: str = "2026-01-01T00:00:00") -> Model:
    """Two-table model: Image.Subject → Subject.RID."""
    doc = {
        "snaptime": snaptime,
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "Subject": {
                        "schema_name": "demo",
                        "table_name": "Subject",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Name",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Subject_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    },
                    "Image": {
                        "schema_name": "demo",
                        "table_name": "Image",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Subject",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Image_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [
                            {
                                "names": [["demo", "Image_Subject_fkey"]],
                                "foreign_key_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Image",
                                        "column_name": "Subject",
                                    }
                                ],
                                "referenced_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Subject",
                                        "column_name": "RID",
                                    }
                                ],
                            }
                        ],
                    },
                },
            }
        },
    }
    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False
    ) as f:
        json.dump(doc, f)
        path = f.name
    return Model.fromfile("file-system", path)


# ---------------------------------------------------------------------------
# ForeignKeyOrderer
# ---------------------------------------------------------------------------


def test_orderer_inserts_parent_before_child() -> None:
    """Subject (PK) precedes Image (FK referring to Subject)."""
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    order = orderer.get_insertion_order(["Image", "Subject"])
    names = [t.name for t in order]
    assert names.index("Subject") < names.index("Image")


def test_orderer_deletion_order_is_reverse_of_insertion() -> None:
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    ins = orderer.get_insertion_order(["Image", "Subject"])
    dele = orderer.get_deletion_order(["Image", "Subject"])
    assert [t.name for t in dele] == list(
        reversed([t.name for t in ins])
    )


def test_orderer_get_dependencies() -> None:
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    deps = orderer.get_dependencies("Image")
    dep_names = {t.name for t in deps}
    assert dep_names == {"Subject"}


def test_orderer_get_dependents() -> None:
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    dependents = orderer.get_dependents("Subject")
    dependent_names = {t.name for t in dependents}
    assert dependent_names == {"Image"}


def test_orderer_validate_returns_empty_for_valid_order() -> None:
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    assert orderer.validate_insertion_order(["Subject", "Image"]) == []


def test_orderer_validate_finds_violation() -> None:
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    violations = orderer.validate_insertion_order(["Image", "Subject"])
    assert len(violations) == 1


def test_orderer_find_cycles_returns_empty_for_dag() -> None:
    """find_cycles returns [] when the FK graph is a DAG."""
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    assert orderer.find_cycles() == []


def _model_with_two_way_cycle(
    snaptime: str = "2026-01-01T00:00:00",
) -> Model:
    """Two tables with FKs in both directions.

    ``Dataset`` has FK ``Version → Dataset_Version.RID`` (nullable).
    ``Dataset_Version`` has FK ``Dataset → Dataset.RID`` (nullable).
    Mirrors the deriva-ml topology that motivated this feature.
    """
    doc = {
        "snaptime": snaptime,
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "Dataset": {
                        "schema_name": "demo",
                        "table_name": "Dataset",
                        "kind": "table",
                        "column_definitions": [
                            {"name": "RID", "type": {"typename": "text"}, "nullok": False, "default": None, "comment": None},
                            {"name": "Version", "type": {"typename": "text"}, "nullok": True, "default": None, "comment": None},
                        ],
                        "keys": [{"names": [["demo", "Dataset_RID_key"]], "unique_columns": ["RID"]}],
                        "foreign_keys": [
                            {
                                "names": [["demo", "Dataset_Version_fkey"]],
                                "foreign_key_columns": [{"schema_name": "demo", "table_name": "Dataset", "column_name": "Version"}],
                                "referenced_columns": [{"schema_name": "demo", "table_name": "Dataset_Version", "column_name": "RID"}],
                            }
                        ],
                    },
                    "Dataset_Version": {
                        "schema_name": "demo",
                        "table_name": "Dataset_Version",
                        "kind": "table",
                        "column_definitions": [
                            {"name": "RID", "type": {"typename": "text"}, "nullok": False, "default": None, "comment": None},
                            {"name": "Dataset", "type": {"typename": "text"}, "nullok": True, "default": None, "comment": None},
                        ],
                        "keys": [{"names": [["demo", "Dataset_Version_RID_key"]], "unique_columns": ["RID"]}],
                        "foreign_keys": [
                            {
                                "names": [["demo", "Dataset_Version_Dataset_fkey"]],
                                "foreign_key_columns": [{"schema_name": "demo", "table_name": "Dataset_Version", "column_name": "Dataset"}],
                                "referenced_columns": [{"schema_name": "demo", "table_name": "Dataset", "column_name": "RID"}],
                            }
                        ],
                    },
                },
            }
        },
    }
    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False
    ) as f:
        json.dump(doc, f)
        path = f.name
    return Model.fromfile("file-system", path)


def test_orderer_cycle_broken_edges_reports_dropped_fk() -> None:
    """``cycle_broken_edges`` returns the FKs the orderer dropped.

    Two-way cycle ``Dataset ↔ Dataset_Version``: the orderer must
    drop exactly one edge to topologically sort. After
    ``get_insertion_order``, the orderer exposes that edge as
    ``(dependent_table, foreign_key)``. The caller uses this to
    defer the FK column on first-pass insert.
    """
    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(model, ["demo"])
    _ = orderer.get_insertion_order(
        ["Dataset", "Dataset_Version"], handle_cycles=True
    )
    broken = orderer.cycle_broken_edges()
    assert len(broken) >= 1
    # Each entry is (dependent_table, foreign_key). The dropped FK
    # must be on one of the two cycle tables, and its target must
    # be the other.
    for dep_table, fk in broken:
        assert dep_table.name in {"Dataset", "Dataset_Version"}
        assert fk.pk_table.name in {"Dataset", "Dataset_Version"}
        assert dep_table.name != fk.pk_table.name


def test_orderer_cycle_broken_edges_empty_for_dag() -> None:
    """No cycles → no broken edges, even after sort runs."""
    model = _model_with_fk()
    orderer = ForeignKeyOrderer(model, ["demo"])
    _ = orderer.get_insertion_order(handle_cycles=True)
    assert orderer.cycle_broken_edges() == []


def test_orderer_break_cycles_raises_on_depth_exhaust() -> None:
    """``_break_cycles_and_sort`` raises when the recursion bound trips.

    Audit §4.5: pre-cleanup, the function logged an error and
    returned ``list(graph.keys())`` — an arbitrary order with no
    broken-edge records. That hid bugs in cycle detection from
    downstream two-phase-insert callers (which silently failed
    FK constraints instead of getting a clear error). Now raises.

    Hard to hit in real schemas (would require more cycles than
    edges, which is impossible). This test constructs the failure
    mode by passing ``_depth`` past the bound directly.
    """
    from graphlib import CycleError

    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(model, ["demo"])
    # Synthesize a graph + CycleError that won't make progress.
    graph: dict[str, set[str]] = {"a": {"b"}, "b": {"a"}}
    err = CycleError("nodes are in a cycle", ["a", "b", "a"])
    with pytest.raises(RuntimeError, match="Too many cycles to break"):
        orderer._break_cycles_and_sort(
            graph, err, _depth=len(graph) + 1
        )


# ---------------------------------------------------------------------------
# DataLoader + SQLiteSink (default sink)
# ---------------------------------------------------------------------------


def test_dataloader_loads_in_fk_order(tmp_path: Path) -> None:
    """DataLoader auto-orders Subject before Image."""
    model = _model_with_fk()
    orm = SchemaBuilder(
        model, ["demo"], database_path=tmp_path
    ).build()
    try:
        # Note: pass Image first to test that the loader orders for us.
        source = IterableDataSource(
            {
                "Image": [{"RID": "I1", "Subject": "S1"}],
                "Subject": [{"RID": "S1", "Name": "A"}],
            }
        )
        loader = DataLoader(orm, source)
        counts = loader.load_tables(["Image", "Subject"])
        assert counts["demo.Subject"] == 1
        assert counts["demo.Image"] == 1
        # Verify the rows landed.
        with orm.engine.connect() as conn:
            subj_rows = conn.execute(
                text('SELECT * FROM "demo".Subject')
            ).fetchall()
        assert len(subj_rows) == 1
    finally:
        orm.dispose()


def test_dataloader_load_table_skips_unknown_source(tmp_path: Path) -> None:
    """When the source lacks a table, loader returns 0 silently."""
    model = _model_with_fk()
    orm = SchemaBuilder(
        model, ["demo"], database_path=tmp_path
    ).build()
    try:
        source = IterableDataSource({})  # no tables at all
        loader = DataLoader(orm, source)
        counts = loader.load_tables(["Subject"])
        assert counts["demo.Subject"] == 0
    finally:
        orm.dispose()


def test_sqlite_sink_rejects_bad_on_conflict() -> None:
    """SQLiteSink validates on_conflict at construction."""
    with pytest.raises(ValueError):
        SQLiteSink(orm=None, on_conflict="bogus")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# CSVSink
# ---------------------------------------------------------------------------


def test_csv_sink_writes_csv_files(tmp_path: Path) -> None:
    """CSVSink writes data/<schema>/<table>.csv with the right columns."""
    model = _model_with_fk()
    orm = SchemaBuilder(
        model, ["demo"], database_path=":memory:"
    ).build()
    try:
        out = tmp_path / "bag-out"
        with CSVSink(out, model) as sink:
            source = IterableDataSource(
                {
                    "Subject": [{"RID": "S1", "Name": "A"}],
                    "Image": [{"RID": "I1", "Subject": "S1"}],
                }
            )
            loader = DataLoader(orm, source, sink=sink)
            counts = loader.load_tables(["Subject", "Image"])
        assert counts == {"demo.Subject": 1, "demo.Image": 1}
        subj_csv = out / "data" / "demo" / "Subject.csv"
        image_csv = out / "data" / "demo" / "Image.csv"
        assert subj_csv.exists()
        assert image_csv.exists()
    finally:
        orm.dispose()


def test_csv_sink_writes_header_row(tmp_path: Path) -> None:
    """CSVSink emits column names as the first CSV row."""
    model = _model_with_fk()
    orm = SchemaBuilder(
        model, ["demo"], database_path=":memory:"
    ).build()
    try:
        out = tmp_path / "bag-out"
        with CSVSink(out, model) as sink:
            source = IterableDataSource(
                {"Subject": [{"RID": "S1", "Name": "A"}]}
            )
            loader = DataLoader(orm, source, sink=sink)
            loader.load_tables(["Subject"])
        with (out / "data" / "demo" / "Subject.csv").open() as f:
            header = next(csv.reader(f))
        assert header == ["RID", "Name"]
    finally:
        orm.dispose()


def test_csv_sink_writes_row_data(tmp_path: Path) -> None:
    """CSVSink writes row dicts as CSV rows in the correct columns."""
    model = _model_with_fk()
    orm = SchemaBuilder(
        model, ["demo"], database_path=":memory:"
    ).build()
    try:
        out = tmp_path / "bag-out"
        with CSVSink(out, model) as sink:
            source = IterableDataSource(
                {
                    "Subject": [
                        {"RID": "S1", "Name": "Alice"},
                        {"RID": "S2", "Name": "Bob"},
                    ]
                }
            )
            DataLoader(orm, source, sink=sink).load_tables(["Subject"])
        with (out / "data" / "demo" / "Subject.csv").open(newline="") as f:
            rows = list(csv.DictReader(f))
        assert rows == [
            {"RID": "S1", "Name": "Alice"},
            {"RID": "S2", "Name": "Bob"},
        ]
    finally:
        orm.dispose()


def test_csv_sink_close_is_idempotent(tmp_path: Path) -> None:
    """Calling close twice on a CSVSink is safe."""
    sink = CSVSink(tmp_path, _model_with_fk())
    sink.close()
    sink.close()


# ---------------------------------------------------------------------------
# F5: cycle-break log dedupe + intentional-cycle allowlist
# ---------------------------------------------------------------------------

def test_orderer_first_cycle_break_logs_at_warning(caplog) -> None:
    """The first cycle-break announcement on a non-intentional cycle WARNs."""
    import logging as _log

    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(model, ["demo"])
    with caplog.at_level(_log.DEBUG, logger="deriva.bag.loader"):
        orderer.get_insertion_order(
            ["Dataset", "Dataset_Version"], handle_cycles=True
        )

    warning_lines = [
        r for r in caplog.records
        if r.levelno == _log.WARNING and "Breaking cycle" in r.message
    ]
    assert len(warning_lines) == 1


def test_orderer_repeated_cycle_break_does_not_re_log(caplog) -> None:
    """A second get_insertion_order on the same instance doesn't re-log."""
    import logging as _log

    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(model, ["demo"])
    with caplog.at_level(_log.DEBUG, logger="deriva.bag.loader"):
        orderer.get_insertion_order(
            ["Dataset", "Dataset_Version"], handle_cycles=True
        )
        orderer.get_insertion_order(
            ["Dataset", "Dataset_Version"], handle_cycles=True
        )

    cycle_lines = [
        r for r in caplog.records
        if "Breaking" in r.message and "cycle" in r.message
    ]
    assert len(cycle_lines) == 1


def test_orderer_intentional_cycle_logs_at_debug_not_warning(caplog) -> None:
    """A cycle in the allowlist logs at DEBUG, not WARNING."""
    import logging as _log

    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(
        model,
        ["demo"],
        intentional_cycles={
            frozenset({"demo.Dataset", "demo.Dataset_Version"})
        },
    )
    with caplog.at_level(_log.DEBUG, logger="deriva.bag.loader"):
        orderer.get_insertion_order(
            ["Dataset", "Dataset_Version"], handle_cycles=True
        )

    warning_lines = [
        r for r in caplog.records
        if r.levelno == _log.WARNING and "Breaking" in r.message
    ]
    debug_lines = [
        r for r in caplog.records
        if r.levelno == _log.DEBUG
        and "Breaking known-intentional cycle" in r.message
    ]
    assert warning_lines == []
    assert len(debug_lines) == 1


def test_orderer_unknown_cycle_still_warns_when_allowlist_nonempty(
    caplog,
) -> None:
    """Allowlist names a different cycle → real cycle still WARNs."""
    import logging as _log

    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(
        model,
        ["demo"],
        intentional_cycles={
            frozenset({"demo.OtherA", "demo.OtherB"})
        },
    )
    with caplog.at_level(_log.DEBUG, logger="deriva.bag.loader"):
        orderer.get_insertion_order(
            ["Dataset", "Dataset_Version"], handle_cycles=True
        )

    warning_lines = [
        r for r in caplog.records
        if r.levelno == _log.WARNING and "Breaking cycle" in r.message
    ]
    assert len(warning_lines) == 1


def test_orderer_cycle_broken_edges_still_populated_when_silenced() -> None:
    """Allowlisting a cycle doesn't suppress edge-tracking.

    Two-phase-insert consumers read ``cycle_broken_edges`` regardless
    of log volume; the silenced log path must still record what got
    dropped.
    """
    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(
        model,
        ["demo"],
        intentional_cycles={
            frozenset({"demo.Dataset", "demo.Dataset_Version"})
        },
    )
    orderer.get_insertion_order(
        ["Dataset", "Dataset_Version"], handle_cycles=True
    )
    broken = orderer.cycle_broken_edges()
    assert len(broken) >= 1


def test_orderer_cycle_identity_is_direction_agnostic() -> None:
    """The allowlist key matches regardless of cycle traversal direction.

    ``CycleError`` reports the cycle path as ordered (e.g.
    ``[A, B, A]`` vs ``[B, A, B]`` depending on which node the sort
    happened to hit first). Both should match the same allowlist
    entry.
    """
    import logging as _log
    from graphlib import CycleError

    model = _model_with_two_way_cycle()
    orderer = ForeignKeyOrderer(
        model,
        ["demo"],
        intentional_cycles={
            frozenset({"demo.Dataset", "demo.Dataset_Version"})
        },
    )
    # Hand-fed cycle in the opposite direction.
    logger_name = "deriva.bag.loader"
    graph: dict[str, set[str]] = {
        "demo.Dataset": {"demo.Dataset_Version"},
        "demo.Dataset_Version": {"demo.Dataset"},
    }
    err = CycleError(
        "nodes are in a cycle",
        ["demo.Dataset_Version", "demo.Dataset", "demo.Dataset_Version"],
    )
    import logging as _l
    handler_cap: list[_l.LogRecord] = []

    class _Cap(_l.Handler):
        def emit(self, record):
            handler_cap.append(record)

    h = _Cap(level=_l.DEBUG)
    log = _l.getLogger(logger_name)
    log.addHandler(h)
    log.setLevel(_l.DEBUG)
    try:
        orderer._break_cycles_and_sort(graph, err)
    finally:
        log.removeHandler(h)

    # No WARNING; one DEBUG with the "known-intentional" wording.
    assert not any(r.levelno == _l.WARNING for r in handler_cap)
    assert any(
        r.levelno == _l.DEBUG
        and "Breaking known-intentional cycle" in r.getMessage()
        for r in handler_cap
    )
