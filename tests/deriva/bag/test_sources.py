"""Tests for :mod:`deriva.bag.sources` adapters."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from deriva.bag.sources import (
    BagDataSource,
    DataFrameDataSource,
    DataSource,
    IterableDataSource,
    LocalDBDataSource,
)
from deriva.bag.sqlite_helpers import create_wal_engine


# ---------------------------------------------------------------------------
# Protocol conformance (runtime_checkable)
# ---------------------------------------------------------------------------


def test_iterable_source_implements_protocol() -> None:
    """IterableDataSource passes the runtime Protocol check."""
    src = IterableDataSource({"T": iter([])})
    assert isinstance(src, DataSource)


def test_dataframe_source_implements_protocol() -> None:
    """DataFrameDataSource passes the runtime Protocol check."""
    src = DataFrameDataSource({"T": object()})
    assert isinstance(src, DataSource)


# ---------------------------------------------------------------------------
# IterableDataSource
# ---------------------------------------------------------------------------


def test_iterable_source_yields_rows() -> None:
    """get_table_data yields the dicts the caller passed in."""
    rows = [{"RID": "1", "Name": "A"}, {"RID": "2", "Name": "B"}]
    src = IterableDataSource({"T": iter(rows)})
    yielded = list(src.get_table_data("T"))
    assert yielded == rows


def test_iterable_source_unknown_table_yields_nothing() -> None:
    """Missing tables yield no rows (graceful, not an error)."""
    src = IterableDataSource({"T": iter([])})
    assert list(src.get_table_data("U")) == []


def test_iterable_source_has_table() -> None:
    """has_table reports presence by bare table name."""
    src = IterableDataSource({"T": iter([])})
    assert src.has_table("T")
    assert not src.has_table("U")


def test_iterable_source_list_available_tables() -> None:
    """list_available_tables returns sorted bare table names."""
    src = IterableDataSource({"B": iter([]), "A": iter([])})
    assert src.list_available_tables() == ["A", "B"]


def test_iterable_source_strips_schema_prefix() -> None:
    """A 'schema.T' lookup matches the iterable stored as 'T'."""
    src = IterableDataSource({"T": iter([{"k": 1}])})
    yielded = list(src.get_table_data("demo.T"))
    assert yielded == [{"k": 1}]


# ---------------------------------------------------------------------------
# DataFrameDataSource (lazy pandas import)
# ---------------------------------------------------------------------------


def test_dataframe_source_yields_rows_if_pandas_present() -> None:
    """A real DataFrame yields rows via to_dict('records')."""
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"RID": ["1", "2"], "Name": ["A", "B"]})
    src = DataFrameDataSource({"T": df})
    rows = list(src.get_table_data("T"))
    assert len(rows) == 2
    assert rows[0]["Name"] == "A"


def test_dataframe_source_list_available_tables() -> None:
    """list_available_tables sorts table names regardless of dtype."""
    src = DataFrameDataSource({"Z": object(), "A": object()})
    assert src.list_available_tables() == ["A", "Z"]


# ---------------------------------------------------------------------------
# BagDataSource
# ---------------------------------------------------------------------------


@pytest.fixture
def two_table_bag(tmp_path: Path) -> Path:
    """A bag with two CSV tables (no schema.json)."""
    cache_key = "demo_xyz"
    bag = tmp_path / cache_key / "bag"
    (bag / "data" / "demo").mkdir(parents=True)
    with (bag / "data" / "demo" / "Subject.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Name"])
        w.writerow(["S1", "Alice"])
        w.writerow(["S2", "Bob"])
    with (bag / "data" / "demo" / "Image.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Filename"])
        w.writerow(["I1", "cat.png"])
    return bag


def test_bag_source_lists_csvs(two_table_bag: Path) -> None:
    """list_available_tables returns CSV stems."""
    src = BagDataSource(two_table_bag)
    assert src.list_available_tables() == ["Image", "Subject"]


def test_bag_source_yields_rows(two_table_bag: Path) -> None:
    """get_table_data parses CSV rows as dicts."""
    src = BagDataSource(two_table_bag)
    rows = list(src.get_table_data("Subject"))
    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice"


def test_bag_source_has_table_for_csv(two_table_bag: Path) -> None:
    """Tables with CSVs are reported as present; others are not."""
    src = BagDataSource(two_table_bag)
    assert src.has_table("Subject")
    assert not src.has_table("Nope")


def test_bag_source_row_count(two_table_bag: Path) -> None:
    """get_row_count returns data-row count (excluding header)."""
    src = BagDataSource(two_table_bag)
    assert src.get_row_count("Subject") == 2
    assert src.get_row_count("Image") == 1
    assert src.get_row_count("Nope") == 0


# ---------------------------------------------------------------------------
# LocalDBDataSource
# ---------------------------------------------------------------------------


def test_localdb_source_lists_tables_in_main(tmp_path: Path) -> None:
    """Tables in the main database are listed (without schema prefix)."""
    db = tmp_path / "local.db"
    engine = create_wal_engine(db)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("CREATE TABLE Foo (k INTEGER PRIMARY KEY)")
            )
            conn.execute(text("INSERT INTO Foo VALUES (1), (2)"))
        src = LocalDBDataSource(engine)
        assert "Foo" in src.list_available_tables()
    finally:
        engine.dispose()


def test_localdb_source_yields_rows(tmp_path: Path) -> None:
    """get_table_data reads rows back as dicts."""
    db = tmp_path / "local.db"
    engine = create_wal_engine(db)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE Foo (RID TEXT PRIMARY KEY, Name TEXT)"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO Foo (RID, Name) "
                    "VALUES ('1', 'A'), ('2', 'B')"
                )
            )
        src = LocalDBDataSource(engine)
        rows = list(src.get_table_data("Foo"))
        assert len(rows) == 2
        assert {r["Name"] for r in rows} == {"A", "B"}
    finally:
        engine.dispose()


def test_localdb_source_skips_schema_meta(tmp_path: Path) -> None:
    """The schema_meta bookkeeping table is hidden from callers."""
    db = tmp_path / "local.db"
    engine = create_wal_engine(db)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE schema_meta "
                    "(version INTEGER PRIMARY KEY, recorded_at TEXT)"
                )
            )
            conn.execute(
                text("CREATE TABLE Real (k INTEGER PRIMARY KEY)")
            )
        src = LocalDBDataSource(engine)
        assert "schema_meta" not in src.list_available_tables()
        assert "Real" in src.list_available_tables()
    finally:
        engine.dispose()
