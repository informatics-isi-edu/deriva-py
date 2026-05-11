"""Tests for :mod:`deriva.bag.cache_index`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from deriva.bag.cache_index import (
    BagCacheIndex,
    CACHE_INDEX_SCHEMA_VERSION,
)
from deriva.bag.sqlite_helpers import SchemaVersionError


def test_cache_index_creates_files(tmp_path: Path) -> None:
    """Constructing creates index.sqlite and the bags/ subdir parent."""
    idx = BagCacheIndex(tmp_path / "cache")
    try:
        assert (tmp_path / "cache").is_dir()
        assert (tmp_path / "cache" / "index.sqlite").is_file()
        assert idx.bags_dir == tmp_path / "cache" / "bags"
    finally:
        idx.dispose()


def test_cache_index_records_bag(tmp_path: Path) -> None:
    idx = BagCacheIndex(tmp_path)
    try:
        idx.record(
            checksum="abc123",
            profile_id="https://example.org/profile.json",
            anchors=[("Dataset", "D1")],
        )
        assert idx.is_cached("abc123")
        record = idx.get("abc123")
        assert record is not None
        assert record["checksum"] == "abc123"
        assert (
            record["profile_id"]
            == "https://example.org/profile.json"
        )
        assert record["anchor_summary"] is None
    finally:
        idx.dispose()


def test_cache_index_find_bags_for_rid(tmp_path: Path) -> None:
    """Reverse lookup returns every bag that anchors at a given RID."""
    idx = BagCacheIndex(tmp_path)
    try:
        idx.record(
            checksum="abc",
            anchors=[("Dataset", "D1"), ("Dataset", "D2")],
        )
        idx.record(
            checksum="def",
            anchors=[("Dataset", "D1"), ("Subject", "S1")],
        )
        found = idx.find_bags_for_rid(table="Dataset", rid="D1")
        # Both bags reference D1; ordering is by built_at DESC, so
        # the more recently-recorded one comes first.
        assert set(found) == {"abc", "def"}

        only_d2 = idx.find_bags_for_rid(table="Dataset", rid="D2")
        assert only_d2 == ["abc"]

        none = idx.find_bags_for_rid(table="Dataset", rid="missing")
        assert none == []
    finally:
        idx.dispose()


def test_cache_index_forget_removes_bag_and_anchors(tmp_path: Path) -> None:
    """FK ON DELETE CASCADE cleans up the anchor rows."""
    idx = BagCacheIndex(tmp_path)
    try:
        idx.record(checksum="abc", anchors=[("Dataset", "D1")])
        removed = idx.forget("abc")
        assert removed is True
        assert not idx.is_cached("abc")
        assert idx.find_bags_for_rid(table="Dataset", rid="D1") == []
    finally:
        idx.dispose()


def test_cache_index_forget_unknown_returns_false(tmp_path: Path) -> None:
    idx = BagCacheIndex(tmp_path)
    try:
        assert idx.forget("never_recorded") is False
    finally:
        idx.dispose()


def test_cache_index_record_is_idempotent(tmp_path: Path) -> None:
    """Re-recording the same checksum updates rather than duplicating."""
    idx = BagCacheIndex(tmp_path)
    try:
        idx.record(checksum="abc", profile_id="old", anchors=[("T", "1")])
        idx.record(checksum="abc", profile_id="new", anchors=[("T", "2")])
        record = idx.get("abc")
        assert record is not None
        assert record["profile_id"] == "new"
        # Anchor list is replaced, not appended.
        assert idx.find_bags_for_rid(table="T", rid="1") == []
        assert idx.find_bags_for_rid(table="T", rid="2") == ["abc"]
    finally:
        idx.dispose()


def test_cache_index_list_bags_orders_by_built_at(tmp_path: Path) -> None:
    """list_bags returns the most-recently-built bag first."""
    idx = BagCacheIndex(tmp_path)
    try:
        old = datetime.now(timezone.utc) - timedelta(days=1)
        new = datetime.now(timezone.utc)
        idx.record(checksum="older", built_at=old)
        idx.record(checksum="newer", built_at=new)
        records = idx.list_bags()
        assert [r["checksum"] for r in records] == ["newer", "older"]
    finally:
        idx.dispose()


def test_cache_index_anchor_summary_round_trips(tmp_path: Path) -> None:
    """A dict anchor summary parses back from JSON cleanly."""
    idx = BagCacheIndex(tmp_path)
    try:
        summary = {"anchors": [{"kind": "rid", "table": "T", "rids": ["1"]}]}
        idx.record(checksum="abc", anchor_summary=summary)
        record = idx.get("abc")
        assert record is not None
        assert record["anchor_summary"] == summary
    finally:
        idx.dispose()


def test_cache_index_total_size_bytes(tmp_path: Path) -> None:
    """The size aggregate sums every recorded ``size_bytes``."""
    idx = BagCacheIndex(tmp_path)
    try:
        idx.record(checksum="a", size_bytes=100)
        idx.record(checksum="b", size_bytes=250)
        idx.record(checksum="c")  # no size recorded → SUM ignores NULL
        assert idx.total_size_bytes() == 350
    finally:
        idx.dispose()


def test_cache_index_total_size_zero_when_empty(tmp_path: Path) -> None:
    idx = BagCacheIndex(tmp_path)
    try:
        assert idx.total_size_bytes() == 0
    finally:
        idx.dispose()


def test_cache_index_persists_across_reopen(tmp_path: Path) -> None:
    """Reopening the same cache root sees previously-recorded bags."""
    idx = BagCacheIndex(tmp_path)
    idx.record(checksum="abc", anchors=[("T", "1")])
    idx.dispose()

    idx2 = BagCacheIndex(tmp_path)
    try:
        assert idx2.is_cached("abc")
        assert idx2.find_bags_for_rid(table="T", rid="1") == ["abc"]
    finally:
        idx2.dispose()


def test_cache_index_rejects_future_schema_version(tmp_path: Path) -> None:
    """A future-version index is refused with SchemaVersionError."""
    idx = BagCacheIndex(tmp_path)
    idx.dispose()

    # Manually bump the schema_meta version on disk.
    from sqlalchemy import text

    from deriva.bag.sqlite_helpers import (
        SCHEMA_META_TABLE,
        create_wal_engine,
    )

    eng = create_wal_engine(tmp_path / "index.sqlite")
    try:
        with eng.begin() as conn:
            conn.execute(text(f"DELETE FROM {SCHEMA_META_TABLE}"))
            conn.execute(
                text(
                    f"INSERT INTO {SCHEMA_META_TABLE}(version) "
                    "VALUES (:v)"
                ),
                {"v": CACHE_INDEX_SCHEMA_VERSION + 1},
            )
    finally:
        eng.dispose()

    with pytest.raises(SchemaVersionError):
        BagCacheIndex(tmp_path)


def test_cache_index_bag_dir_for_returns_path(tmp_path: Path) -> None:
    idx = BagCacheIndex(tmp_path)
    try:
        path = idx.bag_dir_for("abc123")
        assert path == tmp_path / "bags" / "abc123"
        # The path doesn't have to exist yet — it's just where a
        # bag with this checksum *would* go.
        assert not path.exists()
    finally:
        idx.dispose()


def test_cache_index_context_manager_disposes(tmp_path: Path) -> None:
    """Using BagCacheIndex as a context manager closes the engine on exit."""
    with BagCacheIndex(tmp_path) as idx:
        idx.record(checksum="abc")
    # No exception → engine disposed cleanly. We can re-open the same
    # cache root and the data survives.
    with BagCacheIndex(tmp_path) as idx2:
        assert idx2.is_cached("abc")
