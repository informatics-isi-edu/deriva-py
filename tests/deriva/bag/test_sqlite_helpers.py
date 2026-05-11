"""Tests for :mod:`deriva.bag.sqlite_helpers`.

Covers WAL engine creation (writable + read-only), pragma application,
ATTACH/DETACH helpers, and the ``schema_meta`` versioning surface
including the forward-incompat raise path.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import text

from deriva.bag.sqlite_helpers import (
    DEFAULT_BUSY_TIMEOUT_MS,
    SCHEMA_META_TABLE,
    SchemaVersionError,
    attach_database,
    create_wal_engine,
    detach_database,
    ensure_schema_meta,
)


# ---------------------------------------------------------------------------
# create_wal_engine
# ---------------------------------------------------------------------------


def test_writable_engine_creates_file(tmp_path: Path) -> None:
    """A writable engine creates the SQLite file on the first DDL."""
    db_path = tmp_path / "demo.db"
    assert not db_path.exists()
    engine = create_wal_engine(db_path)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
        assert db_path.exists()
    finally:
        engine.dispose()


def test_writable_engine_creates_missing_parent_dir(tmp_path: Path) -> None:
    """Writable mode auto-creates missing parent directories."""
    db_path = tmp_path / "nested" / "dirs" / "demo.db"
    engine = create_wal_engine(db_path)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
        assert db_path.exists()
    finally:
        engine.dispose()


def test_writable_engine_sets_wal_pragma(tmp_path: Path) -> None:
    """The writable engine sets ``journal_mode=WAL``."""
    db_path = tmp_path / "demo.db"
    engine = create_wal_engine(db_path)
    try:
        # WAL is per-database; need a write transaction to actually
        # convert to WAL. Touch the file with a CREATE so the mode
        # sticks before we probe.
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
        with engine.connect() as conn:
            mode = conn.execute(text("PRAGMA journal_mode")).scalar()
        assert mode == "wal"
    finally:
        engine.dispose()


def test_writable_engine_sets_synchronous_normal(tmp_path: Path) -> None:
    """The writable engine sets ``synchronous=NORMAL`` (= 1)."""
    db_path = tmp_path / "demo.db"
    engine = create_wal_engine(db_path)
    try:
        with engine.connect() as conn:
            level = conn.execute(text("PRAGMA synchronous")).scalar()
        # SQLite returns the numeric code: 0=OFF, 1=NORMAL, 2=FULL.
        assert level == 1
    finally:
        engine.dispose()


def test_engine_enables_foreign_keys(tmp_path: Path) -> None:
    """Both writable and read-only engines enable foreign keys.

    SQLite's default is FK enforcement OFF. We want it ON in every
    connection because the bag and local_db layers rely on FK
    integrity for FK-ordered inserts and reads.
    """
    db_path = tmp_path / "demo.db"
    writable = create_wal_engine(db_path)
    try:
        with writable.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
            fk = conn.execute(text("PRAGMA foreign_keys")).scalar()
        assert fk == 1
    finally:
        writable.dispose()

    readonly = create_wal_engine(db_path, read_only=True)
    try:
        with readonly.connect() as conn:
            fk = conn.execute(text("PRAGMA foreign_keys")).scalar()
        assert fk == 1
    finally:
        readonly.dispose()


def test_engine_sets_busy_timeout(tmp_path: Path) -> None:
    """Every connection gets the standard ``busy_timeout``."""
    db_path = tmp_path / "demo.db"
    engine = create_wal_engine(db_path)
    try:
        with engine.connect() as conn:
            timeout = conn.execute(text("PRAGMA busy_timeout")).scalar()
        assert timeout == DEFAULT_BUSY_TIMEOUT_MS
    finally:
        engine.dispose()


def test_readonly_engine_rejects_writes(tmp_path: Path) -> None:
    """A read-only engine cannot insert into the database."""
    db_path = tmp_path / "demo.db"
    writer = create_wal_engine(db_path)
    try:
        with writer.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (1)"))
    finally:
        writer.dispose()

    reader = create_wal_engine(db_path, read_only=True)
    try:
        with pytest.raises(Exception):
            # SQLAlchemy wraps the underlying ``OperationalError`` from
            # sqlite3 ("attempt to write a readonly database"); we don't
            # commit to the exact wrapper class.
            with reader.begin() as conn:
                conn.execute(text("INSERT INTO t VALUES (2)"))
    finally:
        reader.dispose()


def test_readonly_engine_reads_writer_state(tmp_path: Path) -> None:
    """A read-only engine sees the writer's committed rows."""
    db_path = tmp_path / "demo.db"
    writer = create_wal_engine(db_path)
    try:
        with writer.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (1), (2), (3)"))
    finally:
        writer.dispose()

    reader = create_wal_engine(db_path, read_only=True)
    try:
        with reader.connect() as conn:
            rows = list(conn.execute(text("SELECT k FROM t ORDER BY k")))
        assert [r[0] for r in rows] == [1, 2, 3]
    finally:
        reader.dispose()


# ---------------------------------------------------------------------------
# attach_database / detach_database
# ---------------------------------------------------------------------------


def test_attach_database_exposes_external_tables(tmp_path: Path) -> None:
    """Tables in an attached database are visible under the alias."""
    main_path = tmp_path / "main.db"
    side_path = tmp_path / "side.db"

    side = create_wal_engine(side_path)
    try:
        with side.begin() as conn:
            conn.execute(text("CREATE TABLE x (k INTEGER)"))
            conn.execute(text("INSERT INTO x VALUES (7)"))
    finally:
        side.dispose()

    main = create_wal_engine(main_path)
    try:
        with main.connect() as conn:
            attach_database(conn, side_path, "ext")
            row = conn.execute(text('SELECT k FROM "ext".x')).first()
        assert row is not None
        assert row[0] == 7
    finally:
        main.dispose()


def test_attach_detach_roundtrip(tmp_path: Path) -> None:
    """DETACH removes the alias from the connection's namespace."""
    main_path = tmp_path / "main.db"
    side_path = tmp_path / "side.db"

    side = create_wal_engine(side_path)
    try:
        with side.begin() as conn:
            conn.execute(text("CREATE TABLE x (k INTEGER)"))
    finally:
        side.dispose()

    main = create_wal_engine(main_path)
    try:
        with main.connect() as conn:
            attach_database(conn, side_path, "ext")
            # Attached: query should succeed.
            conn.execute(text('SELECT 1 FROM "ext".x WHERE 0=1'))
            detach_database(conn, "ext")
            # After detach the alias is gone; querying it errors out.
            with pytest.raises(Exception):
                conn.execute(text('SELECT 1 FROM "ext".x WHERE 0=1'))
    finally:
        main.dispose()


def test_attach_alias_with_special_characters(tmp_path: Path) -> None:
    """Aliases containing a hyphen (e.g. ``deriva-ml``) are double-quoted.

    Catches the case where an ERMrest schema name contains characters
    that would otherwise need quoting in the ATTACH statement. The
    helper is responsible for emitting a quoted-identifier form.
    """
    main = create_wal_engine(tmp_path / "main.db")
    side_path = tmp_path / "ml.db"
    side = create_wal_engine(side_path)
    try:
        with side.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (42)"))
    finally:
        side.dispose()

    try:
        with main.connect() as conn:
            attach_database(conn, side_path, "deriva-ml")
            row = conn.execute(text('SELECT k FROM "deriva-ml".t')).first()
        assert row is not None
        assert row[0] == 42
    finally:
        main.dispose()


# ---------------------------------------------------------------------------
# ensure_schema_meta
# ---------------------------------------------------------------------------


def test_ensure_schema_meta_creates_table_on_first_call(tmp_path: Path) -> None:
    """First call to ``ensure_schema_meta`` creates the table and inserts the version."""
    engine = create_wal_engine(tmp_path / "demo.db")
    try:
        v = ensure_schema_meta(engine, expected_version=1)
        assert v == 1
        # Table should exist with one row at the expected version.
        with engine.connect() as conn:
            rows = list(
                conn.execute(text(f"SELECT version FROM {SCHEMA_META_TABLE}"))
            )
        assert [r[0] for r in rows] == [1]
    finally:
        engine.dispose()


def test_ensure_schema_meta_idempotent(tmp_path: Path) -> None:
    """Repeated calls with the same expected version are no-ops."""
    engine = create_wal_engine(tmp_path / "demo.db")
    try:
        ensure_schema_meta(engine, expected_version=1)
        ensure_schema_meta(engine, expected_version=1)
        ensure_schema_meta(engine, expected_version=1)
        with engine.connect() as conn:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {SCHEMA_META_TABLE}")
            ).scalar()
        assert count == 1
    finally:
        engine.dispose()


def test_ensure_schema_meta_returns_existing_version_when_lower(
    tmp_path: Path,
) -> None:
    """When the on-disk version is lower than expected, return the on-disk version.

    Models the "we ship code version 2; database was written by code
    version 1" case. Caller is free to run a migration to bring the
    on-disk version up; we don't impose one here.
    """
    db_path = tmp_path / "demo.db"
    engine = create_wal_engine(db_path)
    try:
        ensure_schema_meta(engine, expected_version=1)
    finally:
        engine.dispose()

    engine = create_wal_engine(db_path)
    try:
        v = ensure_schema_meta(engine, expected_version=2)
        assert v == 1
    finally:
        engine.dispose()


def test_ensure_schema_meta_raises_when_on_disk_is_newer(tmp_path: Path) -> None:
    """When the on-disk version is *higher* than expected, raise.

    Models the "we ship code version 1; database was written by code
    version 2" case. Continuing would risk reading a layout we don't
    understand, so we refuse with a clear error.
    """
    db_path = tmp_path / "demo.db"
    engine = create_wal_engine(db_path)
    try:
        # Pretend a future version wrote a row with version 5.
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {SCHEMA_META_TABLE} ("
                    "  version INTEGER PRIMARY KEY,"
                    "  recorded_at TEXT NOT NULL DEFAULT (datetime('now'))"
                    ")"
                )
            )
            conn.execute(
                text(f"INSERT INTO {SCHEMA_META_TABLE}(version) VALUES (:v)"),
                {"v": 5},
            )
    finally:
        engine.dispose()

    engine = create_wal_engine(db_path)
    try:
        with pytest.raises(SchemaVersionError) as excinfo:
            ensure_schema_meta(engine, expected_version=1)
        assert "5" in str(excinfo.value)
        assert "expected 1" in str(excinfo.value)
    finally:
        engine.dispose()


def test_ensure_schema_meta_records_timestamp(tmp_path: Path) -> None:
    """The ``recorded_at`` column is populated by the default expression."""
    engine = create_wal_engine(tmp_path / "demo.db")
    try:
        ensure_schema_meta(engine, expected_version=1)
        with engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT version, recorded_at FROM {SCHEMA_META_TABLE}")
            ).first()
        assert row is not None
        # recorded_at is an ISO-ish timestamp; we just check non-empty.
        assert row[1] is not None
        assert len(str(row[1])) > 0
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Direct sqlite3 verification (sanity)
# ---------------------------------------------------------------------------


def test_underlying_file_is_sqlite(tmp_path: Path) -> None:
    """Sanity: the file SQLAlchemy creates is openable as a SQLite DB."""
    db_path = tmp_path / "demo.db"
    engine = create_wal_engine(db_path)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (k INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (1)"))
    finally:
        engine.dispose()

    # Bypass SQLAlchemy and confirm sqlite3 module sees the data.
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT k FROM t").fetchall()
    finally:
        conn.close()
    assert rows == [(1,)]
