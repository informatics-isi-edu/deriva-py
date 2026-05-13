"""SQLite engine and connection helpers for the bag and local-DB layers.

This module is the single source of truth for SQLite engine configuration
across ``deriva.bag`` and the deriva-ml ``local_db`` subsystem. Both
build SQLite databases that may be touched by multiple processes
(downloaders, readers, the MCP server, multi-worker DataLoaders), so
both want the same pragmas and the same read-only access pattern.

Provided:

- :func:`create_wal_engine`: SQLAlchemy engine factory enforcing
  WAL journaling, ``synchronous=NORMAL``, ``foreign_keys=ON``, and a
  5-second ``busy_timeout``. Supports read-only access via SQLite's
  ``mode=ro&uri=true`` URI form.
- :func:`attach_database` / :func:`detach_database`: ``ATTACH`` /
  ``DETACH`` helpers. The bag and local-db layers both use multi-file
  SQLite layouts where each ERMrest schema lives in its own attached
  database file.
- :func:`ensure_schema_meta`: idempotent schema-version tracking in a
  ``schema_meta`` table. Raises :class:`SchemaVersionError` when the
  on-disk schema is *newer* than the running code expects, giving
  forward-compatible failure rather than mysterious column-missing
  errors.

Lifted from ``deriva_ml.local_db.sqlite_helpers`` so that
:class:`~deriva.bag.database.BagDatabase` and ``deriva-ml``'s
``local_db`` can share one engine policy. The deriva-ml migration PR
replaces ``local_db/sqlite_helpers.py`` with a re-export from this
module (or removes it and updates callers to import here directly).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Connection, Engine

#: Name of the table that records the on-disk schema version. Created
#: by :func:`ensure_schema_meta` on first contact and consulted on
#: every reopen.
SCHEMA_META_TABLE = "schema_meta"

#: Connection-level ``busy_timeout`` in milliseconds. Five seconds is
#: enough to absorb a brief writer (a bag-build pass, a fetch
#: materialization) without making concurrent readers fail. Long
#: enough that we rarely see ``database is locked``; short enough that
#: a stuck writer is visible quickly.
DEFAULT_BUSY_TIMEOUT_MS = 5000


class SchemaVersionError(RuntimeError):
    """The on-disk schema version is newer than this code supports.

    Raised by :func:`ensure_schema_meta` when the database was written
    by a newer version of the code than the one currently running. The
    intent is to fail fast with a clear message rather than crash later
    with a confusing column-missing error.
    """


def create_wal_engine(db_path: Path, *, read_only: bool = False) -> Engine:
    """Create a SQLAlchemy engine for a SQLite file with WAL mode.

    The engine sets per-connection pragmas via a ``connect`` event
    listener so every checkout out of the pool gets the same settings.

    - When ``read_only=False`` the engine sets ``journal_mode=WAL`` and
      ``synchronous=NORMAL`` (durability traded for write speed with
      acceptable crash safety in WAL mode).
    - ``foreign_keys=ON`` is set in both modes; SQLite defaults to off,
      which would silently let FK-violating inserts succeed.
    - ``busy_timeout`` is set to :data:`DEFAULT_BUSY_TIMEOUT_MS` in both
      modes so concurrent readers and writers don't immediately fail
      with ``database is locked``.
    - When ``read_only=True`` the file is opened via SQLite's
      ``mode=ro&uri=true`` URI form. WAL mode can't be set on a
      read-only handle, so that PRAGMA is skipped.

    Args:
        db_path: Path to the SQLite file. The parent directory is
            created if it does not exist (writable mode only).
        read_only: If ``True``, open the database in read-only mode.
            Multiple read-only connections to the same file are safe
            regardless of WAL state.

    Returns:
        A SQLAlchemy :class:`Engine` with the pragmas wired up. The
        caller owns its lifecycle (call ``engine.dispose()`` when done).

    Example:
        Open a writable engine, create a table, insert a row::

            >>> import tempfile
            >>> from pathlib import Path
            >>> from sqlalchemy import text
            >>> with tempfile.TemporaryDirectory() as tmp:
            ...     engine = create_wal_engine(Path(tmp) / "demo.db")
            ...     with engine.begin() as conn:
            ...         _ = conn.execute(text("CREATE TABLE t (rid INTEGER PRIMARY KEY)"))
            ...         _ = conn.execute(text("INSERT INTO t (rid) VALUES (1)"))
            ...     with engine.connect() as conn:
            ...         row = conn.execute(text("SELECT rid FROM t")).first()
            ...     engine.dispose()
            ...     row[0]
            1

        Re-open the same file read-only::

            >>> with tempfile.TemporaryDirectory() as tmp:
            ...     path = Path(tmp) / "demo.db"
            ...     w = create_wal_engine(path)
            ...     with w.begin() as conn:
            ...         _ = conn.execute(text("CREATE TABLE t (rid INTEGER)"))
            ...         _ = conn.execute(text("INSERT INTO t VALUES (42)"))
            ...     w.dispose()
            ...     r = create_wal_engine(path, read_only=True)
            ...     with r.connect() as conn:
            ...         row = conn.execute(text("SELECT rid FROM t")).first()
            ...     r.dispose()
            ...     row[0]
            42
    """
    db_path = Path(db_path)
    # Only create parent dirs in writable mode. In read-only mode the
    # file must already exist; creating its parent would mask a typo
    # in the caller's path.
    if not read_only:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    if read_only:
        # SQLite's URI form lets us pass ``mode=ro`` so concurrent
        # readers don't disturb a writer that might be active. The
        # sqlite3 driver requires ``uri=True`` in connect args to
        # interpret the URL as a URI rather than a plain path.
        url = f"sqlite:///file:{db_path.resolve()}?mode=ro&uri=true"
        engine = create_engine(
            url,
            future=True,
            connect_args={"uri": True},
        )
    else:
        engine = create_engine(f"sqlite:///{db_path.resolve()}", future=True)

    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_conn: Any, _record: Any) -> None:
        """Apply per-connection pragmas at checkout time.

        Runs once per physical connection (not once per checkout from
        the pool) because the event is ``connect``, not ``checkout``.
        That's fine: the pragmas we set are connection-scoped and
        persist until the connection is closed.
        """
        cur = dbapi_conn.cursor()
        try:
            if not read_only:
                # WAL is per-database, not per-connection, but issuing
                # the PRAGMA on every new connection is idempotent and
                # cheap (SQLite no-ops if WAL is already in effect).
                cur.execute("PRAGMA journal_mode=WAL")
                cur.execute("PRAGMA synchronous=NORMAL")
            # foreign_keys must be enabled per-connection — SQLite's
            # default is OFF, which silently lets FK-violating inserts
            # succeed. ``deriva.bag`` and ``local_db`` both rely on FK
            # enforcement for FK-ordered inserts to work.
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute(f"PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}")
        finally:
            cur.close()

    return engine


def attach_database(conn: Connection, db_path: Path, alias: str) -> None:
    """``ATTACH`` a SQLite file under ``alias`` in the given connection.

    The bag-SQLAlchemy layout uses one ``main.db`` plus per-ERMrest-schema
    ``{schema}.db`` files. Schemas are surfaced inside SQLAlchemy via
    ``ATTACH`` so cross-schema references work as ``{schema}.{table}``
    qualified names in SQL and via the ``schema=`` argument on
    SQLAlchemy ``Table`` constructors.

    Args:
        conn: An open SQLAlchemy connection. The ``ATTACH`` is scoped to
            this connection; sibling connections in the same engine do
            not see the attachment.
        db_path: Path to the SQLite file to attach.
        alias: Name to attach it under. Used as the schema qualifier in
            subsequent queries (e.g. ``alias.tablename``).

    Example:
        >>> import tempfile
        >>> from pathlib import Path
        >>> from sqlalchemy import text
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     main = create_wal_engine(Path(tmp) / "main.db")
        ...     side = create_wal_engine(Path(tmp) / "side.db")
        ...     with side.begin() as conn:
        ...         _ = conn.execute(text("CREATE TABLE x (k INTEGER)"))
        ...         _ = conn.execute(text("INSERT INTO x VALUES (7)"))
        ...     side.dispose()
        ...     with main.connect() as conn:
        ...         attach_database(conn, Path(tmp) / "side.db", "ext")
        ...         row = conn.execute(text('SELECT k FROM "ext".x')).first()
        ...     main.dispose()
        ...     row[0]
        7
    """
    # Escape single quotes in the path and double quotes in the alias.
    # SQLite's ATTACH syntax has no parameter binding, so we must
    # interpolate; doubled quotes are the SQL-standard escape.
    path_str = str(Path(db_path).resolve()).replace("'", "''")
    alias_safe = alias.replace('"', '""')
    conn.execute(text(f"ATTACH DATABASE '{path_str}' AS \"{alias_safe}\""))


def detach_database(conn: Connection, alias: str) -> None:
    """``DETACH`` a previously attached database by alias.

    Args:
        conn: An open SQLAlchemy connection — must be the same
            connection that ``ATTACH``ed the database.
        alias: The alias used in the prior :func:`attach_database` call.
    """
    alias_safe = alias.replace('"', '""')
    conn.execute(text(f'DETACH DATABASE "{alias_safe}"'))


def ensure_schema_meta(engine: Engine, expected_version: int) -> int:
    """Ensure the ``schema_meta`` table exists and records the version.

    Behavior:

    - If the table doesn't exist, create it and insert ``expected_version``
      as the initial row.
    - If the table exists with a version ≤ ``expected_version``, return
      the existing version.
    - If the table exists with a version *greater than* ``expected_version``,
      raise :class:`SchemaVersionError`. This catches the case where a
      newer deriva-py wrote a database that an older deriva-py is now
      trying to open.

    The ``schema_meta`` table is two columns: ``version`` (integer
    primary key) and ``recorded_at`` (ISO-8601 string, defaulted to
    ``datetime('now')``). It is intentionally simple — its only job is
    to fail fast when version skew would otherwise cause confusing
    errors at query time.

    Args:
        engine: Writable SQLAlchemy engine for the database.
        expected_version: The schema version the running code expects.
            Bumped by hand whenever the on-disk layout changes in a
            backward-incompatible way.

    Returns:
        The current (or newly-recorded) schema version.

    Raises:
        SchemaVersionError: If the on-disk version is newer than
            ``expected_version``.

    Example:
        >>> import tempfile
        >>> from pathlib import Path
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     engine = create_wal_engine(Path(tmp) / "demo.db")
        ...     v = ensure_schema_meta(engine, expected_version=1)
        ...     v2 = ensure_schema_meta(engine, expected_version=1)
        ...     engine.dispose()
        ...     (v, v2)
        (1, 1)
    """
    with engine.connect() as conn:
        # CREATE TABLE IF NOT EXISTS is the idempotent half.
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA_META_TABLE} ("
                "  version INTEGER PRIMARY KEY,"
                "  recorded_at TEXT NOT NULL DEFAULT (datetime('now'))"
                ")"
            )
        )
        existing = conn.execute(
            text(f"SELECT MAX(version) FROM {SCHEMA_META_TABLE}")
        ).scalar()

        if existing is None:
            # First-create path. Multiple threads can race here:
            # both see an empty table and both attempt the INSERT.
            # ``INSERT OR IGNORE`` makes the second one a no-op
            # rather than raising ``IntegrityError`` on the PK
            # conflict. We then re-read MAX so every thread agrees
            # on the same answer.
            conn.execute(
                text(
                    f"INSERT OR IGNORE INTO {SCHEMA_META_TABLE}(version) "
                    "VALUES (:v)"
                ),
                {"v": expected_version},
            )
            conn.commit()
            existing = conn.execute(
                text(f"SELECT MAX(version) FROM {SCHEMA_META_TABLE}")
            ).scalar()
            if existing is None:
                # Defensive: table got dropped under us. Fall back to
                # the value we tried to write.
                return expected_version
            return int(existing)

        if existing > expected_version:
            raise SchemaVersionError(
                f"Database schema version {existing} is newer than "
                f"expected {expected_version}; upgrade deriva-py."
            )
        return int(existing)
