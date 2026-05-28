"""Tests for :mod:`deriva.bag._column_types`.

The shared column-construction primitives must:

- Expose every ERMrest typename the bag pipeline cares about, with
  the right SQLAlchemy backing class (decorators for CSV-coerced
  scalars, plain SQLAlchemy types for the rest).
- Fall back to :class:`sqlalchemy.String` for unknown ERMrest
  typenames so any value that round-trips as text is preserved.
- Detect ``RID`` as the canonical primary key, and only ``RID``.

This module also pins the cross-module identity invariant — the
re-exports from :mod:`deriva.bag.database` and
:mod:`deriva.bag.schema_io` must be the **same** objects as the
ones in :mod:`deriva.bag._column_types`. Two ``ERMRestBoolean``
classes from different import paths would break ``isinstance``
checks downstream.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from sqlalchemy import (
    JSON,
    Column,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    select,
)

from deriva.bag._column_types import (
    ArrayAsJson,
    ERMREST_TO_SQL,
    ERMRestBoolean,
    StringToDate,
    StringToDateTime,
    StringToFloat,
    StringToInteger,
    is_key_column,
    sql_type_for_ermrest,
)


# =============================================================================
# Type map shape
# =============================================================================


def test_type_map_covers_every_documented_ermrest_typename() -> None:
    """All 19 ERMrest typenames the bag pipeline knows about are mapped.

    Regression: ``SchemaBuilder._TYPE_MAP`` and ``BagDatabase`` used
    to carry a 12-entry subset, silently downgrading ``text`` /
    ``longtext`` / ``markdown`` to ``String``. The shared map must
    cover every typename ``schema_io.ERMREST_TO_SQL`` covered
    historically.
    """
    expected = {
        # Numeric.
        "int2", "int4", "int8",
        "float4", "float8",
        # Boolean.
        "boolean",
        # Date/time.
        "date", "timestamp", "timestamptz",
        # Text.
        "text", "longtext", "markdown",
        # JSON.
        "json", "jsonb",
        # ERMrest system columns.
        "ermrest_rid",
        "ermrest_rct",
        "ermrest_rmt",
        "ermrest_rcb",
        "ermrest_rmb",
    }
    assert set(ERMREST_TO_SQL) == expected


def test_type_map_routes_csv_coerced_types_through_decorators() -> None:
    """Integer/float/timestamp/boolean go through ``StringTo*`` decorators.

    The decorators coerce CSV string values on the way into SQLite.
    Plain SQLAlchemy types (``Integer``, ``Float``, …) would accept
    strings on PostgreSQL but reject them on SQLite, so the bag
    pipeline always routes through the decorator path.
    """
    assert ERMREST_TO_SQL["boolean"] is ERMRestBoolean
    assert ERMREST_TO_SQL["int2"] is StringToInteger
    assert ERMREST_TO_SQL["int4"] is StringToInteger
    assert ERMREST_TO_SQL["int8"] is StringToInteger
    assert ERMREST_TO_SQL["float4"] is StringToFloat
    assert ERMREST_TO_SQL["float8"] is StringToFloat
    assert ERMREST_TO_SQL["date"] is StringToDate
    assert ERMREST_TO_SQL["timestamp"] is StringToDateTime
    assert ERMREST_TO_SQL["timestamptz"] is StringToDateTime
    # ERMrest system audit timestamps route through the same
    # datetime decorator.
    assert ERMREST_TO_SQL["ermrest_rct"] is StringToDateTime
    assert ERMREST_TO_SQL["ermrest_rmt"] is StringToDateTime


def test_type_map_uses_plain_sqlalchemy_types_for_text_and_json() -> None:
    """Text-family and JSON types use plain SQLAlchemy classes.

    No decorator needed — text round-trips through CSV unchanged,
    and SQLAlchemy's JSON type already handles deserialization.
    """
    assert ERMREST_TO_SQL["text"] is String
    assert ERMREST_TO_SQL["longtext"] is String
    assert ERMREST_TO_SQL["markdown"] is String
    assert ERMREST_TO_SQL["json"] is JSON
    assert ERMREST_TO_SQL["jsonb"] is JSON
    # ERMrest system text columns (RID, RCB, RMB) are plain text.
    assert ERMREST_TO_SQL["ermrest_rid"] is String
    assert ERMREST_TO_SQL["ermrest_rcb"] is String
    assert ERMREST_TO_SQL["ermrest_rmb"] is String


# =============================================================================
# Unknown-type fallback
# =============================================================================


def test_sql_type_for_ermrest_falls_back_to_string_on_unknown() -> None:
    """Unknown ERMrest typenames degrade to ``String``, not raise."""
    fake = MagicMock()
    fake.typename = "this_typename_does_not_exist"
    fake.is_array = False
    assert sql_type_for_ermrest(fake) is String


def test_sql_type_for_ermrest_returns_the_mapped_class() -> None:
    """Known typenames return the same class the map carries."""
    fake = MagicMock()
    fake.is_array = False
    fake.typename = "int4"
    assert sql_type_for_ermrest(fake) is StringToInteger
    fake.typename = "boolean"
    assert sql_type_for_ermrest(fake) is ERMRestBoolean


# =============================================================================
# PK detection
# =============================================================================


def _mock_table_with_keys(*key_column_lists: list[Any]):
    """Build a mock table whose ``keys`` attribute returns the given
    column lists wrapped in mock Key objects.

    Each entry is a list of "column" objects forming one key.
    """
    table = MagicMock()
    table.keys = [
        MagicMock(unique_columns=cols) for cols in key_column_lists
    ]
    return table


def _mock_column(name: str):
    col = MagicMock()
    col.name = name
    return col


def test_is_key_column_recognises_rid_as_pk() -> None:
    """``RID`` is the sole primary key when it's the first column of a key."""
    rid = _mock_column("RID")
    name = _mock_column("Name")
    # Two declared keys: (RID,) and (Name,). RID is the canonical PK.
    table = _mock_table_with_keys([rid], [name])
    assert is_key_column(rid, table) is True
    assert is_key_column(name, table) is False


def test_is_key_column_rejects_non_rid_columns() -> None:
    """A column named ``Foo`` is never the PK even if it forms a unique key."""
    foo = _mock_column("Foo")
    table = _mock_table_with_keys([foo])
    assert is_key_column(foo, table) is False


def test_is_key_column_rejects_rid_with_no_key_declaration() -> None:
    """A column named ``RID`` that isn't keyed isn't a PK either.

    The function checks both that the column is named ``RID`` and that
    it appears as the first column of one of the table's keys. A
    catalog with a column named ``RID`` that somehow has no key on it
    (defensive — shouldn't happen in real ERMrest schemas) doesn't get
    promoted to PK status.
    """
    rid = _mock_column("RID")
    # Build a table with a different column keyed, and ``rid`` not in
    # any key's unique_columns.
    other = _mock_column("Other")
    table = _mock_table_with_keys([other])
    assert is_key_column(rid, table) is False


# =============================================================================
# Array column round-trip (ArrayAsJson)
# =============================================================================
#
# Regression: ERMrest array columns (``text[]``, ``int4[]``, …) used
# to silently fall back to :class:`String` because
# :func:`sql_type_for_ermrest` had no :attr:`is_array` branch. The
# SQLite mirror declared the column as ``TEXT`` and SQLAlchemy's
# SQLite dialect raised ``sqlite3.ProgrammingError: type 'list' is
# not supported`` the moment ERMrest handed back a Python ``list``
# for that column (see the bag denormalizer's vocab-table populate
# path).


def test_sql_type_for_ermrest_routes_arrays_through_array_as_json() -> None:
    """``is_array`` types go to :class:`ArrayAsJson` regardless of typename."""
    fake = MagicMock()
    fake.is_array = True
    fake.typename = "text[]"
    assert sql_type_for_ermrest(fake) is ArrayAsJson
    # Element type doesn't matter — int4[] / float8[] / unknown[] all
    # route the same way. The ``is_array`` flag is the only gate.
    fake.typename = "int4[]"
    assert sql_type_for_ermrest(fake) is ArrayAsJson
    fake.typename = "never_seen_this[]"
    assert sql_type_for_ermrest(fake) is ArrayAsJson


def test_sql_type_for_ermrest_scalar_json_still_resolves_via_map() -> None:
    """The new array branch leaves scalar ``json`` / ``jsonb`` untouched."""
    fake = MagicMock()
    fake.is_array = False
    fake.typename = "json"
    assert sql_type_for_ermrest(fake) is JSON
    fake.typename = "jsonb"
    assert sql_type_for_ermrest(fake) is JSON


def test_array_as_json_round_trips_python_lists_through_sqlite() -> None:
    """``ArrayAsJson`` round-trips ``list`` values through a SQLite TEXT cell.

    This is the regression test the audit asks for: the bag's mirror
    used to declare array columns as ``String`` and crash on the
    bind. ``ArrayAsJson`` wraps SQLAlchemy ``JSON`` so the value goes
    in as ``list``, serialises to JSON-encoded TEXT inside SQLite,
    and comes back out as ``list`` (or ``None``) without the caller
    seeing the encoding.
    """
    metadata = MetaData()
    table = Table(
        "vocab",
        metadata,
        Column("rid", String, primary_key=True),
        Column("synonyms", ArrayAsJson),
    )
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

    rows = [
        {"rid": "A", "synonyms": ["plane", "aeroplane"]},
        {"rid": "B", "synonyms": []},
        {"rid": "C", "synonyms": None},
        {"rid": "D", "synonyms": [1, 2, 3]},
    ]
    with engine.begin() as conn:
        conn.execute(insert(table), rows)
        result = {r.rid: r.synonyms for r in conn.execute(select(table))}

    assert result == {
        "A": ["plane", "aeroplane"],
        "B": [],
        "C": None,
        "D": [1, 2, 3],
    }


def test_array_as_json_inverse_routes_back_to_array_typename() -> None:
    """The inverse map :data:`SQL_TO_ERMREST` resolves ``ArrayAsJson``.

    The element type isn't recoverable from the SQLAlchemy type alone
    so the inverse defaults to ``text[]``; callers that need full
    fidelity for non-text element types rely on the
    ``col.info["ermrest_typename"]`` stash that
    :func:`ermrest_json_to_metadata` writes (and
    :func:`metadata_to_ermrest_json` reads).
    """
    from deriva.bag.schema_io import sql_type_to_ermrest_name

    assert sql_type_to_ermrest_name(ArrayAsJson()) == "text[]"


# =============================================================================
# Cross-module identity (no accidental duplication)
# =============================================================================


def test_schema_io_module_reexports_same_map() -> None:
    """``deriva.bag.schema_io.ERMREST_TO_SQL`` is the same dict as the
    canonical one in ``_column_types``."""
    from deriva.bag import schema_io
    from deriva.bag import _column_types

    assert schema_io.ERMREST_TO_SQL is _column_types.ERMREST_TO_SQL


def test_schema_module_reexports_same_decorators() -> None:
    """``deriva.bag.schema`` re-exports the decorator classes for back-compat."""
    from deriva.bag import schema
    from deriva.bag import _column_types

    assert schema.ERMRestBoolean is _column_types.ERMRestBoolean
    assert schema.StringToInteger is _column_types.StringToInteger
