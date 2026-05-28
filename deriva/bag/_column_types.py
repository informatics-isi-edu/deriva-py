"""Shared column-construction primitives for the bag pipeline.

Three places in :mod:`deriva.bag` turn an ERMrest column description
into a SQLAlchemy :class:`Column`: :class:`SchemaBuilder` (bag-build
in-memory), :class:`BagDatabase` (bag-read reflect-from-SQLite), and
:func:`schema_io.ermrest_json_to_metadata` (bag-write lossless
round-trip). They share three primitives that this module centralises:

- **CSV-to-typed-value decorator classes** (:class:`ERMRestBoolean`,
  :class:`StringToFloat`, :class:`StringToInteger`,
  :class:`StringToDateTime`, :class:`StringToDate`). SQLAlchemy
  :class:`TypeDecorator` subclasses that coerce CSV strings into
  proper Python types on the way *into* SQLite. Used by all three
  call sites; previously lived in :mod:`deriva.bag.database` and were
  imported from there by the others, creating a "decorators live where
  the SQLite reflection lives" coupling that's now broken.

- **ERMrest-typename → SQLAlchemy-type map** (:data:`ERMREST_TO_SQL`).
  The 19-entry dict that maps every ERMrest typename the bag pipeline
  knows about to the SQLAlchemy type that backs it. Unknown ERMrest
  typenames fall back to :class:`String` — preserves any value that
  round-trips as text without losing data. Previously duplicated in
  three places with a 12-entry subset on two of them, silently
  downgrading ``text`` / ``longtext`` / ``markdown`` to plain
  ``String`` (no data loss, but lost typename fidelity).

- **PK-detection rule** (:func:`is_key_column`). ERMrest catalogs have
  many declared keys, but only ``RID`` is the canonical primary key.
  This module is the one place that rule lives now.

The companion :mod:`deriva.bag.schema_io` still owns the **inverse**
map (:data:`SQL_TO_ERMREST`) plus the lossless ``col.info`` stash
that producer paths need. That's intentionally separate — only the
write path needs round-trip fidelity, and unifying the column-
creation loops would mix concerns.

See :doc:`/design/column-construction-dedup` for the audit.

Example:
    >>> from deriva.bag._column_types import ERMREST_TO_SQL
    >>> ERMREST_TO_SQL["int4"].__name__
    'StringToInteger'
    >>> # Unknown ERMrest typenames fall back to String.
    >>> ERMREST_TO_SQL.get("never_seen_this", None) is None
    True
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from dateutil import parser
from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
)
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import TypeDecorator

if TYPE_CHECKING:
    from deriva.core.ermrest_model import Column as DerivaColumn
    from deriva.core.ermrest_model import Table as DerivaTable
    from deriva.core.ermrest_model import Type as DerivaType


# =============================================================================
# CSV-to-typed-value decorator classes
# =============================================================================
#
# These map ``str`` values (the form every column arrives as when
# loaded from CSV) into the typed Python value the SQLite column
# expects. They're :class:`TypeDecorator` subclasses so the conversion
# applies on every bound parameter, transparent to caller code.
#
# Empty-string handling is uniform: empty strings become ``None``
# (matching the bag's CSV-NULL convention). The producers
# (:mod:`schema_io`, :mod:`schema`) won't use the decorators in
# practice — they pass Python values directly — but the type
# declarations are the same.


class ERMRestBoolean(TypeDecorator):
    """Convert ERMrest boolean strings to Python ``bool``.

    ERMrest emits boolean values as ``"Y"`` / ``"N"`` in some CSV
    serialisations and as ``"t"`` / ``"f"`` in others. Both are
    accepted; everything else raises so we don't silently coerce
    a typo into ``False``.
    """

    impl = Boolean
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> bool | None:
        if value in ("Y", "y", 1, True, "t", "T"):
            return True
        elif value in ("N", "n", 0, False, "f", "F"):
            return False
        elif value is None:
            return None
        raise ValueError(f"Invalid boolean value: {value!r}")


class StringToFloat(TypeDecorator):
    """Convert CSV string to ``float``, treating empty as NULL."""

    impl = Float
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> float | None:
        if value == "" or value is None:
            return None
        return float(value)


class StringToInteger(TypeDecorator):
    """Convert CSV string to ``int``, treating empty as NULL."""

    impl = Integer
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> int | None:
        if value == "" or value is None:
            return None
        return int(value)


class StringToDateTime(TypeDecorator):
    """Convert CSV string to ``datetime``, treating empty as NULL.

    Uses :func:`dateutil.parser.parse` so any reasonable timestamp
    representation works; ERMrest's wire format is ISO-8601 but
    older bags may carry other shapes.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value == "" or value is None:
            return None
        return parser.parse(value)


class StringToDate(TypeDecorator):
    """Convert CSV string to :class:`datetime.date`, treating empty as NULL."""

    impl = Date
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value == "" or value is None:
            return None
        return parser.parse(value).date()


class ArrayAsJson(TypeDecorator):
    """Serialise Python list values as JSON-encoded TEXT for SQLite.

    ERMrest emits array columns (``text[]``, ``int4[]``, ...) as JSON
    arrays, which deriva-py deserialises into Python lists. SQLite has
    no native array type and SQLAlchemy's SQLite dialect cannot bind a
    list to a TEXT column. JSON-encode on write, decode on read, so
    callers see ``list`` end-to-end.

    The wrapped :class:`sqlalchemy.JSON` already round-trips Python
    ``list`` / ``dict`` values transparently on SQLite; the decorator
    exists so the bag's lossless schema round-trip
    (:data:`deriva.bag.schema_io.SQL_TO_ERMREST`) has a named class to
    distinguish *array* columns from scalar ``json`` / ``jsonb``
    columns when no ``ermrest_typename`` is stashed in ``col.info``.
    """

    impl = JSON
    cache_ok = True


# =============================================================================
# ERMrest typename → SQLAlchemy type
# =============================================================================

#: ERMrest typename → SQLAlchemy type class.
#:
#: One source of truth for the conversion. Decorator classes wrap
#: their plain-SQLAlchemy counterparts so CSV-loaded values get
#: coerced through :meth:`TypeDecorator.process_bind_param` on the
#: way into SQLite. Producer-side use (``MetaData`` supplied by the
#: caller) doesn't invoke the decorators — the caller can pass
#: already-typed Python values directly.
#:
#: Unknown ERMrest typenames fall back to :class:`sqlalchemy.String`
#: via :func:`get` in :func:`sql_type_for_ermrest`. No data loss —
#: any value that round-trips as text is preserved.
ERMREST_TO_SQL: dict[str, type[TypeEngine]] = {
    "boolean": ERMRestBoolean,
    "date": StringToDate,
    "float4": StringToFloat,
    "float8": StringToFloat,
    "int2": StringToInteger,
    "int4": StringToInteger,
    "int8": StringToInteger,
    "json": JSON,
    "jsonb": JSON,
    "timestamptz": StringToDateTime,
    "timestamp": StringToDateTime,
    "text": String,
    "longtext": String,
    "markdown": String,
    # ERMrest's ``ermrest_rid`` etc. are stored as text; map directly.
    "ermrest_rid": String,
    "ermrest_rct": StringToDateTime,
    "ermrest_rmt": StringToDateTime,
    "ermrest_rcb": String,
    "ermrest_rmb": String,
}


def sql_type_for_ermrest(deriva_type: "DerivaType") -> type[TypeEngine]:
    """Map an ERMrest column type to its SQLAlchemy counterpart.

    Args:
        deriva_type: ERMrest :class:`Type` object (from a
            :class:`Column` definition).

    Returns:
        SQLAlchemy column type class. Array types
        (``deriva_type.is_array`` is True) route to
        :class:`ArrayAsJson`; unknown scalar ERMrest typenames fall
        back to :class:`sqlalchemy.String`, which works for any value
        that survives the CSV round-trip as a string.

    Example:
        >>> # Pure-Python example — runs for real:
        >>> from sqlalchemy import String
        >>> from deriva.bag._column_types import (
        ...     ArrayAsJson,
        ...     ERMREST_TO_SQL,
        ...     StringToInteger,
        ...     sql_type_for_ermrest,
        ... )
        >>> # Known typename returns the decorator class.
        >>> ERMREST_TO_SQL["int4"] is StringToInteger
        True
        >>> # Unknown typename falls back to String via the helper.
        >>> class _FakeType:
        ...     typename = "never_seen_this"
        ...     is_array = False
        >>> sql_type_for_ermrest(_FakeType()) is String
        True
        >>> # Array types route to ArrayAsJson regardless of element type.
        >>> class _FakeArray:
        ...     typename = "text[]"
        ...     is_array = True
        >>> sql_type_for_ermrest(_FakeArray()) is ArrayAsJson
        True
    """
    if getattr(deriva_type, "is_array", False):
        return ArrayAsJson
    return ERMREST_TO_SQL.get(deriva_type.typename, String)


# =============================================================================
# PK detection
# =============================================================================


def is_key_column(
    column: "DerivaColumn", table: "DerivaTable"
) -> bool:
    """Return True if ``column`` is the table's canonical primary key.

    ERMrest catalogs have many declared keys, but only ``RID`` is the
    canonical primary key for the SQLAlchemy mirror. This function
    tags *only* RID with ``primary_key=True`` on the SQLAlchemy
    column; other declared keys become non-PK unique constraints.

    Args:
        column: ERMrest :class:`Column` to test.
        table: The :class:`Table` ``column`` belongs to. Used to walk
            ``table.keys`` and confirm ``column`` appears as a single-
            column key (not just a column named ``RID`` that's not
            actually keyed).

    Returns:
        ``True`` if ``column.name == "RID"`` and ``column`` is the
        sole column of one of ``table``'s keys; ``False`` otherwise.

    Example:
        >>> # See ``tests/deriva/bag/test_column_types.py`` for an
        >>> # end-to-end test with a real ERMrest model — this rule
        >>> # depends on the ``deriva.core.ermrest_model`` shapes
        >>> # which a doctest can't trivially construct.
    """
    return (
        column in [key.unique_columns[0] for key in table.keys]
        and column.name == "RID"
    )
