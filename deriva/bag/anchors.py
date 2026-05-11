"""Anchors — starting points for a catalog-to-bag walk.

An *anchor* describes where :class:`CatalogBagBuilder` starts
walking the FK graph. Three concrete forms cover the producer
cases identified in ADR-0006:

- :class:`RIDAnchor` — explicit list of RIDs for one table.
  Example: ``RIDAnchor(table="Subject", rids=["S1", "S2", ...])``.
- :class:`TableAnchor` — every row in a named table.
  Example: ``TableAnchor(table="Subject")``.
- :class:`PathAnchor` — a datapath expression already evaluated
  by the caller to a set of RIDs.
  Example: ``PathAnchor(table="Subject", rids=resolved_set)``.

The union type :data:`Anchor` lets callers pass a list of any
mix. :class:`CatalogBagBuilder` deduplicates overlaps silently
(via the FK walk's visited-set), fails fast on ``RIDAnchor``
RIDs that don't resolve, and warns-but-proceeds on empty
``PathAnchor`` results (per the resolution semantics agreed in
ADR-0006).

Anchors are Pydantic models so they:

- validate on construction (catching e.g. an empty ``rids`` list
  before the walk starts);
- serialize to JSON for ``metadata/`` provenance round-trips;
- carry a discriminator field (``kind``) so the JSON form is
  self-describing.

Naming: "anchor" rather than "seed" because "seed" collides with
the random-number-seed reading in ML contexts.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, field_validator


class AnchorKind(StrEnum):
    """Discriminator values for the :data:`Anchor` union.

    Pydantic uses this enum's string values as the discriminator
    when deserializing JSON; the same strings appear in the bag's
    ``metadata/deriva-bag-provenance.json``.
    """

    RID = "rid"
    TABLE = "table"
    PATH = "path"


class RIDAnchor(BaseModel):
    """Anchor walk at a specific list of RIDs in one table.

    The most precise anchor: the caller names every starting row
    explicitly. Used by :class:`DatasetBagBuilder` (one Dataset
    RID, plus its nested children), by catalog-slice clients
    starting from a known list, and by tests.

    Args:
        table: ERMrest table name. Bare name; the producer is
            responsible for resolving it against the source
            catalog's schemas.
        rids: One or more RIDs in that table. Each must exist in
            the source catalog at walk time (:class:`CatalogBagBuilder`
            validates this with a single ``?RID=in(...)`` query and
            raises with the list of missing RIDs before any walk
            work begins).

    Example:
        >>> a = RIDAnchor(table="Subject", rids=["S1", "S2"])
        >>> a.kind
        <AnchorKind.RID: 'rid'>
        >>> a.rids
        ['S1', 'S2']
    """

    kind: Literal[AnchorKind.RID] = AnchorKind.RID
    table: str = Field(..., min_length=1)
    rids: list[str] = Field(..., min_length=1)

    @field_validator("rids")
    @classmethod
    def _rids_nonempty(cls, v: list[str]) -> list[str]:
        # Pydantic's ``min_length=1`` enforces non-empty list, but
        # callers occasionally pass [""] expecting it to be flagged.
        # Empty RIDs are nonsense; reject them up front.
        for rid in v:
            if not rid or not rid.strip():
                raise ValueError("RID values cannot be empty or whitespace")
        return v


class TableAnchor(BaseModel):
    """Anchor walk at every row in a named table.

    Used for the catalog-wide-clone case (``TableAnchor(table="*")``
    can stand in for "every table" when the producer is willing to
    iterate) and for "every Subject in this catalog" slice queries.

    Args:
        table: ERMrest table name. Empty after-FK-walk results are
            tolerated silently — "no rows in this table" is a
            legitimate outcome.

    Example:
        >>> a = TableAnchor(table="Subject")
        >>> a.kind
        <AnchorKind.TABLE: 'table'>
    """

    kind: Literal[AnchorKind.TABLE] = AnchorKind.TABLE
    table: str = Field(..., min_length=1)


class PathAnchor(BaseModel):
    """Anchor walk at a caller-resolved set of RIDs for one table.

    The most general anchor form. The caller is presumed to have
    built the RID set with a datapath query (e.g.,
    ``"Subject?Age>30"``); ``PathAnchor`` records the result plus
    the (optional) original expression for provenance.

    Empty path results are *warned but allowed* — a filter that
    matches nothing is sometimes the correct outcome, but is
    often a user error worth surfacing.

    Args:
        table: ERMrest table name.
        rids: The resolved RID set. May be empty.
        expression: Optional human-readable description of the
            datapath that produced the RID set. Recorded in the
            bag's provenance for traceability.

    Example:
        >>> a = PathAnchor(table="Subject", rids=["S1"], expression="Age>30")
        >>> a.kind
        <AnchorKind.PATH: 'path'>
    """

    kind: Literal[AnchorKind.PATH] = AnchorKind.PATH
    table: str = Field(..., min_length=1)
    rids: list[str] = Field(default_factory=list)
    expression: str | None = None


#: The :class:`Anchor` discriminated union.
#:
#: Use this as the type for "any anchor" in producer signatures.
#: Pydantic resolves the concrete variant from the ``kind`` field
#: when deserializing.
Anchor = Annotated[
    Union[RIDAnchor, TableAnchor, PathAnchor],
    Field(discriminator="kind"),
]


__all__ = [
    "Anchor",
    "AnchorKind",
    "PathAnchor",
    "RIDAnchor",
    "TableAnchor",
]
