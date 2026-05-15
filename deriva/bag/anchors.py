"""Anchors — starting points for a catalog-to-bag walk.

An *anchor* describes where :class:`CatalogBagBuilder` starts
walking the FK graph. Two concrete forms cover the producer
cases identified in ADR-0006:

- :class:`RIDAnchor` — explicit list of RIDs for one table.
  Example: ``RIDAnchor(table="Subject", rids=["S1", "S2", ...])``.
- :class:`TableAnchor` — every row in a named table.
  Example: ``TableAnchor(table="Subject")``.

The union type :data:`Anchor` lets callers pass a list of any
mix. :class:`CatalogBagBuilder` deduplicates overlaps silently
(via the FK walk's visited-set) and fails fast on ``RIDAnchor``
RIDs that don't resolve.

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


#: The :class:`Anchor` discriminated union.
#:
#: Use this as the type for "any anchor" in producer signatures.
#: Pydantic resolves the concrete variant from the ``kind`` field
#: when deserializing.
Anchor = Annotated[
    Union[RIDAnchor, TableAnchor],
    Field(discriminator="kind"),
]


__all__ = [
    "Anchor",
    "AnchorKind",
    "RIDAnchor",
    "TableAnchor",
]
