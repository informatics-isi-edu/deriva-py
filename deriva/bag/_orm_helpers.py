"""Shared SQLAlchemy-ORM helpers for the bag pipeline.

Both :class:`~deriva.bag.database.BagDatabase` (bag-read side) and
:class:`~deriva.bag.schema.SchemaORM` (bag-build side) need to ask:

- "Is this ORM class an association table?" — used to decide
  whether a row is structural (linking two parents) or domain
  content.
- "Find the association class connecting these two ORM classes" —
  used to traverse many-to-many relationships via the
  associated table.

Both questions are pure functions of the SQLAlchemy ORM state
(``inspect(cls).mapper.relationships`` + the table's unique-key
constraints). Pre-dedup, the same ~135 lines lived in
:mod:`deriva.bag.database` and :mod:`deriva.bag.schema`; that's
two places to update for any future fix. The functions live here
now; both callers re-export them for back-compat with code that
reaches the methods through ``BagDatabase`` or ``SchemaORM``.

See the deriva.bag package audit (``docs/design/bag-package-audit-2026-05.md``,
§1.3) for the original drift.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import UniqueConstraint as SQLUniqueConstraint
from sqlalchemy import inspect


def is_association_table(
    table_class: Any,
    min_arity: int = 2,
    max_arity: int = 2,
    unqualified: bool = True,
    pure: bool = True,
    no_overlap: bool = True,
    return_fkeys: bool = False,
) -> int | bool | set[Any]:
    """Check whether an ORM class represents an association table.

    An association table is a table whose entire content is a
    composite unique key over two or more foreign-key columns,
    each pointing at a different parent table. It carries no
    domain content of its own; its purpose is to record the
    many-to-many relationship between its endpoints.

    The detection is configurable: ``min_arity`` / ``max_arity``
    control how many FKs the candidate must have;
    ``unqualified`` / ``pure`` / ``no_overlap`` toggle stricter
    structural requirements.

    Args:
        table_class: SQLAlchemy ORM class to check.
        min_arity: Minimum number of FKs covered by the
            association's unique key (default 2).
        max_arity: Maximum number of FKs (default 2). ``None``
            for unbounded.
        unqualified: If ``True``, the unique key must contain
            *only* FK columns (no extra qualifier columns).
        pure: If ``True``, the table must have no non-key
            non-system columns (the table is *only* the
            association).
        no_overlap: If ``True``, FK column sets must be disjoint
            (no column participates in two FKs).
        return_fkeys: If ``True``, return the matching FK
            relationships; otherwise return the arity.

    Returns:
        If ``return_fkeys=False``: integer arity if the candidate
        is an association, ``False`` otherwise.
        If ``return_fkeys=True``: set of FK relationships, or
        ``False``.

    Raises:
        ValueError: If ``min_arity < 2`` or
            ``max_arity < min_arity``.
    """
    if min_arity < 2:
        raise ValueError("An association cannot have arity < 2")
    if max_arity is not None and max_arity < min_arity:
        raise ValueError("max_arity cannot be less than min_arity")

    mapper = inspect(table_class).mapper
    system_cols = {"RID", "RCT", "RMT", "RCB", "RMB"}

    non_sys_cols = {
        col.name for col in mapper.columns if col.name not in system_cols
    }

    unique_columns = [
        {c.name for c in constraint.columns}
        for constraint in inspect(table_class).local_table.constraints
        if isinstance(constraint, SQLUniqueConstraint)
    ]

    non_sys_key_colsets = {
        frozenset(uc)
        for uc in unique_columns
        if uc.issubset(non_sys_cols) and len(uc) > 1
    }

    if not non_sys_key_colsets:
        return False

    # Choose the longest compound key — when multiple unique
    # keys exist, the largest is the most likely "this is the
    # association" candidate.
    row_key = sorted(non_sys_key_colsets, key=lambda s: len(s), reverse=True)[0]
    foreign_keys = list(inspect(table_class).relationships.values())

    covered_fkeys = {
        fkey
        for fkey in foreign_keys
        if {c.name for c in fkey.local_columns}.issubset(row_key)
    }
    covered_fkey_cols: set[str] = set()

    if len(covered_fkeys) < min_arity:
        return False
    if max_arity is not None and len(covered_fkeys) > max_arity:
        return False

    for fkey in covered_fkeys:
        fkcols = {c.name for c in fkey.local_columns}
        if no_overlap and fkcols.intersection(covered_fkey_cols):
            return False
        covered_fkey_cols.update(fkcols)

    if unqualified and row_key.difference(covered_fkey_cols):
        return False

    if pure and non_sys_cols.difference(row_key):
        return False

    return covered_fkeys if return_fkeys else len(covered_fkeys)


def get_association_class(
    left_cls: Any,
    right_cls: Any,
) -> tuple[Any, Any, Any] | None:
    """Find an association class connecting two ORM classes.

    Walks ``left_cls``'s relationships looking for any that
    terminate at an association table whose two endpoints are
    ``left_cls`` and ``right_cls``.

    Args:
        left_cls: One end of the relationship.
        right_cls: The other end.

    Returns:
        ``(association_class, left_relationship_attr,
        right_relationship_attr)`` if an association is found;
        ``None`` otherwise.
    """
    for _, left_rel in inspect(left_cls).relationships.items():
        mid_cls = left_rel.mapper.class_
        is_assoc = is_association_table(mid_cls, return_fkeys=True)

        if not is_assoc:
            continue

        assoc_local_columns_left = list(is_assoc)[0].local_columns
        assoc_local_columns_right = list(is_assoc)[1].local_columns

        found_left = found_right = False

        for r in inspect(left_cls).relationships.values():
            remote_side = list(r.remote_side)[0]
            if remote_side in assoc_local_columns_left:
                found_left = r
            if remote_side in assoc_local_columns_right:
                found_left = r
                # Swap if backwards
                assoc_local_columns_left, assoc_local_columns_right = (
                    assoc_local_columns_right,
                    assoc_local_columns_left,
                )

        for r in inspect(right_cls).relationships.values():
            remote_side = list(r.remote_side)[0]
            if remote_side in assoc_local_columns_right:
                found_right = r

        if found_left and found_right:
            return mid_cls, found_left.class_attribute, found_right.class_attribute

    return None
