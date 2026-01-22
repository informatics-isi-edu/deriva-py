"""Typed key (unique constraint) definition for ERMrest tables.

This module provides a dataclass-based typed interface for defining unique key
constraints, replacing the dict-based `Key.define()` method from
`deriva.core.ermrest_model`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class KeyDef:
    """Definition for a unique key constraint in an ERMrest table.

    This is a typed replacement for `deriva.core.ermrest_model.Key.define()`.
    It provides type-safe key constraint definition with automatic conversion
    to the dict format expected by the ERMrest API.

    A key constraint enforces uniqueness across one or more columns. Every table
    automatically has a key on the RID column; additional keys can be defined
    for other unique column combinations.

    Attributes:
        columns: List of column names participating in the key.
            These columns together must have unique values across all rows.
        constraint_name: Optional constraint name string. If not provided,
            ERMrest will auto-generate a name based on table and column names.
        comment: Human-readable description of the key constraint.
        annotations: Annotation URIs mapped to annotation values.

    Example:
        >>> # Single-column key
        >>> key = KeyDef(columns=["Email"])
        >>> key.to_dict()
        {'unique_columns': ['Email'], 'names': [], 'comment': None, ...}

        >>> # Multi-column composite key
        >>> key = KeyDef(
        ...     columns=["FirstName", "LastName", "DOB"],
        ...     constraint_name="person_natural_key",
        ...     comment="Natural key for person identification",
        ... )

    Note:
        The legacy `constraint_names` parameter (a list of [schema, name] pairs)
        is handled internally. Use `constraint_name` for the simpler interface.
    """

    columns: list[str]
    constraint_name: str | None = None
    comment: str | None = None
    annotations: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate key definition after initialization."""
        if not self.columns:
            raise ValueError("Key must have at least one column")
        if not isinstance(self.columns, list):
            self.columns = list(self.columns)

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `Key.define()` output
        and can be used directly with ERMrest schema creation methods.

        Returns:
            A dictionary with keys: unique_columns, names, comment, annotations.
            The 'names' field uses the legacy [[schema, name]] format for
            backwards compatibility.
        """
        from deriva.core.ermrest_model import Key
        return Key.define(
            colnames=self.columns,
            constraint_name=self.constraint_name,
            comment=self.comment,
            annotations=self.annotations,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> KeyDef:
        """Create a KeyDef from a dictionary representation.

        This parses key definition dictionaries from ERMrest API responses
        or from `Key.define()` output.

        Args:
            d: A dictionary with key definition fields.

        Returns:
            A KeyDef instance with the parsed values.
        """
        # Extract constraint name from legacy 'names' format
        constraint_name = None
        names = d.get("names", [])
        if names and len(names) > 0 and len(names[0]) >= 2:
            constraint_name = names[0][1]

        return cls(
            columns=d.get("unique_columns", []),
            constraint_name=constraint_name,
            comment=d.get("comment"),
            annotations=d.get("annotations", {}),
        )

    def with_annotation(self, tag: str, value: Any) -> KeyDef:
        """Return a new KeyDef with an additional annotation.

        This is a fluent method for building key definitions with annotations.

        Args:
            tag: The annotation URI/tag.
            value: The annotation value.

        Returns:
            A new KeyDef with the annotation added.
        """
        new_annotations = {**self.annotations, tag: value}
        return KeyDef(
            columns=self.columns.copy(),
            constraint_name=self.constraint_name,
            comment=self.comment,
            annotations=new_annotations,
        )


def rid_key() -> KeyDef:
    """Create the standard RID key definition.

    This is the default primary key for ERMrest tables, automatically
    created by the system. You typically don't need to define this
    explicitly.

    Returns:
        A KeyDef for the RID column.
    """
    return KeyDef(columns=["RID"])


def name_key(column_name: str = "Name") -> KeyDef:
    """Create a key on a name column.

    This is commonly used for vocabulary tables where the Name column
    should be unique.

    Args:
        column_name: The name column to make unique. Defaults to "Name".

    Returns:
        A KeyDef for the specified name column.
    """
    return KeyDef(columns=[column_name])


def composite_key(*column_names: str, constraint_name: str | None = None) -> KeyDef:
    """Create a composite key on multiple columns.

    Args:
        *column_names: The column names to include in the key.
        constraint_name: Optional constraint name.

    Returns:
        A KeyDef for the specified columns.

    Example:
        >>> key = composite_key("FirstName", "LastName", "DOB")
    """
    return KeyDef(columns=list(column_names), constraint_name=constraint_name)
