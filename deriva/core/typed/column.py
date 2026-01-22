"""Typed column definition for ERMrest tables.

This module provides a dataclass-based typed interface for defining table columns,
replacing the dict-based `Column.define()` method from `deriva.core.ermrest_model`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from deriva.core.typed.types import BuiltinType


@dataclass
class ColumnDef:
    """Definition for a column in an ERMrest table.

    This is a typed replacement for `deriva.core.ermrest_model.Column.define()`.
    It provides type-safe column definition with automatic conversion to the
    dict format expected by the ERMrest API.

    Attributes:
        name: Column name. Must be unique within the table.
        type: Column data type from BuiltinType enum.
        nullok: Whether NULL values are allowed. Defaults to True.
        default: Default value for new rows. Can be a literal value or
            an ERMrest expression string.
        comment: Human-readable description of the column.
        acls: Access control lists mapping AclMode to lists of roles.
        acl_bindings: Dynamic ACL binding configurations.
        annotations: Annotation URIs mapped to annotation values.

    Example:
        >>> col = ColumnDef(
        ...     name="Age",
        ...     type=BuiltinType.int4,
        ...     nullok=False,
        ...     comment="Subject age in years",
        ... )
        >>> col.to_dict()
        {'name': 'Age', 'type': {'typename': 'int4'}, 'nullok': False, ...}

    Note:
        The `type` parameter accepts a `BuiltinType` enum value. For the
        native API, this is converted to a Type object via `to_ermrest_type()`.
    """

    name: str
    type: BuiltinType = BuiltinType.text
    nullok: bool = True
    default: Any = None
    comment: str | None = None
    acls: dict[str, list[str]] = field(default_factory=dict)
    acl_bindings: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `Column.define()` output
        and can be used directly with ERMrest schema creation methods.

        Returns:
            A dictionary with keys: name, type, nullok, default, comment,
            acls, acl_bindings, annotations. The 'type' value is the
            prejson representation of the Type object.
        """
        from deriva.core.ermrest_model import Column
        return Column.define(
            cname=self.name,
            ctype=self.type.to_ermrest_type(),
            nullok=self.nullok,
            default=self.default,
            comment=self.comment,
            acls=self.acls,
            acl_bindings=self.acl_bindings,
            annotations=self.annotations,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ColumnDef:
        """Create a ColumnDef from a dictionary representation.

        This parses column definition dictionaries from ERMrest API responses
        or from `Column.define()` output.

        Args:
            d: A dictionary with column definition fields.

        Returns:
            A ColumnDef instance with the parsed values.
        """
        type_info = d.get("type", {})
        typename = type_info.get("typename", "text") if isinstance(type_info, dict) else "text"

        return cls(
            name=d["name"],
            type=BuiltinType.from_typename(typename),
            nullok=d.get("nullok", True),
            default=d.get("default"),
            comment=d.get("comment"),
            acls=d.get("acls", {}),
            acl_bindings=d.get("acl_bindings", {}),
            annotations=d.get("annotations", {}),
        )

    def with_annotation(self, tag: str, value: Any) -> ColumnDef:
        """Return a new ColumnDef with an additional annotation.

        This is a fluent method for building column definitions with annotations.

        Args:
            tag: The annotation URI/tag.
            value: The annotation value.

        Returns:
            A new ColumnDef with the annotation added.

        Example:
            >>> col = ColumnDef("Name", BuiltinType.text).with_annotation(
            ...     "tag:isrd.isi.edu,2016:column-display",
            ...     {"markdown_pattern": "**{{{Name}}}**"}
            ... )
        """
        new_annotations = {**self.annotations, tag: value}
        return ColumnDef(
            name=self.name,
            type=self.type,
            nullok=self.nullok,
            default=self.default,
            comment=self.comment,
            acls=self.acls,
            acl_bindings=self.acl_bindings,
            annotations=new_annotations,
        )


# Convenience factory functions for common column types


def text_column(
    name: str,
    *,
    nullok: bool = True,
    default: str | None = None,
    comment: str | None = None,
) -> ColumnDef:
    """Create a text column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value for new rows.
        comment: Column description.

    Returns:
        A ColumnDef for a text column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.text,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def int_column(
    name: str,
    *,
    nullok: bool = True,
    default: int | None = None,
    comment: str | None = None,
) -> ColumnDef:
    """Create an integer (int4) column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value for new rows.
        comment: Column description.

    Returns:
        A ColumnDef for an int4 column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.int4,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def float_column(
    name: str,
    *,
    nullok: bool = True,
    default: float | None = None,
    comment: str | None = None,
) -> ColumnDef:
    """Create a float (float8) column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value for new rows.
        comment: Column description.

    Returns:
        A ColumnDef for a float8 column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.float8,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def boolean_column(
    name: str,
    *,
    nullok: bool = True,
    default: bool | None = None,
    comment: str | None = None,
) -> ColumnDef:
    """Create a boolean column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value for new rows.
        comment: Column description.

    Returns:
        A ColumnDef for a boolean column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.boolean,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def date_column(
    name: str,
    *,
    nullok: bool = True,
    default: str | None = None,
    comment: str | None = None,
) -> ColumnDef:
    """Create a date column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value (date string or expression).
        comment: Column description.

    Returns:
        A ColumnDef for a date column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.date,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def timestamp_column(
    name: str,
    *,
    nullok: bool = True,
    default: str | None = None,
    comment: str | None = None,
    with_timezone: bool = True,
) -> ColumnDef:
    """Create a timestamp column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value (timestamp string or expression).
        comment: Column description.
        with_timezone: If True, use timestamptz; otherwise timestamp.

    Returns:
        A ColumnDef for a timestamp/timestamptz column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.timestamptz if with_timezone else BuiltinType.timestamp,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def markdown_column(
    name: str,
    *,
    nullok: bool = True,
    default: str | None = None,
    comment: str | None = None,
) -> ColumnDef:
    """Create a markdown column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value for new rows.
        comment: Column description.

    Returns:
        A ColumnDef for a markdown column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.markdown,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def json_column(
    name: str,
    *,
    nullok: bool = True,
    default: Any = None,
    comment: str | None = None,
    binary: bool = True,
) -> ColumnDef:
    """Create a JSON column definition.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        default: Default value for new rows.
        comment: Column description.
        binary: If True, use jsonb; otherwise json.

    Returns:
        A ColumnDef for a json/jsonb column.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.jsonb if binary else BuiltinType.json,
        nullok=nullok,
        default=default,
        comment=comment,
    )


def rid_column(
    name: str,
    *,
    nullok: bool = True,
    comment: str | None = None,
) -> ColumnDef:
    """Create a column for RID references (foreign keys to other tables).

    This is a convenience for creating text columns that will hold RID values
    as foreign key references.

    Args:
        name: Column name.
        nullok: Whether NULL values are allowed.
        comment: Column description.

    Returns:
        A ColumnDef for a text column suitable for RID references.
    """
    return ColumnDef(
        name=name,
        type=BuiltinType.text,
        nullok=nullok,
        comment=comment,
    )


# Alias for consistency
bool_column = boolean_column
