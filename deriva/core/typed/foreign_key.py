"""Typed foreign key constraint definition for ERMrest tables.

This module provides a dataclass-based typed interface for defining foreign key
constraints, replacing the dict-based `ForeignKey.define()` method from
`deriva.core.ermrest_model`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from deriva.core.typed.types import OnAction


@dataclass
class ForeignKeyDef:
    """Definition for a foreign key constraint in an ERMrest table.

    This is a typed replacement for `deriva.core.ermrest_model.ForeignKey.define()`.
    It provides type-safe foreign key definition with automatic conversion to the
    dict format expected by the ERMrest API.

    A foreign key establishes a referential relationship between columns in the
    current table and columns in a referenced (primary key) table.

    Attributes:
        columns: List of column names in this table participating in the
            foreign key. These columns reference the primary key columns.
        referenced_schema: Schema name of the referenced table.
        referenced_table: Name of the referenced table.
        referenced_columns: List of column names in the referenced table.
            Must have the same length as `columns`. If not provided, defaults
            to ["RID"].
        on_update: Action when referenced primary key values are updated.
            Defaults to NO_ACTION.
        on_delete: Action when referenced primary key rows are deleted.
            Defaults to NO_ACTION.
        constraint_name: Optional constraint name string. If not provided,
            ERMrest will auto-generate a name.
        comment: Human-readable description of the foreign key.
        acls: Access control lists for the foreign key reference.
        acl_bindings: Dynamic ACL binding configurations.
        annotations: Annotation URIs mapped to annotation values.

    Example:
        >>> # Simple foreign key referencing RID
        >>> fkey = ForeignKeyDef(
        ...     columns=["Subject"],
        ...     referenced_schema="domain",
        ...     referenced_table="Subject",
        ... )

        >>> # Foreign key with cascade delete
        >>> fkey = ForeignKeyDef(
        ...     columns=["Parent_ID"],
        ...     referenced_schema="domain",
        ...     referenced_table="Node",
        ...     referenced_columns=["RID"],
        ...     on_delete=OnAction.CASCADE,
        ... )

        >>> # Composite foreign key
        >>> fkey = ForeignKeyDef(
        ...     columns=["Subject_FirstName", "Subject_LastName"],
        ...     referenced_schema="domain",
        ...     referenced_table="Subject",
        ...     referenced_columns=["FirstName", "LastName"],
        ... )

    Note:
        The constraint behavior values (on_update, on_delete) must be one of:
        NO_ACTION, RESTRICT, CASCADE, SET_NULL, SET_DEFAULT
    """

    columns: list[str]
    referenced_schema: str
    referenced_table: str
    referenced_columns: list[str] | None = None
    on_update: OnAction = OnAction.NO_ACTION
    on_delete: OnAction = OnAction.NO_ACTION
    constraint_name: str | None = None
    comment: str | None = None
    acls: dict[str, list[str]] = field(default_factory=dict)
    acl_bindings: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate and normalize foreign key definition."""
        if not self.columns:
            raise ValueError("Foreign key must have at least one column")
        if not isinstance(self.columns, list):
            self.columns = list(self.columns)

        # Default to referencing RID
        if self.referenced_columns is None:
            self.referenced_columns = ["RID"]
        elif not isinstance(self.referenced_columns, list):
            self.referenced_columns = list(self.referenced_columns)

        if len(self.columns) != len(self.referenced_columns):
            raise ValueError(
                f"Foreign key columns ({len(self.columns)}) and referenced columns "
                f"({len(self.referenced_columns)}) must have the same length"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `ForeignKey.define()` output
        and can be used directly with ERMrest schema creation methods.

        Returns:
            A dictionary with keys: foreign_key_columns, referenced_columns,
            on_update, on_delete, names, comment, acls, acl_bindings, annotations.
        """
        from deriva.core.ermrest_model import ForeignKey
        return ForeignKey.define(
            fk_colnames=self.columns,
            pk_sname=self.referenced_schema,
            pk_tname=self.referenced_table,
            pk_colnames=self.referenced_columns,
            on_update=self.on_update.value,
            on_delete=self.on_delete.value,
            constraint_name=self.constraint_name,
            comment=self.comment,
            acls=self.acls,
            acl_bindings=self.acl_bindings,
            annotations=self.annotations,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ForeignKeyDef:
        """Create a ForeignKeyDef from a dictionary representation.

        This parses foreign key definition dictionaries from ERMrest API responses
        or from `ForeignKey.define()` output.

        Args:
            d: A dictionary with foreign key definition fields.

        Returns:
            A ForeignKeyDef instance with the parsed values.
        """
        # Extract column names
        fk_columns = [c.get("column_name") for c in d.get("foreign_key_columns", [])]
        ref_columns_doc = d.get("referenced_columns", [])

        # Get referenced table info from first referenced column
        ref_schema = ref_columns_doc[0].get("schema_name", "") if ref_columns_doc else ""
        ref_table = ref_columns_doc[0].get("table_name", "") if ref_columns_doc else ""
        ref_columns = [c.get("column_name") for c in ref_columns_doc]

        # Extract constraint name from legacy 'names' format
        constraint_name = None
        names = d.get("names", [])
        if names and len(names) > 0 and len(names[0]) >= 2:
            constraint_name = names[0][1]

        return cls(
            columns=fk_columns,
            referenced_schema=ref_schema,
            referenced_table=ref_table,
            referenced_columns=ref_columns,
            on_update=OnAction(d.get("on_update", "NO ACTION")),
            on_delete=OnAction(d.get("on_delete", "NO ACTION")),
            constraint_name=constraint_name,
            comment=d.get("comment"),
            acls=d.get("acls", {}),
            acl_bindings=d.get("acl_bindings", {}),
            annotations=d.get("annotations", {}),
        )

    def with_annotation(self, tag: str, value: Any) -> ForeignKeyDef:
        """Return a new ForeignKeyDef with an additional annotation.

        This is a fluent method for building foreign key definitions with annotations.

        Args:
            tag: The annotation URI/tag.
            value: The annotation value.

        Returns:
            A new ForeignKeyDef with the annotation added.
        """
        new_annotations = {**self.annotations, tag: value}
        return ForeignKeyDef(
            columns=self.columns.copy(),
            referenced_schema=self.referenced_schema,
            referenced_table=self.referenced_table,
            referenced_columns=self.referenced_columns.copy() if self.referenced_columns else None,
            on_update=self.on_update,
            on_delete=self.on_delete,
            constraint_name=self.constraint_name,
            comment=self.comment,
            acls=self.acls.copy(),
            acl_bindings=self.acl_bindings.copy(),
            annotations=new_annotations,
        )


def simple_fkey(
    column: str,
    referenced_schema: str,
    referenced_table: str,
    *,
    on_delete: OnAction = OnAction.NO_ACTION,
    on_update: OnAction = OnAction.NO_ACTION,
) -> ForeignKeyDef:
    """Create a simple foreign key referencing RID.

    This is a convenience function for the common case of a single column
    referencing the RID of another table.

    Args:
        column: The column name in this table.
        referenced_schema: Schema of the referenced table.
        referenced_table: Name of the referenced table.
        on_delete: Action when referenced row is deleted.
        on_update: Action when referenced row is updated.

    Returns:
        A ForeignKeyDef for the simple reference.

    Example:
        >>> fkey = simple_fkey("Subject", "domain", "Subject", on_delete=OnAction.CASCADE)
    """
    return ForeignKeyDef(
        columns=[column],
        referenced_schema=referenced_schema,
        referenced_table=referenced_table,
        referenced_columns=["RID"],
        on_delete=on_delete,
        on_update=on_update,
    )


def cascade_fkey(
    column: str,
    referenced_schema: str,
    referenced_table: str,
) -> ForeignKeyDef:
    """Create a cascading foreign key.

    This creates a foreign key with CASCADE behavior for both updates and deletes,
    which is common for parent-child relationships.

    Args:
        column: The column name in this table.
        referenced_schema: Schema of the referenced table.
        referenced_table: Name of the referenced table.

    Returns:
        A ForeignKeyDef with CASCADE on update and delete.

    Example:
        >>> fkey = cascade_fkey("Parent", "domain", "Node")
    """
    return ForeignKeyDef(
        columns=[column],
        referenced_schema=referenced_schema,
        referenced_table=referenced_table,
        referenced_columns=["RID"],
        on_update=OnAction.CASCADE,
        on_delete=OnAction.CASCADE,
    )


def ermrest_client_fkey(column: str, constraint_name: str | None = None) -> ForeignKeyDef:
    """Create a foreign key to the ERMrest_Client table.

    This is used for columns that reference users/clients, such as
    the system RCB and RMB columns.

    Args:
        column: The column name (e.g., "RCB", "RMB", "Owner").
        constraint_name: Optional constraint name.

    Returns:
        A ForeignKeyDef referencing public.ERMrest_Client.ID
    """
    return ForeignKeyDef(
        columns=[column],
        referenced_schema="public",
        referenced_table="ERMrest_Client",
        referenced_columns=["ID"],
        constraint_name=constraint_name,
    )
