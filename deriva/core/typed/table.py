"""Typed table definition for ERMrest schemas.

This module provides dataclass-based typed interfaces for defining tables,
including regular tables, vocabulary tables, and asset tables. These replace
the dict-based `Table.define()`, `Table.define_vocabulary()`, and
`Table.define_asset()` methods from `deriva.core.ermrest_model`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from deriva.core.typed.column import ColumnDef
from deriva.core.typed.key import KeyDef
from deriva.core.typed.foreign_key import ForeignKeyDef
from deriva.core.typed.types import BuiltinType


@dataclass
class TableDef:
    """Definition for a table in an ERMrest schema.

    This is a typed replacement for `deriva.core.ermrest_model.Table.define()`.
    It provides type-safe table definition with automatic conversion to the
    dict format expected by the ERMrest API.

    Attributes:
        name: Table name. Must be unique within the schema.
        columns: List of column definitions for the table.
        keys: List of unique key constraint definitions.
        foreign_keys: List of foreign key constraint definitions.
        comment: Human-readable description of the table.
        acls: Access control lists mapping AclMode to lists of roles.
        acl_bindings: Dynamic ACL binding configurations.
        annotations: Annotation URIs mapped to annotation values.
        provide_system: Whether to inject standard system column definitions
            (RID, RCT, RMT, RCB, RMB) when missing from columns. Defaults to True.
        provide_system_fkeys: Whether to also inject foreign key definitions
            for RCB/RMB referencing public.ERMrest_Client. Defaults to True.

    Example:
        >>> table = TableDef(
        ...     name="Subject",
        ...     columns=[
        ...         ColumnDef("Name", BuiltinType.text, nullok=False),
        ...         ColumnDef("Age", BuiltinType.int4),
        ...     ],
        ...     keys=[KeyDef(["Name"])],
        ...     comment="Study subjects",
        ... )

    Note:
        System columns (RID, RCT, RMT, RCB, RMB) and their associated keys and
        foreign keys are automatically added unless `provide_system` is False.
    """

    name: str
    columns: list[ColumnDef] = field(default_factory=list)
    keys: list[KeyDef] = field(default_factory=list)
    foreign_keys: list[ForeignKeyDef] = field(default_factory=list)
    comment: str | None = None
    acls: dict[str, list[str]] = field(default_factory=dict)
    acl_bindings: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)
    provide_system: bool = True
    provide_system_fkeys: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `Table.define()` output
        and can be used directly with ERMrest schema creation methods.

        Returns:
            A dictionary with keys: table_name, column_definitions, keys,
            foreign_keys, comment, acls, acl_bindings, annotations.
        """
        from deriva.core.ermrest_model import Table

        return Table.define(
            tname=self.name,
            column_defs=[c.to_dict() for c in self.columns],
            key_defs=[k.to_dict() for k in self.keys],
            fkey_defs=[fk.to_dict() for fk in self.foreign_keys],
            comment=self.comment,
            acls=self.acls,
            acl_bindings=self.acl_bindings,
            annotations=self.annotations,
            provide_system=self.provide_system,
            provide_system_fkeys=self.provide_system_fkeys,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TableDef:
        """Create a TableDef from a dictionary representation.

        This parses table definition dictionaries from ERMrest API responses
        or from `Table.define()` output.

        Args:
            d: A dictionary with table definition fields.

        Returns:
            A TableDef instance with the parsed values.
        """
        return cls(
            name=d.get("table_name", ""),
            columns=[ColumnDef.from_dict(c) for c in d.get("column_definitions", [])],
            keys=[KeyDef.from_dict(k) for k in d.get("keys", [])],
            foreign_keys=[ForeignKeyDef.from_dict(fk) for fk in d.get("foreign_keys", [])],
            comment=d.get("comment"),
            acls=d.get("acls", {}),
            acl_bindings=d.get("acl_bindings", {}),
            annotations=d.get("annotations", {}),
            provide_system=False,  # Already expanded
            provide_system_fkeys=False,
        )

    def with_annotation(self, tag: str, value: Any) -> TableDef:
        """Return a new TableDef with an additional annotation.

        Args:
            tag: The annotation URI/tag.
            value: The annotation value.

        Returns:
            A new TableDef with the annotation added.
        """
        new_annotations = {**self.annotations, tag: value}
        return TableDef(
            name=self.name,
            columns=self.columns.copy(),
            keys=self.keys.copy(),
            foreign_keys=self.foreign_keys.copy(),
            comment=self.comment,
            acls=self.acls.copy(),
            acl_bindings=self.acl_bindings.copy(),
            annotations=new_annotations,
            provide_system=self.provide_system,
            provide_system_fkeys=self.provide_system_fkeys,
        )

    def with_column(self, column: ColumnDef) -> TableDef:
        """Return a new TableDef with an additional column.

        Args:
            column: The column definition to add.

        Returns:
            A new TableDef with the column added.
        """
        return TableDef(
            name=self.name,
            columns=[*self.columns, column],
            keys=self.keys.copy(),
            foreign_keys=self.foreign_keys.copy(),
            comment=self.comment,
            acls=self.acls.copy(),
            acl_bindings=self.acl_bindings.copy(),
            annotations=self.annotations.copy(),
            provide_system=self.provide_system,
            provide_system_fkeys=self.provide_system_fkeys,
        )

    def with_foreign_key(self, fkey: ForeignKeyDef) -> TableDef:
        """Return a new TableDef with an additional foreign key.

        Args:
            fkey: The foreign key definition to add.

        Returns:
            A new TableDef with the foreign key added.
        """
        return TableDef(
            name=self.name,
            columns=self.columns.copy(),
            keys=self.keys.copy(),
            foreign_keys=[*self.foreign_keys, fkey],
            comment=self.comment,
            acls=self.acls.copy(),
            acl_bindings=self.acl_bindings.copy(),
            annotations=self.annotations.copy(),
            provide_system=self.provide_system,
            provide_system_fkeys=self.provide_system_fkeys,
        )


@dataclass
class VocabularyTableDef:
    """Definition for a vocabulary (controlled terminology) table.

    This is a typed replacement for `deriva.core.ermrest_model.Table.define_vocabulary()`.
    Vocabulary tables are used for controlled terminology with standardized columns.

    Vocabulary tables automatically include these columns (unless overridden):
        - ID: ermrest_curie, unique not null, default curie template
        - URI: ermrest_uri, unique not null, default URI template
        - Name: text, unique not null
        - Description: markdown, not null
        - Synonyms: text[]

    Attributes:
        name: Table name for the vocabulary.
        curie_template: RID-based template for the CURIE of locally-defined terms.
            Must match pattern like 'PREFIX:{RID}'.
        uri_template: RID-based template for the URI of locally-defined terms.
            Defaults to '/id/{RID}'.
        columns: Additional column definitions beyond the standard vocabulary columns.
        keys: Additional key constraint definitions.
        foreign_keys: Foreign key constraint definitions.
        comment: Human-readable description of the vocabulary.
        acls: Access control lists.
        acl_bindings: Dynamic ACL binding configurations.
        annotations: Annotation URIs mapped to annotation values.
        provide_system: Whether to inject system columns. Defaults to True.
        provide_system_fkeys: Whether to inject system foreign keys. Defaults to True.
        provide_name_key: Whether to inject a key on the Name column. Defaults to True.

    Example:
        >>> vocab = VocabularyTableDef(
        ...     name="Diagnosis_Type",
        ...     curie_template="MYPROJECT:{RID}",
        ...     comment="Types of diagnoses",
        ... )
    """

    name: str
    curie_template: str
    uri_template: str = "/id/{RID}"
    columns: list[ColumnDef] = field(default_factory=list)
    keys: list[KeyDef] = field(default_factory=list)
    foreign_keys: list[ForeignKeyDef] = field(default_factory=list)
    comment: str | None = None
    acls: dict[str, list[str]] = field(default_factory=dict)
    acl_bindings: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)
    provide_system: bool = True
    provide_system_fkeys: bool = True
    provide_name_key: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `Table.define_vocabulary()` output.

        Returns:
            A dictionary with the complete vocabulary table definition.
        """
        from deriva.core.ermrest_model import Table

        return Table.define_vocabulary(
            tname=self.name,
            curie_template=self.curie_template,
            uri_template=self.uri_template,
            column_defs=[c.to_dict() for c in self.columns],
            key_defs=[k.to_dict() for k in self.keys],
            fkey_defs=[fk.to_dict() for fk in self.foreign_keys],
            comment=self.comment,
            acls=self.acls,
            acl_bindings=self.acl_bindings,
            annotations=self.annotations,
            provide_system=self.provide_system,
            provide_system_fkeys=self.provide_system_fkeys,
            provide_name_key=self.provide_name_key,
        )


@dataclass
class AssetTableDef:
    """Definition for an asset (file storage) table.

    This is a typed replacement for `deriva.core.ermrest_model.Table.define_asset()`.
    Asset tables are used to store file references with automatic URL and checksum
    management through Hatrac integration.

    Asset tables automatically include these columns (unless overridden):
        - URL: ermrest_uri, not null - File location
        - Filename: text - Original filename
        - Length: int8 - File size in bytes
        - MD5: text - MD5 checksum
        - Description: markdown - File description

    Attributes:
        schema_name: Schema name for the asset table.
        name: Table name for the asset.
        columns: Additional column definitions beyond standard asset columns.
        keys: Key constraint definitions.
        foreign_keys: Foreign key constraint definitions.
        comment: Human-readable description of the asset table.
        acls: Access control lists.
        acl_bindings: Dynamic ACL binding configurations.
        annotations: Annotation URIs mapped to annotation values.
        hatrac_template: Hatrac namespace template for file storage.
            Defaults to '/hatrac/{schema_name}/{table_name}/{{{MD5}}}'.

    Example:
        >>> asset = AssetTableDef(
        ...     schema_name="domain",
        ...     name="Image",
        ...     columns=[
        ...         ColumnDef("Width", BuiltinType.int4),
        ...         ColumnDef("Height", BuiltinType.int4),
        ...     ],
        ...     comment="Image assets",
        ... )
    """

    schema_name: str
    name: str
    columns: list[ColumnDef] = field(default_factory=list)
    keys: list[KeyDef] = field(default_factory=list)
    foreign_keys: list[ForeignKeyDef] = field(default_factory=list)
    comment: str | None = None
    acls: dict[str, list[str]] = field(default_factory=dict)
    acl_bindings: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)
    hatrac_template: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `Table.define_asset()` output.

        Returns:
            A dictionary with the complete asset table definition.
        """
        from deriva.core.ermrest_model import Table

        return Table.define_asset(
            sname=self.schema_name,
            tname=self.name,
            column_defs=[c.to_dict() for c in self.columns],
            key_defs=[k.to_dict() for k in self.keys],
            fkey_defs=[fk.to_dict() for fk in self.foreign_keys],
            comment=self.comment,
            acls=self.acls,
            acl_bindings=self.acl_bindings,
            annotations=self.annotations,
            hatrac_template=self.hatrac_template,
        )


# Convenience factory functions


def simple_table(
    name: str,
    columns: list[ColumnDef],
    *,
    comment: str | None = None,
) -> TableDef:
    """Create a simple table with columns and default system configuration.

    Args:
        name: Table name.
        columns: List of column definitions.
        comment: Table description.

    Returns:
        A TableDef with the specified columns and default system columns.
    """
    return TableDef(
        name=name,
        columns=columns,
        comment=comment,
    )


def association_table(
    name: str,
    left_table: str,
    left_schema: str,
    right_table: str,
    right_schema: str,
    *,
    left_column: str | None = None,
    right_column: str | None = None,
    comment: str | None = None,
) -> TableDef:
    """Create an association (many-to-many) table.

    This creates a table with foreign keys to two other tables, commonly used
    to implement many-to-many relationships.

    Args:
        name: Association table name.
        left_table: Name of the first table.
        left_schema: Schema of the first table.
        right_table: Name of the second table.
        right_schema: Schema of the second table.
        left_column: Column name for left reference (defaults to table name).
        right_column: Column name for right reference (defaults to table name).
        comment: Table description.

    Returns:
        A TableDef for the association table.
    """
    left_col = left_column or left_table
    right_col = right_column or right_table

    return TableDef(
        name=name,
        columns=[
            ColumnDef(left_col, BuiltinType.text, nullok=False),
            ColumnDef(right_col, BuiltinType.text, nullok=False),
        ],
        keys=[
            KeyDef([left_col, right_col]),  # Composite key prevents duplicates
        ],
        foreign_keys=[
            ForeignKeyDef(
                columns=[left_col],
                referenced_schema=left_schema,
                referenced_table=left_table,
            ),
            ForeignKeyDef(
                columns=[right_col],
                referenced_schema=right_schema,
                referenced_table=right_table,
            ),
        ],
        comment=comment or f"Association between {left_table} and {right_table}",
    )
