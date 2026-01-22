"""Typed interface for ERMrest model definitions.

This module provides dataclass-based typed wrappers around the dict-based
interfaces in `deriva.core.ermrest_model`. The typed interfaces provide:

- Type hints and IDE autocompletion
- Compile-time type checking with mypy/pyright
- Runtime validation via dataclass field types
- Automatic conversion to the dict formats expected by ERMrest

Usage:
    from deriva.core.typed import (
        ColumnDef, KeyDef, ForeignKeyDef, TableDef, SchemaDef,
        BuiltinType, OnAction,
    )

    # Create typed definitions
    col = ColumnDef(name="Age", type=BuiltinType.int4)
    fkey = ForeignKeyDef(
        columns=["Subject"],
        referenced_schema="domain",
        referenced_table="Subject",
        referenced_columns=["RID"],
        on_delete=OnAction.CASCADE,
    )
    table = TableDef(
        name="Experiment",
        columns=[col],
        foreign_keys=[fkey],
    )

    # Convert to dict for use with ERMrest API
    table_dict = table.to_dict()

The import paths mirror the native interface:
    - deriva.core.ermrest_model.Column -> deriva.core.typed.ColumnDef
    - deriva.core.ermrest_model.Key -> deriva.core.typed.KeyDef
    - deriva.core.ermrest_model.ForeignKey -> deriva.core.typed.ForeignKeyDef
    - deriva.core.ermrest_model.Table -> deriva.core.typed.TableDef
    - deriva.core.ermrest_model.Schema -> deriva.core.typed.SchemaDef
"""

from deriva.core.typed.types import BuiltinType, OnAction, AclMode
from deriva.core.typed.column import ColumnDef
from deriva.core.typed.key import KeyDef
from deriva.core.typed.foreign_key import ForeignKeyDef
from deriva.core.typed.table import TableDef, VocabularyTableDef, AssetTableDef
from deriva.core.typed.schema import SchemaDef
from deriva.core.typed.acl import Acl, AclBinding, AclBindings
from deriva.core.typed.annotations import (
    # Tag URIs
    Tag,
    # Core building blocks
    SortKey,
    ForeignKeyPath,
    SourceEntry,
    PseudoColumn,
    PreFormat,
    # Annotation dataclasses
    DisplayAnnotation,
    TableDisplayOptions,
    TableDisplayAnnotation,
    ColumnDisplayOptions,
    ColumnDisplayAnnotation,
    VisibleColumnsAnnotation,
    VisibleForeignKeysAnnotation,
    AssetAnnotation,
    ForeignKeyAnnotation,
    SourceDefinition,
    SourceDefinitionsAnnotation,
    CitationAnnotation,
    # Convenience functions
    display,
    row_name_pattern,
    visible_columns,
    visible_foreign_keys,
    generated,
    immutable,
    non_deletable,
    required,
)

__all__ = [
    # Types and enums
    "BuiltinType",
    "OnAction",
    "AclMode",
    # Definition classes
    "ColumnDef",
    "KeyDef",
    "ForeignKeyDef",
    "TableDef",
    "VocabularyTableDef",
    "AssetTableDef",
    "SchemaDef",
    # ACL classes
    "Acl",
    "AclBinding",
    "AclBindings",
    # Annotation tag URIs
    "Tag",
    # Annotation building blocks
    "SortKey",
    "ForeignKeyPath",
    "SourceEntry",
    "PseudoColumn",
    "PreFormat",
    # Annotation dataclasses
    "DisplayAnnotation",
    "TableDisplayOptions",
    "TableDisplayAnnotation",
    "ColumnDisplayOptions",
    "ColumnDisplayAnnotation",
    "VisibleColumnsAnnotation",
    "VisibleForeignKeysAnnotation",
    "AssetAnnotation",
    "ForeignKeyAnnotation",
    "SourceDefinition",
    "SourceDefinitionsAnnotation",
    "CitationAnnotation",
    # Annotation convenience functions
    "display",
    "row_name_pattern",
    "visible_columns",
    "visible_foreign_keys",
    "generated",
    "immutable",
    "non_deletable",
    "required",
]
