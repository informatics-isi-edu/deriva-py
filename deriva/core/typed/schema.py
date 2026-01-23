"""Typed schema definition for ERMrest catalogs.

This module provides a dataclass-based typed interface for defining schemas,
replacing the dict-based `Schema.define()` method from `deriva.core.ermrest_model`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from deriva.core.typed.table import TableDef


@dataclass
class SchemaDef:
    """Definition for a schema in an ERMrest catalog.

    This is a typed replacement for `deriva.core.ermrest_model.Schema.define()`.
    It provides type-safe schema definition with automatic conversion to the
    dict format expected by the ERMrest API.

    A schema is a namespace for organizing tables within a catalog. Each catalog
    has at least the 'public' schema, and additional schemas can be created for
    domain-specific data organization.

    Attributes:
        name: Schema name. Must be unique within the catalog.
        tables: Optional dict mapping table names to TableDef instances.
            Tables can also be created separately after the schema.
        comment: Human-readable description of the schema.
        acls: Access control lists mapping AclMode to lists of roles.
        annotations: Annotation URIs mapped to annotation values.

    Example:
        >>> schema = SchemaDef(
        ...     name="domain",
        ...     comment="Domain-specific tables for the application",
        ... )
        >>> schema.to_dict()
        {'schema_name': 'domain', 'acls': {}, 'annotations': {}, 'comment': '...'}

        >>> # Schema with tables
        >>> schema = SchemaDef(
        ...     name="domain",
        ...     tables={
        ...         "Subject": TableDef("Subject", columns=[...]),
        ...         "Image": TableDef("Image", columns=[...]),
        ...     },
        ... )

    Note:
        When creating schemas via the ERMrest API, tables are typically created
        separately after the schema exists. The `tables` attribute is useful
        for defining a complete schema structure upfront.
    """

    name: str
    tables: dict[str, TableDef] | None = None
    comment: str | None = None
    acls: dict[str, list[str]] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `Schema.define()` output
        and can be used directly with ERMrest catalog creation methods.

        Returns:
            A dictionary with keys: schema_name, acls, annotations, comment.
            If tables are provided, includes a 'tables' key with table definitions.
        """
        from deriva.core.ermrest_model import Schema

        result = Schema.define(
            sname=self.name,
            comment=self.comment,
            acls=self.acls,
            annotations=self.annotations,
        )

        if self.tables:
            result["tables"] = {
                name: table.to_dict() for name, table in self.tables.items()
            }

        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SchemaDef:
        """Create a SchemaDef from a dictionary representation.

        This parses schema definition dictionaries from ERMrest API responses
        or from `Schema.define()` output.

        Args:
            d: A dictionary with schema definition fields.

        Returns:
            A SchemaDef instance with the parsed values.
        """
        from deriva.core.typed.table import TableDef

        tables = None
        if "tables" in d:
            tables = {
                name: TableDef.from_dict(tdoc) for name, tdoc in d["tables"].items()
            }

        return cls(
            name=d.get("schema_name", ""),
            tables=tables,
            comment=d.get("comment"),
            acls=d.get("acls", {}),
            annotations=d.get("annotations", {}),
        )

    def with_annotation(self, tag: str, value: Any) -> SchemaDef:
        """Return a new SchemaDef with an additional annotation.

        Args:
            tag: The annotation URI/tag.
            value: The annotation value.

        Returns:
            A new SchemaDef with the annotation added.
        """
        new_annotations = {**self.annotations, tag: value}
        return SchemaDef(
            name=self.name,
            tables=self.tables.copy() if self.tables else None,
            comment=self.comment,
            acls=self.acls.copy(),
            annotations=new_annotations,
        )

    def with_table(self, table: TableDef) -> SchemaDef:
        """Return a new SchemaDef with an additional table.

        Args:
            table: The table definition to add.

        Returns:
            A new SchemaDef with the table added.
        """
        new_tables = dict(self.tables) if self.tables else {}
        new_tables[table.name] = table
        return SchemaDef(
            name=self.name,
            tables=new_tables,
            comment=self.comment,
            acls=self.acls.copy(),
            annotations=self.annotations.copy(),
        )


def domain_schema(
    name: str = "domain",
    *,
    comment: str | None = None,
) -> SchemaDef:
    """Create a domain schema definition.

    This is a convenience function for creating the common pattern of a
    single domain schema for application data.

    Args:
        name: Schema name. Defaults to "domain".
        comment: Schema description.

    Returns:
        A SchemaDef for the domain schema.
    """
    return SchemaDef(
        name=name,
        comment=comment or "Domain schema for application data",
    )


def ml_schema(name: str = "deriva-ml") -> SchemaDef:
    """Create a machine learning schema definition.

    This creates a schema for ML-related tables following the DerivaML pattern.

    Args:
        name: Schema name. Defaults to "deriva-ml".

    Returns:
        A SchemaDef for the ML schema.
    """
    return SchemaDef(
        name=name,
        comment="Machine learning workflow and data management schema",
    )


@dataclass
class WWWSchemaDef:
    """Definition for a wiki-like web content schema.

    This is a typed replacement for `deriva.core.ermrest_model.Schema.define_www()`.
    A WWW schema contains a "Page" wiki-like page table and a "File" asset table
    for attachments to the wiki pages.

    Attributes:
        name: Schema name.
        comment: Human-readable description of the schema.
        acls: Access control lists mapping AclMode to lists of roles.
        annotations: Annotation URIs mapped to annotation values.

    Example:
        >>> www = WWWSchemaDef(
        ...     name="documentation",
        ...     comment="Wiki-like documentation pages",
        ... )
        >>> schema_dict = www.to_dict()
        >>> # schema_dict includes Page and File table definitions
    """

    name: str
    comment: str | None = None
    acls: dict[str, list[str]] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        This produces a dictionary compatible with `Schema.define_www()` output,
        which includes both the schema definition and the Page and File tables.

        Returns:
            A dictionary with the complete WWW schema definition including
            Page and File table definitions.
        """
        from deriva.core.ermrest_model import Schema

        return Schema.define_www(
            sname=self.name,
            comment=self.comment,
            acls=self.acls,
            annotations=self.annotations,
        )
