"""Typed annotation definitions for ERMrest catalog resources.

This module provides dataclass-based typed interfaces for defining annotations,
replacing the dict-based annotation configurations used throughout `deriva.core`.

Annotations control how data is displayed, filtered, and exported in Chaise and
other Deriva clients.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from deriva.core.typed.types import TemplateEngine, DisplayContext


# Re-export the tag constants for convenience
def _get_tags() -> dict[str, str]:
    """Get the annotation tag URIs from core_utils."""
    from deriva.core.utils.core_utils import tag
    return dict(tag)


# Annotation tag URIs
class Tag:
    """Annotation tag URIs for ERMrest resources.

    These are the standard annotation URIs used to control display, behavior,
    and configuration of catalog resources in Chaise and other clients.
    """

    display = "tag:misd.isi.edu,2015:display"
    table_alternatives = "tag:isrd.isi.edu,2016:table-alternatives"
    column_display = "tag:isrd.isi.edu,2016:column-display"
    key_display = "tag:isrd.isi.edu,2017:key-display"
    foreign_key = "tag:isrd.isi.edu,2016:foreign-key"
    generated = "tag:isrd.isi.edu,2016:generated"
    immutable = "tag:isrd.isi.edu,2016:immutable"
    non_deletable = "tag:isrd.isi.edu,2016:non-deletable"
    app_links = "tag:isrd.isi.edu,2016:app-links"
    table_display = "tag:isrd.isi.edu,2016:table-display"
    visible_columns = "tag:isrd.isi.edu,2016:visible-columns"
    visible_foreign_keys = "tag:isrd.isi.edu,2016:visible-foreign-keys"
    export = "tag:isrd.isi.edu,2016:export"
    export_2019 = "tag:isrd.isi.edu,2019:export"
    export_fragment_definitions = "tag:isrd.isi.edu,2021:export-fragment-definitions"
    asset = "tag:isrd.isi.edu,2017:asset"
    citation = "tag:isrd.isi.edu,2018:citation"
    required = "tag:isrd.isi.edu,2018:required"
    indexing_preferences = "tag:isrd.isi.edu,2018:indexing-preferences"
    bulk_upload = "tag:isrd.isi.edu,2017:bulk-upload"
    chaise_config = "tag:isrd.isi.edu,2019:chaise-config"
    source_definitions = "tag:isrd.isi.edu,2019:source-definitions"
    google_dataset = "tag:isrd.isi.edu,2021:google-dataset"
    column_defaults = "tag:isrd.isi.edu,2023:column-defaults"
    viz_3d_display = "tag:isrd.isi.edu,2021:viz-3d-display"


# Type aliases for annotation values
ContextName = Literal[
    "*",
    "compact",
    "compact/select",
    "compact/brief",
    "compact/brief/inline",
    "detailed",
    "entry",
    "entry/edit",
    "entry/create",
    "export",
    "filter",
    "row_name",
    "row_name/title",
    "row_name/compact",
    "row_name/detailed",
]


@dataclass
class SortKey:
    """A sort key for ordering rows.

    Attributes:
        column: The column name to sort by.
        descending: Whether to sort in descending order. Defaults to False.
    """

    column: str
    descending: bool = False

    def to_dict(self) -> dict[str, Any] | str:
        """Convert to dict format for ERMrest API.

        Returns a simple column name string if ascending (the default),
        or a dict with column and descending keys otherwise.
        """
        if self.descending:
            return {"column": self.column, "descending": True}
        return self.column


@dataclass
class ForeignKeyPath:
    """A foreign key path element for source definitions.

    Either `inbound` or `outbound` should be set, but not both.

    Attributes:
        inbound: Constraint name [schema, constraint] for inbound relationship.
        outbound: Constraint name [schema, constraint] for outbound relationship.
    """

    inbound: list[str] | None = None
    outbound: list[str] | None = None

    def __post_init__(self) -> None:
        if self.inbound and self.outbound:
            raise ValueError("Cannot specify both inbound and outbound")
        if not self.inbound and not self.outbound:
            raise ValueError("Must specify either inbound or outbound")

    def to_dict(self) -> dict[str, list[str]]:
        """Convert to dict format for ERMrest API."""
        if self.inbound:
            return {"inbound": self.inbound}
        return {"outbound": self.outbound}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ForeignKeyPath:
        """Create from dictionary representation."""
        return cls(
            inbound=d.get("inbound"),
            outbound=d.get("outbound"),
        )


@dataclass
class SourceEntry:
    """A source entry for pseudo-columns and visible columns.

    A source can be either a simple column name or a path through
    foreign keys to reach a column in a related table.

    Attributes:
        column: The target column name.
        path: Optional list of foreign key path elements leading to the column.
    """

    column: str
    path: list[ForeignKeyPath] = field(default_factory=list)

    def to_dict(self) -> str | list[Any]:
        """Convert to format expected by ERMrest API.

        Returns a simple column name if no path, or a list of path elements
        followed by the column name.
        """
        if not self.path:
            return self.column
        return [*[p.to_dict() for p in self.path], self.column]

    @classmethod
    def from_dict(cls, value: str | list[Any]) -> SourceEntry:
        """Create from dictionary representation."""
        if isinstance(value, str):
            return cls(column=value)

        # Parse path and final column
        path = []
        column = ""
        for item in value:
            if isinstance(item, str):
                column = item
            elif isinstance(item, dict):
                path.append(ForeignKeyPath.from_dict(item))
        return cls(column=column, path=path)


@dataclass
class DisplayAnnotation:
    """Display configuration for tables and columns.

    This corresponds to the 'tag:misd.isi.edu,2015:display' annotation.

    Attributes:
        name: Display name for the resource.
        markdown_name: Display name with markdown formatting.
        name_style: Style for name formatting (title, underline, etc.).
        show_null: How to display null values ("" to hide, text to show).
        show_key_link: Whether to show links for key values.
        show_foreign_key_link: Whether to show links for foreign key values.
    """

    name: str | None = None
    markdown_name: str | None = None
    name_style: dict[str, Any] | None = None
    show_null: str | dict[str, str] | None = None
    show_key_link: bool | None = None
    show_foreign_key_link: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.name is not None:
            result["name"] = self.name
        if self.markdown_name is not None:
            result["markdown_name"] = self.markdown_name
        if self.name_style is not None:
            result["name_style"] = self.name_style
        if self.show_null is not None:
            result["show_null"] = self.show_null
        if self.show_key_link is not None:
            result["show_key_link"] = self.show_key_link
        if self.show_foreign_key_link is not None:
            result["show_foreign_key_link"] = self.show_foreign_key_link
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DisplayAnnotation:
        """Create from dictionary representation."""
        return cls(
            name=d.get("name"),
            markdown_name=d.get("markdown_name"),
            name_style=d.get("name_style"),
            show_null=d.get("show_null"),
            show_key_link=d.get("show_key_link"),
            show_foreign_key_link=d.get("show_foreign_key_link"),
        )


@dataclass
class TableDisplayOptions:
    """Display options for a table in a specific context.

    Attributes:
        row_order: List of sort keys for row ordering.
        page_size: Number of rows per page.
        collapse_toc_panel: Whether to collapse the table of contents panel.
        hide_column_headers: Whether to hide column headers.
        page_markdown_pattern: Markdown pattern for the entire page.
        row_markdown_pattern: Markdown pattern for each row.
        separator_markdown: Markdown separator between rows.
        prefix_markdown: Markdown prefix before all rows.
        suffix_markdown: Markdown suffix after all rows.
        template_engine: Template engine to use (handlebars or mustache).
    """

    row_order: list[SortKey] | None = None
    page_size: int | None = None
    collapse_toc_panel: bool | None = None
    hide_column_headers: bool | None = None
    page_markdown_pattern: str | None = None
    row_markdown_pattern: str | None = None
    separator_markdown: str | None = None
    prefix_markdown: str | None = None
    suffix_markdown: str | None = None
    template_engine: TemplateEngine | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.row_order is not None:
            result["row_order"] = [s.to_dict() for s in self.row_order]
        if self.page_size is not None:
            result["page_size"] = self.page_size
        if self.collapse_toc_panel is not None:
            result["collapse_toc_panel"] = self.collapse_toc_panel
        if self.hide_column_headers is not None:
            result["hide_column_headers"] = self.hide_column_headers
        if self.page_markdown_pattern is not None:
            result["page_markdown_pattern"] = self.page_markdown_pattern
        if self.row_markdown_pattern is not None:
            result["row_markdown_pattern"] = self.row_markdown_pattern
        if self.separator_markdown is not None:
            result["separator_markdown"] = self.separator_markdown
        if self.prefix_markdown is not None:
            result["prefix_markdown"] = self.prefix_markdown
        if self.suffix_markdown is not None:
            result["suffix_markdown"] = self.suffix_markdown
        if self.template_engine is not None:
            result["template_engine"] = self.template_engine.value
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TableDisplayOptions:
        """Create from dictionary representation."""
        row_order = None
        if "row_order" in d:
            row_order = []
            for item in d["row_order"]:
                if isinstance(item, str):
                    row_order.append(SortKey(column=item))
                else:
                    row_order.append(SortKey(
                        column=item["column"],
                        descending=item.get("descending", False),
                    ))

        template_engine = None
        if "template_engine" in d:
            template_engine = TemplateEngine(d["template_engine"])

        return cls(
            row_order=row_order,
            page_size=d.get("page_size"),
            collapse_toc_panel=d.get("collapse_toc_panel"),
            hide_column_headers=d.get("hide_column_headers"),
            page_markdown_pattern=d.get("page_markdown_pattern"),
            row_markdown_pattern=d.get("row_markdown_pattern"),
            separator_markdown=d.get("separator_markdown"),
            prefix_markdown=d.get("prefix_markdown"),
            suffix_markdown=d.get("suffix_markdown"),
            template_engine=template_engine,
        )


@dataclass
class TableDisplayAnnotation:
    """Table display configuration for multiple contexts.

    This corresponds to the 'tag:isrd.isi.edu,2016:table-display' annotation.

    Attributes:
        contexts: Mapping of context names to display options.
    """

    contexts: dict[str, TableDisplayOptions | str | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        for context, options in self.contexts.items():
            if options is None:
                result[context] = None
            elif isinstance(options, str):
                result[context] = options
            else:
                result[context] = options.to_dict()
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TableDisplayAnnotation:
        """Create from dictionary representation."""
        contexts: dict[str, TableDisplayOptions | str | None] = {}
        for context, value in d.items():
            if value is None:
                contexts[context] = None
            elif isinstance(value, str):
                contexts[context] = value
            else:
                contexts[context] = TableDisplayOptions.from_dict(value)
        return cls(contexts=contexts)

    def set_context(
        self, context: str, options: TableDisplayOptions | str | None
    ) -> TableDisplayAnnotation:
        """Return a new annotation with an updated context."""
        new_contexts = dict(self.contexts)
        new_contexts[context] = options
        return TableDisplayAnnotation(contexts=new_contexts)


@dataclass
class PreFormat:
    """Pre-formatting options for column display.

    Attributes:
        format: Printf-style format string.
        bool_true_value: Display value for True.
        bool_false_value: Display value for False.
    """

    format: str | None = None
    bool_true_value: str | None = None
    bool_false_value: str | None = None

    def to_dict(self) -> dict[str, str]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, str] = {}
        if self.format is not None:
            result["format"] = self.format
        if self.bool_true_value is not None:
            result["bool_true_value"] = self.bool_true_value
        if self.bool_false_value is not None:
            result["bool_false_value"] = self.bool_false_value
        return result


@dataclass
class ColumnDisplayOptions:
    """Display options for a column in a specific context.

    Attributes:
        pre_format: Pre-formatting options for the column value.
        markdown_pattern: Markdown pattern for rendering the column.
        template_engine: Template engine to use (handlebars or mustache).
        column_order: Sort ordering for this column (False to disable).
    """

    pre_format: PreFormat | None = None
    markdown_pattern: str | None = None
    template_engine: TemplateEngine | None = None
    column_order: list[SortKey] | Literal[False] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.pre_format is not None:
            result["pre_format"] = self.pre_format.to_dict()
        if self.markdown_pattern is not None:
            result["markdown_pattern"] = self.markdown_pattern
        if self.template_engine is not None:
            result["template_engine"] = self.template_engine.value
        if self.column_order is not None:
            if self.column_order is False:
                result["column_order"] = False
            else:
                result["column_order"] = [s.to_dict() for s in self.column_order]
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ColumnDisplayOptions:
        """Create from dictionary representation."""
        pre_format = None
        if "pre_format" in d:
            pf = d["pre_format"]
            pre_format = PreFormat(
                format=pf.get("format"),
                bool_true_value=pf.get("bool_true_value"),
                bool_false_value=pf.get("bool_false_value"),
            )

        template_engine = None
        if "template_engine" in d:
            template_engine = TemplateEngine(d["template_engine"])

        column_order: list[SortKey] | Literal[False] | None = None
        if "column_order" in d:
            if d["column_order"] is False:
                column_order = False
            else:
                column_order = []
                for item in d["column_order"]:
                    if isinstance(item, str):
                        column_order.append(SortKey(column=item))
                    else:
                        column_order.append(SortKey(
                            column=item["column"],
                            descending=item.get("descending", False),
                        ))

        return cls(
            pre_format=pre_format,
            markdown_pattern=d.get("markdown_pattern"),
            template_engine=template_engine,
            column_order=column_order,
        )


@dataclass
class ColumnDisplayAnnotation:
    """Column display configuration for multiple contexts.

    This corresponds to the 'tag:isrd.isi.edu,2016:column-display' annotation.

    Attributes:
        contexts: Mapping of context names to display options.
    """

    contexts: dict[str, ColumnDisplayOptions | str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        for context, options in self.contexts.items():
            if isinstance(options, str):
                result[context] = options
            else:
                result[context] = options.to_dict()
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ColumnDisplayAnnotation:
        """Create from dictionary representation."""
        contexts: dict[str, ColumnDisplayOptions | str] = {}
        for context, value in d.items():
            if isinstance(value, str):
                contexts[context] = value
            else:
                contexts[context] = ColumnDisplayOptions.from_dict(value)
        return cls(contexts=contexts)


@dataclass
class PseudoColumn:
    """A pseudo-column definition for visible columns and source definitions.

    Pseudo-columns allow displaying derived or related data that isn't a direct
    column in the table.

    Attributes:
        source: Source entry (column or path).
        sourcekey: Reference to a source definition by key.
        entity: Whether this is an entity-mode column.
        aggregate: Aggregation function (min, max, cnt, cnt_d, array, array_d).
        self_link: Whether to show a self-link.
        markdown_name: Display name with markdown.
        comment: Comment or tooltip (False to hide).
        display: Nested display configuration.
    """

    source: SourceEntry | None = None
    sourcekey: str | None = None
    entity: bool | None = None
    aggregate: Literal["min", "max", "cnt", "cnt_d", "array", "array_d"] | None = None
    self_link: bool | None = None
    markdown_name: str | None = None
    comment: str | Literal[False] | None = None
    display: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.source is not None:
            result["source"] = self.source.to_dict()
        if self.sourcekey is not None:
            result["sourcekey"] = self.sourcekey
        if self.entity is not None:
            result["entity"] = self.entity
        if self.aggregate is not None:
            result["aggregate"] = self.aggregate
        if self.self_link is not None:
            result["self_link"] = self.self_link
        if self.markdown_name is not None:
            result["markdown_name"] = self.markdown_name
        if self.comment is not None:
            result["comment"] = self.comment
        if self.display is not None:
            result["display"] = self.display
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> PseudoColumn:
        """Create from dictionary representation."""
        source = None
        if "source" in d:
            source = SourceEntry.from_dict(d["source"])

        return cls(
            source=source,
            sourcekey=d.get("sourcekey"),
            entity=d.get("entity"),
            aggregate=d.get("aggregate"),
            self_link=d.get("self_link"),
            markdown_name=d.get("markdown_name"),
            comment=d.get("comment"),
            display=d.get("display"),
        )


# Type alias for visible column entries
VisibleColumnEntry = str | list[str] | PseudoColumn


@dataclass
class VisibleColumnsAnnotation:
    """Visible columns configuration for multiple contexts.

    This corresponds to the 'tag:isrd.isi.edu,2016:visible-columns' annotation.
    It controls which columns appear and in what order for different display contexts.

    Attributes:
        contexts: Mapping of context names to column lists.
        filter: Optional filter configuration.
    """

    contexts: dict[str, list[VisibleColumnEntry] | str] = field(default_factory=dict)
    filter: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        for context, columns in self.contexts.items():
            if isinstance(columns, str):
                result[context] = columns
            else:
                result[context] = [
                    col.to_dict() if isinstance(col, PseudoColumn) else col
                    for col in columns
                ]
        if self.filter is not None:
            result["filter"] = self.filter
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> VisibleColumnsAnnotation:
        """Create from dictionary representation."""
        contexts: dict[str, list[VisibleColumnEntry] | str] = {}
        for key, value in d.items():
            if key == "filter":
                continue
            if isinstance(value, str):
                contexts[key] = value
            else:
                columns: list[VisibleColumnEntry] = []
                for item in value:
                    if isinstance(item, str):
                        columns.append(item)
                    elif isinstance(item, list):
                        columns.append(item)
                    else:
                        columns.append(PseudoColumn.from_dict(item))
                contexts[key] = columns

        return cls(
            contexts=contexts,
            filter=d.get("filter"),
        )

    def set_context(
        self, context: str, columns: list[VisibleColumnEntry] | str
    ) -> VisibleColumnsAnnotation:
        """Return a new annotation with an updated context."""
        new_contexts = dict(self.contexts)
        new_contexts[context] = columns
        return VisibleColumnsAnnotation(contexts=new_contexts, filter=self.filter)


@dataclass
class VisibleForeignKeysAnnotation:
    """Visible foreign keys configuration for multiple contexts.

    This corresponds to the 'tag:isrd.isi.edu,2016:visible-foreign-keys' annotation.
    It controls which related tables appear and in what order.

    Attributes:
        contexts: Mapping of context names to foreign key lists.
    """

    contexts: dict[str, list[list[str] | dict[str, Any]] | str] = field(
        default_factory=dict
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        return dict(self.contexts)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> VisibleForeignKeysAnnotation:
        """Create from dictionary representation."""
        return cls(contexts=dict(d))

    def set_context(
        self, context: str, fkeys: list[list[str] | dict[str, Any]] | str
    ) -> VisibleForeignKeysAnnotation:
        """Return a new annotation with an updated context."""
        new_contexts = dict(self.contexts)
        new_contexts[context] = fkeys
        return VisibleForeignKeysAnnotation(contexts=new_contexts)


@dataclass
class AssetAnnotation:
    """Asset configuration for file upload columns.

    This corresponds to the 'tag:isrd.isi.edu,2017:asset' annotation.
    It configures how file assets are managed for a URL column.

    Attributes:
        url_pattern: URL pattern for asset storage location.
        browser_upload: Whether browser upload is disabled (only False is valid).
        filename_column: Column storing the filename.
        byte_count_column: Column storing the file size.
        md5: MD5 checksum column or True for auto-computation.
        sha256: SHA256 checksum column or True for auto-computation.
        filename_ext_filter: List of allowed filename extensions.
    """

    url_pattern: str | None = None
    browser_upload: Literal[False] | None = None
    filename_column: str | None = None
    byte_count_column: str | None = None
    md5: str | Literal[True] | None = None
    sha256: str | Literal[True] | None = None
    filename_ext_filter: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.url_pattern is not None:
            result["url_pattern"] = self.url_pattern
        if self.browser_upload is not None:
            result["browser_upload"] = self.browser_upload
        if self.filename_column is not None:
            result["filename_column"] = self.filename_column
        if self.byte_count_column is not None:
            result["byte_count_column"] = self.byte_count_column
        if self.md5 is not None:
            result["md5"] = self.md5
        if self.sha256 is not None:
            result["sha256"] = self.sha256
        if self.filename_ext_filter is not None:
            result["filename_ext_filter"] = self.filename_ext_filter
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AssetAnnotation:
        """Create from dictionary representation."""
        return cls(
            url_pattern=d.get("url_pattern"),
            browser_upload=d.get("browser_upload"),
            filename_column=d.get("filename_column"),
            byte_count_column=d.get("byte_count_column"),
            md5=d.get("md5"),
            sha256=d.get("sha256"),
            filename_ext_filter=d.get("filename_ext_filter"),
        )


@dataclass
class ForeignKeyAnnotation:
    """Foreign key display configuration.

    This corresponds to the 'tag:isrd.isi.edu,2016:foreign-key' annotation.
    It configures how foreign key relationships are displayed.

    Attributes:
        to_name: Display name when navigating to the referenced table.
        from_name: Display name when coming from the referenced table.
        domain_filter: ERMrest filter expression to limit choices.
        domain_filter_pattern: Template pattern for domain filter.
    """

    to_name: str | None = None
    from_name: str | None = None
    domain_filter: str | None = None
    domain_filter_pattern: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.to_name is not None:
            result["to_name"] = self.to_name
        if self.from_name is not None:
            result["from_name"] = self.from_name
        if self.domain_filter is not None:
            result["domain_filter"] = self.domain_filter
        if self.domain_filter_pattern is not None:
            result["domain_filter_pattern"] = self.domain_filter_pattern
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ForeignKeyAnnotation:
        """Create from dictionary representation."""
        return cls(
            to_name=d.get("to_name"),
            from_name=d.get("from_name"),
            domain_filter=d.get("domain_filter"),
            domain_filter_pattern=d.get("domain_filter_pattern"),
        )


@dataclass
class SourceDefinition:
    """A reusable source definition for pseudo-columns.

    Source definitions allow defining complex column paths once and
    referencing them by key throughout annotations.

    Attributes:
        source: The source entry (column or path).
        entity: Whether this is an entity-mode source.
        aggregate: Aggregation function.
        markdown_name: Display name with markdown.
        comment: Comment or tooltip.
        display: Nested display configuration.
    """

    source: SourceEntry
    entity: bool | None = None
    aggregate: Literal["min", "max", "cnt", "cnt_d", "array", "array_d"] | None = None
    markdown_name: str | None = None
    comment: str | Literal[False] | None = None
    display: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {"source": self.source.to_dict()}
        if self.entity is not None:
            result["entity"] = self.entity
        if self.aggregate is not None:
            result["aggregate"] = self.aggregate
        if self.markdown_name is not None:
            result["markdown_name"] = self.markdown_name
        if self.comment is not None:
            result["comment"] = self.comment
        if self.display is not None:
            result["display"] = self.display
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SourceDefinition:
        """Create from dictionary representation."""
        return cls(
            source=SourceEntry.from_dict(d["source"]),
            entity=d.get("entity"),
            aggregate=d.get("aggregate"),
            markdown_name=d.get("markdown_name"),
            comment=d.get("comment"),
            display=d.get("display"),
        )


@dataclass
class SourceDefinitionsAnnotation:
    """Source definitions configuration.

    This corresponds to the 'tag:isrd.isi.edu,2019:source-definitions' annotation.
    It defines reusable source paths that can be referenced by key.

    Attributes:
        columns: Columns to include (True for all, or list of names).
        fkeys: Foreign keys to include (True for all, or list of constraint names).
        sources: Mapping of source keys to source definitions.
    """

    columns: bool | list[str] | None = None
    fkeys: bool | list[list[str]] | None = None
    sources: dict[str, SourceDefinition] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.columns is not None:
            result["columns"] = self.columns
        if self.fkeys is not None:
            result["fkeys"] = self.fkeys
        if self.sources:
            result["sources"] = {k: v.to_dict() for k, v in self.sources.items()}
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SourceDefinitionsAnnotation:
        """Create from dictionary representation."""
        sources = {}
        if "sources" in d:
            for key, value in d["sources"].items():
                if key != "search-box":  # search-box has different structure
                    sources[key] = SourceDefinition.from_dict(value)

        return cls(
            columns=d.get("columns"),
            fkeys=d.get("fkeys"),
            sources=sources,
        )

    def add_source(self, key: str, source: SourceDefinition) -> SourceDefinitionsAnnotation:
        """Return a new annotation with an additional source."""
        new_sources = dict(self.sources)
        new_sources[key] = source
        return SourceDefinitionsAnnotation(
            columns=self.columns,
            fkeys=self.fkeys,
            sources=new_sources,
        )


@dataclass
class CitationAnnotation:
    """Citation configuration for tables.

    This corresponds to the 'tag:isrd.isi.edu,2018:citation' annotation.
    It configures how citations are generated for table rows.

    Attributes:
        template_engine: Template engine to use.
        journal_pattern: Pattern for journal name.
        author_pattern: Pattern for authors.
        title_pattern: Pattern for title.
        year_pattern: Pattern for year.
        url_pattern: Pattern for URL.
        id_pattern: Pattern for identifier.
        wait_for: List of columns to wait for before generating.
    """

    template_engine: TemplateEngine | None = None
    journal_pattern: str | None = None
    author_pattern: str | None = None
    title_pattern: str | None = None
    year_pattern: str | None = None
    url_pattern: str | None = None
    id_pattern: str | None = None
    wait_for: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format for ERMrest API."""
        result: dict[str, Any] = {}
        if self.template_engine is not None:
            result["template_engine"] = self.template_engine.value
        if self.journal_pattern is not None:
            result["journal_pattern"] = self.journal_pattern
        if self.author_pattern is not None:
            result["author_pattern"] = self.author_pattern
        if self.title_pattern is not None:
            result["title_pattern"] = self.title_pattern
        if self.year_pattern is not None:
            result["year_pattern"] = self.year_pattern
        if self.url_pattern is not None:
            result["url_pattern"] = self.url_pattern
        if self.id_pattern is not None:
            result["id_pattern"] = self.id_pattern
        if self.wait_for is not None:
            result["wait_for"] = self.wait_for
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CitationAnnotation:
        """Create from dictionary representation."""
        template_engine = None
        if "template_engine" in d:
            template_engine = TemplateEngine(d["template_engine"])

        return cls(
            template_engine=template_engine,
            journal_pattern=d.get("journal_pattern"),
            author_pattern=d.get("author_pattern"),
            title_pattern=d.get("title_pattern"),
            year_pattern=d.get("year_pattern"),
            url_pattern=d.get("url_pattern"),
            id_pattern=d.get("id_pattern"),
            wait_for=d.get("wait_for"),
        )


# Convenience functions for creating annotations


def display(
    name: str | None = None,
    markdown_name: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Create a display annotation tuple.

    Args:
        name: Display name for the resource.
        markdown_name: Display name with markdown formatting.

    Returns:
        A tuple of (tag URI, annotation dict) for use with annotations dict.
    """
    annotation = DisplayAnnotation(name=name, markdown_name=markdown_name)
    return (Tag.display, annotation.to_dict())


def row_name_pattern(pattern: str) -> tuple[str, dict[str, Any]]:
    """Create a table display annotation for row name pattern.

    Args:
        pattern: Handlebars template pattern for row names.

    Returns:
        A tuple of (tag URI, annotation dict).
    """
    annotation = TableDisplayAnnotation(
        contexts={"row_name": TableDisplayOptions(row_markdown_pattern=pattern)}
    )
    return (Tag.table_display, annotation.to_dict())


def visible_columns(
    *,
    compact: list[str] | None = None,
    detailed: list[str] | None = None,
    entry: list[str] | None = None,
    default: list[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Create a visible columns annotation.

    Args:
        compact: Columns for compact (table) view.
        detailed: Columns for detailed (record) view.
        entry: Columns for entry (edit/create) view.
        default: Default columns for all contexts.

    Returns:
        A tuple of (tag URI, annotation dict).
    """
    contexts: dict[str, list[str]] = {}
    if default is not None:
        contexts["*"] = default
    if compact is not None:
        contexts["compact"] = compact
    if detailed is not None:
        contexts["detailed"] = detailed
    if entry is not None:
        contexts["entry"] = entry

    annotation = VisibleColumnsAnnotation(contexts=contexts)
    return (Tag.visible_columns, annotation.to_dict())


def visible_foreign_keys(
    *,
    compact: list[list[str]] | None = None,
    detailed: list[list[str]] | None = None,
    default: list[list[str]] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Create a visible foreign keys annotation.

    Args:
        compact: Foreign keys for compact view.
        detailed: Foreign keys for detailed view.
        default: Default foreign keys for all contexts.

    Returns:
        A tuple of (tag URI, annotation dict).
    """
    contexts: dict[str, list[list[str]]] = {}
    if default is not None:
        contexts["*"] = default
    if compact is not None:
        contexts["compact"] = compact
    if detailed is not None:
        contexts["detailed"] = detailed

    annotation = VisibleForeignKeysAnnotation(contexts=contexts)
    return (Tag.visible_foreign_keys, annotation.to_dict())


def generated() -> tuple[str, None]:
    """Create a generated annotation.

    Marks a column as generated (not editable by users).

    Returns:
        A tuple of (tag URI, None).
    """
    return (Tag.generated, None)


def immutable() -> tuple[str, None]:
    """Create an immutable annotation.

    Marks a column as immutable (cannot be changed after creation).

    Returns:
        A tuple of (tag URI, None).
    """
    return (Tag.immutable, None)


def non_deletable() -> tuple[str, None]:
    """Create a non-deletable annotation.

    Marks a table or row as non-deletable.

    Returns:
        A tuple of (tag URI, None).
    """
    return (Tag.non_deletable, None)


def required() -> tuple[str, None]:
    """Create a required annotation.

    Marks a column as required in entry forms.

    Returns:
        A tuple of (tag URI, None).
    """
    return (Tag.required, None)
