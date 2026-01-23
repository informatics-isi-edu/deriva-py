"""Handle wrappers for ERMrest model objects.

This module provides wrapper classes for ERMrest Table and Column objects
that offer enhanced usability while delegating to the underlying objects
for full API access.

Classes:
    ColumnHandle: Wrapper for ERMrest Column with simplified property access.
    TableHandle: Wrapper for ERMrest Table with simplified operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator

from deriva.core.ermrest_model import Table, Column, ForeignKey
from deriva.core.utils.core_utils import tag
from deriva.core.typed import ColumnDef, BuiltinType

if TYPE_CHECKING:
    from collections.abc import Iterable


class ColumnHandle:
    """Wrapper providing enhanced access to ERMrest Column.

    Delegates to underlying Column for all operations, adding:
    - Property-based access for description with auto-apply
    - Simplified annotation management
    - Type-safe operations

    All underlying Column attributes and methods are accessible via delegation.

    Attributes:
        name (str): Column name (delegated).
        nullok (bool): Whether NULL values are allowed (delegated).
        default (Any): Default value for the column (delegated).
        type (Type): ERMrest type object (delegated).
        table (Table): Parent table (delegated).
        annotations (dict): Raw annotations dict (delegated).

    Example:
        Get a column handle and inspect properties::

            >>> handle = TableHandle(table)
            >>> col = handle.get_column("Name")
            >>> print(col.name)           # "Name"
            >>> print(col.column_type)    # BuiltinType.text
            >>> print(col.is_system_column)  # False

        Modify column properties::

            >>> col.description = "Primary identifier"  # Auto-applies to catalog
            >>> col.set_display_name("Friendly Name")
            >>> col.set_nullok(False)

        Check and set annotations::

            >>> col.is_immutable = True
            >>> print(col.is_immutable)  # True

        Access delegated properties::

            >>> print(col.nullok)  # Delegated to underlying Column
            >>> print(col.default)
    """

    def __init__(self, column: Column):
        """Initialize a ColumnHandle.

        Args:
            column: The ERMrest Column object to wrap.
        """
        self._column = column

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying Column."""
        return getattr(self._column, name)

    def __dir__(self) -> list[str]:
        """Include both wrapper and delegated attributes for tab-completion."""
        return list(set(super().__dir__()) | set(dir(self._column)))

    def __repr__(self) -> str:
        return f"<ColumnHandle {self._column.table.schema.name}.{self._column.table.name}.{self._column.name}>"

    @property
    def description(self) -> str:
        """Column description/comment."""
        return self._column.comment or ""

    @description.setter
    def description(self, value: str) -> None:
        """Set the column description and apply to catalog.

        Args:
            value: New description for the column.
        """
        self._column.alter(comment=value)

    @property
    def column_type(self) -> BuiltinType:
        """Column type as BuiltinType enum.

        Returns:
            The column's data type.
        """
        return BuiltinType(self._column.type.typename)

    def set_type(self, new_type: BuiltinType, apply: bool = True) -> None:
        """Change the column's data type.

        Args:
            new_type: New type for the column.
            apply: Whether to apply changes immediately (default True).

        Note:
            Type changes may fail if existing data is incompatible.
        """
        # Use the enum directly since alter() accepts type names
        self._column.alter(type=new_type)

    def set_nullok(self, nullok: bool) -> None:
        """Set whether the column allows NULL values.

        Args:
            nullok: True to allow NULL, False to require values.
        """
        self._column.alter(nullok=nullok)

    def set_default(self, default: Any) -> None:
        """Set the column's default value.

        Args:
            default: Default value for new rows, or None to clear.
        """
        self._column.alter(default=default)

    def set_display_name(self, name: str) -> None:
        """Set the display name for the column in the UI.

        Args:
            name: Human-readable name to display.
        """
        if tag.display not in self._column.annotations:
            self._column.annotations[tag.display] = {}
        self._column.annotations[tag.display]["name"] = name
        self._column.apply()

    def get_display_name(self) -> str | None:
        """Get the display name for the column.

        Returns:
            The display name if set, otherwise None.
        """
        display = self._column.annotations.get(tag.display, {})
        return display.get("name")

    def set_column_display(self, markdown_pattern: str, context: str = "*") -> None:
        """Set how the column value is displayed using a markdown pattern.

        Args:
            markdown_pattern: Handlebars template for display (e.g., "[{{{Name}}}]({{{URL}}})")
            context: Display context ("*" for all, or specific like "compact", "detailed")
        """
        if tag.column_display not in self._column.annotations:
            self._column.annotations[tag.column_display] = {}
        self._column.annotations[tag.column_display][context] = {
            "markdown_pattern": markdown_pattern
        }
        self._column.apply()

    @property
    def is_system_column(self) -> bool:
        """Check if this is a system-managed column.

        Returns:
            True if column is RID, RCT, RMT, RCB, or RMB.
        """
        return self._column.name in {"RID", "RCT", "RMT", "RCB", "RMB"}

    @property
    def is_generated(self) -> bool:
        """Check if the column is marked as generated.

        Returns:
            True if the column has the generated annotation.
        """
        return tag.generated in self._column.annotations

    @is_generated.setter
    def is_generated(self, value: bool) -> None:
        """Set whether the column is marked as generated.

        Args:
            value: True to mark as generated, False to remove marking.
        """
        if value:
            self._column.annotations[tag.generated] = None
        else:
            self._column.annotations.pop(tag.generated, None)
        self._column.apply()

    @property
    def is_immutable(self) -> bool:
        """Check if the column is marked as immutable.

        Returns:
            True if the column has the immutable annotation.
        """
        return tag.immutable in self._column.annotations

    @is_immutable.setter
    def is_immutable(self, value: bool) -> None:
        """Set whether the column is marked as immutable.

        Args:
            value: True to mark as immutable, False to remove marking.
        """
        if value:
            self._column.annotations[tag.immutable] = None
        else:
            self._column.annotations.pop(tag.immutable, None)
        self._column.apply()


class TableHandle:
    """Wrapper providing enhanced access to ERMrest Table.

    Delegates to underlying Table for all operations, adding:
    - Property-based access for description with auto-apply
    - Simplified column operations returning ColumnHandle objects
    - Simplified annotation management
    - Common operation shortcuts with validation

    All underlying Table attributes and methods are accessible via delegation.

    Attributes:
        name (str): Table name (delegated).
        schema (Schema): Parent schema (delegated).
        columns (KeyedList): Raw column list (delegated).
        keys (KeyedList): Key constraints (delegated).
        foreign_keys (KeyedList): Foreign key constraints (delegated).
        referenced_by (KeyedList): Foreign keys referencing this table (delegated).
        annotations (dict): Raw annotations dict (delegated).

    Example:
        Create a handle and set basic properties::

            >>> from deriva.core import TableHandle
            >>> handle = TableHandle(ermrest_table)
            >>> handle.description = "My table description"  # Auto-applies
            >>> handle.set_display_name("Friendly Table Name")
            >>> handle.set_row_name_pattern("{{{Name}}}")

        Work with columns::

            >>> # Get a specific column
            >>> col = handle.get_column("Name")  # Returns ColumnHandle or None
            >>> col = handle.column("Name")       # Returns ColumnHandle or raises KeyError

            >>> # Iterate over user-defined columns (excludes system columns)
            >>> for col in handle.user_columns:
            ...     print(col.name, col.description)

            >>> # Add a new column
            >>> new_col = handle.add_column("Status", BuiltinType.text, comment="Item status")
            >>> new_col.set_display_name("Current Status")

        Configure visibility::

            >>> handle.set_visible_columns(["RID", "Name", "Description"])
            >>> handle.add_visible_column("Status")
            >>> handle.remove_visible_column("RID")

        Access delegated properties::

            >>> print(handle.name)         # Table name
            >>> print(handle.foreign_keys) # List of ForeignKey objects
            >>> handle.is_association()    # Delegated method call
    """

    def __init__(self, table: Table):
        """Initialize a TableHandle.

        Args:
            table: The ERMrest Table object to wrap.
        """
        self._table = table
        self._column_handles: dict[str, ColumnHandle] = {}

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying Table."""
        return getattr(self._table, name)

    def __dir__(self) -> list[str]:
        """Include both wrapper and delegated attributes for tab-completion."""
        return list(set(super().__dir__()) | set(dir(self._table)))

    def __repr__(self) -> str:
        return f"<TableHandle {self._table.schema.name}.{self._table.name}>"

    # -------------------------------------------------------------------------
    # Description / Comment
    # -------------------------------------------------------------------------

    @property
    def description(self) -> str:
        """Table description/comment."""
        return self._table.comment or ""

    @description.setter
    def description(self, value: str) -> None:
        """Set the table description and apply to catalog.

        Args:
            value: New description for the table.
        """
        self._table.alter(comment=value)

    # -------------------------------------------------------------------------
    # Column Access
    # -------------------------------------------------------------------------

    def _wrap_column(self, column: Column) -> ColumnHandle:
        """Get or create a ColumnHandle for the given column."""
        if column.name not in self._column_handles:
            self._column_handles[column.name] = ColumnHandle(column)
        return self._column_handles[column.name]

    def get_column(self, name: str) -> ColumnHandle | None:
        """Get a column by name, or None if not found.

        Args:
            name: Column name to look up.

        Returns:
            ColumnHandle for the column, or None if not found.
        """
        if name in self._table.columns.elements:
            return self._wrap_column(self._table.columns[name])
        return None

    def column(self, name: str) -> ColumnHandle:
        """Get a column by name, raising if not found.

        Args:
            name: Column name to look up.

        Returns:
            ColumnHandle for the column.

        Raises:
            KeyError: If column does not exist.
        """
        return self._wrap_column(self._table.columns[name])

    @property
    def all_columns(self) -> Iterator[ColumnHandle]:
        """Iterate over all columns as ColumnHandle objects.

        Yields:
            ColumnHandle for each column in the table.
        """
        for col in self._table.columns:
            yield self._wrap_column(col)

    @property
    def user_columns(self) -> list[ColumnHandle]:
        """Get non-system columns.

        Returns:
            List of ColumnHandle objects excluding RID, RCT, RMT, RCB, RMB.
        """
        system = {"RID", "RCT", "RMT", "RCB", "RMB"}
        return [self._wrap_column(c) for c in self._table.columns if c.name not in system]

    @property
    def required_columns(self) -> list[ColumnHandle]:
        """Get columns that require values (non-nullable without defaults).

        Returns:
            List of ColumnHandle objects for required columns.
        """
        return [
            self._wrap_column(c)
            for c in self._table.columns
            if not c.nullok and c.default is None and c.name not in {"RID", "RCT", "RMT", "RCB", "RMB"}
        ]

    @property
    def column_names(self) -> list[str]:
        """Get list of all column names.

        Returns:
            List of column names.
        """
        return [c.name for c in self._table.columns]

    # -------------------------------------------------------------------------
    # Column Creation
    # -------------------------------------------------------------------------

    def add_column(
        self,
        name: str,
        column_type: BuiltinType,
        nullok: bool = True,
        default: Any = None,
        comment: str | None = None,
    ) -> ColumnHandle:
        """Add a new column to the table.

        Args:
            name: Column name.
            column_type: Data type for the column.
            nullok: Whether NULL values are allowed (default True).
            default: Default value for new rows.
            comment: Description of the column.

        Returns:
            ColumnHandle for the newly created column.

        Raises:
            ValueError: If column already exists.
        """
        if name in self._table.columns.elements:
            raise ValueError(f"Column {name} already exists in table {self._table.name}")

        col_def = ColumnDef(
            name=name,
            type=column_type,
            nullok=nullok,
            default=default,
            comment=comment,
        )
        new_col = self._table.create_column(col_def)
        return self._wrap_column(new_col)

    def add_foreign_key_column(
        self,
        target_table: Table | TableHandle,
        column_name: str | None = None,
        nullok: bool = False,
    ) -> tuple[ColumnHandle, ForeignKey]:
        """Add a column with a foreign key reference to another table.

        Creates a column referencing the target table's preferred key
        (typically RID or Name) and the corresponding foreign key constraint.

        Args:
            target_table: Table to reference (Table or TableHandle).
            column_name: Name for the new column (default: target table name).
            nullok: Whether NULL values are allowed (default False).

        Returns:
            Tuple of (ColumnHandle for new column, ForeignKey constraint).
        """
        if isinstance(target_table, TableHandle):
            target_table = target_table._table

        if column_name is None:
            column_name = target_table.name

        cols, fkey = self._table.create_reference((column_name, nullok, target_table))
        # create_reference returns a list of columns (usually just one)
        return self._wrap_column(cols[0]), fkey

    # -------------------------------------------------------------------------
    # Display Annotations
    # -------------------------------------------------------------------------

    def set_display_name(self, name: str) -> None:
        """Set the display name for the table in the UI.

        Args:
            name: Human-readable name to display.
        """
        self._table.display["name"] = name
        self._table.apply()

    def get_display_name(self) -> str | None:
        """Get the display name for the table.

        Returns:
            The display name if set, otherwise None.
        """
        if tag.display in self._table.annotations:
            return self._table.annotations[tag.display].get("name")
        return None

    def set_row_name_pattern(self, pattern: str) -> None:
        """Set the pattern used to display row names.

        Args:
            pattern: Handlebars template for row names (e.g., "{{{Name}}}" or "{{{RID}}}").
        """
        self._table.table_display["row_name"] = {"row_markdown_pattern": pattern}
        self._table.apply()

    def get_row_name_pattern(self) -> str | None:
        """Get the row name pattern.

        Returns:
            The row name pattern if set, otherwise None.
        """
        if tag.table_display in self._table.annotations:
            row_name = self._table.annotations[tag.table_display].get("row_name", {})
            return row_name.get("row_markdown_pattern")
        return None

    # -------------------------------------------------------------------------
    # Visible Columns
    # -------------------------------------------------------------------------

    def set_visible_columns(
        self,
        columns: Iterable[str],
        context: str = "*",
    ) -> None:
        """Set which columns are visible in a display context.

        Args:
            columns: List of column names to display.
            context: Display context ("*" for all, or "compact", "detailed", "entry", "filter").
        """
        self._table.visible_columns[context] = list(columns)
        self._table.apply()

    def get_visible_columns(self, context: str = "*") -> list[str] | None:
        """Get the visible columns for a display context.

        Args:
            context: Display context to query.

        Returns:
            List of column names, or None if not configured.
        """
        if tag.visible_columns in self._table.annotations:
            return self._table.annotations[tag.visible_columns].get(context)
        return None

    def add_visible_column(self, column: str, context: str = "*") -> None:
        """Add a column to the visible columns list.

        Args:
            column: Column name to add.
            context: Display context to modify.
        """
        current = self.get_visible_columns(context) or []
        if column not in current:
            current.append(column)
            self.set_visible_columns(current, context)

    def remove_visible_column(self, column: str, context: str = "*") -> None:
        """Remove a column from the visible columns list.

        Args:
            column: Column name to remove.
            context: Display context to modify.
        """
        current = self.get_visible_columns(context) or []
        if column in current:
            current.remove(column)
            self.set_visible_columns(current, context)

    # -------------------------------------------------------------------------
    # Visible Foreign Keys
    # -------------------------------------------------------------------------

    def set_visible_foreign_keys(
        self,
        fkeys: Iterable[tuple[str, str]],
        context: str = "*",
    ) -> None:
        """Set which foreign keys are visible in a display context.

        Args:
            fkeys: List of (schema_name, constraint_name) tuples.
            context: Display context ("*" for all, or "compact", "detailed").
        """
        self._table.visible_foreign_keys[context] = [list(fk) for fk in fkeys]
        self._table.apply()

    def get_visible_foreign_keys(self, context: str = "*") -> list[list[str]] | None:
        """Get the visible foreign keys for a display context.

        Args:
            context: Display context to query.

        Returns:
            List of [schema_name, constraint_name] pairs, or None if not configured.
        """
        if tag.visible_foreign_keys in self._table.annotations:
            return self._table.annotations[tag.visible_foreign_keys].get(context)
        return None

    # -------------------------------------------------------------------------
    # Presence Annotations
    # -------------------------------------------------------------------------

    @property
    def is_generated(self) -> bool:
        """Check if the table is marked as generated."""
        return tag.generated in self._table.annotations

    @is_generated.setter
    def is_generated(self, value: bool) -> None:
        """Set whether the table is marked as generated."""
        if value:
            self._table.annotations[tag.generated] = None
        else:
            self._table.annotations.pop(tag.generated, None)
        self._table.apply()

    @property
    def is_immutable(self) -> bool:
        """Check if the table is marked as immutable."""
        return tag.immutable in self._table.annotations

    @is_immutable.setter
    def is_immutable(self, value: bool) -> None:
        """Set whether the table is marked as immutable."""
        if value:
            self._table.annotations[tag.immutable] = None
        else:
            self._table.annotations.pop(tag.immutable, None)
        self._table.apply()

    @property
    def is_non_deletable(self) -> bool:
        """Check if the table is marked as non-deletable."""
        return tag.non_deletable in self._table.annotations

    @is_non_deletable.setter
    def is_non_deletable(self, value: bool) -> None:
        """Set whether the table is marked as non-deletable."""
        if value:
            self._table.annotations[tag.non_deletable] = None
        else:
            self._table.annotations.pop(tag.non_deletable, None)
        self._table.apply()

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def has_column(self, name: str) -> bool:
        """Check if a column exists in the table.

        Args:
            name: Column name to check.

        Returns:
            True if column exists.
        """
        return name in self._table.columns.elements

    def refresh(self) -> None:
        """Refresh the table from the catalog.

        This clears cached column handles and reloads from the server.
        """
        self._column_handles.clear()
        # The underlying table's apply() or a model refresh would update it
