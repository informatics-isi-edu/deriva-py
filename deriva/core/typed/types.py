"""Type enumerations for ERMrest model definitions.

This module provides typed enumerations for ERMrest types, constraint behaviors,
and ACL modes. These replace the string-based constants used in the native API.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deriva.core.ermrest_model import Type


class BuiltinType(str, Enum):
    """ERMrest built-in column data types.

    These correspond to the types available in `deriva.core.ermrest_model.builtin_types`.

    Scalar Types:
        - text: Variable-length character string
        - int2: 2-byte signed integer (-32768 to 32767)
        - int4: 4-byte signed integer (-2147483648 to 2147483647)
        - int8: 8-byte signed integer
        - float4: 4-byte floating point
        - float8: 8-byte floating point (double precision)
        - boolean: True/false value
        - date: Calendar date (year, month, day)
        - timestamp: Date and time without timezone
        - timestamptz: Date and time with timezone
        - json: JSON data stored as text
        - jsonb: JSON data stored in binary format

    Domain Types:
        - markdown: Text with markdown formatting
        - longtext: Long text content
        - ermrest_rid: Record ID (text-based)
        - ermrest_rcb: Record created by (text-based)
        - ermrest_rmb: Record modified by (text-based)
        - ermrest_rct: Record creation time (timestamptz-based)
        - ermrest_rmt: Record modification time (timestamptz-based)
        - ermrest_curie: Compact URI for vocabulary terms
        - ermrest_uri: Full URI for vocabulary terms
        - color_rgb_hex: RGB color in hex format

    Array Types:
        - text_array: Array of text values
        - int4_array: Array of int4 values
        - float8_array: Array of float8 values
        - (and other scalar types with _array suffix)
    """

    # Scalar types
    text = "text"
    int2 = "int2"
    int4 = "int4"
    int8 = "int8"
    float4 = "float4"
    float8 = "float8"
    boolean = "boolean"
    date = "date"
    timestamp = "timestamp"
    timestamptz = "timestamptz"
    json = "json"
    jsonb = "jsonb"

    # Domain types
    markdown = "markdown"
    longtext = "longtext"
    ermrest_rid = "ermrest_rid"
    ermrest_rcb = "ermrest_rcb"
    ermrest_rmb = "ermrest_rmb"
    ermrest_rct = "ermrest_rct"
    ermrest_rmt = "ermrest_rmt"
    ermrest_curie = "ermrest_curie"
    ermrest_uri = "ermrest_uri"
    color_rgb_hex = "color_rgb_hex"

    # Serial types
    serial2 = "serial2"
    serial4 = "serial4"
    serial8 = "serial8"

    # Array types (common ones)
    text_array = "text[]"
    int2_array = "int2[]"
    int4_array = "int4[]"
    int8_array = "int8[]"
    float4_array = "float4[]"
    float8_array = "float8[]"
    boolean_array = "boolean[]"
    date_array = "date[]"
    timestamp_array = "timestamp[]"
    timestamptz_array = "timestamptz[]"
    json_array = "json[]"
    jsonb_array = "jsonb[]"

    def to_ermrest_type(self) -> "Type":
        """Convert to the native ERMrest Type object.

        Returns:
            The corresponding Type instance from builtin_types.

        Raises:
            KeyError: If the type name is not found in builtin_types.
        """
        from deriva.core.ermrest_model import builtin_types
        return builtin_types[self.value]

    @classmethod
    def from_typename(cls, typename: str) -> "BuiltinType":
        """Create a BuiltinType from a type name string.

        Args:
            typename: The ERMrest type name string.

        Returns:
            The corresponding BuiltinType enum member.

        Raises:
            ValueError: If the typename doesn't match any enum member.
        """
        for member in cls:
            if member.value == typename:
                return member
        raise ValueError(f"Unknown type name: {typename}")


class OnAction(str, Enum):
    """Foreign key constraint actions for ON UPDATE and ON DELETE.

    These specify what happens to referencing rows when the referenced
    primary key is updated or deleted.

    Values:
        NO_ACTION: Raise an error if referenced rows exist (default)
        RESTRICT: Same as NO_ACTION, but checked immediately
        CASCADE: Automatically update/delete referencing rows
        SET_NULL: Set referencing columns to NULL
        SET_DEFAULT: Set referencing columns to their default values
    """

    NO_ACTION = "NO ACTION"
    RESTRICT = "RESTRICT"
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"


class AclMode(str, Enum):
    """ACL access mode types for ERMrest resources.

    These define the types of access that can be controlled via ACLs.

    Values:
        owner: Full ownership rights (includes all other modes)
        enumerate: Permission to see that a resource exists
        select: Permission to read data
        insert: Permission to insert new data
        update: Permission to modify existing data
        delete: Permission to delete data
        write: Combined insert, update, and delete permissions
    """

    owner = "owner"
    enumerate = "enumerate"
    select = "select"
    insert = "insert"
    update = "update"
    delete = "delete"
    write = "write"


class DisplayContext(str, Enum):
    """UI display contexts for annotations.

    These define the different contexts in which data can be displayed,
    allowing different configurations for each context.

    Values:
        all: Default/fallback context (represented as "*")
        compact: Compact inline display
        compact_brief: Very compact display for related entities
        compact_select: Compact display in selection dropdowns
        detailed: Full detailed record view
        entry: Data entry/editing form
        entry_create: Entry form for new records
        entry_edit: Entry form for existing records
        filter: Filter/search interface
        row_name: Row name/title display
        export: Data export context
    """

    all = "*"
    compact = "compact"
    compact_brief = "compact/brief"
    compact_select = "compact/select"
    detailed = "detailed"
    entry = "entry"
    entry_create = "entry/create"
    entry_edit = "entry/edit"
    filter = "filter"
    row_name = "row_name"
    export = "export"


class TemplateEngine(str, Enum):
    """Template engine types for display annotations.

    Values:
        handlebars: Handlebars templating engine (default)
        mustache: Mustache templating engine
    """

    handlebars = "handlebars"
    mustache = "mustache"
