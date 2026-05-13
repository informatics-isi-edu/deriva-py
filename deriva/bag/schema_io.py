"""Schema-interchange between ERMrest, SQLAlchemy, and typed defs.

This module owns the conversions between the four schema vocabularies
the bag pipeline touches:

- **ERMrest JSON**: the model-description format ERMrest emits and
  what ``data/schema.json`` carries inside a bag.
- **ERMrest** :class:`~deriva.core.ermrest_model.Model`: the Python
  object form a live catalog returns from ``getCatalogModel()``.
- **SQLAlchemy** :class:`~sqlalchemy.MetaData`: the internal canonical
  schema vocabulary across :mod:`deriva.bag` and deriva-ml's
  ``local_db``.
- **deriva-py** :class:`~deriva.core.typed.SchemaDef`: the typed
  definitions deriva-py users write to push schemas to catalogs.

``MetaData`` is the hub: every other form has a round-trip path
through it. Reasons:

- :class:`~deriva.bag.database.BagDatabase` returns a ``MetaData`` on
  bag open.
- :class:`~deriva.bag.schema.SchemaBuilder` produces a ``MetaData``
  from a ``Model``.
- ``local_db`` manipulates ``MetaData`` internally.
- :class:`~deriva.bag.builder.BagBuilder` (forthcoming) accepts
  ``typed.SchemaDef`` or ``MetaData``; normalizes to ``MetaData``.

Functions provided:

- :func:`metadata_to_ermrest_json` — write the ``data/schema.json``
  contents from a ``MetaData``. Used by :class:`BagBuilder` and by
  :class:`CatalogBagBuilder` (the catalog walker projects its source
  :class:`Model` to ``MetaData`` first so the two producers emit the
  same on-disk JSON byte for byte).
- :func:`ermrest_json_to_metadata` — parse a ``data/schema.json``
  back to a ``MetaData``. Used by :class:`BagDatabase` on bag open.
- :func:`ermrest_model_to_metadata` — convert a live catalog's
  :class:`Model` (or a slice of it) to a ``MetaData``. Used by
  :class:`CatalogBagBuilder`.
- :func:`typed_schema_def_to_metadata` — add deriva-py
  ``typed.SchemaDef`` definitions into an existing ``MetaData``.
  Used by :class:`BagBuilder` when callers supply ``typed`` input.
- :func:`metadata_to_typed_schema_defs` — inverse of the above.
  Useful when :class:`BagCatalogLoader` needs to create destination
  tables from a bag's schema.

The ERMrest ↔ SQLAlchemy type mapping lives here as a pair of
constants (:data:`ERMREST_TO_SQL` and :data:`SQL_TO_ERMREST`), the
single source of truth for the cross-vocabulary type story.
"""

from __future__ import annotations

import logging
from typing import Any

from deriva.core.ermrest_model import Model
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column as SQLColumn,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Table as SQLTable,
    UniqueConstraint as SQLUniqueConstraint,
)
from sqlalchemy.sql.type_api import TypeEngine

# Use the project's CSV-aware decorators so that anything routed
# through ``ermrest_json_to_metadata`` gets the same type-coercion
# behavior as :class:`~deriva.bag.database.BagDatabase` (which is
# itself built on top of the schema_io output indirectly).
#
# Since deriva-py PR #248 (column-construction dedup), the
# decorator classes + the ``ERMREST_TO_SQL`` map live in
# :mod:`deriva.bag._column_types` as the single source of truth.
# ``ERMREST_TO_SQL`` is re-exported here for back-compat — older
# code that did ``from deriva.bag.schema_io import ERMREST_TO_SQL``
# continues to work, and the symbol is part of this module's
# documented public surface.
from deriva.bag._column_types import (
    ERMREST_TO_SQL,
    ERMRestBoolean,
    StringToDate,
    StringToDateTime,
    StringToFloat,
    StringToInteger,
)

logger = logging.getLogger(__name__)


#: SQLAlchemy type class → ERMrest typename.
#:
#: Used to project a SQLAlchemy ``MetaData`` back to ERMrest's wire
#: format. The mapping is one-to-one for primitives but loses some
#: distinctions (e.g., ``Integer`` round-trips as ``int4``; a caller
#: who wants ``int8`` uses :class:`BigInteger` explicitly).
#:
#: The decorator classes (``ERMRestBoolean``, ``StringToFloat``, etc.)
#: are checked *before* their underlying type so a ``MetaData`` that
#: came from ``ermrest_json_to_metadata`` round-trips losslessly.
SQL_TO_ERMREST: list[tuple[type, str]] = [
    # Decorator classes first — these are subclasses of TypeDecorator
    # but isinstance() against the impl base would also match. Order
    # matters.
    (ERMRestBoolean, "boolean"),
    (StringToDate, "date"),
    (StringToDateTime, "timestamptz"),
    (StringToFloat, "float8"),
    (StringToInteger, "int4"),
    # Plain SQLAlchemy types.
    (Boolean, "boolean"),
    (Date, "date"),
    (DateTime, "timestamptz"),
    (Float, "float8"),
    (SmallInteger, "int2"),
    (BigInteger, "int8"),
    (Integer, "int4"),
    (JSON, "jsonb"),
    (LargeBinary, "bytea"),
    (String, "text"),
]


def sql_type_to_ermrest_name(sql_type: TypeEngine) -> str:
    """Return the ERMrest typename for a SQLAlchemy column type.

    Args:
        sql_type: A SQLAlchemy type instance (e.g. ``String()``,
            ``Integer()``).

    Returns:
        The ERMrest typename string (e.g. ``"text"``, ``"int4"``).

    The fallback for unknown types is ``"text"`` — preserves data as
    a CSV-roundtrippable string.

    Example:
        >>> from sqlalchemy import Integer, String, Boolean
        >>> sql_type_to_ermrest_name(String())
        'text'
        >>> sql_type_to_ermrest_name(Integer())
        'int4'
        >>> sql_type_to_ermrest_name(Boolean())
        'boolean'
    """
    for cls, ermrest_name in SQL_TO_ERMREST:
        if isinstance(sql_type, cls):
            return ermrest_name
    # Last resort: stringify and hope.
    return "text"


# =============================================================================
# ERMrest JSON ↔ MetaData
# =============================================================================


def ermrest_json_to_metadata(
    schema_dict: dict[str, Any],
    *,
    schemas: list[str] | None = None,
) -> MetaData:
    """Parse an ERMrest schema JSON document into a SQLAlchemy ``MetaData``.

    Mirrors what :func:`deriva.core.ermrest_model.Model.fromfile` does
    plus what :class:`~deriva.bag.schema.SchemaBuilder` does, but
    stays at the ``MetaData`` level — no engine, no automap.

    Args:
        schema_dict: The dict you get from
            ``json.load(open("data/schema.json"))``. The expected
            shape is ``{"snaptime": <iso>, "schemas": {<name>:
            {"tables": {<name>: {...}}}}}``.
        schemas: Optional list of schema names to include. If
            ``None``, every schema in the document is included.

    Returns:
        A SQLAlchemy :class:`MetaData` carrying every table from the
        selected schemas with columns, keys, and same-schema FKs
        wired up.

    Example:
        >>> doc = {
        ...     "snaptime": "2026-01-01T00:00:00",
        ...     "schemas": {
        ...         "demo": {
        ...             "schema_name": "demo",
        ...             "tables": {
        ...                 "T": {
        ...                     "schema_name": "demo",
        ...                     "table_name": "T",
        ...                     "column_definitions": [
        ...                         {"name": "RID", "type": {"typename": "text"},
        ...                          "nullok": False, "default": None,
        ...                          "comment": None},
        ...                     ],
        ...                     "keys": [{"names": [["demo", "T_RID_key"]],
        ...                               "unique_columns": ["RID"]}],
        ...                     "foreign_keys": [],
        ...                 },
        ...             },
        ...         },
        ...     },
        ... }
        >>> md = ermrest_json_to_metadata(doc)
        >>> sorted(md.tables.keys())
        ['demo.T']
    """
    metadata = MetaData()
    all_schemas = schema_dict.get("schemas", {})
    selected = list(all_schemas.keys()) if schemas is None else schemas

    # Stash document-level metadata that has no SQLAlchemy
    # equivalent (snaptime, per-schema annotations / ACLs) on
    # ``MetaData.info`` so the write path can emit it back. Each
    # selected ERMrest schema gets its own info bucket keyed by
    # schema name.
    metadata.info["snaptime"] = schema_dict.get("snaptime")
    schema_meta: dict[str, dict[str, Any]] = {}
    for schema_name in selected:
        if schema_name not in all_schemas:
            continue
        schema = all_schemas[schema_name]
        bucket: dict[str, Any] = {}
        for key in ("annotations", "acls", "acl_bindings", "comment"):
            if key in schema:
                bucket[key] = schema[key]
        if bucket:
            schema_meta[schema_name] = bucket
    if schema_meta:
        metadata.info["schemas"] = schema_meta

    # We build the FK-target map up front so columns can be born
    # with their ForeignKey constraints attached. Attaching FKs
    # post-hoc (via ``column.foreign_keys.add(...)``) doesn't bind
    # the FK to its parent column properly — SQLAlchemy emits
    # "this ForeignKey object does not yet have a parent Column
    # associated with it" when anything tries to resolve the FK's
    # ``.column`` attribute.
    fk_targets: dict[tuple[str, str, str], str] = {}
    for schema_name in selected:
        if schema_name not in all_schemas:
            continue
        schema = all_schemas[schema_name]
        for table_name, table_def in schema.get("tables", {}).items():
            for fk_def in table_def.get("foreign_keys", []):
                fk_cols = [
                    c["column_name"]
                    for c in fk_def.get("foreign_key_columns", [])
                ]
                ref_cols_def = fk_def.get("referenced_columns", [])
                if not fk_cols or not ref_cols_def:
                    continue
                ref_schema = ref_cols_def[0]["schema_name"]
                ref_table = ref_cols_def[0]["table_name"]
                if ref_schema not in selected:
                    continue
                for fk_col_name, ref_col in zip(fk_cols, ref_cols_def):
                    fk_targets[(schema_name, table_name, fk_col_name)] = (
                        f"{ref_schema}.{ref_table}.{ref_col['column_name']}"
                    )

    for schema_name in selected:
        if schema_name not in all_schemas:
            logger.warning(
                "schema_io: requested schema %r not in document; skipping",
                schema_name,
            )
            continue
        schema = all_schemas[schema_name]
        for table_name, table_def in schema.get("tables", {}).items():
            columns: list[SQLColumn] = []
            for col_def in table_def.get("column_definitions", []):
                ermrest_type = (
                    col_def.get("type", {}).get("typename", "text")
                )
                sql_type_cls = ERMREST_TO_SQL.get(ermrest_type, String)
                # ERMrest's ``nullok`` defaults to True if absent.
                nullok = col_def.get("nullok", True)
                # PK detection: only the RID column is treated as PK,
                # matching SchemaBuilder._is_key_column. Other unique
                # keys become UniqueConstraints below.
                is_pk = col_def["name"] == "RID"
                # The metadata produced here drives in-memory bag
                # staging (``BagBuilder``'s pending-rows writer
                # uses it). Non-PK columns are relaxed to nullable
                # so rows that *will* have server-set defaults at
                # the destination (``RCT``, ``RCB``, ``RMT``,
                # ``RMB``) can land in the bag without violating
                # the local mirror's NOT-NULL — same rule
                # ``SchemaBuilder._create_tables`` applies for the
                # on-disk SQLite mirror that backs already-built
                # bags. The destination ERMrest endpoint is the
                # authoritative validator at insert time.
                mirror_nullable = True if not is_pk else nullok
                col_args: list[Any] = []
                # If this column is the source of an FK, hand the
                # ForeignKey in as a positional argument so it's
                # bound to the column at construction time.
                target = fk_targets.get(
                    (schema_name, table_name, col_def["name"])
                )
                if target is not None:
                    col_args.append(ForeignKey(target))
                # The mirror's ``nullable`` is relaxed for staging
                # (see comment above), but the catalog's authoritative
                # ``nullok`` is preserved in ``info["nullok"]`` so
                # round-trips through :func:`metadata_to_ermrest_json`
                # carry the original constraint back out.
                # ``info`` carries metadata the SQLAlchemy column
                # type can't represent natively but which the
                # ERMrest JSON round-trip needs:
                # - ``nullok``: the catalog's authoritative
                #   not-null flag (separate from the relaxed
                #   ``nullable`` we use in the mirror; see
                #   :func:`metadata_to_ermrest_json` for the
                #   read-back logic).
                # - ``ermrest_typename``: the original ERMrest
                #   typename. Several ERMrest types
                #   (``int2``/``int4``/``int8`` → ``Integer``,
                #   ``timestamptz`` and ``timestamp`` →
                #   ``StringToDateTime``, etc.) map to one
                #   SQLAlchemy type, so a metadata → json
                #   round-trip without this stash would lose the
                #   distinction. Consumers like
                #   :meth:`Table.is_asset` exact-match the
                #   typename, so ``int8`` rounded down to ``int4``
                #   would mis-classify asset tables.
                # - ``annotations``, ``acls``, ``acl_bindings``:
                #   ERMrest-specific column-level metadata.
                #   Stashed here so consumers like
                #   :meth:`Table.is_asset` (which looks for
                #   ``tag.asset`` on the URL column) see them
                #   after a json → metadata → json round-trip.
                col_info: dict[str, Any] = {
                    "nullok": nullok,
                    "ermrest_typename": ermrest_type,
                }
                for key in ("annotations", "acls", "acl_bindings"):
                    if key in col_def:
                        col_info[key] = col_def[key]
                col = SQLColumn(
                    col_def["name"],
                    sql_type_cls(),
                    *col_args,
                    nullable=mirror_nullable,
                    primary_key=is_pk,
                    default=col_def.get("default"),
                    comment=col_def.get("comment"),
                    info=col_info,
                )
                columns.append(col)

            # Stash table-level annotations / ACLs on the SQLAlchemy
            # ``Table.info`` so the write path can emit them.
            table_info: dict[str, Any] = {}
            for key in ("annotations", "acls", "acl_bindings", "comment"):
                if key in table_def:
                    table_info[key] = table_def[key]
            sql_table = SQLTable(
                table_name,
                metadata,
                *columns,
                schema=schema_name,
                info=table_info,
            )

            # Non-RID unique constraints.
            for key_def in table_def.get("keys", []):
                key_cols = key_def.get("unique_columns", [])
                if key_cols == ["RID"]:
                    continue
                if not key_cols:
                    continue
                names = key_def.get("names", [])
                constraint_name = (
                    names[0][1] if names and len(names[0]) == 2 else None
                )
                sql_table.append_constraint(
                    SQLUniqueConstraint(*key_cols, name=constraint_name)
                )

    return metadata


def metadata_to_ermrest_json(metadata: MetaData) -> dict[str, Any]:
    """Project a SQLAlchemy ``MetaData`` to ERMrest schema-JSON form.

    Produces the dict that gets serialized to ``data/schema.json``
    inside a bag. The dict shape matches what ERMrest's ``/schema``
    endpoint returns and what
    :func:`deriva.core.ermrest_model.Model.fromfile` consumes.

    Args:
        metadata: A SQLAlchemy ``MetaData`` whose tables have a
            ``schema`` attribute set (so the function knows which
            ERMrest schema each table belongs to).

    Returns:
        A JSON-serializable dict.

    Annotations, ACLs, and comments round-trip when the metadata
    came from :func:`ermrest_json_to_metadata`: that function
    stashes those values on ``info`` (column-level on
    ``col.info``, table-level on ``Table.info``, document- and
    schema-level on ``MetaData.info``). Metadata built directly
    via SQLAlchemy without going through the reader emits an
    annotation-less, ACL-less JSON document — the same behavior
    as before this support was added.

    Limitations:
        - ``snaptime`` defaults to ``None`` unless the source went
          through :func:`ermrest_json_to_metadata` (which stashes
          the source's snaptime on ``metadata.info``). Constructive
          bags built directly via SQLAlchemy have no source
          snapshot.
    """
    # Bucket tables by schema name.
    schemas: dict[str, dict[str, Any]] = {}

    for sql_table in metadata.sorted_tables:
        schema_name = sql_table.schema or ""
        if not schema_name:
            # An unrooted table — fold it under the empty schema and
            # let the caller decide. Production paths always set
            # ``schema``.
            schema_name = ""
        schemas.setdefault(
            schema_name,
            {"schema_name": schema_name, "tables": {}},
        )
        column_definitions: list[dict[str, Any]] = []
        for col in sql_table.columns:
            # ``ermrest_json_to_metadata`` relaxes non-PK columns to
            # nullable in the mirror but preserves the catalog's
            # authoritative ``nullok`` in ``col.info``. Prefer the
            # stashed value so a round-trip through this function
            # carries the original constraint forward; fall back to
            # the SQLAlchemy ``nullable`` flag for callers that
            # never went through ``ermrest_json_to_metadata`` (in
            # which case the two values are the same anyway).
            if col.info and "nullok" in col.info:
                nullok = bool(col.info["nullok"])
            else:
                nullok = bool(col.nullable)
            # Prefer the stashed ERMrest typename so a round-trip
            # preserves the source's int2/int4/int8 distinction
            # (and similar). Fall back to the SQLAlchemy → ERMrest
            # mapping for callers that built the metadata
            # directly (no info stash).
            if col.info and "ermrest_typename" in col.info:
                typename = col.info["ermrest_typename"]
            else:
                typename = sql_type_to_ermrest_name(col.type)
            col_doc: dict[str, Any] = {
                "name": col.name,
                "type": {
                    "typename": typename,
                },
                "nullok": nullok,
                "default": _serialize_default(col),
                "comment": col.comment,
            }
            # Propagate ERMrest-specific metadata stashed by the
            # reader (annotations, ACLs, ACL bindings). Missing
            # keys mean the metadata didn't come through the
            # reader and there's nothing to emit.
            for key in ("annotations", "acls", "acl_bindings"):
                if col.info and key in col.info:
                    col_doc[key] = col.info[key]
            column_definitions.append(col_doc)

        keys: list[dict[str, Any]] = []
        # Primary key (typically RID).
        if sql_table.primary_key.columns:
            pk_cols = [c.name for c in sql_table.primary_key.columns]
            keys.append(
                {
                    "names": [[schema_name, f"{sql_table.name}_pkey"]],
                    "unique_columns": pk_cols,
                }
            )
        # Other unique constraints.
        for constraint in sql_table.constraints:
            if isinstance(constraint, SQLUniqueConstraint):
                key_cols = [c.name for c in constraint.columns]
                if not key_cols:
                    continue
                name = constraint.name or f"{sql_table.name}_{'_'.join(key_cols)}_key"
                keys.append(
                    {
                        "names": [[schema_name, name]],
                        "unique_columns": key_cols,
                    }
                )

        foreign_keys: list[dict[str, Any]] = []
        for col in sql_table.columns:
            for fk in col.foreign_keys:
                ref_table = fk.column.table
                ref_schema = ref_table.schema or ""
                foreign_keys.append(
                    {
                        "names": [
                            [
                                schema_name,
                                f"{sql_table.name}_{col.name}_fkey",
                            ]
                        ],
                        "foreign_key_columns": [
                            {
                                "schema_name": schema_name,
                                "table_name": sql_table.name,
                                "column_name": col.name,
                            }
                        ],
                        "referenced_columns": [
                            {
                                "schema_name": ref_schema,
                                "table_name": ref_table.name,
                                "column_name": fk.column.name,
                            }
                        ],
                    }
                )

        table_doc: dict[str, Any] = {
            "schema_name": schema_name,
            "table_name": sql_table.name,
            # ERMrest's Model parser expects ``kind`` to identify
            # whether the table is a real table or a view. Without
            # it, ForeignKey.digest_referenced_columns falls into a
            # broken branch that tries to write into
            # ``model._fkeys`` (an attribute that doesn't exist on
            # the Model class). Stamping ``"table"`` keeps the
            # round-trip safe.
            "kind": "table",
            "column_definitions": column_definitions,
            "keys": keys,
            "foreign_keys": foreign_keys,
        }
        # Propagate table-level ERMrest metadata stashed by the
        # reader. Missing keys mean the table didn't come through
        # the reader (or the source doc didn't carry them).
        for key in ("annotations", "acls", "acl_bindings", "comment"):
            if sql_table.info and key in sql_table.info:
                table_doc[key] = sql_table.info[key]
        schemas[schema_name]["tables"][sql_table.name] = table_doc

    # Stamp per-schema metadata (annotations, ACLs, comment) that the
    # reader stashed on ``metadata.info["schemas"][schema_name]``.
    schema_meta = (metadata.info or {}).get("schemas", {})
    for schema_name, bucket in schema_meta.items():
        if schema_name in schemas:
            for key, value in bucket.items():
                schemas[schema_name][key] = value

    return {
        # If the metadata went through ``ermrest_json_to_metadata``,
        # the source's snaptime is on ``metadata.info``; otherwise
        # None for constructive bags.
        "snaptime": (metadata.info or {}).get("snaptime"),
        "schemas": schemas,
    }


def _serialize_default(col: SQLColumn) -> Any:
    """Best-effort: turn a SQLAlchemy column default into JSON.

    SQLAlchemy supports many default forms (scalars, callables, SQL
    expressions). For ERMrest JSON we want a scalar or ``None``;
    anything more complex is dropped with a debug log.
    """
    if col.default is None:
        return None
    arg = getattr(col.default, "arg", None)
    if arg is None:
        return None
    if callable(arg):
        # Functions and Python callables don't round-trip; drop.
        return None
    return arg


# =============================================================================
# ERMrest Model → MetaData
# =============================================================================


def ermrest_model_to_metadata(
    model: Model,
    *,
    schemas: list[str] | None = None,
) -> MetaData:
    """Convert a live catalog's :class:`Model` to a SQLAlchemy ``MetaData``.

    Routes through :func:`metadata_to_ermrest_json`'s sibling form
    rather than re-implementing the column/key/FK walking. The
    function is essentially: serialize the model to ERMrest JSON
    (via ``model.prejson()``), then parse it back with
    :func:`ermrest_json_to_metadata`. The double-trip is cheap (the
    JSON document is the schema, not the data) and guarantees the
    same code path on the file-based and live-catalog sides.

    Args:
        model: Source ERMrest :class:`Model`.
        schemas: Optional list of schema names to include. ``None``
            includes every schema in the model.

    Returns:
        A SQLAlchemy ``MetaData``.

    Example:
        >>> from deriva.core import ErmrestCatalog  # doctest: +SKIP
        >>> catalog = ErmrestCatalog(...)  # doctest: +SKIP
        >>> model = catalog.getCatalogModel()  # doctest: +SKIP
        >>> md = ermrest_model_to_metadata(  # doctest: +SKIP
        ...     model, schemas=["deriva-ml"]
        ... )
    """
    # ``model.prejson()`` returns the schema dict in ERMrest's wire
    # format — exactly what ``ermrest_json_to_metadata`` consumes.
    return ermrest_json_to_metadata(model.prejson(), schemas=schemas)


# =============================================================================
# typed.SchemaDef ↔ MetaData
# =============================================================================
#
# The deriva.core.typed module is the canonical Python vocabulary for
# defining ERMrest schemas. Producers (catalog push, BagBuilder)
# accept ``SchemaDef`` lists; consumers don't typically need them.
# These two functions bridge to and from MetaData so the same
# SchemaDef can drive both a catalog-push and a bag-build.


def typed_schema_def_to_metadata(
    schema_defs: list[Any],
    metadata: MetaData | None = None,
) -> MetaData:
    """Add deriva-py typed ``SchemaDef`` definitions into a ``MetaData``.

    Args:
        schema_defs: A list of :class:`deriva.core.typed.SchemaDef`.
            Each ``SchemaDef`` has ``.schema_name`` and ``.tables``
            (a list of ``TableDef``); each ``TableDef`` has
            ``.table_name``, ``.column_definitions``, ``.keys``,
            and ``.foreign_keys``.
        metadata: Optional existing ``MetaData`` to mutate in
            place. When ``None``, a fresh one is created.

    Returns:
        The (possibly newly-created) ``MetaData``.

    Notes:
        - Implementation routes through ``prejson()``-like dict form
          so the same ``ermrest_json_to_metadata`` path is used. The
          ``typed`` module exposes ``prejson()`` methods on its def
          classes; we collect them into the same schema-document
          shape and reuse :func:`ermrest_json_to_metadata`.
        - This function is a placeholder pending the typed-module
          API check (the ``typed`` package is on the
          ``deriva-ml`` branch of deriva-py but the public surface
          for ``SchemaDef.prejson()`` is being finalized). The
          implementation here uses a reasonable best-effort form;
          adjustment may be needed once the typed API stabilizes.
    """
    if metadata is None:
        metadata = MetaData()

    # Build a single schema document with all schema defs merged in.
    document: dict[str, Any] = {"snaptime": None, "schemas": {}}
    for sd in schema_defs:
        # Each SchemaDef must surface its schema_name and a list of
        # TableDefs we can ``prejson()``.
        schema_name = getattr(sd, "schema_name", None) or getattr(sd, "name")
        tables: dict[str, Any] = {}
        for td in getattr(sd, "tables", []):
            t_name = getattr(td, "table_name", None) or getattr(td, "name")
            # ``prejson()`` on a TableDef returns the column-definitions
            # / keys / foreign_keys shape.
            try:
                tables[t_name] = td.prejson()
            except AttributeError:
                # Fall back to a hand-rolled projection: collect
                # column_definitions / keys / foreign_keys from the
                # attribute names typed defines.
                tables[t_name] = {
                    "schema_name": schema_name,
                    "table_name": t_name,
                    "column_definitions": [
                        c.prejson() if hasattr(c, "prejson") else dict(c)
                        for c in getattr(td, "column_definitions", [])
                    ],
                    "keys": [
                        k.prejson() if hasattr(k, "prejson") else dict(k)
                        for k in getattr(td, "keys", [])
                    ],
                    "foreign_keys": [
                        fk.prejson() if hasattr(fk, "prejson") else dict(fk)
                        for fk in getattr(td, "foreign_keys", [])
                    ],
                }
        document["schemas"][schema_name] = {
            "schema_name": schema_name,
            "tables": tables,
        }

    # Reuse the JSON path. The resulting MetaData is merged into the
    # caller's by copying tables across.
    parsed = ermrest_json_to_metadata(document)
    # Move tables into the target MetaData.
    for table in list(parsed.tables.values()):
        table.tometadata(metadata)
    return metadata


def metadata_to_typed_schema_defs(metadata: MetaData) -> list[dict[str, Any]]:
    """Project a ``MetaData`` to typed-style schema-def dicts.

    Returns plain dicts in the same shape as ``typed.SchemaDef.prejson()``
    output rather than constructed ``SchemaDef`` objects, so the
    function works regardless of the ``typed`` API state. Callers who
    want real ``SchemaDef`` objects can map across with
    ``SchemaDef.from_prejson(d)``.

    Args:
        metadata: SQLAlchemy ``MetaData``.

    Returns:
        A list of dicts; each dict is one schema and carries
        ``schema_name`` + ``tables`` keys.
    """
    return list(metadata_to_ermrest_json(metadata)["schemas"].values())
