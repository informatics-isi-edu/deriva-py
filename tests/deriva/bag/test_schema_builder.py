"""Tests for :mod:`deriva.bag.schema` (SchemaBuilder + SchemaORM).

Covers Phase-1 ORM construction from an in-memory ERMrest Model:
table creation, key/FK propagation, automap class naming, and the
SchemaORM lookup helpers. Uses a minimal hand-built Model so the
tests don't need a live catalog.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from deriva.bag.schema import SchemaBuilder, SchemaORM
from deriva.bag.schema_io import ermrest_json_to_metadata
from deriva.core.ermrest_model import Model


def _build_model(snaptime: str = "2026-01-01T00:00:00") -> Model:
    """Build a small ERMrest Model with two FK-related tables."""
    doc = {
        "snaptime": snaptime,
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "Subject": {
                        "schema_name": "demo",
                        "table_name": "Subject",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Name",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Subject_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    },
                    "Image": {
                        "schema_name": "demo",
                        "table_name": "Image",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Filename",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Subject",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Image_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [
                            {
                                "names": [["demo", "Image_Subject_fkey"]],
                                "foreign_key_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Image",
                                        "column_name": "Subject",
                                    }
                                ],
                                "referenced_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Subject",
                                        "column_name": "RID",
                                    }
                                ],
                            }
                        ],
                    },
                },
            },
        },
    }
    # Write the doc to a temp file and load via Model.fromfile so we
    # exercise the same construction path real callers use.
    import json
    import tempfile

    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False
    ) as f:
        json.dump(doc, f)
        path = f.name
    return Model.fromfile("file-system", path)


def test_schemabuilder_creates_tables(tmp_path: Path) -> None:
    """File-based build produces both tables with the demo. prefix."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    orm = builder.build()
    try:
        names = orm.list_tables()
        assert "demo.Subject" in names
        assert "demo.Image" in names
    finally:
        orm.dispose()


def test_schemabuilder_in_memory_uses_underscore_separator() -> None:
    """In-memory mode folds schema into the table name with underscore."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=":memory:")
    orm = builder.build()
    try:
        names = orm.list_tables()
        # Underscore form rather than schema.table.
        assert "demo_Subject" in names
        assert "demo_Image" in names
        # The dotted form should not be present.
        assert "demo.Subject" not in names
    finally:
        orm.dispose()


def test_schemabuilder_propagates_fks(tmp_path: Path) -> None:
    """An FK on Image.Subject is wired into the SQLite table."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    orm = builder.build()
    try:
        image_tbl = orm.find_table("demo.Image")
        fk_cols = {
            col.name
            for col in image_tbl.columns
            if col.foreign_keys
        }
        assert "Subject" in fk_cols
    finally:
        orm.dispose()


def _build_cross_schema_model_with_hyphen(
    snaptime: str = "2026-01-01T00:00:00",
) -> Model:
    """Two schemas, one hyphenated, with a cross-schema FK.

    Topology::

        deriva-ml.Type ─── test-schema.Image.Type

    Both schema names contain a hyphen (matching deriva-ml's
    ``deriva-ml`` and the demo catalog's ``test-schema``) so the
    SchemaBuilder's name-folding pass has to apply
    ``replace("-", "_")`` consistently at both the table-creation
    site and the cross-schema FK lookup site.
    """
    doc = {
        "snaptime": snaptime,
        "schemas": {
            "deriva-ml": {
                "schema_name": "deriva-ml",
                "tables": {
                    "Type": {
                        "schema_name": "deriva-ml",
                        "table_name": "Type",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Name",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [
                                    ["deriva-ml", "Type_RID_key"]
                                ],
                                "unique_columns": ["RID"],
                            },
                            {
                                "names": [
                                    ["deriva-ml", "Type_Name_key"]
                                ],
                                "unique_columns": ["Name"],
                            },
                        ],
                        "foreign_keys": [],
                    },
                },
            },
            "test-schema": {
                "schema_name": "test-schema",
                "tables": {
                    "Image": {
                        "schema_name": "test-schema",
                        "table_name": "Image",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Type",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [
                                    ["test-schema", "Image_RID_key"]
                                ],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [
                            {
                                "names": [
                                    [
                                        "test-schema",
                                        "Image_Type_fkey",
                                    ]
                                ],
                                "foreign_key_columns": [
                                    {
                                        "schema_name": "test-schema",
                                        "table_name": "Image",
                                        "column_name": "Type",
                                    }
                                ],
                                "referenced_columns": [
                                    {
                                        "schema_name": "deriva-ml",
                                        "table_name": "Type",
                                        "column_name": "Name",
                                    }
                                ],
                            }
                        ],
                    },
                },
            },
        },
    }
    import json
    import tempfile

    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False
    ) as f:
        json.dump(doc, f)
        path = f.name
    return Model.fromfile("file-system", path)


def test_schemabuilder_in_memory_resolves_cross_schema_fk_with_hyphen() -> None:
    """In-memory build with hyphenated schemas wires the cross-schema FK.

    The build folds schema names into table names via underscore
    AND applies ``replace("-", "_")`` so SQLAlchemy gets valid
    identifiers. The cross-schema FK wiring step has to apply the
    same transform when looking up the source/target tables —
    otherwise lookup of ``test-schema_Image`` fails because the
    table was stored as ``test_schema_Image``.
    """
    model = _build_cross_schema_model_with_hyphen()
    # Build does not raise — the cross-schema FK wiring resolved
    # both endpoints.
    builder = SchemaBuilder(
        model,
        ["deriva-ml", "test-schema"],
        database_path=":memory:",
    )
    orm = builder.build()
    try:
        names = orm.list_tables()
        # Hyphenated names land as underscore-normalized in
        # in-memory mode.
        assert "test_schema_Image" in names
        assert "deriva_ml_Type" in names
    finally:
        orm.dispose()


def test_schemabuilder_uses_wal_engine(tmp_path: Path) -> None:
    """File-based builds use the WAL-pragma engine."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    orm = builder.build()
    try:
        with orm.engine.begin() as conn:
            # Need a write to switch to WAL.
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS __probe (k INTEGER)"
                )
            )
        with orm.engine.connect() as conn:
            mode = conn.execute(text("PRAGMA journal_mode")).scalar()
        assert mode == "wal"
    finally:
        orm.dispose()


def test_schemaorm_find_table_handles_bare_name(tmp_path: Path) -> None:
    """find_table accepts a bare name when there's no ambiguity."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    orm = builder.build()
    try:
        # Bare name should resolve to the demo. prefix.
        bare = orm.find_table("Subject")
        prefixed = orm.find_table("demo.Subject")
        assert bare is prefixed
    finally:
        orm.dispose()


def test_schemaorm_find_table_raises_on_unknown(tmp_path: Path) -> None:
    """Unknown tables raise KeyError, not silently return None."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    orm = builder.build()
    try:
        with pytest.raises(KeyError):
            orm.find_table("does_not_exist")
    finally:
        orm.dispose()


def test_schemaorm_get_orm_class_returns_mapper(tmp_path: Path) -> None:
    """The automap base produces an ORM class for each table."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    orm = builder.build()
    try:
        SubjectCls = orm.get_orm_class("Subject")
        assert SubjectCls is not None
        # The class name carries the schema-prefix-with-underscores
        # via SchemaBuilder.classname_for_table.
        assert "Subject" in SubjectCls.__name__
    finally:
        orm.dispose()


def test_schemaorm_dispose_is_idempotent(tmp_path: Path) -> None:
    """Calling dispose twice is fine."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    orm = builder.build()
    orm.dispose()
    orm.dispose()  # second call should be a no-op


def test_schemaorm_context_manager_disposes(tmp_path: Path) -> None:
    """Using SchemaORM as a context manager releases resources on exit."""
    model = _build_model()
    builder = SchemaBuilder(model, ["demo"], database_path=tmp_path)
    with builder.build() as orm:
        assert orm.engine is not None
    # After context exit, dispose has run; further use is undefined,
    # but the flag must be set.
    assert orm._disposed is True


def test_is_association_table_detects_simple_two_fk_table() -> None:
    """A two-FK pure association table is detected as such."""
    # Build a tiny schema with an association table.
    doc = {
        "snaptime": None,
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "A": {
                        "schema_name": "demo",
                        "table_name": "A",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            }
                        ],
                        "keys": [
                            {
                                "names": [["demo", "A_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    },
                    "B": {
                        "schema_name": "demo",
                        "table_name": "B",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            }
                        ],
                        "keys": [
                            {
                                "names": [["demo", "B_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    },
                    "A_B": {
                        "schema_name": "demo",
                        "table_name": "A_B",
                        "kind": "table",
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "A",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "B",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "A_B_RID_key"]],
                                "unique_columns": ["RID"],
                            },
                            {
                                "names": [["demo", "A_B_combo_key"]],
                                "unique_columns": ["A", "B"],
                            },
                        ],
                        "foreign_keys": [
                            {
                                "names": [["demo", "A_B_A_fkey"]],
                                "foreign_key_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "A_B",
                                        "column_name": "A",
                                    }
                                ],
                                "referenced_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "A",
                                        "column_name": "RID",
                                    }
                                ],
                            },
                            {
                                "names": [["demo", "A_B_B_fkey"]],
                                "foreign_key_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "A_B",
                                        "column_name": "B",
                                    }
                                ],
                                "referenced_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "B",
                                        "column_name": "RID",
                                    }
                                ],
                            },
                        ],
                    },
                },
            }
        },
    }
    import json
    import tempfile

    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False
    ) as f:
        json.dump(doc, f)
        path = f.name
    model = Model.fromfile("file-system", path)

    with SchemaBuilder(
        model, ["demo"], database_path=":memory:"
    ).build() as orm:
        AssocCls = orm.get_orm_class("A_B")
        result = SchemaORM.is_association_table(AssocCls)
        # Result is the arity (number of covered FKs).
        assert result == 2
