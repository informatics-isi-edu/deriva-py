"""Tests for :mod:`deriva.bag.schema_io` conversions."""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
)

from deriva.bag.schema_io import (
    ERMREST_TO_SQL,
    ermrest_json_to_metadata,
    metadata_to_ermrest_json,
    metadata_to_typed_schema_defs,
    sql_type_to_ermrest_name,
)


def _two_table_doc() -> dict[str, Any]:
    """A two-table ERMrest schema document for round-trip testing."""
    return {
        "snaptime": "2026-01-01T00:00:00",
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
                                "name": "Age",
                                "type": {"typename": "int4"},
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
            }
        },
    }


# ---------------------------------------------------------------------------
# Type-mapping
# ---------------------------------------------------------------------------


def test_ermrest_to_sql_covers_common_types() -> None:
    """Every common ERMrest type has a SQL mapping."""
    for ermrest in ("text", "int4", "int8", "float8", "boolean", "timestamptz"):
        assert ermrest in ERMREST_TO_SQL


def test_sql_type_to_ermrest_name_handles_primitives() -> None:
    """Bare SQLAlchemy types project to sensible ERMrest names."""
    assert sql_type_to_ermrest_name(String()) == "text"
    assert sql_type_to_ermrest_name(Integer()) == "int4"
    assert sql_type_to_ermrest_name(Boolean()) == "boolean"
    assert sql_type_to_ermrest_name(Float()) == "float8"
    assert sql_type_to_ermrest_name(DateTime()) == "timestamptz"


# ---------------------------------------------------------------------------
# ERMrest JSON → MetaData
# ---------------------------------------------------------------------------


def test_ermrest_json_to_metadata_creates_tables() -> None:
    """Parsing a two-table doc produces both tables in MetaData."""
    md = ermrest_json_to_metadata(_two_table_doc())
    assert "demo.Subject" in md.tables
    assert "demo.Image" in md.tables


def test_ermrest_json_to_metadata_marks_rid_primary_key() -> None:
    """The RID column is marked primary_key on every table."""
    md = ermrest_json_to_metadata(_two_table_doc())
    subj = md.tables["demo.Subject"]
    rid_cols = [c for c in subj.columns if c.primary_key]
    assert len(rid_cols) == 1
    assert rid_cols[0].name == "RID"


def test_ermrest_json_to_metadata_wires_fks() -> None:
    """FKs on Image.Subject land on the MetaData column."""
    md = ermrest_json_to_metadata(_two_table_doc())
    image = md.tables["demo.Image"]
    subject_col = image.c["Subject"]
    assert len(subject_col.foreign_keys) == 1
    fk = next(iter(subject_col.foreign_keys))
    assert fk.target_fullname == "demo.Subject.RID"


def test_ermrest_json_to_metadata_filters_schemas() -> None:
    """Passing ``schemas=[]`` skips every schema in the document."""
    md = ermrest_json_to_metadata(_two_table_doc(), schemas=[])
    assert len(md.tables) == 0


# ---------------------------------------------------------------------------
# MetaData → ERMrest JSON
# ---------------------------------------------------------------------------


def test_metadata_to_ermrest_json_returns_schema_doc() -> None:
    """A hand-built MetaData projects to the expected shape."""
    md = MetaData()
    Table(
        "Subject",
        md,
        Column("RID", String, primary_key=True),
        Column("Name", String, nullable=True),
        schema="demo",
    )
    doc = metadata_to_ermrest_json(md)
    assert "demo" in doc["schemas"]
    assert "Subject" in doc["schemas"]["demo"]["tables"]
    cols = doc["schemas"]["demo"]["tables"]["Subject"]["column_definitions"]
    names = {c["name"] for c in cols}
    assert names == {"RID", "Name"}


def test_metadata_to_ermrest_json_records_pk() -> None:
    """The primary key appears in the table's ``keys`` list."""
    md = MetaData()
    Table(
        "T",
        md,
        Column("RID", String, primary_key=True),
        schema="demo",
    )
    doc = metadata_to_ermrest_json(md)
    keys = doc["schemas"]["demo"]["tables"]["T"]["keys"]
    assert len(keys) >= 1
    assert any(k["unique_columns"] == ["RID"] for k in keys)


def test_metadata_to_ermrest_json_records_fks() -> None:
    """FKs survive the projection."""
    md = MetaData()
    Table(
        "Subject",
        md,
        Column("RID", String, primary_key=True),
        schema="demo",
    )
    Table(
        "Image",
        md,
        Column("RID", String, primary_key=True),
        Column("Subject", String, ForeignKey("demo.Subject.RID")),
        schema="demo",
    )
    doc = metadata_to_ermrest_json(md)
    fks = doc["schemas"]["demo"]["tables"]["Image"]["foreign_keys"]
    assert len(fks) == 1
    fk = fks[0]
    assert fk["foreign_key_columns"][0]["column_name"] == "Subject"
    assert fk["referenced_columns"][0]["table_name"] == "Subject"


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def test_round_trip_preserves_tables() -> None:
    """ERMrest JSON → MetaData → ERMrest JSON keeps every table."""
    original = _two_table_doc()
    md = ermrest_json_to_metadata(original)
    projected = metadata_to_ermrest_json(md)
    original_tables = set(original["schemas"]["demo"]["tables"].keys())
    projected_tables = set(projected["schemas"]["demo"]["tables"].keys())
    assert original_tables == projected_tables


def test_round_trip_preserves_fks() -> None:
    """ERMrest JSON → MetaData → ERMrest JSON keeps every FK."""
    md = ermrest_json_to_metadata(_two_table_doc())
    projected = metadata_to_ermrest_json(md)
    image_fks = projected["schemas"]["demo"]["tables"]["Image"]["foreign_keys"]
    assert len(image_fks) == 1


def test_metadata_to_typed_schema_defs_returns_list() -> None:
    """metadata_to_typed_schema_defs returns one entry per schema."""
    md = ermrest_json_to_metadata(_two_table_doc())
    defs = metadata_to_typed_schema_defs(md)
    assert isinstance(defs, list)
    schema_names = {d["schema_name"] for d in defs}
    assert "demo" in schema_names
