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
    _metadata_to_typed_schema_defs,
    ermrest_json_to_metadata,
    metadata_to_ermrest_json,
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


def test_ermrest_json_to_metadata_relaxes_non_pk_not_null() -> None:
    """Non-PK columns with catalog ``nullok=False`` are nullable in the mirror.

    The mirror is staging — it holds rows the bag wants to ship to
    the destination catalog, not a fidelity copy of the catalog's
    constraints. Rows that *will* have server-set defaults at the
    destination (``RCT``, ``RCB``, ``RMT``, ``RMB``) need to land
    in the mirror without violating its local NOT-NULL. The
    destination's ERMrest endpoint is the authoritative validator
    at insert time.
    """
    doc = {
        "snaptime": "2026-01-01T00:00:00",
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "T": {
                        "schema_name": "demo",
                        "table_name": "T",
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
                                "name": "RCT",
                                "type": {"typename": "timestamptz"},
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
                                "names": [["demo", "T_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    }
                },
            }
        },
    }
    md = ermrest_json_to_metadata(doc)
    cols = {c.name: c for c in md.tables["demo.T"].columns}
    # PK NOT-NULL preserved (catches accidental row-construction
    # bugs at the mirror layer).
    assert not cols["RID"].nullable
    # Non-PK NOT-NULL relaxed — would have been a NOT NULL
    # ``RCT`` in the mirror without this rule.
    assert cols["RCT"].nullable
    assert cols["Name"].nullable
    # The catalog's authoritative nullok is preserved on
    # ``col.info`` so ``metadata_to_ermrest_json`` can round-trip
    # it back out.
    assert cols["RID"].info["nullok"] is False
    assert cols["RCT"].info["nullok"] is False
    assert cols["Name"].info["nullok"] is True


def test_ermrest_json_to_metadata_preserves_column_annotations() -> None:
    """Column-level ERMrest annotations land on ``col.info["annotations"]``.

    The bag's downstream consumers (e.g.
    :meth:`Table.is_asset`) rely on column annotations like
    ``tag.asset`` to recognize asset tables. Without preserving
    annotations through the json → metadata path, every
    round-tripped schema doc loses those tags and asset tables
    look like ordinary content tables.
    """
    asset_tag = "tag:isrd.isi.edu,2017:asset"
    doc = {
        "snaptime": "2026-01-01T00:00:00",
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
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
                                "name": "URL",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": "Asset URL",
                                "annotations": {asset_tag: {}},
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Image_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    }
                },
            }
        },
    }
    md = ermrest_json_to_metadata(doc)
    cols = {c.name: c for c in md.tables["demo.Image"].columns}
    # URL column carries the tag.asset annotation in its info.
    assert cols["URL"].info["annotations"] == {asset_tag: {}}
    # Column without an ``annotations`` key in the source has no
    # annotations in info either.
    assert "annotations" not in cols["RID"].info


def test_ermrest_json_to_metadata_roundtrip_preserves_int8() -> None:
    """``int8`` round-trips losslessly via the stashed ERMrest typename.

    Multiple ERMrest types map to one SQLAlchemy type
    (``int2``/``int4``/``int8`` all map to ``StringToInteger``),
    so a naive write path would round-trip them all as ``int4``.
    :meth:`Table.is_asset` exact-matches ``int8`` for the
    ``Length`` column — losing the distinction mis-classifies
    asset tables.
    """
    from deriva.bag.schema_io import metadata_to_ermrest_json

    doc = {
        "snaptime": "2026-01-01T00:00:00",
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "T": {
                        "schema_name": "demo",
                        "table_name": "T",
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
                                "name": "Length",
                                "type": {"typename": "int8"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Small",
                                "type": {"typename": "int2"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "T_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    }
                },
            }
        },
    }
    md = ermrest_json_to_metadata(doc)
    out = metadata_to_ermrest_json(md)
    out_cols = {
        c["name"]: c
        for c in out["schemas"]["demo"]["tables"]["T"]["column_definitions"]
    }
    assert out_cols["Length"]["type"]["typename"] == "int8"
    assert out_cols["Small"]["type"]["typename"] == "int2"


def test_ermrest_json_to_metadata_roundtrip_preserves_annotations() -> None:
    """A json → metadata → json round-trip preserves column annotations.

    The downstream model parser (``Model.fromfile``) needs the
    annotations to be present in the document to populate
    ``column.annotations``. Without this round-trip, asset
    columns in a constructed bag's schema.json wouldn't carry
    ``tag.asset`` and consumers would miscategorize the table.
    """
    from deriva.bag.schema_io import metadata_to_ermrest_json

    asset_tag = "tag:isrd.isi.edu,2017:asset"
    doc = {
        "snaptime": "2026-01-01T00:00:00",
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "annotations": {"tag:demo,2026:purpose": "test"},
                "tables": {
                    "Image": {
                        "schema_name": "demo",
                        "table_name": "Image",
                        "kind": "table",
                        "annotations": {"tag:demo,2026:role": "asset"},
                        "column_definitions": [
                            {
                                "name": "RID",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "URL",
                                "type": {"typename": "text"},
                                "nullok": False,
                                "default": None,
                                "comment": "Asset URL",
                                "annotations": {asset_tag: {}},
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Image_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    }
                },
            }
        },
    }
    md = ermrest_json_to_metadata(doc)
    out = metadata_to_ermrest_json(md)
    # snaptime preserved.
    assert out["snaptime"] == "2026-01-01T00:00:00"
    # Schema-level annotation preserved.
    assert out["schemas"]["demo"]["annotations"] == {
        "tag:demo,2026:purpose": "test"
    }
    # Table-level annotation preserved.
    out_image = out["schemas"]["demo"]["tables"]["Image"]
    assert out_image["annotations"] == {"tag:demo,2026:role": "asset"}
    # Column-level annotation preserved.
    out_cols = {c["name"]: c for c in out_image["column_definitions"]}
    assert out_cols["URL"]["annotations"] == {asset_tag: {}}
    # Column without source annotations doesn't get an empty dict.
    assert "annotations" not in out_cols["RID"]


def test_ermrest_json_to_metadata_roundtrip_preserves_nullok() -> None:
    """``metadata_to_ermrest_json`` reads ``nullok`` from ``col.info``.

    Without this, a round-trip
    ``ermrest_json_to_metadata`` → ``metadata_to_ermrest_json``
    would lose the catalog's authoritative ``nullok=False`` for
    non-PK NOT-NULL columns (since the mirror relaxes them to
    nullable). Reading from ``col.info["nullok"]`` instead of
    ``col.nullable`` keeps the round-trip honest.
    """
    from deriva.bag.schema_io import metadata_to_ermrest_json

    doc = {
        "snaptime": "2026-01-01T00:00:00",
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "T": {
                        "schema_name": "demo",
                        "table_name": "T",
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
                                "name": "RCT",
                                "type": {"typename": "timestamptz"},
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
                                "names": [["demo", "T_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [],
                    }
                },
            }
        },
    }
    md = ermrest_json_to_metadata(doc)
    round_tripped = metadata_to_ermrest_json(md)
    out_cols = {
        c["name"]: c
        for c in round_tripped["schemas"]["demo"]["tables"]["T"][
            "column_definitions"
        ]
    }
    # Round-trip preserves the catalog's original nullok flags,
    # even though the mirror relaxed non-PK NOT-NULL to nullable.
    assert out_cols["RID"]["nullok"] is False
    assert out_cols["RCT"]["nullok"] is False
    assert out_cols["Name"]["nullok"] is True


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
    """_metadata_to_typed_schema_defs returns one entry per schema."""
    md = ermrest_json_to_metadata(_two_table_doc())
    defs = _metadata_to_typed_schema_defs(md)
    assert isinstance(defs, list)
    schema_names = {d["schema_name"] for d in defs}
    assert "demo" in schema_names
