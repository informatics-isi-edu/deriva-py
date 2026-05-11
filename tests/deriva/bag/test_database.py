"""Tests for :mod:`deriva.bag.database`.

Covers BagDatabase end-to-end against synthetic bag directories:
schema loading, CSV ingest, ORM access, asset URL localization via
fetch.txt, attached-schema visibility, and the new WAL + schema_meta
behavior introduced when the module moved into deriva.bag.

These tests build their bags by writing files directly (no producer
class exists yet at this commit). Once :class:`~deriva.bag.builder.BagBuilder`
lands the fixtures could be migrated to use it, but the direct-write
approach has the side benefit of pinning the bag profile's on-disk
contract.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from deriva.bag.database import BagDatabase
from deriva.bag.profile import BAG_SCHEMA_VERSION
from deriva.bag.sqlite_helpers import SCHEMA_META_TABLE, SchemaVersionError


# ---------------------------------------------------------------------------
# Bag fixtures
# ---------------------------------------------------------------------------


def _minimal_schema(snaptime: str = "2026-01-01T00:00:00") -> dict[str, Any]:
    """Return a minimal ERMrest model JSON with one domain schema + one table.

    The shape mirrors what an ERMrest ``/schema`` endpoint returns and
    what the deriva-bag profile demands at ``data/schema.json``.
    """
    return {
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
                    }
                },
            }
        },
    }


@pytest.fixture
def minimal_bag(tmp_path: Path) -> Path:
    """Build a minimal one-table bag and return the bag-directory path.

    Layout:
      tmp_path/cache_key/bag/data/schema.json
      tmp_path/cache_key/bag/data/demo/Subject.csv
    """
    # BagDatabase uses bag_path.parent.name as the cache key, so we
    # wrap the bag inside a parent directory whose name is the key.
    cache_key = "demo_abc123"
    bag = tmp_path / cache_key / "bag"
    (bag / "data" / "demo").mkdir(parents=True)
    (bag / "data" / "schema.json").write_text(json.dumps(_minimal_schema()))

    with (bag / "data" / "demo" / "Subject.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Name", "Age"])
        w.writerow(["S1", "Alice", "30"])
        w.writerow(["S2", "Bob", ""])
        w.writerow(["S3", "Carol", "42"])

    return bag


# ---------------------------------------------------------------------------
# Basic open / load
# ---------------------------------------------------------------------------


def test_bag_database_loads_schema(minimal_bag: Path, tmp_path: Path) -> None:
    """BagDatabase opens a bag and exposes its tables."""
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        tables = db.list_tables()
        assert "demo.Subject" in tables


def test_bag_database_loads_rows(minimal_bag: Path, tmp_path: Path) -> None:
    """BagDatabase ingests the CSV rows into SQLite."""
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        rows = list(db.get_table_contents("Subject"))
    names = sorted(r["Name"] for r in rows)
    assert names == ["Alice", "Bob", "Carol"]


def test_bag_database_converts_types(minimal_bag: Path, tmp_path: Path) -> None:
    """ERMrest int4 column comes back as int, not string."""
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        ages = {r["Name"]: r["Age"] for r in db.get_table_contents("Subject")}
    assert ages["Alice"] == 30
    assert ages["Carol"] == 42
    # Bob has an empty Age field; should come back as None, not "".
    assert ages["Bob"] is None


def test_bag_database_orm_class_query(minimal_bag: Path, tmp_path: Path) -> None:
    """The automap-built ORM class is queryable via SQLAlchemy."""
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        SubjectCls = db.get_orm_class_by_name("Subject")
        assert SubjectCls is not None
        with db.engine.connect() as conn:
            row = conn.execute(
                text('SELECT RID, Name FROM "demo".Subject WHERE Name = :n'),
                {"n": "Alice"},
            ).first()
    assert row is not None
    assert row[0] == "S1"


def test_bag_database_snaptime_captured(
    minimal_bag: Path, tmp_path: Path
) -> None:
    """The schema's snaptime is exposed on the BagDatabase instance."""
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        assert db.snaptime == "2026-01-01T00:00:00"


# ---------------------------------------------------------------------------
# WAL + pragma integration
# ---------------------------------------------------------------------------


def test_bag_database_uses_wal_mode(
    minimal_bag: Path, tmp_path: Path
) -> None:
    """BagDatabase's SQLite engine runs in WAL mode (regression guard).

    Pre-redesign, BagDatabase used the default SQLAlchemy engine
    (journal_mode=delete). After the move into deriva.bag the engine
    must use the WAL factory so concurrent readers don't block
    writers and vice versa.
    """
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        with db.engine.connect() as conn:
            mode = conn.execute(text("PRAGMA journal_mode")).scalar()
    assert mode == "wal"


def test_bag_database_enables_foreign_keys(
    minimal_bag: Path, tmp_path: Path
) -> None:
    """foreign_keys=ON is set on every BagDatabase connection."""
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        with db.engine.connect() as conn:
            fk = conn.execute(text("PRAGMA foreign_keys")).scalar()
    assert fk == 1


def test_bag_database_records_schema_version(
    minimal_bag: Path, tmp_path: Path
) -> None:
    """ensure_schema_meta is invoked; the version row is present."""
    db_dir = tmp_path / "db"
    with BagDatabase(minimal_bag, db_dir, ["demo"]) as db:
        with db.engine.connect() as conn:
            v = conn.execute(
                text(f"SELECT version FROM {SCHEMA_META_TABLE}")
            ).scalar()
    assert v == BAG_SCHEMA_VERSION


def test_bag_database_rejects_newer_schema_version(
    minimal_bag: Path, tmp_path: Path
) -> None:
    """Opening a cache written by a future deriva.bag raises clearly.

    Prepares a cache with a higher schema_meta version than the code
    knows about; the reopen must raise SchemaVersionError instead of
    silently proceeding (and potentially mis-reading a layout it
    doesn't understand).
    """
    db_dir = tmp_path / "db"
    # First open: writes BAG_SCHEMA_VERSION.
    with BagDatabase(minimal_bag, db_dir, ["demo"]):
        pass

    # Reach into the cache and bump the version to one above ours.
    bag_cache_dir = minimal_bag.parent.name
    cache_root = db_dir / bag_cache_dir
    from deriva.bag.sqlite_helpers import create_wal_engine

    engine = create_wal_engine(cache_root / "main.db")
    try:
        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {SCHEMA_META_TABLE}"),
            )
            conn.execute(
                text(
                    f"INSERT INTO {SCHEMA_META_TABLE}(version) VALUES (:v)"
                ),
                {"v": BAG_SCHEMA_VERSION + 1},
            )
    finally:
        engine.dispose()

    # Re-opening BagDatabase against the bumped cache must raise.
    # Note that BagDatabase re-creates schema/data on every open
    # (it doesn't trust the cache content), but ensure_schema_meta
    # runs early enough to abort before tables are touched.
    with pytest.raises(SchemaVersionError):
        BagDatabase(minimal_bag, db_dir, ["demo"]).dispose()


# ---------------------------------------------------------------------------
# Asset / fetch.txt integration
# ---------------------------------------------------------------------------


def _asset_schema(snaptime: str = "2026-01-01T00:00:00") -> dict[str, Any]:
    """Schema with a single asset table (Filename/URL/Length/MD5/Description)."""
    return {
        "snaptime": snaptime,
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
                                "name": "Filename",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "URL",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Length",
                                "type": {"typename": "int8"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "MD5",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Description",
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
                        "foreign_keys": [],
                    }
                },
            }
        },
    }


@pytest.mark.xfail(
    reason=(
        "Pre-existing bug in BagDatabase._localize_asset_row: the fetch-map "
        "is keyed by urlparse(url).path (just the path portion of the URL), "
        "but the lookup compares against the full URL from the row. This "
        "test pins the intended behavior so the bug can't regress further; "
        "the fix is out-of-scope for the move commit and will land in a "
        "follow-up. Once fixed, remove the xfail."
    ),
    strict=True,
)
def test_bag_database_localizes_asset_urls(tmp_path: Path) -> None:
    """When fetch.txt maps a URL to a local file, the row's Filename column
    gets rewritten to the local path on load."""
    cache_key = "asset_xyz"
    bag = tmp_path / cache_key / "bag"
    (bag / "data" / "demo").mkdir(parents=True)
    (bag / "data" / "schema.json").write_text(json.dumps(_asset_schema()))

    # An asset table CSV pointing at a remote URL.
    with (bag / "data" / "demo" / "Image.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Filename", "URL", "Length", "MD5", "Description"])
        w.writerow(
            [
                "I1",
                "remote.png",
                "https://example.com/hatrac/img.png",
                "1024",
                "abc123",
                "img",
            ]
        )

    # fetch.txt maps the URL (path component) to a local file inside
    # the bag. BagDatabase uses urlparse(url).path as the key.
    local_path = "data/asset/Image/I1/img.png"
    (bag / "data" / "asset" / "Image" / "I1").mkdir(parents=True)
    (bag / local_path).write_bytes(b"\x89PNG\r\n\x1a\n...")
    (bag / "fetch.txt").write_text(
        f"https://example.com/hatrac/img.png\t1024\t{local_path}\n"
    )

    db_dir = tmp_path / "db"
    with BagDatabase(bag, db_dir, ["demo"]) as db:
        rows = list(db.get_table_contents("Image"))
    assert len(rows) == 1
    # Filename should now point at the localized path inside the bag.
    assert rows[0]["Filename"] == f"{bag}/{local_path}"
