"""Tests for :mod:`deriva.bag.catalog_loader`.

Like :mod:`test_catalog_builder`, the live-catalog write-path is
out of scope for unit tests (covered by integration tests on a
real ERMrest server). The unit tests here focus on:

- Bag-state inference (holey vs. materialized) from fetch.txt.
- Policy validation that fails fast on holey+UPLOAD_*.
- Dangling-FK strategy applied to a row set (FAIL/DELETE/NULLIFY).
- Report shape and aggregates.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from deriva.bag.catalog_loader import (
    BagCatalogLoader,
    LoadReport,
    TableLoadStats,
)
from deriva.bag.traversal import (
    AssetMode,
    DanglingFKStrategy,
    FKTraversalPolicy,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_minimal_bag(tmp_path: Path) -> Path:
    """Build a one-table bag and return the bag directory."""
    cache_key = "load_xyz"
    bag = tmp_path / cache_key / "bag"
    (bag / "data" / "demo").mkdir(parents=True)
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
    (bag / "data" / "schema.json").write_text(json.dumps(doc))
    with (bag / "data" / "demo" / "T.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Name"])
        w.writerow(["1", "Alice"])
    return bag


def _mock_catalog(catalog_id: str = "42") -> MagicMock:
    catalog = MagicMock()
    catalog.catalog_id = catalog_id
    return catalog


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


def test_infer_schemas_skips_system_schemas(tmp_path: Path) -> None:
    """``public`` / ``WWW`` / ``_acl_admin`` are filtered from the result.

    The catalog builder's export engine snapshots the *entire*
    source-catalog model into ``schema.json``, including ERMrest's
    structural schemas that carry no user data. Loading them
    upstream of automap produces classes with no usable primary
    key, which crashes the cross-schema FK loop in
    ``BagDatabase._create_tables``. The loader must drop them at
    the inference step so the database layer never sees them.
    """
    bag = _write_minimal_bag(tmp_path)
    schema_file = bag / "data" / "schema.json"
    doc = json.loads(schema_file.read_text())
    # Inject the structural schemas the export engine would carry.
    for sys_schema in ("public", "WWW", "_acl_admin"):
        doc["schemas"][sys_schema] = {
            "schema_name": sys_schema,
            "tables": {},
        }
    schema_file.write_text(json.dumps(doc))

    schemas = BagCatalogLoader._infer_schemas_from_bag(bag)
    assert "demo" in schemas
    assert "public" not in schemas
    assert "WWW" not in schemas
    assert "_acl_admin" not in schemas


# ---------------------------------------------------------------------------
# Bag-state inference
# ---------------------------------------------------------------------------


def test_loader_detects_materialized_bag(tmp_path: Path) -> None:
    """A bag with no fetch.txt is reported as materialized."""
    bag = _write_minimal_bag(tmp_path)
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        database_dir=db_dir,
    )
    try:
        assert loader.holey is False
    finally:
        loader.dispose()


def test_loader_detects_holey_bag(tmp_path: Path) -> None:
    """A bag whose fetch.txt references a missing file is holey."""
    bag = _write_minimal_bag(tmp_path)
    # Reference a file that doesn't exist on disk.
    (bag / "fetch.txt").write_text(
        "https://example.com/img.png\t1024\tdata/asset/T/1/img.png\n"
    )
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=db_dir,
    )
    try:
        assert loader.holey is True
    finally:
        loader.dispose()


def test_loader_detects_materialized_bag_with_present_fetch(
    tmp_path: Path,
) -> None:
    """A bag whose fetch.txt files are all present is materialized."""
    bag = _write_minimal_bag(tmp_path)
    asset_dir = bag / "data" / "asset" / "T" / "1"
    asset_dir.mkdir(parents=True)
    (asset_dir / "img.png").write_bytes(b"PNG")
    (bag / "fetch.txt").write_text(
        "https://example.com/img.png\t3\tdata/asset/T/1/img.png\n"
    )
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        database_dir=db_dir,
    )
    try:
        assert loader.holey is False
    finally:
        loader.dispose()


# ---------------------------------------------------------------------------
# Bag-state × asset-mode validation
# ---------------------------------------------------------------------------


def test_loader_rejects_holey_with_upload_if_missing(
    tmp_path: Path,
) -> None:
    """Holey bag + UPLOAD_IF_MISSING raises clearly before any work."""
    bag = _write_minimal_bag(tmp_path)
    (bag / "fetch.txt").write_text(
        "https://example.com/img\t1024\tdata/asset/T/1/img\n"
    )
    db_dir = tmp_path / "db"
    with pytest.raises(ValueError, match="materialize"):
        BagCatalogLoader(
            catalog=_mock_catalog(),
            bag=bag,
            policy=FKTraversalPolicy(
                asset_mode=AssetMode.UPLOAD_IF_MISSING
            ),
            database_dir=db_dir,
        )


def test_loader_rejects_holey_with_upload_force(tmp_path: Path) -> None:
    bag = _write_minimal_bag(tmp_path)
    (bag / "fetch.txt").write_text(
        "https://example.com/img\t1024\tdata/asset/T/1/img\n"
    )
    db_dir = tmp_path / "db"
    with pytest.raises(ValueError, match="materialize"):
        BagCatalogLoader(
            catalog=_mock_catalog(),
            bag=bag,
            policy=FKTraversalPolicy(
                asset_mode=AssetMode.UPLOAD_FORCE
            ),
            database_dir=db_dir,
        )


def test_loader_accepts_holey_with_rows_only(tmp_path: Path) -> None:
    """Holey + ROWS_ONLY is fine (no bytes required at load time)."""
    bag = _write_minimal_bag(tmp_path)
    (bag / "fetch.txt").write_text(
        "https://example.com/img\t1024\tdata/asset/T/1/img\n"
    )
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=db_dir,
    )
    try:
        assert loader.holey is True
    finally:
        loader.dispose()


# ---------------------------------------------------------------------------
# Dangling-FK strategy
# ---------------------------------------------------------------------------


def _build_fk_bag(tmp_path: Path) -> Path:
    """Bag with Image → Subject FK and one Image row whose parent is missing."""
    cache_key = "fk_demo"
    bag = tmp_path / cache_key / "bag"
    (bag / "data" / "demo").mkdir(parents=True)
    doc = {
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
                            }
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
                                "names": [
                                    ["demo", "Image_Subject_fkey"]
                                ],
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
    (bag / "data" / "schema.json").write_text(json.dumps(doc))
    with (bag / "data" / "demo" / "Subject.csv").open(
        "w", newline=""
    ) as f:
        w = csv.writer(f)
        w.writerow(["RID"])
        w.writerow(["S1"])  # only S1 exists; S2 is "missing"
    # NOTE: the bag's CSV only carries the *valid* row so the
    # BagDatabase SQLite ingest succeeds. The dangling-FK tests
    # below call _apply_dangling_fk_strategy directly with a
    # hand-built row list that includes the violating row;
    # exercising the strategy logic doesn't require the bag
    # itself to be FK-violating.
    with (bag / "data" / "demo" / "Image.csv").open(
        "w", newline=""
    ) as f:
        w = csv.writer(f)
        w.writerow(["RID", "Subject"])
        w.writerow(["I1", "S1"])  # valid
    return bag


def test_dangling_fk_strategy_fail_raises(tmp_path: Path) -> None:
    bag = _build_fk_bag(tmp_path)
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            dangling_fk_strategy=DanglingFKStrategy.FAIL,
        ),
        database_dir=db_dir,
    )
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        with pytest.raises(ValueError, match="Dangling FK"):
            loader._apply_dangling_fk_strategy(
                image,
                [
                    {"RID": "I1", "Subject": "S1"},
                    {"RID": "I2", "Subject": "S2"},
                ],
            )
    finally:
        loader.dispose()


def test_dangling_fk_strategy_delete_drops_row(tmp_path: Path) -> None:
    bag = _build_fk_bag(tmp_path)
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            dangling_fk_strategy=DanglingFKStrategy.DELETE,
        ),
        database_dir=db_dir,
    )
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        survivors, skipped, nullified = (
            loader._apply_dangling_fk_strategy(
                image,
                [
                    {"RID": "I1", "Subject": "S1"},
                    {"RID": "I2", "Subject": "S2"},
                ],
            )
        )
        assert skipped == 1
        assert nullified == 0
        assert len(survivors) == 1
        assert survivors[0]["RID"] == "I1"
    finally:
        loader.dispose()


def test_dangling_fk_strategy_nullify_keeps_row(tmp_path: Path) -> None:
    bag = _build_fk_bag(tmp_path)
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            dangling_fk_strategy=DanglingFKStrategy.NULLIFY,
        ),
        database_dir=db_dir,
    )
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        survivors, skipped, nullified = (
            loader._apply_dangling_fk_strategy(
                image,
                [
                    {"RID": "I1", "Subject": "S1"},
                    {"RID": "I2", "Subject": "S2"},
                ],
            )
        )
        assert skipped == 0
        assert nullified == 1
        assert len(survivors) == 2
        # The dangling row's FK column is now None.
        i2 = [r for r in survivors if r["RID"] == "I2"][0]
        assert i2["Subject"] is None
    finally:
        loader.dispose()


# ---------------------------------------------------------------------------
# Report shape
# ---------------------------------------------------------------------------


def test_load_report_aggregates() -> None:
    report = LoadReport(
        bag_path=Path("/tmp/bag"),
        catalog_id="42",
        table_stats={
            "demo.A": TableLoadStats(
                table="demo.A", rows_inserted=5
            ),
            "demo.B": TableLoadStats(
                table="demo.B",
                rows_inserted=3,
                rows_skipped_orphan=1,
                rows_nullified_orphan=2,
            ),
        },
    )
    assert report.total_rows_inserted == 8
    assert report.total_orphans_handled == 3


def test_table_load_stats_defaults() -> None:
    s = TableLoadStats(table="demo.T")
    assert s.rows_inserted == 0
    assert s.rows_skipped_orphan == 0
    assert s.rows_nullified_orphan == 0
    assert s.assets_uploaded == 0
    assert s.assets_deduped == 0


# ---------------------------------------------------------------------------
# PostgreSQL array-literal coercion
# ---------------------------------------------------------------------------


def test_coerce_pg_array_empty() -> None:
    """``{}`` is the wire form for an empty array."""
    assert BagCatalogLoader._coerce_pg_array("{}") == []


def test_coerce_pg_array_strings() -> None:
    """``{a,b,c}`` becomes a list of strings."""
    assert BagCatalogLoader._coerce_pg_array("{a,b,c}") == [
        "a",
        "b",
        "c",
    ]


def test_coerce_pg_array_quoted() -> None:
    """Quoted elements have their wrapping quotes stripped."""
    assert BagCatalogLoader._coerce_pg_array('{"a","b"}') == [
        "a",
        "b",
    ]


def test_coerce_pg_array_passthrough_non_string() -> None:
    """Non-string values (already-decoded lists, ``None``) pass through."""
    assert BagCatalogLoader._coerce_pg_array(None) is None
    assert BagCatalogLoader._coerce_pg_array([1, 2]) == [1, 2]


def test_coerce_pg_array_passthrough_plain_string() -> None:
    """A plain text value that isn't braced is returned as-is.

    Necessary because the column-coercion loop is keyed on the
    column's array-ness in the schema, not on the value shape; a
    non-array column passes its value through unchanged.
    """
    assert BagCatalogLoader._coerce_pg_array("plain") == "plain"


# ---------------------------------------------------------------------------
# Conflict policy: vocabulary match-by-name + content conflict
# (deriva-py#214 / ADR-0001)
# ---------------------------------------------------------------------------


def _build_vocab_bag(tmp_path: Path) -> Path:
    """Build a bag with one vocabulary table + one content table.

    Shapes:

    * ``demo.Color`` — a vocabulary table (canonical vocab columns).
    * ``demo.Widget`` — a content table with a single-column FK to
      ``demo.Color`` so the RID-remap propagation can be exercised.
    """
    cache_key = "vocab_bag"
    bag = tmp_path / cache_key / "bag"
    (bag / "data" / "demo").mkdir(parents=True)

    doc = {
        "snaptime": "2026-01-01T00:00:00",
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "Color": {
                        "schema_name": "demo",
                        "table_name": "Color",
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
                                "name": "ID",
                                "type": {"typename": "ermrest_curie"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "URI",
                                "type": {"typename": "ermrest_uri"},
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
                            {
                                "name": "Description",
                                "type": {"typename": "markdown"},
                                "nullok": False,
                                "default": None,
                                "comment": None,
                            },
                            {
                                "name": "Synonyms",
                                "type": {
                                    "typename": "text[]",
                                    "is_array": True,
                                    "base_type": {"typename": "text"},
                                },
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Color_RID_key"]],
                                "unique_columns": ["RID"],
                            },
                            {
                                "names": [["demo", "Color_Name_key"]],
                                "unique_columns": ["Name"],
                            },
                        ],
                        "foreign_keys": [],
                    },
                    "Widget": {
                        "schema_name": "demo",
                        "table_name": "Widget",
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
                                "name": "Color",
                                "type": {"typename": "text"},
                                "nullok": True,
                                "default": None,
                                "comment": None,
                            },
                        ],
                        "keys": [
                            {
                                "names": [["demo", "Widget_RID_key"]],
                                "unique_columns": ["RID"],
                            }
                        ],
                        "foreign_keys": [
                            {
                                "names": [
                                    ["demo", "Widget_Color_fkey"]
                                ],
                                "foreign_key_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Widget",
                                        "column_name": "Color",
                                    }
                                ],
                                "referenced_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Color",
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
    (bag / "data" / "schema.json").write_text(json.dumps(doc))
    with (bag / "data" / "demo" / "Color.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["RID", "ID", "URI", "Name", "Description", "Synonyms"]
        )
        w.writerow(
            ["C-SRC-RED", "demo:1", "/id/1", "Red", "Red color", "{}"]
        )
        w.writerow(
            ["C-SRC-BLUE", "demo:2", "/id/2", "Blue", "Blue color", "{}"]
        )
    with (bag / "data" / "demo" / "Widget.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Color"])
        w.writerow(["W1", "C-SRC-RED"])
        w.writerow(["W2", "C-SRC-BLUE"])
    return bag


def test_classify_table_detects_vocabulary(tmp_path: Path) -> None:
    """A vocab-shaped table is reported as ``VOCABULARY``; others as ``CONTENT``."""
    from deriva.bag.catalog_loader import _TableClass

    bag = _build_vocab_bag(tmp_path)
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        database_dir=tmp_path / "db",
    )
    try:
        color = loader.bag_db.model.schemas["demo"].tables["Color"]
        widget = loader.bag_db.model.schemas["demo"].tables["Widget"]
        assert loader._classify_table(color) == _TableClass.VOCABULARY
        assert loader._classify_table(widget) == _TableClass.CONTENT
    finally:
        loader.dispose()


def test_vocab_load_matches_by_name_and_records_remap(tmp_path: Path) -> None:
    """Existing destination rows match by ``Name``; RID remap is recorded."""
    bag = _build_vocab_bag(tmp_path)

    # Mock catalog: destination already has a "Red" row at a *different* RID.
    catalog = _mock_catalog()

    def _get(path: str, **_: Any):
        assert "Color" in path  # only the vocab fetch is expected here
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = [
            {"Name": "Red", "RID": "C-DST-RED"},
            # Blue is absent on the destination — must be inserted.
        ]
        return resp

    insert_payloads: list[list[dict[str, Any]]] = []

    def _post(path: str, **kwargs: Any):
        insert_payloads.append(kwargs["json"])
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        return resp

    catalog.get = _get
    catalog.post = _post

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )
    try:
        report = loader.run()
    finally:
        loader.dispose()

    color_stats = report.table_stats["demo.Color"]
    assert color_stats.rows_matched_by_name == 1  # "Red" matched
    assert color_stats.rows_inserted == 1  # "Blue" inserted

    # Remap has Red → destination RID, Blue → identity.
    remap = loader._rid_remap[("demo", "Color")]
    assert remap["C-SRC-RED"] == "C-DST-RED"
    assert remap["C-SRC-BLUE"] == "C-SRC-BLUE"

    # Widget rows were POSTed with their Color FK rewritten:
    # W1 → Color=C-DST-RED (remapped), W2 → Color=C-SRC-BLUE (identity).
    widget_inserts = [
        p
        for p in insert_payloads
        if any(
            r.get("RID") in {"W1", "W2"}
            for r in (p if isinstance(p, list) else [])
        )
    ]
    assert widget_inserts, "expected Widget rows to be posted"
    widget_rows = widget_inserts[0]
    by_rid = {r["RID"]: r for r in widget_rows}
    assert by_rid["W1"]["Color"] == "C-DST-RED"
    assert by_rid["W2"]["Color"] == "C-SRC-BLUE"


def test_content_conflict_fail_propagates(tmp_path: Path) -> None:
    """Default content_on_conflict=FAIL surfaces the 409 from ERMrest.

    The loader doesn't catch the HTTPError; the caller sees a clear
    raise from the POST and can decide how to recover (typically by
    re-running with SKIP_BY_RID).
    """
    import requests

    bag = _build_vocab_bag(tmp_path)
    catalog = _mock_catalog()

    def _get(path: str, **_: Any):
        # Vocab fetch returns empty so Color rows are both inserts;
        # we want Widget to be the one that 409s.
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = []
        return resp

    def _post(path: str, **_: Any):
        # Simulate a 409 on Widget insert.
        if "Widget" in path:
            err = requests.HTTPError("409 Conflict")
            err.response = MagicMock(status_code=409)
            resp = MagicMock()
            resp.raise_for_status.side_effect = err
            return resp
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        return resp

    catalog.get = _get
    catalog.post = _post

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )
    try:
        with pytest.raises(requests.HTTPError):
            loader.run()
    finally:
        loader.dispose()


def test_content_conflict_skip_by_rid_filters_existing(
    tmp_path: Path,
) -> None:
    """SKIP_BY_RID filters out rows whose RID is already on the destination."""
    from deriva.bag.traversal import ContentConflictStrategy

    bag = _build_vocab_bag(tmp_path)
    catalog = _mock_catalog()

    def _get(path: str, **_: Any):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if "Color" in path and "Name" in path:
            resp.json.return_value = []  # vocab is empty
        elif "Widget" in path:
            # Destination already has W1; W2 is new.
            resp.json.return_value = [{"RID": "W1"}]
        else:
            resp.json.return_value = []
        return resp

    posted: list[dict[str, Any]] = []

    def _post(path: str, **kwargs: Any):
        posted.append({"path": path, "json": kwargs["json"]})
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        return resp

    catalog.get = _get
    catalog.post = _post

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            content_on_conflict=ContentConflictStrategy.SKIP_BY_RID,
        ),
        database_dir=tmp_path / "db",
    )
    try:
        report = loader.run()
    finally:
        loader.dispose()

    widget_stats = report.table_stats["demo.Widget"]
    assert widget_stats.rows_skipped_on_conflict == 1
    assert widget_stats.rows_inserted == 1

    widget_post = next(p for p in posted if "Widget" in p["path"])
    widget_rids = {r["RID"] for r in widget_post["json"]}
    assert widget_rids == {"W2"}, (
        "SKIP_BY_RID should drop W1 (already on destination) but "
        f"send W2; got {widget_rids!r}"
    )
