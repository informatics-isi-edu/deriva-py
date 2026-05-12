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


# ---------------------------------------------------------------------------
# Hatrac URL → path extraction
# ---------------------------------------------------------------------------


def test_hatrac_path_for_full_url() -> None:
    """Full https URL → path-only form."""
    assert (
        BagCatalogLoader._hatrac_path_for(
            "https://example.org/hatrac/Image/abc.png"
        )
        == "/hatrac/Image/abc.png"
    )


def test_hatrac_path_for_bare_path() -> None:
    """Already-bare hatrac path passes through unchanged."""
    assert (
        BagCatalogLoader._hatrac_path_for("/hatrac/Image/abc.png")
        == "/hatrac/Image/abc.png"
    )


def test_hatrac_path_for_non_hatrac_returns_none() -> None:
    """A non-hatrac URL is unrecognized — callers warn and skip."""
    assert (
        BagCatalogLoader._hatrac_path_for(
            "https://cdn.example.org/img.png"
        )
        is None
    )
    assert BagCatalogLoader._hatrac_path_for("/other/img.png") is None


def test_hatrac_path_for_strips_version_suffix() -> None:
    """Versioned ``...:VERSIONID`` URLs strip to the unversioned base.

    Hatrac assigns versions on PUT; trying to write to a versioned
    URL returns ``405 Method Not Allowed``. The source catalog's
    row carries the versioned URL (so consumers can fetch the
    exact version), but the upload target must be the unversioned
    name.
    """
    full = (
        "https://example.org/hatrac/"
        "Execution_Metadata/abc.json:D3EG6MCLC7Y7KDONCTVUB5EEKU"
    )
    assert (
        BagCatalogLoader._hatrac_path_for(full)
        == "/hatrac/Execution_Metadata/abc.json"
    )
    bare = "/hatrac/Image/I1/img.png:VERSION1"
    assert (
        BagCatalogLoader._hatrac_path_for(bare)
        == "/hatrac/Image/I1/img.png"
    )


def test_hatrac_path_for_keeps_colon_in_path() -> None:
    """A ``:`` in a *directory* component is not a version separator.

    The version separator is the *last* colon and it must come
    *after* the last slash. A colon embedded earlier in the path
    (in a directory name) is left alone.
    """
    assert (
        BagCatalogLoader._hatrac_path_for(
            "/hatrac/some:dir/file.png"
        )
        == "/hatrac/some:dir/file.png"
    )


# ---------------------------------------------------------------------------
# Date/datetime coercion for JSON serialization
# ---------------------------------------------------------------------------


def test_coerce_datetimes_handles_date_and_datetime() -> None:
    """``datetime.date`` and ``datetime.datetime`` become ISO strings."""
    import datetime

    row = {
        "RID": "A1",
        "the_date": datetime.date(2026, 5, 11),
        "the_datetime": datetime.datetime(
            2026, 5, 11, 14, 30, 0
        ),
        "plain_text": "hello",
        "an_int": 42,
        "none_val": None,
    }
    coerced = BagCatalogLoader._coerce_datetimes(row)
    assert coerced["RID"] == "A1"
    assert coerced["the_date"] == "2026-05-11"
    assert coerced["the_datetime"] == "2026-05-11T14:30:00"
    assert coerced["plain_text"] == "hello"
    assert coerced["an_int"] == 42
    assert coerced["none_val"] is None


def test_coerce_datetimes_does_not_mutate_input() -> None:
    """The caller's row dict is left untouched."""
    import datetime

    row = {"RID": "A1", "d": datetime.date(2026, 5, 11)}
    BagCatalogLoader._coerce_datetimes(row)
    # Original still has the date object.
    assert isinstance(row["d"], datetime.date)


# ---------------------------------------------------------------------------
# Asset upload — dedupe + force semantics
# ---------------------------------------------------------------------------


def _build_asset_only_bag(tmp_path: Path) -> Path:
    """Build a bag with one asset table and one row referencing a local file."""
    bag = tmp_path / "asset_only" / "bag"
    (bag / "data" / "demo").mkdir(parents=True)
    (bag / "data" / "asset" / "Image" / "I1").mkdir(parents=True)
    local_asset = bag / "data" / "asset" / "Image" / "I1" / "img.png"
    local_asset.write_bytes(b"\x89PNG\r\n\x1a\n...")

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
                            {"name": "RID", "type": {"typename": "text"}, "nullok": False, "default": None, "comment": None},
                            {"name": "Filename", "type": {"typename": "text"}, "nullok": True, "default": None, "comment": None},
                            {"name": "URL", "type": {"typename": "text"}, "nullok": True, "default": None, "comment": None},
                            {"name": "Length", "type": {"typename": "int8"}, "nullok": True, "default": None, "comment": None},
                            {"name": "MD5", "type": {"typename": "text"}, "nullok": True, "default": None, "comment": None},
                            {"name": "Description", "type": {"typename": "markdown"}, "nullok": True, "default": None, "comment": None},
                        ],
                        "keys": [{"names": [["demo", "Image_RID_key"]], "unique_columns": ["RID"]}],
                        "foreign_keys": [],
                    },
                },
            }
        },
    }
    (bag / "data" / "schema.json").write_text(json.dumps(doc))
    with (bag / "data" / "demo" / "Image.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Filename", "URL", "Length", "MD5", "Description"])
        w.writerow(
            [
                "I1",
                str(local_asset),  # local path already populated
                "https://src.example.org/hatrac/Image/img.png",
                "8",
                "deadbeef",  # md5 (fake, but the test mocks the HEAD)
                "img",
            ]
        )
    return bag


def test_upload_assets_dedupe_skips_when_md5_matches(tmp_path: Path) -> None:
    """``UPLOAD_IF_MISSING`` skips bytes when destination MD5 matches."""
    import asyncio

    bag = _build_asset_only_bag(tmp_path)
    catalog = _mock_catalog()
    hatrac = MagicMock()
    head_resp = MagicMock()
    head_resp.status_code = 200
    head_resp.headers = {"Content-MD5": "deadbeef"}
    hatrac.head.return_value = head_resp

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.UPLOAD_IF_MISSING),
        database_dir=tmp_path / "db",
    )
    loader._hatrac_store = hatrac  # bypass real construction
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows = list(loader.bag_db.get_table_contents("Image"))
        uploaded, deduped = asyncio.run(
            loader._upload_assets(image, rows)
        )
    finally:
        loader.dispose()

    assert uploaded == 0
    assert deduped == 1
    hatrac.put_loc.assert_not_called()
    hatrac.head.assert_called_once_with("/hatrac/Image/img.png")


def test_upload_assets_uploads_when_md5_differs(tmp_path: Path) -> None:
    """``UPLOAD_IF_MISSING`` pushes bytes when destination MD5 differs."""
    import asyncio

    bag = _build_asset_only_bag(tmp_path)
    catalog = _mock_catalog()
    hatrac = MagicMock()
    head_resp = MagicMock()
    head_resp.status_code = 200
    head_resp.headers = {"Content-MD5": "different_md5"}
    hatrac.head.return_value = head_resp

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.UPLOAD_IF_MISSING),
        database_dir=tmp_path / "db",
    )
    loader._hatrac_store = hatrac
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows = list(loader.bag_db.get_table_contents("Image"))
        uploaded, deduped = asyncio.run(
            loader._upload_assets(image, rows)
        )
    finally:
        loader.dispose()

    assert uploaded == 1
    assert deduped == 0
    hatrac.put_loc.assert_called_once()
    # The destination Hatrac path is the source URL's path component.
    args, kwargs = hatrac.put_loc.call_args
    assert args[0] == "/hatrac/Image/img.png"
    assert kwargs.get("md5") == "deadbeef"
    assert kwargs.get("force") is False


def test_upload_assets_force_bypasses_head(tmp_path: Path) -> None:
    """``UPLOAD_FORCE`` skips the HEAD and pushes unconditionally."""
    import asyncio

    bag = _build_asset_only_bag(tmp_path)
    catalog = _mock_catalog()
    hatrac = MagicMock()

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.UPLOAD_FORCE),
        database_dir=tmp_path / "db",
    )
    loader._hatrac_store = hatrac
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows = list(loader.bag_db.get_table_contents("Image"))
        uploaded, deduped = asyncio.run(
            loader._upload_assets(image, rows)
        )
    finally:
        loader.dispose()

    assert uploaded == 1
    assert deduped == 0
    hatrac.head.assert_not_called()
    _args, kwargs = hatrac.put_loc.call_args
    assert kwargs.get("force") is True


def test_upload_assets_skips_missing_local_file(tmp_path: Path) -> None:
    """Rows whose Filename doesn't exist on disk are warned and skipped."""
    import asyncio

    bag = _build_asset_only_bag(tmp_path)
    # Remove the local file so the row points at nothing.
    (bag / "data" / "asset" / "Image" / "I1" / "img.png").unlink()

    catalog = _mock_catalog()
    hatrac = MagicMock()
    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.UPLOAD_IF_MISSING),
        database_dir=tmp_path / "db",
    )
    loader._hatrac_store = hatrac
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows = list(loader.bag_db.get_table_contents("Image"))
        uploaded, deduped = asyncio.run(
            loader._upload_assets(image, rows)
        )
    finally:
        loader.dispose()

    assert uploaded == 0
    assert deduped == 0
    hatrac.put_loc.assert_not_called()


# ---------------------------------------------------------------------------
# Empty-string → NULL coercion on nullable columns
# ---------------------------------------------------------------------------


def _table_with_columns(*specs: tuple[str, bool]) -> Any:
    """Build a Table-shaped MagicMock from ``(name, nullable)`` pairs."""
    table = MagicMock()
    cols = []
    for name, nullable in specs:
        col = MagicMock()
        col.name = name
        col.nullok = nullable
        cols.append(col)
    table.column_definitions = cols
    return table


def test_coerce_empty_to_null_nullable_empty_string() -> None:
    """``''`` on a nullable column becomes ``None``."""
    table = _table_with_columns(
        ("RID", False), ("optional_fk", True)
    )
    row = {"RID": "A1", "optional_fk": ""}
    result = BagCatalogLoader._coerce_empty_to_null(table, row)
    assert result["optional_fk"] is None


def test_coerce_empty_to_null_skips_non_null_columns() -> None:
    """``''`` on a NOT-NULL column is left alone (real data error)."""
    table = _table_with_columns(
        ("RID", False), ("required_col", False)
    )
    row = {"RID": "A1", "required_col": ""}
    result = BagCatalogLoader._coerce_empty_to_null(table, row)
    assert result["required_col"] == ""


def test_coerce_empty_to_null_leaves_real_values() -> None:
    """Non-empty values pass through unchanged."""
    table = _table_with_columns(
        ("RID", False), ("optional", True)
    )
    row = {"RID": "A1", "optional": "value"}
    result = BagCatalogLoader._coerce_empty_to_null(table, row)
    assert result == {"RID": "A1", "optional": "value"}


def test_coerce_empty_to_null_does_not_mutate_input() -> None:
    """Caller's row dict is preserved."""
    table = _table_with_columns(("opt", True))
    row = {"opt": ""}
    BagCatalogLoader._coerce_empty_to_null(table, row)
    assert row["opt"] == ""


# ---------------------------------------------------------------------------
# FK cycle: two-phase insert with deferred FK columns
# ---------------------------------------------------------------------------


def _build_cycle_bag(
    tmp_path: Path,
    *,
    cycle_col_nullable: bool = True,
) -> Path:
    """Bag with a two-way ``Dataset ↔ Dataset_Version`` FK cycle.

    Both FKs reference RID. By default the cycle-cut columns are
    nullable, mirroring deriva-ml's design intent. Pass
    ``cycle_col_nullable=False`` to exercise the loader's
    fail-fast guard.
    """
    bag = tmp_path / "cycle_bag" / "bag"
    (bag / "data" / "demo").mkdir(parents=True)

    nullok = cycle_col_nullable
    doc = {
        "snaptime": "2026-01-01T00:00:00",
        "schemas": {
            "demo": {
                "schema_name": "demo",
                "tables": {
                    "Dataset": {
                        "schema_name": "demo",
                        "table_name": "Dataset",
                        "kind": "table",
                        "column_definitions": [
                            {"name": "RID", "type": {"typename": "text"}, "nullok": False, "default": None, "comment": None},
                            {"name": "Version", "type": {"typename": "text"}, "nullok": nullok, "default": None, "comment": None},
                        ],
                        "keys": [{"names": [["demo", "Dataset_RID_key"]], "unique_columns": ["RID"]}],
                        "foreign_keys": [
                            {
                                "names": [["demo", "Dataset_Version_fkey"]],
                                "foreign_key_columns": [{"schema_name": "demo", "table_name": "Dataset", "column_name": "Version"}],
                                "referenced_columns": [{"schema_name": "demo", "table_name": "Dataset_Version", "column_name": "RID"}],
                            }
                        ],
                    },
                    "Dataset_Version": {
                        "schema_name": "demo",
                        "table_name": "Dataset_Version",
                        "kind": "table",
                        "column_definitions": [
                            {"name": "RID", "type": {"typename": "text"}, "nullok": False, "default": None, "comment": None},
                            {"name": "Dataset", "type": {"typename": "text"}, "nullok": nullok, "default": None, "comment": None},
                        ],
                        "keys": [{"names": [["demo", "Dataset_Version_RID_key"]], "unique_columns": ["RID"]}],
                        "foreign_keys": [
                            {
                                "names": [["demo", "Dataset_Version_Dataset_fkey"]],
                                "foreign_key_columns": [{"schema_name": "demo", "table_name": "Dataset_Version", "column_name": "Dataset"}],
                                "referenced_columns": [{"schema_name": "demo", "table_name": "Dataset", "column_name": "RID"}],
                            }
                        ],
                    },
                },
            }
        },
    }
    (bag / "data" / "schema.json").write_text(json.dumps(doc))
    with (bag / "data" / "demo" / "Dataset.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Version"])
        w.writerow(["D1", "DV1"])
    with (bag / "data" / "demo" / "Dataset_Version.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Dataset"])
        w.writerow(["DV1", "D1"])
    return bag


def test_loader_raises_when_cycle_fk_is_not_null(tmp_path: Path) -> None:
    """An FK cycle on a NOT-NULL column must fail-fast.

    Two-phase insert nulls the deferred FK on first-pass and
    patches it after. That can't satisfy a NOT-NULL constraint
    on the cycle column, so the loader raises before any rows
    are sent rather than letting ERMrest reject the insert.
    """
    bag = _build_cycle_bag(tmp_path, cycle_col_nullable=False)
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )
    try:
        with pytest.raises(ValueError, match="NOT NULL"):
            loader.run()
    finally:
        loader.dispose()


def test_loader_defers_cycle_fks_and_patches_in_second_pass(
    tmp_path: Path,
) -> None:
    """Cycle FKs are nulled on insert and PUT in the second pass.

    The two-way ``Dataset ↔ Dataset_Version`` cycle forces the
    orderer to drop one edge. On first-pass insert, the dropped
    edge's FK column must be sent as NULL (the target row hasn't
    landed yet); after every table is inserted, the loader PUTs
    the original values via ``/attributegroup/RID;col``.
    """
    bag = _build_cycle_bag(tmp_path, cycle_col_nullable=True)
    catalog = _mock_catalog()

    posted: list[dict[str, Any]] = []
    put_calls: list[dict[str, Any]] = []

    def _get(path: str, **_: Any):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = []  # destination is empty
        return resp

    def _post(path: str, **kwargs: Any):
        posted.append({"path": path, "json": kwargs["json"]})
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        return resp

    def _put(path: str, **kwargs: Any):
        put_calls.append({"path": path, "json": kwargs["json"]})
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        return resp

    catalog.get = _get
    catalog.post = _post
    catalog.put = _put

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )
    try:
        loader.run()
    finally:
        loader.dispose()

    # The cycle-cut FK column was nulled on the first-pass insert.
    # Exactly one of (Dataset.Version, Dataset_Version.Dataset) is
    # deferred — the orderer picks which.
    insert_targets = {p["path"] for p in posted}
    assert any("Dataset" in t for t in insert_targets)

    # At least one of the inserted rows must have its cycle FK as
    # None (the dropped edge).
    deferred_seen = False
    for entry in posted:
        for row in entry["json"]:
            if (
                "Dataset_Version" in entry["path"]
                and row.get("Dataset") is None
            ) or (
                "Dataset" in entry["path"]
                and "Dataset_Version" not in entry["path"]
                and row.get("Version") is None
            ):
                deferred_seen = True
    assert deferred_seen, (
        "Expected at least one cycle FK to be deferred to NULL on "
        f"first-pass insert; saw payloads: {posted!r}"
    )

    # Second pass: a PUT to /attributegroup/{table}/RID;{col} for
    # the deferred column. Restoring the original value.
    assert put_calls, "Expected at least one second-pass PUT"
    for call in put_calls:
        assert "/attributegroup/" in call["path"]
        # Each row in the PUT payload must include RID + the
        # deferred column.
        for row in call["json"]:
            assert "RID" in row
