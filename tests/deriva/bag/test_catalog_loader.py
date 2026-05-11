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
