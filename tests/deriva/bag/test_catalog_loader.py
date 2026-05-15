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


# Per-catalog cache of (schema, table) → stable ``_TableWrapper`` mocks.
# ``MagicMock.__getitem__`` by default returns the *same* child for every
# key, which collapses different tables into one mock and breaks
# ``call_args`` assertions. Track per-key state externally and wire it
# via ``__getitem__.side_effect``.
#
# The autouse ``_clear_pb_dispatch`` fixture below resets this between
# tests so the dispatcher state stays bounded across the session — tests
# don't leak ``MagicMock`` chains into each other's catalogs.
_pb_dispatch: "dict[int, dict[str, dict[str, MagicMock]]]" = {}


@pytest.fixture(autouse=True)
def _clear_pb_dispatch():
    """Reset the per-catalog mock-dispatch table between tests.

    Without this, every test that calls ``_pb_table`` adds an entry
    keyed by ``id(catalog)`` that lives for the rest of the session.
    The entries are inert (other tests use different catalog
    instances), but they accumulate and obscure leak-debugging.
    """
    _pb_dispatch.clear()
    yield
    _pb_dispatch.clear()


def _pb_table(catalog: MagicMock, schema_name: str, table_name: str) -> MagicMock:
    """Resolve the path-builder ``_TableWrapper`` mock the loader will hit."""
    catalog_key = id(catalog)
    schemas_dict = _pb_dispatch.get(catalog_key)
    if schemas_dict is None:
        schemas_dict = {}
        _pb_dispatch[catalog_key] = schemas_dict
        pb = catalog.getPathBuilder.return_value
        pb.schemas.__getitem__.side_effect = lambda key: schemas_dict.setdefault(
            key, _build_schema_mock()
        )
    schema_mock = schemas_dict.setdefault(schema_name, _build_schema_mock())
    return schema_mock.tables[table_name]


def _build_schema_mock() -> MagicMock:
    """Construct a per-schema mock whose ``tables[name]`` dispatches per-key.

    Newly-minted table-wrapper mocks default ``insert.return_value``
    and ``update.return_value`` to ``[]`` so the loader's
    ``len(list(result))`` accounting doesn't blow up on tests that
    don't otherwise pin a specific return shape.
    """
    schema = MagicMock()
    tables_dict: dict[str, MagicMock] = {}

    def _new_table_wrapper(key: str) -> MagicMock:
        tw = MagicMock(name=f"TableWrapper[{key}]")
        tw.insert.return_value = []
        tw.update.return_value = []
        return tw

    schema.tables.__getitem__.side_effect = lambda key: tables_dict.setdefault(
        key, _new_table_wrapper(key)
    )
    return schema


def _stub_insert_result(tw: MagicMock, rows: list[dict[str, Any]] | None = None) -> None:
    """Make ``tw.insert(...)`` return a list-able stub.

    ``BagCatalogLoader._insert_rows`` calls ``len(list(result))`` to
    compute the inserted-row count (with ``onconflict=skip`` the
    destination returns only the rows it accepted). The default
    ``MagicMock`` is not iterable; the loader would compute 0 rows
    inserted, which breaks tests that assert non-zero counts.
    """
    tw.insert.return_value = list(rows) if rows is not None else []


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


def test_dangling_fk_strategy_preserve_short_circuits(tmp_path: Path) -> None:
    """``PRESERVE`` skips the bag-side check; rows pass through verbatim.

    Use case: end-of-execution commit. The bag ships Image rows
    that reference Subject FKs already at the destination (Subjects
    were created in an earlier execution). The bag never carried
    those Subject rows, so a bag-side check would flag them as
    dangling. ``PRESERVE`` trusts the destination catalog's FK
    constraint to be authoritative — the rows go in, and if a
    parent is genuinely missing the load fails with ERMrest's
    real HTTP 409 unfiltered.
    """
    bag = _build_fk_bag(tmp_path)
    db_dir = tmp_path / "db"
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            dangling_fk_strategy=DanglingFKStrategy.PRESERVE,
        ),
        database_dir=db_dir,
    )
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows_in = [
            {"RID": "I1", "Subject": "S1"},  # parent in bag
            {"RID": "I2", "Subject": "S2"},  # parent NOT in bag
        ]
        survivors, skipped, nullified = (
            loader._apply_dangling_fk_strategy(image, rows_in)
        )
        # Verbatim pass-through — no rows dropped, no FK nulled.
        assert survivors == rows_in
        assert skipped == 0
        assert nullified == 0
        # The S2 reference is preserved as-is; ERMrest will be the
        # authority at insert time.
        i2 = [r for r in survivors if r["RID"] == "I2"][0]
        assert i2["Subject"] == "S2"
    finally:
        loader.dispose()


# ---------------------------------------------------------------------------
# preserve_provenance — nondefaults URL shape
# ---------------------------------------------------------------------------


def _make_fake_table() -> MagicMock:
    """Minimal DerivaTable mock that satisfies ``_insert_rows``.

    We only need ``schema.name``, ``name``, and a column list with
    no array-typed columns (the wire serializer special-cases
    those). Everything else routes through ``catalog.post``, which
    is what the test inspects.
    """
    table = MagicMock(name="Table[demo.Image]")
    table.name = "Image"
    table.schema = MagicMock()
    table.schema.name = "demo"
    table.column_definitions = []
    return table


def test_insert_rows_preserve_provenance_default_sends_rct_rcb(
    tmp_path: Path,
) -> None:
    """Default ``preserve_provenance=True`` opts ``RID,RCT,RCB`` out of defaults.

    Clone semantics: the bag's source audit columns are real
    history worth keeping at the destination. The loader hands
    ``nondefaults={RID,RCT,RCB}`` to ``_TableWrapper.insert`` so
    ERMrest does not overwrite them with server-side defaults.
    """
    import asyncio as _asyncio

    bag = _build_fk_bag(tmp_path)
    catalog = _mock_catalog()

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(),  # preserve_provenance default = True
        database_dir=tmp_path / "db",
    )
    tw = _pb_table(catalog, "demo", "Image")
    _stub_insert_result(tw, [{"RID": "I1"}])
    try:
        _asyncio.run(
            loader._insert_rows(
                _make_fake_table(),
                [{"RID": "I1", "Filename": "a.bin"}],
            )
        )
    finally:
        loader.dispose()

    assert tw.insert.call_args.kwargs["nondefaults"] == {"RID", "RCT", "RCB"}


def test_insert_rows_preserve_provenance_false_sends_only_rid(
    tmp_path: Path,
) -> None:
    """``preserve_provenance=False`` opts only ``RID`` out of defaults.

    Commit semantics: the bag carries newly-minted rows. Only
    ``RID`` is preserved (the caller leased it ahead of time);
    ``RCT`` and ``RCB`` get the destination's current-timestamp /
    current-user defaults. Without this, NULL ``RCB`` would
    violate the ``{Table}_RCB_fkey`` FK constraint at insert
    time.
    """
    import asyncio as _asyncio

    bag = _build_fk_bag(tmp_path)
    catalog = _mock_catalog()

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(preserve_provenance=False),
        database_dir=tmp_path / "db",
    )
    tw = _pb_table(catalog, "demo", "Image")
    _stub_insert_result(tw, [{"RID": "I1"}])
    try:
        _asyncio.run(
            loader._insert_rows(
                _make_fake_table(),
                [{"RID": "I1", "Filename": "a.bin"}],
            )
        )
    finally:
        loader.dispose()

    assert tw.insert.call_args.kwargs["nondefaults"] == {"RID"}


def test_insert_rows_preserve_provenance_false_strips_system_columns(
    tmp_path: Path,
) -> None:
    """``preserve_provenance=False`` strips RCT/RCB/RMT/RMB from each row.

    Bag CSVs serialize NULL as ``""`` (CSV has no NULL sentinel).
    ERMrest rejects ``""`` for the timestamp ``RCT`` / ``RMT``
    columns and the FK-typed ``RCB`` / ``RMB`` columns with a
    400 ``invalid input syntax`` error. Stripping these columns
    from the row dict entirely (relying on the server's defaults
    to populate them) is the symmetrical counterpart of the
    ``nondefaults={RID}`` setting.
    """
    import asyncio as _asyncio

    bag = _build_fk_bag(tmp_path)
    catalog = _mock_catalog()

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(preserve_provenance=False),
        database_dir=tmp_path / "db",
    )
    tw = _pb_table(catalog, "demo", "Image")
    _stub_insert_result(tw, [{"RID": "I1"}])
    try:
        _asyncio.run(
            loader._insert_rows(
                _make_fake_table(),
                [
                    {
                        "RID": "I1",
                        "Filename": "a.bin",
                        # System columns the bag carries as empty
                        # strings from its CSV — must be stripped
                        # before reaching ERMrest.
                        "RCT": "",
                        "RCB": "",
                        "RMT": "",
                        "RMB": "",
                    }
                ],
            )
        )
    finally:
        loader.dispose()

    posted_rows = tw.insert.call_args.args[0]
    row = posted_rows[0]
    assert "RID" in row
    assert "Filename" in row
    for col in ("RCT", "RCB", "RMT", "RMB"):
        assert col not in row, (
            f"system column {col} should have been stripped under "
            f"preserve_provenance=False; got {row}"
        )


def test_insert_rows_preserve_provenance_true_keeps_system_columns(
    tmp_path: Path,
) -> None:
    """Clone semantics keep RCT/RCB in the row dict verbatim.

    Backward-compat guard: the strip behavior is opt-in via
    ``preserve_provenance=False``. Default callers see the
    bag's audit data ride through.
    """
    import asyncio as _asyncio

    bag = _build_fk_bag(tmp_path)
    catalog = _mock_catalog()

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(),  # preserve_provenance=True default
        database_dir=tmp_path / "db",
    )
    tw = _pb_table(catalog, "demo", "Image")
    _stub_insert_result(tw, [{"RID": "I1"}])
    try:
        _asyncio.run(
            loader._insert_rows(
                _make_fake_table(),
                [
                    {
                        "RID": "I1",
                        "Filename": "a.bin",
                        "RCT": "2026-01-01T00:00:00+00:00",
                        "RCB": "https://idp/user1",
                    }
                ],
            )
        )
    finally:
        loader.dispose()

    posted_rows = tw.insert.call_args.args[0]
    row = posted_rows[0]
    assert row["RCT"] == "2026-01-01T00:00:00+00:00"
    assert row["RCB"] == "https://idp/user1"


def test_insert_rows_accounting_works_when_result_spans_multiple_batches(
    tmp_path: Path,
) -> None:
    """``_insert_rows`` counts the returned rows correctly when the
    PathBuilder result spans multiple HTTP batches.

    ``_TableWrapper.insert`` chunks the input into batches of up to
    ``max_batch_rows`` (default 1000) and returns a ``_ResultSet``
    that walks every batch's response. The loader computes
    ``inserted = len(list(result))``. The path-builder result
    typically yields a single concatenated list; this test pins
    the contract by handing back a multi-element iterable and
    asserting the loader counts every element — not just the
    first batch.
    """
    import asyncio as _asyncio

    bag = _build_fk_bag(tmp_path)
    catalog = _mock_catalog()

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(),
        database_dir=tmp_path / "db",
    )
    tw = _pb_table(catalog, "demo", "Image")
    # Simulate the path-builder concatenating three batches of three
    # rows each (i.e. nine accepted-row dicts). ``_TableWrapper.insert``
    # returns a ``_ResultSet`` whose iteration walks the
    # concatenated payload; ``list()`` materializes it.
    accepted = [{"RID": f"I{i}"} for i in range(9)]
    tw.insert.return_value = accepted
    try:
        inserted = _asyncio.run(
            loader._insert_rows(
                _make_fake_table(),
                # Nine input rows; the real PathBuilder would batch
                # internally. The mock returns nine accepted rows.
                [{"RID": f"I{i}", "Filename": f"f{i}.bin"} for i in range(9)],
            )
        )
    finally:
        loader.dispose()

    assert inserted == 9, (
        f"loader must count every accepted row across batches; "
        f"got {inserted} (expected 9)"
    )


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
    assert s.assets_attempted == 0


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
    from deriva.bag.catalog_loader import TableClass

    bag = _build_vocab_bag(tmp_path)
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        database_dir=tmp_path / "db",
    )
    try:
        color = loader.bag_db.model.schemas["demo"].tables["Color"]
        widget = loader.bag_db.model.schemas["demo"].tables["Widget"]
        assert loader._classify_table(color) == TableClass.VOCABULARY
        assert loader._classify_table(widget) == TableClass.CONTENT
    finally:
        loader.dispose()


def test_vocab_load_matches_by_name_and_records_remap(tmp_path: Path) -> None:
    """Existing destination rows match by ``Name``; RID remap is recorded."""
    bag = _build_vocab_bag(tmp_path)

    # Mock catalog: destination already has a "Red" row at a *different* RID.
    catalog = _mock_catalog()

    color_tw = _pb_table(catalog, "demo", "Color")
    # PathBuilder fetch returns the destination's existing vocab rows.
    color_tw.attributes.return_value.fetch.return_value = [
        {"Name": "Red", "RID": "C-DST-RED"},
        # Blue is absent on the destination — must be inserted.
    ]
    _stub_insert_result(color_tw, [{"RID": "C-SRC-BLUE"}])

    widget_tw = _pb_table(catalog, "demo", "Widget")
    _stub_insert_result(widget_tw, [{"RID": "W1"}, {"RID": "W2"}])

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

    # Widget rows passed to insert have their Color FK rewritten:
    # W1 → Color=C-DST-RED (remapped), W2 → Color=C-SRC-BLUE (identity).
    widget_rows = widget_tw.insert.call_args.args[0]
    by_rid = {r["RID"]: r for r in widget_rows}
    assert by_rid["W1"]["Color"] == "C-DST-RED"
    assert by_rid["W2"]["Color"] == "C-SRC-BLUE"


def test_content_conflict_fail_propagates(tmp_path: Path) -> None:
    """Default content_on_conflict=FAIL surfaces the 409 from ERMrest.

    The loader doesn't catch the HTTPError; the caller sees a clear
    raise from the path-builder and can decide how to recover
    (typically by re-running with SKIP_BY_RID).
    """
    import requests

    bag = _build_vocab_bag(tmp_path)
    catalog = _mock_catalog()

    color_tw = _pb_table(catalog, "demo", "Color")
    color_tw.attributes.return_value.fetch.return_value = []
    _stub_insert_result(color_tw, [{"RID": "C-SRC-RED"}, {"RID": "C-SRC-BLUE"}])

    widget_tw = _pb_table(catalog, "demo", "Widget")
    # Simulate a 409 on Widget insert: the path-builder raises HTTPError.
    err = requests.HTTPError("409 Conflict")
    err.response = MagicMock(status_code=409)
    widget_tw.insert.side_effect = err

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
    """SKIP_BY_RID is forwarded to _TableWrapper.insert(on_conflict_skip=True).

    With the new reuse-the-deriva-py path, the loader no longer
    pre-fetches existing RIDs — it asks ERMrest to skip them at
    insert time via ``onconflict=skip``. The mock simulates the
    server's reply: when W1 already exists, the insert result
    returns only W2.
    """
    from deriva.bag.traversal import ContentConflictStrategy

    bag = _build_vocab_bag(tmp_path)
    catalog = _mock_catalog()

    color_tw = _pb_table(catalog, "demo", "Color")
    color_tw.attributes.return_value.fetch.return_value = []  # vocab empty
    _stub_insert_result(color_tw, [{"RID": "C-SRC-RED"}, {"RID": "C-SRC-BLUE"}])

    widget_tw = _pb_table(catalog, "demo", "Widget")
    # Server skipped W1 (already exists); only W2 came back.
    _stub_insert_result(widget_tw, [{"RID": "W2"}])

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

    # The loader handed both rows to insert with on_conflict_skip=True;
    # the server picked which to skip.
    widget_insert_kwargs = widget_tw.insert.call_args.kwargs
    assert widget_insert_kwargs["on_conflict_skip"] is True
    sent_rids = {r["RID"] for r in widget_tw.insert.call_args.args[0]}
    assert sent_rids == {"W1", "W2"}


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


def _install_mock_uploader(loader: BagCatalogLoader, hatrac_store: Any) -> Any:
    """Plant a mock uploader on the loader for asset-upload tests.

    The bag-loader calls
    ``uploader._hatracUpload(hatrac_path, local_path, md5=...,
    chunked=True, force=...)`` — see ``catalog_loader.py``'s
    ``_upload_assets``. Mocking ``_hatracUpload`` directly (per
    audit §B.1) keeps the test focused on that contract and avoids
    importing :class:`DerivaUpload` (which transitively pulls
    heavy modules unrelated to upload).

    The forwarding function below accepts only the kwargs the
    loader actually sends. ``DerivaUpload._hatracUpload`` has a
    broader real signature (``sha256``, ``content_type``,
    ``content_disposition``, ``chunk_size``, ``create_parents``,
    ``allow_versioning``, ``callback``); they're not relevant
    here and advertising them in the mock would let a future
    refactor accidentally rely on a shape production doesn't
    exercise. Tests that need to pin those kwargs should write
    against :class:`DerivaUpload` itself.

    Inside the real ``_hatracUpload``, deriva-py routes byte
    transfer through ``self.store.put_loc(...)``. The forwarding
    function below mirrors that so the existing ``put_loc``
    assertions continue to work.
    """
    uploader = MagicMock()
    uploader.store = hatrac_store

    def _hatrac_upload(hatrac_uri, file_path, md5=None, chunked=True, force=False):
        return hatrac_store.put_loc(
            hatrac_uri,
            file_path,
            md5=md5,
            chunked=chunked,
            force=force,
        )

    uploader._hatracUpload.side_effect = _hatrac_upload
    loader._uploader = uploader
    return uploader


def test_upload_assets_invokes_put_loc_per_row(tmp_path: Path) -> None:
    """``UPLOAD_IF_MISSING`` invokes ``store.put_loc(chunked=True)`` per asset row.

    The loader hands each row to :meth:`DerivaUpload._hatracUpload`, which
    in turn calls :meth:`HatracStore.put_loc` with ``chunked=True``.
    HEAD-then-PUT dedup is decided inside ``put_loc`` server-side; the
    loader does not pre-check or count dedups. :attr:`assets_attempted`
    counts upload invocations.
    """
    import asyncio

    bag = _build_asset_only_bag(tmp_path)
    catalog = _mock_catalog()
    hatrac = MagicMock()
    # put_loc returns a Content-Location string on success.
    hatrac.put_loc.return_value = "/hatrac/Image/img.png:VERSION1"

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.UPLOAD_IF_MISSING),
        database_dir=tmp_path / "db",
    )
    _install_mock_uploader(loader, hatrac)
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows = list(loader.bag_db.get_table_contents("Image"))
        attempted = asyncio.run(loader._upload_assets(image, rows))
    finally:
        loader.dispose()

    assert attempted == 1
    # put_loc was called with the canonicalized destination path, the
    # row's MD5 (for server-side dedup verification), chunked mode on,
    # and force off.
    hatrac.put_loc.assert_called_once()
    args, kwargs = hatrac.put_loc.call_args
    assert args[0] == "/hatrac/Image/img.png"
    assert kwargs.get("md5") == "deadbeef"
    assert kwargs.get("chunked") is True
    assert kwargs.get("force") is False


def test_upload_assets_force_passes_through(tmp_path: Path) -> None:
    """``UPLOAD_FORCE`` propagates as ``force=True`` to ``put_loc``."""
    import asyncio

    bag = _build_asset_only_bag(tmp_path)
    catalog = _mock_catalog()
    hatrac = MagicMock()
    hatrac.put_loc.return_value = "/hatrac/Image/img.png:VERSION1"

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.UPLOAD_FORCE),
        database_dir=tmp_path / "db",
    )
    _install_mock_uploader(loader, hatrac)
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows = list(loader.bag_db.get_table_contents("Image"))
        attempted = asyncio.run(loader._upload_assets(image, rows))
    finally:
        loader.dispose()

    assert attempted == 1
    _args, kwargs = hatrac.put_loc.call_args
    assert kwargs.get("force") is True


def test_upload_assets_skips_missing_local_file(tmp_path: Path) -> None:
    """Rows whose Filename doesn't exist on disk are warned and skipped.

    Skipped rows do not contribute to :attr:`assets_attempted` — the
    counter reflects calls to :meth:`_hatracUpload`, which the loader
    doesn't issue for rows without local bytes.
    """
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
    _install_mock_uploader(loader, hatrac)
    try:
        image = loader.bag_db.model.schemas["demo"].tables["Image"]
        rows = list(loader.bag_db.get_table_contents("Image"))
        attempted = asyncio.run(loader._upload_assets(image, rows))
    finally:
        loader.dispose()

    assert attempted == 0
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
    """Cycle FKs are nulled on insert and updated in the second pass.

    The two-way ``Dataset ↔ Dataset_Version`` cycle forces the
    orderer to drop one edge. On first-pass insert, the dropped
    edge's FK column must be sent as NULL (the target row hasn't
    landed yet); after every table is inserted, the loader patches
    the original values via ``_TableWrapper.update``.
    """
    bag = _build_cycle_bag(tmp_path, cycle_col_nullable=True)
    catalog = _mock_catalog()

    dataset_tw = _pb_table(catalog, "demo", "Dataset")
    dataset_version_tw = _pb_table(catalog, "demo", "Dataset_Version")
    _stub_insert_result(dataset_tw, [{"RID": "D1"}])
    _stub_insert_result(dataset_version_tw, [{"RID": "DV1"}])

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

    # Exactly one of (Dataset.Version, Dataset_Version.Dataset) is
    # deferred — the orderer picks which. Inspect the rows actually
    # handed to each table's insert.
    dataset_rows = (
        dataset_tw.insert.call_args.args[0]
        if dataset_tw.insert.called
        else []
    )
    dv_rows = (
        dataset_version_tw.insert.call_args.args[0]
        if dataset_version_tw.insert.called
        else []
    )

    deferred_seen = any(
        row.get("Version") is None for row in dataset_rows
    ) or any(
        row.get("Dataset") is None for row in dv_rows
    )
    assert deferred_seen, (
        "Expected at least one cycle FK to be deferred to NULL on "
        f"first-pass insert; Dataset rows: {dataset_rows!r}, "
        f"Dataset_Version rows: {dv_rows!r}"
    )

    # Second pass: the loader called ``_TableWrapper.update`` on the
    # table holding the cycle-cut column. ``correlation={"RID"}``;
    # ``targets`` lists the cycle column(s).
    patched = []
    if dataset_tw.update.called:
        patched.append(("Dataset", dataset_tw.update))
    if dataset_version_tw.update.called:
        patched.append(("Dataset_Version", dataset_version_tw.update))
    assert patched, "Expected a second-pass update on a cycle table"
    for _, update_mock in patched:
        kwargs = update_mock.call_args.kwargs
        assert kwargs.get("correlation") == {"RID"}
        # Every patch row carries RID.
        for row in update_mock.call_args.args[0]:
            assert "RID" in row


# =============================================================================
# run() entry-point — notebook-loop fallback
# =============================================================================
#
# BagCatalogLoader.run() bridges the async arun() pipeline into
# sync callers. When invoked from inside an already-running event
# loop (Jupyter / papermill kernels), bare asyncio.run() raises
# "cannot be called from a running event loop". The fallback uses
# nest_asyncio to re-enter the active loop.
#
# Both code paths are exercised below. The tests stub arun() to a
# trivial coroutine so the run-time logic stays focused on the
# scheduling shim, not the load pipeline.


def test_run_outside_event_loop_uses_asyncio_run(tmp_path: Path) -> None:
    """``run()`` from a plain sync caller drives ``arun`` to completion.

    The pre-fallback behaviour. Verifies the no-loop path still
    works after the nest_asyncio addition.
    """
    import asyncio as _asyncio

    bag = _build_fk_bag(tmp_path)
    catalog = _mock_catalog()
    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )

    sentinel = LoadReport(bag_path=bag, catalog_id="42")

    async def _fake_arun() -> LoadReport:
        return sentinel

    try:
        # Sanity: no loop is running at this point.
        with pytest.raises(RuntimeError):
            _asyncio.get_running_loop()
        loader.arun = _fake_arun  # type: ignore[method-assign]
        result = loader.run()
    finally:
        loader.dispose()

    assert result is sentinel


def test_run_inside_event_loop_uses_nest_asyncio(tmp_path: Path) -> None:
    """``run()`` from inside a running loop re-enters via ``nest_asyncio``.

    Drive the loader's ``run()`` from inside an ``async def``
    function so a loop is active. Without the fallback this
    would raise ``RuntimeError: asyncio.run() cannot be called
    from a running event loop``.

    ``nest_asyncio`` is a soft dependency of deriva-py — only
    notebook callers need it, the bag module itself is import-safe
    without it. Skip cleanly when the test environment doesn't
    have it installed.
    """
    pytest.importorskip(
        "nest_asyncio",
        reason="soft dep; only notebook callers need it",
    )
    import asyncio as _asyncio

    bag = _build_fk_bag(tmp_path)
    catalog = _mock_catalog()
    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )

    sentinel = LoadReport(bag_path=bag, catalog_id="42")

    async def _fake_arun() -> LoadReport:
        return sentinel

    loader.arun = _fake_arun  # type: ignore[method-assign]

    async def _from_inside_a_running_loop() -> LoadReport:
        # ``loop.is_running()`` is True here; loader.run() must
        # detect that and use nest_asyncio instead of asyncio.run.
        return loader.run()

    try:
        result = _asyncio.run(_from_inside_a_running_loop())
    finally:
        loader.dispose()

    assert result is sentinel


# =============================================================================
# match_by_columns — caller-supplied unique-key reconciliation
# =============================================================================
#
# Generalises the vocab match-by-Name path to non-vocabulary
# tables that nevertheless have a content-addressed unique key
# (e.g. asset tables whose ``URL`` is hash-derived and stable
# across executions). The classifier routes such tables through
# a parallel ``_load_match_by_columns_table`` whose remap shape
# matches the vocab path's exactly, so child FK rewriting via
# ``_rewrite_fks`` works without further changes.


def _build_image_widget_bag(tmp_path: Path) -> Path:
    """Build a bag with one asset-like table + one content table.

    Shapes:

    * ``demo.Image`` — non-vocab table with a content-addressed
      ``URL`` column. Used to exercise ``match_by_columns``.
    * ``demo.Widget`` — content table with a single-column FK to
      ``demo.Image`` for the RID-remap propagation check.

    Differs from ``_build_vocab_bag`` in that neither table has
    the canonical vocab columns; the classifier would route them
    both as ``CONTENT`` without an explicit ``match_by_columns``
    policy.
    """
    cache_key = "image_widget_bag"
    bag = tmp_path / cache_key / "bag"
    (bag / "data" / "demo").mkdir(parents=True)

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
                                "comment": None,
                            },
                            {
                                "name": "Filename",
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
                            },
                            {
                                "names": [["demo", "Image_URL_key"]],
                                "unique_columns": ["URL"],
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
                                "name": "Image",
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
                                    ["demo", "Widget_Image_fkey"]
                                ],
                                "foreign_key_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Widget",
                                        "column_name": "Image",
                                    }
                                ],
                                "referenced_columns": [
                                    {
                                        "schema_name": "demo",
                                        "table_name": "Image",
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
    with (bag / "data" / "demo" / "Image.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "URL", "Filename"])
        w.writerow(["I-SRC-A", "/hatrac/demo/abc.a.png", "a.png"])
        w.writerow(["I-SRC-B", "/hatrac/demo/def.b.png", "b.png"])
    with (bag / "data" / "demo" / "Widget.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "Image"])
        w.writerow(["W1", "I-SRC-A"])
        w.writerow(["W2", "I-SRC-B"])
    return bag


def test_classify_table_routes_through_match_by_columns(tmp_path: Path) -> None:
    """A table listed in ``match_by_columns`` is reported as ``MATCH_BY_COLUMNS``.

    Other tables fall through to ``VOCABULARY`` (structural) or
    ``CONTENT``. The new class takes precedence over the
    structural vocab check, so a vocab-shaped table listed in
    the policy still routes through the new path.
    """
    from deriva.bag.catalog_loader import TableClass

    # Use the existing vocab bag so we also verify the
    # match_by_columns override of the structural classification.
    bag = _build_vocab_bag(tmp_path)
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(
            match_by_columns={("demo", "Color"): ["Name"]},
        ),
        database_dir=tmp_path / "db",
    )
    try:
        color = loader.bag_db.model.schemas["demo"].tables["Color"]
        widget = loader.bag_db.model.schemas["demo"].tables["Widget"]
        # Color is vocab-shaped AND listed in match_by_columns;
        # explicit policy wins.
        assert loader._classify_table(color) == TableClass.MATCH_BY_COLUMNS
        # Widget isn't listed; default classification (CONTENT).
        assert loader._classify_table(widget) == TableClass.CONTENT
    finally:
        loader.dispose()


def test_match_by_columns_matches_existing_and_records_remap(tmp_path: Path) -> None:
    """Existing destination rows match by the supplied column; remap recorded."""
    bag = _build_image_widget_bag(tmp_path)
    catalog = _mock_catalog()

    image_tw = _pb_table(catalog, "demo", "Image")
    image_tw.attributes.return_value.fetch.return_value = [
        {"URL": "/hatrac/demo/abc.a.png", "RID": "I-DST-A"},
    ]
    _stub_insert_result(image_tw, [{"RID": "I-SRC-B"}])

    widget_tw = _pb_table(catalog, "demo", "Widget")
    _stub_insert_result(widget_tw, [{"RID": "W1"}, {"RID": "W2"}])

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            match_by_columns={("demo", "Image"): ["URL"]},
        ),
        database_dir=tmp_path / "db",
    )
    try:
        report = loader.run()
    finally:
        loader.dispose()

    image_stats = report.table_stats["demo.Image"]
    assert image_stats.rows_matched_by_columns == 1  # I-SRC-A matched
    assert image_stats.rows_inserted == 1  # I-SRC-B inserted
    assert image_stats.rows_matched_by_name == 0

    remap = loader._rid_remap[("demo", "Image")]
    assert remap["I-SRC-A"] == "I-DST-A"
    assert remap["I-SRC-B"] == "I-SRC-B"

    # Widget rows handed to insert with their Image FK rewritten.
    widget_rows = widget_tw.insert.call_args.args[0]
    by_rid = {r["RID"]: r for r in widget_rows}
    assert by_rid["W1"]["Image"] == "I-DST-A"
    assert by_rid["W2"]["Image"] == "I-SRC-B"


def test_match_by_columns_composite_key(tmp_path: Path) -> None:
    """Composite match keys (multi-column) match through the path-builder.

    Uses the vocab bag's Color table with a synthetic composite
    key ``["Name", "Description"]``.
    """
    bag = _build_vocab_bag(tmp_path)
    catalog = _mock_catalog()

    color_tw = _pb_table(catalog, "demo", "Color")
    color_tw.attributes.return_value.fetch.return_value = [
        {"Name": "Red", "Description": "Red color", "RID": "C-DST-RED"},
        # Blue absent → must be inserted.
    ]
    _stub_insert_result(color_tw, [{"RID": "C-SRC-BLUE"}])

    widget_tw = _pb_table(catalog, "demo", "Widget")
    _stub_insert_result(widget_tw, [{"RID": "W1"}, {"RID": "W2"}])

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            match_by_columns={
                ("demo", "Color"): ["Name", "Description"],
            },
        ),
        database_dir=tmp_path / "db",
    )
    try:
        report = loader.run()
    finally:
        loader.dispose()

    color_stats = report.table_stats["demo.Color"]
    assert color_stats.rows_matched_by_columns == 1  # Red matched
    assert color_stats.rows_inserted == 1  # Blue inserted


def test_match_by_columns_null_in_key_falls_through(tmp_path: Path) -> None:
    """Bag row with NULL in any match column is inserted, not matched."""
    bag = _build_image_widget_bag(tmp_path)

    img_csv = bag / "data" / "demo" / "Image.csv"
    with img_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["RID", "URL", "Filename"])
        w.writerow(["I-SRC-A", "", "a.png"])  # NULL URL
        w.writerow(["I-SRC-B", "/hatrac/demo/def.b.png", "b.png"])

    catalog = _mock_catalog()

    image_tw = _pb_table(catalog, "demo", "Image")
    image_tw.attributes.return_value.fetch.return_value = []
    _stub_insert_result(
        image_tw, [{"RID": "I-SRC-A"}, {"RID": "I-SRC-B"}]
    )

    widget_tw = _pb_table(catalog, "demo", "Widget")
    _stub_insert_result(widget_tw, [{"RID": "W1"}, {"RID": "W2"}])

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            match_by_columns={("demo", "Image"): ["URL"]},
        ),
        database_dir=tmp_path / "db",
    )
    try:
        report = loader.run()
    finally:
        loader.dispose()

    image_stats = report.table_stats["demo.Image"]
    assert image_stats.rows_matched_by_columns == 0
    assert image_stats.rows_inserted == 2

    # Both Image rows reached insert.
    image_rids = {r["RID"] for r in image_tw.insert.call_args.args[0]}
    assert image_rids == {"I-SRC-A", "I-SRC-B"}


def test_match_by_columns_policy_rejects_empty_column_list() -> None:
    """``match_by_columns[k] = []`` is a caller bug; validator rejects it."""
    with pytest.raises(ValueError, match="empty"):
        FKTraversalPolicy(
            match_by_columns={("demo", "Image"): []},
        )


def test_match_by_columns_rewrites_fks_on_inserted_rows(tmp_path: Path) -> None:
    """``match_by_columns`` insert path runs ``_rewrite_fks``.

    Regression: ``_load_match_by_columns_table`` originally
    inserted rows whose match-key didn't match without running
    them through ``_rewrite_fks``. That broke the common case
    where a child table itself uses ``match_by_columns`` and its
    parent (e.g. an asset row) was remapped by a previous step.
    The child's FK column would carry a source-RID that doesn't
    exist at the destination, failing the FK constraint with 409.

    Test setup: ``Image`` is deduped by URL (one match, one new);
    ``Widget`` is also deduped by ``match_by_columns`` (composite
    key with no matches → every row inserts). Widget's
    ``Image`` FK references the bag's source RID; after the
    asset row was matched, that FK must be rewritten to the
    destination RID before Widget is inserted.
    """
    bag = _build_image_widget_bag(tmp_path)
    catalog = _mock_catalog()

    image_tw = _pb_table(catalog, "demo", "Image")
    image_tw.attributes.return_value.fetch.return_value = [
        {"URL": "/hatrac/demo/abc.a.png", "RID": "I-DST-A"},
    ]
    _stub_insert_result(image_tw, [{"RID": "I-SRC-B"}])

    widget_tw = _pb_table(catalog, "demo", "Widget")
    widget_tw.attributes.return_value.fetch.return_value = []  # no existing
    _stub_insert_result(widget_tw, [{"RID": "W1"}, {"RID": "W2"}])

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            match_by_columns={
                ("demo", "Image"): ["URL"],
                # Widget composite key forces every row through the
                # insert path so we can assert the FK-rewrite ran.
                ("demo", "Widget"): ["Image", "RID"],
            },
        ),
        database_dir=tmp_path / "db",
    )
    try:
        loader.run()
    finally:
        loader.dispose()

    widget_rows = widget_tw.insert.call_args.args[0]
    by_rid = {r["RID"]: r for r in widget_rows}
    # W1 references the matched Image (I-SRC-A → I-DST-A). Without
    # the FK rewrite, W1.Image would still be I-SRC-A.
    assert by_rid["W1"]["Image"] == "I-DST-A", (
        "Widget.Image FK was not rewritten through the remap; "
        "match_by_columns insert path must call _rewrite_fks"
    )
    # W2's parent (I-SRC-B) was identity-remapped (no destination
    # match), so the FK stays as I-SRC-B.
    assert by_rid["W2"]["Image"] == "I-SRC-B"


def test_match_by_columns_rewrites_fks_before_match_query(tmp_path: Path) -> None:
    """``match_by_columns`` rewrites FKs *before* computing the match key.

    Regression: the original ``_load_match_by_columns_table`` ran
    the match query using the bag-side row's FK column values
    verbatim, then ran ``_rewrite_fks`` only on the unmatched
    rows that fell through to insert. That mis-ordering broke
    the common case where the match key includes an FK column
    whose target table was deduped on the same load:

    - Asset table deduped: ``Image.URL`` matches an existing
      destination row, so ``_rid_remap`` maps ``I-SRC-A → I-DST-A``.
    - Asset-type association deduped: match key is
      ``(Image, Asset_Type)``. The bag row carries
      ``(I-SRC-A, Type-X)``. The query asks "is there an existing
      row with ``Image=I-SRC-A``?" → no (the destination's row has
      ``Image=I-DST-A``). The bag row falls through to insert.
    - Insert: ``_rewrite_fks`` turns the FK into ``I-DST-A``, then
      ERMrest 409s on the **already-existing** composite key
      ``(I-DST-A, Type-X)``.

    Fix: ``_rewrite_fks`` runs at the **top** of the loop, so the
    match query asks the right question and the existing
    destination row is found in one shot.
    """
    bag = _build_image_widget_bag(tmp_path)
    catalog = _mock_catalog()

    image_tw = _pb_table(catalog, "demo", "Image")
    image_tw.attributes.return_value.fetch.return_value = [
        {"URL": "/hatrac/demo/abc.a.png", "RID": "I-DST-A"},
    ]
    _stub_insert_result(image_tw, [{"RID": "I-SRC-B"}])

    # Widget fetch: one existing row matches the *destination*
    # Image RID + Widget RID. Without the FK-rewrite-first ordering,
    # the query would be keyed by ``I-SRC-A`` (bag-side) and miss;
    # with the fix it's keyed by ``I-DST-A`` and finds the row.
    widget_tw = _pb_table(catalog, "demo", "Widget")
    widget_tw.attributes.return_value.fetch.return_value = [
        {"Image": "I-DST-A", "RID": "W1"},
    ]
    _stub_insert_result(widget_tw, [{"RID": "W2"}])

    loader = BagCatalogLoader(
        catalog=catalog,
        bag=bag,
        policy=FKTraversalPolicy(
            asset_mode=AssetMode.ROWS_ONLY,
            match_by_columns={
                ("demo", "Image"): ["URL"],
                ("demo", "Widget"): ["Image", "RID"],
            },
        ),
        database_dir=tmp_path / "db",
    )
    try:
        report = loader.run()
    finally:
        loader.dispose()

    widget_stats = report.table_stats["demo.Widget"]
    assert widget_stats.rows_matched_by_columns == 1, (
        f"W1 should have matched after FK rewrite; got "
        f"rows_matched_by_columns={widget_stats.rows_matched_by_columns}"
    )
    assert widget_stats.rows_inserted == 1, (
        f"W2 should have inserted; got "
        f"rows_inserted={widget_stats.rows_inserted}"
    )


# =============================================================================
# Composite-FK warning in _rewrite_fks (audit §3.1)
# =============================================================================
#
# ``_rewrite_fks`` can only rewrite single-column FKs (the source
# RID maps to one destination RID; composite-column FKs aren't
# representable in the remap shape). Pre-audit, composite FKs
# were silently skipped — fine for deriva-ml-shaped catalogs where
# every FK targets RID, but a footgun for general clone-via-bag
# users. The fix: log a one-shot warning per offending FK.


def test_rewrite_fks_warns_once_on_composite_fk(
    tmp_path: Path, caplog: Any
) -> None:
    """A composite FK is skipped but logs a one-shot warning.

    Two rows over the same composite FK should produce exactly
    one log entry (per ``(schema, table, fk_name)``), so a real
    clone with many rows over the same composite FK doesn't flood
    the log.
    """
    import logging

    bag = _build_fk_bag(tmp_path)
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )
    try:
        # Synthesize a composite-FK shape: a mock table with one
        # FK whose ``foreign_key_columns`` has two entries.
        col1 = MagicMock(name="Col1")
        col1.name = "K1"
        col2 = MagicMock(name="Col2")
        col2.name = "K2"
        composite_fk = MagicMock(name="CompositeFK")
        composite_fk.foreign_key_columns = [col1, col2]
        composite_fk.names = [("demo", "fk_composite")]
        pk_table = MagicMock(name="ParentTable")
        pk_table.schema.name = "demo"
        pk_table.name = "Parent"
        composite_fk.pk_table = pk_table

        child_table = MagicMock(name="ChildTable")
        child_table.schema.name = "demo"
        child_table.name = "Child"
        child_table.foreign_keys = [composite_fk]

        # Seed the remap so the function has something to do
        # (otherwise the early-return short-circuits before
        # iterating FKs).
        loader._rid_remap[("demo", "Other")] = {"X": "Y"}

        with caplog.at_level(logging.WARNING, logger="deriva.bag.catalog_loader"):
            _ = loader._rewrite_fks(child_table, {"K1": "a", "K2": "b"})
            _ = loader._rewrite_fks(child_table, {"K1": "c", "K2": "d"})

        composite_msgs = [
            r
            for r in caplog.records
            if "Composite FK" in r.getMessage()
        ]
        assert len(composite_msgs) == 1, (
            f"expected one composite-FK warning, got {len(composite_msgs)}; "
            f"messages: {[r.getMessage() for r in composite_msgs]}"
        )
    finally:
        loader.dispose()


def test_rewrite_fks_returns_row_unchanged_for_composite_fk(
    tmp_path: Path,
) -> None:
    """Composite FKs aren't rewritten; row passes through verbatim."""
    bag = _build_fk_bag(tmp_path)
    loader = BagCatalogLoader(
        catalog=_mock_catalog(),
        bag=bag,
        policy=FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY),
        database_dir=tmp_path / "db",
    )
    try:
        col1 = MagicMock(name="Col1")
        col1.name = "K1"
        col2 = MagicMock(name="Col2")
        col2.name = "K2"
        composite_fk = MagicMock(name="CompositeFK")
        composite_fk.foreign_key_columns = [col1, col2]
        composite_fk.names = [("demo", "fk_composite")]
        pk_table = MagicMock(name="ParentTable")
        pk_table.schema.name = "demo"
        pk_table.name = "Parent"
        composite_fk.pk_table = pk_table

        child_table = MagicMock(name="ChildTable")
        child_table.schema.name = "demo"
        child_table.name = "Child"
        child_table.foreign_keys = [composite_fk]
        # Remap a different table so the early-return doesn't fire.
        loader._rid_remap[("demo", "Other")] = {"X": "Y"}

        row = {"K1": "a", "K2": "b"}
        out = loader._rewrite_fks(child_table, row)
        assert out == row
        # The function returns a shallow copy; values must match
        # but the dict identity is allowed to differ.
        assert out is not row
    finally:
        loader.dispose()
