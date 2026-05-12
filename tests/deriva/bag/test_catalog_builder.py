"""Tests for :mod:`deriva.bag.catalog_builder`.

The hard work — driving :class:`GenericDownloader` and writing the
bag — needs a live catalog and is exercised by integration tests
on top of a real ERMrest server. The unit tests here focus on the
*spec-generation* and *FK-walk* logic, which doesn't need a
catalog connection: we mock the catalog/model objects and verify
the generated spec's shape, the reached-table set's contents, and
the anchor-validation hot paths.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from deriva.bag.anchors import RIDAnchor, TableAnchor
from deriva.bag.catalog_builder import CatalogBagBuilder
from deriva.bag.profile import BAGIT_PROFILE_IDENTIFIER
from deriva.bag.traversal import (
    AssetMode,
    FKTraversalPolicy,
    VocabExport,
)


# ---------------------------------------------------------------------------
# Mock catalog + model
# ---------------------------------------------------------------------------


def _make_mock_table(
    schema_name: str,
    table_name: str,
    *,
    is_asset: bool = False,
    is_vocabulary: bool = False,
    outbound_fks: list[Any] | None = None,
    inbound_fks: list[Any] | None = None,
) -> MagicMock:
    """Build a Mock that looks like deriva-py's Table."""
    table = MagicMock(name=f"Table[{schema_name}.{table_name}]")
    table.name = table_name
    table.schema = MagicMock(name=f"Schema[{schema_name}]")
    table.schema.name = schema_name
    table.is_asset.return_value = is_asset
    table.is_vocabulary.return_value = is_vocabulary
    table.foreign_keys = list(outbound_fks or [])
    table.referenced_by = list(inbound_fks or [])
    return table


def _make_mock_model(
    schemas: dict[str, dict[str, MagicMock]],
) -> MagicMock:
    """Wrap a schema→table mapping in a Model-shaped Mock."""
    model = MagicMock(name="Model")
    model.schemas = {}
    for schema_name, tables in schemas.items():
        schema = MagicMock(name=f"Schema[{schema_name}]")
        schema.name = schema_name
        schema.tables = tables
        for t in tables.values():
            t.schema = schema  # re-bind so .schema.name is right
        model.schemas[schema_name] = schema
    return model


def _make_mock_catalog(model: MagicMock) -> MagicMock:
    """Wrap a model in an ErmrestCatalog-shaped Mock."""
    catalog = MagicMock(name="ErmrestCatalog")
    catalog.getCatalogModel.return_value = model
    catalog.catalog_id = "42"
    catalog.deriva_server = MagicMock()
    catalog.deriva_server.scheme = "https"
    catalog.deriva_server.server = "example.org"
    return catalog


def _fk_mock(*, src_table: MagicMock, pk_table: MagicMock) -> MagicMock:
    """A foreign-key Mock with the attributes the walker uses."""
    fk = MagicMock(name="ForeignKey")
    fk.table = src_table
    fk.pk_table = pk_table
    return fk


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_catalog_bag_builder_requires_anchors(tmp_path: Path) -> None:
    catalog = MagicMock()
    with pytest.raises(ValueError, match="at least one anchor"):
        CatalogBagBuilder(
            catalog=catalog,
            anchors=[],
            output_dir=tmp_path,
        )


def test_catalog_bag_builder_creates_output_dir(tmp_path: Path) -> None:
    catalog = _make_mock_catalog(
        _make_mock_model({"demo": {"T": _make_mock_table("demo", "T")}})
    )
    out = tmp_path / "deeply" / "nested"
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="T")],
        output_dir=out,
    )
    assert out.is_dir()
    # No anchor validation happens at construction; check that
    # reached_tables is empty pre-build.
    assert cb.reached_tables == set()


# ---------------------------------------------------------------------------
# FK walk
# ---------------------------------------------------------------------------


def test_walk_reaches_outbound_fk(tmp_path: Path) -> None:
    """A single FK from Image → Subject pulls Subject into scope."""
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table(
        "demo",
        "Image",
        outbound_fks=[],  # filled in below after subject is built
    )
    image.foreign_keys = [_fk_mock(src_table=image, pk_table=subject)]
    subject.referenced_by = [_fk_mock(src_table=image, pk_table=subject)]
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Image")],
        output_dir=tmp_path,
    )
    cb._validate_anchors()  # no-op for TableAnchor
    cb._compute_reached_tables()
    assert cb.reached_tables == {
        ("demo", "Image"),
        ("demo", "Subject"),
    }


def test_walk_reaches_inbound_fk(tmp_path: Path) -> None:
    """A walk starting at Subject pulls Image inbound."""
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    fk = _fk_mock(src_table=image, pk_table=subject)
    image.foreign_keys = [fk]
    subject.referenced_by = [fk]
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._validate_anchors()
    cb._compute_reached_tables()
    assert cb.reached_tables == {
        ("demo", "Image"),
        ("demo", "Subject"),
    }


def test_vocab_table_does_not_propagate_inbound(tmp_path: Path) -> None:
    """Walking into a vocab table stops there.

    Even though many other tables reference Species, the walker
    must not pull them in just because the walk touched Species.
    This is the loop-back-prevention rule.
    """
    species = _make_mock_table(
        "demo", "Species", is_vocabulary=True
    )
    subject = _make_mock_table("demo", "Subject")
    other = _make_mock_table("demo", "Other")
    species_fk = _fk_mock(src_table=subject, pk_table=species)
    species_fk2 = _fk_mock(src_table=other, pk_table=species)
    subject.foreign_keys = [species_fk]
    species.referenced_by = [species_fk, species_fk2]
    # Other also references Species but is NOT reachable from
    # Subject by any other path; verify the walker doesn't reach
    # it via Species's inbound FKs.
    model = _make_mock_model(
        {
            "demo": {
                "Species": species,
                "Subject": subject,
                "Other": other,
            }
        }
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._validate_anchors()
    cb._compute_reached_tables()
    assert cb.reached_tables == {
        ("demo", "Subject"),
        ("demo", "Species"),
    }
    assert ("demo", "Other") not in cb.reached_tables


def test_walk_respects_max_depth(tmp_path: Path) -> None:
    """``max_depth=1`` stops after one FK hop from the anchor."""
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    c = _make_mock_table("demo", "C")
    fk_ab = _fk_mock(src_table=a, pk_table=b)
    fk_bc = _fk_mock(src_table=b, pk_table=c)
    a.foreign_keys = [fk_ab]
    b.referenced_by = [fk_ab]
    b.foreign_keys = [fk_bc]
    c.referenced_by = [fk_bc]
    model = _make_mock_model({"demo": {"A": a, "B": b, "C": c}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="A")],
        policy=FKTraversalPolicy(max_depth=1),
        output_dir=tmp_path,
    )
    cb._validate_anchors()
    cb._compute_reached_tables()
    # A and B reachable; C is two hops away.
    assert cb.reached_tables == {("demo", "A"), ("demo", "B")}


def test_walk_respects_exclude_tables(tmp_path: Path) -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    fk = _fk_mock(src_table=a, pk_table=b)
    a.foreign_keys = [fk]
    b.referenced_by = [fk]
    model = _make_mock_model({"demo": {"A": a, "B": b}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="A")],
        policy=FKTraversalPolicy(
            exclude_tables={("demo", "B")},
        ),
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()
    assert cb.reached_tables == {("demo", "A")}


def test_walk_respects_schema_allowlist(tmp_path: Path) -> None:
    """``schemas=`` restricts the walk to those schemas only."""
    a = _make_mock_table("inscope", "A")
    b = _make_mock_table("other", "B")
    fk = _fk_mock(src_table=a, pk_table=b)
    a.foreign_keys = [fk]
    b.referenced_by = [fk]
    model = _make_mock_model(
        {"inscope": {"A": a}, "other": {"B": b}}
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="A")],
        policy=FKTraversalPolicy(schemas={"inscope"}),
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()
    assert cb.reached_tables == {("inscope", "A")}


def test_walk_excludes_system_schemas(tmp_path: Path) -> None:
    """``public`` is always excluded regardless of policy."""
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("public", "Stuff")
    fk = _fk_mock(src_table=a, pk_table=b)
    a.foreign_keys = [fk]
    b.referenced_by = [fk]
    model = _make_mock_model(
        {"demo": {"A": a}, "public": {"Stuff": b}}
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="A")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()
    assert ("public", "Stuff") not in cb.reached_tables


# ---------------------------------------------------------------------------
# Spec generation
# ---------------------------------------------------------------------------


def test_spec_includes_schema_processor(tmp_path: Path) -> None:
    a = _make_mock_table("demo", "A")
    model = _make_mock_model({"demo": {"A": a}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="A")],
        output_dir=tmp_path,
    )
    spec = cb.get_export_spec()
    processors = spec["catalog"]["query_processors"]
    # First processor is the schema dump.
    schema_procs = [
        p for p in processors if p["processor"] == "json"
    ]
    assert len(schema_procs) >= 1
    assert schema_procs[0]["processor_params"]["query_path"] == "/schema"


def test_spec_stamps_profile_identifier(tmp_path: Path) -> None:
    a = _make_mock_table("demo", "A")
    model = _make_mock_model({"demo": {"A": a}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="A")],
        output_dir=tmp_path,
    )
    spec = cb.get_export_spec()
    bag_metadata = spec["bag"]["bag_metadata"]
    assert (
        bag_metadata["BagIt-Profile-Identifier"]
        == BAGIT_PROFILE_IDENTIFIER
    )


def test_spec_includes_csv_processor_for_each_reached_table(
    tmp_path: Path,
) -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    fk = _fk_mock(src_table=a, pk_table=b)
    a.foreign_keys = [fk]
    b.referenced_by = [fk]
    model = _make_mock_model({"demo": {"A": a, "B": b}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="A")],
        output_dir=tmp_path,
    )
    spec = cb.get_export_spec()
    csv_procs = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
    ]
    output_paths = {
        p["processor_params"]["output_path"] for p in csv_procs
    }
    assert "demo/A" in output_paths
    assert "demo/B" in output_paths


def test_spec_includes_fetch_processor_for_asset_table(
    tmp_path: Path,
) -> None:
    """Asset tables get an extra ``fetch`` processor."""
    image = _make_mock_table("demo", "Image", is_asset=True)
    model = _make_mock_model({"demo": {"Image": image}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Image")],
        output_dir=tmp_path,
    )
    spec = cb.get_export_spec()
    fetch_procs = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "fetch"
    ]
    assert len(fetch_procs) == 1


def test_spec_rid_anchor_filters_table(tmp_path: Path) -> None:
    """RIDAnchor restricts the CSV query to those RIDs."""
    s = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": s}})
    catalog = _make_mock_catalog(model)
    # We have to bypass the catalog-validation step because our
    # mock doesn't model getPathBuilder; call the spec generator
    # path with reached_tables pre-set.
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Subject", rids=["S1", "S2"])],
        output_dir=tmp_path,
    )
    cb._reached_tables = {("demo", "Subject")}
    cb._model = model
    spec = cb._build_export_spec()
    csv_proc = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
    ][0]
    qpath = csv_proc["processor_params"]["query_path"]
    assert "any(S1,S2)" in qpath


def test_spec_rid_anchor_chains_path_to_fk_reachable_tables(
    tmp_path: Path,
) -> None:
    """Non-anchor tables reached via FK chain their path from the anchor.

    Without this scoping, a single-RID anchor pulls every row of
    every reached table — which produces dangling FKs when the
    reached rows reference rows outside the anchor's slice.
    The chained path delegates the join to ERMrest, which handles
    natural-FK joins between segments.

    Topology: ``Dataset → Dataset_Version`` (Dataset_Version has an
    FK to Dataset). Anchoring at a single Dataset RID, the
    Dataset_Version query should chain through Dataset so only
    Dataset_Versions referencing the anchored Dataset land in the
    bag.
    """
    ds = _make_mock_table("demo", "Dataset")
    dv = _make_mock_table("demo", "Dataset_Version")
    # Dataset_Version → Dataset FK. From Dataset's perspective this
    # is inbound (Dataset.referenced_by).
    fk = _fk_mock(src_table=dv, pk_table=ds)
    ds.referenced_by = [fk]
    dv.foreign_keys = [fk]

    model = _make_mock_model(
        {"demo": {"Dataset": ds, "Dataset_Version": dv}}
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Dataset", rids=["5HE"])],
        output_dir=tmp_path,
    )
    cb._validate_anchors = lambda: None  # bypass catalog HEAD
    cb._compute_reached_tables()
    spec = cb._build_export_spec()

    by_output = {
        p["processor_params"]["output_path"]: p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
    }
    # The anchored Dataset gets the standard RID filter.
    assert (
        by_output["demo/Dataset"]["processor_params"]["query_path"]
        == "/entity/demo:Dataset/RID=any(5HE)"
    )
    # Dataset_Version chains through Dataset, restricting to rows
    # that reference the anchored Dataset RID.
    assert (
        by_output["demo/Dataset_Version"]["processor_params"][
            "query_path"
        ]
        == "/entity/demo:Dataset/RID=any(5HE)/demo:Dataset_Version"
    )


def test_spec_rid_anchor_chains_through_intermediate_table(
    tmp_path: Path,
) -> None:
    """A three-table chain produces a three-segment ERMrest path.

    Topology: ``Dataset → Dataset_Image → Image``. Each step is an
    FK. Anchoring at a Dataset RID, the Image query should chain
    through Dataset_Image, so only Images that are members of the
    anchored Dataset (via Dataset_Image) land in the bag.
    """
    ds = _make_mock_table("demo", "Dataset")
    di = _make_mock_table("demo", "Dataset_Image")
    img = _make_mock_table("demo", "Image")
    di_to_ds = _fk_mock(src_table=di, pk_table=ds)
    di_to_img = _fk_mock(src_table=di, pk_table=img)
    ds.referenced_by = [di_to_ds]
    di.foreign_keys = [di_to_ds, di_to_img]
    img.referenced_by = [di_to_img]

    model = _make_mock_model(
        {
            "demo": {
                "Dataset": ds,
                "Dataset_Image": di,
                "Image": img,
            }
        }
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Dataset", rids=["5HE"])],
        output_dir=tmp_path,
    )
    cb._validate_anchors = lambda: None
    cb._compute_reached_tables()
    spec = cb._build_export_spec()

    by_output = {
        p["processor_params"]["output_path"]: p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
    }
    # BFS: Dataset → Dataset_Image → Image.
    assert (
        by_output["demo/Image"]["processor_params"]["query_path"]
        == "/entity/demo:Dataset/RID=any(5HE)/demo:Dataset_Image/demo:Image"
    )


def test_spec_table_anchor_no_filter(tmp_path: Path) -> None:
    """TableAnchor: query path is unfiltered ``/entity/...``."""
    s = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": s}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._reached_tables = {("demo", "Subject")}
    cb._model = model
    spec = cb._build_export_spec()
    csv_proc = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
    ][0]
    assert (
        csv_proc["processor_params"]["query_path"]
        == "/entity/demo:Subject"
    )


def test_spec_vocab_full_export_uses_unfiltered_query(
    tmp_path: Path,
) -> None:
    """``vocab_export=FULL``: one unfiltered processor for the vocab.

    Previously the builder emitted **two** processors with the same
    ``output_path`` (FK-bounded + unfiltered), which the export
    engine resolved unpredictably. The fix: one processor per
    vocab, switched on the policy — ``FULL`` → ``/entity/{vocab}``,
    ``REFERENCED_ONLY`` → FK-chained path.
    """
    species = _make_mock_table(
        "demo", "Species", is_vocabulary=True
    )
    model = _make_mock_model({"demo": {"Species": species}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Species")],
        policy=FKTraversalPolicy(vocab_export=VocabExport.FULL),
        output_dir=tmp_path,
    )
    cb._reached_tables = {("demo", "Species")}
    cb._model = model
    spec = cb._build_export_spec()
    csv_procs = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
        and p["processor_params"]["output_path"] == "demo/Species"
    ]
    assert len(csv_procs) == 1
    assert (
        csv_procs[0]["processor_params"]["query_path"]
        == "/entity/demo:Species"
    )


def test_spec_vocab_referenced_only_emits_single_processor(
    tmp_path: Path,
) -> None:
    """Default ``vocab_export=REFERENCED_ONLY`` emits one processor per vocab."""
    species = _make_mock_table(
        "demo", "Species", is_vocabulary=True
    )
    model = _make_mock_model({"demo": {"Species": species}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Species")],
        output_dir=tmp_path,
    )
    cb._reached_tables = {("demo", "Species")}
    cb._model = model
    spec = cb._build_export_spec()
    csv_procs = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
    ]
    output_paths = [
        p["processor_params"]["output_path"] for p in csv_procs
    ]
    assert output_paths.count("demo/Species") == 1


# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------


def test_resolve_table_finds_in_single_schema(tmp_path: Path) -> None:
    a = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": a}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    assert cb._resolve_table(model, "Subject") == ("demo", "Subject")


def test_resolve_table_rejects_missing(tmp_path: Path) -> None:
    a = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": a}})
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    with pytest.raises(KeyError):
        cb._resolve_table(model, "Nope")


def test_resolve_table_rejects_ambiguous(tmp_path: Path) -> None:
    """Bare 'Subject' in two scoped schemas → caller must qualify."""
    a = _make_mock_table("demo", "Subject")
    b = _make_mock_table("other", "Subject")
    model = _make_mock_model(
        {"demo": {"Subject": a}, "other": {"Subject": b}}
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="demo.Subject")],
        output_dir=tmp_path,
    )
    with pytest.raises(ValueError, match="Ambiguous"):
        cb._resolve_table(model, "Subject")


def test_resolve_table_accepts_qualified_name(tmp_path: Path) -> None:
    a = _make_mock_table("demo", "Subject")
    b = _make_mock_table("other", "Subject")
    model = _make_mock_model(
        {"demo": {"Subject": a}, "other": {"Subject": b}}
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="demo.Subject")],
        output_dir=tmp_path,
    )
    assert cb._resolve_table(model, "demo.Subject") == ("demo", "Subject")
    assert cb._resolve_table(model, "other.Subject") == ("other", "Subject")
