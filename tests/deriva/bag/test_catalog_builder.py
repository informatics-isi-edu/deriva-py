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
    # Anchor table A: direct ``{schema}/{table}`` dest.
    # Non-anchor table B (reached via FK from A): per-route dest
    # ``{schema}/{intermediate-chain}/{table}``. The chain is the
    # path's tables minus the terminal, joined by ``_``.
    assert "demo/A" in output_paths
    assert "demo/A/B" in output_paths


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
    # that reference the anchored Dataset RID. Per-route dest:
    # ``{schema}/{intermediate-chain}/{table}`` — intermediate chain
    # is just "Dataset" since the path is Dataset → Dataset_Version.
    assert (
        by_output["demo/Dataset/Dataset_Version"][
            "processor_params"
        ]["query_path"]
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
    # BFS: Dataset → Dataset_Image → Image. Per-route dest path:
    # ``{schema}/{intermediate-chain}/{table}`` with the chain
    # ``Dataset_Dataset_Image`` (the path's tables minus the
    # terminal, joined by ``_``).
    assert (
        by_output["demo/Dataset_Dataset_Image/Image"][
            "processor_params"
        ]["query_path"]
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


def test_spec_multipath_emits_one_processor_per_fk_route(
    tmp_path: Path,
) -> None:
    """A target table reachable via two FK routes gets two csv processors.

    Topology::

        Dataset ─── Dataset_Image ──┐
            │                       │
            └── Dataset_Subject ─── Subject ─── Subject_Image ─── Image

    From a Dataset anchor, ``Image`` is reachable two ways:

    1. Dataset → Dataset_Image → Image (short route).
    2. Dataset → Dataset_Subject → Subject → Subject_Image → Image
       (long route).

    Only one of these routes carries rows in any given catalog
    (Dataset_Image vs Subject_Image association tables are
    mutually exclusive deployments in practice), but the builder
    can't tell up front. Emitting both lets the loader union
    whatever rows each path produces, with RID dedup at insert
    time. The BFS-shortest-only approach silently drops rows
    that live only on the long route.
    """
    ds = _make_mock_table("demo", "Dataset")
    di = _make_mock_table("demo", "Dataset_Image")
    ds_sub = _make_mock_table("demo", "Dataset_Subject")
    sub = _make_mock_table("demo", "Subject")
    si = _make_mock_table("demo", "Subject_Image")
    img = _make_mock_table("demo", "Image")

    di_to_ds = _fk_mock(src_table=di, pk_table=ds)
    di_to_img = _fk_mock(src_table=di, pk_table=img)
    dssub_to_ds = _fk_mock(src_table=ds_sub, pk_table=ds)
    dssub_to_sub = _fk_mock(src_table=ds_sub, pk_table=sub)
    si_to_sub = _fk_mock(src_table=si, pk_table=sub)
    si_to_img = _fk_mock(src_table=si, pk_table=img)

    ds.referenced_by = [di_to_ds, dssub_to_ds]
    di.foreign_keys = [di_to_ds, di_to_img]
    ds_sub.foreign_keys = [dssub_to_ds, dssub_to_sub]
    sub.referenced_by = [dssub_to_sub, si_to_sub]
    si.foreign_keys = [si_to_sub, si_to_img]
    img.referenced_by = [di_to_img, si_to_img]

    model = _make_mock_model(
        {
            "demo": {
                "Dataset": ds,
                "Dataset_Image": di,
                "Dataset_Subject": ds_sub,
                "Subject": sub,
                "Subject_Image": si,
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

    # Count csv processors whose ``output_path`` ends in ``/Image``.
    # Two FK routes → two processors, each at a distinct on-disk
    # location.
    image_procs = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
        and p["processor_params"]["output_path"].endswith("/Image")
    ]
    assert len(image_procs) == 2, [
        p["processor_params"]["output_path"] for p in image_procs
    ]
    query_paths = {
        p["processor_params"]["query_path"] for p in image_procs
    }
    # Short route.
    assert (
        "/entity/demo:Dataset/RID=any(5HE)/demo:Dataset_Image/demo:Image"
        in query_paths
    )
    # Long route through Subject.
    assert (
        "/entity/demo:Dataset/RID=any(5HE)"
        "/demo:Dataset_Subject/demo:Subject"
        "/demo:Subject_Image/demo:Image"
        in query_paths
    )

    # Asset fetch processors are not per-path: one ``fetch`` per
    # asset table is sufficient because each fetch addresses by
    # asset RID and any RID landed by either CSV processor is
    # fetchable.
    fetch_procs = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "fetch"
    ]
    image_fetches = [
        p
        for p in fetch_procs
        if "Image" in p["processor_params"]["output_path"]
    ]
    # (Image isn't marked is_asset in this fixture so 0 is expected;
    # the assertion is that we don't accidentally emit per-path
    # fetch processors.)
    assert len(image_fetches) == 0


def test_terminal_table_blocks_inbound_but_follows_outbound(
    tmp_path: Path,
) -> None:
    """A table in ``policy.terminal_tables`` is asymmetric:
    OUTBOUND FKs (refs the table declares) are followed so the
    rows it references land in the slice; INBOUND FKs (refs
    declared at the table by others) are blocked so the slice
    doesn't aggregate cross-anchor state.

    Topology::

        Workflow ────────────┐
                             │  (Execution.Workflow FK)
        Subject ─── Subject_Health ─── Execution ─── Image_Quality ─── Image

    Subject anchor with Execution terminal:
    - Subject, Subject_Health, Execution reached (Subject's
      provenance lands).
    - Workflow reached too — outbound from Execution: an
      Execution row's Workflow FK must resolve at load.
    - Image_Quality and Image NOT reached — inbound from
      Execution would over-fetch Quality rows belonging to
      Executions of other Subjects.
    """
    sub = _make_mock_table("demo", "Subject")
    sh = _make_mock_table("demo", "Subject_Health")
    exe = _make_mock_table("demo", "Execution")
    wf = _make_mock_table("demo", "Workflow")
    iq = _make_mock_table("demo", "Image_Quality")
    img = _make_mock_table("demo", "Image")

    sh_to_sub = _fk_mock(src_table=sh, pk_table=sub)
    sh_to_exe = _fk_mock(src_table=sh, pk_table=exe)
    exe_to_wf = _fk_mock(src_table=exe, pk_table=wf)
    iq_to_exe = _fk_mock(src_table=iq, pk_table=exe)
    iq_to_img = _fk_mock(src_table=iq, pk_table=img)

    sub.referenced_by = [sh_to_sub]
    sh.foreign_keys = [sh_to_sub, sh_to_exe]
    exe.foreign_keys = [exe_to_wf]
    exe.referenced_by = [sh_to_exe, iq_to_exe]
    wf.referenced_by = [exe_to_wf]
    iq.foreign_keys = [iq_to_exe, iq_to_img]
    img.referenced_by = [iq_to_img]

    model = _make_mock_model(
        {
            "demo": {
                "Subject": sub,
                "Subject_Health": sh,
                "Execution": exe,
                "Workflow": wf,
                "Image_Quality": iq,
                "Image": img,
            }
        }
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Subject", rids=["4AE"])],
        output_dir=tmp_path,
        policy=FKTraversalPolicy(
            terminal_tables={("demo", "Execution")},
        ),
    )
    cb._validate_anchors = lambda: None
    cb._compute_reached_tables()

    reached = {f"{s}.{t}" for s, t in cb._reached_tables}
    # The terminal table itself IS in the slice (provenance row
    # for the Subject's health record).
    assert "demo.Subject" in reached
    assert "demo.Subject_Health" in reached
    assert "demo.Execution" in reached
    # OUTBOUND from Execution still followed — Execution.Workflow
    # FK must resolve, so Workflow lands in the slice.
    assert "demo.Workflow" in reached, reached
    # INBOUND to Execution blocked — Image_Quality and Image
    # are reachable only via inbound FKs from Execution, so they
    # stay out of the slice.
    assert "demo.Image_Quality" not in reached, reached
    assert "demo.Image" not in reached, reached


def test_terminal_table_query_path_terminates_at_table(
    tmp_path: Path,
) -> None:
    """The CSV processor for a terminal table ends *at* the table.

    Verifies the export-spec side: the query path for a terminal
    table is anchored-and-joined-to it, not joined-through it.
    The bag's Execution.csv will carry rows reachable from the
    Subject anchor via the Subject_Health → Execution path,
    nothing further.
    """
    sub = _make_mock_table("demo", "Subject")
    sh = _make_mock_table("demo", "Subject_Health")
    exe = _make_mock_table("demo", "Execution")

    sh_to_sub = _fk_mock(src_table=sh, pk_table=sub)
    sh_to_exe = _fk_mock(src_table=sh, pk_table=exe)
    sub.referenced_by = [sh_to_sub]
    sh.foreign_keys = [sh_to_sub, sh_to_exe]
    exe.referenced_by = [sh_to_exe]

    model = _make_mock_model(
        {
            "demo": {
                "Subject": sub,
                "Subject_Health": sh,
                "Execution": exe,
            }
        }
    )
    catalog = _make_mock_catalog(model)
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Subject", rids=["4AE"])],
        output_dir=tmp_path,
        policy=FKTraversalPolicy(
            terminal_tables={("demo", "Execution")},
        ),
    )
    cb._validate_anchors = lambda: None
    cb._compute_reached_tables()
    spec = cb._build_export_spec()

    exe_procs = [
        p
        for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
        and p["processor_params"]["output_path"].endswith("/Execution")
    ]
    assert len(exe_procs) >= 1
    # Each Execution processor ends at ``demo:Execution`` — no
    # join continues past it.
    for p in exe_procs:
        qpath = p["processor_params"]["query_path"]
        assert qpath.endswith("/demo:Execution"), qpath


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


# =============================================================================
# _validate_anchors — uses the datapath ``.in_()`` operator (deriva-py #242)
# =============================================================================
#
# Before deriva-py #242 the ``_validate_anchors`` impl rolled its
# own ``?RID=any(...)`` URL because the datapath ``_ColumnWrapper``
# had no ``.in_()``. PR #242 added the operator; this validation
# path was migrated to use it as part of the bag-audit cleanup.


def test_validate_anchors_uses_path_builder_in_for_rid_anchors(
    tmp_path: Path,
) -> None:
    """``_validate_anchors`` runs one ``.in_()`` query per RIDAnchor.

    Mocks ``catalog.getPathBuilder()`` to capture which RIDs were
    asked for; verifies the query returned them all so validation
    passes. Also verifies that the raw ``catalog.get`` path is no
    longer used (would have raised ``AssertionError`` if a raw
    URL hit the mock).
    """
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)

    # Catalog with a configured path builder. ``getPathBuilder()``
    # returns a mock pb whose ``schemas[s].tables[t]`` returns a
    # table-path mock that supports ``.RID.in_(rids)`` and
    # ``.filter(...).attributes(...).fetch()``.
    captured_rids: list[str] = []

    def _make_table_path_mock(returned_rids: list[str]) -> MagicMock:
        table_path = MagicMock(name="TablePath")

        def _in_call(rids: list[str]) -> MagicMock:
            captured_rids.extend(rids)
            return MagicMock(name="InPredicate")

        rid_col = MagicMock(name="RidColumn")
        rid_col.in_.side_effect = _in_call
        table_path.RID = rid_col

        filtered = MagicMock(name="Filtered")
        attributed = MagicMock(name="Attributed")
        attributed.fetch.return_value = [
            {"RID": rid} for rid in returned_rids
        ]
        filtered.attributes.return_value = attributed
        table_path.filter.return_value = filtered
        return table_path

    pb = MagicMock(name="PathBuilder")
    pb.schemas = {
        "demo": MagicMock(
            tables={
                "Subject": _make_table_path_mock(["S1", "S2"]),
            }
        )
    }
    catalog.getPathBuilder.return_value = pb

    # Make catalog.get raise so we catch any regression that goes
    # back to the raw URL path.
    catalog.get.side_effect = AssertionError(
        "_validate_anchors should not call catalog.get directly"
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Subject", rids=["S1", "S2"])],
        output_dir=tmp_path,
    )
    cb._validate_anchors()  # No raise — both RIDs were returned.

    assert captured_rids == ["S1", "S2"]


def test_validate_anchors_raises_on_missing_rids(tmp_path: Path) -> None:
    """A RID the catalog doesn't return is reported as missing."""
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)

    table_path = MagicMock(name="TablePath")
    rid_col = MagicMock(name="RidColumn")
    rid_col.in_.return_value = MagicMock(name="InPredicate")
    table_path.RID = rid_col
    filtered = MagicMock(name="Filtered")
    attributed = MagicMock(name="Attributed")
    # Only S1 exists; S2 is missing.
    attributed.fetch.return_value = [{"RID": "S1"}]
    filtered.attributes.return_value = attributed
    table_path.filter.return_value = filtered

    pb = MagicMock(name="PathBuilder")
    pb.schemas = {
        "demo": MagicMock(tables={"Subject": table_path}),
    }
    catalog.getPathBuilder.return_value = pb

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Subject", rids=["S1", "S2"])],
        output_dir=tmp_path,
    )
    with pytest.raises(ValueError, match="not present in the source catalog"):
        cb._validate_anchors()


# ---------------------------------------------------------------------------
# iter_table_datapaths — read-only sibling of build()
# ---------------------------------------------------------------------------
#
# Powers deriva-ml's ``Dataset.is_dirty()`` and ``estimate_bag_size``
# after the dataset-bag cutover. The walk is the same one
# :meth:`build` uses; the difference is the output (live datapaths
# vs. an on-disk bag). See docs/design/dataset-bag-cutover-2026-05.md
# in deriva-ml for the full motivation.


def _make_table_path_for_datapath(
    schema_name: str,
    table_name: str,
    *,
    captured_filters: list[Any] | None = None,
    captured_links: list[Any] | None = None,
    captured_link_kwargs: list[dict[str, Any]] | None = None,
) -> MagicMock:
    """Build a pathBuilder-table mock for datapath construction.

    Captures :meth:`filter` predicates, :meth:`link` targets, and
    ``on=`` kwargs into the provided lists so tests can assert on
    them. ``link`` returns a fresh mock that itself supports
    further ``.link()`` calls so multi-segment chains work.
    """
    path = MagicMock(name=f"PB[{schema_name}.{table_name}]")
    path.__pb_schema_name__ = schema_name
    path.__pb_table_name__ = table_name

    def _filter(predicate: Any) -> MagicMock:
        if captured_filters is not None:
            captured_filters.append(predicate)
        # Return a fresh mock so subsequent .link() calls compose
        # on the filtered path rather than the bare table.
        filtered = MagicMock(name=f"PB[{schema_name}.{table_name}].filtered")

        def _filtered_link(target: MagicMock, **kwargs: Any) -> MagicMock:
            if captured_links is not None:
                captured_links.append(target)
            if captured_link_kwargs is not None:
                captured_link_kwargs.append(kwargs)
            return _filtered_link_target()

        filtered.link.side_effect = _filtered_link
        return filtered

    path.filter.side_effect = _filter

    def _filtered_link_target() -> MagicMock:
        # Each link returns an intermediate that itself supports
        # .link(), so a chain of N links composes correctly.
        nxt = MagicMock(name=f"linked-from[{schema_name}.{table_name}]")

        def _link_again(target: MagicMock, **kwargs: Any) -> MagicMock:
            if captured_links is not None:
                captured_links.append(target)
            if captured_link_kwargs is not None:
                captured_link_kwargs.append(kwargs)
            return _filtered_link_target()

        nxt.link.side_effect = _link_again
        return nxt

    def _direct_link(target: MagicMock, **kwargs: Any) -> MagicMock:
        if captured_links is not None:
            captured_links.append(target)
        if captured_link_kwargs is not None:
            captured_link_kwargs.append(kwargs)
        return _filtered_link_target()

    path.link.side_effect = _direct_link

    # RID column with an .in_() that produces a predicate mock.
    rid = MagicMock(name=f"PB[{schema_name}.{table_name}].RID")
    rid.in_.return_value = MagicMock(name="InPredicate")
    path.RID = rid
    return path


def _make_pathbuilder(
    table_paths: dict[tuple[str, str], MagicMock],
) -> MagicMock:
    """Wrap a ``{(schema, table): pb_table_mock}`` map in a PathBuilder."""
    pb = MagicMock(name="PathBuilder")
    schemas: dict[str, MagicMock] = {}
    for (schema_name, table_name), tp in table_paths.items():
        sm = schemas.setdefault(
            schema_name,
            MagicMock(name=f"Schema[{schema_name}]", tables={}),
        )
        sm.tables[table_name] = tp
    pb.schemas = schemas
    return pb


def test_iter_table_datapaths_returns_one_entry_per_reached_table(
    tmp_path: Path,
) -> None:
    """Each reached table gets at least one entry in the result."""
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    fk = _fk_mock(src_table=image, pk_table=subject)
    image.foreign_keys = [fk]
    subject.referenced_by = [fk]
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    catalog = _make_mock_catalog(model)

    pb_subject = _make_table_path_for_datapath("demo", "Subject")
    pb_image = _make_table_path_for_datapath("demo", "Image")
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {
            ("demo", "Subject"): pb_subject,
            ("demo", "Image"): pb_image,
        }
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()

    out = cb.iter_table_datapaths()

    assert set(out.keys()) == {("demo", "Subject"), ("demo", "Image")}
    for entries in out.values():
        assert len(entries) >= 1
        for dp, pb_table, is_asset in entries:
            assert dp is not None
            assert pb_table is not None
            assert isinstance(is_asset, bool)


def test_iter_table_datapaths_marks_asset_tables_correctly(
    tmp_path: Path,
) -> None:
    """``is_asset`` reflects :meth:`Table.is_asset` on the terminal."""
    image = _make_mock_table("demo", "Image", is_asset=True)
    subject = _make_mock_table("demo", "Subject")
    fk = _fk_mock(src_table=image, pk_table=subject)
    image.foreign_keys = [fk]
    subject.referenced_by = [fk]
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    catalog = _make_mock_catalog(model)
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {
            ("demo", "Subject"): _make_table_path_for_datapath(
                "demo", "Subject"
            ),
            ("demo", "Image"): _make_table_path_for_datapath(
                "demo", "Image"
            ),
        }
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()
    out = cb.iter_table_datapaths()

    # Image is an asset table; Subject is not.
    image_entries = out[("demo", "Image")]
    subject_entries = out[("demo", "Subject")]
    assert all(is_asset for _dp, _pb, is_asset in image_entries)
    assert all(not is_asset for _dp, _pb, is_asset in subject_entries)


def test_iter_table_datapaths_applies_rid_anchor_filter_at_root(
    tmp_path: Path,
) -> None:
    """A ``RIDAnchor`` produces a ``filter(RID.in_(rids))`` predicate."""
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)

    captured_filters: list[Any] = []
    pb_subject = _make_table_path_for_datapath(
        "demo", "Subject", captured_filters=captured_filters
    )
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {("demo", "Subject"): pb_subject}
    )

    # Stub validation so we don't need the full ``.in_().fetch()``
    # round-trip on the anchor check.
    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[RIDAnchor(table="Subject", rids=["S1", "S2", "S3"])],
        output_dir=tmp_path,
    )
    cb._reached_tables = {("demo", "Subject")}
    cb._anchor_tables = {("demo", "Subject")}
    cb._table_path_set = {("demo", "Subject"): [(("demo", "Subject"),)]}

    cb.iter_table_datapaths()

    # The anchor filter ran: one filter() call on the root pb.
    pb_subject.filter.assert_called_once()
    # The RID column's .in_() was called with the anchor RIDs.
    pb_subject.RID.in_.assert_called_once_with(["S1", "S2", "S3"])


def test_iter_table_datapaths_no_filter_for_table_anchor(
    tmp_path: Path,
) -> None:
    """A ``TableAnchor`` leaves the root unfiltered."""
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)

    captured_filters: list[Any] = []
    pb_subject = _make_table_path_for_datapath(
        "demo", "Subject", captured_filters=captured_filters
    )
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {("demo", "Subject"): pb_subject}
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()

    cb.iter_table_datapaths()

    # No anchor filter — the root pb.filter was never called.
    pb_subject.filter.assert_not_called()


def test_iter_table_datapaths_chains_link_for_fk_reachable_tables(
    tmp_path: Path,
) -> None:
    """Tables reached via FK get ``link(...)`` chained from the root."""
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    fk = _fk_mock(src_table=image, pk_table=subject)
    image.foreign_keys = [fk]
    subject.referenced_by = [fk]
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    catalog = _make_mock_catalog(model)

    captured_links: list[Any] = []
    captured_kwargs: list[dict[str, Any]] = []
    pb_subject = _make_table_path_for_datapath(
        "demo", "Subject",
        captured_links=captured_links,
        captured_link_kwargs=captured_kwargs,
    )
    pb_image = _make_table_path_for_datapath("demo", "Image")
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {
            ("demo", "Subject"): pb_subject,
            ("demo", "Image"): pb_image,
        }
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()

    cb.iter_table_datapaths()

    # The Image entry's datapath should have a .link() called
    # (chained from Subject's pb). Simple FK → no on= clause.
    # Two reached tables (Subject, Image); only Image's entry
    # chains a link. Subject is the root — no link.
    assert pb_image in captured_links
    # All link calls for simple FKs have no on= kwarg (datapath
    # resolves implicitly).
    for kw in captured_kwargs:
        assert "on" not in kw or kw["on"] is None


def test_iter_table_datapaths_emits_one_entry_per_fk_path(
    tmp_path: Path,
) -> None:
    """When a table is reached via multiple FK paths, each gets an entry.

    Pre-populates ``_table_path_set`` with two distinct paths to the
    same target, mirroring what the walker would produce for a
    multi-FK-route case.
    """
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    fk = _fk_mock(src_table=image, pk_table=subject)
    image.foreign_keys = [fk]
    subject.referenced_by = [fk]
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    catalog = _make_mock_catalog(model)

    pb_subject = _make_table_path_for_datapath("demo", "Subject")
    pb_image = _make_table_path_for_datapath("demo", "Image")
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {
            ("demo", "Subject"): pb_subject,
            ("demo", "Image"): pb_image,
        }
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._reached_tables = {("demo", "Subject"), ("demo", "Image")}
    cb._anchor_tables = {("demo", "Subject")}
    # Two distinct routes to Image — direct, and a hypothetical
    # via a self-loop on Subject. The shape of the routes doesn't
    # matter for this test; what matters is that the result has
    # two entries for Image.
    cb._table_path_set = {
        ("demo", "Subject"): [(("demo", "Subject"),)],
        ("demo", "Image"): [
            (("demo", "Subject"), ("demo", "Image")),
            (
                ("demo", "Subject"),
                ("demo", "Subject"),
                ("demo", "Image"),
            ),
        ],
    }

    out = cb.iter_table_datapaths()

    # Image has two routes → two entries; Subject has one.
    assert len(out[("demo", "Image")]) == 2
    assert len(out[("demo", "Subject")]) == 1


def test_iter_table_datapaths_uses_composite_on_clause(
    tmp_path: Path,
) -> None:
    """A composite (multi-column) FK gets an explicit ``on=`` clause."""
    parent = _make_mock_table("demo", "Parent")
    child = _make_mock_table("demo", "Child")
    # Composite FK: (Child.PA, Child.PB) → (Parent.A, Parent.B).
    fk_col_a = MagicMock(name="Child.PA")
    fk_col_a.name = "PA"
    fk_col_a.table = child
    fk_col_b = MagicMock(name="Child.PB")
    fk_col_b.name = "PB"
    fk_col_b.table = child
    pk_col_a = MagicMock(name="Parent.A")
    pk_col_a.name = "A"
    pk_col_a.table = parent
    pk_col_b = MagicMock(name="Parent.B")
    pk_col_b.name = "B"
    pk_col_b.table = parent

    fk = _fk_mock(src_table=child, pk_table=parent)
    fk.foreign_key_columns = [fk_col_a, fk_col_b]
    fk.referenced_columns = [pk_col_a, pk_col_b]
    child.foreign_keys = [fk]
    parent.referenced_by = [fk]

    model = _make_mock_model(
        {"demo": {"Parent": parent, "Child": child}}
    )
    catalog = _make_mock_catalog(model)

    captured_kwargs: list[dict[str, Any]] = []
    pb_parent = _make_table_path_for_datapath(
        "demo", "Parent", captured_link_kwargs=captured_kwargs
    )
    pb_child = _make_table_path_for_datapath("demo", "Child")
    # Configure the pb columns the composite-on logic will access.
    pb_parent.A = MagicMock(name="pb.Parent.A")
    pb_parent.B = MagicMock(name="pb.Parent.B")
    pb_child.PA = MagicMock(name="pb.Child.PA")
    pb_child.PB = MagicMock(name="pb.Child.PB")
    # Make the equality predicate composable with ``&``.
    pb_parent.A.__eq__.return_value = MagicMock(name="A==PA")
    pb_parent.B.__eq__.return_value = MagicMock(name="B==PB")
    pb_parent.A.__eq__.return_value.__and__.return_value = MagicMock(
        name="A==PA & B==PB"
    )

    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {
            ("demo", "Parent"): pb_parent,
            ("demo", "Child"): pb_child,
        }
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Parent")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()

    cb.iter_table_datapaths()

    # The link() from Parent to Child should have an on= kwarg.
    on_kwargs = [kw for kw in captured_kwargs if "on" in kw]
    assert on_kwargs, (
        "expected at least one link() call to receive on= for the "
        "composite FK between Parent and Child"
    )


def test_iter_table_datapaths_runs_walk_lazily(
    tmp_path: Path,
) -> None:
    """First call triggers the walk; reached_tables is populated after."""
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {("demo", "Subject"): _make_table_path_for_datapath(
            "demo", "Subject"
        )}
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    # Pre-walk: nothing reached yet.
    assert cb.reached_tables == set()

    cb.iter_table_datapaths()

    # Walk ran.
    assert ("demo", "Subject") in cb.reached_tables


# ---------------------------------------------------------------------------
# iter_reached_paths — symbolic-path sibling of iter_table_datapaths
# ---------------------------------------------------------------------------
#
# Powers deriva-ml's static Chaise export annotations. The walk is the
# same one :meth:`build` uses; what differs is the output form — symbolic
# ``(schema, table)`` path tuples rather than live datapath objects.
# See docs/design/dataset-bag-cutover-2026-05.md in deriva-ml.


def test_iter_reached_paths_returns_paths_for_every_reached_table(
    tmp_path: Path,
) -> None:
    """Every entry in ``reached_tables`` gets at least one path."""
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
    cb._compute_reached_tables()

    paths = cb.iter_reached_paths()

    assert set(paths.keys()) == {("demo", "Subject"), ("demo", "Image")}
    for fk_paths in paths.values():
        assert len(fk_paths) >= 1
        for path in fk_paths:
            assert isinstance(path, tuple)
            assert all(
                isinstance(seg, tuple) and len(seg) == 2
                for seg in path
            )


def test_iter_reached_paths_anchor_table_is_single_segment(
    tmp_path: Path,
) -> None:
    """The anchor table's path is a single ``(schema, table)`` segment."""
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()

    paths = cb.iter_reached_paths()

    subject_paths = paths[("demo", "Subject")]
    assert len(subject_paths) == 1
    assert subject_paths[0] == (("demo", "Subject"),)


def test_iter_reached_paths_fk_reached_table_chains_from_anchor(
    tmp_path: Path,
) -> None:
    """Tables reached via FK get a multi-segment path from the anchor."""
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
    cb._compute_reached_tables()

    paths = cb.iter_reached_paths()

    # Image is reached inbound from Subject; one path from Subject to Image.
    image_paths = paths[("demo", "Image")]
    assert len(image_paths) == 1
    image_path = image_paths[0]
    assert image_path[0] == ("demo", "Subject")
    assert image_path[-1] == ("demo", "Image")


def test_iter_reached_paths_runs_walk_lazily(tmp_path: Path) -> None:
    """First call triggers the walk; reached_tables is populated after."""
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    # Pre-walk: nothing reached yet.
    assert cb.reached_tables == set()

    cb.iter_reached_paths()

    # Walk ran.
    assert ("demo", "Subject") in cb.reached_tables


def test_iter_reached_paths_returns_fresh_copy(tmp_path: Path) -> None:
    """Mutating the returned dict doesn't affect later calls."""
    subject = _make_mock_table("demo", "Subject")
    model = _make_mock_model({"demo": {"Subject": subject}})
    catalog = _make_mock_catalog(model)

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )
    cb._compute_reached_tables()

    first = cb.iter_reached_paths()
    first.clear()
    first[("evil", "Table")] = []

    second = cb.iter_reached_paths()
    assert ("evil", "Table") not in second
    assert ("demo", "Subject") in second


def test_iter_reached_paths_emits_one_entry_per_fk_path(
    tmp_path: Path,
) -> None:
    """A table reached via multiple FK routes gets one path entry per route."""
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
    cb._reached_tables = {("demo", "Subject"), ("demo", "Image")}
    cb._anchor_tables = {("demo", "Subject")}
    # Two distinct routes to Image: direct, and a hypothetical
    # two-hop via Subject's self-association.
    cb._table_path_set = {
        ("demo", "Subject"): [(("demo", "Subject"),)],
        ("demo", "Image"): [
            (("demo", "Subject"), ("demo", "Image")),
            (
                ("demo", "Subject"),
                ("demo", "Subject"),
                ("demo", "Image"),
            ),
        ],
    }

    paths = cb.iter_reached_paths()

    # Image has two distinct route entries.
    assert len(paths[("demo", "Image")]) == 2
    # Subject still has one (anchor).
    assert len(paths[("demo", "Subject")]) == 1


def test_iter_reached_paths_shares_walker_with_iter_table_datapaths(
    tmp_path: Path,
) -> None:
    """Both accessors share ``_compute_reached_tables`` — one walk, two views.

    The invariant the cutover relies on: ``iter_reached_paths`` and
    ``iter_table_datapaths`` must agree on which tables are reached
    and through which FK paths. The cutover ties spec generation and
    annotation generation to the same walker.
    """
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    fk = _fk_mock(src_table=image, pk_table=subject)
    image.foreign_keys = [fk]
    subject.referenced_by = [fk]
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    catalog = _make_mock_catalog(model)
    catalog.getPathBuilder.return_value = _make_pathbuilder(
        {
            ("demo", "Subject"): _make_table_path_for_datapath(
                "demo", "Subject"
            ),
            ("demo", "Image"): _make_table_path_for_datapath(
                "demo", "Image"
            ),
        }
    )

    cb = CatalogBagBuilder(
        catalog=catalog,
        anchors=[TableAnchor(table="Subject")],
        output_dir=tmp_path,
    )

    # Either method triggers the same walk.
    sym = cb.iter_reached_paths()
    dps = cb.iter_table_datapaths()

    # Same reached-table set.
    assert set(sym.keys()) == set(dps.keys())
    # Same per-table path count (one datapath per FK route, one
    # symbolic path per FK route — invariant).
    for key in sym.keys():
        assert len(sym[key]) == len(dps[key])
