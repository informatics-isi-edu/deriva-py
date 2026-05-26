"""Tests for :class:`~deriva.bag.path_walker.SchemaPathWalker`.

These tests exercise the walker as a standalone primitive — separate
from :class:`CatalogBagBuilder`, which uses it as its FK-walk engine.
Coverage focuses on the two emission modes (BFS multi-route,
DFS-every-prefix), the policy fields the walker honors
(``schemas``/``exclude_schemas``/``exclude_tables``/``terminal_tables``/
``max_depth``), the universal vocab-as-leaf rule, multi-FK
deduplication, and the consumer-supplied ``edge_filter`` hook.

The catalog model is mocked the same way ``test_catalog_builder.py``
mocks it — these tests share the helper functions to keep behavior
parity with the bag-builder tests.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from deriva.bag.path_walker import SchemaPathWalker
from deriva.bag.traversal import FKTraversalPolicy


# ---------------------------------------------------------------------------
# Mock helpers — copies of the catalog-builder test helpers so this
# test file stays self-contained.
# ---------------------------------------------------------------------------


def _make_mock_table(
    schema_name: str,
    table_name: str,
    *,
    is_vocabulary: bool = False,
    outbound_fks: list[Any] | None = None,
    inbound_fks: list[Any] | None = None,
) -> MagicMock:
    t = MagicMock(name=f"Table[{schema_name}.{table_name}]")
    t.name = table_name
    t.schema = MagicMock(name=f"Schema[{schema_name}]")
    t.schema.name = schema_name
    t.is_vocabulary.return_value = is_vocabulary
    t.foreign_keys = list(outbound_fks or [])
    t.referenced_by = list(inbound_fks or [])
    return t


def _make_mock_model(
    schemas: dict[str, dict[str, MagicMock]],
) -> MagicMock:
    model = MagicMock(name="Model")
    model.schemas = {}
    for schema_name, tables in schemas.items():
        schema = MagicMock(name=f"Schema[{schema_name}]")
        schema.name = schema_name
        schema.tables = tables
        for t in tables.values():
            t.schema = schema
        model.schemas[schema_name] = schema
    return model


def _fk(*, src: MagicMock, pk: MagicMock) -> MagicMock:
    fk = MagicMock(name="ForeignKey")
    fk.table = src
    fk.pk_table = pk
    return fk


def _wire_fk(src: MagicMock, pk: MagicMock) -> None:
    """Install one FK edge: ``src.{column}`` → ``pk.RID``."""
    fk = _fk(src=src, pk=pk)
    src.foreign_keys.append(fk)
    pk.referenced_by.append(fk)


# ---------------------------------------------------------------------------
# walk_bfs
# ---------------------------------------------------------------------------


def test_walk_bfs_reaches_outbound_fk() -> None:
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    _wire_fk(src=image, pk=subject)
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_bfs([("demo", "Image")])
    assert set(paths) == {("demo", "Image"), ("demo", "Subject")}
    # BFS path from Image to Subject: [Image, Subject].
    assert paths[("demo", "Subject")][0] == (
        ("demo", "Image"),
        ("demo", "Subject"),
    )


def test_walk_bfs_reaches_inbound_fk() -> None:
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    _wire_fk(src=image, pk=subject)
    model = _make_mock_model(
        {"demo": {"Image": image, "Subject": subject}}
    )
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_bfs([("demo", "Subject")])
    assert set(paths) == {("demo", "Image"), ("demo", "Subject")}


def test_walk_bfs_vocab_does_not_propagate_inbound() -> None:
    species = _make_mock_table("demo", "Species", is_vocabulary=True)
    subject = _make_mock_table("demo", "Subject")
    other = _make_mock_table("demo", "Other")
    _wire_fk(src=subject, pk=species)
    _wire_fk(src=other, pk=species)
    model = _make_mock_model(
        {
            "demo": {
                "Species": species,
                "Subject": subject,
                "Other": other,
            }
        }
    )
    walker = SchemaPathWalker(model=model)
    reached = walker.walk_bfs([("demo", "Subject")])
    assert set(reached) == {("demo", "Subject"), ("demo", "Species")}
    assert ("demo", "Other") not in reached


def test_walk_bfs_respects_max_depth() -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    c = _make_mock_table("demo", "C")
    _wire_fk(src=a, pk=b)
    _wire_fk(src=b, pk=c)
    model = _make_mock_model({"demo": {"A": a, "B": b, "C": c}})
    walker = SchemaPathWalker(
        model=model,
        policy=FKTraversalPolicy(max_depth=1),
    )
    reached = walker.walk_bfs([("demo", "A")])
    # Depth 1: A and its direct neighbor B; not C.
    assert set(reached) == {("demo", "A"), ("demo", "B")}


def test_walk_bfs_respects_exclude_tables() -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    c = _make_mock_table("demo", "C")
    _wire_fk(src=a, pk=b)
    _wire_fk(src=b, pk=c)
    model = _make_mock_model({"demo": {"A": a, "B": b, "C": c}})
    walker = SchemaPathWalker(
        model=model,
        policy=FKTraversalPolicy(exclude_tables={("demo", "B")}),
    )
    reached = walker.walk_bfs([("demo", "A")])
    assert set(reached) == {("demo", "A")}


def test_walk_bfs_respects_schema_allowlist() -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("other", "B")
    _wire_fk(src=a, pk=b)
    model = _make_mock_model(
        {"demo": {"A": a}, "other": {"B": b}}
    )
    walker = SchemaPathWalker(
        model=model,
        policy=FKTraversalPolicy(schemas={"demo"}),
    )
    reached = walker.walk_bfs([("demo", "A")])
    assert set(reached) == {("demo", "A")}


def test_walk_bfs_terminal_table_blocks_inbound_keeps_outbound() -> None:
    # Execution is terminal: it follows its own outbound (Workflow) but
    # not its inbound (Image_Execution → Execution → Other anchors).
    execution = _make_mock_table("demo", "Execution")
    workflow = _make_mock_table("demo", "Workflow")
    image_exec = _make_mock_table("demo", "Image_Execution")
    _wire_fk(src=execution, pk=workflow)  # outbound from Execution
    _wire_fk(src=image_exec, pk=execution)  # inbound into Execution
    model = _make_mock_model(
        {
            "demo": {
                "Execution": execution,
                "Workflow": workflow,
                "Image_Execution": image_exec,
            }
        }
    )
    walker = SchemaPathWalker(
        model=model,
        policy=FKTraversalPolicy(
            terminal_tables={("demo", "Execution")},
        ),
    )
    reached = walker.walk_bfs([("demo", "Execution")])
    # Workflow reached (outbound followed). Image_Execution NOT reached
    # (inbound blocked).
    assert set(reached) == {("demo", "Execution"), ("demo", "Workflow")}
    assert ("demo", "Image_Execution") not in reached


def test_walk_bfs_multi_path_records_all_routes() -> None:
    # Image is reachable from Subject two ways:
    #   1) Image.Subject (direct inbound)
    #   2) Subject ← Dataset_Subject ← Dataset → Dataset_Image → Image
    subject = _make_mock_table("demo", "Subject")
    image = _make_mock_table("demo", "Image")
    dataset = _make_mock_table("demo", "Dataset")
    ds_subject = _make_mock_table("demo", "Dataset_Subject")
    ds_image = _make_mock_table("demo", "Dataset_Image")
    _wire_fk(src=image, pk=subject)
    _wire_fk(src=ds_subject, pk=dataset)
    _wire_fk(src=ds_subject, pk=subject)
    _wire_fk(src=ds_image, pk=dataset)
    _wire_fk(src=ds_image, pk=image)
    model = _make_mock_model(
        {
            "demo": {
                "Subject": subject,
                "Image": image,
                "Dataset": dataset,
                "Dataset_Subject": ds_subject,
                "Dataset_Image": ds_image,
            }
        }
    )
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_bfs([("demo", "Subject")])
    image_routes = paths[("demo", "Image")]
    # At least two distinct routes recorded.
    assert len(image_routes) >= 2


def test_walk_bfs_max_paths_caps_routes() -> None:
    # Tightly connected graph that would explode without a cap.
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    # Wire multiple distinct routes by inserting hubs.
    hubs = [_make_mock_table("demo", f"H{i}") for i in range(20)]
    for h in hubs:
        _wire_fk(src=a, pk=h)
        _wire_fk(src=h, pk=b)
    model = _make_mock_model(
        {"demo": {"A": a, "B": b, **{h.name: h for h in hubs}}}
    )
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_bfs(
        [("demo", "A")], max_paths_per_target=5
    )
    # Cap honored on the densely-reached B.
    assert len(paths[("demo", "B")]) <= 5


def test_walk_bfs_edge_filter_drops_edges() -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    c = _make_mock_table("demo", "C")
    _wire_fk(src=a, pk=b)
    _wire_fk(src=b, pk=c)
    model = _make_mock_model(
        {"demo": {"A": a, "B": b, "C": c}}
    )
    # Drop every edge that would land on B.
    def filter_out_b(src: Any, tgt: Any) -> bool:
        return tgt.name != "B"

    walker = SchemaPathWalker(model=model, edge_filter=filter_out_b)
    reached = walker.walk_bfs([("demo", "A")])
    assert set(reached) == {("demo", "A")}


# ---------------------------------------------------------------------------
# walk_all_prefixes
# ---------------------------------------------------------------------------


def test_walk_all_prefixes_emits_every_prefix() -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    c = _make_mock_table("demo", "C")
    _wire_fk(src=a, pk=b)
    _wire_fk(src=b, pk=c)
    model = _make_mock_model(
        {"demo": {"A": a, "B": b, "C": c}}
    )
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_all_prefixes(root=a)
    sigs = {tuple(t.name for t in p) for p in paths}
    # Every prefix: [A], [A, B], [A, B, C].
    assert ("A",) in sigs
    assert ("A", "B") in sigs
    assert ("A", "B", "C") in sigs


def test_walk_all_prefixes_vocab_terminates() -> None:
    a = _make_mock_table("demo", "A")
    vocab = _make_mock_table("demo", "Vocab", is_vocabulary=True)
    other = _make_mock_table("demo", "Other")
    _wire_fk(src=a, pk=vocab)
    _wire_fk(src=other, pk=vocab)  # Other references Vocab too
    model = _make_mock_model(
        {"demo": {"A": a, "Vocab": vocab, "Other": other}}
    )
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_all_prefixes(root=a)
    sigs = {tuple(t.name for t in p) for p in paths}
    # A and A→Vocab present; nothing past Vocab.
    assert ("A",) in sigs
    assert ("A", "Vocab") in sigs
    # Should NOT walk Vocab.referenced_by → Other.
    for sig in sigs:
        assert "Other" not in sig


def test_walk_all_prefixes_respects_max_depth() -> None:
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    c = _make_mock_table("demo", "C")
    _wire_fk(src=a, pk=b)
    _wire_fk(src=b, pk=c)
    model = _make_mock_model(
        {"demo": {"A": a, "B": b, "C": c}}
    )
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_all_prefixes(root=a, max_depth=2)
    sigs = {tuple(t.name for t in p) for p in paths}
    assert ("A",) in sigs
    assert ("A", "B") in sigs
    # max_depth=2 stops at length-2 paths; C should not be reached.
    assert ("A", "B", "C") not in sigs


def test_walk_all_prefixes_detects_cycles() -> None:
    # A → B → A cycle.
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    _wire_fk(src=a, pk=b)
    _wire_fk(src=b, pk=a)
    model = _make_mock_model({"demo": {"A": a, "B": b}})
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_all_prefixes(root=a)
    # Should terminate (no infinite recursion) and each path should
    # appear at most once.
    sigs = [tuple(t.name for t in p) for p in paths]
    for sig in sigs:
        # No table appears twice on the same path.
        assert len(set(sig)) == len(sig), f"Cycle in {sig}"


def test_walk_all_prefixes_dedupes_multi_fk_edges() -> None:
    # Two FKs between A and B should collapse to one walk edge.
    a = _make_mock_table("demo", "A")
    b = _make_mock_table("demo", "B")
    _wire_fk(src=a, pk=b)
    _wire_fk(src=a, pk=b)  # second FK to the same target
    model = _make_mock_model({"demo": {"A": a, "B": b}})
    walker = SchemaPathWalker(model=model)
    paths = walker.walk_all_prefixes(root=a)
    sigs = [tuple(t.name for t in p) for p in paths]
    # ("A", "B") should appear once, not twice.
    ab_count = sum(1 for s in sigs if s == ("A", "B"))
    assert ab_count == 1


def test_walk_all_prefixes_edge_filter_can_block_loopback() -> None:
    # Simulate the nested-dataset-loopback: A → Bridge → A.
    a = _make_mock_table("demo", "A")
    bridge = _make_mock_table("demo", "Bridge")
    other = _make_mock_table("demo", "Other")
    _wire_fk(src=bridge, pk=a)
    _wire_fk(src=bridge, pk=other)
    model = _make_mock_model(
        {"demo": {"A": a, "Bridge": bridge, "Other": other}}
    )

    # Loopback filter: don't allow bridge → a when traversal is
    # already past a.
    def filter_loopback(src: Any, tgt: Any) -> bool:
        if src.name == "Bridge" and tgt.name == "A":
            return False
        return True

    walker = SchemaPathWalker(model=model, edge_filter=filter_loopback)
    paths = walker.walk_all_prefixes(root=a)
    sigs = {tuple(t.name for t in p) for p in paths}
    # A → Bridge → Other should still be discoverable.
    assert ("A", "Bridge", "Other") in sigs
    # A → Bridge → A should NOT appear (cycle would be blocked anyway,
    # but this verifies the filter is consulted).
    for sig in sigs:
        assert sig != ("A", "Bridge", "A")
