"""Tests for :mod:`deriva.bag.anchors`."""

from __future__ import annotations

import json

import pytest
from pydantic import TypeAdapter, ValidationError

from deriva.bag.anchors import (
    Anchor,
    AnchorKind,
    RIDAnchor,
    TableAnchor,
)


# ---------------------------------------------------------------------------
# RIDAnchor
# ---------------------------------------------------------------------------


def test_rid_anchor_constructs() -> None:
    """Basic construction works and ``kind`` defaults correctly."""
    a = RIDAnchor(table="Subject", rids=["S1", "S2"])
    assert a.table == "Subject"
    assert a.rids == ["S1", "S2"]
    assert a.kind == AnchorKind.RID


def test_rid_anchor_rejects_empty_rids() -> None:
    """An empty RID list is a programmer error, not a legitimate value."""
    with pytest.raises(ValidationError):
        RIDAnchor(table="Subject", rids=[])


def test_rid_anchor_rejects_empty_string_rid() -> None:
    """An empty-string RID slipped into the list is also rejected."""
    with pytest.raises(ValidationError):
        RIDAnchor(table="Subject", rids=["S1", ""])


def test_rid_anchor_rejects_whitespace_rid() -> None:
    """Whitespace-only RIDs are rejected (same intent as empty)."""
    with pytest.raises(ValidationError):
        RIDAnchor(table="Subject", rids=["   "])


def test_rid_anchor_rejects_empty_table() -> None:
    """Table name must be non-empty."""
    with pytest.raises(ValidationError):
        RIDAnchor(table="", rids=["S1"])


# ---------------------------------------------------------------------------
# TableAnchor
# ---------------------------------------------------------------------------


def test_table_anchor_constructs() -> None:
    a = TableAnchor(table="Subject")
    assert a.table == "Subject"
    assert a.kind == AnchorKind.TABLE


def test_table_anchor_rejects_empty_table() -> None:
    with pytest.raises(ValidationError):
        TableAnchor(table="")


# ---------------------------------------------------------------------------
# Discriminated union
# ---------------------------------------------------------------------------


_anchor_adapter: TypeAdapter[Anchor] = TypeAdapter(Anchor)


def test_anchor_union_discriminates_by_kind() -> None:
    """A dict carrying ``kind`` parses to the right variant."""
    rid_a = _anchor_adapter.validate_python(
        {"kind": "rid", "table": "Subject", "rids": ["S1"]}
    )
    table_a = _anchor_adapter.validate_python(
        {"kind": "table", "table": "Subject"}
    )
    assert isinstance(rid_a, RIDAnchor)
    assert isinstance(table_a, TableAnchor)


def test_anchor_union_round_trips_through_json() -> None:
    """Pydantic serialize → parse round-trips losslessly."""
    original = RIDAnchor(table="Subject", rids=["S1", "S2"])
    payload = json.loads(
        _anchor_adapter.dump_json(original)
    )
    restored = _anchor_adapter.validate_python(payload)
    assert isinstance(restored, RIDAnchor)
    assert restored.table == "Subject"
    assert restored.rids == ["S1", "S2"]


def test_anchor_union_rejects_unknown_kind() -> None:
    with pytest.raises(ValidationError):
        _anchor_adapter.validate_python(
            {"kind": "bogus", "table": "Subject"}
        )
