"""Tests for :mod:`deriva.bag.traversal`."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from deriva.bag.traversal import (
    AssetMode,
    DEFAULT_EXCLUDE_SCHEMAS,
    DanglingFKStrategy,
    FKTraversalPolicy,
    VocabExport,
)


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


def test_policy_default_construction() -> None:
    """``FKTraversalPolicy()`` is valid and has sane defaults."""
    p = FKTraversalPolicy()
    assert p.schemas is None
    assert p.exclude_schemas == set(DEFAULT_EXCLUDE_SCHEMAS)
    assert p.exclude_tables == set()
    assert p.max_depth is None
    assert p.vocab_export == VocabExport.REFERENCED_ONLY
    assert p.asset_mode == AssetMode.UPLOAD_IF_MISSING
    assert p.dangling_fk_strategy == DanglingFKStrategy.FAIL


def test_default_exclude_schemas_includes_admin_schemas() -> None:
    """Public/_acl_admin/WWW are the standard administrative schemas."""
    assert "public" in DEFAULT_EXCLUDE_SCHEMAS
    assert "_acl_admin" in DEFAULT_EXCLUDE_SCHEMAS
    assert "WWW" in DEFAULT_EXCLUDE_SCHEMAS


# ---------------------------------------------------------------------------
# Field validation
# ---------------------------------------------------------------------------


def test_policy_rejects_negative_max_depth() -> None:
    with pytest.raises(ValidationError):
        FKTraversalPolicy(max_depth=-1)


def test_policy_allows_zero_max_depth() -> None:
    """Depth 0 = "anchored rows only, no FK following.""" ""
    p = FKTraversalPolicy(max_depth=0)
    assert p.max_depth == 0


def test_policy_allows_unbounded_max_depth() -> None:
    p = FKTraversalPolicy(max_depth=None)
    assert p.max_depth is None


# ---------------------------------------------------------------------------
# Enum semantics
# ---------------------------------------------------------------------------


def test_asset_mode_values() -> None:
    """The three modes serialize to the expected wire values."""
    assert AssetMode.ROWS_ONLY.value == "rows_only"
    assert AssetMode.UPLOAD_IF_MISSING.value == "upload_if_missing"
    assert AssetMode.UPLOAD_FORCE.value == "upload_force"


def test_dangling_fk_strategy_values() -> None:
    assert DanglingFKStrategy.FAIL.value == "fail"
    assert DanglingFKStrategy.DELETE.value == "delete"
    assert DanglingFKStrategy.NULLIFY.value == "nullify"


def test_vocab_export_values() -> None:
    assert VocabExport.REFERENCED_ONLY.value == "referenced_only"
    assert VocabExport.FULL.value == "full"


# ---------------------------------------------------------------------------
# Bag-state × asset-mode matrix
# ---------------------------------------------------------------------------


def test_validate_with_bag_state_accepts_materialized_rows_only() -> None:
    FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY).validate_with_bag_state(
        holey=False
    )


def test_validate_with_bag_state_accepts_materialized_upload() -> None:
    FKTraversalPolicy(
        asset_mode=AssetMode.UPLOAD_IF_MISSING
    ).validate_with_bag_state(holey=False)


def test_validate_with_bag_state_accepts_holey_rows_only() -> None:
    """A holey bag with ROWS_ONLY is legitimate — no local bytes needed."""
    FKTraversalPolicy(asset_mode=AssetMode.ROWS_ONLY).validate_with_bag_state(
        holey=True
    )


def test_validate_with_bag_state_rejects_holey_upload_if_missing() -> None:
    """Holey bag + UPLOAD_IF_MISSING raises with the materialize hint."""
    with pytest.raises(ValueError) as excinfo:
        FKTraversalPolicy(
            asset_mode=AssetMode.UPLOAD_IF_MISSING
        ).validate_with_bag_state(holey=True)
    msg = str(excinfo.value)
    # The error should point at the workaround so callers can fix it.
    assert "materialize" in msg.lower()
    assert AssetMode.ROWS_ONLY.value in msg


def test_validate_with_bag_state_rejects_holey_upload_force() -> None:
    """Holey + UPLOAD_FORCE also rejected for the same reason."""
    with pytest.raises(ValueError):
        FKTraversalPolicy(
            asset_mode=AssetMode.UPLOAD_FORCE
        ).validate_with_bag_state(holey=True)


# ---------------------------------------------------------------------------
# JSON round-trip
# ---------------------------------------------------------------------------


def test_policy_round_trips_through_json() -> None:
    """Pydantic dump → load round-trips losslessly."""
    p = FKTraversalPolicy(
        schemas={"deriva-ml"},
        exclude_tables={("deriva-ml", "_internal")},
        max_depth=3,
        vocab_export=VocabExport.FULL,
        asset_mode=AssetMode.ROWS_ONLY,
        dangling_fk_strategy=DanglingFKStrategy.NULLIFY,
    )
    payload = p.model_dump(mode="json")
    p2 = FKTraversalPolicy.model_validate(payload)
    assert p2.schemas == {"deriva-ml"}
    # exclude_tables comes back as a list-of-lists from JSON; the
    # model validator coerces it back into a tuple-set.
    assert ("deriva-ml", "_internal") in p2.exclude_tables
    assert p2.max_depth == 3
    assert p2.vocab_export == VocabExport.FULL
    assert p2.asset_mode == AssetMode.ROWS_ONLY
    assert p2.dangling_fk_strategy == DanglingFKStrategy.NULLIFY


def test_policy_enum_values_in_json() -> None:
    """Enum fields serialize as their string values, not enum names."""
    p = FKTraversalPolicy(
        asset_mode=AssetMode.UPLOAD_FORCE,
        dangling_fk_strategy=DanglingFKStrategy.DELETE,
        vocab_export=VocabExport.FULL,
    )
    payload = json.loads(p.model_dump_json())
    assert payload["asset_mode"] == "upload_force"
    assert payload["dangling_fk_strategy"] == "delete"
    assert payload["vocab_export"] == "full"
