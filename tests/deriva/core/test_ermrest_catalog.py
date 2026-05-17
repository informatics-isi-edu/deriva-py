"""Unit tests for :class:`deriva.core.ErmrestCatalog` glue.

Live-catalog functionality is covered by ``test_datapath.py`` and
the asyncio suites (which require ``DERIVA_PY_TEST_HOSTNAME``).
This module is for the in-process helpers that don't need a
server — currently the path-builder cache.
"""

from unittest.mock import MagicMock, patch


def _make_catalog():
    """Construct an :class:`ErmrestCatalog` without hitting a server.

    ``__init__`` only constructs the base URL — no HTTP — so we can
    instantiate it directly. Tests that exercise ``getPathBuilder``
    patch :func:`deriva.core.datapath.from_catalog` to avoid the
    schema fetch.
    """
    from deriva.core import ErmrestCatalog

    return ErmrestCatalog("https", "example.org", "1")


def test_get_path_builder_caches_the_wrapper() -> None:
    """Repeat calls return the same :class:`_CatalogWrapper`."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk:
        mk.return_value = MagicMock(name="PathBuilder")
        pb1 = catalog.getPathBuilder()
        pb2 = catalog.getPathBuilder()
    assert pb1 is pb2
    # The expensive ``from_catalog`` walk fired exactly once.
    assert mk.call_count == 1


def test_get_path_builder_refresh_rebuilds_the_wrapper() -> None:
    """``refresh=True`` discards the cache and walks again."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk:
        mk.side_effect = lambda c: MagicMock(name="PathBuilder")
        pb1 = catalog.getPathBuilder()
        pb2 = catalog.getPathBuilder(refresh=True)
    assert pb1 is not pb2
    assert mk.call_count == 2


def test_get_path_builder_cache_is_per_catalog() -> None:
    """Two distinct catalogs get independent caches."""
    a = _make_catalog()
    b = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk:
        mk.side_effect = lambda c: MagicMock(name=f"PB[{id(c)}]")
        pb_a = a.getPathBuilder()
        pb_b = b.getPathBuilder()
    assert pb_a is not pb_b
    assert mk.call_count == 2


# ---------------------------------------------------------------------------
# F1: same-instance auto-invalidation on schema mutations
# ---------------------------------------------------------------------------

def _stub_response(status_code: int = 200, snaptime: str | None = None) -> MagicMock:
    """Build a stand-in for a successful HTTP response.

    The override path inspects ``status_code`` to decide whether to
    invalidate; ``snaptime`` (when given) supports the F2 ``GET /``
    probe stub.
    """
    r = MagicMock(name=f"Response[{status_code}]")
    r.status_code = status_code
    if snaptime is not None:
        r.json.return_value = {"snaptime": snaptime}
    return r


def test_post_to_schema_invalidates_path_builder_cache() -> None:
    """``POST /schema/...`` clears the cache; next call rebuilds."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post:
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        mk_get.return_value = _stub_response(snaptime="snap-1")
        mk_post.return_value = _stub_response(status_code=200)

        pb1 = catalog.getPathBuilder()
        catalog.post("/schema/foo/table", json={})
        pb2 = catalog.getPathBuilder()

    assert pb1 is not pb2
    assert mk_pb.call_count == 2


def test_post_to_entity_does_not_invalidate_path_builder_cache() -> None:
    """A non-schema POST leaves the cache alone."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post:
        mk_pb.return_value = MagicMock(name="PathBuilder")
        mk_get.return_value = _stub_response(snaptime="snap-1")
        mk_post.return_value = _stub_response(status_code=200)

        pb1 = catalog.getPathBuilder()
        catalog.post("/entity/schema:table", json={})
        pb2 = catalog.getPathBuilder()

    assert pb1 is pb2
    assert mk_pb.call_count == 1


def test_failed_schema_post_does_not_invalidate_cache() -> None:
    """A 4xx/5xx schema POST means no server-side change; keep the cache.

    In practice ``DerivaBinding.post`` raises before returning on 4xx/5xx
    (via ``raise_for_status``); the invalidate hook is defensive. This
    test simulates the unlikely-but-defensive case by stubbing a 4xx
    response value to verify the status guard works in isolation.
    """
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post:
        mk_pb.return_value = MagicMock(name="PathBuilder")
        mk_get.return_value = _stub_response(snaptime="snap-1")
        mk_post.return_value = _stub_response(status_code=400)

        pb1 = catalog.getPathBuilder()
        catalog.post("/schema/foo/table", json={})
        pb2 = catalog.getPathBuilder()

    assert pb1 is pb2
    assert mk_pb.call_count == 1


def test_put_to_schema_invalidates_path_builder_cache() -> None:
    """``PUT /schema/...`` clears the cache (e.g. annotation updates)."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.deriva_binding.DerivaBinding.put") as mk_put:
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        mk_get.return_value = _stub_response(snaptime="snap-1")
        mk_put.return_value = _stub_response(status_code=200)

        pb1 = catalog.getPathBuilder()
        catalog.put("/schema/foo/table/Bar/annotation/tag:x", json={})
        pb2 = catalog.getPathBuilder()

    assert pb1 is not pb2
    assert mk_pb.call_count == 2


def test_delete_to_schema_invalidates_path_builder_cache() -> None:
    """``DELETE /schema/...`` clears the cache; the catalog-wide guard still fires."""
    from deriva.core.deriva_binding import DerivaPathError

    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.deriva_binding.DerivaBinding.delete") as mk_delete:
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        mk_get.return_value = _stub_response(snaptime="snap-1")
        mk_delete.return_value = _stub_response(status_code=204)

        pb1 = catalog.getPathBuilder()
        catalog.delete("/schema/foo/table/Bar")
        pb2 = catalog.getPathBuilder()

    assert pb1 is not pb2
    assert mk_pb.call_count == 2

    # Whole-catalog DELETE is still guarded.
    import pytest
    with pytest.raises(DerivaPathError):
        catalog.delete("/")


# ---------------------------------------------------------------------------
# F2: cheap "is the cache stale?" check via snaptime probe
# ---------------------------------------------------------------------------

def test_get_path_builder_cold_build_does_not_probe() -> None:
    """Default ``getPathBuilder()`` makes no HTTP call.

    Backward-compat: previously the call was zero-network. Adding F2
    must not change that contract for the default code path.
    """
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get:
        mk_pb.return_value = MagicMock(name="PathBuilder")

        catalog.getPathBuilder()

    assert mk_pb.call_count == 1
    assert mk_get.call_count == 0
    assert catalog._path_builder_snap is None


def test_get_path_builder_if_stale_first_call_rebuilds() -> None:
    """First ``if_stale=True`` after a cold build rebuilds once.

    Cold build leaves ``_path_builder_snap`` as ``None``; the first
    ``if_stale=True`` probe sees ``current_snap != None`` and
    rebuilds. The new cache then records the probed snaptime.
    """
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get:
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        mk_get.return_value = _stub_response(snaptime="snap-1")

        pb1 = catalog.getPathBuilder()                      # cold, no probe
        pb2 = catalog.getPathBuilder(if_stale=True)         # probe + rebuild

    assert pb1 is not pb2
    assert mk_pb.call_count == 2
    assert mk_get.call_count == 1
    assert catalog._path_builder_snap == "snap-1"


def test_get_path_builder_if_stale_steady_state_no_rebuild() -> None:
    """After snap is captured, ``if_stale=True`` with no drift just probes."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get:
        mk_pb.return_value = MagicMock(name="PathBuilder")
        mk_get.return_value = _stub_response(snaptime="snap-1")

        catalog.getPathBuilder()                            # cold
        pb1 = catalog.getPathBuilder(if_stale=True)         # probe + rebuild (records snap)
        pb2 = catalog.getPathBuilder(if_stale=True)         # probe only; snap matches, no rebuild

    assert pb1 is pb2
    assert mk_pb.call_count == 2
    assert mk_get.call_count == 2


def test_get_path_builder_if_stale_rebuilds_when_snaptime_advances() -> None:
    """``if_stale=True`` with an advanced snaptime triggers a rebuild."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get:
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        mk_get.side_effect = [
            _stub_response(snaptime="snap-1"),   # first if_stale probe (after cold build)
            _stub_response(snaptime="snap-2"),   # second if_stale probe — advanced
        ]

        catalog.getPathBuilder()                            # cold, no probe
        pb1 = catalog.getPathBuilder(if_stale=True)         # probe snap-1 + rebuild
        pb2 = catalog.getPathBuilder(if_stale=True)         # probe snap-2 + rebuild

    assert pb1 is not pb2
    assert mk_pb.call_count == 3
    assert mk_get.call_count == 2
    assert catalog._path_builder_snap == "snap-2"


def test_get_path_builder_refresh_does_not_probe() -> None:
    """``refresh=True`` rebuilds unconditionally with no ``GET /`` probe."""
    catalog = _make_catalog()
    with patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch.object(catalog, "get") as mk_get:
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")

        catalog.getPathBuilder()
        catalog.getPathBuilder(refresh=True)

    assert mk_pb.call_count == 2
    assert mk_get.call_count == 0
    assert catalog._path_builder_snap is None
