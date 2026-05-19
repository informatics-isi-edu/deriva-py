"""Unit tests for :class:`deriva.core.ErmrestCatalog` glue.

Live-catalog functionality is covered by ``test_datapath.py`` and
the asyncio suites (which require ``DERIVA_PY_TEST_HOSTNAME``).
This module is for the in-process helpers that don't need a
server -- currently the schema-derived caches.

Architecture
------------
Caching of schema-derived state is layered:

1. **HTTP cache** (``DerivaBinding._cache``). Every GET response is
   stored by URL. ``_pre_get`` adds ``If-None-Match`` from the
   cached etag; the binding's ``_raise_for_status_304`` returns the
   same ``Response`` object on a 304.

2. **Schema-mutation invalidation** (``ErmrestCatalog.{post,put,delete}``).
   A successful 2xx/3xx mutation on a ``/schema/...`` path calls
   :meth:`DerivaBinding.purge_cache_by_prefix` with ``/schema`` --
   dropping every cached GET response under that prefix. The next
   read fetches fresh.

3. **Identity-tied derived caches** (``getCatalogSchema``,
   ``getPathBuilder``). These memoize derived objects (the parsed
   dict, the ``_CatalogWrapper``) keyed on the underlying object's
   identity from layer 1. Same ``Response`` object → same parsed
   dict → same path-builder wrapper. Different ``Response`` (the
   binding refetched) → new parsed dict → new wrapper.

No instance-level snaptime or etag tracking is needed: the binding
layer is the single source of truth.
"""

from unittest.mock import MagicMock, patch


def _make_catalog():
    """Construct an :class:`ErmrestCatalog` without hitting a server.

    ``__init__`` only constructs the base URL -- no HTTP -- so we
    can instantiate it directly.
    """
    from deriva.core import ErmrestCatalog

    return ErmrestCatalog("https", "example.org", "1")


def _stub_response(status_code: int = 200, etag: str | None = None,
                   json_body: dict | None = None) -> MagicMock:
    """Build a stand-in for an HTTP response.

    Args:
        status_code: HTTP status. Used by the invalidation guard
            (only 2xx/3xx invalidate).
        etag: Optional ``etag`` header value.
        json_body: Optional dict to return from ``r.json()``. Default
            empty dict.
    """
    r = MagicMock(name=f"Response[{status_code}]")
    r.status_code = status_code
    r.headers = {}
    if etag is not None:
        r.headers["etag"] = etag
    r.json.return_value = json_body if json_body is not None else {}
    return r


# ---------------------------------------------------------------------------
# Layer 1: HTTP cache (DerivaBinding._cache + purge_cache_by_prefix)
# ---------------------------------------------------------------------------


def test_purge_cache_by_prefix_clears_matching_entries() -> None:
    """``purge_cache_by_prefix`` removes cached GET entries by URL-path prefix."""
    catalog = _make_catalog()
    base = catalog._server_uri
    # Seed the cache with entries covering several path families.
    catalog._cache[base + "/schema"] = MagicMock(name="schema-root")
    catalog._cache[base + "/schema/foo"] = MagicMock(name="schema-foo")
    catalog._cache[base + "/schema/foo/table/Bar"] = MagicMock(name="schema-bar")
    catalog._cache[base + "/entity/foo:Bar"] = MagicMock(name="entity-bar")
    catalog._cache[base + "/"] = MagicMock(name="root")

    catalog.purge_cache_by_prefix("/schema")

    # All three /schema* entries gone.
    assert base + "/schema" not in catalog._cache
    assert base + "/schema/foo" not in catalog._cache
    assert base + "/schema/foo/table/Bar" not in catalog._cache
    # Unrelated paths preserved.
    assert base + "/entity/foo:Bar" in catalog._cache
    assert base + "/" in catalog._cache


def test_purge_cache_by_prefix_is_safe_on_empty_cache() -> None:
    """No exception when there's nothing to purge."""
    catalog = _make_catalog()
    catalog.purge_cache_by_prefix("/schema")  # should not raise


def test_purge_cache_by_prefix_respects_caching_flag() -> None:
    """If caching is off, purge is a no-op."""
    catalog = _make_catalog()
    catalog._caching = False
    catalog._cache["should-not-be-touched"] = MagicMock()
    catalog.purge_cache_by_prefix("/schema")
    # Still there -- _caching=False means we don't trust _cache anyway,
    # but the method must not error.
    assert "should-not-be-touched" in catalog._cache


# ---------------------------------------------------------------------------
# Layer 2: schema-mutation invalidation on ErmrestCatalog
# ---------------------------------------------------------------------------


def test_post_to_schema_purges_schema_cache_entries() -> None:
    """A successful POST /schema/... clears every cached /schema* GET."""
    catalog = _make_catalog()
    base = catalog._server_uri
    catalog._cache[base + "/schema"] = MagicMock(name="schema-root")
    catalog._cache[base + "/schema/foo/table/Bar"] = MagicMock(name="schema-bar")
    catalog._cache[base + "/entity/foo:Bar"] = MagicMock(name="entity-bar")

    with patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post:
        mk_post.return_value = _stub_response(status_code=200)
        catalog.post("/schema/foo/table", json={})

    assert base + "/schema" not in catalog._cache
    assert base + "/schema/foo/table/Bar" not in catalog._cache
    # Unrelated entries preserved.
    assert base + "/entity/foo:Bar" in catalog._cache


def test_post_to_entity_does_not_purge_schema_cache() -> None:
    """A non-schema POST leaves the /schema cache alone."""
    catalog = _make_catalog()
    base = catalog._server_uri
    sentinel = MagicMock(name="schema-root")
    catalog._cache[base + "/schema"] = sentinel

    with patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post:
        mk_post.return_value = _stub_response(status_code=200)
        catalog.post("/entity/schema:table", json={})

    assert catalog._cache[base + "/schema"] is sentinel


def test_failed_schema_post_does_not_purge_cache() -> None:
    """A 4xx/5xx schema POST means no server-side change; keep the cache."""
    catalog = _make_catalog()
    base = catalog._server_uri
    sentinel = MagicMock(name="schema-root")
    catalog._cache[base + "/schema"] = sentinel

    with patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post:
        mk_post.return_value = _stub_response(status_code=400)
        catalog.post("/schema/foo/table", json={})

    assert catalog._cache[base + "/schema"] is sentinel


def test_put_to_schema_purges_schema_cache_entries() -> None:
    """``PUT /schema/...`` (e.g. annotation updates) clears the /schema cache."""
    catalog = _make_catalog()
    base = catalog._server_uri
    catalog._cache[base + "/schema"] = MagicMock(name="schema-root")

    with patch("deriva.core.deriva_binding.DerivaBinding.put") as mk_put:
        mk_put.return_value = _stub_response(status_code=200)
        catalog.put("/schema/foo/table/Bar/annotation/tag:x", json={})

    assert base + "/schema" not in catalog._cache


def test_delete_to_schema_purges_schema_cache_entries() -> None:
    """``DELETE /schema/...`` clears the /schema cache; catalog-wide guard still fires."""
    from deriva.core.deriva_binding import DerivaPathError

    catalog = _make_catalog()
    base = catalog._server_uri
    catalog._cache[base + "/schema"] = MagicMock(name="schema-root")

    with patch("deriva.core.deriva_binding.DerivaBinding.delete") as mk_delete:
        mk_delete.return_value = _stub_response(status_code=204)
        catalog.delete("/schema/foo/table/Bar")

    assert base + "/schema" not in catalog._cache

    # Whole-catalog DELETE is still guarded.
    import pytest
    with pytest.raises(DerivaPathError):
        catalog.delete("/")


# ---------------------------------------------------------------------------
# Layer 3: identity-tied derived caches (parsed dict, path-builder wrapper)
# ---------------------------------------------------------------------------


def test_get_catalog_schema_returns_parsed_dict() -> None:
    """First call fetches /schema, parses the body, returns the dict."""
    catalog = _make_catalog()
    body = {"schemas": {"foo": {}}}
    response = _stub_response(etag='"e1"', json_body=body)
    with patch.object(catalog, "get") as mk_get:
        mk_get.return_value = response
        result = catalog.getCatalogSchema()
    assert result == body
    response.json.assert_called_once()


def test_get_catalog_schema_memoizes_when_response_identity_unchanged() -> None:
    """Repeat call returns the same parsed dict without re-parsing.

    The binding-layer HTTP cache returns the same ``Response`` object
    when nothing on the server has changed; ``getCatalogSchema()``
    keys its parsed-dict memo on that ``Response`` identity.
    """
    catalog = _make_catalog()
    body = {"schemas": {"foo": {}}}
    response = _stub_response(etag='"e1"', json_body=body)
    with patch.object(catalog, "get") as mk_get:
        mk_get.return_value = response  # same object on both calls
        d1 = catalog.getCatalogSchema()
        d2 = catalog.getCatalogSchema()
    # Same dict object -- no re-parse.
    assert d1 is d2
    # response.json() was called exactly once (on first parse).
    response.json.assert_called_once()


def test_get_catalog_schema_reparses_when_response_identity_changes() -> None:
    """A different ``Response`` object (server returned fresh body) → re-parse."""
    catalog = _make_catalog()
    r1 = _stub_response(etag='"e1"', json_body={"v": 1})
    r2 = _stub_response(etag='"e2"', json_body={"v": 2})
    with patch.object(catalog, "get") as mk_get:
        mk_get.side_effect = [r1, r2]
        d1 = catalog.getCatalogSchema()
        d2 = catalog.getCatalogSchema()
    assert d1 != d2
    assert d1 == {"v": 1}
    assert d2 == {"v": 2}


def test_get_path_builder_caches_the_wrapper() -> None:
    """Repeat calls return the same wrapper when the schema dict is unchanged."""
    catalog = _make_catalog()
    response = _stub_response(etag='"e1"')
    with patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb:
        mk_get.return_value = response
        mk_pb.return_value = MagicMock(name="PathBuilder")
        pb1 = catalog.getPathBuilder()
        pb2 = catalog.getPathBuilder()
    assert pb1 is pb2
    # The expensive ``from_catalog`` walk fired exactly once.
    assert mk_pb.call_count == 1


def test_get_path_builder_refresh_rebuilds_the_wrapper() -> None:
    """``refresh=True`` discards the cached wrapper and rebuilds."""
    catalog = _make_catalog()
    response = _stub_response(etag='"e1"')
    with patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb:
        mk_get.return_value = response
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        pb1 = catalog.getPathBuilder()
        pb2 = catalog.getPathBuilder(refresh=True)
    assert pb1 is not pb2
    assert mk_pb.call_count == 2


def test_get_path_builder_rebuilds_when_schema_changes() -> None:
    """A fresh schema body (different Response object) → wrapper rebuilds."""
    catalog = _make_catalog()
    r1 = _stub_response(etag='"e1"', json_body={"v": 1})
    r2 = _stub_response(etag='"e2"', json_body={"v": 2})
    with patch.object(catalog, "get") as mk_get, \
         patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb:
        mk_get.side_effect = [r1, r2]
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        pb1 = catalog.getPathBuilder()
        pb2 = catalog.getPathBuilder()
    assert pb1 is not pb2
    assert mk_pb.call_count == 2


def test_get_path_builder_cache_is_per_catalog() -> None:
    """Two distinct catalogs get independent wrapper caches."""
    a = _make_catalog()
    b = _make_catalog()
    ra = _stub_response(etag='"a"')
    rb = _stub_response(etag='"b"')
    with patch.object(a, "get") as mk_get_a, \
         patch.object(b, "get") as mk_get_b, \
         patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb:
        mk_get_a.return_value = ra
        mk_get_b.return_value = rb
        mk_pb.side_effect = lambda c: MagicMock(name=f"PB[{id(c)}]")
        pb_a = a.getPathBuilder()
        pb_b = b.getPathBuilder()
    assert pb_a is not pb_b
    assert mk_pb.call_count == 2


# ---------------------------------------------------------------------------
# End-to-end: schema mutation invalidates derived caches transparently
#
# These verify the layered design works as one system: a schema
# mutation at layer 2 cascades through the binding's HTTP cache, the
# parsed-dict memo, and the path-builder wrapper -- all without any
# code on this class touching individual derived-cache slots.
# ---------------------------------------------------------------------------


def test_schema_mutation_cascades_through_all_derived_caches() -> None:
    """Schema mutation invalidates parsed dict AND path-builder wrapper transparently.

    Each ``getCatalogSchema()`` / ``getPathBuilder()`` pair issues a
    single ``self.get('/schema')`` call (the path-builder uses the
    parsed dict returned by the schema getter, not an independent
    fetch). So we stub two pairs of GETs: one set of pre-mutation
    calls returning r1, one set of post-mutation calls returning r2.
    The real ``DerivaBinding`` would memoize within each set, but the
    mock surfaces every call as a fresh return value.
    """
    catalog = _make_catalog()
    r1 = _stub_response(etag='"e1"', json_body={"v": 1})
    r2 = _stub_response(etag='"e2"', json_body={"v": 2})

    def _get_side_effect(path, headers=None):
        # Mock the cache memoization so getCatalogSchema sees the
        # same Response within each pre/post-mutation block.
        return _get_side_effect.current

    _get_side_effect.current = r1

    with patch.object(catalog, "get", side_effect=_get_side_effect) as mk_get, \
         patch("deriva.core.ermrest_catalog.datapath.from_catalog") as mk_pb, \
         patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post:
        mk_pb.side_effect = lambda c: MagicMock(name="PathBuilder")
        mk_post.return_value = _stub_response(status_code=200)

        # Pre-mutation: take a snapshot.
        d1 = catalog.getCatalogSchema()
        pb1 = catalog.getPathBuilder()
        # Path-builder uses the schema dict; identity-tied cache means
        # both reads see r1.
        assert d1 is catalog.getCatalogSchema()

        # Schema mutation. Server returns a fresh body on next GET.
        catalog.post("/schema/foo/table", json={})
        _get_side_effect.current = r2

        # Post-mutation: everything rebuilds.
        d2 = catalog.getCatalogSchema()
        pb2 = catalog.getPathBuilder()

    assert d1 is not d2
    assert pb1 is not pb2


def test_data_writes_do_not_invalidate_schema_cache() -> None:
    """``POST /entity/...`` (data write) leaves the /schema cache intact.

    This is the key invariant that the old snaptime-based mechanism
    got wrong: the catalog snaptime advances on every catalog update,
    including data writes that don't touch the schema, so a snaptime-
    based probe would refetch /schema after every insert. Here we
    verify the new prefix-based purge is correctly scoped: only
    /schema mutations invalidate /schema cache entries.
    """
    catalog = _make_catalog()
    base = catalog._server_uri
    sentinel = MagicMock(name="schema-root")
    catalog._cache[base + "/schema"] = sentinel

    with patch("deriva.core.deriva_binding.DerivaBinding.post") as mk_post, \
         patch("deriva.core.deriva_binding.DerivaBinding.put") as mk_put, \
         patch("deriva.core.deriva_binding.DerivaBinding.delete") as mk_delete:
        mk_post.return_value = _stub_response(status_code=200)
        mk_put.return_value = _stub_response(status_code=200)
        mk_delete.return_value = _stub_response(status_code=204)

        catalog.post("/entity/schema:table", json={})
        catalog.put("/attribute/schema:table/col", json={})
        catalog.delete("/entity/schema:table/RID=1")

    assert catalog._cache[base + "/schema"] is sentinel
