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
