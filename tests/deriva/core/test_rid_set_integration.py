"""End-to-end Format-B rid-set integration test (no live catalog).

The unit tests in ``test_rid_set_fetch.py`` prove each link in the chain in
isolation: chunking, per-RID URL quoting, the chunk-append in ``get_as_file``,
and the processor's forwarding of ``rid_set``/``rid_table`` into
``get_as_file`` (with ``get_as_file`` itself mocked). None of them drive the
*whole* chain — spec -> processor -> the REAL ``get_as_file`` -> a CSV file on
disk. That gap is exactly why FIX 2 (the ``catalogQuery`` short-circuit that
returned early for a query-less rid-set processor) survived five tasks.

This test closes the gap. It builds a ``CSVQueryProcessor`` configured the way
``CatalogBagBuilder`` emits a Format-B processor — ``rid_set`` in
``processor_params``, NO ``query_path`` — backed by a real ``ErmrestCatalog``
whose only mock is ``_session.get`` (serving CSV pages per RID-chunk URL). It
then calls ``proc.catalogQuery()`` and asserts a real, chunk-appended CSV is on
disk. Because the catalog's ``get_as_file`` is the genuine
``ErmrestCatalog.get_as_file`` (not a ``MagicMock``), the full
processor -> ``catalogQuery`` (FIX 2 guard) -> ``get_as_file`` ->
``_get_rid_set_as_file`` -> ``_fetch_paged_content`` path executes and writes
correct bytes.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

from deriva.core.ermrest_catalog import ErmrestCatalog
import deriva.core.ermrest_catalog as ec
from deriva.transfer.download.processors.query.base_query_processor import (
    BaseQueryProcessor,
    CSVQueryProcessor,
)

# Reuse the CSV-page fake helpers that already drive the get_as_file unit test.
from tests.deriva.core.test_rid_set_fetch import _FakeResponse, _csv


def _make_catalog_serving(pages, header):
    """Build a real ``ErmrestCatalog`` whose ``_session.get`` serves CSV pages.

    The catalog is constructed via ``__new__`` to bypass ``__init__``/network,
    then its ``_session.get`` is given the ``@after``-terminating fake from
    ``test_rid_set_fetch.py``: a request whose URL contains ``@after`` returns
    an empty CSV page (ending that chunk's cursor loop); otherwise the page body
    keyed by the matching RID-chunk path is returned. Everything else on the
    catalog — including ``get_as_file`` and its rid-set chunk-append helpers — is
    the genuine class implementation.

    Args:
        pages: Mapping of RID-chunk path (e.g. ``/entity/S:T/RID=any(r1,r2)``)
            to the CSV body that chunk should return.
        header: The shared CSV header line (e.g. ``"RID,Name"``).

    Returns:
        A real ``ErmrestCatalog`` instance ready for ``get_as_file``.
    """
    cat = ErmrestCatalog.__new__(ErmrestCatalog)  # bypass __init__/network
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    def fake_get(url, headers=None, stream=False):
        if "@after" in url:
            # Second page of any chunk: no more rows -> terminate the page loop.
            return _FakeResponse(_csv(header, []))
        for path, body in pages.items():
            if path in url:
                return _FakeResponse(body)
        raise AssertionError("unexpected URL: %s" % url)

    cat._session = MagicMock()
    cat._session.get.side_effect = fake_get
    cat._response_raise_for_status = lambda r: None
    return cat


def _make_format_b_processor(catalog, rid_set, rid_table, output_abspath):
    """Hand-build a ``BaseQueryProcessor`` shaped like a Format-B emission.

    Mirrors how ``CatalogBagBuilder`` configures the processor: ``rid_set`` and
    ``rid_table`` come from ``processor_params``, and there is NO ``query_path``
    (``self.query`` is empty). Built via ``__new__`` + manual attribute setup
    — the same pattern the forwarding unit test uses — so we exercise
    ``catalogQuery`` without ``CSVQueryProcessor.__init__``'s path-creation
    machinery, while still pointing ``self.catalog`` at the REAL catalog so the
    genuine ``get_as_file`` runs.

    Args:
        catalog: The real ``ErmrestCatalog`` whose ``get_as_file`` will run.
        rid_set: The RIDs the processor should fetch.
        rid_table: ``"schema:table"`` the RIDs belong to.
        output_abspath: Real on-disk path the CSV is written to.

    Returns:
        A ready-to-run ``BaseQueryProcessor``.
    """
    proc = BaseQueryProcessor.__new__(BaseQueryProcessor)
    proc.parameters = {"rid_set": rid_set, "rid_table": rid_table}
    proc.envars = {}
    proc.rid_set = proc.parameters.get("rid_set")
    proc.rid_table = proc.parameters.get("rid_table")
    proc.query = ""  # Format B: no query_path at all.
    proc.output_abspath = output_abspath
    proc.paged_query = False
    proc.paged_query_size = 100000
    proc.paged_query_sort_columns = ["RID"]
    proc.HEADERS = {}
    proc.callback = None
    proc.catalog = catalog
    # __init__ sets ``sessions`` (used by __del__); the __new__ build skips it,
    # so set it here to keep teardown clean.
    proc.sessions = {}
    return proc


def test_format_b_processor_writes_one_clean_csv_end_to_end():
    """spec(rid_set) -> processor -> REAL get_as_file -> ONE clean CSV on disk.

    Drives the full Format-B chain end-to-end with only ``_session.get`` mocked:

    1. ``proc.catalogQuery()`` runs (FIX 2: the empty-query guard does NOT
       short-circuit because ``self.rid_set`` is populated).
    2. It calls the REAL ``ErmrestCatalog.get_as_file`` with the rid_set, which
       chunk-appends across 2 chunks (3 RIDs, ``RID_SET_CHUNK_SIZE`` patched to
       2) into one CSV.
    3. The resulting file has the header exactly once and all three rows.

    This is the test that would have caught FIX 2: ``get_as_file`` is the
    genuine implementation, so a real CSV is produced — not a captured-kwargs
    mock.
    """
    header = "RID,Name"
    pages = {
        "/entity/S:T/RID=any(r1,r2)": _csv(header, ["r1,Alice", "r2,Bob"]),
        "/entity/S:T/RID=any(r3)": _csv(header, ["r3,Carol"]),
    }
    catalog = _make_catalog_serving(pages, header)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out = f.name
    try:
        proc = _make_format_b_processor(
            catalog, ["r1", "r2", "r3"], "S:T", out
        )

        # Sanity-check the wiring: the processor calls the genuine bound method,
        # NOT a MagicMock. (A mocked get_as_file would write nothing.)
        assert proc.catalog.get_as_file.__func__ is ErmrestCatalog.get_as_file

        # chunk size 2 -> 2 chunks: [r1,r2], [r3]. Patch the module constant so
        # the chain spans multiple chunks without needing 500+ RIDs, proving the
        # multi-chunk append fires through the processor path.
        orig = ec.RID_SET_CHUNK_SIZE
        ec.RID_SET_CHUNK_SIZE = 2
        # Patch make_dirs (as the forwarding unit test does) so catalogQuery's
        # mkdir-for-output-dir is a no-op for the temp file.
        try:
            with patch(
                "deriva.transfer.download.processors.query."
                "base_query_processor.make_dirs"
            ):
                proc.catalogQuery()
        finally:
            ec.RID_SET_CHUNK_SIZE = orig

        # The REAL get_as_file wrote a real file: header once + all 3 rows.
        assert os.path.exists(out)
        with open(out, encoding="utf-8") as fh:
            content = fh.read()
        assert content.count("RID,Name") == 1  # header exactly once
        for rid in ("r1", "r2", "r3"):
            assert rid in content  # every chunk's row landed
        nonblank = [ln for ln in content.splitlines() if ln.strip()]
        assert len(nonblank) == 4  # 1 header + 3 rows, chunk-appended
        # The mocked session was actually exercised (chain ran, not skipped).
        assert catalog._session.get.called
    finally:
        if os.path.exists(out):
            os.unlink(out)
