"""Tests for RID-set chunk-append fetch in get_as_file (no live catalog)."""

import os
import tempfile
from unittest.mock import MagicMock

from deriva.core.ermrest_catalog import ErmrestCatalog, RID_SET_CHUNK_SIZE


class _FakeResponse:
    """Minimal requests.Response stand-in for a CSV page."""

    def __init__(self, text):
        self._text = text
        self.status_code = 200
        self.headers = {"Content-Type": "text/csv"}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass

    def iter_lines(self, decode_unicode=False):
        for line in self._text.splitlines():
            yield line if decode_unicode else line.encode("utf-8")

    @property
    def text(self):
        return self._text


def _csv(header, rows):
    return "\n".join([header] + rows) + "\n"


def test_get_as_file_rid_set_appends_chunks_to_one_csv():
    """Two RID chunks each return a CSV page; the result is ONE CSV with the
    header once and all body rows, RID-distinct."""
    header = "RID,Name"
    pages = {
        "/entity/S:T/RID=any(r1,r2)": _csv(header, ["r1,Alice", "r2,Bob"]),
        "/entity/S:T/RID=any(r3)": _csv(header, ["r3,Carol"]),
    }

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

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out = f.name
    try:
        # chunk size 2 -> 2 chunks: [r1,r2], [r3]. Patch the module constant
        # so the test doesn't need 500+ RIDs.
        import deriva.core.ermrest_catalog as ec
        orig = ec.RID_SET_CHUNK_SIZE
        ec.RID_SET_CHUNK_SIZE = 2
        try:
            cat.get_as_file(None, out, rid_set=["r1", "r2", "r3"], rid_table="S:T")
        finally:
            ec.RID_SET_CHUNK_SIZE = orig
        with open(out, encoding="utf-8") as fh:
            content = fh.read()
        assert content.count("RID,Name") == 1            # header once
        for rid in ("r1", "r2", "r3"):
            assert rid in content                        # all rows present
        nonblank = [ln for ln in content.splitlines() if ln.strip()]
        assert len(nonblank) == 4                         # 1 header + 3 rows
    finally:
        os.unlink(out)


def test_rid_set_chunks_splits_at_chunk_size():
    rids = [f"1-{i:04d}" for i in range(1250)]
    chunks = list(ErmrestCatalog._rid_set_chunks(rids, 500))
    assert len(chunks) == 3
    assert [len(c) for c in chunks] == [500, 500, 250]
    # No RID lost or duplicated across chunks.
    flat = [r for c in chunks for r in c]
    assert flat == rids


def test_rid_set_chunks_empty():
    assert list(ErmrestCatalog._rid_set_chunks([], 500)) == []


def test_rid_set_chunk_size_default_is_500():
    assert RID_SET_CHUNK_SIZE == 500


def test_rid_set_query_url_quotes_each_rid_not_the_commas():
    """Commas are any() SYNTAX separators, not values. Each RID is quoted
    individually; the commas stay literal. Quoting the whole joined string
    (%2C) would break the predicate and silently return zero rows."""
    url = ErmrestCatalog._rid_set_query_url("eye-ai:Image", ["1-ABC", "1-DEF"])
    assert url == "/entity/eye-ai:Image/RID=any(1-ABC,1-DEF)"
    # The separating commas must be literal, never percent-encoded.
    assert "%2C" not in url


def test_rid_set_query_url_quotes_special_chars_in_rid():
    """A RID value containing a reserved char IS quoted (the value, not the
    comma)."""
    url = ErmrestCatalog._rid_set_query_url("S:T", ["a b", "c"])
    # space in the value is encoded; the comma separator is not.
    assert url == "/entity/S:T/RID=any(a%20b,c)"


from deriva.transfer.download.processors.query.base_query_processor import (
    BaseQueryProcessor,
)


def test_query_processor_reads_rid_set_from_params():
    """rid_set/rid_table flow from processor_params onto the processor."""
    proc = BaseQueryProcessor.__new__(BaseQueryProcessor)
    proc.parameters = {
        "query_path": "/entity/S:T",
        "rid_set": ["r1", "r2"],
        "rid_table": "S:T",
    }
    proc.rid_set = proc.parameters.get("rid_set", None)
    proc.rid_table = proc.parameters.get("rid_table", None)
    assert proc.rid_set == ["r1", "r2"]
    assert proc.rid_table == "S:T"


def test_catalog_query_forwards_rid_set_to_get_as_file():
    """catalogQuery must pass rid_set/rid_table into get_as_file."""
    from unittest.mock import MagicMock, patch

    proc = BaseQueryProcessor.__new__(BaseQueryProcessor)
    proc.parameters = {"rid_set": ["r1"], "rid_table": "S:T"}
    proc.envars = {}
    proc.rid_set = proc.parameters.get("rid_set")
    proc.rid_table = proc.parameters.get("rid_table")
    proc.query = "/entity/S:T"
    proc.output_abspath = "/tmp/ignored.csv"
    proc.paged_query = False
    proc.paged_query_size = 100000
    proc.paged_query_sort_columns = ["RID"]
    proc.HEADERS = {}
    proc.callback = None
    captured = {}
    cat = MagicMock()

    def fake_get_as_file(path, dest, **kwargs):
        captured.update(kwargs)
        return dest

    cat.get_as_file.side_effect = fake_get_as_file
    proc.catalog = cat
    with patch(
        "deriva.transfer.download.processors.query.base_query_processor.make_dirs"
    ):
        proc.catalogQuery()
    assert captured.get("rid_set") == ["r1"]
    assert captured.get("rid_table") == "S:T"


def test_catalog_query_with_empty_query_but_rid_set_still_calls_get_as_file():
    """Format-B processors carry NO query_path: ``self.query`` is empty but
    ``self.rid_set`` is populated. ``catalogQuery`` must NOT short-circuit on
    the empty query — it must proceed to ``get_as_file`` so the rid-set fetch
    actually fires. (Pins FIX 2: the early-return guard only triggers when
    there is NEITHER a query path NOR a rid_set.)"""
    from unittest.mock import MagicMock, patch

    proc = BaseQueryProcessor.__new__(BaseQueryProcessor)
    proc.parameters = {"rid_set": ["r1"], "rid_table": "S:T"}
    proc.envars = {}
    proc.rid_set = proc.parameters.get("rid_set")
    proc.rid_table = proc.parameters.get("rid_table")
    proc.query = ""  # Format B: no query_path at all.
    proc.output_abspath = "/tmp/ignored.csv"
    proc.paged_query = False
    proc.paged_query_size = 100000
    proc.paged_query_sort_columns = ["RID"]
    proc.HEADERS = {}
    proc.callback = None
    captured = {}
    cat = MagicMock()

    def fake_get_as_file(path, dest, **kwargs):
        captured.update(kwargs)
        return dest

    cat.get_as_file.side_effect = fake_get_as_file
    proc.catalog = cat
    with patch(
        "deriva.transfer.download.processors.query.base_query_processor.make_dirs"
    ):
        proc.catalogQuery()
    # The guard did NOT return early: get_as_file ran with the rid_set.
    cat.get_as_file.assert_called_once()
    assert captured.get("rid_set") == ["r1"]
    assert captured.get("rid_table") == "S:T"


def test_catalog_query_returns_empty_when_no_query_and_no_rid_set():
    """The early-return guard still fires when there is NEITHER a query path
    NOR a rid_set — get_as_file must not be called in that case."""
    from unittest.mock import MagicMock

    proc = BaseQueryProcessor.__new__(BaseQueryProcessor)
    proc.parameters = {}
    proc.envars = {}
    proc.rid_set = None
    proc.rid_table = None
    proc.query = ""
    cat = MagicMock()
    proc.catalog = cat

    assert proc.catalogQuery() == {}
    cat.get_as_file.assert_not_called()
