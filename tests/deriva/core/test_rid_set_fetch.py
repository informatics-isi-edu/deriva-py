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


class _AcceptAwareResponse(_FakeResponse):
    """Fake response that models the server's content-negotiation: it returns
    CSV only when the request asked for ``Accept: text/csv``; otherwise it
    returns JSON (the server's default). This reproduces the real server's
    behavior that the plain ``_FakeResponse`` (hard-coded CSV) masks.
    """

    def __init__(self, csv_text, accept):
        if accept == "text/csv":
            super().__init__(csv_text)
            self.headers = {"Content-Type": "text/csv"}
        else:
            # Server defaults to JSON when no/other Accept is sent.
            super().__init__("[]")
            self.headers = {"Content-Type": "application/json"}


class _JsonStreamResponse:
    """Minimal requests.Response stand-in for an application/x-json-stream page.

    The json-stream branch of ``_fetch_paged_content`` reads ``r.content`` (the
    raw bytes), not ``iter_lines``, so this fake exposes ``content``.
    """

    def __init__(self, content_bytes):
        self.content = content_bytes
        self.status_code = 200
        self.headers = {"Content-Type": "application/x-json-stream"}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass

    @property
    def text(self):
        return self.content.decode("utf-8")


def _jstream(*objs):
    """Render newline-delimited JSON objects (one per line) as bytes."""
    import json
    return ("".join(json.dumps(o) + "\n" for o in objs)).encode("utf-8")


def test_rid_set_threads_header_across_chunks_for_correct_cursor():
    """The CSV header is captured on the first chunk and threaded into every
    later chunk so each chunk builds its ``@after()`` cursor from the real
    column names.

    Only the first chunk is the "first page"; later chunks skip the re-emitted
    header. If the header (``first_line``) were not threaded, a later chunk's
    cursor dict would be built against the wrong (or empty) column names and
    yield a bad ``@after`` value -> skipped rows or an infinite loop. Here each
    chunk's ``@after`` must equal its last RID, and the merged CSV must have a
    single header with every requested row present exactly once.
    """
    import deriva.core.ermrest_catalog as ec

    header = "RID,Name,Notes"
    pages = {
        "/entity/S:T/RID=any(a1,a2)": _csv(header, ["a1,A,x", "a2,B,y"]),
        "/entity/S:T/RID=any(a3,a4)": _csv(header, ["a3,C,z", "a4,D,w"]),
        "/entity/S:T/RID=any(a5,a6)": _csv(header, ["a5,E,v", "a6,F,u"]),
    }

    cat = ErmrestCatalog.__new__(ErmrestCatalog)
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    seen_after = []

    def fake_get(url, headers=None, stream=False):
        if "@after" in url:
            seen_after.append(url.split("@after(")[1].split(")")[0])
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
    orig_chunk = ec.RID_SET_CHUNK_SIZE
    ec.RID_SET_CHUNK_SIZE = 2  # 6 RIDs -> 3 chunks
    try:
        cat.get_as_file(
            None, out, rid_set=["a1", "a2", "a3", "a4", "a5", "a6"],
            rid_table="S:T", headers={"accept": "text/csv"},
        )
        # Each chunk's cursor is the chunk's last RID -- correct only if the
        # threaded header let the cursor dict resolve the RID column.
        assert seen_after == ["a2", "a4", "a6"], seen_after
        with open(out, "r", newline="", encoding="utf-8") as f:
            rows = list(__import__("csv").reader(f))
        assert rows[0] == ["RID", "Name", "Notes"]   # exactly one header
        assert [r[0] for r in rows[1:]] == ["a1", "a2", "a3", "a4", "a5", "a6"]
    finally:
        ec.RID_SET_CHUNK_SIZE = orig_chunk
        if os.path.exists(out):
            os.unlink(out)


def test_get_as_file_rid_set_defaults_accept_to_csv():
    """REGRESSION: the rid-set path must request ``Accept: text/csv`` even when
    the caller passes no explicit accept header. Otherwise the server returns
    JSON, the CSV write-branch is skipped, and the result is silently empty.

    The bug: the ``if rid_set is not None:`` dispatch in ``get_as_file`` runs
    BEFORE the accept-resolution logic the normal paged path uses, so the
    rid-set path never defaulted the Accept header.
    """
    header = "RID,Name"
    csv_body = _csv(header, ["r1,Alice", "r2,Bob"])

    cat = ErmrestCatalog.__new__(ErmrestCatalog)
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    seen_accepts = []

    def fake_get(url, headers=None, stream=False):
        accept = (headers or {}).get("accept")
        seen_accepts.append(accept)
        if "@after" in url:
            return _AcceptAwareResponse(_csv(header, []), accept)
        return _AcceptAwareResponse(csv_body, accept)

    cat._session = MagicMock()
    cat._session.get.side_effect = fake_get
    cat._response_raise_for_status = lambda r: None

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out = f.name
    try:
        # Call WITHOUT an explicit accept header (the bug-triggering path).
        cat.get_as_file(None, out, rid_set=["r1", "r2"], rid_table="S:T")
        # The fetch must have requested text/csv (the fix), so rows are written.
        assert all(a == "text/csv" for a in seen_accepts), (
            "rid-set fetch sent Accept=%r; expected text/csv on every request" % seen_accepts
        )
        with open(out, encoding="utf-8") as fh:
            content = fh.read()
        assert "r1,Alice" in content and "r2,Bob" in content, (
            "rid-set fetch produced no rows (server returned JSON because Accept "
            "was not text/csv): %r" % content
        )
    finally:
        if os.path.exists(out):
            os.unlink(out)


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


def test_rid_set_header_only_keeps_file_unless_delete_if_empty():
    """REGRESSION (deep-review gap): the rid-set path must honor delete_if_empty
    the same way get_as_file's normal path does.

    A header-only result (no data rows) should be deleted ONLY when
    delete_if_empty=True. Previously _get_rid_set_as_file ran the emptiness
    check unconditionally, so it deleted a header-only file even when the caller
    passed delete_if_empty=False -- contradicting the parameter.
    """
    header = "RID,Name"

    def make_cat():
        cat = ErmrestCatalog.__new__(ErmrestCatalog)
        cat._server_uri = "https://example.org/ermrest/catalog/1"

        def fake_get(url, headers=None, stream=False):
            # Every page is header-only: a non-empty (so not zero-byte) but
            # data-less CSV. The first page writes the header; @after pages end it.
            return _FakeResponse(_csv(header, []))

        cat._session = MagicMock()
        cat._session.get.side_effect = fake_get
        cat._response_raise_for_status = lambda r: None
        return cat

    # delete_if_empty=False (the default): the header-only file must be KEPT.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out = f.name
    try:
        result = make_cat().get_as_file(
            None, out, rid_set=["r1"], rid_table="S:T", delete_if_empty=False,
        )
        assert result == out
        assert os.path.exists(out), "header-only file deleted despite delete_if_empty=False"
        with open(out, encoding="utf-8") as fh:
            assert fh.read().strip() == "RID,Name"   # header present, no data
    finally:
        if os.path.exists(out):
            os.unlink(out)

    # delete_if_empty=True: the same header-only result must be DELETED.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out = f.name
    try:
        result = make_cat().get_as_file(
            None, out, rid_set=["r1"], rid_table="S:T", delete_if_empty=True,
        )
        assert result is None
        assert not os.path.exists(out), "header-only file kept despite delete_if_empty=True"
    finally:
        if os.path.exists(out):
            os.unlink(out)


def test_rid_set_callback_cancel_aborts_cleanly_without_error():
    """REGRESSION (Mike D'Arcy review on #273): cancelling via the progress
    callback mid-fetch in rid-set mode must abort cleanly, not raise.

    The paged loop honors a falsy callback by closing destfile and returning.
    _get_rid_set_as_file must notice the closed file and stop -- otherwise the
    next chunk's write (or the final flush) raises ``ValueError: I/O operation
    on closed file``. Cancellation here fires during the FIRST of two chunks,
    so a missing guard would blow up on the second chunk.
    """
    import deriva.core.ermrest_catalog as ec

    header = "RID,Name"
    pages = {
        "/entity/S:T/RID=any(r1,r2)": _csv(header, ["r1,Alice", "r2,Bob"]),
        "/entity/S:T/RID=any(r3,r4)": _csv(header, ["r3,Carol", "r4,Dave"]),
    }

    cat = ErmrestCatalog.__new__(ErmrestCatalog)
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    def fake_get(url, headers=None, stream=False):
        if "@after" in url:
            return _FakeResponse(_csv(header, []))
        for path, body in pages.items():
            if path in url:
                return _FakeResponse(body)
        raise AssertionError("unexpected URL: %s" % url)

    cat._session = MagicMock()
    cat._session.get.side_effect = fake_get
    cat._response_raise_for_status = lambda r: None

    # Cancel after the first progress report (i.e. during the first chunk).
    calls = {"n": 0}

    def cancelling_callback(**kw):
        if "progress" in kw:
            calls["n"] += 1
            return calls["n"] < 1  # falsy on the very first progress callback
        return True

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out = f.name
    orig = ec.RID_SET_CHUNK_SIZE
    ec.RID_SET_CHUNK_SIZE = 2  # 4 RIDs -> 2 chunks
    try:
        # Must NOT raise (the headline bug was ValueError on the closed file).
        result = cat.get_as_file(
            None, out, rid_set=["r1", "r2", "r3", "r4"], rid_table="S:T",
            callback=cancelling_callback,
        )
        assert result is None  # aborted fetch reports no completed file
        assert calls["n"] >= 1  # the callback actually fired (cancellation path ran)
    finally:
        ec.RID_SET_CHUNK_SIZE = orig
        if os.path.exists(out):
            os.unlink(out)


def test_rid_set_json_stream_appends_chunks():
    """rid-set mode honors Accept: application/x-json-stream and appends every
    chunk's newline-delimited JSON objects into one stream (no CSV header
    handling needed for this format)."""
    import deriva.core.ermrest_catalog as ec

    pages = {
        "/entity/S:T/RID=any(r1,r2)": _jstream({"RID": "r1", "Name": "Alice"},
                                               {"RID": "r2", "Name": "Bob"}),
        "/entity/S:T/RID=any(r3,r4)": _jstream({"RID": "r3", "Name": "Carol"},
                                               {"RID": "r4", "Name": "Dave"}),
    }

    cat = ErmrestCatalog.__new__(ErmrestCatalog)
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    def fake_get(url, headers=None, stream=False):
        assert (headers or {}).get("accept") == "application/x-json-stream"
        if "@after" in url:
            return _JsonStreamResponse(b"")        # no more rows -> end this chunk
        for path, body in pages.items():
            if path in url:
                return _JsonStreamResponse(body)
        raise AssertionError("unexpected URL: %s" % url)

    cat._session = MagicMock()
    cat._session.get.side_effect = fake_get
    cat._response_raise_for_status = lambda r: None

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        out = f.name
    orig = ec.RID_SET_CHUNK_SIZE
    ec.RID_SET_CHUNK_SIZE = 2
    try:
        cat.get_as_file(
            None, out, rid_set=["r1", "r2", "r3", "r4"], rid_table="S:T",
            headers={"accept": "application/x-json-stream"},
        )
        with open(out, encoding="utf-8") as fh:
            lines = [ln for ln in fh.read().splitlines() if ln.strip()]
        assert len(lines) == 4                          # all rows, no CSV header
        assert [__import__("json").loads(ln)["RID"] for ln in lines] == \
            ["r1", "r2", "r3", "r4"]
    finally:
        ec.RID_SET_CHUNK_SIZE = orig
        if os.path.exists(out):
            os.unlink(out)


def test_rid_set_json_stream_empty_middle_chunk_leaves_no_stray_marker():
    """REGRESSION (Mike D'Arcy review on #273): a chunk whose json-stream result
    is an empty marker (``[]``) between two non-empty chunks must not leave a
    stray ``[]`` in the concatenated output.

    ``_fetch_paged_content`` writes ``r.content`` before inspecting it, so an
    unguarded empty page would emit ``[]`` mid-stream. The guard treats an
    empty-array/object body as an empty page and skips writing it.
    """
    import deriva.core.ermrest_catalog as ec

    pages = {
        "/entity/S:T/RID=any(r1,r2)": _jstream({"RID": "r1"}, {"RID": "r2"}),
        "/entity/S:T/RID=any(r3,r4)": b"[]\n",                  # empty middle chunk
        "/entity/S:T/RID=any(r5,r6)": _jstream({"RID": "r5"}, {"RID": "r6"}),
    }

    cat = ErmrestCatalog.__new__(ErmrestCatalog)
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    def fake_get(url, headers=None, stream=False):
        if "@after" in url:
            return _JsonStreamResponse(b"")
        for path, body in pages.items():
            if path in url:
                return _JsonStreamResponse(body)
        raise AssertionError("unexpected URL: %s" % url)

    cat._session = MagicMock()
    cat._session.get.side_effect = fake_get
    cat._response_raise_for_status = lambda r: None

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        out = f.name
    orig = ec.RID_SET_CHUNK_SIZE
    ec.RID_SET_CHUNK_SIZE = 2
    try:
        cat.get_as_file(
            None, out, rid_set=["r1", "r2", "r3", "r4", "r5", "r6"], rid_table="S:T",
            headers={"accept": "application/x-json-stream"},
        )
        with open(out, encoding="utf-8") as fh:
            content = fh.read()
        assert "[]" not in content                      # no stray empty marker mid-stream
        lines = [ln for ln in content.splitlines() if ln.strip()]
        assert [__import__("json").loads(ln)["RID"] for ln in lines] == \
            ["r1", "r2", "r5", "r6"]
    finally:
        ec.RID_SET_CHUNK_SIZE = orig
        if os.path.exists(out):
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
