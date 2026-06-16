"""Tests for CSV paging with multi-line quoted fields (no live catalog).

The paged CSV path in ``get_as_file`` builds the ``@after()`` cursor for the
next page from the last complete record of the current page. The record is
recovered by parsing the page's data lines forward with the csv module, which
tracks quote state across embedded newlines (RFC 4180). Taking the last raw
``\\n``-delimited line instead would, for a record whose quoted field contains
a newline, yield a fragment -> a wrong cursor -> skipped rows or an infinite
re-fetch loop. These tests drive ``get_as_file`` through mocked paged
responses and assert the cursor is byte-exact, including when a multi-line
quoted field straddles a page boundary.
"""

import csv
import io
import os
import tempfile
from unittest.mock import MagicMock

from deriva.core.ermrest_catalog import ErmrestCatalog


def _csv(header, rows):
    """Render a CSV body (header + rows) with RFC 4180 quoting."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()


class _FakeResponse:
    """Minimal requests.Response stand-in for one CSV page."""

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
        # Split on physical newlines, exactly like requests' iter_lines -- a
        # quoted field containing a newline is therefore split across yields,
        # which is the hazard get_as_file's forward parse must absorb.
        for line in self._text.split("\n"):
            if line == "" and self._text.endswith("\n"):
                continue
            yield line if decode_unicode else line.encode("utf-8")

    @property
    def text(self):
        return self._text


def _run_paged_get(pages, page_sort_columns=frozenset(["RID"])):
    """Drive get_as_file in paged CSV mode against a scripted page sequence.

    ``pages`` is a list of CSV bodies returned in order. The cursor each page
    yields is captured from the outgoing ``@after(...)`` URL so the test can
    assert it is the real last record, not a fragment.
    """
    cat = ErmrestCatalog.__new__(ErmrestCatalog)
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    seen_after = []
    state = {"i": 0}

    def fake_get(url, headers=None, stream=False):
        if "@after" in url:
            after = url.split("@after(")[1].split(")")[0]
            seen_after.append(after)
        idx = state["i"]
        state["i"] += 1
        if idx < len(pages):
            return _FakeResponse(pages[idx])
        # Past the last scripted page: empty page (header only) ends the loop.
        return _FakeResponse(pages[-1].split("\n")[0] + "\n")

    cat._session = MagicMock()
    cat._session.get.side_effect = fake_get
    cat._response_raise_for_status = lambda r: None

    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    try:
        cat.get_as_file(
            "/entity/S:T", path,
            headers={"accept": "text/csv"},
            paged=True, page_size=10,
            page_sort_columns=page_sort_columns,
        )
        with open(path, "r", newline="", encoding="utf-8") as f:
            written = f.read()
    finally:
        os.unlink(path)
    return seen_after, written


def test_cursor_from_simple_single_line_records():
    """Plain single-line rows: cursor is the last row's RID."""
    header = ["RID", "Name", "Value"]
    page = _csv(header, [["2-ABC", "Alice", "100"],
                         ["2-DEF", "Bob", "200"],
                         ["2-GHI", "Carol", "300"]])
    seen_after, _ = _run_paged_get([page])
    assert seen_after[0] == "2-GHI"


def test_cursor_with_multiline_quoted_last_field():
    """A multi-line quoted field in the LAST record must not corrupt the cursor."""
    header = ["RID", "Name", "Notes"]
    page = _csv(header, [["2-ABC", "Alice", "Simple"],
                         ["2-GHI", "Carol", "Grid:\n 1 2 3\n 4 5 6\n 7 8 9"]])
    seen_after, _ = _run_paged_get([page])
    # Wrong (raw-last-line) behavior would yield "9" or "" here, not the RID.
    assert seen_after[0] == "2-GHI"


def test_cursor_when_quoted_field_straddles_page_boundary():
    """REGRESSION (Mike D'Arcy review on #274): the record this PR exists to
    handle is one whose quoted multi-line field is large. Each page is parsed
    forward from its own header, so the embedded newlines reassemble into one
    record and the cursor is the record's real RID -- never a front-truncated
    fragment of the quoted field.
    """
    header = ["RID", "Data", "Side"]
    big = "\n".join("  ".join(f"{x:3d}" for x in range(i * 10, i * 10 + 10))
                    for i in range(2000))  # ~140 KB quoted field
    # Page 1 ends on the big-field record; page 2 has plain rows then ends.
    page1 = _csv(header, [["2-AAA", "small", "Left"],
                          ["2-BBB", big, "Right"]])
    page2 = _csv(header, [["2-CCC", "small", "Left"],
                          ["2-DDD", "small", "Right"]])
    seen_after, written = _run_paged_get([page1, page2])
    assert seen_after[0] == "2-BBB"   # not a fragment of the grid
    assert seen_after[1] == "2-DDD"
    # The on-disk file must contain the big field intact (bytes preserved).
    assert big.split("\n")[-1] in written


def test_cursor_honors_non_rid_sort_column():
    """Cursor must use page_sort_columns, not a hardcoded RID, so paging by a
    different column still produces the right @after value.
    """
    header = ["RID", "Name", "Seq"]
    page = _csv(header, [["2-ABC", "Alice", "001"],
                         ["2-DEF", "Bob", "002"]])
    seen_after, _ = _run_paged_get([page], page_sort_columns=["Seq"])
    assert seen_after[0] == "002"


def test_empty_page_terminates_without_cursor():
    """A header-only page yields no data rows, so paging stops and no @after
    cursor is built from a phantom record (which would loop forever).
    """
    header = ["RID", "Name"]
    page = _csv(header, [])  # header only
    seen_after, _ = _run_paged_get([page])
    assert seen_after == []
