# RID-Set Bag Generation (deriva-py, Plan B1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the bag export engine fetch a large explicit RID set as ONE logical query that produces ONE clean CSV per table — eliminating the deep-FK-join cost and the per-path CSV fragmentation.

**Architecture:** Add an optional `rid_set` + `rid_table` to `ErmrestCatalog.get_as_file`: when present, it chunks the RIDs into URL-safe batches (~500), fetches `/entity/{rid_table}/RID=any(...)` per chunk (each RID individually URL-quoted), and appends every chunk to one CSV by reusing the existing per-page header-skip append loop. `CSVQueryProcessor` passes `rid_set`/`rid_table` through from `processor_params`. `CatalogBagBuilder` emits one rid-set csv processor per reached table from a supplied `{table: rids}` map, replacing the per-FK-path emission. The SQLite loader needs no change (one CSV per table is a strict subset of what it already handles).

**Tech Stack:** Python, deriva-py (`deriva/core/ermrest_catalog.py`, `deriva/transfer/download/processors/query/base_query_processor.py`, `deriva/bag/catalog_builder.py`), pytest. Tests mock the HTTP session / catalog model (no live catalog), mirroring `tests/deriva/core/test_csv_paging.py` and `tests/deriva/bag/test_catalog_builder.py`.

---

## Scope: this is Plan B1 of two (deriva-py side)

Stage B spans two repos. **This plan is the deriva-py half** and ships as its own deriva-py PR on the `deriva-ml` branch (the branch deriva-ml pins). It produces working, testable software on its own: the `rid_set` engine capability + rid-set spec emission, with upstream contract tests. **Plan B2 (deriva-ml)** — written after this lands — bumps the deriva-py pin and wires `DatasetBagBuilder` → `compute_reachability` → Format-B export spec.

Design: `deriva-ml/docs/superpowers/specs/2026-06-14-stage-b-fast-portable-bag-design.md` (decisions D1–D4) + `2026-06-14-portable-bag-csv-contract.md` (the CSV contract). Proven prototype + gotchas: deriva-ml memory `csv-ridset-chunk-append-proto.md`.

**The per-RID-quoting gotcha (load-bearing, pinned by Task 2):** in `RID=any(a,b,c)` the commas are ERMrest *syntax separators*, not part of the values. Quote each RID individually (`",".join(urlquote(r) for r in batch)`), NOT `urlquote(",".join(batch))` — the latter encodes commas to `%2C`, breaks the predicate, and **silently returns zero rows**.

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `deriva/core/ermrest_catalog.py` | `get_as_file` gains `rid_set`/`rid_table`; new private `_rid_set_chunks` helper; the chunk loop wraps the existing paged fetch with a persistent `first_page` flag. | **Modify** (`get_as_file` ~606; add helper) |
| `tests/deriva/core/test_rid_set_fetch.py` | Unit tests for the chunking helper (pure) + the chunk-append behavior (mocked session): header-once, RID-distinct, complete, per-RID-quoting. | **Create** |
| `deriva/transfer/download/processors/query/base_query_processor.py` | `BaseQueryProcessor.__init__` reads `rid_set`/`rid_table` from `processor_params`; `catalogQuery` passes them to `get_as_file`. | **Modify** (~39, ~74) |
| `deriva/bag/catalog_builder.py` | `_build_export_spec` emits one rid-set csv processor per reached table from a supplied `{(schema,table): rids}` map, replacing the `for fk_path in self._fk_paths_for(key)` per-path emission. New `rid_sets` attribute + constructor/`get_export_spec` param. | **Modify** (~495, ~584) |
| `tests/deriva/bag/test_catalog_builder.py` | Extend: when `rid_sets` is supplied, the spec has one csv processor per table carrying `rid_table`+`rid_set`, not one-per-FK-path. | **Modify** |

The SQLite loader (`deriva/bag/database.py` / `loader.py`) is intentionally **not** modified: it already groups CSVs by `{schema}.{table}` basename and unions by RID via `ON CONFLICT`. One clean CSV per table is a strict subset of that — it loads correctly with zero change. (Plan B2's integration test confirms the round-trip.)

---

## Interface (locked here so all tasks agree)

```python
# deriva/core/ermrest_catalog.py

def get_as_file(self, path, destfilename, headers=DEFAULT_HEADERS, callback=None,
                delete_if_empty=False, paged=False, page_size=DEFAULT_PAGE_SIZE,
                page_sort_columns=frozenset(["RID"]),
                rid_set=None, rid_table=None):
    """... existing behavior unchanged when rid_set is None ...

    When ``rid_set`` (a list/iterable of RID strings) is provided, ``path`` is
    ignored and the data is fetched by chunking ``rid_set`` into URL-safe
    batches and querying ``/entity/{rid_table}/RID=any(...)`` per batch,
    appending all batches to one CSV. ``rid_table`` (``"schema:table"``) is
    required when ``rid_set`` is given. Forces paged CSV semantics internally.
    """

RID_SET_CHUNK_SIZE = 500   # module constant; URL-safe batch size
```

`CatalogBagBuilder` gains:
```python
# deriva/bag/catalog_builder.py
def __init__(self, ..., rid_sets: dict[tuple[str, str], list[str]] | None = None):
    ...
    self.rid_sets = rid_sets   # {(schema, table): [RID, ...]} or None
```
When `rid_sets` is not None, `_build_export_spec` emits one rid-set csv processor per reached table (using `rid_sets[key]`) instead of per-FK-path processors. When None, current per-path behavior is unchanged (so existing callers and tests keep working until B2 supplies rid_sets).

---

### Task 1: RID-set chunking helper (pure)

**Files:**
- Modify: `deriva/core/ermrest_catalog.py` (add module constant + `_rid_set_chunks` staticmethod)
- Test: `tests/deriva/core/test_rid_set_fetch.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/deriva/core/test_rid_set_fetch.py
"""Tests for RID-set chunk-append fetch in get_as_file (no live catalog)."""

from deriva.core.ermrest_catalog import ErmrestCatalog, RID_SET_CHUNK_SIZE


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py -v`
Expected: FAIL — `ImportError: cannot import name 'RID_SET_CHUNK_SIZE'` (and `_rid_set_chunks` missing)

- [ ] **Step 3: Write minimal implementation**

Add the module constant near the other module-level constants at the top of `deriva/core/ermrest_catalog.py` (after the imports):

```python
RID_SET_CHUNK_SIZE = 500
"""URL-safe batch size for RID-set fetches. A ``RID=any(...)`` filter over
thousands of RIDs exceeds the server's URI length limit (a 225k-RID URL is
~1.9 MB → HTTP 414); chunking keeps each request URL well within limits."""
```

Add the static helper as a method on `ErmrestCatalog` (place near `_read_last_csv_record`):

```python
    @staticmethod
    def _rid_set_chunks(rid_set, chunk_size):
        """Yield successive ``chunk_size``-length lists from ``rid_set``.

        Args:
            rid_set: Iterable of RID strings.
            chunk_size: Max RIDs per chunk.

        Yields:
            Lists of at most ``chunk_size`` RIDs, preserving order, no RID
            lost or duplicated.
        """
        rids = list(rid_set)
        for i in range(0, len(rids), chunk_size):
            yield rids[i:i + chunk_size]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/carl/GitHub/deriva-py && git add deriva/core/ermrest_catalog.py tests/deriva/core/test_rid_set_fetch.py && git commit -m "feat(ermrest): RID-set chunking helper for bag fetch"
```

---

### Task 2: RID-set URL builder (per-RID quoting — the gotcha)

**Files:**
- Modify: `deriva/core/ermrest_catalog.py` (add `_rid_set_query_url` staticmethod)
- Test: `tests/deriva/core/test_rid_set_fetch.py`

This isolates the load-bearing per-RID-quoting rule into a pure, directly-tested function so the gotcha can never silently regress.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/deriva/core/test_rid_set_fetch.py
from deriva.core.ermrest_catalog import ErmrestCatalog


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py::test_rid_set_query_url_quotes_each_rid_not_the_commas -v`
Expected: FAIL — `AttributeError: ... has no attribute '_rid_set_query_url'`

- [ ] **Step 3: Write minimal implementation**

`urlquote` is already imported at the top of the module (`from . import urlquote, ...`). Add:

```python
    @staticmethod
    def _rid_set_query_url(rid_table, rid_chunk):
        """Build a ``/entity/{rid_table}/RID=any(...)`` path for one RID chunk.

        Each RID *value* is URL-quoted individually; the comma separators are
        ``any()`` syntax and stay literal. Quoting the joined string instead
        (encoding commas to ``%2C``) breaks the predicate and silently returns
        zero rows — the bug this function exists to prevent.

        Args:
            rid_table: ``"schema:table"`` of the table being queried.
            rid_chunk: List of RID strings (one URL-safe batch).

        Returns:
            Catalog-relative path string starting at ``/entity/``.
        """
        joined = ",".join(urlquote(str(rid)) for rid in rid_chunk)
        return "/entity/%s/RID=any(%s)" % (rid_table, joined)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/carl/GitHub/deriva-py && git add deriva/core/ermrest_catalog.py tests/deriva/core/test_rid_set_fetch.py && git commit -m "feat(ermrest): per-RID-quoted RID-set query URL builder"
```

---

### Task 3: get_as_file rid_set chunk-append (the core)

**Files:**
- Modify: `deriva/core/ermrest_catalog.py` (`get_as_file` signature + chunk loop)
- Test: `tests/deriva/core/test_rid_set_fetch.py`

The core change. When `rid_set` is provided, wrap the existing paged-CSV fetch in an outer loop over chunks. The `first_page` flag persists ACROSS chunks (only the very first page of the very first chunk writes a header), so all chunks append into one clean CSV. Within a chunk, the existing `@after(RID)` paging still applies (a 500-RID `any()` result can itself exceed `page_size`).

The implementation strategy that minimizes risk: extract the existing paged-fetch inner body into a helper `_fetch_paged_into(destfile, base_path, headers, page_size, page_sort_columns, first_page)` that returns the updated `(first_page, total_so_far)`, then call it once per chunk (or once for the non-rid_set path). Because the existing paged loop is long and subtle (runtime-limit backoff, multi-line CSV `_read_last_csv_record`), the helper preserves it verbatim — only its entry/exit (the `first_page` seed and the destfile open) move out.

- [ ] **Step 1: Write the failing test**

This test mocks `_session.get` to serve canned CSV pages per RID-chunk URL, and asserts the chunk-append produces one clean CSV. It mirrors the mocked-session style.

```python
# add to tests/deriva/core/test_rid_set_fetch.py
import os
import tempfile
from unittest.mock import MagicMock, patch

from deriva.core.ermrest_catalog import ErmrestCatalog


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
    # 3 RIDs, chunk size 2 -> 2 chunks: [r1, r2], [r3].
    header = "RID,Name"
    pages = {
        "/entity/S:T/RID=any(r1,r2)": _csv(header, ["r1,Alice", "r2,Bob"]),
        "/entity/S:T/RID=any(r3)": _csv(header, ["r3,Carol"]),
    }

    cat = ErmrestCatalog.__new__(ErmrestCatalog)  # bypass __init__/network
    cat._server_uri = "https://example.org/ermrest/catalog/1"

    def fake_get(url, headers=None, stream=False):
        # url ends with one of the page paths; serve the matching CSV.
        for path, body in pages.items():
            if url.endswith(path):
                return _FakeResponse(body)
        raise AssertionError("unexpected URL: %s" % url)

    cat._session = MagicMock()
    cat._session.get.side_effect = fake_get
    cat._response_raise_for_status = lambda r: None

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out = f.name
    try:
        cat.get_as_file(
            None, out, rid_set=["r1", "r2", "r3"], rid_table="S:T",
        )
        with open(out, encoding="utf-8") as fh:
            content = fh.read()
        # Header appears exactly once.
        assert content.count("RID,Name") == 1
        # All three body rows present.
        for rid in ("r1", "r2", "r3"):
            assert rid in content
        # RID-distinct: 1 header + 3 data lines.
        nonblank = [ln for ln in content.splitlines() if ln.strip()]
        assert len(nonblank) == 4
    finally:
        os.unlink(out)
```

Note: this test exercises the chunking + append + header-skip with `page_size` large enough that each chunk is a single page (so the within-chunk `@after` loop terminates after one page). The within-chunk multi-page path is covered by the existing `test_csv_paging.py` machinery (the same loop body), so this task does not re-test paging itself — only the chunk-append wrapper.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py::test_get_as_file_rid_set_appends_chunks_to_one_csv -v`
Expected: FAIL — `get_as_file` doesn't accept `rid_set` (TypeError: unexpected keyword argument)

- [ ] **Step 3: Write minimal implementation**

In `get_as_file`, add `rid_set=None, rid_table=None` to the signature (after `page_sort_columns`). At the top of the method body, validate and branch:

```python
        if rid_set is not None:
            if not rid_table:
                raise ValueError("rid_table is required when rid_set is provided")
            return self._get_rid_set_as_file(
                rid_set, rid_table, destfilename, headers=headers,
                callback=callback, delete_if_empty=delete_if_empty,
                page_size=page_size, page_sort_columns=page_sort_columns,
            )
```

Add the new method `_get_rid_set_as_file`. It opens the destfile ONCE, then loops chunks, running the existing paged-CSV fetch body per chunk with a `first_page` flag that persists across chunks. To avoid duplicating the long paged loop, refactor: extract the body of the existing `else:` (paged) branch — from `first_page = True` through the page `while True:` loop — into a method `_fetch_paged_csv(destfile, base_path, headers, callback, page_size, page_sort_columns, first_page)` that takes the open destfile and the starting `first_page`, runs the page loop for ONE base path, and returns `(first_page, total_written)`. The existing `get_as_file` paged branch then calls it with `first_page=True` for the single path; `_get_rid_set_as_file` calls it once per chunk, threading `first_page` through:

```python
    def _get_rid_set_as_file(self, rid_set, rid_table, destfilename, *, headers,
                             callback, delete_if_empty, page_size, page_sort_columns):
        """Fetch a RID set as one CSV by chunk-append (see RID_SET_CHUNK_SIZE)."""
        destfile = open(destfilename, 'w+b')
        try:
            first_page = True
            total = 0
            for chunk in self._rid_set_chunks(rid_set, RID_SET_CHUNK_SIZE):
                base_path = self._rid_set_query_url(rid_table, chunk)
                first_page, written = self._fetch_paged_csv(
                    destfile, base_path, headers, callback,
                    page_size, page_sort_columns, first_page,
                )
                total += written
            destfile.flush()
            # delete_if_empty: a header-only (or empty) CSV is "empty".
            delete_file = (total == 0)
            if delete_if_empty and total > 0:
                destfile.seek(0)
                reader = csv.reader(codecs.iterdecode(destfile, 'utf-8'))
                rowcount = sum(1 for _ in reader)
                delete_file = rowcount <= 1
        finally:
            destfile.close()
        if delete_file and os.path.exists(destfilename):
            os.remove(destfilename)
            return None
        return destfilename
```

Refactor the existing paged branch into `_fetch_paged_csv(self, destfile, base_path, headers, callback, page_size, page_sort_columns, first_page)`: move the code that is currently inside `get_as_file`'s `else:` (paged) branch — the `first_page`/`first_line`/`last_record` setup and the `while True:` page loop — into this method, parameterized by the supplied `destfile` and the incoming `first_page` (instead of always starting True), querying `base_path` (instead of `path`). It returns `(first_page, total)`. Keep the runtime-limit backoff, the CSV/JSON-stream branches, and the `_read_last_csv_record` call EXACTLY as they are — only the entry seed (`first_page` param) and the path source (`base_path`) change. Then in `get_as_file`'s own paged branch, replace the inlined loop with:

```python
            else:
                _first_page, total = self._fetch_paged_csv(
                    destfile, path, headers, callback,
                    page_size, page_sort_columns, True,
                )
```

> **Implementer note:** this refactor is the delicate part. Do it in two moves: (1) extract `_fetch_paged_csv` verbatim and make `get_as_file`'s paged branch call it (run the FULL existing suite — `tests/deriva/core/test_csv_paging.py` plus any get_as_file integration tests — to prove the extraction is behavior-preserving BEFORE adding rid_set). (2) Then add `_get_rid_set_as_file` + the signature branch. If the extraction can't be made cleanly behavior-preserving, STOP and report — do not force it.

- [ ] **Step 4: Run test to verify it passes (and the extraction didn't break paging)**

Run:
```bash
cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py tests/deriva/core/test_csv_paging.py -v
```
Expected: the new rid_set test PASSES and ALL existing `test_csv_paging.py` tests still PASS (proving the extraction preserved behavior).

- [ ] **Step 5: Commit**

```bash
cd /Users/carl/GitHub/deriva-py && git add deriva/core/ermrest_catalog.py tests/deriva/core/test_rid_set_fetch.py && git commit -m "feat(ermrest): get_as_file rid_set chunk-append into one CSV"
```

---

### Task 4: CSVQueryProcessor passes rid_set/rid_table through

**Files:**
- Modify: `deriva/transfer/download/processors/query/base_query_processor.py` (`__init__` ~39, `catalogQuery` ~74)
- Test: `tests/deriva/core/test_rid_set_fetch.py`

- [ ] **Step 1: Write the failing test**

```python
# add to tests/deriva/core/test_rid_set_fetch.py
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
    # Replicate the __init__ param reads (the lines under test).
    proc.rid_set = proc.parameters.get("rid_set", None)
    proc.rid_table = proc.parameters.get("rid_table", None)
    assert proc.rid_set == ["r1", "r2"]
    assert proc.rid_table == "S:T"
```

This test pins the param names; the real wiring is verified in Step 4 by a smoke import + a `catalogQuery` argument-forwarding check below.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py::test_query_processor_reads_rid_set_from_params -v`
Expected: PASS trivially (the test replicates the reads). The REAL gate is that the processor `__init__` and `catalogQuery` actually do this — implement that now and confirm via the forwarding assertion in Step 3's test.

Add a second, behavior-pinning test that fails until the wiring exists:

```python
# add to tests/deriva/core/test_rid_set_fetch.py
def test_catalog_query_forwards_rid_set_to_get_as_file():
    """catalogQuery must pass rid_set/rid_table into get_as_file."""
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
    # Avoid mkdir on a real path.
    with patch(
        "deriva.transfer.download.processors.query.base_query_processor.make_dirs"
    ):
        proc.catalogQuery()
    assert captured.get("rid_set") == ["r1"]
    assert captured.get("rid_table") == "S:T"
```

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py::test_catalog_query_forwards_rid_set_to_get_as_file -v`
Expected: FAIL — `catalogQuery` doesn't pass `rid_set`/`rid_table` yet (captured lacks them).

- [ ] **Step 3: Write minimal implementation**

In `BaseQueryProcessor.__init__`, after the existing `self.paged_query_*` reads (~line 41), add:

```python
        self.rid_set = self.parameters.get("rid_set", None)
        self.rid_table = self.parameters.get("rid_table", None)
```

In `catalogQuery`, in the `get_as_file` call (~line 74), add the two kwargs:

```python
                return self.catalog.get_as_file(self.query, self.output_abspath,
                                                headers=headers,
                                                callback=self.callback,
                                                delete_if_empty=True,
                                                paged=self.paged_query,
                                                page_size=self.paged_query_size,
                                                page_sort_columns=self.paged_query_sort_columns,
                                                rid_set=self.rid_set,
                                                rid_table=self.rid_table)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/core/test_rid_set_fetch.py -v`
Expected: PASS (all rid_set tests incl. the forwarding one)

- [ ] **Step 5: Commit**

```bash
cd /Users/carl/GitHub/deriva-py && git add deriva/transfer/download/processors/query/base_query_processor.py tests/deriva/core/test_rid_set_fetch.py && git commit -m "feat(download): CSVQueryProcessor forwards rid_set/rid_table to get_as_file"
```

---

### Task 5: CatalogBagBuilder emits rid-set processors when rid_sets supplied

**Files:**
- Modify: `deriva/bag/catalog_builder.py` (`__init__`, `_build_export_spec` ~495/584)
- Test: `tests/deriva/bag/test_catalog_builder.py`

When `rid_sets` is supplied, emit ONE rid-set csv processor per reached table (flat `output_path`, carrying `rid_table` + `rid_set`) instead of one-per-FK-path. When `rid_sets` is None, the current per-path behavior is untouched.

- [ ] **Step 1: Write the failing test**

First READ `tests/deriva/bag/test_catalog_builder.py` to reuse its existing mock-catalog/model fixtures (`_make_mock_table`, the builder construction helper). Then add a test that constructs a builder with `rid_sets` and asserts the spec shape. Use the file's existing fixture style — the test below shows the assertion contract; adapt the builder construction to match the file's helpers:

```python
# add to tests/deriva/bag/test_catalog_builder.py
def test_rid_set_spec_emits_one_csv_processor_per_table(<existing fixture args>):
    """With rid_sets supplied, the export spec has ONE csv processor per
    reached table carrying rid_table+rid_set, not one-per-FK-path."""
    # Build a mock model where table ('S','Image') is reached via TWO FK paths
    # (so the per-path emission would produce 2 processors).
    builder = <construct CatalogBagBuilder with the mock catalog/model,
               anchors, policy AS THE EXISTING TESTS DO>,
              rid_sets={("S", "Image"): ["r1", "r2", "r3"]})
    spec = builder.get_export_spec()
    csv_procs = [
        p for p in spec["catalog"]["query_processors"]
        if p["processor"] == "csv"
        and p["processor_params"].get("output_path", "").endswith("Image")
    ]
    # Exactly ONE Image csv processor (not one-per-FK-path).
    assert len(csv_procs) == 1
    params = csv_procs[0]["processor_params"]
    assert params["rid_table"] == "S:Image"
    assert params["rid_set"] == ["r1", "r2", "r3"]
    assert "query_path" not in params       # rid-set replaces query_path
    assert params["output_path"] == "S/Image"   # flat, one file per table
```

> **Implementer note:** read the existing tests in `test_catalog_builder.py` to find the exact fixture/helper names and the builder constructor signature; mirror them. Do not invent fixture names — use what the file already provides. If the builder's constructor doesn't yet accept `rid_sets`, that's what Step 3 adds.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/bag/test_catalog_builder.py::test_rid_set_spec_emits_one_csv_processor_per_table -v`
Expected: FAIL — `CatalogBagBuilder` doesn't accept `rid_sets` (TypeError) or doesn't emit the rid-set shape.

- [ ] **Step 3: Write minimal implementation**

In `CatalogBagBuilder.__init__`, add a `rid_sets=None` parameter and store `self.rid_sets = rid_sets`.

In `_build_export_spec`, in the per-reached-table loop, branch on `self.rid_sets`. The current code (~line 584) is:

```python
                for fk_path in self._fk_paths_for(key):
                    qpath = self._table_query_path(schema_name, table_name, anchor_rid_filter, fk_path)
                    dest = self._output_path_for(schema_name, table_name, fk_path)
                    query_processors.append({
                        "processor": "csv",
                        "processor_params": {
                            "query_path": qpath,
                            "output_path": dest,
                            "paged_query": True,
                        },
                    })
```

Wrap it:

```python
                if self.rid_sets is not None:
                    # Format B: one rid-set csv processor per table — flat
                    # output_path, RID set carried inline. The engine chunks
                    # and appends to one clean CSV (see get_as_file rid_set).
                    rids = self.rid_sets.get(key, [])
                    query_processors.append({
                        "processor": "csv",
                        "processor_params": {
                            "rid_table": "%s:%s" % (schema_name, table_name),
                            "rid_set": rids,
                            "output_path": "%s/%s" % (schema_name, table_name),
                            "paged_query": True,
                        },
                    })
                else:
                    for fk_path in self._fk_paths_for(key):
                        qpath = self._table_query_path(schema_name, table_name, anchor_rid_filter, fk_path)
                        dest = self._output_path_for(schema_name, table_name, fk_path)
                        query_processors.append({
                            "processor": "csv",
                            "processor_params": {
                                "query_path": qpath,
                                "output_path": dest,
                                "paged_query": True,
                            },
                        })
```

> **Implementer note:** match the exact local variable names (`schema_name`, `table_name`, `key`, `anchor_rid_filter`) the surrounding method actually uses — READ the method first; the names above are from the spec inspection but verify against the live code. The vocab-full-export branch and the asset `fetch` branch are UNCHANGED (assets still emit a fetch processor; rid_sets only changes the non-vocab csv emission).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/bag/test_catalog_builder.py -v`
Expected: PASS — the new rid-set test passes AND all existing per-path tests still pass (rid_sets=None default keeps old behavior).

- [ ] **Step 5: Commit**

```bash
cd /Users/carl/GitHub/deriva-py && git add deriva/bag/catalog_builder.py tests/deriva/bag/test_catalog_builder.py && git commit -m "feat(bag): CatalogBagBuilder emits rid-set csv processors when rid_sets supplied"
```

---

### Task 6: Full suite, lint, PR

**Files:** none (verification + PR)

- [ ] **Step 1: Run the bag + core suites**

Run:
```bash
cd /Users/carl/GitHub/deriva-py && uv run pytest tests/deriva/bag/ tests/deriva/core/test_csv_paging.py tests/deriva/core/test_rid_set_fetch.py -v
```
Expected: all pass. The existing bag tests prove the `rid_sets=None` default path is untouched; the new tests prove the rid_set path. If the repo uses a different runner than `uv` (check `Makefile`/`README` — deriva-py may use plain `pytest` or `python -m pytest`), use the repo's convention.

- [ ] **Step 2: Lint (match the repo's tooling)**

READ the repo's lint config (`setup.cfg`, `Makefile`, any `.flake8`/`ruff.toml`). Run whatever the repo uses (deriva-py historically uses `flake8`; if a `Makefile` `lint` target exists, use it):
```bash
cd /Users/carl/GitHub/deriva-py && make lint 2>/dev/null || python -m flake8 deriva/core/ermrest_catalog.py deriva/transfer/download/processors/query/base_query_processor.py deriva/bag/catalog_builder.py
```
Expected: clean on the touched files. Fix any new warnings this change introduced (don't fix pre-existing warnings on untouched lines).

- [ ] **Step 3: Push the branch and open the deriva-py PR**

This work lands on the `deriva-ml` branch (the branch deriva-ml pins). Confirm the current branch, then push and PR:

```bash
cd /Users/carl/GitHub/deriva-py && git status --short && git branch --show-current
```

If on `deriva-ml` directly, create a feature branch off it for the PR:
```bash
cd /Users/carl/GitHub/deriva-py && git checkout -b feature/rid-set-bag-generation && git push -u origin feature/rid-set-bag-generation
gh pr create --base deriva-ml --title "feat(bag): RID-set fetch for fast one-CSV-per-table bag generation" --body "Adds get_as_file rid_set chunk-append + CatalogBagBuilder rid-set spec emission. Lets the bag exporter fetch a large explicit RID set as one logical query producing one clean CSV per table, eliminating the deep-FK-join cost and per-path fragmentation. Additive: existing query_path csv processors (incl. Chaise exports) are unaffected; rid_sets=None keeps current behavior. Consumer of deriva-ml's client-side reachability engine (PR #300). Per-RID-quoting gotcha pinned by test. Contract test upstream per the workspace cross-repo rule.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

> **Note:** the controller (not the subagent) decides whether to actually merge. The subagent's job ends at "PR opened". Do NOT bump version.

---

## Self-Review

**1. Spec coverage** (against `2026-06-14-stage-b-fast-portable-bag-design.md` D1–D4 and the contract spec):
- D1 `get_as_file` rid_set chunk-append → Tasks 1–3. ✓
- The per-RID-quoting gotcha → Task 2 (isolated + directly tested). ✓
- D1 reuse of the page-append loop → Task 3 (extract `_fetch_paged_csv`, thread `first_page` across chunks). ✓
- Processor passthrough → Task 4. ✓
- D2 Format-B one-csv-per-table emission → Task 5. ✓
- D2 loader unchanged (one CSV per table is a loader subset) → documented in File Structure; no task needed; B2 integration test confirms round-trip. ✓
- D3 deriva-py stays domain-agnostic (consumes `rid_sets`, no reachability logic) → Task 5 takes a supplied map; no FK-reachability added upstream. ✓
- D4 deriva-py PR first with upstream contract tests → Task 6 opens the PR; the contract tests are Tasks 1–5. ✓
- Vocab full-export + asset fetch unchanged → Task 5 note. ✓
- Chaise unaffected (additive param) → Task 5 (rid_sets=None default) + PR body. ✓

**2. Placeholder scan:** No "TBD"/"handle edge cases". Tasks 5's test has explicit `<fixture>` placeholders BY DESIGN — they instruct the implementer to read and reuse the existing `test_catalog_builder.py` fixtures rather than invent names, with a clear note. Every code step shows complete code except where it explicitly defers to "match the live local variable names" (Task 3, Task 5) — these are flagged as READ-first because the surrounding method's exact identifiers must be verified against code, not transcribed from a plan. This is intentional precision, not a placeholder.

**3. Type consistency:**
- `rid_set` (list of RID strings) + `rid_table` ("schema:table") consistent across Tasks 1–5. ✓
- `RID_SET_CHUNK_SIZE = 500` defined Task 1, used Task 3. ✓
- `_rid_set_chunks` (Task 1), `_rid_set_query_url` (Task 2), `_fetch_paged_csv` + `_get_rid_set_as_file` (Task 3) — names consistent where cross-referenced. ✓
- `CatalogBagBuilder(rid_sets=...)` keyed by `(schema, table)` tuple (Task 5) matches the `key` the reached-table loop iterates. ✓
- The export-spec processor shape (`rid_table`, `rid_set`, `output_path`, no `query_path`) is identical between the contract spec, Task 4's forwarding, and Task 5's emission. ✓

**Risk flagged for the implementer:** Task 3's `_fetch_paged_csv` extraction is the one delicate refactor — it moves a long, subtle loop (runtime-limit backoff, multi-line CSV cursor) out of `get_as_file`. The plan mandates a two-move approach (extract-and-prove-green FIRST, then add rid_set) and a STOP-and-report if the extraction can't be made behavior-preserving. This is the task most likely to need a more capable model.
