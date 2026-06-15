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
