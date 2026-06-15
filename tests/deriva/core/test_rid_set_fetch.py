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
