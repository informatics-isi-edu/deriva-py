"""Unit tests for deriva-py's use_pre_allocated_rid asset-mapping flag.

These tests exercise two narrowly-scoped changes on GenericUploader:

1. A fast-fail validation in ``_initFileMetadata`` when the flag is
   set but the regex didn't capture a RID group.
2. A new ``_createFileRecordWithRid`` method that bypasses the
   MD5+Filename lookup in ``_getFileRecord`` and creates (or
   idempotently returns) a catalog row at the caller-supplied RID.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def uploader():
    """Construct a GenericUploader with mocked catalog + identity."""
    from deriva.transfer.upload.deriva_upload import GenericUploader
    inst = GenericUploader.__new__(GenericUploader)
    # Fill the attributes _initFileMetadata and _createFileRecordWithRid read.
    inst.metadata = {}
    inst.catalog = MagicMock()
    inst.identity = {"id": "anon", "display_name": "a", "full_name": "A", "email": "a@b"}
    inst.processor_output = {}
    inst.cancelled = False
    return inst


def test_init_file_metadata_raises_when_flag_set_and_no_rid_captured(uploader):
    from deriva.transfer.upload.deriva_upload import DerivaUploadConfigurationError
    asset_mapping = {
        "use_pre_allocated_rid": True,
        "target_table": ["S", "T"],
    }
    # match_groupdict has NO 'RID' key — regex didn't capture it.
    match_groupdict = {"schema": "S", "asset_table": "T"}
    # Stub getCatalogTable to avoid needing a real catalog model.
    uploader.getCatalogTable = MagicMock(return_value="S:T")
    uploader.getFileDisplayName = MagicMock(return_value="f.bin")
    uploader.getFileSize = MagicMock(return_value=123)

    with pytest.raises(DerivaUploadConfigurationError) as ei:
        uploader._initFileMetadata("/tmp/f.bin", asset_mapping, match_groupdict)
    msg = str(ei.value)
    assert "use_pre_allocated_rid" in msg
    assert "RID" in msg


def test_init_file_metadata_passes_when_flag_set_and_rid_captured(uploader):
    asset_mapping = {
        "use_pre_allocated_rid": True,
        "target_table": ["S", "T"],
    }
    match_groupdict = {"schema": "S", "asset_table": "T", "RID": "1-ABC"}
    uploader.getCatalogTable = MagicMock(return_value="S:T")
    uploader.getFileDisplayName = MagicMock(return_value="f.bin")
    uploader.getFileSize = MagicMock(return_value=123)

    # Should not raise.
    uploader._initFileMetadata("/tmp/f.bin", asset_mapping, match_groupdict)
    assert uploader.metadata["RID"] == "1-ABC"


def test_init_file_metadata_passes_when_flag_absent(uploader):
    """Legacy callers (flag not set) see no behavior change — no RID check."""
    asset_mapping = {"target_table": ["S", "T"]}
    match_groupdict = {"schema": "S", "asset_table": "T"}
    uploader.getCatalogTable = MagicMock(return_value="S:T")
    uploader.getFileDisplayName = MagicMock(return_value="f.bin")
    uploader.getFileSize = MagicMock(return_value=123)

    # Should not raise — legacy path doesn't require RID.
    uploader._initFileMetadata("/tmp/f.bin", asset_mapping, match_groupdict)
    assert "RID" not in uploader.metadata
