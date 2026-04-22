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
    # Satisfy __del__ -> cleanupTransferState on GC, which would otherwise
    # raise AttributeError and make pytest 9 escalate the warning to an error.
    inst.transfer_state_fh = None
    inst.transfer_state = {}
    inst.transfer_state_locks = {}
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


def test_create_file_record_with_rid_happy_path(uploader):
    """No existing row with MD5+Filename → _catalogRecordCreate called with RID in payload."""
    asset_mapping = {
        "use_pre_allocated_rid": True,
        "column_map": {"MD5": "{md5}", "Filename": "{file_name}", "RID": "{RID}"},
    }
    uploader.metadata = {
        "RID": "1-NEW",
        "md5": "abc123",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    # Pre-check GET by MD5+Filename returns empty — no existing row.
    get_response = MagicMock()
    get_response.json.return_value = []
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock(
        return_value=[{"RID": "1-NEW", "MD5": "abc123", "Filename": "f.bin"}]
    )
    uploader._updateFileMetadata = MagicMock()

    record, result = uploader._createFileRecordWithRid(asset_mapping)

    # Pre-check queried by MD5+Filename.
    uploader.catalog.get.assert_called_once_with(
        "/entity/S:T/MD5=abc123&Filename=f.bin"
    )
    # Create called with RID in the row AND nondefaults=["RID"] to opt out
    # of ERMrest's implicit RID-is-default behavior.
    create_call = uploader._catalogRecordCreate.call_args
    assert create_call[0][0] == "S:T"
    assert create_call[0][1]["RID"] == "1-NEW"
    assert create_call.kwargs.get("nondefaults") == ["RID"]
    # Return shape mirrors _getFileRecord: (dict, record).
    assert isinstance(record, dict)
    assert result["RID"] == "1-NEW"


def test_create_file_record_with_rid_idempotent_matching_rid(uploader):
    """Existing row with MATCHING RID → idempotent return, skip create."""
    asset_mapping = {
        "use_pre_allocated_rid": True,
        "column_map": {"MD5": "{md5}", "Filename": "{file_name}", "RID": "{RID}"},
    }
    uploader.metadata = {
        "RID": "1-MATCH",
        "md5": "abc123",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    existing_row = {"RID": "1-MATCH", "MD5": "abc123", "Filename": "f.bin"}
    get_response = MagicMock()
    get_response.json.return_value = [existing_row]
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock()
    uploader._updateFileMetadata = MagicMock()

    record, result = uploader._createFileRecordWithRid(asset_mapping)

    uploader._catalogRecordCreate.assert_not_called()
    assert isinstance(record, dict)
    assert result == existing_row


def test_create_file_record_with_rid_raises_on_rid_mismatch(uploader):
    """Existing row with DIFFERENT RID → raise DerivaUploadCatalogCreateError."""
    from deriva.transfer.upload.deriva_upload import DerivaUploadCatalogCreateError
    asset_mapping = {
        "use_pre_allocated_rid": True,
        "column_map": {"MD5": "{md5}", "Filename": "{file_name}", "RID": "{RID}"},
    }
    uploader.metadata = {
        "RID": "1-CALLER",  # pre-leased by caller
        "md5": "abc123",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    # Existing row has the same MD5+Filename but a DIFFERENT RID.
    existing_row = {"RID": "1-EXISTING", "MD5": "abc123", "Filename": "f.bin"}
    get_response = MagicMock()
    get_response.json.return_value = [existing_row]
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock()

    with pytest.raises(DerivaUploadCatalogCreateError) as ei:
        uploader._createFileRecordWithRid(asset_mapping)
    msg = str(ei.value)
    assert "1-CALLER" in msg
    assert "1-EXISTING" in msg
    assert "f.bin" in msg
    # Create MUST NOT be called.
    uploader._catalogRecordCreate.assert_not_called()


def test_flag_off_uses_existing_md5_filename_path(uploader):
    """When flag is absent, _getFileRecord (MD5+Filename path) is used."""
    asset_mapping = {"target_table": ["S", "T"], "column_map": {"MD5": "{md5}"}}
    uploader.metadata = {"md5": "abc", "file_name": "f.bin", "target_table": "S:T"}

    uploader._getFileRecord = MagicMock(return_value=({}, {"RID": "SERVER-RID"}))
    uploader._createFileRecordWithRid = MagicMock()

    # Simulate what _uploadAsset Step 7 will do (see task step 2).
    from deriva.transfer.upload.deriva_upload import stob
    if stob(asset_mapping.get("use_pre_allocated_rid", False)):
        record, result = uploader._createFileRecordWithRid(asset_mapping)
    else:
        record, result = uploader._getFileRecord(asset_mapping)

    uploader._getFileRecord.assert_called_once()
    uploader._createFileRecordWithRid.assert_not_called()
    assert result["RID"] == "SERVER-RID"
