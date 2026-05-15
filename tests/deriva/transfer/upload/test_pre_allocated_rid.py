"""Unit tests for deriva-py's use_pre_allocated_rid asset-mapping flag.

These tests exercise two narrowly-scoped changes on GenericUploader:

1. A fast-fail validation in ``_initFileMetadata`` when the flag is
   set but the regex didn't capture a RID group.
2. Pre-allocated-RID semantics inside ``_getFileRecord`` when the
   flag is set: the catalog lookup goes through the config-driven
   ``record_query_template``; on a hit the caller-supplied RID is
   compared to the existing row's RID (strict-or-soft per table
   annotation); on a miss the create path passes
   ``nondefaults=["RID"]`` so ERMrest honors the caller's RID.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def uploader():
    """Construct a GenericUploader with mocked catalog + identity."""
    from deriva.transfer.upload.deriva_upload import GenericUploader
    inst = GenericUploader.__new__(GenericUploader)
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

    uploader._initFileMetadata("/tmp/f.bin", asset_mapping, match_groupdict)
    assert uploader.metadata["RID"] == "1-ABC"


def test_init_file_metadata_passes_when_flag_absent(uploader):
    """Legacy callers (flag not set) see no behavior change — no RID check."""
    asset_mapping = {"target_table": ["S", "T"]}
    match_groupdict = {"schema": "S", "asset_table": "T"}
    uploader.getCatalogTable = MagicMock(return_value="S:T")
    uploader.getFileDisplayName = MagicMock(return_value="f.bin")
    uploader.getFileSize = MagicMock(return_value=123)

    uploader._initFileMetadata("/tmp/f.bin", asset_mapping, match_groupdict)
    assert "RID" not in uploader.metadata


def _base_mapping():
    """Asset mapping with a config-driven record_query_template."""
    return {
        "use_pre_allocated_rid": True,
        "record_query_template": "/entity/{target_table}/MD5={md5}&Filename={file_name}",
        "column_map": {"MD5": "{md5}", "Filename": "{file_name}", "RID": "{RID}"},
    }


def test_get_file_record_with_rid_happy_path(uploader):
    """No existing row → _catalogRecordCreate called with RID + nondefaults=['RID']."""
    asset_mapping = _base_mapping()
    uploader.metadata = {
        "RID": "1-NEW",
        "md5": "abc123",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    get_response = MagicMock()
    get_response.json.return_value = []
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock(
        return_value=[{"RID": "1-NEW", "MD5": "abc123", "Filename": "f.bin"}]
    )
    uploader._updateFileMetadata = MagicMock()

    record, result = uploader._getFileRecord(asset_mapping)

    # Lookup went through the config-driven template, not a hardcoded URL.
    uploader.catalog.get.assert_called_once_with(
        "/entity/S:T/MD5=abc123&Filename=f.bin"
    )
    create_call = uploader._catalogRecordCreate.call_args
    assert create_call[0][0] == "S:T"
    assert create_call[0][1]["RID"] == "1-NEW"
    assert create_call.kwargs.get("nondefaults") == ["RID"]
    assert isinstance(record, dict)
    assert result["RID"] == "1-NEW"


def test_get_file_record_with_rid_idempotent_matching_rid(uploader):
    """Existing row with MATCHING RID → idempotent return, skip create."""
    asset_mapping = _base_mapping()
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

    record, result = uploader._getFileRecord(asset_mapping)

    uploader._catalogRecordCreate.assert_not_called()
    assert isinstance(record, dict)
    assert result == existing_row


def test_get_file_record_with_rid_raises_on_rid_mismatch_when_strict(uploader):
    """Existing row with different RID + strict annotation set → raise."""
    from deriva.transfer.upload.deriva_upload import DerivaUploadCatalogCreateError
    asset_mapping = _base_mapping()
    uploader.metadata = {
        "RID": "1-CALLER",
        "md5": "abc123",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    existing_row = {"RID": "1-EXISTING", "MD5": "abc123", "Filename": "f.bin"}
    get_response = MagicMock()
    get_response.json.return_value = [existing_row]
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock()

    table_obj = MagicMock()
    table_obj.annotations = {
        "tag:isrd.isi.edu,2026:strict-preallocated-rid": {"strict": True}
    }
    schema_obj = MagicMock()
    schema_obj.tables = {"T": table_obj}
    catalog_model = MagicMock()
    catalog_model.schemas = {"S": schema_obj}
    uploader.catalog_model = catalog_model
    uploader.catalog.splitQualifiedCatalogName = MagicMock(return_value=("S", "T"))

    with pytest.raises(DerivaUploadCatalogCreateError) as ei:
        uploader._getFileRecord(asset_mapping)
    msg = str(ei.value)
    assert "1-CALLER" in msg
    assert "1-EXISTING" in msg
    assert "f.bin" in msg
    uploader._catalogRecordCreate.assert_not_called()


def test_get_file_record_with_rid_soft_mode_adopts_existing_rid(uploader):
    """Existing row with different RID + annotation absent → soft fallback.

    Returns the existing row's RID and updates self.metadata['RID'] so
    downstream processing sees the final RID.
    """
    asset_mapping = _base_mapping()
    uploader.metadata = {
        "RID": "1-CALLER",
        "md5": "abc123",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    existing_row = {"RID": "1-EXISTING", "MD5": "abc123", "Filename": "f.bin"}
    get_response = MagicMock()
    get_response.json.return_value = [existing_row]
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock()
    uploader._updateFileMetadata = MagicMock()

    table_obj = MagicMock()
    table_obj.annotations = {}
    schema_obj = MagicMock()
    schema_obj.tables = {"T": table_obj}
    catalog_model = MagicMock()
    catalog_model.schemas = {"S": schema_obj}
    uploader.catalog_model = catalog_model
    uploader.catalog.splitQualifiedCatalogName = MagicMock(return_value=("S", "T"))

    record, result = uploader._getFileRecord(asset_mapping)

    assert uploader.metadata["RID"] == "1-EXISTING"
    assert result == existing_row
    uploader._catalogRecordCreate.assert_not_called()


def test_get_file_record_with_rid_soft_mode_when_strict_false(uploader):
    """Annotation present with strict=false → treated the same as absent."""
    asset_mapping = _base_mapping()
    uploader.metadata = {
        "RID": "1-CALLER",
        "md5": "abc123",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    existing_row = {"RID": "1-EXISTING", "MD5": "abc123", "Filename": "f.bin"}
    get_response = MagicMock()
    get_response.json.return_value = [existing_row]
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock()
    uploader._updateFileMetadata = MagicMock()

    table_obj = MagicMock()
    table_obj.annotations = {
        "tag:isrd.isi.edu,2026:strict-preallocated-rid": {"strict": False}
    }
    schema_obj = MagicMock()
    schema_obj.tables = {"T": table_obj}
    catalog_model = MagicMock()
    catalog_model.schemas = {"S": schema_obj}
    uploader.catalog_model = catalog_model
    uploader.catalog.splitQualifiedCatalogName = MagicMock(return_value=("S", "T"))

    record, result = uploader._getFileRecord(asset_mapping)

    assert uploader.metadata["RID"] == "1-EXISTING"
    assert result == existing_row
    uploader._catalogRecordCreate.assert_not_called()


def test_flag_off_does_not_pass_nondefaults_rid(uploader):
    """Without the flag, the create path goes through the legacy code — no nondefaults=RID."""
    asset_mapping = {
        "record_query_template": "/entity/{target_table}/MD5={md5}&Filename={file_name}",
        "column_map": {"MD5": "{md5}", "Filename": "{file_name}"},
    }
    uploader.metadata = {
        "md5": "abc",
        "file_name": "f.bin",
        "target_table": "S:T",
    }

    get_response = MagicMock()
    get_response.json.return_value = []
    uploader.catalog.get.return_value = get_response

    uploader._catalogRecordCreate = MagicMock(
        return_value=[{"RID": "SERVER-RID", "MD5": "abc", "Filename": "f.bin"}]
    )
    uploader._updateFileMetadata = MagicMock()

    record, result = uploader._getFileRecord(asset_mapping)

    create_call = uploader._catalogRecordCreate.call_args
    # No nondefaults arg (or None) when flag is off.
    assert create_call.kwargs.get("nondefaults") is None
    assert result["RID"] == "SERVER-RID"
