"""Tests for the defer_record_creation flag on _uploadAsset.

When the flag is set on an asset_mapping, _uploadAsset must:
  - perform the Hatrac upload (we mock this — actual upload tested elsewhere)
  - skip the per-file record GET / POST / UPDATE
  - return {"_deferred_row": ..., "_target_table": ...}
  - run post_processors (with no record context)

When the flag is unset (default), _uploadAsset must behave exactly as before.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def uploader():
    """Build a minimally-mocked GenericUploader for unit testing _uploadAsset.

    We bypass __init__ (which requires server connection) by constructing
    via __new__ and populating only the attributes _uploadAsset reads.
    """
    from deriva.transfer.upload.deriva_upload import GenericUploader

    u = GenericUploader.__new__(GenericUploader)
    u.metadata = {}
    u.processor_output = {}
    u.cancelled = False
    u.skipped_files = []
    u.config = {}
    u.catalog = MagicMock()
    u.identity = {"id": "anon", "display_name": "a", "full_name": "A", "email": "a@b"}
    # Satisfy __del__ -> cleanupTransferState on GC, which would otherwise
    # raise AttributeError and make pytest 9 escalate the warning to an error.
    u.transfer_state_fh = None
    u.transfer_state = {}
    u.transfer_state_locks = {}
    return u


def test_defer_record_creation_skips_record_creation(uploader, tmp_path):
    """With flag true, _uploadAsset returns _deferred_row and never calls record-creation methods."""
    f = tmp_path / "test.txt"
    f.write_bytes(b"hello")

    asset_mapping = {
        "asset_type": "file",
        "target_table": ["S", "T"],
        "checksum_types": ["md5"],
        "column_map": {"MD5": "{md5}", "Filename": "{file_name}", "RID": "{RID}"},
        "hatrac_options": {},
        "hatrac_templates": {"hatrac_uri": "/hatrac/T/{md5}.{file_name}"},
        "use_pre_allocated_rid": True,
        "defer_record_creation": True,
    }
    match_groupdict = {"RID": "R-AAA", "schema": "S", "table": "T"}

    # Patch the Hatrac upload to avoid network. _hatracUpload returns
    # a versioned URI string.
    with patch.object(uploader, "_hatracUpload", return_value="/hatrac/T/abc.test.txt:v1"), \
         patch.object(uploader, "_queryFileMetadata"), \
         patch.object(uploader, "_getFileRecord") as mock_get, \
         patch.object(uploader, "_createFileRecordWithRid") as mock_create, \
         patch.object(uploader, "_catalogRecordCreate") as mock_post:
        result = uploader._uploadAsset(str(f), asset_mapping, match_groupdict)

    assert isinstance(result, dict)
    assert "_deferred_row" in result
    assert "_target_table" in result
    assert result["_target_table"] == uploader.metadata["target_table"]
    # Row should carry the column_map'd fields
    row = result["_deferred_row"]
    assert row["RID"] == "R-AAA"
    assert "MD5" in row
    assert row["Filename"] == "test.txt"

    # Critically: none of the catalog write paths should have been called.
    mock_get.assert_not_called()
    mock_create.assert_not_called()
    mock_post.assert_not_called()


def test_defer_record_creation_default_unchanged(uploader, tmp_path):
    """With flag absent, _uploadAsset hits _createFileRecordWithRid (existing behavior)."""
    f = tmp_path / "test.txt"
    f.write_bytes(b"hello")

    asset_mapping = {
        "asset_type": "file",
        "target_table": ["S", "T"],
        "checksum_types": ["md5"],
        "column_map": {"MD5": "{md5}", "Filename": "{file_name}", "RID": "{RID}"},
        "hatrac_options": {},
        "hatrac_templates": {"hatrac_uri": "/hatrac/T/{md5}.{file_name}"},
        "use_pre_allocated_rid": True,
        # no defer_record_creation
    }
    match_groupdict = {"RID": "R-AAA", "schema": "S", "table": "T"}

    # The "record" returned by _createFileRecordWithRid must match the row that
    # step 8's column_map interpolation produces; otherwise the existing code
    # follows the UPDATE path. Use a sentinel that compares equal to anything to
    # avoid recomputing md5 here, then patch the UPDATE path defensively as well.
    class _AnyDict(dict):
        def __eq__(self, other):
            return True

        def __ne__(self, other):
            return False

        def __hash__(self):
            return 0

    record_sentinel = _AnyDict()

    with patch.object(uploader, "_hatracUpload", return_value="/hatrac/T/abc.test.txt:v1"), \
         patch.object(uploader, "_queryFileMetadata"), \
         patch.object(uploader, "_catalogRecordUpdate"), \
         patch.object(uploader, "_getFileRecord", return_value=(record_sentinel, record_sentinel)), \
         patch.object(uploader, "_createFileRecordWithRid",
                      return_value=(record_sentinel, record_sentinel)) as mock_create:
        uploader._uploadAsset(str(f), asset_mapping, match_groupdict)

    # Default path called the existing record creator
    mock_create.assert_called_once()
