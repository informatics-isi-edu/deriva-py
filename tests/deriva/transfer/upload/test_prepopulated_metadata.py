"""Unit tests for the ``prepopulated_metadata`` asset-mapping key.

The ``prepopulated_metadata`` key on an asset-mapping is an opt-in
overlay merged into ``self.metadata`` during
:meth:`DerivaUpload._initFileMetadata`, after the regex groupdict and
before the framework-derived fields. It lets callers feed row metadata
into the upload pipeline from a source other than the filesystem (e.g.,
a bag's CSV row), so the recipe-driven uploader can be used by callers
that know their metadata externally rather than deriving it from
filenames.

See the docstring on :meth:`DerivaUpload._initFileMetadata` for the
precise interaction order with framework-derived fields.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def uploader():
    """Construct a GenericUploader with mocked catalog + identity.

    Bypasses ``__init__`` (which would try to load credentials and a
    server config) and populates only the attributes the methods under
    test read.
    """
    from deriva.transfer.upload.deriva_upload import GenericUploader
    inst = GenericUploader.__new__(GenericUploader)
    inst.metadata = {}
    inst.catalog = MagicMock()
    inst.identity = {"id": "anon", "display_name": "a", "full_name": "A", "email": "a@b"}
    inst.processor_output = {}
    inst.cancelled = False
    # Satisfy __del__ → cleanupTransferState on GC; otherwise pytest 9
    # escalates the AttributeError to a test error.
    inst.transfer_state_fh = None
    inst.transfer_state = {}
    inst.transfer_state_locks = {}
    # Stub the helpers _initFileMetadata calls so the test doesn't need
    # a real catalog model or an on-disk file.
    inst.getCatalogTable = MagicMock(return_value="S:T")
    inst.getFileDisplayName = MagicMock(return_value="payload.dat")
    inst.getFileSize = MagicMock(return_value=4096)
    return inst


def test_prepopulated_metadata_absent_is_no_op(uploader):
    """Asset mappings without the new key behave exactly as before.

    Existing callers (every current asset_mapping) shouldn't see any
    difference. Verifies the opt-in property is genuine.
    """
    asset_mapping = {"target_table": ["S", "T"]}
    match_groupdict = {"schema": "S", "asset_table": "T"}

    uploader._initFileMetadata("/tmp/payload.dat", asset_mapping, match_groupdict)

    # Framework-derived fields and groupdict captures are present.
    assert uploader.metadata["schema"] == "S"
    assert uploader.metadata["asset_table"] == "T"
    assert uploader.metadata["file_name"] == "payload.dat"
    assert uploader.metadata["file_size"] == 4096
    # No spurious overlay-derived keys leaked in.
    assert "URI" not in uploader.metadata
    assert "MD5" not in uploader.metadata


def test_prepopulated_metadata_supplies_external_values(uploader):
    """Caller-supplied values land in self.metadata and survive."""
    asset_mapping = {
        "target_table": ["S", "T"],
        "prepopulated_metadata": {
            "URI": "/hatrac/Image/abc.png",
            "MD5": "deadbeef",
            "Length": 12345,
            "Description": "training image",
        },
    }
    match_groupdict = {"schema": "S", "asset_table": "T", "RID": "1-ABC"}

    uploader._initFileMetadata("/tmp/payload.dat", asset_mapping, match_groupdict)

    assert uploader.metadata["URI"] == "/hatrac/Image/abc.png"
    assert uploader.metadata["MD5"] == "deadbeef"
    assert uploader.metadata["Length"] == 12345
    assert uploader.metadata["Description"] == "training image"
    # Groupdict captures still present.
    assert uploader.metadata["RID"] == "1-ABC"
    # Framework-derived fields still computed.
    assert uploader.metadata["file_size"] == 4096


def test_prepopulated_metadata_overrides_groupdict(uploader):
    """When a key exists in both groupdict and overlay, overlay wins.

    Caller-supplied values are treated as authoritative — typically they
    come from a structured source (CSV row, JSON manifest) while the
    groupdict captures come from path-pattern parsing, which is less
    precise.
    """
    asset_mapping = {
        "target_table": ["S", "T"],
        "prepopulated_metadata": {
            "RID": "1-OVERLAY",
            "extra_col": "from-overlay",
        },
    }
    match_groupdict = {"RID": "1-FROMPATH", "extra_col": "from-path"}

    uploader._initFileMetadata("/tmp/payload.dat", asset_mapping, match_groupdict)

    # Overlay wins over groupdict for both keys.
    assert uploader.metadata["RID"] == "1-OVERLAY"
    assert uploader.metadata["extra_col"] == "from-overlay"


def test_framework_fields_override_prepopulated_metadata(uploader):
    """Framework-derived fields are last-wins over any overlay value.

    A caller can put ``file_name`` etc. in ``prepopulated_metadata``
    without breaking framework invariants — the value is silently
    overwritten by the framework's own computation. Surfaces as
    "harmless but ineffective" rather than a config error.
    """
    asset_mapping = {
        "target_table": ["S", "T"],
        "prepopulated_metadata": {
            "file_name": "WRONG.bin",
            "file_size": 999,
        },
    }
    match_groupdict = {}

    uploader._initFileMetadata("/tmp/payload.dat", asset_mapping, match_groupdict)

    # Framework values present and correct — overlay had no effect.
    assert uploader.metadata["file_name"] == "payload.dat"
    assert uploader.metadata["file_size"] == 4096


def test_prepopulated_metadata_combines_with_use_pre_allocated_rid(uploader):
    """Overlay can supply RID for use_pre_allocated_rid asset mappings.

    Today ``use_pre_allocated_rid`` requires a ``(?P<RID>...)`` capture
    in the file_pattern regex. The overlay is a second path to satisfy
    the same check: as long as ``self.metadata["RID"]`` is set by the
    time the validation runs, the source (groupdict OR overlay) doesn't
    matter.
    """
    asset_mapping = {
        "target_table": ["S", "T"],
        "use_pre_allocated_rid": True,
        "prepopulated_metadata": {"RID": "1-XYZ"},
    }
    # Groupdict does NOT capture RID — overlay must supply it.
    match_groupdict = {"schema": "S", "asset_table": "T"}

    # Should not raise — overlay populated RID before the check.
    uploader._initFileMetadata("/tmp/payload.dat", asset_mapping, match_groupdict)

    assert uploader.metadata["RID"] == "1-XYZ"
